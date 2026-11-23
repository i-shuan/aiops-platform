"""資料源替身。trace／日誌：回應平台真的 GrafanaSpec 發出的請求（tests/grafana_fake.FakeGrafana 解開資料源 proxy 的
_msearch，再以「/<索引>/_search」的樣子交給這裡的 ChaosES.handler）。指標：平台不再直連 Elasticsearch（2026-11-20），
沒有任何平台資料源會送下面這種 date_histogram 查詢了；改由模擬器自己的資料源 chaos_metrics（ChaosMetricsSpec，
查詢語法在 scripts/chaos/metric_query.py，就是原本平台的 ES 指標查詢搬過來）直接問 ChaosES._metrics，數值算法不變。

- metrics-apm.transaction.1m-*：date_histogram（fixed_interval、extended_bounds、min_doc_count 0）+ avg／sum／max／min／count，
  filter 認 @timestamp 範圍、service.name、event.outcome。value_field 認 transaction.duration.summary.sum／value_count。
  每分鐘每服務最多兩份文件（success、failure），跟 Elastic APM 一樣；avg 是「桶內有文件的」平均。
- traces-apm-*、logs-apm.*：沿用 tests/test_dynamic_topology 的 _FakeES（範圍、服務名、trace.id、search_after、random_score 抽樣、
  track_total_hits 都解讀），文件是世界模擬器產的 APM transaction／span／error。
整個替身只在記憶體裡，不連任何真的 Grafana／ES、不寫任何索引（平台本來就只讀）。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from test_dynamic_topology import _FakeES  # noqa: E402  會解讀查詢的假 ES（範圍、服務名、抽樣、分頁）

from aiops.sources.base import BaseDataSourceSpec, InvalidQueryError, NoDataError  # noqa: E402
from aiops.sources.query.window import align_window  # noqa: E402
from scripts.chaos.metric_query import bucket_value, build_search_body, parse_es_query  # noqa: E402
from scripts.chaos.world import World  # noqa: E402

METRIC_SOURCE = "chaos_metrics"  # 模擬器自己的指標資料源型別（harness.sources_for 註冊進 aiops.sources._SPECS）


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


class ChaosES:
    def __init__(self, world: World, trace_lo: datetime | None = None, trace_hi: datetime | None = None):
        self.world = world
        world.build()
        docs, logs = world.traces(trace_lo, trace_hi) if trace_lo and trace_hi else ([], [])
        self.trace_es, self.log_es = _FakeES(docs), _FakeES(logs)
        self.requests: list[tuple[str, dict]] = []
        self.trace_docs, self.log_docs = docs, logs

    # ── httpx 入口 ──
    def handler(self, req: httpx.Request) -> httpx.Response:
        path = req.url.path
        body = json.loads(req.content) if req.content else {}
        self.requests.append((path, body))
        if "metrics-apm" in path:
            return httpx.Response(200, json=self._metrics(body))
        if "traces-apm" in path:
            return self.trace_es.handler(req)
        if "logs-apm" in path:
            return self.log_es.handler(req)
        return httpx.Response(404, json={"error": {"reason": f"chaos es: unknown index {path}"}})

    def metric_spec(self) -> type[BaseDataSourceSpec]:
        """讀這個世界的指標資料源類別。fetch_data 跟原本平台的 ElasticsearchSpec.fetch_data 同一套步驟
        （解析查詢 -> 對齊時窗 -> date_histogram body -> 每桶取值 -> 丟掉沒資料與 NaN／Inf 的桶），只是不經 HTTP。"""
        es = self

        class ChaosMetricsSpec(BaseDataSourceSpec):
            source_type = METRIC_SOURCE

            def fetch_data(self, query, start, end, step_seconds, field_map=None):
                try:
                    q = parse_es_query(query, field_map)
                except ValueError as e:
                    raise InvalidQueryError(str(e)) from e
                s, e = align_window(start, end, step_seconds)
                body = build_search_body(q, s, e, step_seconds)
                es.requests.append((f"/{q.index}/_search", body))
                buckets = es._metrics(body)["aggregations"]["series"]["buckets"]
                points = [(b["key"], bucket_value(q, b)) for b in buckets]
                points = [(t, v) for t, v in points if v is not None and np.isfinite(v)]
                if not points:
                    raise NoDataError(f"chaos_metrics 在這段時間沒有資料：index={q.index}")
                return pd.DataFrame({"ts": pd.to_datetime([t for t, _ in points], unit="ms", utc=True),
                                     "value": [v for _, v in points]})

            def check(self):
                return {"ok": True, "detail": "chaos world"}

        return ChaosMetricsSpec

    # ── 指標 ──
    def _metrics(self, body: dict) -> dict:
        w = self.world
        filters = body["query"]["bool"]["filter"]
        lo = hi = None
        svc = outcome = None
        for f in filters:
            if "range" in f:
                r = f["range"]["@timestamp"]
                lo, hi = _ts(r["gte"]), _ts(r.get("lt") or r["lte"])
            elif "term" in f:
                (field, v), = f["term"].items()
                v = v["value"] if isinstance(v, dict) else v
                if field == "service.name":
                    svc = v
                elif field == "event.outcome":
                    outcome = v
        series = body["aggs"]["series"]
        step = int(series["date_histogram"]["fixed_interval"].rstrip("s"))
        sub = series.get("aggs", {}).get("v")
        agg, field = (next(iter(sub.items())) if sub else ("count", {"field": None}))
        field = field["field"] if isinstance(field, dict) else None
        den = series.get("aggs", {}).get("d")  # divide_by 的分母（一律 sum）
        den_field = den["sum"]["field"] if den else None
        bounds = series["date_histogram"].get("extended_bounds") or {}
        b_lo = _ts(bounds["min"]) if bounds else lo
        b_hi = _ts(bounds["max"]) if bounds else hi
        first = int(b_lo.timestamp() // step * step)
        keys = np.arange(first, int(b_hi.timestamp()), step)
        buckets = []
        per = max(step // 60, 1)
        m0 = int((first - w.t0.timestamp()) // 60)
        for i, k in enumerate(keys):
            ms = np.arange(m0 + i * per, m0 + (i + 1) * per)
            ms = ms[(ms >= 0) & (ms < w.minutes)]
            ms = ms[[(w.t0.timestamp() + int(m) * 60) >= lo.timestamp() and (w.t0.timestamp() + int(m) * 60) < hi.timestamp() for m in ms]] if len(ms) else ms
            vals, dens, n_docs = [], [], 0
            if svc in w.svcs and len(ms):
                for which in ("success", "failure"):
                    if outcome and outcome != which:
                        continue
                    n = (w.n_ok if which == "success" else w.n_fail)[svc][ms]
                    s_us = (w.sum_ok if which == "success" else w.sum_fail)[svc][ms]
                    present = n > 0
                    n_docs += int(present.sum())
                    if field == "transaction.duration.summary.sum":
                        vals += list(s_us[present])
                    elif field == "transaction.duration.summary.value_count":
                        vals += list(n[present].astype(float))
                    if den_field == "transaction.duration.summary.value_count":
                        dens += list(n[present].astype(float))
                    elif den_field == "transaction.duration.summary.sum":
                        dens += list(s_us[present])
            bucket = {"key": int(k) * 1000, "doc_count": n_docs}
            if sub:
                if vals:
                    v = {"avg": float(np.mean(vals)), "sum": float(np.sum(vals)), "max": float(np.max(vals)), "min": float(np.min(vals))}[agg]
                    bucket["v"] = {"value": v}
                else:
                    bucket["v"] = {"value": 0.0 if agg == "sum" else None}
                if den:
                    bucket["d"] = {"value": float(np.sum(dens)) if dens else 0.0}  # 真實 ES 的 sum 對空桶回 0.0
            buckets.append(bucket)
        return {"took": 1, "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []}, "aggregations": {"series": {"buckets": buckets}}}
