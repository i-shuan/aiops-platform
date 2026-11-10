"""Elasticsearch 的 HTTP 層替身：回應平台真的 ElasticsearchSpec 發出的 _search 請求。

- metrics-apm.transaction.1m-*：date_histogram（fixed_interval、extended_bounds、min_doc_count 0）+ avg／sum／max／min／count，
  filter 認 @timestamp 範圍、service.name、event.outcome。value_field 認 transaction.duration.summary.sum／value_count。
  每分鐘每服務最多兩份文件（success、failure），跟 Elastic APM 一樣；avg 是「桶內有文件的」平均。
- traces-apm-*、logs-apm.*：沿用 tests/test_dynamic_topology 的 _FakeES（範圍、服務名、trace.id、search_after、random_score 抽樣、
  track_total_hits 都解讀），文件是世界模擬器產的 APM transaction／span／error。
整個替身只在記憶體裡，不連任何真的 ES、不寫任何索引（平台本來就只讀）。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from test_dynamic_topology import _FakeES  # noqa: E402  會解讀查詢的假 ES（範圍、服務名、抽樣、分頁）

from scripts.chaos.world import World  # noqa: E402


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
        if req.method == "GET" and path in ("", "/"):
            return httpx.Response(200, json={"version": {"number": "8.19.0"}})
        body = json.loads(req.content) if req.content else {}
        self.requests.append((path, body))
        if "metrics-apm" in path:
            return httpx.Response(200, json=self._metrics(body))
        if "traces-apm" in path:
            return self.trace_es.handler(req)
        if "logs-apm" in path:
            return self.log_es.handler(req)
        return httpx.Response(404, json={"error": {"reason": f"chaos es: unknown index {path}"}})

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
