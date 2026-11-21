"""跟公司 Grafana 講話的唯一出口：只用 GRAFANA_URL + 一把 Editor Token，不直連任何資料庫、Prometheus 或 ES。

  list_rules        GET  /api/v1/provisioning/alert-rules        讀貼了 aiops_detect 標籤的告警規則
  query_series      POST /api/ds/query                           重放規則的查詢，拿歷史與最新時序（每段最多 5,000 點）
  search_errors     POST /api/datasources/proxy/uid/<uid>/_msearch  經 Grafana 的 ES 資料源查錯誤日誌與 Trace 片段
  upsert_annotation POST／PATCH /api/annotations                  每個事故一筆註記

回應格式以 tests/fixtures/grafana13/（Grafana 13.0.7 實際回應）為準。防呆原則：格式不對就明講是哪條規則的查詢要改，
不默默取第一欄、第一條序列。
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
import pandas as pd

log = logging.getLogger("aiops_lite.grafana")

MAX_POINTS = 5000                      # 單次 /api/ds/query 的點數上限；訓練窗比它長就分段
RETRY_STATUS = {429, 502, 503, 504}
SEVERITIES = ("P0", "P1", "P2", "P3")
DEFAULT_SEVERITY = "P3"                # aiops_severity 沒寫或寫錯：當最低優先權，並列進這一輪的問題清單
EXPRESSION_UIDS = {"__expr__", "-100"}  # Grafana 的運算式節點（reduce／threshold），不是資料源查詢
ERROR_LEVELS = ["ERROR", "error", "Error", "FATAL", "fatal", "CRITICAL", "critical"]
ERROR_STATUS = ["error", "ERROR", "Error", "STATUS_CODE_ERROR"]


class GrafanaError(Exception):
    """Grafana 連不上、Token 沒權限、回應不是預期的格式：這一輪做不下去。"""


class QueryError(GrafanaError):
    """某一條規則的查詢不能用（寫錯、多個數值欄、長格式、沒有時間條件）：只影響那條規則。"""


@dataclass(frozen=True)
class Rule:
    uid: str
    title: str
    service: str
    severity: str
    datasource_uid: str
    model: dict[str, Any] = field(hash=False)

    @property
    def query_hash(self) -> str:
        """查詢或資料源改了，舊基準線就不能用了。"""
        raw = json.dumps([self.datasource_uid, self.model], sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


class Grafana:
    def __init__(self, url: str, token: str, transport: httpx.BaseTransport | None = None, timeout: float = 30.0):
        self.url = url.rstrip("/")
        self._http = httpx.Client(base_url=self.url, timeout=timeout, transport=transport,
                                  headers={"Authorization": f"Bearer {token}"})

    def close(self) -> None:
        self._http.close()

    def _request(self, method: str, path: str, *, accept: tuple[int, ...] = (), **kw: Any) -> Any:
        """短暫抖動（連線失敗、逾時、429／502／503／504）重試 2 次；其他錯誤直接丟 GrafanaError。"""
        last = ""
        for attempt in range(3):
            if attempt:
                time.sleep(0.5 * attempt)
            try:
                r = self._http.request(method, path, **kw)
            except httpx.TransportError as e:
                last = f"{type(e).__name__}: {e}"
                continue
            if r.status_code in RETRY_STATUS:
                last = f"HTTP {r.status_code}"
                continue
            if r.status_code >= 400 and r.status_code not in accept:
                raise GrafanaError(f"{method} {path} -> HTTP {r.status_code}: {r.text[:300]}")
            try:
                return r.json()
            except ValueError as e:
                raise GrafanaError(f"{method} {path} 回的不是 JSON：{r.text[:200]}") from e
        raise GrafanaError(f"{method} {path} 重試後仍失敗：{last}")

    # ── 規則 ──

    def list_rules(self) -> tuple[list[Rule], list[str]]:
        """回傳 (納管的規則, 問題清單)。問題不擋其他規則：少了 service 標籤的規則略過並列出來。"""
        raw = self._request("GET", "/api/v1/provisioning/alert-rules")
        if not isinstance(raw, list):
            raise GrafanaError("告警規則清單不是陣列")
        rules, problems = [], []
        for item in raw:
            labels = item.get("labels") or {}
            if str(labels.get("aiops_detect", "")).strip().lower() not in {"true", "1", "yes", "on"}:
                continue
            uid, title = str(item.get("uid", "")), str(item.get("title", ""))
            if item.get("isPaused"):
                continue
            service = str(labels.get("service", "")).strip()
            if not service:
                problems.append(f"規則「{title}」({uid}) 沒有 service 標籤：略過")
                continue
            severity = str(labels.get("aiops_severity", "")).strip().upper()
            if severity not in SEVERITIES:
                problems.append(f"規則「{title}」({uid}) 的 aiops_severity={labels.get('aiops_severity')!r} "
                                f"不是 P0～P3：當成 {DEFAULT_SEVERITY}")
                severity = DEFAULT_SEVERITY
            queries = [q for q in item.get("data") or [] if q.get("datasourceUid") not in EXPRESSION_UIDS]
            if not queries:
                problems.append(f"規則「{title}」({uid}) 沒有資料源查詢：略過")
                continue
            if len(queries) > 1:
                problems.append(f"規則「{title}」({uid}) 有 {len(queries)} 個資料源查詢：只重放第一個"
                                f"（{queries[0].get('refId')}）")
            rules.append(Rule(uid, title, service, severity, str(queries[0]["datasourceUid"]),
                              dict(queries[0].get("model") or {})))
        return rules, problems

    # ── 時序 ──

    def query_series(self, rule: Rule, start: datetime, end: datetime, step_seconds: int) -> pd.DataFrame:
        """重放規則的查詢，回傳 DataFrame(ts[UTC], value)，對齊到 step 的格線。超過 5,000 點就分段查。"""
        step_ms = step_seconds * 1000
        frames, cursor, end_ms = [], _ms(start), _ms(end)
        while cursor < end_ms:
            chunk_end = min(end_ms, cursor + MAX_POINTS * step_ms)
            frames.append(self._query_chunk(rule, cursor, chunk_end, step_ms))
            cursor = chunk_end
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame({"ts": [], "value": []})
        if df.empty:
            return pd.DataFrame({"ts": pd.Series([], dtype="datetime64[ns, UTC]"), "value": pd.Series([], dtype=float)})
        df["ts"] = pd.to_datetime(df["ts"] // step_ms * step_ms, unit="ms", utc=True)
        return df.groupby("ts", as_index=False)["value"].mean().sort_values("ts").reset_index(drop=True)

    def _query_chunk(self, rule: Rule, from_ms: int, to_ms: int, step_ms: int) -> pd.DataFrame:
        model = {k: v for k, v in rule.model.items() if k not in ("intervalMs", "maxDataPoints", "datasource")}
        if "expr" in model:
            # 告警規則的 PromQL 常是 instant（只要最新一點）；重放要的是整段時序
            model.update({"instant": False, "range": True})
            if model.get("queryType") == "instant":
                model["queryType"] = "range"
        body = {"from": str(from_ms), "to": str(to_ms), "queries": [{
            **model, "refId": "A", "datasource": {"uid": rule.datasource_uid},
            "intervalMs": step_ms, "maxDataPoints": MAX_POINTS}]}
        # 查詢寫錯時 Grafana 回 400，錯誤訊息在 results.A.error：那是這條規則的問題，不是 Grafana 掛了
        raw = self._request("POST", "/api/ds/query", json=body, accept=(400,))
        return parse_frames(raw, rule, from_ms, to_ms, step_ms)

    # ── 日誌與 Trace（經 Grafana 的 ES 資料源代理）──

    def search_errors(self, datasource_uid: str, log_index: str, trace_index: str | None, services: list[str],
                      start: datetime, end: datetime, size: int = 100) -> dict[str, list[dict[str, Any]]]:
        """撈時間窗內這些系統的錯誤日誌與錯誤 span，回傳 {"logs": [...], "traces": [...]}（各是 ES 的 _source）。"""
        window = {"range": {"@timestamp": {"gte": _ms(start), "lte": _ms(end), "format": "epoch_millis"}}}

        def either(name: str, values: list[str]) -> dict[str, Any]:
            # 欄位是 keyword 或 text（動態 mapping 會多一個 .keyword）都查得到
            return {"bool": {"minimum_should_match": 1, "should": [
                {"terms": {name: values}}, {"terms": {f"{name}.keyword": values}}]}}

        def search(index: str, extra: dict[str, Any]) -> str:
            query = {"bool": {"filter": [window, either("service.name", services), extra]}}
            return (json.dumps({"index": index, "ignore_unavailable": True}) + "\n"
                    + json.dumps({"size": size, "sort": [{"@timestamp": "desc"}], "query": query}) + "\n")

        kinds = [("logs", log_index, either("log.level", ERROR_LEVELS))]
        if trace_index:
            kinds.append(("traces", trace_index, either("status.code", ERROR_STATUS)))
        raw = self._request("POST", f"/api/datasources/proxy/uid/{datasource_uid}/_msearch",
                            content="".join(search(index, extra) for _k, index, extra in kinds),
                            headers={"Content-Type": "application/x-ndjson"})
        responses = raw.get("responses") if isinstance(raw, dict) else None
        if not isinstance(responses, list) or len(responses) != len(kinds):
            raise GrafanaError("ES _msearch 的回應筆數跟請求對不上")
        out: dict[str, list[dict[str, Any]]] = {"logs": [], "traces": []}
        for (kind, index, _extra), resp in zip(kinds, responses):
            if resp.get("error"):
                raise GrafanaError(f"ES 查詢 {index} 失敗：{json.dumps(resp['error'], ensure_ascii=False)[:300]}")
            out[kind] = [h.get("_source") or {} for h in (resp.get("hits") or {}).get("hits") or []]
        return out

    # ── 註記 ──

    def upsert_annotation(self, annotation_id: int | None, uid_tag: str, body: dict[str, Any]) -> int:
        """建立或更新一筆註記，回傳它的 id。沒有 id 時先用事故標籤找（上一輪建立成功、但 id 沒存到的情況不會重複建）。"""
        if annotation_id is None:
            found = self._request("GET", "/api/annotations", params={"tags": uid_tag, "limit": 1})
            if isinstance(found, list) and found:
                annotation_id = int(found[0]["id"])
        if annotation_id is None:
            return int(self._request("POST", "/api/annotations", json=body)["id"])
        self._request("PATCH", f"/api/annotations/{annotation_id}", json=body)
        return annotation_id


def parse_frames(raw: Any, rule: Rule, from_ms: int, to_ms: int, step_ms: int) -> pd.DataFrame:
    """把 /api/ds/query 的回應轉成 (ts 毫秒, value)。只接受「一個時間欄 + 一個數值欄」的單一序列。"""
    who = f"規則「{rule.title}」({rule.uid})"
    result = ((raw or {}).get("results") or {}).get("A") if isinstance(raw, dict) else None
    if result is None:
        raise GrafanaError(f"{who} 的查詢回應沒有 results.A：{str(raw)[:200]}")
    if result.get("error"):
        raise QueryError(f"{who} 的查詢被資料源拒絕：{result['error']}")
    frames = [f for f in result.get("frames") or [] if (f.get("schema") or {}).get("fields")]
    frames = [f for f in frames if any(len(col) for col in (f.get("data") or {}).get("values") or [])]
    if not frames:
        return pd.DataFrame({"ts": pd.Series([], dtype="int64"), "value": pd.Series([], dtype=float)})
    if len(frames) > 1:
        raise QueryError(f"{who} 的查詢回了 {len(frames)} 條序列：一條規則只能對應一條序列，請在查詢裡聚合"
                         f"（PromQL 用 sum／max，SQL 用 GROUP BY 時間）")
    fields = frames[0]["schema"]["fields"]
    values = frames[0]["data"]["values"]
    by_type: dict[str, list[int]] = {}
    for i, f in enumerate(fields):
        by_type.setdefault(str(f.get("type")), []).append(i)
    others = [fields[i]["name"] for t, idx in by_type.items() if t not in ("time", "number") for i in idx]
    if others:
        raise QueryError(f"{who} 的查詢結果有非數值欄 {others}（長格式）：平台分不出哪一列是哪個實體，"
                         f"請改成「時間 + 單一數值」並在 SQL 裡聚合或篩到一個實體")
    if len(by_type.get("time", [])) != 1:
        raise QueryError(f"{who} 的查詢結果要有剛好一個時間欄，現在是 {len(by_type.get('time', []))} 個")
    numbers = by_type.get("number", [])
    if len(numbers) != 1:
        raise QueryError(f"{who} 的查詢結果有 {len(numbers)} 個數值欄 {[fields[i]['name'] for i in numbers]}："
                         f"只能有一個（不會默默取第一欄），其他欄請拆成另一條規則")
    df = pd.DataFrame({"ts": values[by_type["time"][0]], "value": values[numbers[0]]}).dropna()
    if df.empty:
        return df.astype({"ts": "int64", "value": float})
    df = df.astype({"ts": "int64", "value": float})
    low, high = int(df["ts"].min()), int(df["ts"].max())
    if low < from_ms - step_ms or high > to_ms + step_ms:
        iso = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
        raise QueryError(f"{who} 的查詢回了請求視窗以外的資料（{iso(low)} ～ {iso(high)}，"
                         f"請求的是 {iso(from_ms)} ～ {iso(to_ms)}）：SQL 請加上 $__timeFilter(時間欄)")
    return df
