import math
import os
from datetime import datetime
from typing import Any

import httpx
import pandas as pd

from aiops.sources import _http
from aiops.sources.base import (BaseDataSourceSpec, InvalidQueryError, NoDataError, as_utc_datetime,
                                canonical_service)
from aiops.sources.query.elasticsearch import bucket_value, build_search_body, parse_es_query
from aiops.sources.query.window import align_window


class ElasticsearchSpec(BaseDataSourceSpec):
    """Elasticsearch：date_histogram 降採樣成時序。query 是 JSON（格式見 sources/query/elasticsearch.py）。

    環境變數：AIOPS_ES_URL（必填）、AIOPS_ES_API_KEY（選填，送 Authorization: ApiKey <key>）。
    只讀：只呼叫 _search 與根路徑，不寫任何索引。
    """

    source_type = "elasticsearch"

    def __init__(self, base_url: str, api_key: str | None = None, client: httpx.Client | None = None,
                 timeout: float = 15.0):
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"ApiKey {api_key}"} if api_key else {}
        self.client = client or httpx.Client(timeout=timeout)

    @classmethod
    def from_env(cls) -> "ElasticsearchSpec":
        return cls(_http.env_required("AIOPS_ES_URL"), os.environ.get("AIOPS_ES_API_KEY") or None)

    @classmethod
    def configured(cls) -> bool:
        return bool(os.environ.get("AIOPS_ES_URL", "").strip())

    def fetch_data(self, query: str, start: datetime, end: datetime, step_seconds: int,
                   field_map: dict[str, str] | None = None) -> pd.DataFrame:
        try:
            q = parse_es_query(query, field_map)
        except ValueError as e:
            raise InvalidQueryError(str(e)) from e
        s, e = align_window(start, end, step_seconds)
        resp = _http.request(
            self.client, self.source_type, "POST", f"{self.base_url}/{q.index}/_search", headers=self.headers,
            json=build_search_body(q, s, e, step_seconds),
        )
        payload = resp.json() if resp.content else {}
        if not resp.is_success:
            reason = (payload.get("error") or {}).get("reason") if isinstance(payload.get("error"), dict) else None
            raise InvalidQueryError(f"elasticsearch 查詢失敗（HTTP {resp.status_code}）：{reason or resp.text[:200]}")

        buckets = (payload.get("aggregations") or {}).get("series", {}).get("buckets", [])
        points = [(b["key"], bucket_value(q, b)) for b in buckets]
        points = [(t, v) for t, v in points if v is not None]
        if not points:
            raise NoDataError(f"elasticsearch 在這段時間沒有資料：index={q.index}")
        return pd.DataFrame({
            "ts": pd.to_datetime([t for t, _ in points], unit="ms", utc=True),
            "value": [v for _, v in points],
        })

    def fetch_spans(self, start: datetime, end: datetime, services: list[str] | None = None,
                    max_spans: int = 20000) -> tuple[list[dict[str, Any]], bool]:
        """APM 資料模型（processor.event = transaction／span）：transaction 的 id 在 transaction.id，span 的在 span.id；
        兩者都用 parent.id 指向父節點，跨服務呼叫時下游 transaction 的 parent.id 就是上游 span 的 id。
        只呼叫 _search，不寫任何索引。索引用 AIOPS_ES_TRACE_INDEX 改。

        區間內 span 總數 <= max_spans：用 search_after 全部撈完（完整，不抽樣）。
        超過：不能只拿時間最早的 max_spans 筆（尖峰時那只是窗口開頭幾秒，拓撲會偏），改成 _sampled_spans：
        把窗口切成時間桶，每桶隨機挑 trace，輪流把「整條 trace」的 span 撈回來直到湊滿 max_spans。
        以 trace 為單位抽樣，父子才接得起來（抽 span 會把跨服務的邊拆斷）。回傳的 truncated 為 True。"""
        index = os.environ.get("AIOPS_ES_TRACE_INDEX", "").strip() or DEFAULT_TRACE_INDEX
        filters = _window_filters(start, end, services)
        spans: list[dict[str, Any]] = []
        after = None
        first = True
        while len(spans) < max_spans:
            body: dict[str, Any] = {
                "size": min(5000, max_spans - len(spans)), "query": {"bool": {"filter": filters}},
                "_source": SPAN_FIELDS, "sort": SPAN_SORT,
            }
            if first:
                body["track_total_hits"] = max_spans + 1  # 只要知道「有沒有超過上限」，不必數完全部
            if after is not None:
                body["search_after"] = after
            payload = self._search(index, body, "撈 trace")
            total = ((payload.get("hits") or {}).get("total") or {}).get("value")
            if first and total is not None and total > max_spans:
                return self._sampled_spans(index, start, end, services, max_spans), True
            first = False
            hits = payload.get("hits", {}).get("hits", [])
            if not hits:
                return spans, False
            spans += [s for s in (_span(h["_source"]) for h in hits) if s is not None]
            after = hits[-1]["sort"]
        return spans[:max_spans], True

    def _search(self, index: str, body: dict[str, Any], what: str) -> dict[str, Any]:
        resp = _http.request(self.client, self.source_type, "POST", f"{self.base_url}/{index}/_search",
                             headers=self.headers, json=body)
        payload = resp.json() if resp.content else {}
        if not resp.is_success:
            reason = (payload.get("error") or {}).get("reason") if isinstance(payload.get("error"), dict) else None
            raise InvalidQueryError(f"elasticsearch {what}失敗（HTTP {resp.status_code}）：{reason or resp.text[:200]}")
        return payload

    def _sampled_spans(self, index: str, start: datetime, end: datetime, services: list[str] | None,
                       max_spans: int) -> list[dict[str, Any]]:
        """時間分桶 + 以 trace 為單位的均勻抽樣。

        1. 窗口切成 min(分鐘數, SAMPLE_MAX_BUCKETS) 個桶；每桶用固定 seed 的 random_score 挑一批候選文件，只拿 trace.id
           （固定 seed：同一個事件重跑報告，抽到的 trace 一樣）。
        2. 一輪從每個桶各拿 SAMPLE_TRACES_PER_ROUND 條還沒撈過的 trace，用 terms trace.id 把它們的 span 全撈回來；
           整條放得下才收（不收半條），湊滿 max_spans 或候選用完就停。各桶輪流拿，所以每個時段的 trace 數差不多。
        """
        buckets = max(1, min(SAMPLE_MAX_BUCKETS, math.ceil((end - start).total_seconds() / 60)))
        step = (end - start) / buckets
        candidates_per_bucket = min(5000, max(SAMPLE_TRACES_PER_ROUND * 4, max_spans // buckets))
        queues: list[list[str]] = []
        seen: set[str] = set()
        for b in range(buckets):
            lo, hi = start + step * b, (start + step * (b + 1) if b < buckets - 1 else end)
            body = {"size": candidates_per_bucket, "_source": ["trace.id"],
                    "query": {"function_score": {"query": {"bool": {"filter": _window_filters(lo, hi, services, b == buckets - 1)}},
                                                 "random_score": {"seed": SAMPLE_SEED, "field": "_seq_no"},
                                                 "boost_mode": "replace"}}}
            ids = []
            for h in self._search(index, body, "抽樣 trace").get("hits", {}).get("hits", []):
                tid = ((h.get("_source") or {}).get("trace") or {}).get("id")
                if tid and tid not in seen:
                    seen.add(tid)
                    ids.append(tid)
            queues.append(ids)

        spans: list[dict[str, Any]] = []
        window = _window_filters(start, end, services)
        while len(spans) < max_spans and any(queues):
            # 交錯排（桶0第1條、桶1第1條…桶0第2條…），最後一輪放不下時各桶少的一樣多，不會偏向前面的桶
            batch = [q[i] for i in range(SAMPLE_TRACES_PER_ROUND) for q in queues if i < len(q)]
            queues = [q[SAMPLE_TRACES_PER_ROUND:] for q in queues]
            by_trace: dict[str, list[dict[str, Any]]] = {}
            after = None
            while True:
                body = {"size": 5000, "_source": SPAN_FIELDS, "sort": SPAN_SORT,
                        "query": {"bool": {"filter": [*window, {"terms": {"trace.id": batch}}]}}}
                if after is not None:
                    body["search_after"] = after
                hits = self._search(index, body, "撈抽樣 trace").get("hits", {}).get("hits", [])
                if not hits:
                    break
                for s in (_span(h["_source"]) for h in hits):
                    if s is not None:
                        by_trace.setdefault(s["trace_id"], []).append(s)
                after = hits[-1]["sort"]
            for tid in batch:  # 依抽樣順序收整條 trace；放不下的跳過，換下一條比較短的
                trace = by_trace.get(tid, [])
                if trace and len(spans) + len(trace) <= max_spans:
                    spans += trace
        return sorted(spans, key=lambda s: s["ts"])

    def fetch_logs(self, start: datetime, end: datetime, services: list[str] | None = None,
                   max_logs: int = 5000) -> tuple[list[dict[str, Any]], bool]:
        """APM 錯誤日誌（processor.event = error）。訊息依序取：error.exception（陣列或單一物件都可）的 message、
        error.log.message、頂層 message。索引用 AIOPS_ES_LOG_INDEX 改。只呼叫 _search。

        區間內筆數 <= max_logs：全部撈（每筆 weight=1）。超過：不再只拿時間最早的 max_logs 筆（事故後段的暴增會整段被丟掉），
        改成把區間切成時間桶，每桶撈固定筆數，每筆帶 weight = 該桶總筆數 ÷ 撈到的筆數，暴增倍率用加權後的筆數算。
        回傳的 truncated 為 True。"""
        index = os.environ.get("AIOPS_ES_LOG_INDEX", "").strip() or DEFAULT_LOG_INDEX
        filters = _window_filters(start, end, services)
        logs: list[dict[str, Any]] = []
        after = None
        first = True
        while len(logs) < max_logs:
            body: dict[str, Any] = {
                "size": min(5000, max_logs - len(logs)), "query": {"bool": {"filter": filters}},
                "_source": LOG_FIELDS, "sort": LOG_SORT,
            }
            if first:
                body["track_total_hits"] = max_logs + 1
            if after is not None:
                body["search_after"] = after
            payload = self._search(index, body, "撈日誌")
            total = ((payload.get("hits") or {}).get("total") or {}).get("value")
            if first and total is not None and total > max_logs:
                return self._sampled_logs(index, start, end, services, max_logs), True
            first = False
            hits = payload.get("hits", {}).get("hits", [])
            if not hits:
                return logs, False
            logs += [r for r in (_log(h["_source"]) for h in hits) if r is not None]
            after = hits[-1]["sort"]
        return logs[:max_logs], True

    def _sampled_logs(self, index: str, start: datetime, end: datetime, services: list[str] | None,
                      max_logs: int) -> list[dict[str, Any]]:
        buckets = max(1, min(SAMPLE_MAX_BUCKETS, math.ceil((end - start).total_seconds() / 60)))
        step = (end - start) / buckets
        quota = max(1, min(5000, max_logs // buckets))
        out: list[dict[str, Any]] = []
        for b in range(buckets):
            lo, hi = start + step * b, (start + step * (b + 1) if b < buckets - 1 else end)
            body = {"size": quota, "track_total_hits": True, "_source": LOG_FIELDS, "sort": LOG_SORT,
                    "query": {"bool": {"filter": _window_filters(lo, hi, services, b == buckets - 1)}}}
            payload = self._search(index, body, "抽樣日誌")
            hits = payload.get("hits", {}).get("hits", [])
            rows = [r for r in (_log(h["_source"]) for h in hits) if r is not None]
            total = ((payload.get("hits") or {}).get("total") or {}).get("value") or len(rows)
            weight = total / len(rows) if rows else 1.0
            out += [{**r, "weight": weight} for r in rows]
        return out

    def check(self) -> dict[str, Any]:
        return _http.check(self.client, self.source_type, f"{self.base_url}/", headers=self.headers)


DEFAULT_TRACE_INDEX = "traces-apm-*"
DEFAULT_LOG_INDEX = "logs-apm.*"
SPAN_FIELDS = ["@timestamp", "trace.id", "span.id", "transaction.id", "parent.id", "service.name",
               "span.duration.us", "transaction.duration.us", "event.outcome"]
SPAN_SORT = [{"@timestamp": "asc"}, {"trace.id": "asc"}, {"span.id": {"order": "asc", "missing": "_first"}}]
SAMPLE_MAX_BUCKETS = 30
LOG_FIELDS = ["@timestamp", "service.name", "trace.id", "message", "error.exception", "error.log", "error.culprit"]
# 分頁要穩定的排序：時間＋每筆唯一的 error.id（不用 _doc，跨分片沒有 PIT 時不保證順序）
LOG_SORT = [{"@timestamp": "asc"}, {"error.id": {"order": "asc", "missing": "_last"}}]
SAMPLE_TRACES_PER_ROUND = 20
SAMPLE_SEED = 20261103


def _window_filters(start: datetime, end: datetime, services: list[str] | None,
                    end_inclusive: bool = True) -> list[dict[str, Any]]:
    """時間區間＋服務過濾。分桶抽樣時，除了最後一桶都要 end_inclusive=False（含頭不含尾），
    不然剛好落在桶邊界的文件會被相鄰兩桶各算一次，日誌權重就會偏高。"""
    upper = "lte" if end_inclusive else "lt"
    filters: list[dict[str, Any]] = [{"range": {"@timestamp": {"gte": start.isoformat(), upper: end.isoformat()}}}]
    if services:
        # 大小寫不分（case_insensitive，ES 7.10+）：事件實體叫 cart-api、索引裡寫 Cart-API 一樣要撈得到
        names = sorted({canonical_service(s) for s in services})
        filters.append({"bool": {"should": [{"term": {"service.name": {"value": n, "case_insensitive": True}}}
                                            for n in names], "minimum_should_match": 1}})
    return filters


def _first_exception(exc: Any) -> dict[str, Any]:
    """error.exception 可能是陣列（APM 標準）、單一物件、字串、或陣列裡放字串；一律整理成 {message, type}。"""
    first = exc[0] if isinstance(exc, list) and exc else exc
    if isinstance(first, dict):
        return first
    return {"message": first} if isinstance(first, str) and first.strip() else {}


def _log(src: dict[str, Any]) -> dict[str, Any] | None:
    service = (src.get("service") or {}).get("name")
    if not service or "@timestamp" not in src:
        return None
    error = src.get("error") or {}
    first = _first_exception(error.get("exception"))
    message = first.get("message") or (error.get("log") or {}).get("message") or src.get("message") or ""
    if not message:
        return None
    return {"ts": as_utc_datetime(src["@timestamp"]), "service": canonical_service(service),
            "message": str(message), "error_type": str(first.get("type") or ""),
            "trace_id": str((src.get("trace") or {}).get("id") or "")}


def _span(src: dict[str, Any]) -> dict[str, Any] | None:
    own = src.get("span") or src.get("transaction") or {}
    span_id = (src.get("span") or {}).get("id") or (src.get("transaction") or {}).get("id")
    service = (src.get("service") or {}).get("name")
    trace_id = (src.get("trace") or {}).get("id")
    if not (span_id and service and trace_id):
        return None
    return {"trace_id": trace_id, "span_id": span_id, "parent_id": (src.get("parent") or {}).get("id"),
            "service": canonical_service(service), "ts": as_utc_datetime(src["@timestamp"]),
            "duration_ms": float((own.get("duration") or {}).get("us") or 0) / 1000.0,
            "error": (src.get("event") or {}).get("outcome") == "failure"}
