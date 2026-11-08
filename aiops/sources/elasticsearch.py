import os
from datetime import datetime
from typing import Any

import httpx
import pandas as pd

from aiops.sources import _http
from aiops.sources.base import BaseDataSourceSpec, InvalidQueryError, NoDataError
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
        用 search_after 分頁撈到 max_spans 為止（只呼叫 _search，不寫任何索引）。索引用 AIOPS_ES_TRACE_INDEX 改。"""
        index = os.environ.get("AIOPS_ES_TRACE_INDEX", "").strip() or DEFAULT_TRACE_INDEX
        filters: list[dict[str, Any]] = [{"range": {"@timestamp": {"gte": start.isoformat(), "lte": end.isoformat()}}}]
        if services:
            filters.append({"terms": {"service.name": list(services)}})
        spans: list[dict[str, Any]] = []
        after = None
        while len(spans) < max_spans:
            body: dict[str, Any] = {
                "size": min(5000, max_spans - len(spans)), "query": {"bool": {"filter": filters}},
                "_source": ["@timestamp", "trace.id", "span.id", "transaction.id", "parent.id", "service.name",
                            "span.duration.us", "transaction.duration.us", "event.outcome"],
                "sort": [{"@timestamp": "asc"}, {"trace.id": "asc"}, {"span.id": {"order": "asc", "missing": "_first"}}],
            }
            if after is not None:
                body["search_after"] = after
            resp = _http.request(self.client, self.source_type, "POST", f"{self.base_url}/{index}/_search",
                                 headers=self.headers, json=body)
            payload = resp.json() if resp.content else {}
            if not resp.is_success:
                reason = (payload.get("error") or {}).get("reason") if isinstance(payload.get("error"), dict) else None
                raise InvalidQueryError(f"elasticsearch 撈 trace 失敗（HTTP {resp.status_code}）：{reason or resp.text[:200]}")
            hits = payload.get("hits", {}).get("hits", [])
            if not hits:
                return spans, False
            spans += [s for s in (_span(h["_source"]) for h in hits) if s is not None]
            after = hits[-1]["sort"]
        return spans[:max_spans], True

    def fetch_logs(self, start: datetime, end: datetime, services: list[str] | None = None,
                   max_logs: int = 5000) -> tuple[list[dict[str, Any]], bool]:
        """APM 錯誤日誌（processor.event = error）。訊息依序取：error.exception（陣列或單一物件都可）的 message、
        error.log.message、頂層 message。索引用 AIOPS_ES_LOG_INDEX 改。只呼叫 _search。"""
        index = os.environ.get("AIOPS_ES_LOG_INDEX", "").strip() or DEFAULT_LOG_INDEX
        filters: list[dict[str, Any]] = [{"range": {"@timestamp": {"gte": start.isoformat(), "lte": end.isoformat()}}}]
        if services:
            filters.append({"terms": {"service.name": list(services)}})
        logs: list[dict[str, Any]] = []
        after = None
        while len(logs) < max_logs:
            body: dict[str, Any] = {
                "size": min(5000, max_logs - len(logs)), "query": {"bool": {"filter": filters}},
                "_source": ["@timestamp", "service.name", "trace.id", "message", "error.exception", "error.log", "error.culprit"],
                # 分頁要穩定的排序：時間＋每筆唯一的 error.id（不用 _doc，跨分片沒有 PIT 時不保證順序）
                "sort": [{"@timestamp": "asc"}, {"error.id": {"order": "asc", "missing": "_last"}}],
            }
            if after is not None:
                body["search_after"] = after
            resp = _http.request(self.client, self.source_type, "POST", f"{self.base_url}/{index}/_search",
                                 headers=self.headers, json=body)
            payload = resp.json() if resp.content else {}
            if not resp.is_success:
                reason = (payload.get("error") or {}).get("reason") if isinstance(payload.get("error"), dict) else None
                raise InvalidQueryError(f"elasticsearch 撈日誌失敗（HTTP {resp.status_code}）：{reason or resp.text[:200]}")
            hits = payload.get("hits", {}).get("hits", [])
            if not hits:
                return logs, False
            logs += [r for r in (_log(h["_source"]) for h in hits) if r is not None]
            after = hits[-1]["sort"]
        return logs[:max_logs], True

    def check(self) -> dict[str, Any]:
        return _http.check(self.client, self.source_type, f"{self.base_url}/", headers=self.headers)


DEFAULT_TRACE_INDEX = "traces-apm-*"
DEFAULT_LOG_INDEX = "logs-apm.*"


def _log(src: dict[str, Any]) -> dict[str, Any] | None:
    service = (src.get("service") or {}).get("name")
    if not service or "@timestamp" not in src:
        return None
    error = src.get("error") or {}
    exc = error.get("exception")
    first = (exc[0] if exc else {}) if isinstance(exc, list) else (exc or {})
    message = first.get("message") or (error.get("log") or {}).get("message") or src.get("message") or ""
    if not message:
        return None
    return {"ts": pd.Timestamp(src["@timestamp"]).tz_convert("UTC").to_pydatetime(), "service": service,
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
            "service": service, "ts": pd.Timestamp(src["@timestamp"]).tz_convert("UTC").to_pydatetime(),
            "duration_ms": float((own.get("duration") or {}).get("us") or 0) / 1000.0,
            "error": (src.get("event") or {}).get("outcome") == "failure"}
