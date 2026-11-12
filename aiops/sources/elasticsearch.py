import math
import os
from datetime import datetime
from typing import Any, Callable

import httpx
import pandas as pd

from aiops.sources import _http
from aiops.sources.base import BaseDataSourceSpec, InvalidQueryError, NoDataError, canonical_service
from aiops.sources.es_models import ELASTIC_APM, Resolved, current_model, get_path
from aiops.sources.es_models import _links as _apm_links
from aiops.sources.query.elasticsearch import bucket_value, build_search_body, parse_es_query
from aiops.sources.query.window import align_window


class ElasticsearchSpec(BaseDataSourceSpec):
    """Elasticsearch：date_histogram 降採樣成時序。query 是 JSON（格式見 sources/query/elasticsearch.py）。

    環境變數：AIOPS_ES_URL（必填）、AIOPS_ES_API_KEY（選填，送 Authorization: ApiKey <key>）、
    AIOPS_ES_DATA_MODEL（trace／日誌的資料模型：elastic_apm〔預設〕／otel，見 sources/es_models.py）。
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
        try:
            points = [(b["key"], bucket_value(q, b)) for b in buckets]
        except (KeyError, TypeError, ValueError) as e:
            # 回 200 但內容不是數值序列（value_field 指到字串欄位、桶沒有 key）：是查詢要改，不是整輪該掛
            raise InvalidQueryError(f"elasticsearch 回應格式不對（index={q.index}, value_field={q.value_field}）：{e!r}") from e
        points = [(t, v) for t, v in points if v is not None and math.isfinite(v)]  # NaN／Inf 跟沒資料一樣，不能學成基準線
        if not points:
            raise NoDataError(f"elasticsearch 在這段時間沒有資料（或只有 NaN／Inf）：index={q.index}")
        return pd.DataFrame({
            "ts": pd.to_datetime([t for t, _ in points], unit="ms", utc=True),
            "value": [v for _, v in points],
        })

    def fetch_spans(self, start: datetime, end: datetime, services: list[str] | None = None,
                    max_spans: int = 20000, field_map: dict[str, str] | None = None) -> tuple[list[dict[str, Any]], bool]:
        """[start, end] 內的 span。欄位與索引依資料模型（AIOPS_ES_DATA_MODEL，見 sources/es_models.py）：
        elastic_apm（預設）的 transaction／span 文件、或 OTel Collector（otel mapping）的 span 文件。
        field_map 是已核准的欄位對照，標準欄 span.* 的那幾條會蓋過模型的預設欄位。只呼叫 _search，不寫任何索引。

        區間內 span 總數 <= max_spans：用 search_after 全部撈完（完整，不抽樣）。
        超過：不能只拿時間最早的 max_spans 筆（尖峰時那只是窗口開頭幾秒，拓撲會偏），改成 _sampled_spans：
        把窗口切成時間桶，每桶隨機挑 trace，輪流把「整條 trace」的 span 撈回來直到湊滿 max_spans。
        以 trace 為單位抽樣，父子才接得起來（抽 span 會把跨服務的邊拆斷）。回傳的 truncated 為 True。"""
        r = current_model().resolve(field_map)
        index = _trace_index(r)
        rows, over = self._paginate(index, _window_filters(start, end, services, r.query_field("span.service")),
                                    r.span_source, r.span_sort, r.span, max_spans, "撈 trace")
        if over:
            return self._sampled_spans(r, index, start, end, services, max_spans), True
        return rows[:max_spans], over is None

    def fetch_linked_spans(self, trace_ids: list[str], start: datetime, end: datetime,
                           max_spans: int = 20000, field_map: dict[str, str] | None = None) -> tuple[list[dict[str, Any]], bool]:
        """訊息佇列跨時窗：把指定 trace 在 [start, end] 內的 span 全撈回來（消費者 parent／span.links 指到的生產者那一段）。
        不帶服務過濾：生產者是誰要撈回來才知道。trace ID 每 500 個一批、每批 search_after 分頁，湊滿 max_spans 就停。"""
        r = current_model().resolve(field_map)
        index = _trace_index(r)
        ids = sorted({t for t in trace_ids if t})
        window = _window_filters(start, end, None)
        spans: list[dict[str, Any]] = []
        for i in range(0, len(ids), LINKED_TRACES_PER_QUERY):
            batch = ids[i:i + LINKED_TRACES_PER_QUERY]
            after = None
            while True:
                if len(spans) >= max_spans:
                    return spans[:max_spans], True
                body: dict[str, Any] = {"size": min(5000, max_spans - len(spans)), "_source": r.span_source,
                                        "sort": r.span_sort,
                                        "query": {"bool": {"filter": [*window, {"terms": {r.query_field("span.trace_id"): batch}}]}}}
                if after is not None:
                    body["search_after"] = after
                hits = self._search(index, body, "撈訊息生產者 trace").get("hits", {}).get("hits", [])
                if not hits:
                    break
                spans += [s for s in (r.span(h["_source"]) for h in hits) if s is not None]
                after = hits[-1]["sort"]
        return spans, False

    def _paginate(self, index: str, filters: list[dict[str, Any]], source: list[str], sort: list[dict[str, Any]],
                  parse: Callable[[dict[str, Any]], dict[str, Any] | None], limit: int,
                  what: str) -> tuple[list[dict[str, Any]], bool | None]:
        """search_after 分頁撈到 limit 筆（fetch_spans 與 fetch_logs 共用）。回傳 (rows, over)：
        over=True：第一頁就知道總數超過 limit（呼叫端改走分桶抽樣，rows 是空的）；
        over=False：撈完了（沒有下一頁）；over=None：撈滿 limit 筆就停了（當成被截斷）。"""
        rows: list[dict[str, Any]] = []
        after = None
        first = True
        while len(rows) < limit:
            body: dict[str, Any] = {"size": min(5000, limit - len(rows)), "query": {"bool": {"filter": filters}},
                                    "_source": source, "sort": sort}
            if first:
                body["track_total_hits"] = limit + 1  # 只要知道「有沒有超過上限」，不必數完全部
            if after is not None:
                body["search_after"] = after
            payload = self._search(index, body, what)
            total = ((payload.get("hits") or {}).get("total") or {}).get("value")
            if first and total is not None and total > limit:
                return [], True
            first = False
            hits = payload.get("hits", {}).get("hits", [])
            if not hits:
                return rows, False
            rows += [x for x in (parse(h["_source"]) for h in hits) if x is not None]
            after = hits[-1]["sort"]
        return rows[:limit], None

    def _search(self, index: str, body: dict[str, Any], what: str) -> dict[str, Any]:
        resp = _http.request(self.client, self.source_type, "POST", f"{self.base_url}/{index}/_search",
                             headers=self.headers, json=body)
        payload = resp.json() if resp.content else {}
        if not resp.is_success:
            reason = (payload.get("error") or {}).get("reason") if isinstance(payload.get("error"), dict) else None
            raise InvalidQueryError(f"elasticsearch {what}失敗（HTTP {resp.status_code}）：{reason or resp.text[:200]}")
        return payload

    def _sampled_spans(self, r: Resolved, index: str, start: datetime, end: datetime, services: list[str] | None,
                       max_spans: int) -> list[dict[str, Any]]:
        """時間分桶 + 以 trace 為單位的均勻抽樣。

        1. 窗口切成 min(分鐘數, SAMPLE_MAX_BUCKETS) 個桶；每桶用固定 seed 的 random_score 挑一批候選文件，只拿 trace ID
           （固定 seed：同一個事件重跑報告，抽到的 trace 一樣）。
        2. 一輪從每個桶各拿 SAMPLE_TRACES_PER_ROUND 條還沒撈過的 trace，用 terms trace ID 把它們的 span 全撈回來；
           整條放得下才收（不收半條），湊滿 max_spans 或候選用完就停。各桶輪流拿，所以每個時段的 trace 數差不多。
        """
        trace_field, service_field = r.query_field("span.trace_id"), r.query_field("span.service")
        buckets = max(1, min(SAMPLE_MAX_BUCKETS, math.ceil((end - start).total_seconds() / 60)))
        step = (end - start) / buckets
        candidates_per_bucket = min(5000, max(SAMPLE_TRACES_PER_ROUND * 4, max_spans // buckets))
        queues: list[list[str]] = []
        seen: set[str] = set()
        for b in range(buckets):
            lo, hi = start + step * b, (start + step * (b + 1) if b < buckets - 1 else end)
            body = {"size": candidates_per_bucket, "_source": [trace_field],
                    "query": {"function_score": {"query": {"bool": {"filter": _window_filters(
                                                     lo, hi, services, service_field, b == buckets - 1)}},
                                                 "random_score": {"seed": SAMPLE_SEED, "field": "_seq_no"},
                                                 "boost_mode": "replace"}}}
            ids = []
            for h in self._search(index, body, "抽樣 trace").get("hits", {}).get("hits", []):
                tid = get_path(h.get("_source") or {}, trace_field)
                if tid and tid not in seen:
                    seen.add(tid)
                    ids.append(tid)
            queues.append(ids)

        spans: list[dict[str, Any]] = []
        window = _window_filters(start, end, services, service_field)
        while len(spans) < max_spans and any(queues):
            # 交錯排（桶0第1條、桶1第1條…桶0第2條…），最後一輪放不下時各桶少的一樣多，不會偏向前面的桶
            batch = [q[i] for i in range(SAMPLE_TRACES_PER_ROUND) for q in queues if i < len(q)]
            queues = [q[SAMPLE_TRACES_PER_ROUND:] for q in queues]
            by_trace: dict[str, list[dict[str, Any]]] = {}
            after = None
            while True:
                body = {"size": 5000, "_source": r.span_source, "sort": r.span_sort,
                        "query": {"bool": {"filter": [*window, {"terms": {trace_field: batch}}]}}}
                if after is not None:
                    body["search_after"] = after
                hits = self._search(index, body, "撈抽樣 trace").get("hits", {}).get("hits", [])
                if not hits:
                    break
                for s in (r.span(h["_source"]) for h in hits):
                    if s is not None:
                        by_trace.setdefault(s["trace_id"], []).append(s)
                after = hits[-1]["sort"]
            for tid in batch:  # 依抽樣順序收整條 trace；放不下的跳過，換下一條比較短的
                trace = by_trace.get(tid, [])
                if trace and len(spans) + len(trace) <= max_spans:
                    spans += trace
        return sorted(spans, key=lambda s: s["ts"])

    def fetch_logs(self, start: datetime, end: datetime, services: list[str] | None = None,
                   max_logs: int = 5000, field_map: dict[str, str] | None = None) -> tuple[list[dict[str, Any]], bool]:
        """錯誤日誌。elastic_apm：APM 錯誤文件（processor.event = error），訊息依序取 error.exception（陣列或單一物件都可）
        的 message、error.log.message、頂層 message。otel：OTel 日誌資料流裡 ERROR 以上（severity_number >= 17）與
        span 的 exception 事件，訊息取 body.text › exception.message › 結構化 body。索引用 AIOPS_ES_LOG_INDEX 改。只呼叫 _search。

        區間內筆數 <= max_logs：全部撈（每筆 weight=1）。超過：不再只拿時間最早的 max_logs 筆（事故後段的暴增會整段被丟掉），
        改成把區間切成時間桶，每桶撈固定筆數，每筆帶 weight = 該桶總筆數 ÷ 撈到的筆數，暴增倍率用加權後的筆數算。
        回傳的 truncated 為 True。"""
        r = current_model().resolve(field_map)
        index = _log_index(r)
        filters = [*_window_filters(start, end, services, r.query_field("log.service")), *r.model.log_filters]
        rows, over = self._paginate(index, filters, r.log_source, r.model.log_sort, r.log, max_logs, "撈日誌")
        if over:
            return self._sampled_logs(r, index, start, end, services, max_logs), True
        return rows, over is None

    def _sampled_logs(self, r: Resolved, index: str, start: datetime, end: datetime, services: list[str] | None,
                      max_logs: int) -> list[dict[str, Any]]:
        buckets = max(1, min(SAMPLE_MAX_BUCKETS, math.ceil((end - start).total_seconds() / 60)))
        step = (end - start) / buckets
        quota = max(1, min(5000, max_logs // buckets))
        out: list[dict[str, Any]] = []
        for b in range(buckets):
            lo, hi = start + step * b, (start + step * (b + 1) if b < buckets - 1 else end)
            filters = [*_window_filters(lo, hi, services, r.query_field("log.service"), b == buckets - 1),
                       *r.model.log_filters]
            body = {"size": quota, "track_total_hits": True, "_source": r.log_source, "sort": r.model.log_sort,
                    "query": {"bool": {"filter": filters}}}
            payload = self._search(index, body, "抽樣日誌")
            hits = payload.get("hits", {}).get("hits", [])
            rows = [x for x in (r.log(h["_source"]) for h in hits) if x is not None]
            total = ((payload.get("hits") or {}).get("total") or {}).get("value") or len(rows)
            weight = total / len(rows) if rows else 1.0
            out += [{**x, "weight": weight} for x in rows]
        return out

    def check(self) -> dict[str, Any]:
        return _http.check(self.client, self.source_type, f"{self.base_url}/", headers=self.headers)


SAMPLE_MAX_BUCKETS = 30
LINKED_TRACES_PER_QUERY = 500
SAMPLE_TRACES_PER_ROUND = 20
SAMPLE_SEED = 20261103


def _trace_index(r: Resolved) -> str:
    return os.environ.get("AIOPS_ES_TRACE_INDEX", "").strip() or r.model.trace_index


def _log_index(r: Resolved) -> str:
    return os.environ.get("AIOPS_ES_LOG_INDEX", "").strip() or r.model.log_index


def _window_filters(start: datetime, end: datetime, services: list[str] | None, service_field: str = "service.name",
                    end_inclusive: bool = True) -> list[dict[str, Any]]:
    """時間區間＋服務過濾。分桶抽樣時，除了最後一桶都要 end_inclusive=False（含頭不含尾），
    不然剛好落在桶邊界的文件會被相鄰兩桶各算一次，日誌權重就會偏高。"""
    upper = "lte" if end_inclusive else "lt"
    filters: list[dict[str, Any]] = [{"range": {"@timestamp": {"gte": start.isoformat(), upper: end.isoformat()}}}]
    if services:
        # 大小寫不分（case_insensitive，ES 7.10+）：事件實體叫 cart-api、索引裡寫 Cart-API 一樣要撈得到
        names = sorted({canonical_service(s) for s in services})
        filters.append({"bool": {"should": [{"term": {service_field: {"value": n, "case_insensitive": True}}}
                                            for n in names], "minimum_should_match": 1}})
    return filters


# 相容舊介面（測試與合成資料驗證直接拿來解析單筆 APM 文件）：一律走 Elastic APM 模型、不套欄位對照
_APM = ELASTIC_APM.resolve(None)


def _span(src: dict[str, Any]) -> dict[str, Any] | None:
    return _APM.span(src)


def _log(src: dict[str, Any]) -> dict[str, Any] | None:
    return _APM.log(src)


def _links(raw: Any) -> tuple[tuple[str, str], ...]:
    return _apm_links(raw, "trace.id", "span.id")
