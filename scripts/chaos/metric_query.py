"""混沌模擬器的指標查詢（資料源 chaos_metrics，scripts/chaos/es.py）：JSON 字串 -> 降採樣（date_histogram）的 search body。

這原本是平台的 aiops/sources/query/elasticsearch.py。平台不再直連 Elasticsearch（2026-11-20）後平台裡沒有人用它，
搬到這裡只給模擬器的假世界回答指標查詢；不是平台程式，aiops/ 不 import 它。

query 範例（就是 MetricTarget.query 的內容）：
  {"index": "metrics-apm.transaction.1m-*", "value_field": "transaction.duration.histogram",
   "agg": "avg", "filter": {"term": {"service.name": "cart"}}}

agg：avg／max／min／sum／count（文件數）／p95／p99。count 不需要 value_field。

divide_by（只能配 agg=sum）：每一桶回 Σvalue_field ÷ Σdivide_by。預先聚合的指標（Elastic APM 的
transaction.duration.summary.sum 是「這一分鐘所有請求的總耗時」）要除以筆數才是每筆請求的平均延遲：
  {"index": "metrics-apm.transaction.1m-*", "agg": "sum", "value_field": "transaction.duration.summary.sum",
   "divide_by": "transaction.duration.summary.value_count", "filter": {"term": {"service.name": "cart"}}}
不除的話數值跟請求數成正比：每分鐘 0～3 筆的低流量服務，剛好 3 筆的那一分鐘會被當成延遲暴增 3 倍。
分母是 0 或沒有資料的桶當成「沒資料」（不是 0）。

有核准的欄位對照（field_map：來源欄位 -> 標準欄位）時，查詢可以省略欄位名稱：
- 標準欄位 ts    -> 補成 time_field
- 標準欄位 value -> 補成 value_field
- 標準欄位 entity -> 查詢寫 "entity": "cart"，自動加上 {"term": {<對到 entity 的欄位>: "cart"}}
查詢裡明確寫的欄位一律優先，對照只補沒寫的。
"""
import json
from datetime import datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

_PERCENTILES = {"p95": 95.0, "p99": 99.0}


class EsMetricQuery(BaseModel):
    # 打錯的鍵（fliter、value_feild）以前被默默丟掉：少了 filter 就變成整個 index 的平均，數字照出、沒人發現
    model_config = ConfigDict(extra="forbid")

    index: str
    time_field: str = "@timestamp"
    value_field: str | None = None
    agg: Literal["avg", "max", "min", "sum", "count", "p95", "p99"] = "avg"
    divide_by: str | None = None  # 分母欄位：Σvalue_field ÷ Σdivide_by（見檔頭）
    # 原樣放進 bool.filter 的 ES query 子句
    filter: dict[str, Any] | list[dict[str, Any]] | None = None

    @model_validator(mode="after")
    def _value_field_required(self):
        if self.agg != "count" and not self.value_field:
            raise ValueError(f"agg={self.agg} 需要 value_field")
        if self.divide_by is not None and (self.agg != "sum" or not self.divide_by.strip()):
            raise ValueError("divide_by 只能配 agg=sum（Σvalue_field ÷ Σdivide_by），而且不能是空字串")
        if "/" in self.index or self.index.startswith("_"):
            raise ValueError(f"不合法的 index：{self.index}")
        return self


def _apply_field_map(raw: dict[str, Any], field_map: dict[str, str] | None) -> dict[str, Any]:
    standard_to_source = {std: src for src, std in (field_map or {}).items()}
    raw = dict(raw)
    if "time_field" not in raw and "ts" in standard_to_source:
        raw["time_field"] = standard_to_source["ts"]
    if "value_field" not in raw and "value" in standard_to_source:
        raw["value_field"] = standard_to_source["value"]
    entity = raw.pop("entity", None)
    if entity is not None:
        entity_field = raw.pop("entity_field", None) or standard_to_source.get("entity")
        if not entity_field:
            raise ValueError("查詢給了 entity，但沒有 entity_field，也沒有核准的欄位對照把某個欄位對到 entity")
        existing = raw.get("filter")
        filters = existing if isinstance(existing, list) else ([existing] if existing else [])
        raw["filter"] = filters + [{"term": {entity_field: entity}}]
    return raw


def parse_es_query(query: str, field_map: dict[str, str] | None = None) -> EsMetricQuery:
    try:
        raw = json.loads(query)
        if not isinstance(raw, dict):
            raise ValueError("查詢必須是 JSON 物件")
        return EsMetricQuery.model_validate(_apply_field_map(raw, field_map))
    except (json.JSONDecodeError, ValidationError) as e:
        raise ValueError(f"Elasticsearch 查詢格式錯誤：{e}") from e


def build_search_body(q: EsMetricQuery, start: datetime, end: datetime, step_seconds: int) -> dict[str, Any]:
    filters: list[dict[str, Any]] = [
        {"range": {q.time_field: {"gte": start.isoformat(), "lt": end.isoformat()}}}
    ]
    if isinstance(q.filter, list):
        filters += q.filter
    elif q.filter:
        filters.append(q.filter)

    histogram: dict[str, Any] = {
        "date_histogram": {
            "field": q.time_field,
            "fixed_interval": f"{step_seconds}s",
            "min_doc_count": 0,
            # range 是含頭不含尾（lt end）：最後一個補出來的桶必須是 end − step，不能是 end。以前補到 end，那一桶永遠
            # 在 range 之外、一定是空的；avg 回 null 會被丟掉沒事，sum／count 卻回 0——每一輪最後一點都是假的「掉到 0」
            "extended_bounds": {"min": start.isoformat(),
                                "max": max(start, end - timedelta(seconds=step_seconds)).isoformat()},
        }
    }
    if q.agg in _PERCENTILES:
        histogram["aggs"] = {"v": {"percentiles": {"field": q.value_field, "percents": [_PERCENTILES[q.agg]]}}}
    elif q.agg != "count":
        histogram["aggs"] = {"v": {q.agg: {"field": q.value_field}}}
        if q.divide_by:
            histogram["aggs"]["d"] = {"sum": {"field": q.divide_by}}
    return {"size": 0, "query": {"bool": {"filter": filters}}, "aggs": {"series": histogram}}


def bucket_value(q: EsMetricQuery, bucket: dict[str, Any]) -> float | None:
    if q.agg == "count":
        return float(bucket["doc_count"])
    v = bucket.get("v") or {}
    if q.agg in _PERCENTILES:
        values = v.get("values") or {}
        raw = next(iter(values.values()), None)
    else:
        raw = v.get("value")
    if raw is None:
        return None
    if q.divide_by:
        denominator = (bucket.get("d") or {}).get("value")
        if not denominator or float(denominator) <= 0:  # 這一桶沒有請求：沒資料，不是「延遲 0」
            return None
        return float(raw) / float(denominator)
    return float(raw)
