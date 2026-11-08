"""Elasticsearch 指標查詢：JSON 字串 -> 降採樣（date_histogram）的 search body。

query 範例（就是 MetricTarget.query 的內容）：
  {"index": "metrics-apm.transaction.1m-*", "value_field": "transaction.duration.histogram",
   "agg": "avg", "filter": {"term": {"service.name": "cart"}}}

agg：avg／max／min／sum／count（文件數）／p95／p99。count 不需要 value_field。
"""
import json
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ValidationError, model_validator

_PERCENTILES = {"p95": 95.0, "p99": 99.0}


class EsMetricQuery(BaseModel):
    index: str
    time_field: str = "@timestamp"
    value_field: str | None = None
    agg: Literal["avg", "max", "min", "sum", "count", "p95", "p99"] = "avg"
    # 原樣放進 bool.filter 的 ES query 子句
    filter: dict[str, Any] | list[dict[str, Any]] | None = None

    @model_validator(mode="after")
    def _value_field_required(self):
        if self.agg != "count" and not self.value_field:
            raise ValueError(f"agg={self.agg} 需要 value_field")
        if "/" in self.index or self.index.startswith("_"):
            raise ValueError(f"不合法的 index：{self.index}")
        return self


def parse_es_query(query: str) -> EsMetricQuery:
    try:
        return EsMetricQuery.model_validate(json.loads(query))
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
            "extended_bounds": {"min": start.isoformat(), "max": end.isoformat()},
        }
    }
    if q.agg in _PERCENTILES:
        histogram["aggs"] = {"v": {"percentiles": {"field": q.value_field, "percents": [_PERCENTILES[q.agg]]}}}
    elif q.agg != "count":
        histogram["aggs"] = {"v": {q.agg: {"field": q.value_field}}}
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
    return None if raw is None else float(raw)
