"""事件群的特徵簽章：給歷史相似事故比對用。純函式。

指標名稱只取「指標本身」，去掉標籤選擇器與 PromQL 函式：
  sum(rate(http_requests_total{code=~"5.."}[5m])) -> http_requests_total
  cpu{service="cart"} -> cpu
這樣不同服務、不同寫法的同一種指標才比得起來。
"""
from __future__ import annotations

import re

from aiops.engine.types import Incident

_PROMQL_WORDS = frozenset({
    "sum", "avg", "min", "max", "count", "rate", "irate", "increase", "by", "without", "on", "ignoring",
    "histogram_quantile", "quantile", "topk", "bottomk", "stddev", "abs", "delta", "deriv", "offset",
    "and", "or", "unless", "group_left", "group_right", "avg_over_time", "max_over_time", "min_over_time",
    "sum_over_time", "count_over_time", "clamp_min", "clamp_max", "round", "le",
})
_BRACES = re.compile(r"\{[^}]*\}|\[[^\]]*\]")
_IDENT = re.compile(r"[A-Za-z_:][A-Za-z0-9_:.]*")


def metric_names(metric: str) -> set[str]:
    stripped = _BRACES.sub(" ", metric)
    names = {t for t in _IDENT.findall(stripped) if t.lower() not in _PROMQL_WORDS}
    return names or {metric.strip()}


def incident_signature(incident: Incident) -> tuple[list[str], list[str], list[str]]:
    """(實體, 指標名稱, 訊號)，各自排序去重。"""
    entities = sorted(set(incident.entities))
    metrics = sorted({n for e in incident.events for n in metric_names(e.metric)})
    signals = sorted({str(e.attrs["signal"]) for e in incident.events if e.attrs.get("signal")})
    return entities, metrics, signals


def jaccard(a: list[str] | set[str], b: list[str] | set[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)
