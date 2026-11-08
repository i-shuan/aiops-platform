"""內部來源 trace 的查詢語法：「服務/欄位」，例如 cart/p95_ms。欄位只能是 trace 合成產出的三個。"""

TRACE_FIELDS = ("count", "error_rate", "p95_ms")


def parse_trace_query(query: str) -> tuple[str, str]:
    service, sep, field = query.strip().rpartition("/")
    if not sep or not service:
        raise ValueError(f"trace 查詢格式是「服務/欄位」，例如 cart/p95_ms：{query}")
    if field not in TRACE_FIELDS:
        raise ValueError(f"trace 欄位只能是 {', '.join(TRACE_FIELDS)}：{field}")
    return service, field
