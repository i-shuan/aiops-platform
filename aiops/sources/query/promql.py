"""PromQL（Prometheus／VictoriaMetrics）查詢的基本檢查。完整語法交給伺服器驗證，這裡只擋明顯會出錯的寫法。"""

_PAIRS = {")": "(", "]": "[", "}": "{"}


def validate_promql(query: str) -> str:
    q = query.strip()
    if not q:
        raise ValueError("PromQL 查詢不可為空")
    stack: list[str] = []
    quote: str | None = None
    for ch in q:
        if quote:
            if ch == quote:
                quote = None
            continue
        if ch in "\"'`":
            quote = ch
        elif ch in "([{":
            stack.append(ch)
        elif ch in _PAIRS:
            if not stack or stack.pop() != _PAIRS[ch]:
                raise ValueError(f"PromQL 括號不成對：{q}")
    if stack or quote:
        raise ValueError(f"PromQL 括號或引號沒有關閉：{q}")
    if q.endswith("]"):
        # 例如 cpu[5m]：range vector 不能直接 query_range，要包在 rate()／avg_over_time() 等函式裡
        raise ValueError("查詢結果是 range vector，請包在 rate()、avg_over_time() 等函式裡")
    return q
