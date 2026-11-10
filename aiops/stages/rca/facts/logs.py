"""錯誤日誌 -> 事實（log_template_surge）。樣板化與頻率比直接重用 stages/rca/logs.rank_patterns，不另寫一份。

剝掉的解讀：舊引擎 service_burst 只挑每個服務「最暴增」的一個樣板當根因佐證；這裡每個服務列前 max_per_service 個樣板，
不挑、不加權、不判斷暴增算不算顯著。新出現的樣板（基準期 0 筆）倍率照 rank_patterns 的慣例以基準期 1 筆計，
並在 detail.new 標明，讓消費端自己決定怎麼看。
"""
from __future__ import annotations

from aiops.engine.context import LogRecord
from aiops.engine.rca_types import Fact
from aiops.engine.types import as_utc
from aiops.stages.rca.logs import rank_patterns

SOURCE = "stages.rca.facts.logs"


def log_facts(during: list[LogRecord], baseline: list[LogRecord], minutes: tuple[float, float], sampled: bool,
              min_count: int, max_per_service: int, skips: list[str]) -> list[Fact]:
    if not during:
        skips.append("logs:事件期間沒有錯誤日誌")
        return []
    if minutes[0] <= 0 or minutes[1] <= 0:
        # 沒有窗口長度就算不出每分鐘頻率；rank_patterns 會用 1e-9 當分母，倍率變成天文數字，不能當事實
        skips.append(f"logs:日誌窗口長度不明（事件期間 {minutes[0]:g}、基準期 {minutes[1]:g} 分鐘），不算頻率比")
        return []
    # 先排序：rank_patterns 的代表 trace_id 取「第一筆」、同倍率的樣板依出現順序，輸入順序不同就會變
    key = lambda r: (as_utc(r.ts), r.service, r.error_type, r.message, r.trace_id, r.weight)  # noqa: E731
    during, baseline = sorted(during, key=key), sorted(baseline, key=key)
    kept: dict[str, int] = {}
    out: list[Fact] = []
    for p in rank_patterns(during, baseline, minutes, min_count):
        svc = p["service"]
        if kept.get(svc, 0) >= max_per_service:
            continue
        kept[svc] = kept.get(svc, 0) + 1
        out.append(Fact("log_template_surge", svc, p["ratio"], "ratio", "delta",
                        {"template": p["template"], "error_type": p["error_type"], "during": p["during"],
                         "baseline": p["baseline"], "rate_during": p["rate_during"], "rate_baseline": p["rate_baseline"],
                         "new": p["new"], "n": p["during"], "trace_id": p["trace_id"], "sampled": sampled}, SOURCE))
    for svc in sorted({r.service for r in during} - set(kept)):
        skips.append(f"log_template_surge:{svc}:沒有任何樣板在事件期間達到 {min_count} 筆")
    return out
