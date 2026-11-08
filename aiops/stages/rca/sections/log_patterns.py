"""Log 樣式比對：事件期間哪種例外訊息暴增了。補齊「Metrics → Trace → Log」報告的最後一段。

樣板化與排序在 stages/rca/logs.py（根因算子 rca.root_cause.evidence 也用同一份）。
資料要靠前面的 ingestion.incident_logs 撈；沒撈到任何事件期間的日誌就略過這段。
日誌超過上限時資料源會依時間分桶抽樣、每筆帶權重，這裡的筆數是加權後的估計值，段落開頭會寫明。
"""
from __future__ import annotations

from aiops.engine.registry import OperatorRegistry
from aiops.stages.rca.base import RcaSection
from aiops.stages.rca.logs import rank_patterns, template_of  # noqa: F401  template_of 給舊的 import 路徑用

__all__ = ["LogPatterns", "rank_patterns", "template_of"]


@OperatorRegistry.register("rca.section.log_patterns")
class LogPatterns(RcaSection):
    """params: top_n（列幾個，預設 3）、min_count（事件期間至少幾筆才列，預設 3）、
    min_ratio（暴增倍率至少多少才算暴增，預設 2；新出現的樣板以正常期間 1 筆計算倍率）。"""

    key, title = "log_patterns", "錯誤日誌：事件期間暴增的例外"

    def __init__(self, top_n: int = 3, min_count: int = 3, min_ratio: float = 2.0):
        if top_n < 1 or min_count < 1 or min_ratio < 1:
            raise ValueError("top_n、min_count 至少 1；min_ratio 至少 1")
        self.top_n, self.min_count, self.min_ratio = int(top_n), int(min_count), float(min_ratio)

    def applies(self, incident, ctx):
        return bool(ctx.rca_logs)

    def build(self, incident, ctx):
        ranked = rank_patterns(ctx.rca_logs, ctx.rca_logs_baseline, ctx.rca_log_window_minutes, self.min_count)
        bursts = [p for p in ranked if p["ratio"] >= self.min_ratio][: self.top_n]
        m_during, m_base = ctx.rca_log_window_minutes
        sampled = ctx.rca_logs_sampled or any(r.weight != 1.0 for r in (*ctx.rca_logs, *ctx.rca_logs_baseline))
        n_during = sum(r.weight for r in ctx.rca_logs)
        n_base = sum(r.weight for r in ctx.rca_logs_baseline)
        head = f"事件期間 {n_during:.0f} 筆錯誤日誌（{m_during:.0f} 分鐘），事件前 {m_base:.0f} 分鐘 {n_base:.0f} 筆。"
        if sampled:
            head += (f"\n⚠ 日誌超過撈取上限：實際讀到 {len(ctx.rca_logs) + len(ctx.rca_logs_baseline)} 筆，"
                     "依時間均勻抽樣並按各時段總量加權，以下筆數是估計值。")
        if not bursts:
            common = ", ".join(f"[{p['service']}] {p['template']}（{p['during']:.0f} 筆）" for p in ranked[:3]) or "無"
            return head + f"\n沒有明顯暴增的例外（頻率 ≥ {self.min_ratio:g} 倍）。事件期間最常見：{common}"
        lines = [head]
        for i, p in enumerate(bursts, 1):
            kind = f"{p['error_type']}: " if p["error_type"] else ""
            change = (f"新出現（平常沒有，以 1 筆計約 {p['ratio']:.0f} 倍）" if p["new"]
                      else f"平常 {p['baseline']:.0f} 筆，頻率 {p['ratio']:.1f} 倍")
            lines.append(f"{i}. [{p['service']}] {kind}{p['template']}\n"
                         f"   事件期間 {p['during']:.0f} 筆（每分鐘 {p['rate_during']:.2f}），{change}"
                         + (f"；例：trace {p['trace_id']}" if p["trace_id"] else ""))
        return "\n".join(lines)
