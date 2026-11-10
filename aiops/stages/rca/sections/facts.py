"""證據層事實段落（認知型 RCA）：把 ctx.rca_facts 與驗證後的 LLM 證據原樣列給值班的人。

這段只呈現、不下結論。判定狀態是 undetermined（LLM 沒設定、連不上、逾時、格式不合）時，報告沒有根因結論，
這段把原因寫明，並指向 trace 佐證與日誌樣板兩段（原始證據的入口）。沒有事實也沒有證據、狀態也正常就略過。
"""
from __future__ import annotations

from aiops.engine.registry import OperatorRegistry
from aiops.engine.rca_types import Fact
from aiops.stages.rca.base import RcaSection


def _value(f: Fact) -> str:
    return f"{f.value:g}{f.unit}" if f.unit else f"{f.value:g}"


@OperatorRegistry.register("rca.section.facts")
class FactsSection(RcaSection):
    """params: max_facts（最多列幾筆事實，預設 30；依證據層固定排序取前面幾筆，超過的只報筆數）。"""

    key, title = "facts", "證據層事實與日誌證據"

    def __init__(self, max_facts: int = 30):
        if int(max_facts) < 1:
            raise ValueError("max_facts 至少 1")
        self.max_facts = int(max_facts)

    def applies(self, incident, ctx):
        return bool(ctx.rca_facts or ctx.rca_evidence or ctx.rca_status == "undetermined")

    def build(self, incident, ctx):
        lines: list[str] = []
        if ctx.rca_status == "undetermined":
            lines.append(f"判定狀態：無法判定（{ctx.rca_undetermined_reason or '原因未記錄'}）。"
                         "沒有根因結論；原始證據請看「trace 佐證」與「錯誤日誌」兩段。")
        if ctx.rca_evidence:
            lines.append("日誌證據（LLM 抽取、已驗證 quote 逐字出現在原日誌）：")
            for e in ctx.rca_evidence:
                lines.append(f"- {e.entity}：{e.kind}（{e.state}，信心 {e.confidence:g}）「{e.quote}」")
        if ctx.rca_facts:
            shown = ctx.rca_facts[:self.max_facts]
            lines.append(f"事實（證據層算出，共 {len(ctx.rca_facts)} 筆，只陳述不判斷）：")
            for f in shown:
                lines.append(f"- {f.kind}｜{f.entity}｜{_value(f)}｜{f.window}")
            if len(ctx.rca_facts) > len(shown):
                lines.append(f"（另有 {len(ctx.rca_facts) - len(shown)} 筆未列出）")
        return "\n".join(lines)
