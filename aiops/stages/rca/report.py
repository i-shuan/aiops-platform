from __future__ import annotations

from typing import Any

from aiops.engine.context import PipelineContext
from aiops.engine.errors import WorkflowError
from aiops.engine.registry import OperatorRegistry
from aiops.engine.operator import BaseOperator
from aiops.stages.rca.base import SECTION_PREFIX, RcaSection, need_incident


@OperatorRegistry.register("rca.report")
class RcaReport(BaseOperator):
    """依 sections 清單逐段產生 RCA 報告。段落是註冊在 OperatorRegistry 的 rca.section.<代號>，
    放進 stages/rca/sections/ 就能用，這裡不寫死任何段落。資料不足的段落略過（記在 ctx.rca_skipped）。

    根因候選由前面的 rca.root_cause.* 節點產生；沒有根因節點時，根因相關段落會略過。
    params: sections（段落代號，順序就是報告順序）、section_params（{代號: 參數}，給需要參數的段落）。
    """

    scope = "batch"

    def __init__(self, sections: list[str], section_params: dict[str, dict[str, Any]] | None = None):
        section_params = section_params or {}
        for key in sections:  # 先查段落名稱：寫錯段落是根本錯誤，section_params 對不上只是它的後果
            if not OperatorRegistry.has(f"{SECTION_PREFIX}{key}"):
                available = ", ".join(n.removeprefix(SECTION_PREFIX) for n in OperatorRegistry.names(SECTION_PREFIX))
                raise WorkflowError(f"未知的報告段落：{key}（可用：{available}）")
        unknown_params = sorted(set(section_params) - set(sections))
        if unknown_params:
            raise WorkflowError(f"section_params 給了沒有列在 sections 裡的段落：{unknown_params}")
        self.sections: list[RcaSection] = [OperatorRegistry.create(f"{SECTION_PREFIX}{key}", section_params.get(key))
                                           for key in sections]

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        need_incident(ctx)
        for section in self.sections:
            ctx = section.execute(ctx)
        return ctx
