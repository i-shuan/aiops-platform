from __future__ import annotations

from aiops.core.context import PipelineContext
from aiops.core.errors import NotFound, WorkflowError
from aiops.core.registry import OperatorRegistry
from aiops.operators.base import BaseOperator
from aiops.operators.rca.sections import SECTIONS


@OperatorRegistry.register("rca.report")
class RcaReport(BaseOperator):
    """產生 RCA 報告：依 sections 清單逐段產生，資料不足的段落略過（記在 ctx.rca_skipped），不輸出空殼。
    params: sections（段落代號，順序就是報告順序）。
    """

    scope = "batch"

    def __init__(self, sections: list[str]):
        unknown = [s for s in sections if s not in SECTIONS]
        if unknown:
            raise WorkflowError(f"未知的報告段落：{unknown}（可用：{', '.join(sorted(SECTIONS))}）")
        self.sections = list(sections)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not ctx.incidents:
            raise NotFound("沒有可以產生報告的事件群")
        incident = ctx.incidents[0]
        for key in self.sections:
            section = SECTIONS[key]
            if section.applies(incident, ctx):
                ctx.rca_sections.append(section.build(incident, ctx))
            else:
                ctx.rca_skipped.append(key)
        return ctx
