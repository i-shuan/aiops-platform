from __future__ import annotations

from aiops.core.context import PipelineContext
from aiops.core.registry import OperatorRegistry
from aiops.core.topology import propagation_edges
from aiops.operators.base import BaseOperator
from aiops.operators.llm import narrative
from aiops.operators.llm.base import LLMError, parse_json_reply
from aiops.operators.llm import registry
from aiops.operators.llm.providers import build_llm


@OperatorRegistry.register("enrichment.llm_summary")
class LLMSummarizer(BaseOperator):
    """RCA 總結敘述。provider=noop（預設）時完全不連外，直接用模板；
    設了供應商就讓 LLM 寫、平台驗證依據，任何一步失敗（沒金鑰、連不上、輸出驗證不過）都退回模板，絕不阻斷管線。
    結果放 ctx.llm_summary／llm_claims，並標 llm_generated_by（llm／template）與 llm_rejected（退回原因）。
    """

    scope = "batch"

    def __init__(self, provider: str = "noop", model: str | None = None, base_url: str | None = None):
        if provider not in registry.names():
            raise TypeError(f"provider 只能是 {', '.join(registry.names())}，不是 {provider!r}")
        self.provider, self.model, self.base_url = provider, model, base_url

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        sections = [s.to_dict() for s in ctx.rca_sections]
        fallback = narrative.template(sections)
        ctx.llm_summary, ctx.llm_claims, ctx.llm_generated_by, ctx.llm_rejected = (
            fallback["summary"], fallback["claims"], "template", None)
        if self.provider == "noop" or not sections:
            return ctx
        try:
            llm = build_llm(self.provider, self.model, self.base_url)
            assert llm is not None
            reply = parse_json_reply(llm.complete(narrative.SYSTEM, narrative.build_prompt(sections)))
        except LLMError as e:
            ctx.llm_rejected = str(e)
            return ctx

        incident = ctx.incidents[0]
        known = set(incident.entities) | {n for edge in propagation_edges(ctx.topology, ctx.lot_edges) for n in edge}
        reason = narrative.validate(reply, sections, set(incident.entities), known)
        if reason:
            ctx.llm_rejected = reason
            return ctx
        ctx.llm_summary, ctx.llm_claims, ctx.llm_generated_by = reply["summary"], reply["claims"], "llm"
        return ctx
