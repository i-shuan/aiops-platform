from __future__ import annotations

import logging

from aiops.engine.context import PipelineContext
from aiops.engine.registry import OperatorRegistry
from aiops.engine.operator import BaseOperator
from aiops.stages.enrichment import narrative
from aiops.stages.enrichment.base import LLMError, parse_json_reply
from aiops.stages.enrichment.masking import MASK_FAILED, Masker, MaskError, MaskPolicy, mask_or_fail, rca_keep_names, send
from aiops.stages.enrichment import registry
from aiops.stages.enrichment.providers import build_llm

logger = logging.getLogger("aiops.enrichment")


@OperatorRegistry.register("enrichment.llm_summary")
class LLMSummarizer(BaseOperator):
    """RCA 總結敘述。provider=noop（預設）時完全不連外，直接用模板；
    設了供應商就讓 LLM 寫、平台驗證依據，任何一步失敗（沒金鑰、連不上、輸出驗證不過）都退回模板，絕不阻斷管線。
    結果放 ctx.llm_summary／llm_claims，並標 llm_generated_by（llm／template）與 llm_rejected（退回原因）。
    送 LLM 的段落先經 masking 遮罩敏感資訊（驗證也對遮過的那份比；遮罩失敗就不送、用模板）。mask_hostnames 見 masking.py。
    """

    scope = "batch"

    def __init__(self, provider: str = "noop", model: str | None = None, base_url: str | None = None,
                 mask_hostnames: bool = False):
        if provider not in registry.names():
            raise TypeError(f"provider 只能是 {', '.join(registry.names())}，不是 {provider!r}")
        if not isinstance(mask_hostnames, bool):
            raise ValueError(f"mask_hostnames 必須是 true／false，不是 {mask_hostnames!r}")
        self.provider, self.model, self.base_url = provider, model, base_url
        self.mask = MaskPolicy(mask_hostnames=mask_hostnames)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        sections = [s.to_dict() for s in ctx.rca_sections]
        fallback = narrative.template(sections)
        ctx.llm_summary, ctx.llm_claims, ctx.llm_generated_by, ctx.llm_rejected = (
            fallback["summary"], fallback["claims"], "template", None)
        if self.provider == "noop" or not sections:
            return ctx
        masker = Masker(self.mask, keep=rca_keep_names(ctx))
        try:
            sections = mask_or_fail(masker, sections)
            llm = build_llm(self.provider, self.model, self.base_url)
            assert llm is not None
            reply = parse_json_reply(send(llm, narrative.SYSTEM, narrative.build_prompt(sections), masker))
        except MaskError as e:
            logger.warning("llm_summary 遮罩失敗，不送出：%s", e)
            ctx.llm_rejected = f"{MASK_FAILED}：{e}"
            return ctx
        except LLMError as e:
            ctx.llm_rejected = str(e)
            return ctx

        incident = ctx.incidents[0]
        known = set(incident.entities) | {n for edge in ctx.graph.propagation() for n in edge}
        reason = narrative.validate(reply, sections, set(incident.entities), known)
        if reason:
            ctx.llm_rejected = reason
            return ctx
        ctx.llm_summary, ctx.llm_claims, ctx.llm_generated_by = reply["summary"], reply["claims"], "llm"
        return ctx


@OperatorRegistry.register("enrichment.llm_briefing")
class LLMBriefing(BaseOperator):
    """三行式止血摘要（docs/RCA_STEP2_DESIGN.md 第 4 節）。放在 rca.report 之後：context 只吃已算好的根因歸因、範例 trace、
    日誌樣板與命中的 Runbook（ctx.rca_runbook），不丟原始資料。provider=noop（預設）完全不連外、直接用模板；
    設了供應商就讓 LLM 寫、平台驗（白名單、封閉詞表、數字 ±10%），任何一步失敗都退回模板，絕不阻斷管線。
    params: provider、model、base_url、json_mode（預設 true；只對 OpenAI 相容供應商有效）、mask_hostnames（預設 false，見 masking.py）。
    送 LLM 的 context 先經 masking 遮罩（驗證也對遮過的那份比）；模板用原本的 context，不送出。
    """

    scope = "batch"

    def __init__(self, provider: str = "noop", model: str | None = None, base_url: str | None = None, json_mode: bool = True,
                 mask_hostnames: bool = False):
        if provider not in registry.names():
            raise TypeError(f"provider 只能是 {', '.join(registry.names())}，不是 {provider!r}")
        if not isinstance(mask_hostnames, bool):
            raise ValueError(f"mask_hostnames 必須是 true／false，不是 {mask_hostnames!r}")
        self.provider, self.model, self.base_url, self.json_mode = provider, model, base_url, bool(json_mode)
        self.mask = MaskPolicy(mask_hostnames=mask_hostnames)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not ctx.incidents:
            return ctx
        incident = ctx.incidents[0]
        context = narrative.build_context(ctx, incident)
        ctx.llm_briefing = narrative.briefing_template(context)
        ctx.llm_briefing_generated_by, ctx.llm_briefing_rejected = "template", None
        if self.provider == "noop":
            return ctx
        masker = Masker(self.mask, keep=rca_keep_names(ctx))
        try:
            context = mask_or_fail(masker, context)
            llm = build_llm(self.provider, self.model, self.base_url, json_mode=self.json_mode)
            assert llm is not None
            reply = parse_json_reply(send(llm, narrative.BRIEFING_SYSTEM, narrative.build_briefing_prompt(context), masker))
        except MaskError as e:
            logger.warning("llm_briefing 遮罩失敗，不送出：%s", e)
            ctx.llm_briefing_rejected = f"{MASK_FAILED}：{e}"
            return ctx
        except LLMError as e:
            ctx.llm_briefing_rejected = str(e)
            return ctx
        known = set(incident.entities) | {n for edge in ctx.graph.propagation() for n in edge}
        reason = narrative.validate_briefing(reply, context, known)
        if reason:
            logger.warning("HallucinationBlocked incident=%s provider=%s：%s", incident.uid, self.provider, reason)
            ctx.llm_briefing_rejected = reason
            return ctx
        ctx.llm_briefing = {k: reply[k].strip() for k in narrative.BRIEFING_KEYS}
        ctx.llm_briefing_generated_by = "llm"
        return ctx
