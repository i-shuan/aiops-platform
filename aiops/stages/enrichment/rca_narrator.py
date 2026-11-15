"""新引擎（rca_report_llm）的戰情摘要。

放在根因排名（rca.root_cause.llm）與 rca.report 之後，只吃已經算好的東西：判定狀態、根因候選、證據層事實、
LLM 抽出且驗證過的證據、命中的 Runbook。LLM 只負責把它們寫成人看得懂的摘要，**不下新的結論**。

驗證沿用 narrative.validate（每條 claim 要引用存在的段落、不准提事件群與這些段落以外的已知實體、數字要在段落原文裡）；
另加一條：判定狀態是 undetermined 時摘要必須寫「無法判定」。
驗證不過或 LLM 失敗 → 退回模板。這一步是旁路：不改 rca_root_candidates，也不改 rca_status。
結果放既有欄位 ctx.llm_summary／llm_claims／llm_generated_by／llm_rejected，報告與 API 不用改。
"""
from __future__ import annotations

import json
import logging
from typing import Any

from aiops.engine.context import PipelineContext
from aiops.engine.operator import BaseOperator
from aiops.engine.params import positive
from aiops.engine.rca_types import CONCLUSION_LABELS
from aiops.engine.registry import OperatorRegistry
from aiops.stages.enrichment import narrative
from aiops.stages.enrichment.base import LLMError, parse_json_reply
from aiops.stages.enrichment.masking import MASK_FAILED, Masker, MaskError, MaskPolicy, mask_or_fail, rca_keep_names
from aiops.stages.enrichment.evidence_extractor import (CallFailure, check_provider, complete_with_policy, known_entities,
                                                        resolve_provider)

logger = logging.getLogger("aiops.enrichment")

UNDETERMINED_WORD = "無法判定"
SYSTEM = (
    "你是 SRE 戰情摘要作者。根因排名與機率已經由平台算好，你只能把使用者給的段落寫成摘要，"
    "不得改變排名、不得自己下新的根因結論、不得加入段落以外的實體或數字。"
    f"判定狀態是「{UNDETERMINED_WORD}」時，摘要必須明講{UNDETERMINED_WORD}與原因，不准指認根因。只輸出 JSON，不要其他文字。"
)
_MAX_ITEMS = 8


def _num(v: float) -> str:
    return f"{v:.3f}".rstrip("0").rstrip(".") if isinstance(v, float) else str(v)


# 模板摘要第一句的開頭，依結論等級（engine/rca_types.ConclusionType）；confirmed 維持原本的「最可能的根因」
_LEAD = {"symptom_location": "根因未確認，症狀出現的位置", "topology_inference": "沒有 trace 證據，依拓撲推測的源頭（未確認）"}


def build_sections(ctx: PipelineContext) -> list[dict[str, str]]:
    """把已算好的結果攤成 {key, title, body}（narrative.validate 吃的形狀）。沒有內容的段落不列。"""
    out: list[dict[str, str]] = []
    if ctx.rca_status == "undetermined":
        status = f"判定狀態：{UNDETERMINED_WORD}（{ctx.rca_undetermined_reason or '原因未記錄'}）"
    else:
        status = "判定狀態：ok"
    if ctx.rca_root_candidates:  # 結論等級（演算法宣告的）：推測與症狀位置不准被寫成確定的根因
        status += f"；{CONCLUSION_LABELS[ctx.rca_conclusion_type]}"
    out.append({"key": "rca_status", "title": "判定狀態", "body": status})
    # 候選是誰判的照實寫：LLM 判斷（rca.root_cause.llm）的分數是它自評的信心；其他（原本的機率模型，已移除）照舊寫機率
    by_llm = ctx.rca_root_algorithm == "rca.root_cause.llm"
    title, word = ("根因候選（LLM 判斷）", "信心") if by_llm else ("根因候選（機率模型）", "機率")
    if ctx.rca_root_candidates:
        rows = []
        for i, c in enumerate(ctx.rca_root_candidates[:5], 1):
            dep = f"，等待 {c.dependency}" if c.dependency else ""
            rows.append(f"{i}. {c.entity}（{word} {_num(round(float(c.score), 3))}{dep}）：{c.reason}")
        out.append({"key": "root_candidates", "title": title, "body": "\n".join(rows)})
    elif ctx.rca_root_note:
        out.append({"key": "root_candidates", "title": title, "body": f"沒有候選：{ctx.rca_root_note}"})
    if ctx.rca_facts:
        rows = [f"{f.entity} {f.kind}={_num(float(f.value))}{(' ' + f.unit) if f.unit else ''}（{f.window}）"
                for f in ctx.rca_facts[:20]]
        out.append({"key": "facts", "title": "證據層事實", "body": "\n".join(rows)})
    if ctx.rca_evidence:
        rows = [f"{e.entity} {e.kind} {e.state}（信心 {_num(round(e.confidence, 2))}）：「{e.quote}」" for e in ctx.rca_evidence[:20]]
        out.append({"key": "evidence", "title": "日誌證據", "body": "\n".join(rows)})
    if ctx.rca_runbook:
        rb = ctx.rca_runbook
        acts = "；".join(rb.get("actions") or [])
        out.append({"key": "runbook", "title": "命中的 Runbook", "body": f"[{rb.get('id')}] {rb.get('name', '')}：{acts}"})
    return out


def template(ctx: PipelineContext, sections: list[dict[str, str]]) -> dict[str, Any]:
    """不靠 LLM 的摘要：每條都附來源段落。undetermined 時第一句就講無法判定與原因。"""
    by_key = {s["key"]: s for s in sections}
    claims: list[dict[str, Any]] = []
    if ctx.rca_status == "undetermined":
        reason = ctx.rca_undetermined_reason or "原因未記錄"
        summary = f"{UNDETERMINED_WORD}根因：{reason}。以下只列證據層算好的事實與原始日誌證據，請人工判讀。"
        claims.append({"text": f"{UNDETERMINED_WORD}：{reason}", "evidence": ["rca_status"]})
    elif ctx.rca_root_candidates:
        lead = _LEAD.get(ctx.rca_conclusion_type, "最可能的根因")
        summary = f"{lead}：{by_key['root_candidates']['body'].splitlines()[0].split('. ', 1)[-1]}"
        claims.append({"text": summary, "evidence": ["root_candidates"]})
    else:
        summary = f"證據不足，不指認單一服務：{ctx.rca_root_note or '機率模型沒有給出候選'}"
        claims.append({"text": summary, "evidence": ["root_candidates" if "root_candidates" in by_key else "rca_status"]})
    for key in ("facts", "evidence", "runbook"):
        if key in by_key and len(claims) < _MAX_ITEMS:
            claims.append({"text": by_key[key]["body"].splitlines()[0], "evidence": [key]})
    return {"summary": summary[:600], "claims": claims}


def build_prompt(sections: list[dict[str, str]]) -> str:
    return (
        "以下是平台已算好的根因分析結果（key 是段落代號）：\n"
        + json.dumps(sections, ensure_ascii=False, indent=2)
        + '\n\n請輸出：{"summary": "300 字以內的戰情摘要", '
        '"claims": [{"text": "一句話", "evidence": ["引用的段落 key", ...]}]}，claims 1～8 條。'
    )


def validate(reply: dict[str, Any], ctx: PipelineContext, sections: list[dict[str, str]]) -> str | None:
    allowed = {e for inc in ctx.incidents for e in inc.entities}
    allowed |= {c.entity for c in ctx.rca_root_candidates} | {f.entity for f in ctx.rca_facts} | {e.entity for e in ctx.rca_evidence}
    reason = narrative.validate(reply, sections, allowed, known_entities(ctx) | allowed)
    if reason:
        return reason
    if ctx.rca_status == "undetermined" and UNDETERMINED_WORD not in reply["summary"]:
        return f"判定狀態是 undetermined，但摘要沒有寫「{UNDETERMINED_WORD}」"
    return None


@OperatorRegistry.register("enrichment.rca_narrator")
class RcaNarrator(BaseOperator):
    """戰情摘要（見模組說明）。params：provider（不寫看環境變數 AIOPS_LLM_PROVIDER）、model、base_url、json_mode（預設 true）、
    timeout_seconds（預設 30）、mask_hostnames（預設 false，見 masking.py）。不重試：摘要是旁路，失敗就用模板。
    送 LLM 的是遮過的段落（驗證也對遮過的那份比）；模板不送出，用原本的段落。"""

    scope = "batch"

    def __init__(self, provider: str | None = None, model: str | None = None, base_url: str | None = None,
                 json_mode: bool = True, timeout_seconds: float = 30.0, mask_hostnames: bool = False):
        check_provider(provider)
        if not isinstance(mask_hostnames, bool):
            raise ValueError(f"mask_hostnames 必須是 true／false，不是 {mask_hostnames!r}")
        self.mask = MaskPolicy(mask_hostnames=mask_hostnames)
        self.provider, self.model, self.base_url, self.json_mode = provider, model, base_url, bool(json_mode)
        self.timeout = positive("timeout_seconds", timeout_seconds)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not ctx.incidents:
            return ctx
        sections = build_sections(ctx)
        fallback = template(ctx, sections)
        ctx.llm_summary, ctx.llm_claims, ctx.llm_generated_by, ctx.llm_rejected = (
            fallback["summary"], fallback["claims"], "template", None)
        masker = Masker(self.mask, keep=rca_keep_names(ctx) | known_entities(ctx))
        try:
            sections = mask_or_fail(masker, sections)
            text = complete_with_policy(resolve_provider(self.provider), self.model, self.base_url, self.json_mode,
                                        SYSTEM, build_prompt(sections), self.timeout, 0, masker=masker)
            reply = parse_json_reply(text)
        except MaskError as e:
            return self._fallback(ctx, f"{MASK_FAILED}：{e}")
        except CallFailure as f:
            return self._fallback(ctx, f.reason())
        except LLMError as e:
            return self._fallback(ctx, f"llm_bad_json：{e}")
        reason = validate(reply, ctx, sections)
        if reason:
            return self._fallback(ctx, reason)
        ctx.llm_summary, ctx.llm_claims, ctx.llm_generated_by = reply["summary"], reply["claims"], "llm"
        return ctx

    @staticmethod
    def _fallback(ctx: PipelineContext, reason: str) -> PipelineContext:
        """模板已經放好了，這裡只記原因。"""
        logger.warning("NarratorFallback incidents=%s：%s", [i.uid for i in ctx.incidents], reason)
        ctx.llm_rejected = reason
        return ctx
