from aiops.commands.base import BaseCommand, CommandConflict, CommandError, CommandNotFound
from aiops.domains.tuning.commands import rule_extraction
from aiops.domains.tuning.commands.proposals import CreateProposalCommand
from aiops.domains.tuning.dao import ActiveConfigDAO, FeedbackDAO, IncidentReadDAO
from aiops.domains.tuning.schemas import FeedbackSubmitRequest, ProposalCreateRequest, ProposeFromFeedbackRequest
from aiops.infrastructure.errors import InfrastructureError
from aiops.llm import get_llm
from aiops.llm.base import LLMError, parse_json_reply
from aiops.plugins.registry import DetectorRegistry


class SubmitFeedbackCommand(BaseCommand):
    def __init__(self, req: FeedbackSubmitRequest):
        self.req = req

    def validate(self) -> None:
        if IncidentReadDAO.get_by_uid(self.req.incident_uid) is None:
            raise CommandNotFound(f"找不到事件群 {self.req.incident_uid}")
        if self.req.detector_name and not DetectorRegistry.has(self.req.detector_name):
            raise CommandError(f"未知的檢測外掛: {self.req.detector_name}")

    def run(self) -> dict:
        self.validate()
        return FeedbackDAO.create(self.req.model_dump())


class ProposeFromFeedbackCommand(BaseCommand):
    """回饋 -> 調參提案。LLM 抽規則並驗證，不行就用固定規則；結果只是提案，仍要回測與核准才生效。"""

    def __init__(self, feedback_id: int, req: ProposeFromFeedbackRequest):
        self.feedback_id = feedback_id
        self.req = req

    def validate(self) -> None:
        self.fb = FeedbackDAO.get(self.feedback_id)
        if self.fb is None:
            raise CommandNotFound(f"找不到回饋 {self.feedback_id}")
        if self.fb["proposal_id"] is not None:
            raise CommandConflict(f"回饋 {self.feedback_id} 已轉成提案 {self.fb['proposal_id']}")
        if self.fb["verdict"] == "true_positive":
            raise CommandConflict("這筆回饋確認是真異常，不需要調參")
        self.detector = self.req.detector_name or self.fb["detector_name"]
        if not self.detector or not DetectorRegistry.has(self.detector):
            raise CommandError(f"請指定有效的偵測器（目前：{self.detector}）")
        self.tunable = DetectorRegistry.get(self.detector).tunable
        if not self.tunable:
            raise CommandError(f"{self.detector} 沒有可自動調整的參數，請走人工提案")

    def _extract(self, current: dict[str, float]) -> tuple[dict, str, str, str | None]:
        """回傳 (要改的參數, 理由, generated_by, 退回原因)。"""
        fb = self.fb
        fallback = (*rule_extraction.template(fb["verdict"], self.tunable, current), "template")
        try:
            llm = get_llm()
        except InfrastructureError as e:
            return (*fallback, f"LLM 設定錯誤：{e}")
        if llm is None:
            return (*fallback, None)
        try:
            prompt = rule_extraction.build_prompt(self.detector, fb["verdict"], fb["text"], self.tunable, current)
            reply = parse_json_reply(llm.complete(rule_extraction.SYSTEM, prompt))
        except LLMError as e:
            return (*fallback, str(e))
        reason = rule_extraction.validate(reply, fb["verdict"], self.tunable, current)
        if reason:
            return (*fallback, reason)
        return reply["params"], reply["reason"], "llm", None

    def run(self) -> dict:
        self.validate()
        active = (ActiveConfigDAO.get(self.detector) or {}).get("params", {})
        current = {k: float(active.get(k, default)) for k, (default, _low, _high) in self.tunable.items()}
        changes, reason, generated_by, rejected = self._extract(current)
        proposal = CreateProposalCommand(ProposalCreateRequest(
            source="llm" if generated_by == "llm" else "auto",
            detector_name=self.detector,
            params={**active, **changes},
            reason=f"來自回饋 #{self.fb['id']}（{self.fb['verdict']}）：{reason}",
            proposer=self.req.proposer,
        )).run()
        FeedbackDAO.link_proposal(self.fb["id"], proposal["id"])
        return {"proposal": proposal, "generated_by": generated_by, "rejected": rejected}
