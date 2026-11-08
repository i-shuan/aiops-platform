from datetime import timedelta

from aiops.commands.base import BaseCommand, CommandError, CommandNotFound
from aiops.domains.rca.commands import narrative
from aiops.domains.rca.dao import IncidentReadDAO, LotPathReadDAO, ServiceEdgeReadDAO, TraceSeriesReadDAO
from aiops.domains.rca.schemas import GenerateRCARequest
from aiops.infrastructure.errors import InfrastructureError
from aiops.llm import get_llm
from aiops.llm.base import LLMError, parse_json_reply
from aiops.plugins.base import Incident, fill_context, propagation_edges

# trace 佐證看事件群前後多久的 trace 時序（跟 trace_evidence 段落的範圍一致）
_TRACE_MARGIN = timedelta(minutes=10)
from aiops.plugins.registry import SectionRegistry
from aiops.presets import get_preset


class GenerateRCACommand(BaseCommand):
    """報告段落由 preset 決定，不寫死段數；資料不足的段落直接略過，不輸出空殼。

    最後產生總結敘述：有設定 LLM 就讓 LLM 寫並做依據驗證，沒設定、失敗或驗證不過一律退回模板。
    """

    def __init__(self, req: GenerateRCARequest):
        self.req = req

    def validate(self) -> None:
        self.preset = get_preset(self.req.preset)
        missing = [s for s in self.preset.rca_sections if not SectionRegistry.has(s)]
        if missing:
            raise CommandError(f"preset {self.preset.name} 引用了未註冊的報告段落: {missing}")
        self.row = IncidentReadDAO.get(self.req.incident_id)
        if self.row is None:
            raise CommandNotFound(f"找不到 incident {self.req.incident_id}")

    def _context(self, incident: Incident) -> dict:
        """呼叫端沒給的場景資料，從已存的推導資料補上（呼叫端明確給的一律優先）。"""
        ctx = fill_context(self.req.context, service_edges=ServiceEdgeReadDAO.all, lot_paths=LotPathReadDAO.paths)
        if "trace_series" not in ctx:
            rows = TraceSeriesReadDAO.for_services(incident.entities, incident.start - _TRACE_MARGIN,
                                                   incident.end + _TRACE_MARGIN)
            if rows:
                ctx["trace_series"] = [
                    {"service": r["service"], "ts": r["ts"].isoformat(), "count": r["count"],
                     "error_rate": r["error_rate"], "p95_ms": r["p95_ms"]} for r in rows
                ]
        return ctx

    def _narrative(self, incident: Incident, sections: list[dict], ctx: dict) -> dict:
        result = {**narrative.template(sections), "generated_by": "template", "rejected": None}
        if not self.req.narrative:
            return result
        try:
            llm = get_llm()
        except InfrastructureError as e:
            return {**result, "rejected": f"LLM 設定錯誤：{e}"}
        if llm is None:
            return result
        try:
            reply = parse_json_reply(llm.complete(narrative.SYSTEM, narrative.build_prompt(sections)))
        except LLMError as e:
            return {**result, "rejected": str(e)}
        known = set(incident.entities) | {n for edge in propagation_edges(ctx) for n in edge}
        known |= set(ctx.get("p0_entities", []))
        reason = narrative.validate(reply, sections, set(incident.entities), known)
        if reason:
            return {**result, "rejected": reason}
        return {"summary": reply["summary"], "claims": reply["claims"], "generated_by": "llm", "rejected": None}

    def run(self) -> dict:
        self.validate()
        incident = Incident.from_dict(self.row["incident"])
        ctx = self._context(incident)
        sections, skipped = [], []
        for name in self.preset.rca_sections:
            builder = SectionRegistry.get(name)()
            if builder.applies(incident, ctx):
                sections.append(builder.build(incident, ctx).to_dict())
            else:
                skipped.append(name)
        return {"incident_id": self.req.incident_id, "incident_uid": self.row["uid"], "preset": self.preset.name,
                "sections": sections, "skipped": skipped, "narrative": self._narrative(incident, sections, ctx)}
