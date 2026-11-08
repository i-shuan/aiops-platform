from aiops.commands.base import BaseCommand, CommandError, CommandNotFound
from aiops.domains.rca.dao import IncidentReadDAO
from aiops.domains.rca.schemas import GenerateRCARequest
from aiops.plugins.base import Incident
from aiops.plugins.registry import SectionRegistry
from aiops.presets import get_preset


class GenerateRCACommand(BaseCommand):
    """報告段落由 preset 決定，不寫死段數；資料不足的段落直接略過，不輸出空殼。"""

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

    def run(self) -> dict:
        self.validate()
        incident = Incident.from_dict(self.row["incident"])
        ctx = self.req.context
        sections, skipped = [], []
        for name in self.preset.rca_sections:
            builder = SectionRegistry.get(name)()
            if builder.applies(incident, ctx):
                sections.append(builder.build(incident, ctx).to_dict())
            else:
                skipped.append(name)
        return {"incident_id": self.req.incident_id, "preset": self.preset.name, "sections": sections, "skipped": skipped}
