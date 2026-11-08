from aiops.commands.base import BaseCommand, CommandError
from aiops.domains.alerts.dao import IncidentDAO
from aiops.domains.alerts.schemas import ProcessAlertsRequest
from aiops.plugins.base import AnomalyEvent, Incident
from aiops.plugins.registry import GrouperRegistry, ScorerRegistry
from aiops.presets import get_preset


class ProcessAlertsCommand(BaseCommand):
    """收斂管線：每筆異常先自成一群 -> 依序跑 preset 的 Grouper -> 依序跑 Scorer -> 存檔。

    流程固定，零件由 preset 決定；工廠、IT、純指標共用同一條管線。
    """

    def __init__(self, req: ProcessAlertsRequest):
        self.req = req

    def validate(self) -> None:
        self.preset = get_preset(self.req.preset)
        missing = [g for g in self.preset.groupers if not GrouperRegistry.has(g)]
        missing += [s for s in self.preset.scorers if not ScorerRegistry.has(s)]
        if missing:
            raise CommandError(f"preset {self.preset.name} 引用了未註冊的外掛: {missing}")

    def _params(self, name: str) -> dict:
        return self.req.plugin_params.get(name, {})

    def run(self) -> list[dict]:
        self.validate()
        ctx = self.req.context
        incidents = [Incident([e], score=e.score) for e in (AnomalyEvent(**ev.model_dump()) for ev in self.req.events)]

        for name in self.preset.groupers:
            incidents = GrouperRegistry.get(name)(self._params(name)).group(incidents, ctx)

        scorers = [ScorerRegistry.get(name)(self._params(name)) for name in self.preset.scorers]
        for inc in incidents:
            for scorer in scorers:
                inc.score = scorer.score(inc, ctx)

        incidents.sort(key=lambda i: i.score, reverse=True)
        saved = []
        for inc in incidents:
            row = IncidentDAO.create(self.preset.name, inc.to_dict())
            saved.append({"id": row["id"], **row["incident"]})
        return saved
