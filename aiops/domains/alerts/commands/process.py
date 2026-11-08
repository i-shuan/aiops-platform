from aiops.commands.base import BaseCommand
from aiops.domains.alerts.commands.pipeline import AlertPipeline, check_preset
from aiops.domains.alerts.schemas import ProcessAlertsRequest
from aiops.plugins.base import AnomalyEvent
from aiops.presets import get_preset


class ProcessAlertsCommand(BaseCommand):
    """直接收一批異常事件（例如外部系統推來的）跑告警管線。"""

    def __init__(self, req: ProcessAlertsRequest):
        self.req = req

    def validate(self) -> None:
        self.preset = get_preset(self.req.preset)
        check_preset(self.preset)

    def run(self) -> list[dict]:
        self.validate()
        events = [AnomalyEvent(**ev.model_dump()) for ev in self.req.events]
        return AlertPipeline(self.preset, self.req.context, self.req.plugin_params).run(events)
