from aiops.plugins.base import GOLDEN_SIGNALS, BaseSectionBuilder, Incident, ReportSection
from aiops.plugins.registry import SectionRegistry


def _golden_events(incident: Incident):
    return [e for e in incident.events if e.metric in GOLDEN_SIGNALS or e.attrs.get("signal") in GOLDEN_SIGNALS]


@SectionRegistry.register
class GoldenSignalsSection(BaseSectionBuilder):
    """IT 場景專用：只有事件群真的含黃金訊號時才產生。"""

    name = "golden_signals"
    title = "黃金訊號"

    def applies(self, incident: Incident, context: dict) -> bool:
        return bool(_golden_events(incident))

    def build(self, incident: Incident, context: dict) -> ReportSection:
        lines = [f"{e.entity} {e.attrs.get('signal', e.metric)} score={e.score:.2f}" for e in _golden_events(incident)]
        return ReportSection(self.name, self.title, "\n".join(lines))
