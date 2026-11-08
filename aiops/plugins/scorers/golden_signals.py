from aiops.plugins.base import GOLDEN_SIGNALS, BaseScorer, Incident
from aiops.plugins.registry import ScorerRegistry


@ScorerRegistry.register
class GoldenSignalsScorer(BaseScorer):
    """事件群含黃金訊號（latency／errors／traffic／saturation）時加權。params: boost（倍數，預設 1.5）。"""

    name = "golden_signals"

    def score(self, incident: Incident, context: dict) -> float:
        signals = set(self.params.get("signals", GOLDEN_SIGNALS))
        hit = any(e.metric in signals or e.attrs.get("signal") in signals for e in incident.events)
        return incident.score * float(self.params.get("boost", 1.5)) if hit else incident.score
