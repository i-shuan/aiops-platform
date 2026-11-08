from aiops.plugins.base import BaseScorer, Incident
from aiops.plugins.registry import ScorerRegistry


@ScorerRegistry.register
class P0BoostScorer(BaseScorer):
    """事件群牽涉 P0 實體時加分。context["p0_entities"]：實體名稱清單。params: boost（加分，預設 10）。"""

    name = "p0_boost"

    def score(self, incident: Incident, context: dict) -> float:
        p0 = set(context.get("p0_entities", []))
        return incident.score + float(self.params.get("boost", 10)) if p0 & set(incident.entities) else incident.score
