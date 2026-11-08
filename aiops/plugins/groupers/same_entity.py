from aiops.plugins.base import BaseGrouper, Incident, gap_seconds, merge_incidents
from aiops.plugins.registry import GrouperRegistry


@GrouperRegistry.register
class SameEntityGrouper(BaseGrouper):
    """同一實體、時間相近的異常併成一群。params: window_minutes（預設 10）。"""

    name = "same_entity"

    def group(self, incidents: list[Incident], context: dict) -> list[Incident]:
        window = float(self.params.get("window_minutes", 10)) * 60
        return merge_incidents(
            incidents,
            lambda a, b: bool(set(a.entities) & set(b.entities)) and gap_seconds(a, b) <= window,
            tag="same_entity",
        )
