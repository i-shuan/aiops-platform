from aiops.plugins.base import BaseGrouper, Incident, gap_seconds, merge_incidents
from aiops.plugins.registry import GrouperRegistry


@GrouperRegistry.register
class TopologyChainGrouper(BaseGrouper):
    """拓撲上相鄰（呼叫關係）且時間相近的異常併成一條鏈。

    context["topology"]：邊的清單，例如 [["frontend", "cart"], ["cart", "redis"]]。
    params: window_minutes（預設 10）。
    """

    name = "topology_chain"

    def group(self, incidents: list[Incident], context: dict) -> list[Incident]:
        edges = {frozenset(e) for e in context.get("topology", []) if len(set(e)) == 2}
        if not edges:
            return incidents
        window = float(self.params.get("window_minutes", 10)) * 60

        def linked(a: Incident, b: Incident) -> bool:
            adjacent = any(frozenset((x, y)) in edges for x in a.entities for y in b.entities)
            return adjacent and gap_seconds(a, b) <= window

        return merge_incidents(incidents, linked, tag="topology_chain")
