from aiops.plugins.base import BaseGrouper, Incident, gap_seconds, merge_incidents
from aiops.plugins.registry import GrouperRegistry


@GrouperRegistry.register
class LotPathChainGrouper(BaseGrouper):
    """工廠：Lot 流經的上下游機台在相近時間都出事，併成一條鏈（跟 IT 的 topology_chain 同一個概念）。

    context["lot_edges"]：MergeLotPathsCommand 輸出的 edges，[{"from", "to", "lots"}]。
    params: window_minutes（預設 60，工廠的傳遞比 IT 慢）、min_lots（預設 1，經過 Lot 太少的流向不算）。
    """

    name = "lot_path_chain"

    def group(self, incidents: list[Incident], context: dict) -> list[Incident]:
        min_lots = int(self.params.get("min_lots", 1))
        edges = {
            frozenset((e["from"], e["to"])) for e in context.get("lot_edges", [])
            if e.get("from") != e.get("to") and int(e.get("lots", 1)) >= min_lots
        }
        if not edges:
            return incidents
        window = float(self.params.get("window_minutes", 60)) * 60

        def linked(a: Incident, b: Incident) -> bool:
            adjacent = any(frozenset((x, y)) in edges for x in a.entities for y in b.entities)
            return adjacent and gap_seconds(a, b) <= window

        return merge_incidents(incidents, linked, tag="lot_path_chain")
