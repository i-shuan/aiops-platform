"""拓撲啟發式（預設）：在異常傳播圖上，「沒有異常上游」的異常實體就是源頭候選，依最早出現、分數高低排序。

可預期、好解釋；缺點是只看圖的結構與先後，不看時序形狀。全部互為上下游（環）時就全部列出。
"""
from __future__ import annotations

from aiops.core.context import RootCandidate
from aiops.core.registry import OperatorRegistry
from aiops.operators.rca.base import RootCauseOperator, first_seen, graph_edges, in_graph


@OperatorRegistry.register("rca.root_cause.topological")
class TopologicalRootCause(RootCauseOperator):
    def rank(self, incident, ctx):
        edges = graph_edges(ctx)
        if not any(in_graph(e, edges) for e in incident.entities):
            return []  # 沒有拓撲或 Lot 流向：判斷不了上下游
        anomalous = set(incident.entities)
        seen = first_seen(incident)
        roots = [e for e in anomalous if not any((src, e) in edges for src in anomalous if src != e)] or list(anomalous)
        ordered = sorted(roots, key=lambda e: (seen[e][0], -seen[e][1]))
        out = []
        for i, entity in enumerate(ordered):
            ts, score = seen[entity]
            why = "上游沒有其他異常" if in_graph(entity, edges) else "不在拓撲中，無法判斷上下游"
            out.append(RootCandidate(entity, float(len(ordered) - i),
                                     f"最早 {ts.isoformat()}，最高分數 {score:.2f}；{why}"))
        return out
