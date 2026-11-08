"""拓撲啟發式（預設）：在異常傳播圖上，「沒有異常上游」的異常實體就是源頭候選，依最早出現、分數高低排序。

可預期、好解釋；缺點是只看圖的結構與先後，不看時序形狀。全部互為上下游（環）時就全部列出。
"""
from __future__ import annotations

from aiops.engine.context import RootCandidate
from aiops.engine.registry import OperatorRegistry
from aiops.stages.rca.base import RootCauseOperator, first_seen, graph_edges, in_graph


def onset_text(ts, earliest) -> str:
    """實體自己的首次異常時間，並對照整個事件群最早的時間（避免「最早」被讀成全場最早）。"""
    lag = round((ts - earliest).total_seconds() / 60, 1)
    if lag <= 0:
        return f"首次異常 {ts.isoformat()}（事件群中最早）"
    return f"首次異常 {ts.isoformat()}（事件群最早是 {earliest.isoformat()}，晚 {lag:g} 分鐘）"


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
        earliest = min(ts for ts, _ in seen.values())
        out = []
        for i, entity in enumerate(ordered):
            ts, score = seen[entity]
            why = "上游沒有其他異常" if in_graph(entity, edges) else "不在拓撲中，無法判斷上下游"
            out.append(RootCandidate(entity, float(len(ordered) - i),
                                     f"{onset_text(ts, earliest)}，最高分數 {score:.2f}；{why}"))
        return out
