"""拓撲根因推論：在「異常傳播方向」的圖上，沒有上游異常的異常實體就是可能的源頭。

傳播方向由 propagation_edges() 統一：IT 是 callee -> caller，工廠是上游機台 -> 下游機台。
"""
from datetime import datetime

from aiops.plugins.base import BaseSectionBuilder, Incident, ReportSection, propagation_edges
from aiops.plugins.registry import SectionRegistry


def _first_seen(incident: Incident) -> dict[str, tuple[datetime, float]]:
    seen: dict[str, tuple[datetime, float]] = {}
    for e in incident.events:
        ts, score = seen.get(e.entity, (e.ts, e.score))
        seen[e.entity] = (min(ts, e.ts), max(score, e.score))
    return seen


def root_candidates(incident: Incident, edges: set[tuple[str, str]]) -> list[str]:
    """沒有「異常的上游」的異常實體，依最早出現、分數高低排序。全部互為上下游（環）時就全部列出。"""
    anomalous = set(incident.entities)
    seen = _first_seen(incident)
    roots = [e for e in anomalous if not any((src, e) in edges for src in anomalous if src != e)] or list(anomalous)
    return sorted(roots, key=lambda e: (seen[e][0], -seen[e][1]))


def _in_graph(entity: str, edges: set[tuple[str, str]]) -> bool:
    return any(entity in edge for edge in edges)


@SectionRegistry.register
class RootCauseCandidatesSection(BaseSectionBuilder):
    name = "root_cause_candidates"
    title = "可能的源頭"

    def applies(self, incident: Incident, context: dict) -> bool:
        edges = propagation_edges(context)
        return any(_in_graph(e, edges) for e in incident.entities)

    def build(self, incident: Incident, context: dict) -> ReportSection:
        edges = propagation_edges(context)
        seen = _first_seen(incident)
        lines = []
        for i, entity in enumerate(root_candidates(incident, edges), 1):
            ts, score = seen[entity]
            reason = "上游沒有其他異常" if _in_graph(entity, edges) else "不在拓撲中，無法判斷上下游"
            lines.append(f"{i}. {entity}：最早 {ts.isoformat()}，最高分數 {score:.2f}；{reason}")
        return ReportSection(self.name, self.title, "\n".join(lines))


@SectionRegistry.register
class PropagationPathSection(BaseSectionBuilder):
    name = "propagation_path"
    title = "傳播路徑"

    def applies(self, incident: Incident, context: dict) -> bool:
        anomalous = set(incident.entities)
        return any(a in anomalous and b in anomalous for a, b in propagation_edges(context))

    def build(self, incident: Incident, context: dict) -> ReportSection:
        edges = propagation_edges(context)
        anomalous = set(incident.entities)
        seen = _first_seen(incident)
        paths: list[list[str]] = []

        def walk(path: list[str]) -> None:
            nexts = sorted(b for a, b in edges if a == path[-1] and b in anomalous and b not in path)
            if not nexts or len(paths) >= 10:
                if len(path) > 1:
                    paths.append(path)
                return
            for b in nexts:
                walk(path + [b])

        for root in root_candidates(incident, edges):
            walk([root])
        lines = [" → ".join(f"{e}（{seen[e][0].strftime('%H:%M:%S')}）" for e in p) for p in paths]
        return ReportSection(self.name, self.title, "\n".join(lines))
