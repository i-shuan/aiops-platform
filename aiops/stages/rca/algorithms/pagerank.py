"""PageRank（Personalized，反向傳播圖）：把「異常從 A 傳到 B」的邊反過來（B 指回 A），
讓分數從受影響的下游往上游流，最後分數集中在「被很多異常實體共同指向」的源頭。

起始權重（personalization）用各實體的最高異常分數，所以本身很異常、又在上游的實體排最前面。
跟拓撲啟發式的差別：多個上游同時異常時，它會依「拖累了多少下游、多嚴重」排出先後，而不只是看誰先出現。
純 Python 冪次迭代，不依賴 networkx。
"""
from __future__ import annotations

from aiops.engine.context import RootCandidate
from aiops.engine.registry import OperatorRegistry
from aiops.stages.rca.base import RootCauseOperator, first_seen, graph_edges, in_graph
from aiops.stages.rca.algorithms.topological import onset_text


def personalized_pagerank(nodes: list[str], edges: set[tuple[str, str]], weights: dict[str, float],
                          damping: float = 0.85, iterations: int = 100, tol: float = 1e-10) -> dict[str, float]:
    """edges 是 (from, to)：分數沿邊從 from 流到 to。沒有出邊的節點把分數依 personalization 重新分配。"""
    total = sum(weights.values()) or 1.0
    p = {n: weights.get(n, 0.0) / total for n in nodes}
    out: dict[str, list[str]] = {n: [] for n in nodes}
    for a, b in edges:
        if a in out and b in out and a != b:
            out[a].append(b)
    rank = dict(p)
    for _ in range(iterations):
        dangling = sum(rank[n] for n in nodes if not out[n])
        nxt = {n: (1 - damping) * p[n] + damping * dangling * p[n] for n in nodes}
        for a in nodes:
            if out[a]:
                share = damping * rank[a] / len(out[a])
                for b in out[a]:
                    nxt[b] += share
        delta = sum(abs(nxt[n] - rank[n]) for n in nodes)
        rank = nxt
        if delta < tol:
            break
    return rank


@OperatorRegistry.register("rca.root_cause.pagerank")
class PageRankRootCause(RootCauseOperator):
    """params: damping（0~1，預設 0.85）、iterations。"""

    def __init__(self, damping: float = 0.85, iterations: int = 100):
        if not 0 < damping < 1 or iterations < 1:
            raise ValueError("damping 必須在 0 到 1 之間（不含）；iterations 至少 1")
        self.damping, self.iterations = float(damping), int(iterations)

    def rank(self, incident, ctx):
        edges = graph_edges(ctx)
        if not any(in_graph(e, edges) for e in incident.entities):
            return []
        anomalous = sorted(set(incident.entities))
        seen = first_seen(incident)
        inner = {(a, b) for a, b in edges if a in anomalous and b in anomalous and a != b}
        reversed_edges = {(b, a) for a, b in inner}  # 受影響者 -> 源頭
        weights = {e: max(seen[e][1], 1e-6) for e in anomalous}
        scores = personalized_pagerank(anomalous, reversed_edges, weights, self.damping, self.iterations)
        downstream = {e: sum(1 for a, _ in inner if a == e) for e in anomalous}
        ordered = sorted(anomalous, key=lambda e: (-scores[e], seen[e][0]))
        earliest = min(ts for ts, _ in seen.values())
        return [RootCandidate(e, scores[e], f"PageRank {scores[e]:.3f}；直接影響 {downstream[e]} 個異常實體；"
                                            f"{onset_text(seen[e][0], earliest)}，最高分數 {seen[e][1]:.2f}")
                for e in ordered]
