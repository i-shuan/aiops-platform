"""拓撲方向：把 IT 的服務呼叫關係與工廠的 Lot 流向，統一成「異常會從 A 傳到 B」的有向邊。純函式。"""
from __future__ import annotations

from aiops.engine.context import LotEdge


def lot_edges_from_paths(paths: list[list[str]]) -> list[LotEdge]:
    """Lot 路徑 -> 機台流向：每條相鄰的 (上游, 下游) 被幾個不同的 Lot 走過。"""
    lots_per_edge: dict[tuple[str, str], int] = {}
    for path in paths:
        for edge in {(a, b) for a, b in zip(path, path[1:]) if a != b}:
            lots_per_edge[edge] = lots_per_edge.get(edge, 0) + 1
    return [LotEdge(a, b, n) for (a, b), n in sorted(lots_per_edge.items(), key=lambda kv: (-kv[1], kv[0]))]


def propagation_edges(topology: list[tuple[str, str]], lot_edges: list[LotEdge], min_lots: int = 1) -> set[tuple[str, str]]:
    """IT 的 topology 是 (caller, callee)：callee（被呼叫的依賴）出事會拖垮 caller，所以傳播方向是 callee -> caller。
    工廠的 lot_edges 是 上游 -> 下游：上游機台出事會流到下游，傳播方向是 上游 -> 下游。
    兩者方向相反，所以 Lot 流向絕不能塞進 topology。
    """
    edges = {(callee, caller) for caller, callee in topology if caller != callee}
    edges |= {(e.upstream, e.downstream) for e in lot_edges if e.upstream != e.downstream and e.lots >= min_lots}
    return edges
