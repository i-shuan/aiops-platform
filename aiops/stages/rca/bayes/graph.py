"""傳播圖的結構：強連通分量縮點（迭代式 Tarjan，不遞迴）＋縮點圖上的跳數。純函式、確定性。

邊沿用 graph_edges(ctx) 的方向：(src, dst) = 異常從 src 傳到 dst（IT：被呼叫端 → 呼叫端）。
循環呼叫（A↔B）縮成一個分量後，縮點圖一定是 DAG，BFS 走得完、不會來回甩鍋；
分量內「誰先傳給誰」沒有意義，一律用同一個係數（priors.INTRA_SCC_FACTOR），由各自身上的證據決定誰是源頭。
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


def strongly_connected(nodes: list[str], edges: set[tuple[str, str]]) -> dict[str, int]:
    """迭代式 Tarjan。回傳 節點 → 分量編號；編號依分量內最小的節點名排序後重編，跟走訪順序無關（確定性）。"""
    succ: dict[str, list[str]] = {n: [] for n in nodes}
    for a, b in sorted(edges):
        if a in succ and b in succ and a != b:
            succ[a].append(b)
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    raw: dict[str, int] = {}
    counter = 0
    n_comp = 0
    for start in nodes:
        if start in index:
            continue
        work: list[tuple[str, int]] = [(start, 0)]  # （節點, 下一個要看的後繼 index）
        while work:
            v, i = work.pop()
            if i == 0:
                index[v] = low[v] = counter
                counter += 1
                stack.append(v)
                on_stack.add(v)
            if i < len(succ[v]):
                work.append((v, i + 1))
                w = succ[v][i]
                if w not in index:
                    work.append((w, 0))
                elif w in on_stack:
                    low[v] = min(low[v], index[w])
                continue
            if low[v] == index[v]:
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    raw[w] = n_comp
                    if w == v:
                        break
                n_comp += 1
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[v])
    first = {}
    for n in sorted(nodes):
        first.setdefault(raw[n], n)
    order = {c: i for i, c in enumerate(sorted(first, key=lambda c: first[c]))}
    return {n: order[raw[n]] for n in nodes}


@dataclass
class Graph:
    nodes: list[str]
    edges: set[tuple[str, str]]
    comp: dict[str, int] = field(default_factory=dict)
    callees: dict[str, list[str]] = field(default_factory=dict)  # X 依賴誰（誰出事會傳到 X）
    callers: dict[str, list[str]] = field(default_factory=dict)  # 誰依賴 X
    _dist: dict[int, dict[int, int]] = field(default_factory=dict)

    @classmethod
    def build(cls, nodes: list[str], edges: set[tuple[str, str]]) -> "Graph":
        nodes = sorted(set(nodes))
        inner = {(a, b) for a, b in edges if a in nodes and b in nodes and a != b}
        g = cls(nodes, inner, strongly_connected(nodes, inner))
        g.callees = {n: sorted(a for a, b in inner if b == n) for n in nodes}
        g.callers = {n: sorted(b for a, b in inner if a == n) for n in nodes}
        cedges: dict[int, set[int]] = {c: set() for c in set(g.comp.values())}
        for a, b in inner:
            if g.comp[a] != g.comp[b]:
                cedges[g.comp[a]].add(g.comp[b])
        for c in sorted(cedges):  # 每個分量 BFS 一次：縮點圖是 DAG，visited 保證每個分量只進佇列一次
            dist = {c: 0}
            queue = deque([c])
            while queue:
                x = queue.popleft()
                for y in sorted(cedges[x]):
                    if y not in dist:
                        dist[y] = dist[x] + 1
                        queue.append(y)
            g._dist[c] = dist
        return g

    def hops(self, root: str, entity: str) -> int | None:
        """根因傳到 entity 要幾跳（縮點圖上）。0 = 同一個分量（但不是自己）；None = 傳不到。"""
        if root == entity or root not in self.comp or entity not in self.comp:
            return None
        return self._dist[self.comp[root]].get(self.comp[entity])

    def same_cycle(self, a: str, b: str) -> bool:
        return a != b and a in self.comp and b in self.comp and self.comp[a] == self.comp[b]

    def reaches(self, root: str, entity: str) -> bool:
        return root == entity or self.hops(root, entity) is not None

    def cycles(self) -> list[list[str]]:
        groups: dict[int, list[str]] = {}
        for n in self.nodes:
            groups.setdefault(self.comp[n], []).append(n)
        return [sorted(v) for _, v in sorted(groups.items()) if len(v) > 1]
