"""根因段落：可能的源頭、傳播路徑。

這兩段只負責「呈現」：根因怎麼排序由前面的 rca.root_cause.* 節點決定（ctx.rca_root_candidates），
換演算法不必改這裡。沒有根因節點、或演算法因資料不足沒給候選時，這兩段略過。
"""
from __future__ import annotations

from aiops.engine.registry import OperatorRegistry
from aiops.stages.rca.base import RcaSection, first_seen, graph_edges

MAX_PATHS = 10


@OperatorRegistry.register("rca.section.root_cause_candidates")
class RootCauseCandidates(RcaSection):
    key, title = "root_cause_candidates", "可能的源頭"

    def applies(self, incident, ctx):
        # 演算法明確說「證據不足」時也要出這一段：讓值班的人知道不是沒算，而是算了但不能指認
        return bool(ctx.rca_root_candidates or ctx.rca_root_note)

    def build(self, incident, ctx):
        lines = [f"{i}. {c.entity}：{c.reason}" for i, c in enumerate(ctx.rca_root_candidates, 1)]
        if ctx.rca_root_note:
            lines.append(ctx.rca_root_note)
        algorithm = ctx.rca_root_algorithm.removeprefix("rca.root_cause.")
        return "\n".join(lines) + f"\n（排序方法：{algorithm}）"


@OperatorRegistry.register("rca.section.propagation_path")
class PropagationPath(RcaSection):
    """從候選根因出發、沿著異常實體之間的傳播邊走出的路徑。需要拓撲或 Lot 流向。"""

    key, title = "propagation_path", "傳播路徑"

    def applies(self, incident, ctx):
        anomalous = set(incident.entities)
        return any(a in anomalous and b in anomalous for a, b in graph_edges(ctx))

    def build(self, incident, ctx):
        edges = graph_edges(ctx)
        anomalous = set(incident.entities)
        seen = first_seen(incident)
        paths: list[list[str]] = []

        def walk(path: list[str]) -> None:
            nexts = sorted(b for a, b in edges if a == path[-1] and b in anomalous and b not in path)
            if not nexts or len(paths) >= MAX_PATHS:
                if len(path) > 1:
                    paths.append(path)
                return
            for b in nexts:
                walk(path + [b])

        # 起點：根因候選的順序；沒有候選時用「沒有異常上游」的實體
        roots = [c.entity for c in ctx.rca_root_candidates if c.entity in anomalous]
        if not roots:
            roots = sorted(e for e in anomalous if not any((s, e) in edges for s in anomalous if s != e))
        for root in roots:
            walk([root])
        return "\n".join(" → ".join(f"{e}（{seen[e][0].strftime('%H:%M:%S')}）" for e in p) for p in paths)
