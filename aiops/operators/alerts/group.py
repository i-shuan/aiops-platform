"""事件收斂：把零散的異常合成「一件事」。輸入 ctx.anomalies，輸出 ctx.incidents。"""
from __future__ import annotations

from aiops.core.context import PipelineContext
from aiops.core.registry import OperatorRegistry
from aiops.core.types import gap_seconds, merge_incidents
from aiops.operators.alerts._common import ensure_incidents
from aiops.operators.base import BaseOperator


@OperatorRegistry.register("alert.same_entity")
class SameEntityGrouper(BaseOperator):
    """同一實體、時間相近的異常併成一群。params: window_minutes。"""

    scope = "batch"

    def __init__(self, window_minutes: float = 10):
        self.window = float(window_minutes) * 60

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        ctx.incidents = merge_incidents(
            ensure_incidents(ctx),
            lambda a, b: bool(set(a.entities) & set(b.entities)) and gap_seconds(a, b) <= self.window,
            tag="same_entity",
        )
        return ctx


@OperatorRegistry.register("alert.topology_chain")
class TopologyChainGrouper(BaseOperator):
    """IT：服務呼叫關係上相鄰、時間相近的異常併成一條鏈。拓撲來自 ctx.topology。params: window_minutes。"""

    scope = "batch"

    def __init__(self, window_minutes: float = 10):
        self.window = float(window_minutes) * 60

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        incidents = ensure_incidents(ctx)
        edges = {frozenset(e) for e in ctx.topology if e[0] != e[1]}
        if not edges:
            return ctx

        def linked(a, b) -> bool:
            adjacent = any(frozenset((x, y)) in edges for x in a.entities for y in b.entities)
            return adjacent and gap_seconds(a, b) <= self.window

        ctx.incidents = merge_incidents(incidents, linked, tag="topology_chain")
        return ctx


@OperatorRegistry.register("alert.lot_path_chain")
class LotPathChainGrouper(BaseOperator):
    """工廠：Lot 流經的上下游機台在相近時間都出事，併成一條鏈。流向來自 ctx.lot_edges。
    params: window_minutes（工廠的傳遞比 IT 慢，預設 60）、min_lots（經過的 Lot 太少的流向不算）。
    """

    scope = "batch"

    def __init__(self, window_minutes: float = 60, min_lots: int = 1):
        self.window = float(window_minutes) * 60
        self.min_lots = int(min_lots)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        incidents = ensure_incidents(ctx)
        edges = {frozenset((e.upstream, e.downstream)) for e in ctx.lot_edges
                 if e.upstream != e.downstream and e.lots >= self.min_lots}
        if not edges:
            return ctx

        def linked(a, b) -> bool:
            adjacent = any(frozenset((x, y)) in edges for x in a.entities for y in b.entities)
            return adjacent and gap_seconds(a, b) <= self.window

        ctx.incidents = merge_incidents(incidents, linked, tag="lot_path_chain")
        return ctx
