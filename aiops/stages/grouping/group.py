"""事件收斂：把零散的異常合成「一件事」。輸入 ctx.anomalies，輸出 ctx.incidents。"""
from __future__ import annotations

from aiops.engine.context import PipelineContext
from aiops.engine.params import number, positive, whole
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import gap_seconds, merge_incidents
from aiops.stages._common import ensure_incidents
from aiops.engine.operator import BaseOperator


@OperatorRegistry.register("alert.same_entity")
class SameEntityGrouper(BaseOperator):
    """同一實體、時間相近的異常併成一群。params: window_minutes。"""

    scope = "batch"

    def __init__(self, window_minutes: float = 10):
        self.window = positive("window_minutes", window_minutes) * 60

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
        self.window = positive("window_minutes", window_minutes) * 60

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


@OperatorRegistry.register("alert.messaging_link")
class MessagingLinkGrouper(BaseOperator):
    """IT：訊息佇列的生產者先出事、消費者晚一點才出事，併成一件事。非同步邊來自 ctx.async_edges
    （(生產者, 消費者) -> 延遲 P95 毫秒；admin.topology_refresh 從 trace 沉澱）。
    params: min_window_minutes（預設 10）、max_window_minutes（預設 120）、lag_factor（預設 1.5）。

    跟 alert.topology_chain 的差別：
    - 有方向：生產者那一群的起點不能晚於消費者那一群（消費者先出事、生產者後出事不是「訊息帶過去的」，不併）
    - 時間窗跟著這條邊的延遲走：兩群的間隔 <= clamp(延遲 P95 × lag_factor, min_window, max_window)。
      同步呼叫幾秒內就傳到，固定 10 分鐘夠用；佇列積壓時消費者可能晚幾十分鐘才處理到那批訊息。

    邊界（照實寫，不假裝做得到）：
    - 只併「這一輪的事件群」。生產者那一群已經在進行中（上一輪就開了）而且這一輪還有異常時，併出來的群跟進行中的群
      有共同實體，後面的 alert.absorb_open 會照既有規則吸收進同一個 incident_uid；生產者這一輪已經安靜了，
      消費者這一輪才出事，這裡併不到（absorb_open 只認共同實體，不認非同步邊）——兩張卡，靠 RCA 的訊息佇列段落指回生產者。
    - 延遲是最近一次拓撲刷新看到的值：積壓才剛開始、還沒被刷新到的時候，窗口只有 min_window。
    - 沒有 ctx.async_edges（沒接 trace、或 workflow 自己指定了拓撲）就什麼都不做。
    一定要放在計分之前（併群取最高分，計分後再併會讓加權失真），跟其他收斂算子一樣。
    """

    scope = "batch"

    def __init__(self, min_window_minutes: float = 10, max_window_minutes: float = 120, lag_factor: float = 1.5):
        for key, value in (("min_window_minutes", min_window_minutes), ("max_window_minutes", max_window_minutes),
                           ("lag_factor", lag_factor)):
            number(key, value)
        if min_window_minutes < 0 or max_window_minutes < min_window_minutes or lag_factor <= 0:
            raise ValueError("min_window_minutes 不能小於 0；max_window_minutes 要大於等於 min_window_minutes；lag_factor 必須大於 0")
        self.min_window, self.max_window = float(min_window_minutes) * 60, float(max_window_minutes) * 60
        self.lag_factor = float(lag_factor)

    def _window(self, lag_ms: float) -> float:
        return min(max(lag_ms / 1000.0 * self.lag_factor, self.min_window), self.max_window)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        incidents = ensure_incidents(ctx)
        edges = {(p, c): lag for (p, c), lag in ctx.async_edges.items() if p != c}
        if not edges:
            return ctx

        def carried(early, late) -> bool:
            """early 裡的生產者發的訊息，把問題帶給 late 裡的消費者。"""
            if early.start > late.start:
                return False
            windows = [self._window(lag) for (p, c), lag in edges.items() if p in early.entities and c in late.entities]
            return bool(windows) and gap_seconds(early, late) <= max(windows)

        ctx.incidents = merge_incidents(incidents, lambda a, b: carried(a, b) or carried(b, a), tag="messaging_link")
        return ctx


@OperatorRegistry.register("alert.lot_path_chain")
class LotPathChainGrouper(BaseOperator):
    """工廠：Lot 流經的上下游機台在相近時間都出事，併成一條鏈。流向來自 ctx.lot_edges。
    params: window_minutes（工廠的傳遞比 IT 慢，預設 60）、min_lots（經過的 Lot 太少的流向不算）。
    """

    scope = "batch"

    def __init__(self, window_minutes: float = 60, min_lots: int = 1):
        self.window = positive("window_minutes", window_minutes) * 60
        self.min_lots = whole("min_lots", min_lots)

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
