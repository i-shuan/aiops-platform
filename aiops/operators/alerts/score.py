"""事件群計分。每個算子回傳調整後的分數，直接寫回 incident.score。"""
from __future__ import annotations

from aiops.core.context import PipelineContext
from aiops.core.registry import OperatorRegistry
from aiops.operators.alerts._common import ensure_incidents
from aiops.operators.base import BaseOperator

GOLDEN_SIGNALS = ("latency", "errors", "traffic", "saturation")


@OperatorRegistry.register("alert.golden_signals")
class GoldenSignalsScorer(BaseOperator):
    """事件群含黃金訊號（latency／errors／traffic／saturation）時加權。params: boost（倍數）、signals。"""

    scope = "batch"

    def __init__(self, boost: float = 1.5, signals: list[str] | None = None):
        self.boost = float(boost)
        self.signals = set(signals or GOLDEN_SIGNALS)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        for inc in ensure_incidents(ctx):
            hit = any(e.metric in self.signals or e.attrs.get("signal") in self.signals for e in inc.events)
            if hit:
                inc.score *= self.boost
        return ctx


@OperatorRegistry.register("alert.p0_boost")
class P0BoostScorer(BaseOperator):
    """事件群牽涉 P0 實體時加分。params: entities（P0 實體名稱）、boost（加分）。"""

    scope = "batch"

    def __init__(self, entities: list[str] | None = None, boost: float = 10):
        self.entities = set(entities or [])
        self.boost = float(boost)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        for inc in ensure_incidents(ctx):
            if self.entities & set(inc.entities):
                inc.score += self.boost
        return ctx
