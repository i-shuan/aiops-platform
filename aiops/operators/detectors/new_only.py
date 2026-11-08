from aiops.core.context import PipelineContext
from aiops.core.registry import OperatorRegistry
from aiops.core.types import as_utc
from aiops.operators.base import BaseOperator


@OperatorRegistry.register("detector.new_only")
class NewOnly(BaseOperator):
    """只留比「這條序列最後一筆已存的異常」更新的異常。重跑同一段時間不會重複告警、重複寫入。"""

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if ctx.last_anomaly_ts is not None:
            cutoff = as_utc(ctx.last_anomaly_ts)
            ctx.anomalies = [a for a in ctx.anomalies if as_utc(a.ts) > cutoff]
        return ctx
