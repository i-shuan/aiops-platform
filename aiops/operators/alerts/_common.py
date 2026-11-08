"""告警算子共用的小工具。純函式，不碰 DB。"""
from __future__ import annotations

from aiops.core.context import PipelineContext
from aiops.core.types import Incident


def ensure_incidents(ctx: PipelineContext) -> list[Incident]:
    """還沒有事件群時，每筆異常先自成一群。之後的收斂算子在這個基礎上合併。"""
    if not ctx.incidents and ctx.anomalies:
        ctx.incidents = [Incident([e], score=e.score) for e in ctx.anomalies]
    return ctx.incidents
