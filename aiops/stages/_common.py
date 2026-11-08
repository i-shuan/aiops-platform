"""告警算子共用的小工具。純函式，不碰 DB。"""
from __future__ import annotations

from aiops.engine.context import PipelineContext
from aiops.engine.types import Incident


def ensure_incidents(ctx: PipelineContext) -> list[Incident]:
    """還沒有事件群時，每筆異常先自成一群。之後的收斂算子在這個基礎上合併。
    只建一次：之後就算清單變空（例如全部被 alert.absorb_open 併進進行中的事件群），也不能再從 anomalies 重建，
    否則下游每一筆異常都會變成一張新卡。"""
    if not ctx.incidents_built:
        if not ctx.incidents and ctx.anomalies:
            ctx.incidents = [Incident([e], score=e.score) for e in ctx.anomalies]
        ctx.incidents_built = True
    return ctx.incidents
