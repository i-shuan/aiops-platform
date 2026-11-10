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


# alert.fatal_floor 打在事件群上的標籤前綴：fatal:<偵測節點名稱>
FATAL_TAG = "fatal"


def tagged_with(incident: Incident, tags) -> list[str]:
    """事件群身上符合的標籤：完全相同、或是「<tag>:」開頭（fatal 會對到 fatal:fatal、fatal:overheat）。"""
    return [t for t in incident.tags if any(t == tag or t.startswith(f"{tag}:") for tag in tags)]
