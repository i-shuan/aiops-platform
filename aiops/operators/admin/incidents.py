"""事件群的手動結案與查詢。"""
from __future__ import annotations

from aiops.core.context import PipelineContext
from aiops.core.errors import BadRequest, Conflict, NotFound
from aiops.core.registry import OperatorRegistry
from aiops.core.requests import IdRef, UidRef
from aiops.core.types import Incident
from aiops.operators.alerts.payload import build_notifications
from aiops.operators.base import BaseOperator
from aiops.storage.repository import Incidents


@OperatorRegistry.register("admin.incident_get")
class IncidentGet(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not isinstance(ctx.request, IdRef):
            raise BadRequest("admin.incident_get 需要 IdRef 請求")
        row = Incidents.get(ctx.request.id)
        if row is None:
            raise NotFound(f"找不到事件群 {ctx.request.id}")
        ctx.result = {"incident": row}
        return ctx


@OperatorRegistry.register("admin.incident_resolve")
class IncidentResolve(BaseOperator):
    """人工結案（例如直接匯入、沒有排程心跳的事件群）：產生帶 endsAt 的恢復通知，由後面的 notifier 送出，
    送成功後引擎才把事件群標成 resolved（resolved_incident_ids）。送不出去就不會標成已結案。
    """

    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not isinstance(ctx.request, UidRef):
            raise BadRequest("admin.incident_resolve 需要 UidRef 請求")
        row = Incidents.get_by_uid(ctx.request.uid)
        if row is None:
            raise NotFound(f"找不到事件群 {ctx.request.uid}")
        if row["status"] != "open":
            raise Conflict(f"事件群 {ctx.request.uid} 狀態是 {row['status']}，只有 open 能結案")
        incident = Incident.from_dict(row["incident"], uid=row["uid"])
        ctx.notifications.extend(build_notifications(incident, row["preset"], row["severity"], ends_at=ctx.now))
        ctx.resolved_incident_ids.append(row["id"])
        ctx.result = {"uid": row["uid"], "resolved": True}
        return ctx
