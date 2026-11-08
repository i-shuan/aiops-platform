"""欄位對照的審核工作流：提交 -> 核准（同來源舊版自動作廢）-> 查詢現行版。

資料源層只吃「已核准」的對照（引擎載入 ctx.field_maps），不管審核狀態。
"""
from __future__ import annotations

from aiops.core.context import PipelineContext
from aiops.core.errors import BadRequest, Conflict, NotFound
from aiops.core.registry import OperatorRegistry
from aiops.core.requests import MappingApprove, MappingSubmit, SourceRef
from aiops.operators.base import BaseOperator
from aiops.storage.repository import Mappings


@OperatorRegistry.register("admin.mapping_submit")
class MappingSubmitOp(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not isinstance(ctx.request, MappingSubmit):
            raise BadRequest("admin.mapping_submit 需要 MappingSubmit 請求")
        ctx.result = {"mapping": Mappings.create({**ctx.request.model_dump(), "status": "pending", "approved_by": None})}
        return ctx


@OperatorRegistry.register("admin.mapping_approve")
class MappingApproveOp(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = ctx.request
        if not isinstance(req, MappingApprove):
            raise BadRequest("admin.mapping_approve 需要 MappingApprove 請求")
        row = Mappings.get(req.mapping_id)
        if row is None:
            raise NotFound(f"找不到欄位對照 {req.mapping_id}")
        if row["status"] != "pending":
            raise Conflict(f"欄位對照 {req.mapping_id} 狀態為 {row['status']}，只有 pending 能核准")
        for old in Mappings.list_by(row["source_type"], "approved"):
            Mappings.update(old["id"], status="superseded")
        ctx.result = {"mapping": Mappings.update(req.mapping_id, status="approved", approved_by=req.approver)}
        return ctx


@OperatorRegistry.register("admin.mapping_get_approved")
class MappingGetApproved(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not isinstance(ctx.request, SourceRef):
            raise BadRequest("admin.mapping_get_approved 需要 SourceRef 請求")
        rows = Mappings.list_by(ctx.request.source_type, "approved")
        if not rows:
            raise NotFound(f"{ctx.request.source_type} 沒有已核准的欄位對照")
        ctx.result = {"mapping": rows[0]}
        return ctx
