"""欄位對照的審核工作流：提交 -> 核准（同來源舊版自動作廢）-> 查詢現行版。

資料源層只吃「已核准」的對照（引擎載入 ctx.field_maps），不管審核狀態。
同一份對照兩種標準欄：span.*／log.* 給 trace 與日誌（資料源 grafana 後面的 Elasticsearch 文件；蓋過資料模型 profile 的預設來源欄，
見 sources/es_models.py）。ts／value／entity 原本給 Elasticsearch 的指標查詢，那個查詢已隨直連 ES 移除（2026-11-20）：
目前還收、還存，但沒有任何資料源會讀。
"""
from __future__ import annotations

from aiops.engine.context import PipelineContext
from aiops.engine.errors import BadRequest, Conflict, NotFound
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import MappingApprove, MappingSubmit, SourceRef
from aiops.engine.operator import BaseOperator
from aiops.sources.es_models import STANDARD_FIELDS
from aiops.storage.repository import Mappings

# 指標用的標準欄（目前沒有資料源讀，見檔頭）＋ trace／日誌的標準欄（sources/es_models.STANDARD_FIELDS）
KNOWN_STANDARD_FIELDS = frozenset({"ts", "value", "entity", *STANDARD_FIELDS})


@OperatorRegistry.register("admin.mapping_submit")
class MappingSubmitOp(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not isinstance(ctx.request, MappingSubmit):
            raise BadRequest("admin.mapping_submit 需要 MappingSubmit 請求")
        unknown = sorted(set(ctx.request.fields.values()) - KNOWN_STANDARD_FIELDS)
        if unknown:
            # 標準欄打錯字以前會被資料源默默忽略（對照核准了卻沒有任何效果），在提交時就擋下來
            raise BadRequest(f"不認得的標準欄位：{'、'.join(unknown)}；可用：指標 ts／value／entity，"
                             f"trace／日誌 {'、'.join(sorted(STANDARD_FIELDS))}")
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
