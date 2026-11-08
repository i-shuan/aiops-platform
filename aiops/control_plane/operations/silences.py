"""靜音、維護窗口、人工排除時段：只有一份設定，在平台。接收端不另設靜音。"""
from __future__ import annotations

from aiops.engine.context import PipelineContext
from aiops.engine.errors import BadRequest
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import SilenceCreate
from aiops.engine.operator import BaseOperator
from aiops.storage.repository import Silences


@OperatorRegistry.register("admin.silence_create")
class SilenceCreateOp(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not isinstance(ctx.request, SilenceCreate):
            raise BadRequest("admin.silence_create 需要 SilenceCreate 請求")
        ctx.result = {"silence": Silences.create(ctx.request.model_dump())}
        return ctx
