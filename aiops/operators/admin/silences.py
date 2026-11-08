"""靜音、維護窗口、人工排除時段：只有一份設定，在平台。接收端不另設靜音。"""
from __future__ import annotations

from aiops.core.context import PipelineContext
from aiops.core.errors import BadRequest
from aiops.core.registry import OperatorRegistry
from aiops.core.requests import SilenceCreate
from aiops.operators.base import BaseOperator
from aiops.storage.repository import Silences


@OperatorRegistry.register("admin.silence_create")
class SilenceCreateOp(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not isinstance(ctx.request, SilenceCreate):
            raise BadRequest("admin.silence_create 需要 SilenceCreate 請求")
        ctx.result = {"silence": Silences.create(ctx.request.model_dump())}
        return ctx
