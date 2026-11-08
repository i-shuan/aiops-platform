from __future__ import annotations

import os

from aiops.core.context import PipelineContext
from aiops.core.errors import InfrastructureError
from aiops.core.registry import OperatorRegistry
from aiops.operators.base import BaseOperator
from aiops.operators.notifiers.transport import AlertmanagerTransport, LogTransport


@OperatorRegistry.register("notifier.log")
class LogNotifier(BaseOperator):
    """只寫日誌、不真的送。開發用。"""

    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        LogTransport().send(ctx.notifications)
        return ctx


@OperatorRegistry.register("notifier.auto")
class AutoNotifier(BaseOperator):
    """依環境變數 AIOPS_NOTIFIER 選擇出口：log（預設，只寫日誌）或 alertmanager。
    內建 workflow 用它，部署時才決定；要在 YAML 裡固定，就改用 notifier.log／notifier.alertmanager。"""

    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        kind = os.environ.get("AIOPS_NOTIFIER", "log").strip().lower() or "log"
        if kind == "log":
            return LogNotifier().execute(ctx)
        if kind == "alertmanager":
            return AlertmanagerNotifier().execute(ctx)
        raise InfrastructureError(f"未知的 AIOPS_NOTIFIER：{kind}（可用：log、alertmanager）")


@OperatorRegistry.register("notifier.alertmanager")
class AlertmanagerNotifier(BaseOperator):
    """把 ctx.notifications 推到 Alertmanager。params: endpoint（不給就用環境變數 AIOPS_ALERTMANAGER_URL）。
    token 只從環境變數 AIOPS_ALERTMANAGER_TOKEN 讀，不寫在 YAML。送不出去丟 InfrastructureError，整輪失敗、不會假裝成功。
    """

    scope = "batch"

    def __init__(self, endpoint: str | None = None):
        self.endpoint = endpoint

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        url = self.endpoint or os.environ.get("AIOPS_ALERTMANAGER_URL", "").strip()
        if not url:
            raise InfrastructureError("notifier.alertmanager 沒有 endpoint，也沒有設定 AIOPS_ALERTMANAGER_URL")
        AlertmanagerTransport(url, os.environ.get("AIOPS_ALERTMANAGER_TOKEN") or None).send(ctx.notifications)
        return ctx
