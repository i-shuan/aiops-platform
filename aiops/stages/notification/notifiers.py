from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

from aiops.engine.context import PipelineContext
from aiops.engine.errors import InfrastructureError
from aiops.engine.registry import OperatorRegistry
from aiops.engine.operator import BaseOperator
from aiops.engine.types import Notification
from aiops.stages.notification.transport import AlertmanagerTransport, LogTransport, logger


@OperatorRegistry.register("notifier.log")
class LogNotifier(BaseOperator):
    """只寫日誌、不真的送。開發用。"""

    scope = "after_persist"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        LogTransport().send(ctx.notifications)
        return ctx


@OperatorRegistry.register("notifier.auto")
class AutoNotifier(BaseOperator):
    """依環境變數 AIOPS_NOTIFIER 選擇出口：log（預設，只寫日誌）或 alertmanager。
    內建 workflow 用它，部署時才決定；要在 YAML 裡固定，就改用 notifier.log／notifier.alertmanager。

    所有 notifier 都在 after_persist：事件群先寫進資料庫、通知才送出。送不出去時事件群已經是「進行中」，
    下一輪 alert.heartbeat 會用同一組標籤補送；通知紀錄與結案要等送成功（hooks.finalize_batch）才寫。
    只有 notifier.watchdog 更晚（after_finalize）：連結案都寫成功了才續命。"""

    scope = "after_persist"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        kind = os.environ.get("AIOPS_NOTIFIER", "log").strip().lower() or "log"
        if kind == "log":
            return LogNotifier().execute(ctx)
        if kind == "alertmanager":
            return AlertmanagerNotifier().execute(ctx)
        raise InfrastructureError(f"未知的 AIOPS_NOTIFIER：{kind}（可用：log、alertmanager）")


WATCHDOG_ALERTNAME = "AIOpsWatchdog"


@OperatorRegistry.register("notifier.watchdog")
class WatchdogNotifier(BaseOperator):
    """Dead Man's Snitch：每輪成功跑完（寫入、通知、結案紀錄都成功）就送一則「我還活著」的心跳告警，endsAt = 現在 + ttl。

    排程停了、Pod 掛了、資料源或 DB 連不上讓整輪失敗 -> 沒有人再續命，ttl 一到 Alertmanager 把它標成 resolved；
    在 Alertmanager 給 alertname=AIOpsWatchdog 一條 send_resolved: true 的路由（或接 Healthchecks 這類 DMS 服務），
    「心跳恢復通知」就等於「AIOps 自己掛了」。設定方式見 README「自我健康心跳」。

    params: ttl_minutes（預設 15；設成排程間隔的 2～3 倍，偶爾慢一輪不會誤報）。
    出口跟 notifier.auto 一樣看 AIOPS_NOTIFIER：log 只寫日誌（不經過 LogTransport，不會混進事件通知）。
    時間用牆上時鐘而不是 ctx.now：心跳是「程式現在活著」，回放舊資料時也一樣。
    """

    scope = "after_finalize"  # 比其他 notifier 更晚：finalize_batch（已通知、結案）寫失敗這一輪也不算活著

    def __init__(self, ttl_minutes: float = 15):
        if ttl_minutes <= 0:
            raise ValueError("ttl_minutes 必須大於 0")
        self.ttl = timedelta(minutes=float(ttl_minutes))

    def heartbeat(self, ctx: PipelineContext, now: datetime) -> Notification:
        return Notification(
            labels={"alertname": WATCHDOG_ALERTNAME, "pipeline": ctx.pipeline, "severity": "none", "source": "aiops"},
            annotations={"summary": f"AIOps 管線 {ctx.pipeline} 心跳（停止續命 = 管線沒在跑）",
                         "last_run": now.isoformat(), "target_errors": str(len(ctx.errors)),
                         "anomalies": str(len(ctx.anomalies)), "notifications": str(len(ctx.notifications))},
            starts_at=now, ends_at=now + self.ttl)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        beat = self.heartbeat(ctx, datetime.now(timezone.utc))
        kind = os.environ.get("AIOPS_NOTIFIER", "log").strip().lower() or "log"
        if kind == "alertmanager":
            url = os.environ.get("AIOPS_ALERTMANAGER_URL", "").strip()
            if not url:
                raise InfrastructureError("notifier.watchdog：AIOPS_NOTIFIER=alertmanager 但沒有設定 AIOPS_ALERTMANAGER_URL")
            AlertmanagerTransport(url, os.environ.get("AIOPS_ALERTMANAGER_TOKEN") or None).send([beat])
        elif kind == "log":
            logger.info("heartbeat %s", json.dumps(beat.labels | beat.annotations, ensure_ascii=False))
        else:
            raise InfrastructureError(f"未知的 AIOPS_NOTIFIER：{kind}（可用：log、alertmanager）")
        ctx.result["heartbeat"] = {"ends_at": beat.ends_at.isoformat()}
        ctx.log(f"heartbeat:{kind}")
        return ctx


@OperatorRegistry.register("notifier.alertmanager")
class AlertmanagerNotifier(BaseOperator):
    """把 ctx.notifications 推到 Alertmanager。params: endpoint（不給就用環境變數 AIOPS_ALERTMANAGER_URL）。
    token 只從環境變數 AIOPS_ALERTMANAGER_TOKEN 讀，不寫在 YAML。送不出去丟 InfrastructureError，整輪失敗、不會假裝成功。
    """

    scope = "after_persist"

    def __init__(self, endpoint: str | None = None):
        self.endpoint = endpoint

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        url = self.endpoint or os.environ.get("AIOPS_ALERTMANAGER_URL", "").strip()
        if not url:
            raise InfrastructureError("notifier.alertmanager 沒有 endpoint，也沒有設定 AIOPS_ALERTMANAGER_URL")
        AlertmanagerTransport(url, os.environ.get("AIOPS_ALERTMANAGER_TOKEN") or None).send(ctx.notifications)
        return ctx
