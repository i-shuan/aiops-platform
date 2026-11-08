"""進行中事件群的心跳與結案（恢復通知）。

Alertmanager 的慣例：發送端要定期重送仍在發生的告警，沒重送就會在它設定的自動結案時限後自己結案。所以每輪：
- 實體還有新異常（或還沒安靜夠久）的事件群 -> 原樣重送（心跳，不帶 endsAt）
- 實體安靜超過 resolve_after_minutes 的 -> 帶 endsAt 重送一次，接收端結案、發恢復通知，平台標成 resolved
排在 alert.notify_payload 之前或之後都行；它只處理「已經通知過、還在進行中」的事件群，不碰這一輪新產生的。
"""
from __future__ import annotations

from datetime import timedelta

from aiops.engine.context import PipelineContext
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import as_utc
from aiops.stages.notification.payload import build_notifications
from aiops.engine.operator import BaseOperator


@OperatorRegistry.register("alert.heartbeat")
class Heartbeat(BaseOperator):
    scope = "batch"

    def __init__(self, resolve_after_minutes: float = 15):
        self.quiet_after = timedelta(minutes=float(resolve_after_minutes))

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        now = as_utc(ctx.now)
        newest_this_run: dict[str, object] = {}
        for a in ctx.anomalies:  # 這一輪剛偵測到、還沒存進資料庫的異常也算「又出現了」
            if a.entity not in newest_this_run or as_utc(a.ts) > newest_this_run[a.entity]:
                newest_this_run[a.entity] = as_utc(a.ts)

        for open_inc in ctx.open_incidents:
            inc = open_inc.incident
            seen = [as_utc(inc.end)]
            for entity in inc.entities:
                if entity in ctx.entity_last_seen:
                    seen.append(as_utc(ctx.entity_last_seen[entity]))
                if entity in newest_this_run:
                    seen.append(newest_this_run[entity])
            quiet = now - max(seen) >= self.quiet_after
            ctx.notifications.extend(build_notifications(inc, open_inc.preset, open_inc.severity,
                                                         ends_at=now if quiet else None))
            if quiet:
                ctx.resolved_incident_ids.append(open_inc.id)
        return ctx
