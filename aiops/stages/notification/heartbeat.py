"""進行中事件群的心跳、升級與結案（恢復通知）。

Alertmanager 的慣例：發送端要定期重送仍在發生的告警，沒重送就會在它設定的自動結案時限後自己結案。所以每輪：
- 實體還有新異常（或還沒安靜夠久）的事件群 -> 用最新內容重送（心跳，不帶 endsAt）。startsAt 固定是初發時間，
  annotations 依最新事件群重算（已持續幾分鐘、累計幾筆、起跳 -> 峰值 -> 最新）
- 標籤跟上次送出的不一樣（嚴重度升級、warning 變 anomaly、吸收了新實體）-> 先對「上次的標籤」送 endsAt=now 關掉，
  同一輪再送新標籤；升級時標題加 [Escalated from 舊等級]。Alertmanager 用整組 labels 辨識告警，不關舊的就會兩張同時掛著
- 實體安靜超過 resolve_after_minutes 的 -> 帶 endsAt 重送一次，接收端結案、發恢復通知，平台標成 resolved。
  這就是防抖動：中間短暫低於門檻（毛刺回落）不會結案，要連續安靜滿這麼久才算真的恢復

要排在 alert.notify_payload 之後（升級的嚴重度由它算），只處理「已經通知過、還在進行中」的事件群，不碰這一輪新產生的。
"""
from __future__ import annotations

from datetime import timedelta

from aiops.engine.context import PipelineContext
from aiops.engine.params import positive
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import as_utc
from aiops.stages.notification.payload import build_notifications
from aiops.engine.operator import BaseOperator


def _label_key(n) -> tuple:
    return tuple(sorted(n.labels.items()))


@OperatorRegistry.register("alert.heartbeat")
class Heartbeat(BaseOperator):
    scope = "batch"

    def __init__(self, resolve_after_minutes: float = 15):
        self.quiet_after = timedelta(minutes=positive("resolve_after_minutes", resolve_after_minutes))

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        now = as_utc(ctx.now)
        newest_this_run: dict[str, object] = {}
        for a in ctx.anomalies:  # 這一輪剛偵測到、還沒存進資料庫的異常也算「又出現了」
            if a.entity not in newest_this_run or as_utc(a.ts) > newest_this_run[a.entity]:
                newest_this_run[a.entity] = as_utc(a.ts)

        for open_inc in ctx.open_incidents:
            inc = open_inc.incident
            # 最後一次「接得上」的異常：從事件群自己的最後一筆開始，只有跟它間隔不到 resolve_after 的才算續命。
            # 中間斷過這麼久（排程停了一陣子才恢復）就是該結案了，後來的異常屬於另一場（alert.absorb_open 也不會吸收）
            last = as_utc(inc.end)
            candidates = sorted(as_utc(t) for entity in inc.entities
                                for t in (ctx.entity_last_seen.get(entity), newest_this_run.get(entity)) if t is not None)
            for t in candidates:
                if t - last < self.quiet_after:
                    last = max(last, t)
            quiet = now - last >= self.quiet_after
            links = ctx.grafana_links  # alert.notify_payload 這一輪組好的（排在它後面）；沒設定 Grafana 就是 None
            current = build_notifications(inc, open_inc.preset, open_inc.severity, ends_at=now if quiet else None,
                                          now=now, links=links)
            if open_inc.previous is not None:
                prev_inc, prev_sev = open_inc.previous
                still = {_label_key(n) for n in current}
                stale = [n for n in build_notifications(prev_inc, open_inc.preset, prev_sev, ends_at=now, now=now,
                                                        links=links)
                         if _label_key(n) not in still]
                if stale:
                    ctx.log(f"supersede:{open_inc.uid}:{len(stale)}")
                ctx.notifications.extend(stale)  # 舊標籤先關，再送新的
            ctx.notifications.extend(current)
            if quiet:
                ctx.resolved_incident_ids.append(open_inc.id)
        return ctx
