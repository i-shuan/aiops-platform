"""進行中事件群吸收新異常：一場事故從初發到結案只有一個 incident_uid、一張告警卡。

收斂（same_entity、topology_chain、lot_path_chain）只看這一輪的異常；沒有這一步時，事故延燒期間每一輪都會組出新事件群，
冷卻一過就另開一張卡（實測 cart-api 11/3 一場 100 分鐘的事故開了 3 張）。

規則：這一輪的事件群只要跟某個進行中事件群（ctx.open_incidents，由 storage 載入）有共同實體，就把它的異常點
併進那個進行中事件群（去重、依時間排序；實體集合取聯集），分數取最大，然後從 ctx.incidents 拿掉，不另立新 uid。
碰到好幾個進行中事件群時併進最早開的那個。嚴重度不在這裡算（門檻歸 alert.notify_payload 管）。

位置：收斂與計分之後、抑制之前（被吸收的不該再被冷卻或同根因判斷一次）；alert.heartbeat 要排在 alert.notify_payload 之後。
"""
from __future__ import annotations

from datetime import timedelta

from aiops.engine.context import PipelineContext
from aiops.engine.operator import BaseOperator
from aiops.engine.params import positive
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import Incident, as_utc
from aiops.stages._common import ensure_incidents


def _key(e):
    return (e.entity, e.metric, e.ts, e.detector_node)


@OperatorRegistry.register("alert.absorb_open")
class AbsorbOpenIncidents(BaseOperator):
    """params: resolve_after_minutes（預設 15，要跟 alert.heartbeat 的一樣）：新異常距離進行中事件群的最後一筆超過這麼久，
    代表中間已經安靜到該結案了（例如排程停了一陣子才恢復），不吸收、另開新卡；舊的那張由心跳結案。"""

    scope = "batch"

    def __init__(self, resolve_after_minutes: float = 15):
        self.max_gap = timedelta(minutes=positive("resolve_after_minutes", resolve_after_minutes))

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        opened = sorted(ctx.open_incidents, key=lambda o: o.incident.start)
        kept: list[Incident] = []
        absorbed = 0
        for inc in ensure_incidents(ctx):
            entities = set(inc.entities)
            target = next((o for o in opened if entities & set(o.incident.entities)
                           and as_utc(inc.start) - as_utc(o.incident.end) < self.max_gap), None)
            if target is None:
                kept.append(inc)
                continue
            target.remember()
            cur = target.incident
            events = {_key(e): e for e in cur.events}
            for e in inc.events:
                events.setdefault(_key(e), e)
            tags = list(dict.fromkeys([*cur.tags, *inc.tags, "absorbed"]))
            target.incident = Incident(sorted(events.values(), key=lambda e: e.ts), max(cur.score, inc.score), tags,
                                       severity=cur.severity, uid=target.uid)
            absorbed += len(inc.events)
        if absorbed:
            ctx.log(f"absorb_open:{len(ctx.incidents) - len(kept)}->{len({o.uid for o in opened if o.changed})}:"
                    f"{absorbed}events")
        ctx.incidents = kept
        return ctx
