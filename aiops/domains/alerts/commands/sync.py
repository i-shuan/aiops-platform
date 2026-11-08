"""進行中事件群的心跳與結案（恢復通知）。

Alertmanager 的慣例：發送端要定期重送仍在發生的告警，沒重送就會在它設定的自動結案時限後自己結案。
所以每輪排程：
- 實體還有新異常（或還沒安靜夠久）的事件群 -> 原樣重送（心跳，不帶 endsAt）
- 實體安靜超過 resolve_after_minutes 的 -> 帶 endsAt 重送一次，接收端結案、發恢復通知，平台標成 resolved
重送的 labels 由事件群內容重建，跟第一次一模一樣，接收端才認得是同一筆。
"""
from datetime import datetime, timedelta, timezone

from aiops.commands.base import BaseCommand, CommandConflict, CommandNotFound
from aiops.domains.alerts.commands.payload import build_notifications
from aiops.domains.alerts.dao import AnomalyReadDAO, ExternalAnomalyReadDAO, IncidentDAO
from aiops.domains.alerts.schemas import SyncIncidentsRequest
from aiops.notifiers import get_notifier
from aiops.plugins.base import Incident, as_utc


def _last_seen(pipeline: str, incident: Incident) -> datetime:
    """事件群最後一次有異常的時間：事件群本身的結束時間，或這些實體後來在同管線又出現的異常。"""
    times = [as_utc(incident.end)]
    for entity in incident.entities:
        for dao in (AnomalyReadDAO, ExternalAnomalyReadDAO):
            ts = dao.latest_ts(pipeline, entity)
            if ts is not None:
                times.append(as_utc(ts))
    return max(times)


class SyncOpenIncidentsCommand(BaseCommand):
    def __init__(self, req: SyncIncidentsRequest, now: datetime | None = None):
        self.req = req
        self.now = now

    def validate(self) -> None:
        pass  # 參數已由 schema 驗證

    def run(self) -> dict:
        self.validate()
        now = as_utc(self.now or datetime.now(timezone.utc))
        quiet_after = timedelta(minutes=self.req.resolve_after_minutes)
        refreshed, resolved, outgoing = [], [], []
        for row in IncidentDAO.list_open(self.req.pipeline):
            inc = Incident.from_dict(row["incident"])
            quiet = now - _last_seen(self.req.pipeline, inc) >= quiet_after
            outgoing += build_notifications(row["uid"], inc, row["preset"], row["severity"],
                                            ends_at=now if quiet else None)
            (resolved if quiet else refreshed).append(row)

        get_notifier().send(outgoing)  # 送出失敗就丟 InfrastructureError，狀態不改，下一輪再試
        for row in resolved:
            IncidentDAO.mark_resolved(row["id"], now)
        return {"pipeline": self.req.pipeline, "refreshed": len(refreshed), "resolved": len(resolved),
                "resolved_uids": [r["uid"] for r in resolved]}


class ResolveIncidentCommand(BaseCommand):
    """人工結案（例如直接呼叫 /alerts/process 建立、沒有排程心跳的事件群）。"""

    def __init__(self, uid: str, now: datetime | None = None):
        self.uid = uid
        self.now = now

    def validate(self) -> None:
        self.row = IncidentDAO.get_by_uid(self.uid)
        if self.row is None:
            raise CommandNotFound(f"找不到事件群 {self.uid}")
        if self.row["status"] != "open":
            raise CommandConflict(f"事件群 {self.uid} 狀態是 {self.row['status']}，只有 open 能結案")

    def run(self) -> dict:
        self.validate()
        now = as_utc(self.now or datetime.now(timezone.utc))
        inc = Incident.from_dict(self.row["incident"])
        get_notifier().send(build_notifications(self.uid, inc, self.row["preset"], self.row["severity"], ends_at=now))
        return IncidentDAO.mark_resolved(self.row["id"], now)
