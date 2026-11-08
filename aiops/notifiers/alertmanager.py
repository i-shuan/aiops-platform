from datetime import datetime, timezone

import httpx

from aiops.infrastructure.errors import InfrastructureError
from aiops.notifiers.base import BaseNotifier, Notification


def _iso_utc(dt: datetime) -> str:
    dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


class AlertmanagerNotifier(BaseNotifier):
    """推到 Alertmanager v2 API：POST {base_url}/api/v2/alerts。

    接收端（Alertmanager 或相容的告警接收端）只依 labels 路由到 Contact Point，不再判斷要不要發。
    """

    name = "alertmanager"

    def __init__(self, base_url: str, token: str | None = None, timeout: float = 5.0):
        self.url = base_url.rstrip("/") + "/api/v2/alerts"
        self.token = token
        self.timeout = timeout

    def to_body(self, notifications: list[Notification]) -> list[dict]:
        body = []
        for n in notifications:
            alert = {"labels": n.labels, "annotations": n.annotations, "startsAt": _iso_utc(n.starts_at)}
            if n.ends_at is not None:
                alert["endsAt"] = _iso_utc(n.ends_at)
            body.append(alert)
        return body

    def send(self, notifications: list[Notification]) -> None:
        if not notifications:
            return
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        try:
            resp = httpx.post(self.url, json=self.to_body(notifications), headers=headers, timeout=self.timeout)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise InfrastructureError(f"推送通知到 {self.url} 失敗：{e}") from e
