"""通知出口的傳輸實作。只負責把已決定好的通知送出去，不做任何判斷。"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import httpx

from aiops.core.errors import InfrastructureError
from aiops.core.types import Notification

logger = logging.getLogger("aiops.notifiers")


def _iso_utc(dt: datetime) -> str:
    dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


class AlertmanagerTransport:
    """推到 Alertmanager v2 API：POST {base_url}/api/v2/alerts。接收端只依 labels 路由到 Contact Point。"""

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


class LogTransport:
    """開發與測試用：只寫日誌，並保留在 sent 讓測試檢查。正式環境請用 alertmanager。"""

    sent: list[Notification] = []

    def send(self, notifications: list[Notification]) -> None:
        for n in notifications:
            logger.info(json.dumps({"labels": n.labels, "annotations": n.annotations}, ensure_ascii=False))
        LogTransport.sent.extend(notifications)
