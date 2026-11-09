"""通知出口的傳輸實作。只負責把已決定好的通知送出去，不做任何判斷。"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import httpx

from aiops.engine.errors import InfrastructureError
from aiops.engine.types import Notification

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
            if n.generator_url:
                alert["generatorURL"] = n.generator_url  # Grafana 卡片上的 See source
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
    """只寫日誌、不保留任何東西（AIOPS_NOTIFIER 沒設時的預設出口，常駐進程裡也會一直被呼叫）。
    正式環境請用 alertmanager。測試要檢查送了什麼，由測試自己包 send 來錄（tests/conftest.py）。"""

    def send(self, notifications: list[Notification]) -> None:
        for n in notifications:
            logger.info(json.dumps({"labels": n.labels, "annotations": n.annotations}, ensure_ascii=False))
