import json
import logging

from aiops.notifiers.base import BaseNotifier, Notification

logger = logging.getLogger("aiops.notifiers")


class LogNotifier(BaseNotifier):
    """開發與測試用：只寫日誌，並保留在 sent 讓測試檢查。正式環境請用 alertmanager。"""

    name = "log"
    sent: list[Notification] = []

    def send(self, notifications: list[Notification]) -> None:
        for n in notifications:
            logger.info(json.dumps({"labels": n.labels, "annotations": n.annotations}, ensure_ascii=False))
        LogNotifier.sent.extend(notifications)
