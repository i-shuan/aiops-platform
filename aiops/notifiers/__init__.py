"""通知出口入口。領域只能用 get_notifier() 與 notifiers.base，不得 import 具體實作。

環境變數：
- AIOPS_NOTIFIER：log（預設，只寫日誌）／alertmanager
- AIOPS_ALERTMANAGER_URL：例如 http://alertmanager:9093
- AIOPS_ALERTMANAGER_TOKEN：選填，Bearer token
"""
import os

from aiops.infrastructure.errors import InfrastructureError
from aiops.notifiers.alertmanager import AlertmanagerNotifier
from aiops.notifiers.base import BaseNotifier
from aiops.notifiers.log import LogNotifier


def get_notifier() -> BaseNotifier:
    kind = os.environ.get("AIOPS_NOTIFIER", "log")
    if kind == "log":
        return LogNotifier()
    if kind == "alertmanager":
        url = os.environ.get("AIOPS_ALERTMANAGER_URL")
        if not url:
            raise InfrastructureError("AIOPS_NOTIFIER=alertmanager 但沒有設定 AIOPS_ALERTMANAGER_URL")
        return AlertmanagerNotifier(url, os.environ.get("AIOPS_ALERTMANAGER_TOKEN"))
    raise InfrastructureError(f"未知的 AIOPS_NOTIFIER: {kind}")
