"""通知出口：把平台已決定要發的通知送到外部接收端。只負責傳送，不做任何判斷。"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Notification:
    # 接收端只依 labels 路由（例如依 dept 分送到不同 Slack channel）
    labels: dict[str, str]
    annotations: dict[str, str]
    starts_at: datetime


class BaseNotifier(ABC):
    name: str = ""

    @abstractmethod
    def send(self, notifications: list[Notification]) -> None:
        """送不出去時丟 InfrastructureError（由 initialization 轉 503 並走看守機制）。"""
