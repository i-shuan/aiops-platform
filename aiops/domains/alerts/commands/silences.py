"""靜音、維護窗口、人工排除時段：只有一份設定，在平台。接收端不另設靜音。"""
from aiops.commands.base import BaseCommand
from aiops.domains.alerts.dao import SilenceDAO
from aiops.domains.alerts.schemas import SilenceCreateRequest


class CreateSilenceCommand(BaseCommand):
    def __init__(self, req: SilenceCreateRequest):
        self.req = req

    def validate(self) -> None:
        pass  # 時段順序與 kind 已由 schema 驗證

    def run(self) -> dict:
        self.validate()
        return SilenceDAO.create(self.req.model_dump())


class ListSilencesCommand(BaseCommand):
    def validate(self) -> None:
        pass

    def run(self) -> list[dict]:
        return SilenceDAO.list_all()
