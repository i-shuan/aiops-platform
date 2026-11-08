from abc import ABC, abstractmethod
from typing import Any


class CommandError(Exception):
    """Command 流程中的業務錯誤，initialization 統一轉成 HTTP 4xx。"""

    status_code = 400


class CommandNotFound(CommandError):
    status_code = 404


class CommandConflict(CommandError):
    """狀態不允許此動作（例如未回測就核准、設定已被別人改過）。"""

    status_code = 409


class BaseCommand(ABC):
    """所有領域流程的基底：validate -> run。API 只認識 Command。"""

    @abstractmethod
    def validate(self) -> None: ...

    @abstractmethod
    def run(self) -> Any: ...
