from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd

from aiops.commands.base import CommandError, CommandNotFound


class InvalidQueryError(CommandError):
    """查詢寫錯（語法、多條序列、索引不存在）：是使用者要改的，回 400。"""


class NoDataError(CommandNotFound):
    """查詢合法但這段時間沒有資料：回 404。排程會把它當成這個 target 失敗回報。"""


class BaseDataSourceSpec(ABC):
    """仿 db_engine_specs：各資料源方言，統一輸出 DataFrame(ts, value)，ts 為 UTC。

    錯誤分工：查詢寫錯 -> InvalidQueryError；沒資料 -> NoDataError；連不上／認證失敗 -> InfrastructureError。
    """

    source_type: str = ""

    @classmethod
    def from_env(cls) -> "BaseDataSourceSpec":
        """依環境變數建立；缺必要設定丟 InfrastructureError。"""
        return cls()

    @classmethod
    def configured(cls) -> bool:
        return True

    @abstractmethod
    def fetch_data(self, query: str, start: datetime, end: datetime, step_seconds: int) -> pd.DataFrame: ...

    @abstractmethod
    def check(self) -> dict[str, Any]:
        """連線自我檢查：{"ok": bool, "detail": str}。不得丟例外。"""
