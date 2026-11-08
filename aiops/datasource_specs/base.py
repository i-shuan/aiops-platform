from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd

from aiops.core.errors import BadRequest, NotFound


class InvalidQueryError(BadRequest):
    """查詢寫錯（語法、多條序列、索引不存在）：是使用者要改的，回 400。"""


class NoDataError(NotFound):
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
    def fetch_data(self, query: str, start: datetime, end: datetime, step_seconds: int,
                   field_map: dict[str, str] | None = None) -> pd.DataFrame:
        """field_map：已核准的欄位對照（來源欄位 -> 標準欄位），由領域 Command 讀出後傳入；資料源自己不查資料庫。
        用不到對照的資料源（例如 PromQL 本來就指定好序列）可以忽略它。"""

    @abstractmethod
    def check(self) -> dict[str, Any]:
        """連線自我檢查：{"ok": bool, "detail": str}。不得丟例外。"""
