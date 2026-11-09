from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd

from aiops.engine.errors import BadRequest, NoDataError  # noqa: F401  NoDataError 定義在 engine，這裡再匯出給資料源用


def canonical_service(name: Any) -> str:
    """服務名稱正規化：去頭尾空白、轉小寫。ES 裡的 `Cart-API ` 跟事件實體 `cart-api` 要能對得上，
    不然查詢與比對會悄悄漏掉（沒有任何錯誤，只是撈不到）。"""
    return str(name).strip().lower()


def as_utc_datetime(value: Any) -> datetime:
    """ES 的 @timestamp 轉成帶 UTC 時區的 datetime。有時區就換算成 UTC；沒帶時區（naive）一律當 UTC，不丟 TypeError。"""
    ts = pd.Timestamp(value)
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return ts.to_pydatetime()


class InvalidQueryError(BadRequest):
    """查詢寫錯（語法、多條序列、索引不存在）：是使用者要改的，回 400。"""


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

    def fetch_group(self, query: str, start: datetime, end: datetime, step_seconds: int, by: str,
                    field_map: dict[str, str] | None = None) -> dict[str, pd.DataFrame]:
        """動態同儕：一個查詢撈回多條序列，依 by 標籤分組 -> {成員: DataFrame(ts, value)}。
        預設不支援；支援多序列查詢的資料源（Prometheus 系列）覆寫它。"""
        raise InvalidQueryError(f"{self.source_type or type(self).__name__} 不支援動態同儕（peer_group），請改用 peers 寫死名單")

    def fetch_spans(self, start: datetime, end: datetime, services: list[str] | None = None,
                    max_spans: int = 20000) -> tuple[list[dict[str, Any]], bool]:
        """RCA 隨需撈 trace：[start, end] 內的 span／transaction，services 給了就只撈這些服務。
        回傳 (spans, 是否因為 max_spans 被截斷)。每個 span 是 dict：
        trace_id、span_id、parent_id（可能是 None）、service、ts（UTC datetime）、duration_ms、error。
        預設不支援；存 trace 的資料源（Elasticsearch APM）覆寫它。"""
        raise InvalidQueryError(f"{self.source_type or type(self).__name__} 不支援撈 trace")

    def fetch_logs(self, start: datetime, end: datetime, services: list[str] | None = None,
                   max_logs: int = 5000) -> tuple[list[dict[str, Any]], bool]:
        """RCA 隨需撈錯誤日誌：[start, end] 內、services 給了就只撈這些服務。回傳 (logs, 是否因為 max_logs 被截斷)。
        每筆是 dict：ts（UTC datetime）、service、message、error_type、trace_id。預設不支援。"""
        raise InvalidQueryError(f"{self.source_type or type(self).__name__} 不支援撈日誌")

    @abstractmethod
    def check(self) -> dict[str, Any]:
        """連線自我檢查：{"ok": bool, "detail": str}。不得丟例外。"""
