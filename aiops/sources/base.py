from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd

from aiops.engine.errors import BadRequest, NoDataError  # noqa: F401  NoDataError 定義在 engine，這裡再匯出給資料源用


def canonical_service(name: Any) -> str:
    """服務名稱正規化：去頭尾空白、轉小寫。ES 裡的 `Cart-API ` 跟事件實體 `cart-api` 要能對得上，
    不然查詢與比對會悄悄漏掉（沒有任何錯誤，只是撈不到）。"""
    return str(name).strip().lower()


def in_environment(rows: list[dict[str, Any]], environment: str | None) -> list[dict[str, Any]]:
    """只留屬於這個部署環境的 span／日誌（每筆的 environment 鍵，來自 deployment.environment／service.environment）。
    environment 是空的 = 不分環境，原樣回傳。資料源應該在查詢時就濾掉（fetch_spans／fetch_logs 的 environment 參數），
    這裡是呼叫端的第二道：資料源忽略那個參數時，別環境的資料也進不來。
    某一筆**沒有** environment 這個鍵 = 那個資料源不提供環境資訊，留著（濾掉的話這種資料源一設環境就整個撈不到）；
    有這個鍵但值不同（含空字串）就丟掉。"""
    if not environment:
        return rows
    return [r for r in rows if "environment" not in r or r["environment"] == environment]


def as_utc_datetime(value: Any) -> datetime:
    """ES 的 @timestamp 轉成帶 UTC 時區的 datetime。有時區就換算成 UTC；沒帶時區（naive）一律當 UTC，不丟 TypeError。

    數字或數字字串一律當 epoch 毫秒（ES 日期欄位的預設格式是 strict_date_optional_time||epoch_millis）：
    OTel Collector 的 elasticsearchexporter（otel mapping）在 _source 裡存的就是 "1794487341966.212300"（毫秒＋次毫秒小數，
    實測見 tests/fixtures/otel_es/）。直接丟給 pd.Timestamp 會被當成奈秒（整數解成 1970 年、這種字串直接溢位）。"""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return pd.Timestamp(float(value), unit="ms", tz="UTC").to_pydatetime()
    if isinstance(value, str) and value.strip().replace(".", "", 1).isdigit():
        whole, _, frac = value.strip().partition(".")
        # 整數毫秒＋小數部分分開算到奈秒，不經過 float（13 位毫秒＋6 位小數超過 float 精度）
        nanos = int(whole) * 1_000_000 + int((frac + "000000")[:6])
        return pd.Timestamp(nanos // 1000, unit="us", tz="UTC").to_pydatetime()  # datetime 只到微秒
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
                    max_spans: int = 20000, field_map: dict[str, str] | None = None,
                    environment: str | None = None) -> tuple[list[dict[str, Any]], bool]:
        """RCA 隨需撈 trace：[start, end] 內的 span／transaction，services 給了就只撈這些服務。
        回傳 (spans, 是否因為 max_spans 被截斷)。每個 span 是 dict：
        trace_id、span_id、parent_id（可能是 None）、service、ts（UTC datetime）、duration_ms、error。
        選填鍵（沒給就當空字串／0，RCA 退回只看服務）：kind（span.type：app／db／external…；入口 transaction 留空）、
        subtype（redis／http／postgresql）、resource（span.destination.service.resource，例如 tax-gw:443）、
        status_code（http.response.status_code）、name（transaction.name）。
        訊息佇列（非同步邊）用的選填鍵：consumer（transaction.type = messaging 的消費者入口）、queue（佇列名）、
        age_ms（訊息年齡，沒有就是 None）、links（批次消費的 span.links：[(trace_id, span_id), ...]）。
        field_map：已核准的欄位對照（標準欄 span.* 蓋過資料源預設的來源欄；只有真的有對照時呼叫端才會帶）。
        environment：只撈這個部署環境的（管線設了 Workflow.environment 時呼叫端才會帶）；每筆回傳的 dict 帶
        environment 鍵（該筆的環境，沒有就是空字串），呼叫端會用 in_environment 再濾一次。
        預設不支援；存 trace 的資料源（grafana：後面的 Elasticsearch 存 Elastic APM 或 OTel 文件，見 sources/es_models.py）覆寫它。"""
        raise InvalidQueryError(f"{self.source_type or type(self).__name__} 不支援撈 trace")

    def fetch_linked_spans(self, trace_ids: list[str], start: datetime, end: datetime,
                           max_spans: int = 20000, field_map: dict[str, str] | None = None) -> tuple[list[dict[str, Any]], bool]:
        """訊息佇列跨時窗：把指定 trace 在 [start, end] 內的 span 撈回來（格式同 fetch_spans，不帶服務過濾）。
        消費者的 parent／span.links 指向的生產者 span 常常落在事故窗之前，靠它補回來。預設不支援。"""
        raise InvalidQueryError(f"{self.source_type or type(self).__name__} 不支援依 trace 撈 span（訊息佇列跨時窗）")

    def fetch_logs(self, start: datetime, end: datetime, services: list[str] | None = None,
                   max_logs: int = 5000, field_map: dict[str, str] | None = None,
                   environment: str | None = None) -> tuple[list[dict[str, Any]], bool]:
        """RCA 隨需撈錯誤日誌：[start, end] 內、services 給了就只撈這些服務。回傳 (logs, 是否因為 max_logs 被截斷)。
        每筆是 dict：ts（UTC datetime）、service、message、error_type、trace_id。預設不支援。"""
        raise InvalidQueryError(f"{self.source_type or type(self).__name__} 不支援撈日誌")

    @abstractmethod
    def check(self) -> dict[str, Any]:
        """連線自我檢查：{"ok": bool, "detail": str}。不得丟例外。"""
