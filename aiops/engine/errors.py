"""平台錯誤型別。API 層統一把它們轉成 HTTP 狀態碼。"""


class PlatformError(Exception):
    status_code = 400


class BadRequest(PlatformError):
    status_code = 400


class NotFound(PlatformError):
    status_code = 404


class NoDataError(NotFound):
    """資料源說「查詢合法，但這段時間沒有資料」：回 404。

    只有資料源（sources/）與 ingestion.fetch 可以丟它；引擎只把它算成「這條序列抓不到資料」（資料斷流的分子），
    唯一例外是 strict=True（手動動作）時抓回來是空的序列也丟它，讓「沒資料」一律是 404。
    其他地方查不到東西丟一般的 NotFound，不會被誤算成斷流。
    """


class Conflict(PlatformError):
    """狀態不允許此動作（例如未回測就核准、設定已被別人改過）。"""

    status_code = 409


class InfrastructureError(PlatformError):
    """基礎設施故障（DB、ES、Alertmanager 連不上）。不是業務錯誤，API 回 503 並走看守機制。"""

    status_code = 503


class WorkflowError(BadRequest):
    """Workflow 定義本身有錯（未註冊的算子、拼錯參數、順序不對）。啟動時就要擋下，不是執行期才發現。"""
