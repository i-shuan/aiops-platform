"""平台錯誤型別。API 層統一把它們轉成 HTTP 狀態碼。"""


class PlatformError(Exception):
    status_code = 400


class BadRequest(PlatformError):
    status_code = 400


class NotFound(PlatformError):
    status_code = 404


class Conflict(PlatformError):
    """狀態不允許此動作（例如未回測就核准、設定已被別人改過）。"""

    status_code = 409


class InfrastructureError(PlatformError):
    """基礎設施故障（DB、ES、Alertmanager 連不上）。不是業務錯誤，API 回 503 並走看守機制。"""

    status_code = 503


class WorkflowError(BadRequest):
    """Workflow 定義本身有錯（未註冊的算子、拼錯參數、順序不對）。啟動時就要擋下，不是執行期才發現。"""
