"""API 認證：Bearer token。

由 initialization 掛在「每一個」領域 router 上（APIRouter 的 dependencies），api.py 不寫任何驗證邏輯，
新增領域也不會漏掛。

環境變數：
- AIOPS_API_TOKEN：必填。可用逗號分隔多組（換 token 時新舊並存）。
- AIOPS_AUTH_DISABLED=true：只限本機開發，關閉驗證（啟動時會寫警告）。
沒設 token 又沒關閉驗證時一律拒絕（fail closed），不會因為忘了設定就整個開放。
"""
import logging
import os
import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger("aiops.security")
_bearer = HTTPBearer(auto_error=False)


def auth_disabled() -> bool:
    return os.environ.get("AIOPS_AUTH_DISABLED", "").lower() == "true"


def _tokens() -> list[str]:
    return [t.strip() for t in os.environ.get("AIOPS_API_TOKEN", "").split(",") if t.strip()]


def _reject(detail: str) -> HTTPException:
    return HTTPException(status.HTTP_401_UNAUTHORIZED, detail=detail, headers={"WWW-Authenticate": "Bearer"})


def require_token(credentials: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> None:
    if auth_disabled():
        return
    tokens = _tokens()
    if not tokens:
        raise _reject("伺服器沒有設定 AIOPS_API_TOKEN，所有 API 一律拒絕")
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise _reject("需要 Authorization: Bearer <token>")
    if not any(secrets.compare_digest(credentials.credentials.encode(), t.encode()) for t in tokens):
        raise _reject("token 不正確")


def warn_if_disabled() -> None:
    if auth_disabled():
        logger.warning("AIOPS_AUTH_DISABLED=true：API 驗證已關閉，只能用在本機開發")
