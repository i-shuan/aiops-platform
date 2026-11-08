import importlib
import pkgutil

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from aiops import api
from aiops.core.errors import InfrastructureError, PlatformError
from aiops.infrastructure.fallback import init_fallback_logging, report_failure
from aiops.operators import load_all
from aiops.security.auth import require_token, warn_if_disabled


def init_operators() -> None:
    """掃描 operators/ 底下所有算子模組，載入即完成 @OperatorRegistry.register。"""
    load_all()


def init_routers(app: FastAPI) -> None:
    """掃描 api/ 底下每個模組，有 router 就自動掛載，並一律掛上 token 驗證。新增端點模組不必改這裡。"""
    warn_if_disabled()
    for m in pkgutil.iter_modules(api.__path__):
        module = importlib.import_module(f"aiops.api.{m.name}")
        router = getattr(module, "router", None)
        if router is not None:
            app.include_router(router, dependencies=[Depends(require_token)])


def init_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(PlatformError)
    async def _platform_error(_: Request, exc: PlatformError):
        return JSONResponse(status_code=exc.status_code, content={"message": str(exc)})

    @app.exception_handler(InfrastructureError)
    async def _infrastructure_error(request: Request, exc: InfrastructureError):
        report_failure(exc, {"method": request.method, "path": request.url.path})
        return JSONResponse(status_code=503, content={"message": "基礎設施暫時無法使用，已通報"})


def init_infrastructure() -> None:
    init_fallback_logging()
    init_db()


def init_db(sleep=None, clock=None) -> None:
    """建立還不存在的資料表。上正式環境前要改用 Alembic migration。

    資料庫可能比平台晚就緒（K8s 同時啟動、資料庫重啟中）：最多等 AIOPS_DB_STARTUP_TIMEOUT 秒（預設 60），
    每 2 秒重試一次，超過才放棄（丟 InfrastructureError，讓 K8s 重啟）。
    """
    import logging
    import os
    import time

    from aiops.storage.db import create_all

    sleep, clock = sleep or time.sleep, clock or time.monotonic
    deadline = clock() + float(os.environ.get("AIOPS_DB_STARTUP_TIMEOUT", "60"))
    while True:
        try:
            create_all()
            return
        except InfrastructureError as e:
            if clock() >= deadline:
                raise
            logging.getLogger("aiops.startup").warning("等待資料庫就緒：%s", e)
            sleep(2)
