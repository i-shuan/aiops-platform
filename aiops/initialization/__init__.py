import importlib
import pkgutil

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from aiops import domains, plugins
from aiops.commands.base import CommandError
from aiops.infrastructure.errors import InfrastructureError
from aiops.infrastructure.fallback import init_fallback_logging, report_failure


def init_plugins() -> None:
    """掃描 plugins/ 底下每個子套件的所有模組，載入即完成 @register。領域層不必 import 具體外掛。"""
    for pkg in pkgutil.iter_modules(plugins.__path__):
        if not pkg.ispkg:
            continue
        package = importlib.import_module(f"aiops.plugins.{pkg.name}")
        for m in pkgutil.iter_modules(package.__path__):
            importlib.import_module(f"aiops.plugins.{pkg.name}.{m.name}")


def init_routers(app: FastAPI) -> None:
    """掃描 domains/ 下每個子套件，有 api.py 就自動掛載其 router。新增領域不必改這裡。"""
    for m in pkgutil.iter_modules(domains.__path__):
        if not m.ispkg:
            continue
        try:
            api = importlib.import_module(f"aiops.domains.{m.name}.api")
        except ModuleNotFoundError as e:
            if e.name == f"aiops.domains.{m.name}.api":
                continue  # 該領域還沒有 api.py
            raise
        app.include_router(api.router)


def init_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(CommandError)
    async def _command_error(_: Request, exc: CommandError):
        return JSONResponse(status_code=exc.status_code, content={"message": str(exc)})

    @app.exception_handler(InfrastructureError)
    async def _infrastructure_error(request: Request, exc: InfrastructureError):
        report_failure(exc, {"method": request.method, "path": request.url.path})
        return JSONResponse(status_code=503, content={"message": "基礎設施暫時無法使用，已通報"})


def init_infrastructure() -> None:
    init_fallback_logging()
    init_db()


def init_db() -> None:
    """建立還不存在的資料表。上正式環境前要改用 Alembic migration。"""
    from aiops.models.db import create_all

    create_all()
