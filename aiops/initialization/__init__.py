import importlib
import pkgutil

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from aiops import domains
from aiops.commands.base import CommandError


def init_plugins() -> None:
    """自動掃描 plugins/，載入即完成 @register，領域層不必 import 具體外掛。"""
    for pkg in ("detectors", "llm"):
        mod = importlib.import_module(f"aiops.plugins.{pkg}")
        for m in pkgutil.iter_modules(mod.__path__):
            importlib.import_module(f"aiops.plugins.{pkg}.{m.name}")


def init_routers(app: FastAPI) -> None:
    """掃描 domains/ 下每個子套件，有 api.py 就自動掛載其 router。新增領域不必改這裡。"""
    for m in pkgutil.iter_modules(domains.__path__):
        if not m.ispkg:
            continue
        try:
            api = importlib.import_module(f"aiops.domains.{m.name}.api")
        except ModuleNotFoundError as e:
            if e.name == f"aiops.domains.{m.name}.api":
                continue  # 該領域還沒有 api.py（例如空殼領域）
            raise
        app.include_router(api.router)


def init_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(CommandError)
    async def _command_error(_: Request, exc: CommandError):
        return JSONResponse(status_code=exc.status_code, content={"message": str(exc)})
