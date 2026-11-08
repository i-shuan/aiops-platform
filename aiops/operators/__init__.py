"""可插拔算子。load_all() 掃描整個套件，載入即完成 @OperatorRegistry.register。"""
import importlib
import pkgutil


def load_all() -> None:
    for module in pkgutil.walk_packages(__path__, prefix=f"{__name__}."):
        importlib.import_module(module.name)
