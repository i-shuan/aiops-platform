"""LLM 供應商註冊表：新增供應商 = 在 stages/enrichment/ 放一個檔案、用 @register 登記，不必改別的地方。

    @register("mistral")
    def _mistral(model, base_url, api_key):
        return MistralLLM(api_key, model, base_url or "https://api.mistral.ai")

第一次查詢（names／build）時，會自動掃描並載入 aiops.stages.enrichment 底下所有模組，
所以「放進資料夾就生效」，不會因為沒人 import 那個檔案而讓裝飾器沒執行。
"""
from __future__ import annotations

import importlib
import pkgutil
from dataclasses import dataclass
from typing import Callable

from aiops.stages.enrichment.base import BaseLLM, LLMError

# (model, base_url, api_key) -> BaseLLM；noop 回傳 None（完全不連外）
Factory = Callable[[str, str, "str | None"], "BaseLLM | None"]


@dataclass(frozen=True)
class _Entry:
    factory: Factory
    requires_model: bool


_ENTRIES: dict[str, _Entry] = {}
_scanned = False


def register(name: str, *, requires_model: bool = True):
    """登記一個 LLM 供應商。requires_model=False 的供應商（例如 noop）不需要指定模型。"""

    def deco(factory: Factory) -> Factory:
        existing = _ENTRIES.get(name)
        if existing is not None and existing.factory is not factory:
            raise TypeError(f"LLM provider 名稱重複：{name}")
        _ENTRIES[name] = _Entry(factory, requires_model)
        return factory

    return deco


def _scan() -> None:
    global _scanned
    if _scanned:
        return
    _scanned = True  # 先標記，掃描中被載入的模組再呼叫 names() 時不會遞迴
    import aiops.stages.enrichment as package

    for module in pkgutil.iter_modules(package.__path__, prefix=f"{package.__name__}."):
        importlib.import_module(module.name)


def names() -> tuple[str, ...]:
    _scan()
    return tuple(_ENTRIES)


def build(provider: str, model: str, base_url: str, api_key: str | None) -> BaseLLM | None:
    """providers 已經處理好環境變數的預設值，這裡只負責查表與檢查必要欄位。"""
    _scan()
    entry = _ENTRIES.get(provider)
    if entry is None:
        raise LLMError(f"未知的 LLM provider：{provider}（可用：{', '.join(_ENTRIES)}）")
    if entry.requires_model and not model:
        raise LLMError(f"provider={provider} 但沒有指定模型（YAML 的 model 或環境變數 AIOPS_LLM_MODEL）")
    return entry.factory(model, base_url, api_key)
