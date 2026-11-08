"""LLM 出口（跟 notifiers、datasource_specs 同一類：會連外部服務，所以不放在 plugins）。

領域只能用 get_llm() 與 llm.base。LLM 的輸出一律要經過領域內的「依據驗證」，不通過就退回模板；
沒設定 LLM 時 get_llm() 回傳 None，功能照常（全部走模板）。

環境變數：
- AIOPS_LLM_PROVIDER：none（預設）／openai／anthropic
- AIOPS_LLM_MODEL：模型名稱（openai／anthropic 必填），例如 claude-sonnet-5-5
- AIOPS_LLM_API_KEY：API key（anthropic 必填；openai 相容的內網閘道可不填）
- AIOPS_LLM_BASE_URL：選填，內網閘道位址（不含 /v1）
"""
import os

from aiops.infrastructure.errors import InfrastructureError
from aiops.llm.base import BaseLLM
from aiops.llm.providers import AnthropicLLM, OpenAICompatibleLLM


def get_llm() -> BaseLLM | None:
    provider = os.environ.get("AIOPS_LLM_PROVIDER", "none").strip().lower()
    if provider in ("", "none"):
        return None
    model = os.environ.get("AIOPS_LLM_MODEL", "").strip()
    key = os.environ.get("AIOPS_LLM_API_KEY", "").strip() or None
    base = os.environ.get("AIOPS_LLM_BASE_URL", "").strip()
    if not model:
        raise InfrastructureError("AIOPS_LLM_PROVIDER 有設定但沒有 AIOPS_LLM_MODEL")
    if provider == "openai":
        return OpenAICompatibleLLM(key, model, base or "https://api.openai.com")
    if provider == "anthropic":
        if not key:
            raise InfrastructureError("AIOPS_LLM_PROVIDER=anthropic 需要 AIOPS_LLM_API_KEY")
        return AnthropicLLM(key, model, base or "https://api.anthropic.com")
    raise InfrastructureError(f"未知的 AIOPS_LLM_PROVIDER: {provider}")
