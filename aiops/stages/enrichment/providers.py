from __future__ import annotations

import os

import httpx

from aiops.stages.enrichment import registry
from aiops.stages.enrichment.base import BaseLLM, LLMError
from aiops.stages.enrichment.registry import register


def _post(client: httpx.Client, url: str, headers: dict, body: dict) -> dict:
    try:
        resp = client.post(url, headers=headers, json=body)
    except httpx.HTTPError as e:
        raise LLMError(f"LLM 連不上：{e}") from e
    if not resp.is_success:
        raise LLMError(f"LLM 回應錯誤（HTTP {resp.status_code}）：{resp.text[:200]}")
    try:
        return resp.json()
    except ValueError as e:
        raise LLMError("LLM 回應不是 JSON") from e


class OpenAICompatibleLLM(BaseLLM):
    """OpenAI Chat Completions 相容 API（OpenAI、內網閘道、vLLM、Ollama 的 /v1 相容端點）。"""

    name = "openai"

    def __init__(self, api_key: str | None, model: str, base_url: str = "https://api.openai.com",
                 client: httpx.Client | None = None, timeout: float = 30.0, json_mode: bool = False):
        self.url = base_url.rstrip("/") + "/v1/chat/completions"
        self.headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.model = model
        self.client = client or httpx.Client(timeout=timeout)
        self.json_mode = json_mode  # True 時帶 response_format json_object（Anthropic 沒有對應參數，靠 prompt 約束）

    def complete(self, system: str, prompt: str) -> str:
        body: dict = {"model": self.model, "temperature": 0,
                      "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}]}
        if self.json_mode:
            body["response_format"] = {"type": "json_object"}
        try:
            payload = _post(self.client, self.url, self.headers, body)
        except LLMError as e:
            # 新一代（推理型）模型只接受預設 temperature，帶 0 會回 400 "temperature does not support 0"：
            # 拿掉 temperature 再送一次。這是請求參數相容，不是對模型輸出重試
            if "temperature" not in str(e) or "temperature" not in body:
                raise
            body.pop("temperature")
            payload = _post(self.client, self.url, self.headers, body)
        try:
            return payload["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as e:
            raise LLMError("LLM 回應缺少 choices[0].message.content") from e


class AnthropicLLM(BaseLLM):
    """Anthropic Messages API。"""

    name = "anthropic"

    def __init__(self, api_key: str, model: str, base_url: str = "https://api.anthropic.com",
                 client: httpx.Client | None = None, timeout: float = 30.0):
        self.url = base_url.rstrip("/") + "/v1/messages"
        self.headers = {"x-api-key": api_key, "anthropic-version": "2023-06-01"}
        self.model = model
        self.client = client or httpx.Client(timeout=timeout)

    def complete(self, system: str, prompt: str) -> str:
        payload = _post(self.client, self.url, self.headers, {
            "model": self.model, "max_tokens": 1500, "temperature": 0, "system": system,
            "messages": [{"role": "user", "content": prompt}],
        })
        try:
            return "".join(block["text"] for block in payload["content"] if block.get("type") == "text")
        except (KeyError, TypeError) as e:
            raise LLMError("LLM 回應缺少 content") from e


@register("noop", requires_model=False)
def _noop(_model: str, _base_url: str, _api_key: str | None) -> None:
    return None  # 完全不連外，呼叫端直接用模板


@register("openai")
def _openai(model: str, base_url: str, api_key: str | None) -> BaseLLM:
    # OpenAI 相容端點（內網閘道、vLLM、Ollama 的 /v1）可以沒有金鑰
    return OpenAICompatibleLLM(api_key, model, base_url or "https://api.openai.com")


@register("anthropic")
def _anthropic(model: str, base_url: str, api_key: str | None) -> BaseLLM:
    if not api_key:
        raise LLMError("provider=anthropic 需要環境變數 AIOPS_LLM_API_KEY")
    return AnthropicLLM(api_key, model, base_url or "https://api.anthropic.com")


def build_llm(provider: str, model: str | None = None, base_url: str | None = None,
              json_mode: bool = False) -> BaseLLM | None:
    """provider=noop 回傳 None（完全不連外）。金鑰只從環境變數 AIOPS_LLM_API_KEY 讀，不寫在 YAML。
    設定不全丟 LLMError，由呼叫端退回模板，不阻斷管線。
    環境變數預設值：AIOPS_LLM_MODEL、AIOPS_LLM_BASE_URL。供應商清單來自 registry（自動掃描本資料夾）。
    json_mode：只對 OpenAI 相容供應商有效（response_format json_object）；其他供應商忽略，factory 簽名不變。
    """
    provider = (provider or "noop").lower()
    llm = registry.build(
        provider,
        model or os.environ.get("AIOPS_LLM_MODEL", "").strip(),
        base_url or os.environ.get("AIOPS_LLM_BASE_URL", "").strip(),
        os.environ.get("AIOPS_LLM_API_KEY", "").strip() or None,
    )
    if json_mode and isinstance(llm, OpenAICompatibleLLM):
        llm.json_mode = True
    return llm
