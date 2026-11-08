import httpx

from aiops.llm.base import BaseLLM, LLMError


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
                 client: httpx.Client | None = None, timeout: float = 30.0):
        self.url = base_url.rstrip("/") + "/v1/chat/completions"
        self.headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.model = model
        self.client = client or httpx.Client(timeout=timeout)

    def complete(self, system: str, prompt: str) -> str:
        payload = _post(self.client, self.url, self.headers, {
            "model": self.model, "temperature": 0,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}],
        })
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
