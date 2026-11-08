import json
import re
from abc import ABC, abstractmethod
from typing import Any

_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


def parse_json_reply(text: str) -> dict[str, Any]:
    """模型常把 JSON 包在 ```json 區塊裡；去掉後解析，不是物件就丟 LLMError。"""
    try:
        data = json.loads(_FENCE.sub("", text.strip()))
    except json.JSONDecodeError as e:
        raise LLMError(f"LLM 回應不是合法 JSON：{e}") from e
    if not isinstance(data, dict):
        raise LLMError("LLM 回應不是 JSON 物件")
    return data


class LLMError(Exception):
    """LLM 呼叫失敗（連不上、逾時、額度、回應格式錯）。呼叫端一律退回模板，不讓整個流程失敗。"""


class BaseLLM(ABC):
    name: str = ""

    @abstractmethod
    def complete(self, system: str, prompt: str) -> str:
        """回傳模型的文字輸出；失敗丟 LLMError。"""
