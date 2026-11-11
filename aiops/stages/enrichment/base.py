from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from typing import Any

_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


class LLMError(Exception):
    """LLM 呼叫失敗（沒設定、連不上、逾時、額度、回應格式錯）。呼叫端一律退回模板，不讓整個流程失敗。"""


class BaseLLM(ABC):
    name: str = ""

    @abstractmethod
    def complete(self, system: str, prompt: str) -> str:
        """回傳模型的文字輸出；失敗丟 LLMError。"""


def parse_json_reply(text: str) -> dict[str, Any]:
    """模型常把 JSON 包在 ```json 區塊裡；去掉後解析，不是物件就丟 LLMError。"""
    if not isinstance(text, str):  # content: null（拒答、token 用完、tool call）也是「回應格式錯」，不是程式錯誤
        raise LLMError(f"LLM 回應不是文字（{type(text).__name__}）")
    try:
        data = json.loads(_FENCE.sub("", text.strip()))
    except json.JSONDecodeError as e:
        raise LLMError(f"LLM 回應不是合法 JSON：{e}") from e
    if not isinstance(data, dict):
        raise LLMError("LLM 回應不是 JSON 物件")
    return data
