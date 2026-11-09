"""從回饋文字抽調參規則：LLM 提議、平台驗，不通過就退回固定規則。產出一律只是提案。

驗證（全部要過）：
1. 只能改偵測器宣告的可調參數（tunable），值必須是數字且在範圍內。
2. 單次變動不超過現值的 50%。
3. 方向要跟回饋一致：誤報只能把 threshold 調高，漏報只能調低。
4. 要附理由（500 字內）。
"""
from __future__ import annotations

import json
import math
from typing import Any

SYSTEM = (
    "你是 AIOps 調參助理。根據維運人員的回饋，建議偵測器參數的調整。"
    "只能調整提供的可調參數，數值必須在範圍內。只輸出 JSON，不要其他文字。"
)
_FACTOR = {"false_positive": 1.2, "missed": 0.8}
_MAX_CHANGE = 0.5


def build_prompt(detector: str, verdict: str, text: str, tunable: dict, current: dict[str, float]) -> str:
    spec = {k: {"目前": current[k], "最小": low, "最大": high} for k, (_d, low, high) in tunable.items()}
    return (
        f"偵測器：{detector}\n回饋類型：{verdict}\n回饋內容：{text}\n可調參數：{json.dumps(spec, ensure_ascii=False)}\n\n"
        '請輸出：{"params": {"參數名": 新數值}, "reason": "理由"}。'
        "誤報（false_positive）代表太敏感，漏報（missed）代表太遲鈍。"
    )


def validate(reply: dict[str, Any], verdict: str, tunable: dict, current: dict[str, float]) -> str | None:
    params, reason = reply.get("params"), reply.get("reason")
    if not isinstance(params, dict) or not params:
        return "缺少 params"
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 500:
        return "缺少理由或超過 500 字"
    for key, value in params.items():
        if key not in tunable:
            return f"{key} 不是可調參數"
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return f"{key} 不是數字"
        _default, low, high = tunable[key]
        if not low <= value <= high:
            return f"{key}={value} 超出範圍 [{low}, {high}]"
        now = current[key]
        if now and abs(value - now) / abs(now) > _MAX_CHANGE:
            return f"{key} 變動超過 {int(_MAX_CHANGE * 100)}%（{now} -> {value}）"
        if key == "threshold" and ((verdict == "false_positive" and value < now) or (verdict == "missed" and value > now)):
            return f"threshold 調整方向與回饋相反（{verdict}：{now} -> {value}）"
    return None


def template(verdict: str, tunable: dict, current: dict[str, float]) -> tuple[dict[str, float], str]:
    """固定規則：誤報門檻 ×1.2、漏報 ×0.8（夾在範圍內）。沒有 threshold 就調第一個可調參數。"""
    key = "threshold" if "threshold" in tunable else next(iter(tunable))
    _default, low, high = tunable[key]
    value = round(min(high, max(low, current[key] * _FACTOR[verdict])), 4)
    return {key: value}, f"固定規則：{verdict} -> {key} {current[key]} -> {value}"


def quantile_step(target: float, tunable: dict, current: dict[str, float]) -> tuple[dict[str, float] | None, str]:
    """把分位數反推的目標門檻（誤報 -> 只能調高）變成一次提案：夾在範圍內、單次最多 +50%，並通過 validate。
    回 (參數, 說明)；不該調（目標不高於現行）或驗證不過回 (None, 原因)。"""
    _default, low, high = tunable["threshold"]
    now = current["threshold"]
    value = min(high, max(low, target))
    note = f"目標 {target} 超出範圍，夾到 {value}。" if value != target else ""
    if value <= now:
        return None, (f"目標門檻 {value} 不高於現行 {now}：誤報時窗裡這個偵測器的分數沒有越線，"
                      "誤報不是它造成的（例如是多數決裡其他節點）")
    cap = math.floor(now * (1 + _MAX_CHANGE) * 10000) / 10000 if now else value
    if value > cap:
        note += (f"目標 {round(value, 4)} 超過單次上限 {int(_MAX_CHANGE * 100)}%，這次先調到 {cap}"
                 f"（還差 {round(value - cap, 4)}；核准後若還誤報，再回饋一次會從 {cap} 往上算）。")
        value = cap
    value = round(value, 4)
    problem = validate({"params": {"threshold": value}, "reason": "quantile"}, "false_positive", tunable, current)
    if problem:
        return None, f"反推結果驗證不過：{problem}"
    return {"threshold": value}, note or f"新門檻 {value}。"
