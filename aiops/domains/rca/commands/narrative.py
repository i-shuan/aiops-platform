"""RCA 報告的總結敘述：LLM 寫、平台驗，不通過就退回模板。

依據驗證（全部要過）：
1. 格式：summary 是字串、claims 是 1～8 個 {"text", "evidence"}。
2. 每個 claim 都要引用至少一個「存在的」報告段落 key。
3. 不得提到事件群以外的已知實體（例如拓撲裡沒出事的服務）。
4. 文字裡出現的每個數字，都要能在證據段落裡找到原文。
"""
import json
import re
from typing import Any

SYSTEM = (
    "你是 AIOps 根因分析助理。只能根據使用者提供的報告段落寫結論，"
    "不得加入段落以外的實體、數字或推測。只輸出 JSON，不要其他文字。"
)
_NUMBER = re.compile(r"\d+(?:\.\d+)?")


def build_prompt(sections: list[dict[str, str]]) -> str:
    return (
        "以下是事件群的報告段落（key 是段落代號）：\n"
        + json.dumps(sections, ensure_ascii=False, indent=2)
        + '\n\n請輸出：{"summary": "300 字以內的總結", '
        '"claims": [{"text": "一句結論", "evidence": ["引用的段落 key", ...]}]}，claims 1～8 條。'
    )


def validate(reply: dict[str, Any], sections: list[dict[str, str]], incident_entities: set[str],
             known_entities: set[str]) -> str | None:
    """通過回 None；不通過回原因。"""
    keys = {s["key"] for s in sections}
    summary, claims = reply.get("summary"), reply.get("claims")
    if not isinstance(summary, str) or not summary.strip() or len(summary) > 600:
        return "summary 缺少或超過 600 字"
    if not isinstance(claims, list) or not 1 <= len(claims) <= 8:
        return "claims 必須是 1～8 條"
    for c in claims:
        if not isinstance(c, dict) or not isinstance(c.get("text"), str) or not c["text"].strip():
            return "claim 缺少 text"
        evidence = c.get("evidence")
        if not isinstance(evidence, list) or not evidence or not set(evidence) <= keys:
            return f"claim 引用了不存在的段落：{evidence}"

    text = summary + "\n" + "\n".join(c["text"] for c in claims)
    for name in sorted(known_entities - incident_entities):
        if name and name in text:
            return f"提到事件群以外的實體：{name}"
    evidence_text = "\n".join(s["body"] for s in sections)
    for num in _NUMBER.findall(text):
        if num not in evidence_text:
            return f"數字 {num} 在證據中找不到"
    return None


def template(sections: list[dict[str, str]]) -> dict[str, Any]:
    """不靠 LLM 的總結：取最關鍵的段落第一行，每條都附上來源段落。"""
    by_key = {s["key"]: s for s in sections}
    order = ("root_cause_candidates", "summary", "propagation_path", "trace_evidence", "suggested_actions")
    claims = [{"text": by_key[k]["body"].splitlines()[0], "evidence": [k]}
              for k in order if k in by_key and by_key[k]["body"].strip()]
    summary = claims[0]["text"] if claims else "資料不足，無法產生總結"
    return {"summary": summary, "claims": claims}
