"""RCA 報告的總結敘述：LLM 寫、平台驗，不通過就退回模板。

依據驗證（全部要過）：
1. 格式：summary 是字串、claims 是 1～8 個 {"text", "evidence"}。
2. 每個 claim 都要引用至少一個「存在的」報告段落 key。
3. 不得提到事件群以外的已知實體（例如拓撲裡沒出事的服務）。
4. 文字裡出現的每個數字，都要能在證據段落裡找到原文。
"""
from __future__ import annotations

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


# ── 三行式止血摘要（enrichment.llm_briefing；docs/RCA_STEP2_DESIGN.md 第 4 節）──
# 跟上面的 summary／claims 總結分開：那一段看全部報告段落寫結論；這一段只吃壓縮過的硬證據，寫給值班的人 3 行就夠。

BRIEFING_SYSTEM = (
    "You are an expert SRE incident commander. Analyze the provided AIOps incident JSON and write a 3-line tactical briefing "
    "in Traditional Chinese.\n"
    "Constraints:\n"
    "1. ONLY reference services, resources, endpoints, trace ids and numbers that appear in the JSON. "
    "Do not invent anything else (no Kafka, no DNS, no other service unless it is in the JSON).\n"
    "2. Keep the numbers as they are in the JSON (rounding is fine).\n"
    "3. Output ONLY a JSON object with exactly these keys: summary_line, action_line, verification_line. "
    "summary_line starts with 【一句話根因】, action_line with 【立即止血動作】, verification_line with 【驗證與排除項】. "
    "Each line is one sentence, at most 200 characters."
)
BRIEFING_KEYS = ("summary_line", "action_line", "verification_line")
_PREFIX = {"summary_line": "【一句話根因】", "action_line": "【立即止血動作】", "verification_line": "【驗證與排除項】"}
# 常見的幻覺詞：context 裡沒有這個字就不准出現（有的話是真證據，放行）
_HALLUCINATION_WORDS = ("kafka", "rabbitmq", "dns", "redis", "oracle", "postgres", "mysql", "mongodb", "nginx", "istio",
                        "envoy", "kubernetes", "cdn", "firewall", "防火牆")
_TOLERANCE = 0.10
_MAX_LINE = 200


def build_context(ctx, incident) -> dict[str, Any]:
    """只放已經算好的東西，鍵固定；缺的給 None。不塞原始 metrics／span／log。"""
    from datetime import timedelta

    from aiops.engine.types import as_utc
    from aiops.stages.rca.logs import rank_patterns
    from aiops.stages.rca.spans import resource_label, sample_trace

    top = ctx.rca_root_candidates[0] if ctx.rca_root_candidates else None
    sample = None
    if ctx.rca_spans:
        s = sample_trace(ctx.rca_spans, as_utc(incident.start), as_utc(incident.end) + timedelta(minutes=1))
        if s is not None:
            failed = sorted({resource_label(c) for c in ctx.rca_spans
                             if c.trace_id == s.trace_id and c.error and c.kind not in ("", "app")})
            sample = {"trace_id": s.trace_id, "endpoint": s.name or None, "duration_ms": round(s.duration_ms, 1),
                      "status_code": s.status_code or None, "failed": bool(s.error) or s.status_code >= 500,
                      "failed_resource": "、".join(failed) or None}
    logs = []
    if ctx.rca_logs:
        for p in rank_patterns(ctx.rca_logs, ctx.rca_logs_baseline, ctx.rca_log_window_minutes)[:3]:
            logs.append({"service": p["service"], "error_type": p["error_type"] or None, "template": p["template"][:160],
                         "count": round(p["during"]), "ratio": round(p["ratio"], 1)})
    minutes = max(0, round((as_utc(incident.end) - as_utc(incident.start)).total_seconds() / 60))
    return {
        "incident_meta": {"entities": sorted(set(incident.entities)), "duration_minutes": minutes,
                          "severity": incident.severity or None},
        "root_cause": ({"candidate": top.entity, "score": round(top.score, 3), "dependency": top.dependency or None,
                        "attribution": top.attribution or None} if top else None),
        "root_note": ctx.rca_root_note or None,
        "sample_trace": sample,
        "top_error_logs": logs,
        "matched_runbook": ctx.rca_runbook,
    }


def build_briefing_prompt(context: dict[str, Any]) -> str:
    return "Incident context (JSON):\n" + json.dumps(context, ensure_ascii=False, indent=2)


def _numbers_in(obj: Any, out: set[float]) -> None:
    if isinstance(obj, bool):
        return
    if isinstance(obj, (int, float)):
        out.add(float(obj))
    elif isinstance(obj, str):
        out.update(float(n) for n in _NUMBER.findall(obj))
    elif isinstance(obj, dict):
        for v in obj.values():
            _numbers_in(v, out)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            _numbers_in(v, out)


def _number_ok(value: float, allowed: set[float]) -> bool:
    return any(a == value or abs(a - value) <= max(abs(a) * _TOLERANCE, 0.5) for a in allowed)


def briefing_whitelist(context: dict[str, Any]) -> set[str]:
    names = set(context["incident_meta"]["entities"])
    rc = context.get("root_cause") or {}
    if rc.get("candidate"):
        names.add(rc["candidate"])
    if (rc.get("attribution") or {}).get("resource"):
        names.add(rc["attribution"]["resource"])
    st = context.get("sample_trace") or {}
    for key in ("endpoint", "failed_resource", "trace_id"):
        if st.get(key):
            names.add(str(st[key]))
    if context.get("matched_runbook"):
        names.add(context["matched_runbook"]["id"])
    return names


def validate_briefing(reply: dict[str, Any], context: dict[str, Any], known_entities: set[str]) -> str | None:
    """通過回 None；不通過回原因。
    1. 三個鍵都要是非空字串、≤ 200 字、以規定的【】開頭。
    2. 實體白名單：known_entities（拓撲裡的服務）之中不在白名單的名字不准出現。
    3. 數字：文字裡每個數字都要在 context 的數值（含字串裡的數字）±10% 內找到。
    4. 黑名單詞（kafka、dns…）只有 context 裡有才放行。"""
    for key in BRIEFING_KEYS:
        line = reply.get(key)
        if not isinstance(line, str) or not line.strip():
            return f"缺少 {key}"
        if len(line) > _MAX_LINE:
            return f"{key} 超過 {_MAX_LINE} 字"
        if not line.strip().startswith(_PREFIX[key]):
            return f"{key} 必須以 {_PREFIX[key]} 開頭"
    text = "\n".join(reply[k] for k in BRIEFING_KEYS)
    allowed_names = briefing_whitelist(context)
    for name in sorted(known_entities - allowed_names):
        if name and name in text:
            return f"提到白名單以外的實體：{name}"
    allowed_numbers: set[float] = set()
    _numbers_in(context, allowed_numbers)
    for num in _NUMBER.findall(text):
        if not _number_ok(float(num), allowed_numbers):
            return f"數字 {num} 在證據中找不到（±10%）"
    haystack = json.dumps(context, ensure_ascii=False).lower()
    lowered = text.lower()
    for word in _HALLUCINATION_WORDS:
        if word in lowered and word not in haystack:
            return f"提到證據裡沒有的 {word}"
    return None


def briefing_template(context: dict[str, Any]) -> dict[str, str]:
    """不靠 LLM 的三行：全部從 attribution、runbook、範例 trace 拼，數字一定精確。"""
    rc = context.get("root_cause") or {}
    a = rc.get("attribution") or {}
    rb = context.get("matched_runbook") or {}
    st = context.get("sample_trace") or {}
    entities = "、".join(context["incident_meta"]["entities"]) or "事件群"
    if rc.get("candidate") and rc.get("dependency"):
        kind = "/".join(p for p in (a.get("kind", ""), a.get("subtype", "")) if p)
        summary = (f"{_PREFIX['summary_line']}{rc['candidate']} 等待 {a['resource']}（{kind}）"
                   f"{a['wait_ms']['before']}→{a['wait_ms']['during']} ms，程式自身 {a['self_ms']['before']}→{a['self_ms']['during']} ms，"
                   f"資源失敗 {a['failures']['before']}→{a['failures']['during']} 筆")
        verify = f"{_PREFIX['verification_line']}{rc['candidate']} 程式自身耗時沒有惡化，排除它本身的程式與機器"
        if st.get("failed_resource"):
            verify += f"；範例 trace {st['trace_id']} 入口 {st.get('status_code') or '失敗'}，失敗在 {st['failed_resource']}"
    elif rc.get("candidate"):
        summary = f"{_PREFIX['summary_line']}{rc['candidate']} 自身耗時或自身錯誤率上升（分數 {rc['score']}），沒有單一下游資源主導"
        verify = f"{_PREFIX['verification_line']}依 trace 佐證與日誌段落確認 {rc['candidate']} 的程式與近期變更"
        if st.get("trace_id"):
            verify += f"；範例 trace {st['trace_id']}"
    else:
        note = (context.get("root_note") or "").split("：", 1)[-1][:120]
        summary = f"{_PREFIX['summary_line']}證據不足，不指認單一服務（{entities}）：{note}"
        verify = f"{_PREFIX['verification_line']}先查共用的資料源、網路與未被追蹤的依賴，比對同時間其他告警"
    first = f"[{rb['id']}] {rb['actions'][0]}" if rb.get("actions") else "比對事件前後的變更紀錄（部署、設定、配方）"
    action = _PREFIX["action_line"] + first
    return {"summary_line": summary[:_MAX_LINE], "action_line": action[:_MAX_LINE], "verification_line": verify[:_MAX_LINE]}
