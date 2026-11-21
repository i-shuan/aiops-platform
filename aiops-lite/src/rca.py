"""RCA：Python 不推理，只收集 4 包證據交給 LLM，再檢查它有沒有瞎編。

  1. 這個事故收斂的告警（系統、規則、開始時間、嚴重度、觸發當下的數值、是否已恢復）
  2. 系統上下游關係邊（掛載的 YAML）
  3. 事故時間窗內的錯誤日誌樣本與 Trace 錯誤片段（有就附，沒有就空）
  4. eqp_id／lot_id／carrier_id 在錯誤日誌裡的出現次數

LLM 連不上、逾時、格式不對、或點名了證據裡沒有的系統／機台／批號：退回「[未經分析] 原始異常摘要」，排程照常結束。

已知限制（照實寫）：
  - 送給 LLM 的日誌樣本**沒有遮罩**（封存的 aiops-platform 有遮罩出口，lite 沒有）。日誌裡的帳號、IP、業務機密
    要由產生日誌的系統先去識別化，或把 LLM_API_URL 指到公司內部的模型。
  - 幻覺檢查只擋「名稱不存在」：LLM 把存在的系統講錯因果，程式看不出來，所以第 2 行固定標 [LLM 推論]。
  - 工廠沒有 Trace 時，「集中在哪台機台／哪個批號」只是錯誤日誌的計數，不是因果證據。
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter
from datetime import datetime, timedelta
from typing import Any

import httpx

from src.grafana import Grafana, GrafanaError
from src.grouping import Incident

log = logging.getLogger("aiops_lite.rca")

MAX_LOG_SAMPLES = 20
MAX_TRACE_SAMPLES = 10
MAX_MESSAGE_CHARS = 300
MAX_SUMMARY_CHARS = 300
TOP_IDS = 10
EVIDENCE_PAD = timedelta(minutes=10)  # 事故時間窗前後多看幾分鐘：錯誤日誌常比告警觸發早出現（規則有 for 的等待時間）
ID_TOKEN = re.compile(r"[A-Za-z][A-Za-z0-9_.\-]*\d[A-Za-z0-9_.\-]*")  # 夾數字的識別碼樣式（EQP-CVD-07、LOT24A0031）

SYSTEM_PROMPT = """你是半導體工廠的 SRE。依照使用者給的四包證據判斷這個事故最可能的源頭與影響範圍。
規則：
- 只能引用證據裡出現的系統名稱、機台、批號、載具，不准自己編。
- 不要寫任何筆數、次數、時間：那些由程式另外顯示。
- 證據不足以判斷源頭時直接說「證據不足」，並說明還缺什麼。
- 只輸出一個 JSON 物件，不要 Markdown：
  {"summary": "一句繁體中文，120 字以內：可能的源頭、影響範圍、集中的機台或批號", "systems": ["你點名的系統"], "ids": ["你點名的機台／批號／載具"]}"""


def _dig(doc: dict[str, Any], path: str) -> Any:
    """讀 'service.name' 這種欄位：ES 文件可能是巢狀物件，也可能是帶點的扁平鍵。"""
    if path in doc:
        return doc[path]
    cur: Any = doc
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _members(incident: Incident) -> list[dict[str, Any]]:
    return sorted(incident.signals.values(), key=lambda m: (m["starts_at"], m["title"], m["service"]))


def raw_summary(incident: Incident) -> str:
    """不經 LLM 的原始摘要：各系統哪些告警響了（不含推論）。"""
    by_service: dict[str, list[str]] = {}
    for m in _members(incident):
        titles = by_service.setdefault(m["service"], [])
        if m["title"] not in titles:
            titles.append(m["title"])
    return "；".join(f"{svc}：{'、'.join(titles)}" for svc, titles in by_service.items())


def collect_evidence(incident: Incident, edges: list[tuple[str, str]], grafana: Grafana, *, es_datasource_uid: str,
                     log_index: str, trace_index: str, id_fields: list[str], now: datetime) -> dict[str, Any]:
    """組 4 包證據。日誌／Trace 撈不到（沒設資料源、Grafana 回錯）不擋分析：那一包留空並寫明原因。"""
    iso = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")
    evidence: dict[str, Any] = {
        "anomalies": [{"system": m["service"], "rule": m["title"], "severity": m["severity"],
                       "started": iso(m["starts_at"]), "status": m["status"], "values": m["values"]}
                      for m in _members(incident)],
        "edges": [{"upstream": a, "downstream": b} for a, b in edges],
        "error_logs": [], "error_traces": [], "id_counts": {f: {} for f in id_fields}, "notes": [],
    }
    if not es_datasource_uid:
        evidence["notes"].append("沒有設定日誌資料源：沒有日誌與 Trace 證據")
        return evidence
    try:
        found = grafana.search_errors(es_datasource_uid, log_index, trace_index or None, incident.services,
                                      incident.started_at - EVIDENCE_PAD, max(incident.last_seen_at, now))
    except GrafanaError as e:
        log.warning("事故 %s 撈不到日誌證據：%s", incident.uid, e)
        evidence["notes"].append(f"日誌與 Trace 撈取失敗：{e}")
        return evidence
    counts = {f: Counter() for f in id_fields}
    for doc in found["logs"]:
        for f in id_fields:
            value = _dig(doc, f)
            if value not in (None, ""):
                counts[f][str(value)] += 1
    evidence["id_counts"] = {f: dict(c.most_common(TOP_IDS)) for f, c in counts.items()}
    evidence["error_logs"] = [{
        "time": _dig(d, "@timestamp"), "system": _dig(d, "service.name"),
        "message": str(_dig(d, "message") or "")[:MAX_MESSAGE_CHARS],
        **{f: _dig(d, f) for f in id_fields if _dig(d, f) not in (None, "")}} for d in found["logs"][:MAX_LOG_SAMPLES]]
    evidence["error_traces"] = [{
        "time": _dig(d, "@timestamp"), "system": _dig(d, "service.name"), "span": _dig(d, "span.name"),
        "trace_id": _dig(d, "trace.id"), "error": str(_dig(d, "error.message") or "")[:MAX_MESSAGE_CHARS]}
        for d in found["traces"][:MAX_TRACE_SAMPLES]]
    if not found["logs"] and not found["traces"]:
        evidence["notes"].append("事故時間窗內沒有查到這些系統的錯誤日誌或錯誤 Trace")
    return evidence


def build_prompt(evidence: dict[str, Any]) -> str:
    dump = lambda x: json.dumps(x, ensure_ascii=False, indent=1)
    return (f"【1. 本次收斂的告警（依開始時間排序；values 是觸發當下規則各節點的數值）】\n{dump(evidence['anomalies'])}\n\n"
            f"【2. 系統上下游關係（upstream 的問題會影響 downstream）】\n{dump(evidence['edges'])}\n\n"
            f"【3. 事故時間窗內的錯誤日誌樣本】\n{dump(evidence['error_logs'])}\n"
            f"【3b. Trace 錯誤片段】\n{dump(evidence['error_traces'])}\n\n"
            f"【4. 錯誤日誌裡各識別碼的出現次數】\n{dump(evidence['id_counts'])}\n\n"
            f"【補充】{'；'.join(evidence['notes']) or '無'}")


def call_llm(api_url: str, api_key: str, model: str, prompt: str, transport: httpx.BaseTransport | None = None,
             timeout: float = 60.0) -> str:
    """OpenAI 相容的 chat completions。LLM_API_URL 可以是 …/v1 或完整的 …/chat/completions。失敗丟例外。"""
    url = api_url.rstrip("/")
    if not url.endswith("/chat/completions"):
        url += "/chat/completions"
    with httpx.Client(timeout=timeout, transport=transport) as http:
        r = http.post(url, headers={"Authorization": f"Bearer {api_key}"} if api_key else {}, json={
            "model": model, "temperature": 0,
            "messages": [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}]})
    r.raise_for_status()
    return str(r.json()["choices"][0]["message"]["content"])


def check_answer(text: str, evidence: dict[str, Any]) -> str:
    """幻覺檢查：LLM 點名的系統與機台／批號必須真的在證據或關係邊裡。通過回傳摘要文字，不通過丟 ValueError。"""
    body = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    answer = json.loads(body)
    summary = " ".join(str(answer["summary"]).split())
    if not summary:
        raise ValueError("summary 是空的")
    systems = {a["system"] for a in evidence["anomalies"]} | {s for e in evidence["edges"] for s in e.values()}
    ids = {v for counts in evidence["id_counts"].values() for v in counts}
    unknown = [s for s in answer.get("systems") or [] if s not in systems]
    unknown += [i for i in answer.get("ids") or [] if i not in ids]
    # 摘要文字裡夾數字的識別碼（機台、批號樣式）也要在證據裡找得到：防它只寫在句子裡、沒列進 ids
    corpus = json.dumps(evidence, ensure_ascii=False)
    unknown += [t for t in ID_TOKEN.findall(summary) if t not in corpus]
    if unknown:
        raise ValueError(f"點名了證據裡沒有的名稱：{sorted(set(map(str, unknown)))}")
    return summary[:MAX_SUMMARY_CHARS]


def analyze(incident: Incident, evidence: dict[str, Any], *, api_url: str, api_key: str, model: str,
            transport: httpx.BaseTransport | None = None) -> tuple[str, bool, str]:
    """回傳 (註記第 2 行, 是不是通過檢查的 LLM 推論, 沒通過的原因)。任何失敗都退回原始摘要，不丟例外。"""
    fallback = f"[未經分析] {raw_summary(incident)}"
    if not api_url or not model:
        return fallback, False, "沒有設定 LLM_API_URL／LLM_MODEL"
    try:
        summary = check_answer(call_llm(api_url, api_key, model, build_prompt(evidence), transport), evidence)
    except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as e:  # ValueError 含 JSON 解析失敗
        reason = f"{type(e).__name__}: {e}"[:300]
        log.warning("事故 %s 的 LLM 分析沒有採用：%s", incident.uid, reason)
        return fallback, False, reason
    return f"[LLM 推論] {summary}", True, ""
