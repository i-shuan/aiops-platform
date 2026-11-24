"""RCA：Python 不推理，只收集證據交給 LLM，再檢查它有沒有瞎編。

  1. 這個事故收斂的告警（系統、規則、開始時間、嚴重度、觸發當下的數值、是否已恢復）
  2. 系統上下游關係邊（設定檔）
  3. 事故時間窗內的錯誤日誌樣本與 Trace 錯誤片段（ES；有就附，沒有就空）
  4. eqp_id／lot_id／carrier_id 在錯誤日誌裡的出現次數
  5. 關聯時間線（ES）：出現最多次的批號／載具，在各系統的紀錄照時間排好。工廠沒有 trace id，用它代替
  6. 呼叫鏈（Tempo）：失敗請求實際的呼叫路徑。有埋點的 IT 服務才有

指標不在這裡：工廠（日誌轉指標）與 IT 服務（原生 metrics）都是 Grafana 規則判定之後以告警的形式進來（第 1 包）。

LLM 連不上、逾時、格式不對、或點名了證據裡沒有的系統／機台／批號：退回「[未經分析] 原始異常摘要」，排程照常結束。

已知限制（照實寫）：
  - 送給 LLM 的日誌樣本**沒有遮罩**（沒有遮罩出口）。日誌裡的帳號、IP、業務機密
    要由產生日誌的系統先去識別化，或把 llm.url 指到公司內部的模型。
  - 幻覺檢查只擋「名稱不存在」：LLM 把存在的系統講錯因果，程式看不出來，所以第 2 行固定標 [LLM 推論]。
  - 工廠沒有 Trace 時，「集中在哪台機台／哪個批號」只是錯誤日誌的計數，不是因果證據。
  - 關聯時間線靠各系統日誌的時間戳排序：系統之間時鐘差幾秒，先後順序就可能是錯的。
  - 呼叫鏈只看得到有埋點、而且沒被取樣掉的請求；Tempo 收到 Trace 後要幾秒才搜尋得到。
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import httpx

from src.grafana import ERROR_LEVELS, ERROR_STATUS, Grafana, GrafanaError, LogFields, TraceFields
from src.grouping import Incident

log = logging.getLogger("aiops_lite.rca")

# 夾數字的識別碼樣式（EQP-CVD-07、LOT24A0031）：幻覺檢查用它在摘要文字裡找「像機台／批號的字」（llm.id_pattern 的預設值）
DEFAULT_ID_PATTERN = r"[A-Za-z][A-Za-z0-9_.\-]*\d[A-Za-z0-9_.\-]*"

SYSTEM_PROMPT = """你是負責工廠系統與 IT 服務的 SRE。依照使用者給的證據判斷這個事故最可能的源頭與影響範圍。
規則：
- 只能引用證據裡出現的系統名稱、機台、批號、載具等識別碼，不准自己編。
- 不要寫任何筆數、次數、時間：那些由程式另外顯示。
- 「關聯時間線」只代表同一個批號或載具在各系統出現的先後，不代表因果；各系統的時鐘可能有幾秒誤差。
- 「呼叫鏈」是實際發生的呼叫：最下游、而且底下沒有其他失敗呼叫的那一段，比上游更接近源頭。
- 證據不足以判斷源頭時直接說「證據不足」，並說明還缺什麼。
- 只輸出一個 JSON 物件，不要 Markdown：
  {"summary": "一句繁體中文，120 字以內：可能的源頭、影響範圍、集中的機台或批號", "systems": ["你點名的系統"], "ids": ["你點名的機台／批號／載具"]}"""

TRACE_SEARCH_LIMIT = 20  # 向 Tempo 搜尋時最多要幾條，再從裡面挑最近的 max_traces 條
SPAN_ATTRIBUTES = ("peer.service", "db.system", "server.address", "http.response.status_code", "messaging.destination.name")


@dataclass(frozen=True)
class EvidenceLimits:
    """證據要撈多少、給 LLM 看多少（config.yaml 的 es.* 與 llm.max_*；都是選填，下面是預設值）。"""
    max_hits: int = 100                  # 每次 ES 查詢最多撈幾筆；識別碼的出現次數只統計這幾筆
    window_padding_minutes: int = 10     # 事故時間窗往前多看幾分鐘：錯誤日誌常比告警觸發早出現（規則有 for 的等待時間）
    error_levels: tuple[str, ...] = ERROR_LEVELS     # 日誌等級欄位是這些值才算錯誤日誌
    error_statuses: tuple[str, ...] = ERROR_STATUS   # span 狀態欄位是這些值才算錯誤 span
    max_log_samples: int = 20            # 給 LLM 的錯誤日誌樣本筆數
    max_trace_samples: int = 10          # 給 LLM 的 Trace 錯誤片段筆數
    max_message_chars: int = 300         # 單筆訊息截斷長度
    top_ids: int = 10                    # 每種識別碼只列出現次數前幾名
    timeline_fields: tuple[str, ...] = ("lot_id", "carrier_id")  # 哪些識別碼要串成跨系統的時間線；空的 = 不做
    max_timeline_ids: int = 2            # 最多替幾個關聯碼做時間線（取錯誤日誌裡出現最多次的）
    max_timeline_events: int = 30        # 每條時間線最多幾筆紀錄
    trace_id_field: str = "trace.id"     # 錯誤日誌裡記 trace id 的欄位：有的話直接用編號向 Tempo 取（不必等它搜尋得到）
    max_traces: int = 3                  # 最多取回幾條錯誤 Trace 的呼叫鏈（Tempo）
    max_trace_spans: int = 30            # 每條呼叫鏈最多列幾個 span（失敗的優先）


@dataclass(frozen=True)
class LlmOptions:
    """呼叫 LLM 與檢查回答的選項（config.yaml 的 llm.*；都是選填，下面是預設值）。"""
    timeout_seconds: float = 60.0
    temperature: float | None = None     # None = 不送，用模型的預設值
    max_summary_chars: int = 300         # 摘要超過就截斷（註記第 2 行）
    system_prompt: str = ""              # 空字串 = 用內建的 SYSTEM_PROMPT；來自 llm.system_prompt_file
    id_pattern: str = DEFAULT_ID_PATTERN  # 空字串 = 不檢查摘要文字裡的識別碼（systems／ids 清單照樣檢查）


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
                     log_index: str, trace_index: str, id_fields: list[str], now: datetime,
                     log_fields: LogFields = LogFields(), trace_fields: TraceFields = TraceFields(),
                     limits: EvidenceLimits = EvidenceLimits(), tempo_datasource_uid: str = "") -> dict[str, Any]:
    """組證據：告警、關係邊、錯誤日誌與識別碼統計（ES）、關聯時間線（ES）、呼叫鏈（Tempo）。
    任何一包撈不到（沒設資料源、Grafana 回錯）都不擋分析：那一包留空，並在 notes 寫明原因。"""
    iso = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")
    start = incident.started_at - timedelta(minutes=limits.window_padding_minutes)
    end = max(incident.last_seen_at, now)
    evidence: dict[str, Any] = {
        "anomalies": [{"system": m["service"], "rule": m["title"], "severity": m["severity"],
                       "started": iso(m["starts_at"]), "status": m["status"], "values": m["values"]}
                      for m in _members(incident)],
        "edges": [{"upstream": a, "downstream": b} for a, b in edges],
        "error_logs": [], "error_traces": [], "id_counts": {f: {} for f in id_fields},
        "timelines": [], "call_chains": [], "notes": [],
    }
    trace_ids: list[str] = []
    if es_datasource_uid:
        trace_ids = _collect_logs(evidence, incident, grafana, es_datasource_uid, log_index, trace_index, id_fields,
                                  start, end, log_fields, trace_fields, limits)
    else:
        evidence["notes"].append("沒有設定日誌資料源：沒有日誌證據")
    if tempo_datasource_uid:
        _collect_call_chains(evidence, incident, grafana, tempo_datasource_uid, start, end, limits, trace_ids)
    return evidence


def _collect_logs(evidence: dict[str, Any], incident: Incident, grafana: Grafana, uid: str, log_index: str,
                  trace_index: str, id_fields: list[str], start: datetime, end: datetime, log_fields: LogFields,
                  trace_fields: TraceFields, limits: EvidenceLimits) -> list[str]:
    """填好日誌相關的幾包證據；回傳錯誤日誌裡看到的 trace id（新的在前、不重複），給呼叫鏈直接取用。"""
    try:
        found = grafana.search_errors(uid, log_index, trace_index or None, incident.services, start, end,
                                      limits.max_hits, log_fields=log_fields, trace_fields=trace_fields,
                                      error_levels=limits.error_levels, error_statuses=limits.error_statuses)
    except GrafanaError as e:
        log.warning("事故 %s 撈不到日誌證據：%s", incident.uid, e)
        evidence["notes"].append(f"日誌撈取失敗：{e}")
        return []
    counts = {f: Counter() for f in id_fields}
    for doc in found["logs"]:
        for f in id_fields:
            value = _dig(doc, f)
            if value not in (None, ""):
                counts[f][str(value)] += 1
    evidence["id_counts"] = {f: dict(c.most_common(limits.top_ids)) for f, c in counts.items()}

    def entry(d: dict[str, Any], with_level: bool = False) -> dict[str, Any]:
        out = {"time": _dig(d, log_fields.time), "system": _dig(d, log_fields.service)}
        if with_level and log_fields.level:
            out["level"] = _dig(d, log_fields.level)
        out["message"] = str(_dig(d, log_fields.message) or "")[:limits.max_message_chars]
        out.update({f: _dig(d, f) for f in id_fields if _dig(d, f) not in (None, "")})
        return out

    evidence["error_logs"] = [entry(d) for d in found["logs"][:limits.max_log_samples]]
    evidence["error_traces"] = [{
        "time": _dig(d, trace_fields.time), "system": _dig(d, trace_fields.service), "span": _dig(d, trace_fields.span),
        "trace_id": _dig(d, trace_fields.trace_id), "error": str(_dig(d, trace_fields.error) or "")[:limits.max_message_chars]}
        for d in found["traces"][:limits.max_trace_samples]]
    if not found["logs"] and not found["traces"]:
        evidence["notes"].append("事故時間窗內沒有查到這些系統的錯誤日誌")
    elif found["logs"] and not log_fields.level:  # 讓 LLM 知道這批樣本怎麼來的，不要把每一筆都當成錯誤
        evidence["notes"].append("日誌沒有等級欄位：錯誤日誌樣本是依訊息文字裡的等級字樣挑的，可能混入不是錯誤的紀錄")

    # 關聯時間線：錯誤日誌裡出現最多次的批號／載具，回頭把它在各系統的紀錄（不限等級）照時間排出來。
    # 工廠沒有 trace id，批號與載具編號是天然的關聯碼；它只說明先後，不說明因果。
    picks = [(f, value) for f in limits.timeline_fields if f in counts
             for value, _n in counts[f].most_common(limits.max_timeline_ids)]
    picks = sorted(picks, key=lambda fv: -counts[fv[0]][fv[1]])[:limits.max_timeline_ids]
    for field in dict.fromkeys(f for f, _v in picks):
        values = [v for f, v in picks if f == field]
        try:
            lines = grafana.search_timeline(uid, log_index, field, values, start, end, limits.max_timeline_events,
                                            log_fields=log_fields)
        except GrafanaError as e:
            log.warning("事故 %s 撈不到 %s 的時間線：%s", incident.uid, field, e)
            evidence["notes"].append(f"關聯時間線撈取失敗：{e}")
            continue
        for value in values:
            got = lines.get(value) or {"hits": [], "total": 0}
            if got["hits"]:
                evidence["timelines"].append({
                    "key": field, "id": value, "events": [entry(d, with_level=True) for d in got["hits"]],
                    "systems": sorted({str(_dig(d, log_fields.service)) for d in got["hits"] if _dig(d, log_fields.service)}),
                    "truncated": got["total"] > len(got["hits"])})
    seen = (str(_dig(d, limits.trace_id_field) or "") for d in found["logs"]) if limits.trace_id_field else ()
    return list(dict.fromkeys(t for t in seen if t))


def _attributes(items: Any) -> dict[str, str]:
    """OTLP 的 attributes 陣列 → {key: 值}（只取字串化之後的值）。"""
    out = {}
    for item in items or []:
        value = item.get("value") or {}
        out[str(item.get("key"))] = str(next(iter(value.values()), ""))
    return out


def summarize_trace(trace: dict[str, Any], trace_id: str, limits: EvidenceLimits) -> dict[str, Any] | None:
    """把 Tempo 回的整條 Trace（batches／resourceSpans → scopeSpans → spans）壓成給 LLM 看的呼叫鏈。
    origin = 失敗、而且底下沒有其他失敗 span 的那一段（最接近源頭的失敗點）。"""
    spans = []
    for batch in trace.get("batches") or trace.get("resourceSpans") or (trace.get("trace") or {}).get("resourceSpans") or []:
        service = _attributes((batch.get("resource") or {}).get("attributes")).get("service.name", "")
        for scope in batch.get("scopeSpans") or []:
            for sp in scope.get("spans") or []:
                status = sp.get("status") or {}
                spans.append({"id": sp.get("spanId"), "parent": sp.get("parentSpanId") or None, "service": service,
                              "name": str(sp.get("name", "")), "start": int(sp.get("startTimeUnixNano") or 0),
                              "end": int(sp.get("endTimeUnixNano") or 0),
                              "failed": status.get("code") in ("STATUS_CODE_ERROR", 2, "2"),
                              "error": str(status.get("message") or "")[:limits.max_message_chars],
                              "attrs": {k: v for k, v in _attributes(sp.get("attributes")).items() if k in SPAN_ATTRIBUTES}})
    if not spans:
        return None
    by_id = {sp["id"]: sp for sp in spans}
    failed_parents = {sp["parent"] for sp in spans if sp["failed"] and sp["parent"]}
    spans.sort(key=lambda sp: sp["start"])
    t0 = spans[0]["start"]
    keep = [sp for sp in spans if sp["failed"] or sp["parent"] is None]          # 失敗的與入口一定留
    keep += [sp for sp in spans if sp not in keep][:max(0, limits.max_trace_spans - len(keep))]
    keep = sorted(keep[:limits.max_trace_spans], key=lambda sp: sp["start"])
    rows = []
    for sp in keep:
        row = {"service": sp["service"], "span": sp["name"],
               "called_by": by_id[sp["parent"]]["service"] if sp["parent"] in by_id else None,
               "offset_ms": (sp["start"] - t0) // 1_000_000, "duration_ms": max(0, sp["end"] - sp["start"]) // 1_000_000,
               "status": "error" if sp["failed"] else "ok"}
        if sp["failed"]:
            row["error"] = sp["error"]
            row["origin"] = sp["id"] not in failed_parents
        if sp["attrs"]:
            row["attributes"] = sp["attrs"]
        rows.append(row)
    systems = {sp["service"] for sp in spans if sp["service"]} | {sp["attrs"]["peer.service"] for sp in spans
                                                                  if sp["attrs"].get("peer.service")}
    return {"trace_id": trace_id, "spans": rows, "systems": sorted(systems), "truncated": len(spans) > len(rows)}


def _collect_call_chains(evidence: dict[str, Any], incident: Incident, grafana: Grafana, uid: str, start: datetime,
                         end: datetime, limits: EvidenceLimits, hinted_ids: list[str]) -> None:
    """先用錯誤日誌裡的 trace id 直接取（立即取得到，而且一定跟這次的錯誤有關）；不夠才用搜尋補。
    為什麼不只靠搜尋：實測 Tempo 2.9.0 收到 Trace 後 8～40 秒以上才搜尋得到，剛發生的事故常常搜不到自己的 Trace。"""
    if limits.max_traces < 1:
        return

    def add(trace_id: str) -> None:
        if trace_id and len(evidence["call_chains"]) < limits.max_traces \
                and trace_id not in {c["trace_id"] for c in evidence["call_chains"]}:
            trace = grafana.get_trace(uid, trace_id)
            chain = summarize_trace(trace, trace_id, limits) if trace else None
            if chain:
                evidence["call_chains"].append(chain)

    try:
        for trace_id in hinted_ids[:limits.max_traces * 2]:      # 有些 id 可能被取樣掉、取不回來，多試幾個
            add(trace_id)
        if len(evidence["call_chains"]) < limits.max_traces:
            # Tempo 的搜尋不保證先回最新的（實測會先回時間窗裡比較舊的）：多搜一些，自己挑離現在最近的幾條
            found = grafana.search_error_traces(uid, incident.services, start, end, max(TRACE_SEARCH_LIMIT, limits.max_traces))
            for item in sorted(found, key=lambda t: int(t.get("startTimeUnixNano") or 0), reverse=True):
                add(str(item.get("traceID") or ""))
    except GrafanaError as e:
        log.warning("事故 %s 撈不到呼叫鏈：%s", incident.uid, e)
        evidence["notes"].append(f"呼叫鏈（Tempo）撈取失敗：{e}")
        return
    if not evidence["call_chains"]:
        evidence["notes"].append("Tempo 在事故時間窗內沒有這些系統的錯誤 Trace（沒有埋點、被取樣掉、或還沒寫入）")


def build_prompt(evidence: dict[str, Any]) -> str:
    dump = lambda x: json.dumps(x, ensure_ascii=False, indent=1)
    text = (f"【1. 本次收斂的告警（依開始時間排序；values 是觸發當下規則各節點的數值）】\n{dump(evidence['anomalies'])}\n\n"
            f"【2. 系統上下游關係（upstream 的問題會影響 downstream）】\n{dump(evidence['edges'])}\n\n"
            f"【3. 事故時間窗內的錯誤日誌樣本】\n{dump(evidence['error_logs'])}\n"
            f"【3b. Trace 錯誤片段】\n{dump(evidence['error_traces'])}\n\n"
            f"【4. 錯誤日誌裡各識別碼的出現次數】\n{dump(evidence['id_counts'])}\n\n")
    if evidence.get("timelines"):
        text += ("【5. 關聯時間線：同一個批號／載具在各系統的紀錄，由舊到新（只代表先後，不代表因果；各系統時鐘可能有誤差）】\n"
                 f"{dump(evidence['timelines'])}\n\n")
    if evidence.get("call_chains"):
        text += ("【6. 呼叫鏈：失敗請求實際的呼叫路徑（called_by 是呼叫它的系統；origin=true 是底下沒有其他失敗呼叫的失敗點）】\n"
                 f"{dump(evidence['call_chains'])}\n\n")
    return text + f"【補充】{'；'.join(evidence['notes']) or '無'}"


def call_llm(api_url: str, api_key: str, model: str, prompt: str, transport: httpx.BaseTransport | None = None,
             timeout: float = 60.0, temperature: float | None = None, system_prompt: str = "") -> str:
    """OpenAI 相容的 chat completions。llm.url 可以是 …/v1 或完整的 …/chat/completions。失敗丟例外。
    temperature 預設不送（用模型自己的預設值）：有些模型只接受預設值，硬送 0 會被 400 拒絕（實測 gpt-6-luna 就是）。"""
    url = api_url.rstrip("/")
    if not url.endswith("/chat/completions"):
        url += "/chat/completions"
    with httpx.Client(timeout=timeout, transport=transport) as http:
        r = http.post(url, headers={"Authorization": f"Bearer {api_key}"} if api_key else {}, json={
            "model": model, **({} if temperature is None else {"temperature": temperature}),
            "messages": [{"role": "system", "content": system_prompt or SYSTEM_PROMPT},
                         {"role": "user", "content": prompt}]})
    r.raise_for_status()
    return str(r.json()["choices"][0]["message"]["content"])


def check_answer(text: str, evidence: dict[str, Any], *, max_summary_chars: int = 300,
                 id_pattern: str = DEFAULT_ID_PATTERN) -> str:
    """幻覺檢查：LLM 點名的系統與機台／批號必須真的在證據或關係邊裡。通過回傳摘要文字，不通過丟 ValueError。"""
    body = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    answer = json.loads(body)
    summary = " ".join(str(answer["summary"]).split())
    if not summary:
        raise ValueError("summary 是空的")
    systems = {a["system"] for a in evidence["anomalies"]} | {s for e in evidence["edges"] for s in e.values()}
    # 時間線與呼叫鏈裡實際出現的系統也算有憑據（例如呼叫鏈指到一個沒有告警、也不在關係邊裡的資料庫）
    systems |= {s for group in (evidence.get("timelines") or [], evidence.get("call_chains") or [])
                for item in group for s in item.get("systems") or []}
    ids = {v for counts in evidence["id_counts"].values() for v in counts}
    unknown = [s for s in answer.get("systems") or [] if s not in systems]
    unknown += [i for i in answer.get("ids") or [] if i not in ids]
    # 摘要文字裡夾數字的識別碼（機台、批號樣式）也要在證據裡找得到：防它只寫在句子裡、沒列進 ids
    corpus = json.dumps(evidence, ensure_ascii=False)
    # 泛稱（HTTP2、v2 這類）不在證據裡也會被擋：樣式不合用就改 llm.id_pattern，設成空字串 = 關掉這一項
    if id_pattern:
        unknown += [t for t in re.findall(id_pattern, summary) if t not in corpus]
    if unknown:
        raise ValueError(f"點名了證據裡沒有的名稱：{sorted(set(map(str, unknown)))}")
    return summary[:max_summary_chars]


def analyze(incident: Incident, evidence: dict[str, Any], *, api_url: str, api_key: str, model: str,
            transport: httpx.BaseTransport | None = None, options: LlmOptions = LlmOptions()) -> tuple[str, bool, str]:
    """回傳 (註記第 2 行, 是不是通過檢查的 LLM 推論, 沒通過的原因)。任何失敗都退回原始摘要，不丟例外。"""
    fallback = f"[未經分析] {raw_summary(incident)}"
    if not api_url or not model:
        return fallback, False, "沒有設定 llm.url、llm.model"
    try:
        prompt = build_prompt(evidence)
        log.debug("事故 %s 送給 LLM 的內容：\n%s", incident.uid, prompt)
        answer = call_llm(api_url, api_key, model, prompt, transport, timeout=options.timeout_seconds,
                          temperature=options.temperature, system_prompt=options.system_prompt)
        log.debug("事故 %s LLM 的原始回應：%s", incident.uid, answer)
        summary = check_answer(answer, evidence, max_summary_chars=options.max_summary_chars,
                               id_pattern=options.id_pattern)
    except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as e:  # ValueError 含 JSON 解析失敗
        reason = f"{type(e).__name__}: {e}"[:300]
        log.warning("事故 %s 的 LLM 分析沒有採用：%s", incident.uid, reason)
        return fallback, False, reason
    return f"[LLM 推論] {summary}", True, ""
