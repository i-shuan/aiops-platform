"""RCA：Python 不推理，只收集證據交給 LLM，再檢查它有沒有瞎編。

  1. 這個事故收斂的告警（系統、規則、開始時間、嚴重度、觸發當下的數值、是否已恢復）
  2. 系統上下游關係邊（設定檔）
  3. 事故時間窗內的錯誤日誌樣本（有就附，沒有就空）
  4. eqp_id／lot_id／carrier_id 在錯誤日誌裡的出現次數
  5. 關聯時間線：出現最多次的批號／載具，在各系統的紀錄照時間排好。工廠沒有 trace id，用它代替
  6. 呼叫鏈：失敗請求實際的呼叫路徑。有埋點的 IT 服務才有
  7. 指標走勢（VictoriaMetrics，Prometheus 相容）：設定的查詢在事故前後的數值變化。有設定才有

錯誤日誌交給 LLM 之前先處理過（_profile_logs）：同類去重、各系統最早的錯誤不被擠掉、事故前後各出現幾筆、警告。
另外由程式產生「證據概況」（overview）：手上有哪些、缺哪些與原因，寫在註記上，也交給 LLM。
這些處理都只是歸類與計數：什麼算異常仍然由 Grafana 規則決定，這裡沒有任何門檻。

LLM 連不上、逾時、格式不對、或點名了證據裡沒有的系統／機台／批號：退回「[未經分析] 原始異常摘要」，排程照常結束。

已知限制（照實寫）：
  - 送給 LLM 的日誌樣本**沒有遮罩**（沒有遮罩出口）。日誌裡的帳號、IP、業務機密
    要由產生日誌的系統先去識別化，或把 llm.url 指到公司內部的模型。
  - 幻覺檢查只擋「名稱不存在」：LLM 把存在的系統講錯因果，程式看不出來，所以第 2 行固定標 [LLM 推論]。
  - 工廠沒有 Trace 時，「集中在哪台機台／哪個批號」只是錯誤日誌的計數，不是因果證據。
  - 關聯時間線靠各系統日誌的時間戳排序：系統之間時鐘差幾秒，先後順序就可能是錯的。
  - 呼叫鏈只看得到有埋點、而且沒被取樣掉的請求。
"""
from __future__ import annotations

import json
import logging
import re
import statistics
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from src.grafana import Grafana, GrafanaError
from src.logs import ERROR_LEVELS, MESSAGE_FIELD, TIME_FIELD, WARN_LEVELS, LogFields
from src.grouping import Incident

log = logging.getLogger("aiops_lite.rca")

# 夾數字的識別碼樣式（EQP-CVD-07、LOT24A0031）：幻覺檢查用它在摘要文字裡找「像機台／批號的字」（llm.id_pattern 的預設值）
DEFAULT_ID_PATTERN = r"[A-Za-z][A-Za-z0-9_.\-]*\d[A-Za-z0-9_.\-]*"

SYSTEM_PROMPT = """你是負責工廠系統與 IT 服務的 SRE。依照使用者給的證據，說明這個事故為什麼會發生（哪裡出了什麼事、怎麼造成現在的現象）、影響到哪裡。
根因不一定是「哪個系統是源頭」：也可能是資源用完、設定或憑證有問題、相依的元件連不上、流量變了。有多少證據就分析多少，不要因為少了某一種證據就不分析。
規則：
- 只能引用證據裡出現的系統名稱、機台、批號、載具等識別碼，不准自己編。
- 不要寫任何筆數、次數、時間：那些由程式另外顯示。
- 「關聯時間線」只代表同一個批號或載具在各系統出現的先後，不代表因果；各系統的時鐘可能有幾秒誤差。
- 「呼叫鏈」是實際發生的呼叫：最下游、而且底下沒有其他失敗呼叫的那一段，比上游更接近源頭。
- 「指標走勢」是數值在事故前後的變化（baseline 是事故前的中位數）：它說明哪個數值先變、變了多少，本身不代表因果。
- 「關係邊」只說明誰可能影響誰。某個系統在上游，不能當成它就是源頭的依據；要有指向它的證據：它自己的錯誤日誌或警告、它的指標變化、
  呼叫鏈停在它，或別的系統的錯誤訊息明確寫連不上它、等不到它（連線被拒、逾時並指名它）。
- 「錯誤種類與事故前後的次數」：事故前就以差不多的頻率出現的錯誤是平常就有的，不能當成這次事故的原因；
  事故前沒出現過（new=true）或明顯變多的才是線索。沒有次數的錯誤不能推論它是新的還是平常就有的。
- 「警告」常常寫出後來出錯的原因（資源快用完、重試變多）：錯誤訊息本身沒寫原因時，看同一個系統在出錯前的警告。
- 一定要列出 1 到 3 個可能的原因（candidates），由最可能排到最不可能。每個寫：
  source＝系統名稱；
  reason＝一句白話，講「發生了什麼、為什麼」：出了什麼事、機制是什麼、依據是哪一種證據裡的什麼，直接引用內容。不要只寫「它是源頭」；
  kind＝evidence 或 inference。evidence：證據直接寫出了這個原因（錯誤訊息自己講了原因、呼叫鏈停在它、它的指標先變壞）。
        inference：證據沒有直接寫，是你從現象、名稱、先後或常識推測的。可以推測，但要說是從哪一項證據推的；
  confidence＝0 到 100 的整數。
- 信心分數怎麼給（依據有多直接，不是機率）：
  70～100＝有直接指向它的證據，而且沒有別的合理解釋（它自己這次才出現的錯誤、呼叫鏈停在它、它的指標先變壞、別的系統的錯誤訊息指名連不上它）。
  40～69＝有指向它的證據，但還有別的可能。
  0～39＝只是從告警的名稱、觸發的數值、哪一筆先響、關係邊推測，沒有直接證據。kind 是 inference 的一律給 39 以下。
- 就算只有告警，也要根據告警的名稱、數值、先後與關係邊列出可能的原因（分數給 39 以下），不要只回「無法判定」。
  有別的合理可能就列出來，不要只列一個。連推測的依據都沒有時 candidates 才可以是空的。
- summary 是寫給值班人員看的一句結論，用白話講這個問題為什麼會發生：哪裡出了什麼事、造成什麼現象。
  不要只寫哪個系統是源頭，也不要抱怨缺了什麼證據（那個由程式另外顯示）。分數全部在 39 以下時，開頭寫「目前只能推測」。
- 只輸出一個 JSON 物件，不要 Markdown：
  {"summary": "繁體中文，80 字以內", "candidates": [{"source": "系統名稱", "kind": "evidence", "confidence": 85, "reason": "一句話"}], "systems": ["你點名的系統"], "ids": ["你點名的機台／批號／載具"]}"""

MAX_CANDIDATES = 3
MIN_NAME_CHARS = 3   # 名稱至少這麼長，才用「原樣出現在證據文字裡」當憑據：一兩個字的名稱在任何文字裡都找得到。沒有依據的數字，未驗證泛化
DIRECT_EVIDENCE = ("error_logs", "error_patterns", "warnings", "timelines", "call_chains", "metrics")
# 信心分數的分段（提示詞裡跟模型講的同一套）。分數是模型自評「依據有多直接」，不是機率、沒有校準過。
CONFIDENCE_HIGH, CONFIDENCE_MEDIUM = 70, 40
NO_DIRECT_EVIDENCE_CAP = 30   # 沒有直接證據指向的系統，分數最高只到這裡（落在「只是推測」那一段）
LEGACY_CONFIDENCE = {"高": 85, "中": 55, "低": 25}   # 舊格式的高／中／低換成各段的中間值

TRACE_SEARCH_LIMIT = 20  # 向 Trace 資料庫搜尋時最多要幾條，再從裡面挑最近的 max_traces 條
# 呼叫鏈上保留的 span 屬性：能指出「下一站是誰」或「這一段是佇列」的那幾個。佇列的兩種命名都留
# （新版 messaging.destination.name；舊版 messaging.destination，實測 pika 的自動埋點送的是舊版）。
SPAN_ATTRIBUTES = ("peer.service", "db.system", "server.address", "http.response.status_code", "messaging.system",
                   "messaging.destination.name", "messaging.destination")


@dataclass(frozen=True)
class EvidenceLimits:
    """證據要撈多少、給 LLM 看多少（config.yaml 的 es.* 與 llm.max_*；都是選填，下面是預設值）。"""
    max_hits: int = 100                  # 錯誤日誌最多撈幾筆（最新的在前）；識別碼的出現次數只統計這幾筆
    window_padding_minutes: int = 10     # 事故時間窗往前多看幾分鐘：錯誤日誌常比告警觸發早出現（規則有 for 的等待時間）
    error_levels: tuple[str, ...] = ERROR_LEVELS     # 日誌等級欄位是這些值才算錯誤日誌
    max_log_samples: int = 20            # 給 LLM 的錯誤日誌樣本筆數
    max_message_chars: int = 300         # 單筆訊息截斷長度
    top_ids: int = 10                    # 每種識別碼只列出現次數前幾名
    timeline_fields: tuple[str, ...] = ("lot_id", "carrier_id")  # 哪些識別碼要串成跨系統的時間線；空的 = 不做
    max_timeline_ids: int = 2            # 最多替幾個關聯碼做時間線（取錯誤日誌裡出現最多次的）
    max_timeline_events: int = 30        # 每條時間線最多幾筆紀錄
    trace_id_field: str = "trace_id"     # 錯誤日誌裡記 trace id 的欄位：有的話直接用編號取那條 Trace（一定跟這次的錯誤有關）
    max_traces: int = 3                  # 最多整理幾條錯誤 Trace 的呼叫鏈
    max_trace_spans: int = 30            # 每條呼叫鏈最多列幾個 span（失敗的優先）
    baseline_minutes: int = 30           # 「事故前」看多久：同一種錯誤在事故前這段時間出現幾次，用來分辨平常就有的與新出現的
    max_log_patterns: int = 6            # 最多替幾種錯誤比較事故前後的次數；0 = 不比較
    warn_levels: tuple[str, ...] = WARN_LEVELS   # 日誌等級是這些值算警告；空的 = 不撈警告
    max_warn_patterns: int = 5           # 最多給 LLM 看幾種警告；0 = 不撈警告


SERVICE_PLACEHOLDER = "$service"   # 指標查詢裡的這個字會換成系統名稱


@dataclass(frozen=True)
class MetricQuery:
    """一條要在事故時查的指標（config.yaml 的 metrics.queries）。expr 是 PromQL／MetricsQL，裡面的 $service 會換成系統名稱。
    services 有寫就只對這幾個系統查（例如佇列堆積只對 broker 查）；空的 = 事故裡每個系統都查。"""
    name: str
    expr: str
    services: tuple[str, ...] = ()


@dataclass(frozen=True)
class MetricOptions:
    """指標走勢這一包證據（config.yaml 的 metrics.*）。datasource_uid 或 queries 是空的 = 不查。"""
    datasource_uid: str = ""             # Grafana 上 Prometheus 相容資料源（Prometheus、VictoriaMetrics…）的 uid
    queries: tuple[MetricQuery, ...] = ()
    lookback_minutes: int = 60           # 事故開始前往回看多久當基準：太短會把事故前的爬升也算進基準
    step_seconds: int = 60               # 取樣間隔；要不小於指標的抓取間隔，不然每一格都是同一個值
    max_points: int = 12                 # 每條序列給 LLM 看幾個點（整段時間窗平均抽樣）
    max_series: int = 5                  # 一條查詢回很多條序列時（例如每個佇列一條）只留事故期間數值最大的幾條


@dataclass(frozen=True)
class LlmOptions:
    """呼叫 LLM 與檢查回答的選項（config.yaml 的 llm.*；都是選填，下面是預設值）。"""
    timeout_seconds: float = 60.0
    temperature: float | None = None     # None = 不送，用模型的預設值
    max_summary_chars: int = 300         # 摘要超過就截斷（註記第 2 行）
    system_prompt: str = ""              # 空字串 = 用內建的 SYSTEM_PROMPT；來自 llm.system_prompt_file
    id_pattern: str = DEFAULT_ID_PATTERN  # 空字串 = 不檢查摘要文字裡的識別碼（systems／ids 清單照樣檢查）


def _dig(doc: dict[str, Any], path: str) -> Any:
    """讀 'service.name' 這種欄位：日誌是帶點的扁平鍵（src/logs.py 保證）；巢狀物件也讀得到。"""
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


def collect_evidence(incident: Incident, edges: list[tuple[str, str]], grafana: Grafana, *, logs: Any = None,
                     traces: Any = None, id_fields: list[str], now: datetime, limits: EvidenceLimits = EvidenceLimits(),
                     metrics: MetricOptions = MetricOptions()) -> dict[str, Any]:
    """組證據：告警、關係邊、錯誤日誌與識別碼統計、關聯時間線、呼叫鏈、指標走勢。全部經 Grafana 的資料源代理。
    logs 是 src/logs.py 的 VictoriaLogs 或 Elasticsearch，traces 是 src/traces.py 的 VictoriaTraces 或 Tempo；None = 沒接。
    這裡不管後面是哪一種：兩種來源回的日誌與 Trace 是同一種形狀。
    任何一包撈不到（沒設資料源、Grafana 回錯）都不擋分析：那一包留空，並在 notes 寫明原因。"""
    iso = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")
    start = incident.started_at - timedelta(minutes=limits.window_padding_minutes)
    end = max(incident.last_seen_at, now)
    evidence: dict[str, Any] = {
        "anomalies": [{"system": m["service"], "rule": m["title"], "severity": m["severity"],
                       "started": iso(m["starts_at"]), "status": m["status"], "values": m["values"]}
                      for m in _members(incident)],
        "edges": [{"upstream": a, "downstream": b} for a, b in edges],
        "error_logs": [], "id_counts": {f: {} for f in id_fields},
        "timelines": [], "call_chains": [], "metrics": [], "notes": [],
        "error_patterns": [], "warnings": [],
        # 每一包的狀態，給 overview() 用：off 沒接／failed 查詢失敗／empty 查了但沒有／ok 有
        "sources": {"logs": "off", "call_chains": "off", "metrics": "off"},
    }
    trace_ids: list[str] = []
    if logs is not None:
        evidence["log_source"] = {"name": logs.name, "scope_hint": logs.scope_hint}
        trace_ids = _collect_logs(evidence, incident, logs, id_fields, start, end, limits)
    else:
        evidence["notes"].append("沒有設定日誌資料源：沒有日誌證據")
    if traces is not None and limits.max_traces >= 1:
        evidence["trace_source"] = traces.name
        _collect_call_chains(evidence, incident, traces, start, end, limits, trace_ids)
    if metrics.datasource_uid and metrics.queries:
        _collect_metrics(evidence, incident, grafana, metrics, end)
    evidence["window"] = {"start": iso(start), "end": iso(end)}
    return evidence


def promql_string(value: str) -> str:
    """把系統名稱放進 PromQL 雙引號字串之前的跳脫。系統名稱來自告警的標籤（外部輸入），不跳脫的話名稱裡的引號
    可以把查詢截斷、接上別的條件。"""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _round(value: float) -> float:
    return float(f"{value:.4g}")   # 四位有效數字：給 LLM 看走勢夠用，不會被一長串小數淹沒


def summarize_series(points: list[tuple[datetime, float]], incident_start: datetime, max_points: int) -> dict[str, Any] | None:
    """一條序列壓成：事故前的基準（中位數）、事故期間的最小／最大／最後一個值、相對基準的倍數、平均抽樣的幾個點。
    只做算術，不判斷「算不算異常」：那是 Grafana 規則的事，這裡沒有任何門檻。事故前沒有資料時 baseline 是 None。"""
    if not points:
        return None
    before = [v for t, v in points if t < incident_start]
    during = [(t, v) for t, v in points if t >= incident_start]
    baseline = statistics.median(before) if before else None
    out: dict[str, Any] = {"baseline": None if baseline is None else _round(baseline)}
    if during:
        values = [v for _t, v in during]
        peak_at = max(during, key=lambda tv: tv[1])[0]
        out["during"] = {"min": _round(min(values)), "max": _round(max(values)), "last": _round(values[-1]),
                         "max_at": peak_at.strftime("%H:%M:%SZ")}
        if baseline:   # 基準是 0 或沒有基準就不算倍數（除以零沒有意義）
            out["last_vs_baseline"] = _round(values[-1] / baseline)
            out["max_vs_baseline"] = _round(max(values) / baseline)
    step = max(1, -(-len(points) // max_points))   # 無條件進位的整數除法
    picked = points[::step]
    if picked[-1] != points[-1]:
        picked.append(points[-1])   # 最後一個點一定留：它是「現在」
    out["points"] = [{"t": t.strftime("%H:%M:%S"), "v": _round(v)} for t, v in picked]
    return out


def _collect_metrics(evidence: dict[str, Any], incident: Incident, grafana: Grafana, options: MetricOptions,
                     end: datetime) -> None:
    """指標走勢：對事故裡的每個系統跑設定的每條查詢，時間窗是事故開始前 lookback_minutes 到現在。
    一條查詢失敗（語法錯、資料源連不上）只記進 notes，其他查詢照跑；查得到但這個系統沒有資料的不列、也不算錯。"""
    start = incident.started_at - timedelta(minutes=options.lookback_minutes)
    failed: list[str] = []
    for query in options.queries:
        for service in incident.services:
            if query.services and service not in query.services:
                continue
            expr = query.expr.replace(SERVICE_PLACEHOLDER, promql_string(service))
            try:
                series = grafana.query_range(options.datasource_uid, expr, start, end, options.step_seconds)
            except GrafanaError as e:
                log.warning("事故 %s 查不到指標「%s」（%s）：%s", incident.uid, query.name, service, e)
                failed.append(f"{query.name}（{service}）")
                continue
            series = [s for s in series if s["points"]]
            # 一條查詢回很多條序列時，留事故期間數值最大的幾條（沒有事故期間的點就看整段）
            peak = lambda s: max((v for t, v in s["points"] if t >= incident.started_at), default=max(v for _t, v in s["points"]))
            kept = sorted(series, key=peak, reverse=True)[:options.max_series]
            for item in kept:
                summary = summarize_series(item["points"], incident.started_at, options.max_points)
                labels = {k: v for k, v in item["labels"].items() if k != "__name__"}
                evidence["metrics"].append({"system": service, "metric": query.name, **({"labels": labels} if labels else {}),
                                            **summary})
            if len(series) > len(kept):
                evidence["notes"].append(f"指標「{query.name}」（{service}）有 {len(series)} 條序列，只列數值最大的 {len(kept)} 條")
    evidence["sources"]["metrics"] = "ok" if evidence["metrics"] else ("failed" if failed else "empty")
    if failed:
        evidence["notes"].append(f"指標查詢失敗：{'、'.join(failed)}")
    elif not evidence["metrics"]:
        evidence["notes"].append("設定的指標查詢在這段時間都沒有資料")


def stamp(raw: Any) -> datetime:
    """日誌的 _time（RFC3339，小數位數不固定）轉成時間，給排序用。字串直接比會排錯：
    "…:40.5Z" 會排在 "…:40Z" 前面。讀不懂的當成最早。"""
    text = re.sub(r"(\.\d{6})\d+", r"\1", str(raw or "").replace("Z", "+00:00"))
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _collect_logs(evidence: dict[str, Any], incident: Incident, store: Any, id_fields: list[str],
                  start: datetime, end: datetime, limits: EvidenceLimits) -> list[str]:
    """填好日誌相關的幾包證據；回傳錯誤日誌裡看到的 trace id（新的在前、不重複），給呼叫鏈直接取用。"""
    log_fields: LogFields = store.fields
    try:
        found = store.search_errors(incident.services, start, end, limits.max_hits, error_levels=limits.error_levels)
    except GrafanaError as e:
        log.warning("事故 %s 撈不到日誌證據：%s", incident.uid, e)
        evidence["notes"].append(f"日誌撈取失敗：{e}")
        evidence["sources"]["logs"] = "failed"
        return []
    evidence["sources"]["logs"] = "ok" if found else "empty"
    if not found:
        # 0 筆有三種完全不同的原因，處理方式也不同：多問一次總數，把它分清楚（只在 0 筆時多這一次查詢）
        try:
            evidence["log_counts"] = store.count_logs(incident.services, start, end)
        except GrafanaError as e:
            log.warning("事故 %s 數不出日誌筆數：%s", incident.uid, e)
    counts = {f: Counter() for f in id_fields}
    for doc in found:
        for f in id_fields:
            value = _dig(doc, f)
            if value not in (None, ""):
                counts[f][str(value)] += 1
    evidence["id_counts"] = {f: dict(c.most_common(limits.top_ids)) for f, c in counts.items()}

    def entry(d: dict[str, Any], with_level: bool = False) -> dict[str, Any]:
        out = {"time": _dig(d, TIME_FIELD), "system": _dig(d, log_fields.service)}
        if with_level and log_fields.level:
            out["level"] = _dig(d, log_fields.level)
        out["message"] = str(_dig(d, MESSAGE_FIELD) or "")[:limits.max_message_chars]
        out.update({f: _dig(d, f) for f in id_fields if _dig(d, f) not in (None, "")})
        return out

    evidence["error_logs"] = [entry(d) for d in found[:limits.max_log_samples]]
    if found:
        _profile_logs(evidence, incident, store, start, end, limits, found, entry)
    if not found:
        evidence["notes"].append("事故時間窗內沒有查到這些系統的錯誤日誌")
    elif not log_fields.level:  # 讓 LLM 知道這批樣本怎麼來的，不要把每一筆都當成錯誤
        evidence["notes"].append("日誌沒有等級欄位：錯誤日誌樣本是依訊息文字裡的等級字樣挑的，可能混入不是錯誤的紀錄")

    # 關聯時間線：錯誤日誌裡出現最多次的批號／載具，回頭把它在各系統的紀錄（不限等級）照時間排出來。
    # 工廠沒有 trace id，批號與載具編號是天然的關聯碼；它只說明先後，不說明因果。
    picks = [(f, value) for f in limits.timeline_fields if f in counts
             for value, _n in counts[f].most_common(limits.max_timeline_ids)]
    picks = sorted(picks, key=lambda fv: -counts[fv[0]][fv[1]])[:limits.max_timeline_ids]
    for field in dict.fromkeys(f for f, _v in picks):
        values = [v for f, v in picks if f == field]
        try:
            lines = store.search_timeline(field, values, start, end, limits.max_timeline_events)
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
    seen = (str(_dig(d, limits.trace_id_field) or "") for d in found) if limits.trace_id_field else ()
    return list(dict.fromkeys(t for t in seen if t))


_UUID = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
_LONG_HEX = re.compile(r"\b(?=[0-9a-fA-F]*\d)[0-9a-fA-F]{8,}\b")   # 至少帶一個數字的長十六進位字串（雜湊、位址）
_DIGITS = re.compile(r"\d+")
_STAR_RUN = re.compile(r"\*(?:[.:/\-_,]?\*)+")                      # *.*.*.*:* 這種連續的星號合成一個
FIRST_ERRORS_PER_SYSTEM = 3   # 每個系統取最早的幾筆錯誤：保證被別的系統擠掉的錯誤至少有樣本，也用來決定先比較哪一種錯誤
WARN_SAMPLE = 50              # 警告最多取樣幾筆來歸類（由早到晚）


def log_pattern(message: str) -> str:
    """把訊息裡會變的部分（UUID、長十六進位字串、數字）換成 *，同一種錯誤就會得到同一個樣板。
    只用來歸類與計數；給 LLM 看的範例永遠是原文（機台、批號常常只寫在訊息裡，不能被洗掉）。"""
    text = _DIGITS.sub("*", _LONG_HEX.sub("*", _UUID.sub("*", message)))
    return _STAR_RUN.sub("*", " ".join(text.split()))


def pattern_words(pattern: str) -> list[str] | None:
    """把樣板變成可以拿去數筆數的條件：固定不變的詞，照原本的順序（LogsQL 的 seq：這些詞照順序出現，中間可以隔別的字）。
    帶 * 的詞整個丟掉（「attempt=*」「v*」「*ms」在原文裡是完整的一個詞，留半個反而比不到）；詞兩端的標點去掉。
    固定的詞少於兩個就不數（回 None）：只有一個詞會把不相干的訊息都算進來。
    這是近似：別種錯誤剛好有同樣的詞、順序也一樣時會被算在一起。"""
    kept = [re.sub(r"^\W+|\W+$", "", token) for token in pattern.split() if "*" not in token]
    kept = [word for word in kept if word]
    return kept if len(kept) >= 2 else None


def _profile_logs(evidence: dict[str, Any], incident: Incident, store: Any, start: datetime, end: datetime,
                  limits: EvidenceLimits, sample: list[dict[str, Any]], entry: Any) -> None:
    """錯誤日誌的前處理（都是算術與歸類，沒有「算不算異常」的門檻）：
      error_logs      同一種錯誤只留最早的一筆原文，附樣本裡同類的筆數。樣本除了最新的那一批，還加上每個系統最早的幾筆
                      （各系統分開問）：某個系統大量報錯時，別的系統較早、較少的錯誤不會被擠掉
      error_patterns  幾種主要的錯誤在事故期間與事故前各出現幾筆（向日誌資料庫計數）；new = 事故前一筆都沒有
      warnings        這些系統的警告，同樣歸類並比較事故前後
    多三次日誌查詢（各系統最早的錯誤、警告、計數）。任何一次失敗都只記進 notes，原本的樣本照舊。

    試過但拿掉的（eval 的 16 個案例量不到差別，見 EVAL.md）：把各系統第一筆錯誤的時間排成一張表、
    列出兩個以上系統都提到的識別碼。"""
    log_fields: LogFields = store.fields
    time_of = lambda d: str(_dig(d, TIME_FIELD) or "")
    system_of = lambda d: str(_dig(d, log_fields.service) or "")
    message_of = lambda d: str(_dig(d, MESSAGE_FIELD) or "")
    warn_levels = limits.warn_levels if log_fields.level and limits.max_warn_patterns > 0 else ()
    try:
        profile = store.error_profile(incident.services, start, end, error_levels=limits.error_levels, warn_levels=warn_levels,
                                      first_n=FIRST_ERRORS_PER_SYSTEM, warn_n=WARN_SAMPLE)
    except GrafanaError as e:
        log.warning("事故 %s 查不到錯誤概況：%s", incident.uid, e)
        evidence["notes"].append("錯誤概況查詢失敗：沒有事故前後的次數比較，樣本也只有最新的那一批")
        profile = {"services": {}, "warnings": []}

    # 樣本 = 最新的那一批 + 每個系統最早的幾筆（大量報錯的系統不會把別人擠掉）
    pool, seen = [], set()
    for doc in [*sample, *(d for first in profile["services"].values() for d in first)]:
        key = (time_of(doc), system_of(doc), message_of(doc))
        if key not in seen:
            seen.add(key)
            pool.append(doc)

    def grouped(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        groups: dict[tuple[str, str], dict[str, Any]] = {}
        for doc in sorted(docs, key=lambda d: stamp(time_of(d))):
            item = groups.setdefault((system_of(doc), log_pattern(message_of(doc))), {"count": 0, "first": doc})
            item["count"] += 1
        return [{"system": system, "pattern": pattern, **item} for (system, pattern), item in groups.items()]

    errors = grouped(pool)   # 已經照各組最早的時間排好
    evidence["error_logs"] = [{**entry(g["first"]), **({"similar_in_sample": g["count"]} if g["count"] > 1 else {})}
                              for g in errors[:limits.max_log_samples]]

    # 要比較事故前後次數的錯誤：每個系統最早出現的那一種先選，其餘照樣本裡的筆數由多到少補滿
    earliest = {system_of(first[0]): log_pattern(message_of(first[0])) for first in profile["services"].values() if first}
    chosen = [g for g in errors if earliest.get(g["system"]) == g["pattern"]]
    chosen += sorted((g for g in errors if g not in chosen), key=lambda g: -g["count"])
    chosen = chosen[:limits.max_log_patterns] if profile["services"] else []
    warns = sorted(grouped(profile["warnings"]), key=lambda g: -g["count"])[:limits.max_warn_patterns]
    jobs = [(g, limits.error_levels) for g in chosen] + [(g, limits.warn_levels) for g in warns]
    countable = [(g, levels, pattern_words(g["pattern"])) for g, levels in jobs]
    before_start = start - timedelta(minutes=limits.baseline_minutes)
    try:
        counted = store.count_phrases([(g["system"], words, levels) for g, levels, words in countable if words],
                                      start, end, before_start)
    except GrafanaError as e:
        log.warning("事故 %s 數不出事故前後的次數：%s", incident.uid, e)
        evidence["notes"].append("事故前後的次數比較查詢失敗")
        counted = []
    numbers = iter(counted)
    for g, _levels, words in countable:
        during, before = next(numbers, (0, 0)) if words else (0, 0)
        row = {"system": g["system"], "pattern": g["pattern"], "example": message_of(g["first"])[:limits.max_message_chars],
               "first_seen": time_of(g["first"])}
        if during > 0:   # 比不到（0 筆）就不給次數：寧可沒有，也不要給一個錯的 0
            row.update(during=during, before=before, new=before == 0)
        (evidence["warnings"] if g in warns else evidence["error_patterns"]).append(row)
    if evidence["error_patterns"] or evidence["warnings"]:
        evidence["count_windows"] = {"during_minutes": max(1, round((end - start).total_seconds() / 60)),
                                     "before_minutes": limits.baseline_minutes}


ERROR_TAG_VALUES = ("true", "error", "2")   # Jaeger 的 error 標籤、otel.status_code＝ERROR／2：都表示這個 span 失敗


def _tags(items: Any) -> dict[str, str]:
    """Jaeger 的 tags 陣列（[{key, type, value}]）→ {key: 字串化的值}。"""
    return {str(item.get("key")): str(item.get("value", "")) for item in items or [] if isinstance(item, dict)}


def summarize_trace(trace: dict[str, Any], trace_id: str, limits: EvidenceLimits) -> dict[str, Any] | None:
    """把一條 Trace（Jaeger 格式：spans 帶 processID 與 references，processes 對到服務名稱）壓成給 LLM 看的呼叫鏈。
    失敗的判定：span 的 error 標籤是 true，或 otel.status_code 是 ERROR。錯誤訊息取 otel.status_description（沒有就取
    error.message／exception.message）。origin = 失敗、而且底下沒有其他失敗 span 的那一段（最接近源頭的失敗點）。"""
    processes = trace.get("processes") or {}
    spans = []
    for sp in trace.get("spans") or []:
        tags = _tags(sp.get("tags"))
        parent = next((str(ref.get("spanID")) for ref in sp.get("references") or [] if ref.get("refType") == "CHILD_OF"), None)
        failed = tags.get("error", "").lower() in ERROR_TAG_VALUES or tags.get("otel.status_code", "").lower() in ERROR_TAG_VALUES
        message = tags.get("otel.status_description") or tags.get("error.message") or tags.get("exception.message") or ""
        start = int(sp.get("startTime") or 0)   # 微秒
        spans.append({"id": str(sp.get("spanID")), "parent": parent, "name": str(sp.get("operationName", "")),
                      "service": str((processes.get(sp.get("processID")) or {}).get("serviceName", "")),
                      "start": start, "end": start + int(sp.get("duration") or 0), "failed": failed,
                      "error": message[:limits.max_message_chars],
                      "attrs": {k: v for k, v in tags.items() if k in SPAN_ATTRIBUTES}})
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
               "offset_ms": (sp["start"] - t0) // 1000, "duration_ms": max(0, sp["end"] - sp["start"]) // 1000,
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


def _collect_call_chains(evidence: dict[str, Any], incident: Incident, store: Any, start: datetime, end: datetime,
                         limits: EvidenceLimits, hinted_ids: list[str]) -> None:
    """先用錯誤日誌裡的 trace id 直接取（一定跟這次的錯誤有關，而且不用等它搜尋得到）；不夠才用搜尋補，挑離現在最近的幾條。
    搜尋結果有的已經帶整條 Trace（VictoriaTraces），有的只有編號（Tempo）：只有編號的，要用到時才取。"""
    def add(trace_id: str, trace: dict[str, Any] | None = None) -> None:
        if trace_id and len(evidence["call_chains"]) < limits.max_traces \
                and trace_id not in {c["trace_id"] for c in evidence["call_chains"]}:
            trace = trace or store.get_trace(trace_id)
            chain = summarize_trace(trace, trace_id, limits) if trace else None
            if chain:
                evidence["call_chains"].append(chain)

    try:
        for trace_id in hinted_ids[:limits.max_traces * 2]:      # 有些 id 可能被取樣掉、取不回來，多試幾個
            add(trace_id)
        if len(evidence["call_chains"]) < limits.max_traces:
            # 搜尋不保證先回最新的：多搜一些，自己挑離現在最近的幾條
            found = store.search_error_traces(incident.services, start, end, max(TRACE_SEARCH_LIMIT, limits.max_traces))
            for item in sorted(found, key=lambda t: t["start"], reverse=True):
                add(item["id"], item["trace"])
    except GrafanaError as e:
        log.warning("事故 %s 撈不到呼叫鏈：%s", incident.uid, e)
        evidence["notes"].append(f"呼叫鏈撈取失敗：{e}")
        evidence["sources"]["call_chains"] = "ok" if evidence["call_chains"] else "failed"
        return
    evidence["sources"]["call_chains"] = "ok" if evidence["call_chains"] else "empty"
    if not evidence["call_chains"]:
        evidence["notes"].append("事故時間窗內沒有這些系統的錯誤 Trace（沒有埋點、被取樣掉、或失敗沒有被標成 error）")


# 每一種證據能回答什麼問題。缺的時候寫在註記上：「缺錯誤日誌｜還看不出哪個系統出了什麼錯…」
CAN_ANSWER = {
    "關係邊": "這幾個系統誰影響誰",
    "日誌": "哪個系統出了什麼錯、哪些錯誤是這次才出現的",
    "錯誤日誌": "哪個系統出了什麼錯、哪些錯誤是這次才出現的",
    "機台與批號": "問題集中在哪台機台、哪個批號",
    "呼叫鏈": "失敗的請求實際停在哪個系統（包含沒有告警的相依服務）",
    "指標走勢": "數值是怎麼變壞的（流量、佇列、資源）",
}


def overview(evidence: dict[str, Any], log_fields: LogFields = LogFields(),
             error_levels: tuple[str, ...] = ERROR_LEVELS) -> dict[str, list[Any]]:
    """這次分析手上有什麼、缺什麼、為什麼缺。全部由程式從實際撈到的結果算出來，不經過 LLM。
    回傳四組：have（有的，白話的句子）、missing（缺的：查了但沒有，或沒有宣告）、failed（查詢失敗）、off（還沒接）。
    後三組的每一項是 (名稱, 原因與下一步)；off 的第二段是 config.yaml 要設的鍵。名稱要在 CAN_ANSWER 裡：
    只說「沒有日誌」沒有用，要說少了它哪一種分析做不了、是哪一關沒過（這段時間沒資料、系統名稱對不上、等級對不上）。"""
    out: dict[str, list[Any]] = {"have": [], "missing": [], "failed": [], "off": []}
    sources = evidence.get("sources") or {}
    systems = {a["system"] for a in evidence["anomalies"]}
    out["have"].append(f"告警 {len(evidence['anomalies'])} 筆")
    related = [e for e in evidence["edges"] if e["upstream"] in systems and e["downstream"] in systems]
    touching = [e for e in evidence["edges"] if e["upstream"] in systems or e["downstream"] in systems]
    if related:
        out["have"].append(f"這些系統之間的關係邊 {len(related)} 條")
    elif touching:
        out["have"].append(f"連到這個系統的關係邊 {len(touching)} 條（另一端沒有告警）")
    # 沒有宣告關係邊：不列為「缺」。關係邊是加分的（幫忙把上下游的告警併成一張、提示誰可能影響誰），
    # 沒寫照樣分析；每次都列出來只會讓人以為非寫不可（使用者的決定，2026-11-26）

    logs = sources.get("logs", "off")
    log_source = evidence.get("log_source") or {"name": "日誌", "scope_hint": "檢查日誌有沒有在送進來"}
    if logs == "off":
        out["off"].append(("日誌", "logs.datasource_uid"))
    elif logs == "failed":
        out["failed"].append(("日誌", f"Grafana 的 {log_source['name']} 資料源查不動，原因見服務的 log"))
    elif logs == "empty":
        counts = evidence.get("log_counts")
        if counts is None:
            why = "沒有查到這些系統的錯誤日誌"
        elif counts["all"] == 0:
            why = f"這段時間一筆日誌都沒有。{log_source['scope_hint']}"
        elif counts["services"] == 0:
            why = (f"這段時間有 {counts['all']} 筆日誌，但沒有一筆的 {log_fields.service} 是這些系統。"
                   f"檢查欄位名稱，以及值是否跟告警的系統名稱完全一樣")
        elif log_fields.level:
            levels = "／".join(dict.fromkeys(v.upper() for v in error_levels))
            why = (f"這些系統有 {counts['services']} 筆日誌，但 {log_fields.level} 沒有一筆是 {levels}。"
                   f"可能真的沒有錯誤，或等級的寫法對不上")
        else:
            why = f"這些系統有 {counts['services']} 筆日誌，但訊息文字裡沒有錯誤字樣。可能真的沒有錯誤"
        out["missing"].append(("錯誤日誌", why))
    else:
        patterns = evidence.get("error_patterns") or []
        counted = [r for r in patterns if "during" in r]
        out["have"].append(f"錯誤日誌 {len(evidence['error_logs'])} 種")
        if counted:
            out["have"].append(f"事故前後的次數比較 {len(counted)} 種錯誤（{sum(r['new'] for r in counted)} 種事故前沒出現過）")
        if evidence.get("warnings"):
            out["have"].append(f"警告 {len(evidence['warnings'])} 種")
        ids = sum(len(counts) for counts in evidence["id_counts"].values())
        if ids:
            out["have"].append(f"機台／批號等識別碼 {ids} 個")
        # 日誌沒有機台、批號欄位：不列為「缺」。IT 服務本來就沒有這些欄位，有就用、沒有就照其他證據分析
        if evidence["timelines"]:
            out["have"].append(f"批號／載具的跨系統時間線 {len(evidence['timelines'])} 條")

    chains = sources.get("call_chains", "off")
    if chains == "ok":
        out["have"].append(f"失敗請求的呼叫鏈 {len(evidence['call_chains'])} 條")
    elif chains == "empty":
        out["missing"].append(("呼叫鏈", f"{evidence.get('trace_source', 'Trace 資料源')} 裡沒有這些系統的失敗請求。可能沒有埋點、被取樣掉，或失敗沒有被標成 error"))
    elif chains == "failed":
        out["failed"].append(("呼叫鏈", "Grafana 的 Trace 資料源查不動，原因見服務的 log"))
    else:
        out["off"].append(("呼叫鏈", "traces.datasource_uid"))

    metrics = sources.get("metrics", "off")
    if metrics == "ok":
        out["have"].append(f"指標走勢 {len(evidence['metrics'])} 條")
    elif metrics == "empty":
        out["missing"].append(("指標走勢", "設定的查詢在這段時間都沒有資料。檢查指標上對應系統名稱的標籤"))
    elif metrics == "failed":
        out["failed"].append(("指標走勢", "查詢失敗，原因見服務的 log"))
    else:
        out["off"].append(("指標走勢", "metrics.datasource_uid 與 metrics.queries"))
    return out


def overview_text(groups: dict[str, list[Any]]) -> str:
    """把 overview() 的結果寫成幾行（註記上證據的那幾行，也交給 LLM）。
    重點不是列出證據的名稱，而是「少了它，哪一種分析做不了」：
      已有的證據｜…                                    一行
      缺<名稱>｜還看不出<它能回答的問題>。<原因與下一步>   每項一行
      <名稱>查詢失敗｜還看不出…。<原因>                   每項一行
      沒接<名稱>｜還看不出…。要接就設定 <config.yaml 的鍵>  每項一行"""
    lines = [f"已有的證據｜{'、'.join(groups['have'])}"]
    lines += [f"缺{name}｜還看不出{CAN_ANSWER[name]}。{why}" for name, why in groups["missing"]]
    lines += [f"{name}查詢失敗｜還看不出{CAN_ANSWER[name]}。{why}" for name, why in groups["failed"]]
    lines += [f"沒接{name}｜還看不出{CAN_ANSWER[name]}。要接就設定 {key}" for name, key in groups["off"]]
    return "\n".join(lines)


def overview_brief(groups: dict[str, list[Any]]) -> str:
    """註記上的補充說明，只有一行：主角是根因分析，證據的狀況放在最後帶過。
    缺的與查詢失敗的留原因（那是要動手處理的），沒接的只列名稱。完整版（overview_text）只交給 LLM。"""
    parts = [f"已有{'、'.join(groups['have'])}"]
    parts += [f"缺{name}（{why}）" for name, why in groups["missing"]]
    parts += [f"{name}查詢失敗（{why}）" for name, why in groups["failed"]]
    if groups["off"]:
        parts.append(f"沒接{'、'.join(name for name, _key in groups['off'])}")
    return "證據｜" + "；".join(parts)


MAX_FACT_LINES = 5   # LLM 沒有分析時，註記上最多直接列幾行證據（版面的上限，不是判斷門檻）
FACT_CHARS = 120     # 每一行引用的原文最多幾個字
FAILURE_TEXT = {"unreachable": "LLM 目前連不上", "rejected": "LLM 的回答沒有通過檢查"}


def alerts_fact(incident: Incident) -> str:
    return f"・告警：{raw_summary(incident)}"


def evidence_facts(incident: Incident, evidence: dict[str, Any]) -> list[str]:
    """LLM 沒有分析的時候直接貼在註記上的幾行證據。全部是程式收集到的原文與筆數，沒有推論、沒有排名次：
    第一行是哪些告警響了；再來是錯誤種類（事故前沒出現過的排前面，再照事故期間的筆數）；最後一行是呼叫鏈最底層的失敗點。"""
    clip = lambda text: " ".join(str(text).split())[:FACT_CHARS]
    lines = [alerts_fact(incident)]
    chain = next((sp for c in evidence.get("call_chains") or [] for sp in c["spans"] if sp.get("origin")), None)
    room = MAX_FACT_LINES - 1 - (1 if chain else 0)
    patterns = sorted(evidence.get("error_patterns") or [], key=lambda r: (not r.get("new", False), -r.get("during", 0)))
    for r in patterns[:room]:
        if "during" not in r:
            lines.append(f"・{r['system']}：{clip(r['example'])}")
        else:
            lines.append(f"・{r['system']}{' 新出現' if r['new'] else ''}：{clip(r['example'])}"
                         f"（事故期間 {r['during']} 筆，事故前 {r['before']} 筆）")
    if not patterns:   # 沒有做次數比較（查詢失敗或關掉了）：退而列錯誤日誌的樣本
        for e in (evidence.get("error_logs") or [])[:room]:
            lines.append(f"・{e['system']}：{clip(e['message'])}")
    if chain:
        target = (chain.get("attributes") or {}).get("peer.service")
        path = " → ".join(x for x in (chain.get("called_by") if chain.get("called_by") != chain["service"] else None,
                                      chain["service"], target) if x)
        lines.append(f"・呼叫鏈停在：{path}，{clip(chain.get('error') or chain['span'])}")
    return lines


def failure_line(kind: str, failures: int, retry_seconds: int) -> str:
    """LLM 沒有分析時註記第 2 行的內容：說明原因、什麼時候再試，並交代下面幾行是什麼。"""
    minutes = max(1, round(retry_seconds / 60))
    return (f"[未經分析] {FAILURE_TEXT.get(kind, FAILURE_TEXT['unreachable'])}（連續失敗 {failures} 次，約 {minutes} 分鐘後重試）。"
            f"以下是程式收集到的證據，沒有經過推論：")


def build_prompt(evidence: dict[str, Any], summary_of_evidence: str = "") -> str:
    dump = lambda x: json.dumps(x, ensure_ascii=False, indent=1)
    head = f"【0. 證據概況（程式統計的事實：已有的證據；缺的、查詢失敗的、沒接的各自讓哪一種分析做不了）】\n{summary_of_evidence}\n\n" if summary_of_evidence else ""
    text = (f"{head}【1. 本次收斂的告警（依開始時間排序；values 是觸發當下規則各節點的數值）】\n{dump(evidence['anomalies'])}\n\n"
            f"【2. 系統上下游關係（upstream 的問題會影響 downstream）】\n{dump(evidence['edges'])}\n\n"
            f"【3. 事故時間窗內的錯誤日誌樣本（同一種錯誤只列最早的一筆原文；similar_in_sample 是樣本裡同類的筆數）】\n"
            f"{dump(evidence['error_logs'])}\n\n"
            f"【4. 錯誤日誌裡各識別碼的出現次數】\n{dump(evidence['id_counts'])}\n\n")
    windows = evidence.get("count_windows") or {}
    counts_note = (f"during＝事故期間 {windows.get('during_minutes', '?')} 分鐘內的筆數，before＝事故前 {windows.get('before_minutes', '?')} 分鐘內的筆數；"
                   f"new=true 表示事故前一筆都沒有；沒有這些欄位的是數不出來")
    # pattern（把數字換成 * 的樣板）只是程式歸類用的，不給模型看：它會把樣板裡的字當成真的名稱照抄
    # （現場遇到的：k8s 被寫成 k*s）。模型看 example，那是原文。
    shown = lambda rows: [{k: v for k, v in row.items() if k != "pattern"} for row in rows]
    if evidence.get("error_patterns"):
        text += (f"【3d. 主要的錯誤種類與事故前後的次數（example 是這一種錯誤最早的一筆原文；{counts_note}）】\n"
                 f"{dump(shown(evidence['error_patterns']))}\n\n")
    if evidence.get("warnings"):
        text += (f"【3e. 警告（等級是警告的日誌，還不是錯誤；{counts_note}）】\n{dump(shown(evidence['warnings']))}\n\n")
    if evidence.get("timelines"):
        text += ("【5. 關聯時間線：同一個批號／載具在各系統的紀錄，由舊到新（只代表先後，不代表因果；各系統時鐘可能有誤差）】\n"
                 f"{dump(evidence['timelines'])}\n\n")
    if evidence.get("call_chains"):
        text += ("【6. 呼叫鏈：失敗請求實際的呼叫路徑（called_by 是呼叫它的系統；origin=true 是底下沒有其他失敗呼叫的失敗點）】\n"
                 f"{dump(evidence['call_chains'])}\n\n")
    if evidence.get("metrics"):
        text += ("【7. 指標走勢：各系統的數值在事故前後的變化（baseline 是事故開始前的中位數；during 是事故開始後；"
                 "points 是整段時間平均抽樣的點，時間是 UTC。只說明數值怎麼變，不代表因果）】\n"
                 f"{dump(evidence['metrics'])}\n\n")
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


def read_answer(text: str, evidence: dict[str, Any], *, max_summary_chars: int = 300,
                id_pattern: str = DEFAULT_ID_PATTERN) -> tuple[str, list[dict[str, Any]]]:
    """幻覺檢查：LLM 點名的系統與機台／批號必須真的在證據裡。通過回傳 (摘要, 可能的原因)，不通過丟 ValueError。

    什麼叫「在證據裡」：是告警的系統、關係邊或呼叫鏈裡的系統，**或者名稱原樣出現在送給模型的證據文字裡**（不分大小寫）。
    後者是為了日誌訊息裡指名的元件：`Cannot connect to MariaDb`、`datasource vmselect-…-k8s-stack returned 503`。
    它們沒有告警、也沒有人把它寫進關係邊，但確實是日誌寫的，不是模型編的。關係邊有寫當然更好，沒寫不該擋掉分析
    （使用者的決定，2026-11-26，現場 AP1105 的兩張事故就是這樣被退回的）。
    證據裡完全找不到的名稱：排第一的可能原因是它、或結論的句子提到它 → 整份不採用；只是排在後面的可能原因 → 只拿掉那一個。

    可能的原因最多留 MAX_CANDIDATES 個，每個是 {source, confidence（0～100）, reason}。分數是模型自己給的，程式只做一件事：
    **沒有直接證據指向那個系統，分數最高只到 NO_DIRECT_EVIDENCE_CAP**。直接證據 = 那個系統的名稱出現在日誌、警告、Trace、
    時間線、呼叫鏈或指標裡（DIRECT_EVIDENCE）；只出現在告警與關係邊裡的不算。這不是校準：分數不是機率。"""
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
    # 比對用的文字 = 送給模型的那些內容。pattern（數字換成 * 的樣板）模型看不到，也不能當憑據：k*s 不是真的名稱
    visible = {**evidence, **{key: [{k: v for k, v in row.items() if k != "pattern"} for row in evidence.get(key) or []]
                              for key in ("error_patterns", "warnings")}}
    corpus = json.dumps(visible, ensure_ascii=False)
    lowered = corpus.lower()

    def grounded(name: Any) -> bool:
        """這個名稱有憑據：是已知的系統，或原樣（不分大小寫）出現在證據文字裡。太短的名稱不用文字比對（隨便都對得到）。"""
        text = str(name).strip()
        return text in systems or (len(text) >= MIN_NAME_CHARS and text.lower() in lowered)

    # systems 清單是模型自己列的「我提到了誰」。清單裡找不到憑據的名稱，只有當結論的句子真的提到它才算問題
    unknown = [s for s in answer.get("systems") or [] if not grounded(s) and str(s).strip() and str(s).lower() in summary.lower()]
    source = answer.get("source") or ""   # 舊格式（自訂提示詞可能還在用）：有填就必須有憑據
    if source and not grounded(source):
        unknown.append(source)
    # ids：在識別碼統計裡，或原樣出現在送給模型的證據裡（跟下面摘要文字同一個標準）都算有憑據。
    # 只認統計會誤擋兩種真的有憑據的情況：日誌沒拆欄位、機台與批號只寫在訊息文字裡；以及有拆欄位但沒排進 top_ids 的。
    # 這是字串比對：編出來的 EQP-1 剛好是證據裡 EQP-10 的開頭時擋不到（摘要文字的檢查本來就有同樣的限制）。
    unknown += [i for i in answer.get("ids") or [] if i not in ids and not (isinstance(i, str) and i.strip() and i in corpus)]
    raw = answer.get("candidates")
    raw = raw if isinstance(raw, list) else []   # 選填：自訂提示詞可以不要求
    direct = json.dumps({key: visible.get(key) for key in DIRECT_EVIDENCE}, ensure_ascii=False).lower()
    candidates: list[dict[str, Any]] = []
    for rank, item in enumerate(raw[:MAX_CANDIDATES]):
        if not isinstance(item, dict) or not str(item.get("source") or "").strip():
            raise ValueError("candidates 的每一項都要有 source")
        name = str(item["source"]).strip()
        if not grounded(name):
            if rank == 0:   # 排第一的就是結論在講的那一個：它沒有憑據，整份不能用
                unknown.append(name)
            continue        # 排在後面的：只拿掉這一個，其他照用
        score = _confidence(item.get("confidence"))
        if name.lower() not in direct:   # 沒有直接證據指向它：分數最高只到「只是推測」那一段
            score = min(score, NO_DIRECT_EVIDENCE_CAP)
        # 模型自己說這一條是推測的（證據沒有直接寫）：可以列，但分數不能進到「有證據」那一段，註記上也會標「推測」
        inferred = str(item.get("kind") or "").strip().lower() == "inference"
        if inferred:
            score = min(score, CONFIDENCE_MEDIUM - 1)
        candidates.append({"source": name, "confidence": score, "reason": " ".join(str(item.get("reason") or "").split()),
                           **({"inferred": True} if inferred else {})})
    # 摘要與理由的文字裡夾數字的識別碼（機台、批號樣式）也要在證據裡找得到：防它只寫在句子裡、沒列進 ids
    # 泛稱（HTTP2、v2 這類）不在證據裡也會被擋：樣式不合用就改 llm.id_pattern，設成空字串 = 關掉這一項
    if id_pattern:
        for sentence in (summary, *(c["reason"] for c in candidates)):
            unknown += [t for t in re.findall(id_pattern, sentence) if t not in corpus]
    if unknown:
        raise ValueError(f"點名了證據裡沒有的名稱：{sorted(set(map(str, unknown)))}")
    return summary[:max_summary_chars], candidates


def _confidence(raw: Any) -> int:
    """模型給的信心分數整理成 0～100 的整數。沒給、不是數字：當成 0（不替它猜一個分數）。"""
    if isinstance(raw, str) and raw.strip() in LEGACY_CONFIDENCE:
        return LEGACY_CONFIDENCE[raw.strip()]
    try:
        value = float(str(raw).strip().rstrip("%"))
    except (TypeError, ValueError):
        return 0
    return max(0, min(100, round(value))) if value == value else 0   # value != value 是 NaN


def check_answer(text: str, evidence: dict[str, Any], *, max_summary_chars: int = 300,
                 id_pattern: str = DEFAULT_ID_PATTERN) -> str:
    """read_answer 只取摘要的那一半（通過檢查回傳摘要文字，不通過丟 ValueError）。"""
    return read_answer(text, evidence, max_summary_chars=max_summary_chars, id_pattern=id_pattern)[0]


def candidates_text(candidates: list[dict[str, Any]], max_chars: int = 300) -> str:
    """可能的原因寫成註記上的列點，分數放最後：1. mq-broker：…（信心 90）。模型標成推測的寫（推測，信心 30）。"""
    return "\n".join(f"{n}. {c['source']}：{c['reason'][:max_chars]}（{'推測，' if c.get('inferred') else ''}信心 {c['confidence']}）"
                     for n, c in enumerate(candidates, 1))


@dataclass(frozen=True)
class Analysis:
    line: str          # 註記的「根因分析」那一行：[LLM 推論] … 或 [未經分析] …
    analyzed: bool     # 是不是通過檢查的 LLM 推論
    reason: str        # 沒通過的原因
    candidates: str    # 註記上「可能原因」的幾行（candidates_text）；沒有就是空字串
    failure: str = ""  # 沒有分析的話是哪一種：unreachable（連不上、逾時、回錯誤碼）／rejected（有回答但沒通過檢查）；其餘是空的


def analyze_full(incident: Incident, evidence: dict[str, Any], *, api_url: str, api_key: str, model: str,
                 transport: httpx.BaseTransport | None = None, options: LlmOptions = LlmOptions(),
                 overview_text: str = "") -> Analysis:
    """問 LLM 並檢查回答。任何失敗都退回原始摘要（沒有可能原因），不丟例外。"""
    fallback = f"[未經分析] {raw_summary(incident)}"
    if not api_url or not model:
        return Analysis(fallback, False, "沒有設定 llm.url、llm.model", "")
    try:
        prompt = build_prompt(evidence, overview_text)
        log.debug("事故 %s 送給 LLM 的內容：\n%s", incident.uid, prompt)
        answer = call_llm(api_url, api_key, model, prompt, transport, timeout=options.timeout_seconds,
                          temperature=options.temperature, system_prompt=options.system_prompt)
        log.debug("事故 %s LLM 的原始回應：%s", incident.uid, answer)
        summary, candidates = read_answer(answer, evidence, max_summary_chars=options.max_summary_chars,
                                          id_pattern=options.id_pattern)
    except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as e:  # ValueError 含 JSON 解析失敗
        reason = f"{type(e).__name__}: {e}"[:300]
        log.warning("事故 %s 的 LLM 分析沒有採用：%s", incident.uid, reason)
        # 連不上、逾時、回 4xx／5xx 是「LLM 不通」；其餘是它有回答、但格式不對或沒通過幻覺檢查
        return Analysis(fallback, False, reason, "", "unreachable" if isinstance(e, httpx.HTTPError) else "rejected")
    return Analysis(f"[LLM 推論] {summary}", True, "", candidates_text(candidates, options.max_summary_chars))


def analyze(incident: Incident, evidence: dict[str, Any], *, api_url: str, api_key: str, model: str,
            transport: httpx.BaseTransport | None = None, options: LlmOptions = LlmOptions(),
            overview_text: str = "") -> tuple[str, bool, str]:
    """analyze_full 的前三項：(註記的根因分析那一行, 是不是通過檢查的 LLM 推論, 沒通過的原因)。"""
    got = analyze_full(incident, evidence, api_url=api_url, api_key=api_key, model=model, transport=transport,
                       options=options, overview_text=overview_text)
    return got.line, got.analyzed, got.reason
