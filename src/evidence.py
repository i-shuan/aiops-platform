"""收集證據：這張事故手上有什麼。Python 不推理，只把查得到的東西整理好，交給 src/rca.py 去問 LLM。

  collect_evidence   告警、系統關係、錯誤日誌與識別碼統計、關聯時間線、呼叫鏈、指標走勢。任何一包撈不到都不擋，留空並寫進 notes
  _profile_logs      錯誤日誌的前處理：同類去重、各系統最早的錯誤不被擠掉、事故前後各出現幾筆、警告
  summarize_trace    一條 Trace 壓成呼叫鏈；senders_of 追訊息佇列的連結；calls_in 讀出誰呼叫誰
  overview           「證據概況」：手上有哪些、缺哪些與原因（寫在註記上，也交給 LLM）

日誌與 Trace 的來源是傳進來的物件（src/logs.py、src/traces.py），這裡不知道後面是哪一種資料庫。
這些處理都只是歸類與計數：什麼算異常由 Grafana 規則決定，這裡沒有任何門檻。
"""
from __future__ import annotations

import logging
import re
import statistics
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from src import promql
from src.grafana import Grafana, GrafanaError
from src.graph import EDGE_FIELDS
from src.grouping import Incident
from src.logs import ERROR_LEVELS, MESSAGE_FIELD, TIME_FIELD, WARN_LEVELS, LogFields

log = logging.getLogger("aiops_lite.rca")   # 沿用原本的 logger 名稱：現場的 log 篩選條件不用改

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
    trace_id_field: str = "trace_id"     # 日誌裡記 trace id 的欄位。兩個方向都用：錯誤日誌帶的編號直接取那條 Trace；
                                         # 取到的呼叫鏈也用編號回頭撈同一個請求的日誌（不限等級）。空的 = 兩個方向都不做
    max_traces: int = 3                  # 最多整理幾條錯誤 Trace 的呼叫鏈（只取失敗的）
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
    datasource_uid: str = ""             # Grafana 上指到 VictoriaMetrics 的資料源（Prometheus 型別）的 uid
    queries: tuple[MetricQuery, ...] = ()
    lookback_minutes: int = 60           # 事故開始前往回看多久當基準：太短會把事故前的爬升也算進基準
    step_seconds: int = 60               # 取樣間隔；要不小於指標的抓取間隔，不然每一格都是同一個值
    max_points: int = 12                 # 每條序列給 LLM 看幾個點（整段時間窗平均抽樣）
    max_series: int = 5                  # 一條查詢回很多條序列時（例如每個佇列一條）只留事故期間數值最大的幾條


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


def relation(edge: Any) -> dict[str, Any]:
    """證據裡的一條系統關係。(上游, 下游) 的 tuple 原樣轉；db.calls_touching 的列（caller 呼叫 callee）轉成
    upstream＝被呼叫的、downstream＝呼叫的（被呼叫的出問題會影響呼叫它的），並帶上來源、最後看到的時間、這一輪的請求數與失敗數。"""
    if not isinstance(edge, dict):
        upstream, downstream = edge
        return {"upstream": upstream, "downstream": downstream}
    # 一般的呼叫：被呼叫的出問題會影響呼叫它的。經訊息佇列的反過來：送訊息的（或佇列）出問題，收的那一端才會受影響
    asynchronous = edge.get("kind") == "messaging"
    out = {"upstream": edge["caller"], "downstream": edge["callee"]} if asynchronous else {"upstream": edge["callee"], "downstream": edge["caller"]}
    out.update({k: edge[k] for k in ("source", "last_seen", "times", "requests", "failed", "kind", "hops", *EDGE_FIELDS)
                if edge.get(k) not in (None, "")})
    return out


def collect_evidence(incident: Incident, edges: list[Any], grafana: Grafana, *, logs: Any = None,
                     traces: Any = None, id_fields: list[str], now: datetime, limits: EvidenceLimits = EvidenceLimits(),
                     metrics: MetricOptions = MetricOptions()) -> dict[str, Any]:
    """組證據：告警、關係邊、錯誤日誌與識別碼統計、關聯時間線、呼叫鏈、指標走勢。全部經 Grafana 的資料源代理。
    logs 是 src/logs.py 的 VictoriaLogs 或 Elasticsearch，traces 是 src/traces.py 的 VictoriaTraces 或 Tempo；None = 沒接。
    這裡不管後面是哪一種：兩種來源回的日誌與 Trace 是同一種形狀。
    任何一包撈不到（沒設資料源、Grafana 回錯）都不擋分析：那一包留空，並在 notes 寫明原因。"""
    iso = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")
    window = promql.Window.of_incident(incident.started_at, incident.last_seen_at, now, padding_minutes=limits.window_padding_minutes,
                                       baseline_minutes=limits.baseline_minutes)
    start, end = window.start, window.end
    evidence: dict[str, Any] = {
        "anomalies": [{"system": m["service"], "rule": m["title"], "severity": m["severity"],
                       "started": iso(m["starts_at"]), "status": m["status"], "values": m["values"]}
                      for m in _members(incident)],
        "edges": [relation(e) for e in edges],
        "baseline_minutes": limits.baseline_minutes,
        "window_minutes": window.minutes,   # 「事故前」的長度：日誌的次數比較與 service graph 的基線同一段
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
        if logs is not None and limits.trace_id_field and evidence["call_chains"]:
            _collect_trace_logs(evidence, incident, logs, id_fields, start, end, limits)
    if metrics.datasource_uid and metrics.queries:
        _collect_metrics(evidence, incident, grafana, metrics, end)
    evidence["window"] = {"start": iso(start), "end": iso(end)}
    return evidence


promql_string = promql.string   # 舊名稱（rca 照這個名字再匯出）
_round = promql.round4


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


def _entry_of(log_fields: LogFields, id_fields: list[str], limits: EvidenceLimits) -> Any:
    """回傳「一筆日誌 → 給 LLM 看的樣子」的函式：時間、系統、（要的話）等級、截斷的訊息、有值的識別碼。"""
    def entry(d: dict[str, Any], with_level: bool = False) -> dict[str, Any]:
        out = {"time": _dig(d, TIME_FIELD), "system": _dig(d, log_fields.service)}
        if with_level and log_fields.level:
            out["level"] = _dig(d, log_fields.level)
        out["message"] = str(_dig(d, MESSAGE_FIELD) or "")[:limits.max_message_chars]
        out.update({f: _dig(d, f) for f in id_fields if _dig(d, f) not in (None, "")})
        return out
    return entry


def _collect_trace_logs(evidence: dict[str, Any], incident: Incident, store: Any, id_fields: list[str],
                        start: datetime, end: datetime, limits: EvidenceLimits) -> None:
    """呼叫鏈 → 日誌：用每條呼叫鏈的 trace id 回頭撈「同一個請求」在各系統留下的日誌，不限等級、不限系統，由舊到新。
    照時間撈的錯誤日誌是「這段時間這些系統的所有錯誤」，不保證跟出事的請求有關；這一包一定有關，而且警告與一般紀錄也帶得出來。
    前提是日誌有記 trace id（limits.trace_id_field），而且寫法跟 Trace 資料庫的編號一樣（小寫十六進位）。
    沒撈到不算缺：多數日誌本來就沒有這個欄位，註記不提。多一次日誌查詢（兩個請求）；失敗只記進 notes。"""
    ids = [chain["trace_id"] for chain in evidence["call_chains"]]
    try:
        lines = store.search_timeline(limits.trace_id_field, ids, start, end, limits.max_timeline_events)
    except GrafanaError as e:
        log.warning("事故 %s 撈不到呼叫鏈對應的日誌：%s", incident.uid, e)
        evidence["notes"].append(f"呼叫鏈對應的日誌撈取失敗：{e}")
        return
    log_fields: LogFields = store.fields
    entry = _entry_of(log_fields, id_fields, limits)
    rows = [{"trace_id": trace_id, "events": [entry(d, with_level=True) for d in got["hits"]],
             "systems": sorted({str(_dig(d, log_fields.service)) for d in got["hits"] if _dig(d, log_fields.service)}),
             "truncated": got["total"] > len(got["hits"])}
            for trace_id in ids for got in [lines.get(trace_id) or {"hits": [], "total": 0}] if got["hits"]]
    if rows:
        evidence["trace_logs"] = rows


def _collect_logs(evidence: dict[str, Any], incident: Incident, store: Any, id_fields: list[str],
                  start: datetime, end: datetime, limits: EvidenceLimits) -> list[str]:
    """填好日誌相關的幾包證據；回傳錯誤日誌裡看到的 trace id（新的在前、不重複），給呼叫鏈直接取用。
    時間區間內的錯誤與警告**一次查詢一起撈**（最新的在前，最多 max_hits 筆），撈回來再照等級分開。
    沒有等級欄位時不撈警告：只能比對訊息文字裡的字樣，warn 這個詞太常出現，分不出來。"""
    log_fields: LogFields = store.fields
    warn_levels = limits.warn_levels if log_fields.level and limits.max_warn_patterns > 0 else ()
    try:
        rows = store.search_errors(incident.services, start, end, limits.max_hits, error_levels=(*limits.error_levels, *warn_levels))
    except GrafanaError as e:
        log.warning("事故 %s 撈不到日誌證據：%s", incident.uid, e)
        evidence["notes"].append(f"日誌撈取失敗：{e}")
        evidence["sources"]["logs"] = "failed"
        return []
    is_warning = lambda d: bool(warn_levels) and str(_dig(d, log_fields.level) or "") in warn_levels
    found = [d for d in rows if not is_warning(d)]
    warnings = [d for d in rows if is_warning(d)]
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

    entry = _entry_of(log_fields, id_fields, limits)
    evidence["error_logs"] = [entry(d) for d in found[:limits.max_log_samples]]
    # 只有警告、沒有錯誤也照樣歸類與計數：延遲類的事故常常只有警告（慢查詢、重試變多）
    if found or warnings:
        _profile_logs(evidence, incident, store, start, end, limits, found, warnings, entry)
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
                  limits: EvidenceLimits, sample: list[dict[str, Any]], warnings: list[dict[str, Any]], entry: Any) -> None:
    """錯誤日誌的前處理（都是算術與歸類，沒有「算不算異常」的門檻）：
      error_logs      同一種錯誤只留最早的一筆原文，附樣本裡同類的筆數。樣本除了最新的那一批，還加上每個系統最早的幾筆
                      （各系統分開問）：某個系統大量報錯時，別的系統較早、較少的錯誤不會被擠掉
      error_patterns  幾種主要的錯誤在事故期間與事故前各出現幾筆（向日誌資料庫計數）；new = 事故前一筆都沒有
      warnings        這些系統的警告（跟錯誤同一次查詢撈回來的），同樣歸類並比較事故前後
    多兩次日誌查詢（各系統最早的錯誤、計數；沒有錯誤時不問各系統最早的錯誤）。任何一次失敗都只記進 notes，原本的樣本照舊。

    試過但拿掉的（eval 的 16 個案例量不到差別）：把各系統第一筆錯誤的時間排成一張表、
    列出兩個以上系統都提到的識別碼。"""
    log_fields: LogFields = store.fields
    time_of = lambda d: str(_dig(d, TIME_FIELD) or "")
    system_of = lambda d: str(_dig(d, log_fields.service) or "")
    message_of = lambda d: str(_dig(d, MESSAGE_FIELD) or "")
    try:
        profile = (store.error_profile(incident.services, start, end, error_levels=limits.error_levels,
                                       first_n=FIRST_ERRORS_PER_SYSTEM) if sample else {"services": {}})
    except GrafanaError as e:
        log.warning("事故 %s 查不到錯誤概況：%s", incident.uid, e)
        evidence["notes"].append("錯誤概況查詢失敗：沒有事故前後的次數比較，樣本也只有最新的那一批")
        profile = {"services": {}}

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
    warns = sorted(grouped(warnings), key=lambda g: -g["count"])[:limits.max_warn_patterns]
    jobs = [(g, limits.error_levels) for g in chosen] + [(g, limits.warn_levels) for g in warns]
    countable = [(g, levels, pattern_words(g["pattern"])) for g, levels in jobs]
    before_start = promql.Window(start, end, limits.baseline_minutes).baseline_start   # 跟 service graph 的基線同一段
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


MAX_LINKED_TRACES = 3   # 一條 Trace 最多追幾條它連結到的 Trace（訊息佇列的發送端）。查詢量的上限，未驗證泛化


def linked_spans(trace: dict[str, Any]) -> list[tuple[str, str]]:
    """這條 Trace 裡，有哪幾段是接續「另一條 Trace」的某一段：[(那條的 trace id, 那一段的 span id)]。
    訊息佇列常這樣接：消費端自己開一條新的 Trace，用連結指回發送的那一段。連結寫明了對方是誰，不用靠時間去猜。"""
    own = str(trace.get("traceID") or "")
    refs = [(str(ref.get("traceID") or ""), str(ref.get("spanID") or "")) for sp in trace.get("spans") or []
            for ref in sp.get("references") or [] if ref.get("refType") == "FOLLOWS_FROM"]
    return list(dict.fromkeys(r for r in refs if r[0] and r[0] != own))


def senders_of(trace: dict[str, Any], store: Any) -> dict[tuple[str, str], str]:
    """這條 Trace 用連結接續的那幾段，各是哪個服務送的：把對方那條 Trace 取回來看（最多 MAX_LINKED_TRACES 條）。
    取不到（過期、被取樣掉）就沒有，那一段的 called_by 維持空的。"""
    found: dict[tuple[str, str], str] = {}
    wanted = linked_spans(trace)
    for other_id in list(dict.fromkeys(t for t, _s in wanted))[:MAX_LINKED_TRACES]:
        other = store.get_trace(other_id) or {}
        processes = other.get("processes") or {}
        for sp in other.get("spans") or []:
            key = (other_id, str(sp.get("spanID")))
            if key in wanted:
                found[key] = str((processes.get(sp.get("processID")) or {}).get("serviceName", ""))
    return {key: name for key, name in found.items() if name}


def summarize_trace(trace: dict[str, Any], trace_id: str, limits: EvidenceLimits,
                    linked: dict[tuple[str, str], str] | None = None) -> dict[str, Any] | None:
    """把一條 Trace（Jaeger 格式：spans 帶 processID 與 references，processes 對到服務名稱）壓成給 LLM 看的呼叫鏈。
    失敗的判定：span 的 error 標籤是 true，或 otel.status_code 是 ERROR。錯誤訊息取 otel.status_description（沒有就取
    error.message／exception.message）。origin = 失敗、而且底下沒有其他失敗 span 的那一段（最接近源頭的失敗點）。
    linked 是 {(另一條的 trace id, span id): 那一段的服務名稱}：這條 Trace 的某一段是用連結接續別條 Trace 時，called_by 填那個服務。
    經訊息佇列收到的那一段（span.kind 是 consumer）標 via="messaging"：它跟發送端是非同步的關係。"""
    linked = linked or {}
    processes = trace.get("processes") or {}
    spans = []
    for sp in trace.get("spans") or []:
        tags = _tags(sp.get("tags"))
        parent = next((str(ref.get("spanID")) for ref in sp.get("references") or [] if ref.get("refType") == "CHILD_OF"), None)
        sender = next((linked[key] for ref in sp.get("references") or [] if ref.get("refType") == "FOLLOWS_FROM"
                       for key in [(str(ref.get("traceID") or ""), str(ref.get("spanID") or ""))] if key in linked), None)
        failed = tags.get("error", "").lower() in ERROR_TAG_VALUES or tags.get("otel.status_code", "").lower() in ERROR_TAG_VALUES
        message = tags.get("otel.status_description") or tags.get("error.message") or tags.get("exception.message") or ""
        start = int(sp.get("startTime") or 0)   # 微秒
        spans.append({"id": str(sp.get("spanID")), "parent": parent, "name": str(sp.get("operationName", "")),
                      "service": str((processes.get(sp.get("processID")) or {}).get("serviceName", "")),
                      "start": start, "end": start + int(sp.get("duration") or 0), "failed": failed, "sender": sender,
                      "messaging": tags.get("span.kind") == "consumer",
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
               "called_by": by_id[sp["parent"]]["service"] if sp["parent"] in by_id else sp["sender"],
               "offset_ms": (sp["start"] - t0) // 1000, "duration_ms": max(0, sp["end"] - sp["start"]) // 1000,
               "status": "error" if sp["failed"] else "ok"}
        if sp["failed"]:
            row["error"] = sp["error"]
            row["origin"] = sp["id"] not in failed_parents
        if sp["attrs"]:
            row["attributes"] = sp["attrs"]
        if sp["messaging"]:
            row["via"] = "messaging"
        rows.append(row)
    systems = {sp["service"] for sp in spans if sp["service"]} | {sp["attrs"]["peer.service"] for sp in spans
                                                                  if sp["attrs"].get("peer.service")}
    return {"trace_id": trace_id, "spans": rows, "systems": sorted(systems), "truncated": len(spans) > len(rows)}


def _collect_call_chains(evidence: dict[str, Any], incident: Incident, store: Any, start: datetime, end: datetime,
                         limits: EvidenceLimits, hinted_ids: list[str]) -> None:
    """先用錯誤日誌裡的 trace id 直接取（一定跟這次的錯誤有關，而且不用等它搜尋得到）；不夠才用搜尋補，挑離現在最近的幾條。
    搜尋結果有的已經帶整條 Trace（VictoriaTraces），有的只有編號（Tempo）：只有編號的，要用到時才取。
    只取失敗的 Trace。延遲類的事故（請求沒有失敗、只是慢）看 service graph 的延遲統計（關係邊上的 p95），
    不挑「最慢的幾條」：沒有對照組的單一樣本會把偶發的慢（GC、網路抖動）當成線索。"""
    def add(trace_id: str, trace: dict[str, Any] | None = None) -> None:
        if trace_id and len(evidence["call_chains"]) < limits.max_traces \
                and trace_id not in {c["trace_id"] for c in evidence["call_chains"]}:
            trace = trace or store.get_trace(trace_id)
            chain = summarize_trace(trace, trace_id, limits, senders_of(trace, store)) if trace else None
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
    further = len(evidence["edges"]) - len(touching)   # 兩跳的（兩端都沒有告警）
    timed = sum(1 for e in evidence["edges"] if "latency_from" in e)
    extra = "".join([f"，另有隔一個系統的 {further} 條" if further else "", f"；其中 {timed} 條有延遲與基線" if timed else ""])
    if related:
        out["have"].append(f"這些系統之間的關係邊 {len(related)} 條{extra}")
    elif touching:
        out["have"].append(f"連到這個系統的關係邊 {len(touching)} 條（另一端沒有告警）{extra}")
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
            why = (f"事故時間窗內（{evidence.get('window_minutes', '?')} 分鐘）一筆日誌都沒有：可能這段時間真的沒有輸出，也可能日誌沒有送進來。"
                   f"更早有沒有，這裡沒有查。要確認的話{log_source['scope_hint']}")
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
        if evidence.get("warnings"):
            out["have"].append(f"警告 {len(evidence['warnings'])} 種")
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
        if evidence.get("trace_logs"):
            out["have"].append(f"呼叫鏈對應的日誌 {sum(len(t['events']) for t in evidence['trace_logs'])} 筆")
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


MAX_RELATIONS = 20   # 一次最多給 LLM 看幾條系統關係（兩跳內；一跳的優先，同一跳照延遲變成基線的幾倍排）。版面的上限，未驗證泛化


def calls_in(evidence: dict[str, Any], *, failed_only: bool = False, messaging_only: bool = False) -> list[tuple[str, str]]:
    """這次的呼叫鏈裡實際出現的「誰呼叫誰」（系統層級，同一個系統內部的呼叫不算）。
    來源只有 Trace：是真的發生過的呼叫，不是從指標或日誌推的。peer.service 屬性指到的對象（常見的是資料庫）也算。
    failed_only：只留「被呼叫的那一段自己失敗」的呼叫。失敗的 Trace 裡也有成功的呼叫（例如請求失敗了，但順手送出去的訊息有送到），
    判斷要不要合併事故時那種不算。
    messaging_only：只留經訊息佇列的（接收的那一段標了 via="messaging"）：發送的 → 接收的，是非同步的關係。"""
    pairs: list[tuple[str, str]] = []
    for chain in evidence.get("call_chains") or []:
        for span in chain["spans"]:
            if failed_only and span.get("status") != "error":
                continue
            service, caller = span.get("service") or "", span.get("called_by") or ""
            if caller and service and caller != service and (not messaging_only or span.get("via") == "messaging"):
                pairs.append((caller, service))
            target = (span.get("attributes") or {}).get("peer.service") or ""
            if service and target and target != service and not messaging_only:
                pairs.append((service, target))
    return list(dict.fromkeys(pairs))


