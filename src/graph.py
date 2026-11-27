"""系統之間的關係：全部從資料來，沒有手寫的設定（使用者的決定，2026-11-27）。

三個來源，都只記「真的看到的東西」，存進資料庫的 observed_calls 表（db.record_calls），各自標來源：
  service_graph  Trace 資料庫自己統計的誰呼叫誰。兩種都要對方有開才有：
                 Tempo：metrics-generator 的 service graph 指標，存在另一個 Prometheus 相容的資料源，有請求數與失敗數（service_graph）
                 VictoriaTraces：-servicegraph.enableTask，從同一個 Jaeger 資料源的 /api/dependencies 讀，只有誰呼叫誰（dependencies）
  trace          事故時撈到的呼叫鏈裡的父子關係（rca.calls_in）
  log            某個系統的錯誤日誌裡，原樣出現另一個已知系統的名稱（mentions）。方向不一定是呼叫，比前兩種弱

兩個用途：
  1. 合併事故：兩個系統都在告警，而且**這段時間它們之間有失敗的呼叫、或其中一個的錯誤日誌指名另一個**，才併成一張（failing_links）。
     只是「平常有呼叫關係」不算：大家都連的元件不會把不相干的告警拉成同一張。
  2. 給 LLM 當線索：跟事故的系統有關的關係附進證據（main.py 用 db.calls_touching 取）。

不做的：不從「哪些系統常常一起告警」學（那是巧合，不是關係）；不用時間接近去猜誰接誰。
"""
from __future__ import annotations

import logging
import math
import re
from datetime import datetime
from typing import Any

from src.grafana import Grafana, GrafanaError

log = logging.getLogger("aiops_lite.graph")

REQUESTS, FAILED = "traces_service_graph_request_total", "traces_service_graph_request_failed_total"
# 延遲直方圖：server 端是被呼叫的那一段自己量的；client 端是呼叫的那一段量的（含網路）。
# 沒有埋點的資料庫、外部服務沒有 server 端的 span，只有 client 端的數字（Tempo 2.9.0 實測，見 service_graph.json）
LATENCY = {"server": "traces_service_graph_request_server_seconds", "client": "traces_service_graph_request_client_seconds"}
QUANTILE = 0.95
KINDS = {"messaging_system": "messaging", "database": "database"}   # Tempo 標的連線種類 → 我們用的寫法
VIRTUAL_CLIENT = "user"   # Tempo 把「沒有上游的入口請求」畫成 client="user" 的虛擬節點：不是系統，不記
MIN_NAME_CHARS = 3        # 用名稱比對日誌文字時，名稱至少要這麼長（跟 rca.MIN_NAME_CHARS 同一個理由）。未驗證泛化
MAX_RANGE_MINUTES = 1440


def _minutes(start: datetime, end: datetime) -> int:
    return min(MAX_RANGE_MINUTES, max(1, math.ceil((end - start).total_seconds() / 60)))


def _per_edge(grafana: Grafana, datasource_uid: str, expr: str, at: datetime) -> dict[tuple[str, str], float]:
    """一條單點查詢的結果照 (呼叫的, 被呼叫的) 加總。不是系統的（client="user" 的虛擬入口、自己呼叫自己）丟掉。"""
    out: dict[tuple[str, str], float] = {}
    for series in grafana.query_instant(datasource_uid, expr, at):
        labels = series["labels"]
        caller, callee = str(labels.get("client") or ""), str(labels.get("server") or "")
        if caller and callee and caller != callee and caller != VIRTUAL_CLIENT:
            out[(caller, callee)] = out.get((caller, callee), 0.0) + series["value"]
    return out


def service_graph(grafana: Grafana, datasource_uid: str, start: datetime, end: datetime,
                  baseline_minutes: int = 0) -> list[dict[str, Any]]:
    """[start, end] 這段時間 Tempo service graph 看到的呼叫：[{"caller", "callee", "requests", "failed", "kind"}]。
    兩次單點查詢（請求數、失敗數的增量）。kind 是 Tempo 標的連線種類（database／messaging_system／空字串）。
    baseline_minutes > 0 時每條呼叫再帶上延遲與基線（latency()）：拿不到就只有請求數與失敗數，不擋路。
    格式以 tests/fixtures/grafana13/service_graph.json 為準（Tempo 2.9.0 的 metrics-generator 寫進 VictoriaMetrics，經 Grafana 查）。"""
    minutes = _minutes(start, end)
    edges: dict[tuple[str, str], dict[str, Any]] = {}
    for metric, key in ((REQUESTS, "requests"), (FAILED, "failed")):
        for series in grafana.query_instant(datasource_uid, f"sum by (client, server, connection_type) (increase({metric}[{minutes}m]))", end):
            labels = series["labels"]
            caller, callee = str(labels.get("client") or ""), str(labels.get("server") or "")
            if not caller or not callee or caller == callee or caller == VIRTUAL_CLIENT:
                continue
            edge = edges.setdefault((caller, callee), {"caller": caller, "callee": callee, "requests": 0, "failed": 0,
                                                       "kind": KINDS.get(str(labels.get("connection_type") or ""), "")})
            edge[key] += round(series["value"])
    if baseline_minutes > 0 and edges:
        try:
            numbers = latency(grafana, datasource_uid, start, end, baseline_minutes,
                              {key: (e["requests"], e["failed"]) for key, e in edges.items()})
        except GrafanaError as e:   # 優雅降級：延遲拿不到，請求數與失敗數照用
            log.warning("讀不到 service graph 的延遲（只用請求數與失敗數）：%s", e)
            numbers = {}
        for key, edge in edges.items():
            edge.update(numbers.get(key, {}))
    return sorted(edges.values(), key=lambda e: (e["caller"], e["callee"]))


def latency(grafana: Grafana, datasource_uid: str, start: datetime, end: datetime, baseline_minutes: int,
            counts: dict[tuple[str, str], tuple[int, int]]) -> dict[tuple[str, str], dict[str, Any]]:
    """每條呼叫在事故期間（[start, end]）與基線（事故前 baseline_minutes 分鐘，[start - baseline, start)）的數字。只做算術，沒有門檻：
      rps、rps_baseline                每秒請求數（請求數的增量 ÷ 秒數）
      error_rate_pct、…_baseline_pct   失敗數 ÷ 請求數 × 100
      p95_ms、p95_baseline_ms           histogram_quantile(0.95)。是在直方圖的桶裡線性內插出來的：Tempo 預設的桶是 0.1、0.2、0.4…12.8 秒，
                                       100 毫秒以下的差別分不出來（12 毫秒與 45 毫秒都算成桶內的同一段）
      avg_ms、avg_baseline_ms           平均（耗時總和 ÷ 次數）。不受桶寬影響，補 p95 在快的呼叫上量不準的地方
      p95_ratio、avg_ratio              事故期間 ÷ 基線（基線是 0 或沒有就不算）
      latency_from                      數字取自哪一端：server（被呼叫的那一段自己量的）；server 端沒有資料（沒埋點的資料庫、外部服務）
                                        改用 client（呼叫的那一段量的，含網路）
    counts 是事故期間的 (請求數, 失敗數)，service_graph() 已經查過。事故期間與基線用同一端，兩邊才比得了。
    拿不到的欄位不放（不寫 0、不寫 null）。共 10 次單點查詢（基線的請求數與失敗數；兩端 × 兩段時間 × p95 與平均）。"""
    by = "client, server, connection_type"
    increase = lambda metric, minutes: f"sum by ({by}) (increase({metric}[{minutes}m]))"
    windows = {"": (end, _minutes(start, end)), "_baseline": (start, baseline_minutes)}   # (查詢的時間點, 往回看幾分鐘)
    raw = {name: _per_edge(grafana, datasource_uid, increase(metric, baseline_minutes), start)
           for metric, name in ((REQUESTS, "requests_baseline"), (FAILED, "failed_baseline"))}
    for suffix, (at, minutes) in windows.items():
        for side, metric in LATENCY.items():
            raw[f"p95_{side}{suffix}"] = _per_edge(grafana, datasource_uid,
                                                   f"histogram_quantile({QUANTILE}, sum by ({by}, le) (increase({metric}_bucket[{minutes}m])))", at)
            raw[f"avg_{side}{suffix}"] = _per_edge(grafana, datasource_uid,
                                                   f"{increase(metric + '_sum', minutes)} / {increase(metric + '_count', minutes)}", at)
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for key, (requests, failed) in counts.items():
        row: dict[str, Any] = {}
        before_requests = raw["requests_baseline"].get(key)
        for suffix, n, bad, seconds in (("", requests, failed, windows[""][1] * 60),
                                        ("_baseline", before_requests, raw["failed_baseline"].get(key, 0.0), baseline_minutes * 60)):
            if n is not None:
                row[f"rps{suffix}"] = _round(n / seconds)
                if n > 0:
                    row[f"error_rate{suffix}_pct"] = _round(100 * bad / n)
        side = next((s for s in LATENCY if key in raw[f"p95_{s}"] or key in raw[f"p95_{s}_baseline"]), None)
        if side:
            row["latency_from"] = side
            for stat in ("p95", "avg"):
                value, before = raw[f"{stat}_{side}"].get(key), raw[f"{stat}_{side}_baseline"].get(key)
                if value is not None:
                    row[f"{stat}_ms"] = _round(value * 1000)
                if before is not None:
                    row[f"{stat}_baseline_ms"] = _round(before * 1000)
                if value is not None and before:
                    row[f"{stat}_ratio"] = _round(value / before)
        out[key] = row
    return out


def _round(value: float) -> float:
    return float(f"{value:.4g}")   # 四位有效數字（跟指標走勢同一個寫法）


def dependencies(grafana: Grafana, datasource_uid: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    """VictoriaTraces 的 service graph（Jaeger 相容的 /api/dependencies）：[{"caller", "callee", "requests": None, "failed": None, "kind": ""}]。
    它只告訴我們誰呼叫誰：沒有失敗數；callCount 會重複計算（實測 v0.5.0，6 條 Trace 回報 108 次），所以不用；
    沒埋點的對象（只出現在 peer.service 的資料庫）不會在上面。沒開 -servicegraph.enableTask 的話回空的。"""
    raw = grafana.request("GET", f"/api/datasources/proxy/uid/{datasource_uid}/api/dependencies", params={
        "endTs": int(end.timestamp() * 1000), "lookback": min(MAX_RANGE_MINUTES * 60_000, max(60_000, int((end - start).total_seconds() * 1000)))})
    data = raw.get("data") if isinstance(raw, dict) else None
    if not isinstance(data, list):
        raise GrafanaError(f"相依關係查詢的回應沒有 data：{str(raw)[:200]}")
    pairs = sorted({(str(d.get("parent") or ""), str(d.get("child") or "")) for d in data if isinstance(d, dict)})
    return [{"caller": a, "callee": b, "requests": None, "failed": None, "kind": ""} for a, b in pairs if a and b and a != b]


def mentions(logs: list[dict[str, Any]], names: set[str]) -> list[tuple[str, str]]:
    """錯誤日誌裡「A 的訊息原樣提到 B」的 (A, B)。logs 是證據裡的日誌樣本（system、message）；names 是已知的系統名稱。
    比對完整的名稱、不分大小寫（前後不能是字母、數字、底線、連字號、點，免得 mes 對到 mes-api）。只認已知的名稱：
    沒有告警過、也不在圖上的元件，這裡學不到。"""
    pairs: list[tuple[str, str]] = []
    for name in sorted(n for n in names if len(n) >= MIN_NAME_CHARS):
        pattern = re.compile(rf"(?<![\w.\-]){re.escape(name)}(?![\w\-]|\.\w)", re.IGNORECASE)
        for entry in logs:
            owner = str(entry.get("system") or "")
            if owner and owner != name and pattern.search(str(entry.get("message") or "")):
                pairs.append((owner, name))
    return list(dict.fromkeys(pairs))


def failing_links(alerting: set[str], *, graph_edges: list[dict[str, Any]], trace_calls: list[tuple[str, str]],
                  log_mentions: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """可以拿來合併事故的連結：兩端都在告警，而且這段時間 (1) service graph 上這條呼叫有失敗，或 (2) Trace 裡這條呼叫自己失敗
    （trace_calls 要傳 rca.calls_in(..., failed_only=True) 的結果），
    或 (3) 一方的錯誤日誌指名另一方。回傳不分方向的 (a, b)。純函式。"""
    pairs = [(e["caller"], e["callee"]) for e in graph_edges if (e["failed"] or 0) > 0] + list(trace_calls) + list(log_mentions)
    return list(dict.fromkeys(tuple(sorted(p)) for p in pairs if p[0] in alerting and p[1] in alerting and p[0] != p[1]))


def read_service_graph(grafana: Grafana, *, traces_type: str, traces_uid: str, metrics_uid: str, start: datetime, end: datetime,
                       baseline_minutes: int = 0) -> list[dict[str, Any]]:
    """照 Trace 的來源讀它的 service graph，不擋路：沒有可讀的回空清單；查不動記一行 log、回空清單（這一輪當成沒有這個來源）。
    Tempo：要另外設 traces.service_graph_datasource_uid（存指標的資料源）。VictoriaTraces：用同一個 traces.datasource_uid。
    baseline_minutes > 0：Tempo 的每條呼叫多帶延遲與基線（VictoriaTraces 沒有這些數字，照舊只有誰呼叫誰）。"""
    try:
        if metrics_uid:
            return service_graph(grafana, metrics_uid, start, end, baseline_minutes)
        return dependencies(grafana, traces_uid, start, end) if traces_type == "victoriatraces" and traces_uid else []
    except GrafanaError as e:
        log.warning("讀不到 service graph：%s", e)
        return []
