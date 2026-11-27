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
import re
from datetime import datetime
from typing import Any

from src import promql
from src.grafana import Grafana, GrafanaError
from src.promql import MAX_RANGE_MINUTES, Window

log = logging.getLogger("aiops_lite.graph")

REQUESTS, FAILED = "traces_service_graph_request_total", "traces_service_graph_request_failed_total"
# 延遲直方圖：server 端是被呼叫的那一段自己量的；client 端是呼叫的那一段量的（含網路）。
# Tempo 2.9.0 實測：沒埋點的資料庫，它把呼叫端量的耗時也填進 server 那一份（兩端一模一樣，見 service_graph.json）
LATENCY = {"server": "traces_service_graph_request_server_seconds", "client": "traces_service_graph_request_client_seconds"}
QUANTILE = 0.95
BY = "client, server, connection_type"
KINDS = {"messaging_system": "messaging", "database": "database"}   # Tempo 標的連線種類 → 我們用的寫法
VIRTUAL_CLIENT = "user"   # Tempo 把「沒有上游的入口請求」畫成 client="user" 的虛擬節點：不是系統，不記
MIN_NAME_CHARS = 3        # 用名稱比對日誌文字時，名稱至少要這麼長（跟 rca.MIN_NAME_CHARS 同一個理由）。未驗證泛化
# 一條邊在這段時間的數字（latency() 算的）。查詢層算、裁切層照搬、證據層原樣交給 LLM：三個地方用同一份清單
EDGE_FIELDS = ("rps", "rps_baseline", "error_rate_pct", "error_rate_baseline_pct", "p95_ms", "p95_baseline_ms", "p95_ratio",
               "avg_ms", "avg_baseline_ms", "avg_ratio", "latency_from")


# ── 查詢與算術 ──

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
    window = Window(start, end, baseline_minutes)
    edges: dict[tuple[str, str], dict[str, Any]] = {}
    for metric, key in ((REQUESTS, "requests"), (FAILED, "failed")):
        for series in grafana.query_instant(datasource_uid, promql.increase(metric, BY, window.minutes), end):
            labels = series["labels"]
            caller, callee = str(labels.get("client") or ""), str(labels.get("server") or "")
            if not caller or not callee or caller == callee or caller == VIRTUAL_CLIENT:
                continue
            edge = edges.setdefault((caller, callee), {"caller": caller, "callee": callee, "requests": 0, "failed": 0,
                                                       "kind": KINDS.get(str(labels.get("connection_type") or ""), "")})
            edge[key] += round(series["value"])
    if baseline_minutes > 0 and edges:
        try:
            numbers = latency(grafana, datasource_uid, window, {key: (e["requests"], e["failed"]) for key, e in edges.items()})
        except GrafanaError as e:   # 優雅降級：延遲拿不到，請求數與失敗數照用
            log.warning("讀不到 service graph 的延遲（只用請求數與失敗數）：%s", e)
            numbers = {}
        for key, edge in edges.items():
            edge.update(numbers.get(key, {}))
    return sorted(edges.values(), key=lambda e: (e["caller"], e["callee"]))


def latency(grafana: Grafana, datasource_uid: str, window: Window,
            counts: dict[tuple[str, str], tuple[int, int]]) -> dict[tuple[str, str], dict[str, Any]]:
    """每條呼叫在事故期間（window.start～end）與基線（事故前 window.baseline_minutes 分鐘）的數字。只做算術，沒有門檻：
      rps、rps_baseline                每秒請求數（請求數的增量 ÷ 秒數）
      error_rate_pct、…_baseline_pct   失敗數 ÷ 請求數 × 100
      p95_ms、p95_baseline_ms           histogram_quantile(0.95)。是在直方圖的桶裡線性內插出來的：Tempo 預設的桶是 0.1、0.2、0.4…12.8 秒，
                                       100 毫秒以下的差別分不出來（12 毫秒與 45 毫秒都算成桶內的同一段）
      avg_ms、avg_baseline_ms           平均（耗時總和 ÷ 次數）。不受桶寬影響，補 p95 在快的呼叫上量不準的地方
      p95_ratio、avg_ratio              事故期間 ÷ 基線（基線是 0 或沒有就不算）
      latency_from                      數字取自哪一端：server（被呼叫的那一段自己量的）；server 端沒有資料改用 client
                                        （呼叫的那一段量的，含網路）
    counts 是事故期間的 (請求數, 失敗數)，service_graph() 已經查過。事故期間與基線用同一端，兩邊才比得了。
    拿不到的欄位不放（不寫 0、不寫 null）。共 10 次單點查詢（基線的請求數與失敗數；兩端 × 兩段時間 × p95 與平均）。"""
    raw = _latency_queries(grafana, datasource_uid, window)
    return {key: edge_numbers(key, requests, failed, raw, window) for key, (requests, failed) in counts.items()}


def _latency_queries(grafana: Grafana, datasource_uid: str, window: Window) -> dict[str, dict[tuple[str, str], float]]:
    """查詢：照 (呼叫的, 被呼叫的) 分好的原始數值，鍵是 requests_baseline、p95_server、avg_client_baseline 這種名稱。"""
    points = {"": (window.end, window.minutes), "_baseline": (window.start, window.baseline_minutes)}   # (查詢的時間點, 往回看幾分鐘)
    raw = {name: _per_edge(grafana, datasource_uid, promql.increase(metric, BY, window.baseline_minutes), window.start)
           for metric, name in ((REQUESTS, "requests_baseline"), (FAILED, "failed_baseline"))}
    for suffix, (at, minutes) in points.items():
        for side, histogram in LATENCY.items():
            raw[f"p95_{side}{suffix}"] = _per_edge(grafana, datasource_uid, promql.quantile(QUANTILE, histogram, BY, minutes), at)
            raw[f"avg_{side}{suffix}"] = _per_edge(grafana, datasource_uid, promql.mean(histogram, BY, minutes), at)
    return raw


def edge_numbers(key: tuple[str, str], requests: int, failed: int, raw: dict[str, dict[tuple[str, str], float]],
                 window: Window) -> dict[str, Any]:
    """算術（純函式）：一條呼叫的原始數值 → 給 LLM 的數字。欄位見 latency()。"""
    row: dict[str, Any] = {}
    for suffix, n, bad, seconds in (("", requests, failed, window.minutes * 60),
                                    ("_baseline", raw["requests_baseline"].get(key), raw["failed_baseline"].get(key, 0.0),
                                     window.baseline_minutes * 60)):
        if n is not None:
            row[f"rps{suffix}"] = promql.round4(n / seconds)
            if n > 0:
                row[f"error_rate{suffix}_pct"] = promql.round4(100 * bad / n)
    side = next((s for s in LATENCY if key in raw[f"p95_{s}"] or key in raw[f"p95_{s}_baseline"]), None)
    if side:
        row["latency_from"] = side
        for stat in ("p95", "avg"):
            value, before = raw[f"{stat}_{side}"].get(key), raw[f"{stat}_{side}_baseline"].get(key)
            if value is not None:
                row[f"{stat}_ms"] = promql.round4(value * 1000)
            if before is not None:
                row[f"{stat}_baseline_ms"] = promql.round4(before * 1000)
            if (r := promql.ratio(value, before)) is not None:
                row[f"{stat}_ratio"] = r
    return row


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


# ── 子圖裁切（給 LLM 之前：取哪幾條、照什麼順序）──

def prune(first: list[dict[str, Any]], second: list[dict[str, Any]], live: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """純函式。first 是直接連到告警系統的關係（hops=1），second 是那些鄰居自己連到的（hops=2），都是 db.calls_touching 的列；
    live 是這段時間 service graph 上的邊（帶次數與延遲）。回傳最多 limit 條，service graph 來源的列帶上這段時間的數字。
    排序只是算術，沒有門檻：一跳的在前；同一跳裡 p95（沒有就看平均）變成基線的幾倍由大到小，沒有倍數的照原本的順序（最近看到的在前）。"""
    numbers_of = {(e["caller"], e["callee"]): e for e in live}
    out, seen = [], set()
    for hops, rows in ((1, first), (2, second)):
        for row in rows:
            key = (row["caller"], row["callee"], row["source"])
            if key in seen:
                continue
            seen.add(key)
            numbers = numbers_of.get((row["caller"], row["callee"])) if row["source"] == "service_graph" else None
            out.append({**row, "hops": hops, **({k: numbers[k] for k in ("requests", "failed", "kind", *EDGE_FIELDS)
                                                 if numbers.get(k) is not None} if numbers else {})})
    growth = lambda e: e.get("p95_ratio", e.get("avg_ratio"))
    order = sorted(range(len(out)), key=lambda i: (out[i]["hops"], growth(out[i]) is None, -(growth(out[i]) or 0), i))
    return [out[i] for i in order][:limit]
