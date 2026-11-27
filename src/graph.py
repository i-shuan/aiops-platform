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
VIRTUAL_CLIENT = "user"   # Tempo 把「沒有上游的入口請求」畫成 client="user" 的虛擬節點：不是系統，不記
MIN_NAME_CHARS = 3        # 用名稱比對日誌文字時，名稱至少要這麼長（跟 rca.MIN_NAME_CHARS 同一個理由）。未驗證泛化
MAX_RANGE_MINUTES = 1440


def service_graph(grafana: Grafana, datasource_uid: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    """[start, end] 這段時間 Tempo service graph 看到的呼叫：[{"caller", "callee", "requests", "failed", "kind"}]。
    兩次單點查詢（請求數、失敗數的增量）。kind 是 Tempo 標的連線種類（database／messaging_system／空字串）。
    格式以 tests/fixtures/grafana13/service_graph_*.json 為準（Tempo 2.9.0 的 metrics-generator 寫進 VictoriaMetrics，經 Grafana 查）。"""
    minutes = min(MAX_RANGE_MINUTES, max(1, math.ceil((end - start).total_seconds() / 60)))
    edges: dict[tuple[str, str], dict[str, Any]] = {}
    for metric, key in ((REQUESTS, "requests"), (FAILED, "failed")):
        for series in grafana.query_instant(datasource_uid, f"sum by (client, server, connection_type) (increase({metric}[{minutes}m]))", end):
            labels = series["labels"]
            caller, callee = str(labels.get("client") or ""), str(labels.get("server") or "")
            if not caller or not callee or caller == callee or caller == VIRTUAL_CLIENT:
                continue
            edge = edges.setdefault((caller, callee), {"caller": caller, "callee": callee, "requests": 0, "failed": 0,
                                                       "kind": str(labels.get("connection_type") or "")})
            edge[key] += round(series["value"])
    return sorted(edges.values(), key=lambda e: (e["caller"], e["callee"]))


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


def read_service_graph(grafana: Grafana, *, traces_type: str, traces_uid: str, metrics_uid: str, start: datetime, end: datetime
                       ) -> list[dict[str, Any]]:
    """照 Trace 的來源讀它的 service graph，不擋路：沒有可讀的回空清單；查不動記一行 log、回空清單（這一輪當成沒有這個來源）。
    Tempo：要另外設 traces.service_graph_datasource_uid（存指標的資料源）。VictoriaTraces：用同一個 traces.datasource_uid。"""
    try:
        if metrics_uid:
            return service_graph(grafana, metrics_uid, start, end)
        return dependencies(grafana, traces_uid, start, end) if traces_type == "victoriatraces" and traces_uid else []
    except GrafanaError as e:
        log.warning("讀不到 service graph：%s", e)
        return []
