"""拓撲方向：把 IT 的服務呼叫關係與工廠的 Lot 流向，統一成「異常會從 A 傳到 B」的有向邊。純函式。"""
from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import Any

from aiops.engine.context import LotEdge
from aiops.engine.graph import ServiceGraph


def service_edges(spans: Iterable[Any]) -> Counter:
    """span 的父子關係 -> 服務呼叫邊 (caller, callee)：子 span 的 parent_id 指到別的服務的 span，就是一次跨服務呼叫。

    上游的 client span 與下游的 server span 共用 trace，下游的 parent_id 就是上游那個 span 的 id，所以只靠
    span_id／parent_id 就能接起來，不需要 span.kind。同服務內的父子（handler -> db 查詢）不算。
    span 只要有 span_id、parent_id、service 三個屬性（批次匯入的 SpanIn 與 RCA 現抓的 SpanRecord 都有）。

    訊息消費者（consumer = True）不算：它的 parent 是生產者的 send span，但那是非同步邊，生產者不等它。
    收進來的話 propagation_edges 會把它翻成「消費者出事拖垮生產者」，方向剛好相反。非同步邊另外走 engine/mq.async_lags。
    """
    spans = list(spans)
    service_of = {s.span_id: s.service for s in spans if s.span_id}
    return Counter((service_of[s.parent_id], s.service) for s in spans
                   if s.parent_id in service_of and service_of[s.parent_id] != s.service
                   and not getattr(s, "consumer", False))


def service_edge_last_seen(spans: Iterable[Any]) -> dict[tuple[str, str], Any]:
    """每條服務呼叫邊 (caller, callee) 最後一筆跨服務呼叫的時間（被呼叫端那個 span 的 ts）。邊的定義跟 service_edges 同一個；
    span 沒有 ts 的不算。給關係圖的出處用（engine/graph.Provenance.last_seen）。"""
    spans = list(spans)
    service_of = {s.span_id: s.service for s in spans if s.span_id}
    out: dict[tuple[str, str], Any] = {}
    for s in spans:
        ts = getattr(s, "ts", None)
        if ts is None or s.parent_id not in service_of or service_of[s.parent_id] == s.service or getattr(s, "consumer", False):
            continue
        pair = (service_of[s.parent_id], s.service)
        if pair not in out or ts > out[pair]:
            out[pair] = ts
    return out


def lot_edges_from_paths(paths: list[list[str]]) -> list[LotEdge]:
    """Lot 路徑 -> 機台流向：每條相鄰的 (上游, 下游) 被幾個不同的 Lot 走過。"""
    lots_per_edge: dict[tuple[str, str], int] = {}
    for path in paths:
        for edge in {(a, b) for a, b in zip(path, path[1:]) if a != b}:
            lots_per_edge[edge] = lots_per_edge.get(edge, 0) + 1
    return [LotEdge(a, b, n) for (a, b), n in sorted(lots_per_edge.items(), key=lambda kv: (-kv[1], kv[0]))]


def propagation_edges(topology: list[tuple[str, str]], lot_edges: list[LotEdge], min_lots: int = 1) -> set[tuple[str, str]]:
    """IT 的 topology 是 (caller, callee)：callee（被呼叫的依賴）出事會拖垮 caller，所以傳播方向是 callee -> caller。
    工廠的 lot_edges 是 上游 -> 下游：上游機台出事會流到下游，傳播方向是 上游 -> 下游。
    兩者方向相反，所以 Lot 流向絕不能塞進 topology。方向的規則只有一份，在 engine/graph.py 的 KINDS；
    這個函式留給手上只有清單、沒有 ctx 的呼叫端（算子用 ctx.graph.propagation()）。
    """
    return ServiceGraph.from_parts(topology, lot_edges).propagation(min_lots)
