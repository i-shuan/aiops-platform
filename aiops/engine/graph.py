"""ServiceGraph：實體之間的關係圖。每條邊帶語意型別，「異常往哪個方向傳」由型別決定，不靠呼叫端記得誰要翻。

以前這張圖散成三個欄位（ctx.topology、ctx.lot_edges、ctx.async_edges），十個檔案各自用推導式拼出自己要的形狀
（無向相鄰、有向呼叫、傳播方向），方向只有 engine/topology.propagation_edges 一個函式記得要翻。現在算子一律問
ctx.graph；三個欄位留著當「載入與存檔的格式」（YAML、資料表、事件群快照都是這個形狀），只有載入端與寫入端碰。

邊的宣告方向（source -> target）照資料來源的說法，不是傳播方向：
  CALL   呼叫者 -> 被呼叫者（同步呼叫）。被呼叫的依賴出事會拖垮呼叫者，所以異常是 target -> source（反向）。
  ASYNC  生產者 -> 消費者（訊息佇列）。目前**不進傳播圖**：這次是不改行為的重構，照舊只有 alert.messaging_link 用它；
         納入傳播圖是之後的行為變更（docs/CROSS_DOMAIN_CONSISTENCY_AUDIT.md 的 M3），改 KINDS 裡那一格就好。
  FLOW   上游機台 -> 下游機台（Lot 流向）。上游出事會流到下游，異常是 source -> target（正向）。

圖可以有環（a 呼叫 b、b 又呼叫 a），這裡不假設它是 DAG；要拓撲順序的演算法自己處理環。
weight 的意義跟著型別：CALL 沒有用到（固定 1）、ASYNC 是延遲 P95 毫秒、FLOW 是走過這條邊的 Lot 數。

沒有狀態：ServiceGraph 建好就不變，ctx.graph 每次從當下的三個欄位現算、不快取（rca.build_dynamic_topology 會在
一輪中途改 ctx.topology，快取會讓後面的節點讀到舊圖）。
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any


class EdgeKind(Enum):
    CALL = "call"
    ASYNC = "async"
    FLOW = "flow"


@dataclass(frozen=True)
class KindSpec:
    reverse: bool  # True = 異常從 target 傳到 source（依賴出事拖垮依賴它的人）
    in_propagation: bool  # 這種邊算不算進傳播圖


KINDS: dict[EdgeKind, KindSpec] = {
    EdgeKind.CALL: KindSpec(reverse=True, in_propagation=True),
    EdgeKind.ASYNC: KindSpec(reverse=False, in_propagation=False),
    EdgeKind.FLOW: KindSpec(reverse=False, in_propagation=True),
}


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    kind: EdgeKind
    weight: float = 1.0

    @property
    def is_loop(self) -> bool:
        return self.source == self.target

    @property
    def pair(self) -> tuple[str, str]:
        """宣告方向 (source, target)。"""
        return (self.source, self.target)

    @property
    def propagation(self) -> tuple[str, str]:
        """異常傳播方向 (先出事的, 被影響的)。"""
        return (self.target, self.source) if KINDS[self.kind].reverse else (self.source, self.target)


class ServiceGraph:
    def __init__(self, edges: Iterable[Edge] = ()):
        self._edges: tuple[Edge, ...] = tuple(edges)

    @classmethod
    def from_parts(cls, calls: Iterable[Any] = (), lot_edges: Iterable[Any] = (),
                   async_edges: Mapping[tuple[str, str], float] | None = None) -> ServiceGraph:
        """從載入格式組圖：calls 是 (caller, callee)；lot_edges 的元素有 upstream／downstream／lots；
        async_edges 是 (producer, consumer) -> 延遲 P95 毫秒。順序照給的順序，自迴圈與重複都留著（查詢時才濾）。"""
        edges = [Edge(str(a), str(b), EdgeKind.CALL) for a, b in calls]
        edges += [Edge(e.upstream, e.downstream, EdgeKind.FLOW, float(e.lots)) for e in lot_edges]
        edges += [Edge(p, c, EdgeKind.ASYNC, float(lag)) for (p, c), lag in (async_edges or {}).items()]
        return cls(edges)

    def edges(self, kind: EdgeKind | None = None) -> tuple[Edge, ...]:
        return self._edges if kind is None else tuple(e for e in self._edges if e.kind is kind)

    def declared(self, kind: EdgeKind) -> list[tuple[str, str]]:
        """這種邊照宣告順序的 (source, target) 清單，含自迴圈與重複（原樣，給要保留順序的寫入端用）。"""
        return [e.pair for e in self._edges if e.kind is kind]

    def _real(self, kind: EdgeKind, min_weight: float | None) -> Iterable[Edge]:
        return (e for e in self._edges if e.kind is kind and not e.is_loop
                and (min_weight is None or e.weight >= min_weight))

    def pairs(self, kind: EdgeKind, min_weight: float | None = None) -> set[tuple[str, str]]:
        """有向 (source, target)，不含自迴圈。"""
        return {e.pair for e in self._real(kind, min_weight)}

    def adjacent(self, kind: EdgeKind, min_weight: float | None = None) -> set[frozenset[str]]:
        """無向相鄰（不管方向，只問兩個實體之間有沒有這種邊），不含自迴圈。"""
        return {frozenset(e.pair) for e in self._real(kind, min_weight)}

    def weights(self, kind: EdgeKind) -> dict[tuple[str, str], float]:
        """有向 (source, target) -> weight，不含自迴圈。"""
        return {e.pair: e.weight for e in self._real(kind, None)}

    def propagation(self, min_lots: float = 1) -> set[tuple[str, str]]:
        """異常傳播方向的有向邊 (先出事的, 被影響的)，不含自迴圈。只收 KINDS 標了 in_propagation 的型別。
        min_lots：走過的 Lot 少於這個數的流向不算（只影響 FLOW）。"""
        out: set[tuple[str, str]] = set()
        for e in self._edges:
            if e.is_loop or not KINDS[e.kind].in_propagation:
                continue
            if e.kind is EdgeKind.FLOW and e.weight < min_lots:
                continue
            out.add(e.propagation)
        return out

    def nodes(self, kind: EdgeKind | None = None) -> set[str]:
        """邊上出現過的實體（含只出現在自迴圈上的）。"""
        return {n for e in self.edges(kind) for n in e.pair}

    def __len__(self) -> int:
        return len(self._edges)

    def __bool__(self) -> bool:
        return bool(self._edges)
