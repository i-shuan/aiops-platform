"""ServiceGraph：實體之間的關係圖。每條邊帶語意型別，「異常往哪個方向傳」由型別決定，不靠呼叫端記得誰要翻。

以前這張圖散成三個欄位（ctx.topology、ctx.lot_edges、ctx.async_edges），十個檔案各自用推導式拼出自己要的形狀
（無向相鄰、有向呼叫、傳播方向），方向只有 engine/topology.propagation_edges 一個函式記得要翻。現在算子一律問
ctx.graph；三個欄位留著當「載入與存檔的格式」（YAML、資料表、事件群快照都是這個形狀），只有載入端與寫入端碰。

邊的宣告方向（source -> target）照資料來源的說法，不是傳播方向：
  CALL   呼叫者 -> 被呼叫者（同步呼叫）。被呼叫的依賴出事會拖垮呼叫者，所以異常是 target -> source（反向）。
  ASYNC  生產者 -> 消費者（訊息佇列）。目前**不進傳播圖**：這次是不改行為的重構，照舊只有 alert.messaging_link 用它；
         納入傳播圖是之後的行為變更（docs/CROSS_DOMAIN_CONSISTENCY_AUDIT.md 的 M3），改 KINDS 裡那一格就好。
  FLOW   上游機台 -> 下游機台（Lot 流向）。上游出事會流到下游，異常是 source -> target（正向）。
  SUPPLY 供應端 -> 使用端（供電、供氣、冷卻水、共用主機：沒有 trace、靠人宣告的依賴）。供應端出事使用端跟著出事，
         異常是 source -> target（正向）。跟 CALL 是同一種依賴關係從另一頭寫：「A 依賴 B」可以宣告成 call A -> B，
         也可以宣告成 supply B -> A，傳播方向都是 B -> A——寫的人只要照資料來源的說法選型別，不用自己翻方向。

宣告的邊（Workflow 的 graph、admin.graph_import 匯入的 declared_edges）每一條都必須寫 kind，沒有預設值：
沒有 trace 的環境（工廠、純指標）整張圖都靠宣告，寫的人以為的方向跟程式以為的方向不一樣，根因就會指反。
同一對實體被不同型別宣告成相反的傳播方向（call A -> B 又 supply A -> B）在載入期報錯（contradictions）。

圖可以有環（a 呼叫 b、b 又呼叫 a），這裡不假設它是 DAG；要拓撲順序的演算法自己處理環。
weight 的意義跟著型別：CALL、SUPPLY 沒有用到（固定 1）、ASYNC 是延遲 P95 毫秒、FLOW 是走過這條邊的 Lot 數
（宣告的 flow 邊沒寫 weight 就是 1；算子用 min_lots 篩流向時，要通過就把 weight 寫成實際的量）。

每條邊帶出處（Provenance）：哪裡來的（origin）、來源的細節（detail）、最後觀測或更新的時間（last_seen）。
出處只用來「交代這條邊憑什麼存在」（RCA 報告的 rca.section.graph_basis），不參與任何判斷：兩條邊相不相等、
傳播方向、收斂都不看它（compare=False）。同一對實體可以同時有宣告的邊與觀測到的邊，各自帶自己的出處。
沒有「圖資版本」：用時間代替——as_of() 回每種出處最新的 last_seen，報告照實寫「這次用的圖是什麼時候的」。

沒有狀態：ServiceGraph 建好就不變，ctx.graph 每次從當下的三個欄位現算、不快取（rca.build_dynamic_topology 會在
一輪中途改 ctx.topology，快取會讓後面的節點讀到舊圖）。
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class EdgeKind(Enum):
    CALL = "call"
    ASYNC = "async"
    FLOW = "flow"
    SUPPLY = "supply"


@dataclass(frozen=True)
class KindSpec:
    reverse: bool  # True = 異常從 target 傳到 source（依賴出事拖垮依賴它的人）
    in_propagation: bool  # 這種邊算不算進傳播圖
    # True = 這種邊是「一方依賴另一方」（CALL：source 依賴 target；SUPPLY：target 依賴 source，同一種關係從兩頭寫）。
    # 依賴邊的收斂（alert.topology_chain）與同根因抑制（alert.suppress_same_root_cause）問 dependencies()，
    # 所以宣告成 call 或 supply 的結果一樣；FLOW 是流向不是依賴（有 Lot 數門檻，另有 alert.lot_path_chain）
    dependency: bool = False


KINDS: dict[EdgeKind, KindSpec] = {
    EdgeKind.CALL: KindSpec(reverse=True, in_propagation=True, dependency=True),
    EdgeKind.ASYNC: KindSpec(reverse=False, in_propagation=False),
    EdgeKind.FLOW: KindSpec(reverse=False, in_propagation=True),
    EdgeKind.SUPPLY: KindSpec(reverse=False, in_propagation=True, dependency=True),
}


# 邊的出處（Provenance.origin）
ORIGIN_TRACE = "trace"  # admin.topology_refresh／admin.merge_traces 從 trace 沉澱進資料表的；last_seen = 最近一次刷新看到它
ORIGIN_INCIDENT_TRACE = "incident_trace"  # 這次 RCA 撈到的 span 裡直接看到的；last_seen = 這條邊最後一筆 span 的時間
ORIGIN_DECLARED = "declared"  # admin.graph_import 匯入的宣告；detail = 匯入時填的來源，last_seen = 最後更新時間
ORIGIN_WORKFLOW = "workflow"  # 管線 YAML 寫死的（topology／lot_edges／graph）；沒有時間
ORIGIN_LOT = "lot_paths"  # 從 Lot 履歷推導的流向；沒有時間
ORIGIN_UNKNOWN = ""  # 沒記（舊的事件群快照、測試直接給的邊）


@dataclass(frozen=True)
class Provenance:
    origin: str = ORIGIN_UNKNOWN
    detail: str = ""
    last_seen: datetime | None = None


UNKNOWN = Provenance()


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    kind: EdgeKind
    weight: float = 1.0
    provenance: Provenance = field(default=UNKNOWN, compare=False)  # 出處不算進相等與雜湊：同一條邊就是同一條邊

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


def contradictions(edges: Iterable[Edge]) -> list[tuple[Edge, Edge]]:
    """不同型別把同一對實體宣告成相反傳播方向的邊對（例如 call A -> B 是「B 拖垮 A」，supply A -> B 是「A 拖垮 B」）。
    同一種型別的互相依賴（a 呼叫 b、b 又呼叫 a）是真的環，不算矛盾；不進傳播圖的型別（ASYNC）不檢查。"""
    seen: dict[tuple[str, str], Edge] = {}
    out: list[tuple[Edge, Edge]] = []
    for e in edges:
        if e.is_loop or not KINDS[e.kind].in_propagation:
            continue
        opposite = seen.get((e.propagation[1], e.propagation[0]))
        if opposite is not None and opposite.kind is not e.kind:
            out.append((opposite, e))
        seen.setdefault(e.propagation, e)
    return out


class ServiceGraph:
    def __init__(self, edges: Iterable[Edge] = ()):
        self._edges: tuple[Edge, ...] = tuple(edges)

    @classmethod
    def from_parts(cls, calls: Iterable[Any] = (), lot_edges: Iterable[Any] = (),
                   async_edges: Mapping[tuple[str, str], float] | None = None,
                   declared: Iterable[Edge] = (),
                   provenance: Mapping[tuple[str, str, str], Provenance] | None = None) -> ServiceGraph:
        """從載入格式組圖：calls 是 (caller, callee)；lot_edges 的元素有 upstream／downstream／lots；
        async_edges 是 (producer, consumer) -> 延遲 P95 毫秒。順序照給的順序，自迴圈與重複都留著（查詢時才濾）。
        provenance：(型別的值, source, target) -> 出處，給前三種載入格式的邊用（它們的格式放不下出處）；宣告的邊自己帶。"""
        known = provenance or {}

        def why(kind: EdgeKind, a: str, b: str) -> Provenance:
            return known.get((kind.value, a, b), UNKNOWN)

        edges = [Edge(str(a), str(b), EdgeKind.CALL, provenance=why(EdgeKind.CALL, str(a), str(b))) for a, b in calls]
        edges += [Edge(e.upstream, e.downstream, EdgeKind.FLOW, float(e.lots), why(EdgeKind.FLOW, e.upstream, e.downstream))
                  for e in lot_edges]
        edges += [Edge(p, c, EdgeKind.ASYNC, float(lag), why(EdgeKind.ASYNC, p, c)) for (p, c), lag in (async_edges or {}).items()]
        edges += list(declared)  # 宣告的邊（每條自帶型別）接在觀測到的後面；重複的邊查詢時是集合，不會算兩次
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

    def dependencies(self) -> set[tuple[str, str]]:
        """有向 (依賴別人的, 被依賴的)，不含自迴圈：CALL 的 (呼叫者, 被呼叫者) 加上 SUPPLY 的 (使用端, 供應端)。
        只有 CALL 邊的圖，結果跟 pairs(EdgeKind.CALL) 一樣。"""
        return {(e.propagation[1], e.propagation[0]) for e in self._edges if KINDS[e.kind].dependency and not e.is_loop}

    def dependency_adjacent(self) -> set[frozenset[str]]:
        """無向相鄰：兩個實體之間有沒有依賴邊（不管誰依賴誰）。只有 CALL 邊的圖，結果跟 adjacent(EdgeKind.CALL) 一樣。"""
        return {frozenset(p) for p in self.dependencies()}

    def between(self, entities: Iterable[str]) -> tuple[Edge, ...]:
        """兩端都在 entities 裡的邊（所有型別、不含自迴圈），照建圖順序。同一對實體有幾種出處就回幾條。"""
        wanted = set(entities)
        return tuple(e for e in self._edges if not e.is_loop and e.source in wanted and e.target in wanted)

    def as_of(self) -> dict[str, datetime]:
        """每種出處最新的 last_seen（沒有時間的出處不列）：「這張圖是什麼時候的」，代替圖資版本。"""
        out: dict[str, datetime] = {}
        for e in self._edges:
            p = e.provenance
            if p.last_seen is not None and (p.origin not in out or p.last_seen > out[p.origin]):
                out[p.origin] = p.last_seen
        return out

    def nodes(self, kind: EdgeKind | None = None) -> set[str]:
        """邊上出現過的實體（含只出現在自迴圈上的）。"""
        return {n for e in self.edges(kind) for n in e.pair}

    def __len__(self) -> int:
        return len(self._edges)

    def __bool__(self) -> bool:
        return bool(self._edges)
