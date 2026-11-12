"""ServiceGraph（engine/graph.py）：邊帶語意型別，傳播方向由型別決定。

這次是不改行為的重構：每個查詢都跟「重構前各算子自己寫的推導式」對同一批隨機圖逐一比對（含自迴圈、重複邊、有環、
Lot 數為 0 的流向）。舊的推導式原樣抄在 _old_* 裡當對照，不是被測程式。
"""
import random
from datetime import datetime, timezone

import pytest

from aiops.engine.context import LotEdge, PipelineContext
from aiops.engine.graph import KINDS, Edge, EdgeKind, ServiceGraph
from aiops.engine.topology import propagation_edges

NAMES = ["a", "b", "c", "d", "e", "f"]


def _random_parts(seed: int):
    rnd = random.Random(seed)
    calls = [(rnd.choice(NAMES), rnd.choice(NAMES)) for _ in range(rnd.randint(0, 12))]
    lots = [LotEdge(rnd.choice(NAMES), rnd.choice(NAMES), rnd.randint(0, 5)) for _ in range(rnd.randint(0, 8))]
    mq = {(rnd.choice(NAMES), rnd.choice(NAMES)): rnd.uniform(0, 90_000) for _ in range(rnd.randint(0, 6))}
    return calls, lots, mq


# ── 重構前各算子自己寫的推導式（原樣），當對照 ──

def _old_topology_chain(topology):
    return {frozenset(e) for e in topology if e[0] != e[1]}


def _old_messaging_link(async_edges):
    return {(p, c): lag for (p, c), lag in async_edges.items() if p != c}


def _old_lot_path_chain(lot_edges, min_lots):
    return {frozenset((e.upstream, e.downstream)) for e in lot_edges if e.upstream != e.downstream and e.lots >= min_lots}


def _old_same_root_cause(topology):
    return {(caller, callee) for caller, callee in topology if caller != callee}


def _old_propagation(topology, lot_edges, min_lots=1):
    edges = {(callee, caller) for caller, callee in topology if caller != callee}
    edges |= {(e.upstream, e.downstream) for e in lot_edges if e.upstream != e.downstream and e.lots >= min_lots}
    return edges


def _old_known_entities(topology, lot_edges):
    names = {n for edge in topology for n in edge}
    names.update(n for edge in _old_propagation(topology, lot_edges) for n in edge)
    return names


@pytest.mark.parametrize("seed", range(200))
def test_every_query_matches_what_the_operators_used_to_compute(seed):
    calls, lots, mq = _random_parts(seed)
    graph = ServiceGraph.from_parts(calls, lots, mq)
    assert graph.adjacent(EdgeKind.CALL) == _old_topology_chain(calls)
    assert graph.weights(EdgeKind.ASYNC) == _old_messaging_link(mq)
    for min_lots in (1, 2, 4):
        assert graph.adjacent(EdgeKind.FLOW, min_weight=min_lots) == _old_lot_path_chain(lots, min_lots)
        assert graph.propagation(min_lots) == _old_propagation(calls, lots, min_lots)
        assert propagation_edges(calls, lots, min_lots) == _old_propagation(calls, lots, min_lots)
    assert graph.pairs(EdgeKind.CALL) == _old_same_root_cause(calls)
    assert sorted(graph.pairs(EdgeKind.CALL)) == sorted({(a, b) for a, b in calls if a != b})  # rca_judge.graph_rows
    assert graph.propagation() == _old_propagation(calls, lots)
    assert graph.nodes(EdgeKind.CALL) | {n for e in graph.propagation() for n in e} == _old_known_entities(calls, lots)
    assert graph.declared(EdgeKind.CALL) == list(calls)  # 順序、重複、自迴圈都原樣


# ── 型別決定方向 ──

def test_a_call_edge_propagates_from_callee_to_caller_and_a_flow_edge_from_upstream_to_downstream():
    assert Edge("web", "db", EdgeKind.CALL).propagation == ("db", "web")
    assert Edge("etch", "litho", EdgeKind.FLOW).propagation == ("etch", "litho")
    graph = ServiceGraph.from_parts([("web", "db")], [LotEdge("etch", "litho", 3)])
    assert graph.propagation() == {("db", "web"), ("etch", "litho")}


def test_async_edges_are_kept_out_of_the_propagation_graph_for_now():
    """現況照舊（不改行為）：佇列邊只有 alert.messaging_link 用，RCA 的傳播圖看不到。
    要納入時改 KINDS[ASYNC].in_propagation，這個測試會紅，提醒那是行為變更、要重跑混沌回歸。"""
    graph = ServiceGraph.from_parts([], [], {("producer", "consumer"): 1200.0})
    assert not KINDS[EdgeKind.ASYNC].in_propagation
    assert graph.propagation() == set()
    assert graph.weights(EdgeKind.ASYNC) == {("producer", "consumer"): 1200.0}


def test_every_edge_kind_declares_its_direction():
    assert set(KINDS) == set(EdgeKind)


def test_a_cycle_is_a_legal_graph():
    graph = ServiceGraph.from_parts([("a", "b"), ("b", "a")])
    assert graph.propagation() == {("a", "b"), ("b", "a")}
    assert graph.adjacent(EdgeKind.CALL) == {frozenset(("a", "b"))}


def test_self_loops_are_dropped_from_queries_but_their_nodes_are_known():
    graph = ServiceGraph.from_parts([("a", "a")], [LotEdge("m", "m", 9)], {("q", "q"): 5.0})
    assert not graph.pairs(EdgeKind.CALL) and not graph.adjacent(EdgeKind.FLOW) and not graph.weights(EdgeKind.ASYNC)
    assert not graph.propagation()
    assert graph.nodes() == {"a", "m", "q"} and len(graph) == 3


def test_an_empty_graph_is_falsy():
    assert not ServiceGraph() and not ServiceGraph.from_parts()
    assert ServiceGraph.from_parts([("a", "b")])


# ── ctx.graph：每次現算，不快取 ──

def test_ctx_graph_reflects_edges_added_in_the_middle_of_a_round():
    """rca.build_dynamic_topology 在一輪中途改 ctx.topology；後面的節點要讀到新圖。"""
    ctx = PipelineContext("p", datetime(2026, 11, 3, tzinfo=timezone.utc), topology=[("a", "b")])
    assert ctx.graph.pairs(EdgeKind.CALL) == {("a", "b")}
    ctx.topology = [*ctx.topology, ("b", "c")]
    ctx.lot_edges = [LotEdge("m1", "m2", 2)]
    ctx.async_edges = {("p", "c"): 10.0}
    graph = ctx.graph
    assert graph.pairs(EdgeKind.CALL) == {("a", "b"), ("b", "c")}
    assert graph.propagation() == {("b", "a"), ("c", "b"), ("m1", "m2")}
    assert graph.weights(EdgeKind.ASYNC) == {("p", "c"): 10.0}


def test_a_new_context_starts_with_an_empty_graph():
    """常駐行程跨輪次：圖沒有掛在模組或類別上，每輪新建的 ctx 從自己的欄位算。"""
    first = PipelineContext("p", datetime(2026, 11, 3, tzinfo=timezone.utc), topology=[("a", "b")])
    assert first.graph
    assert not PipelineContext("p", datetime(2026, 11, 3, tzinfo=timezone.utc)).graph
