"""rca.root_cause.bayes（docs/COGNITIVE_RCA_DESIGN.md §5）：手造 Fact／EvidenceItem 餵機率模型。

證據層與 LLM 抽取還在別的分支施工，這裡照合約的型別與詞彙自己造輸入。每個情境都對應舊啟發式的一個死穴：
場景 17 熔斷、16 逾時（子 span 比父 span 晚結束）、21 循環呼叫、18 鎖車隊、22 GC 停頓。
"""
import random
from datetime import datetime, timedelta, timezone

import pytest

from aiops.engine.context import PipelineContext
from aiops.engine.errors import WorkflowError
from aiops.engine.rca_types import EvidenceItem, Fact
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.interfaces.runner import load_all
from aiops.stages.rca.bayes.graph import Graph, strongly_connected

T0 = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)

load_all()


def F(kind, entity, value, unit="", window="delta", **detail):
    return Fact(kind, entity, float(value), unit, window, detail, "test")


def E(kind, entity, confidence=0.9, state="present", quote="..."):
    return EvidenceItem(kind, entity, state, confidence, quote, T0.isoformat())


def run(topology, incident_entities, facts, evidence=(), status="ok", **params):
    ctx = PipelineContext(pipeline="rca", now=T0 + timedelta(hours=1))
    ctx.topology = list(topology)
    ctx.rca_facts, ctx.rca_evidence, ctx.rca_status = list(facts), list(evidence), status
    ctx.incidents = [Incident([AnomalyEvent(e, "latency", T0 + timedelta(minutes=m), 6.0)
                               for e in incident_entities for m in range(5)])]
    ctx.incidents_built = True
    op = OperatorRegistry.create("rca.root_cause.bayes", params)
    ctx = op.execute(ctx)
    return ctx.rca_root_candidates, ctx


def top(cands):
    return cands[0].entity if cands else None


def score(cands, entity):
    return next((c.score for c in cands if c.entity == entity), 0.0)


def assert_valid(cands):
    assert sum(c.score for c in cands) <= 1.0 + 1e-9
    assert all(0.0 <= c.score <= 1.0 for c in cands)
    assert [c.score for c in cands] == sorted((c.score for c in cands), reverse=True)
    for c in cands:
        assert c.reason and "posterior" in c.attribution and "evidence_used" in c.attribution
        assert abs(sum(c.attribution["posterior"]["self"].values()) - c.attribution["posterior"]["self_total"]) < 1e-3


# ── 場景 17：熔斷。上游 order 100% 快速失敗、沒有失敗的子 span、延遲沒漲；下游 inventory 流量幾乎歸零 ──
BREAKER_TOPO = [("order", "inventory")]  # (caller, callee)
BREAKER_FACTS = [
    F("entry_failure_rate", "order", 1.0, "ratio", "incident", before=0.0),
    F("failed_without_failed_child", "order", 1.0, "ratio", "incident"),
    F("self_time_delta", "order", -4.0, "ms"),
    F("entry_latency", "order", -30.0, "ms"),
    F("traffic_drop", "inventory", 0.97, "ratio"),
]
BREAKER_EVIDENCE = [E("circuit_breaker_open", "order", 0.9, quote="CircuitBreaker 'inventory' changed state from CLOSED to OPEN")]


def test_breaker_evidence_points_to_the_cut_downstream_not_the_failing_upstream():
    cands, _ = run(BREAKER_TOPO, ["order"], BREAKER_FACTS, BREAKER_EVIDENCE)
    assert_valid(cands)
    assert top(cands) == "inventory"
    order = next(c for c in cands if c.entity == "order") if score(cands, "order") else None
    # 上游被判成「被下游切斷（熔斷）」而不是自己壞
    inv = cands[0]
    assert inv.score > 0.6
    assert order is None or order.attribution["posterior"]["cut_by_callee"]["breaker"] > order.score
    assert any(u["kind"] == "circuit_breaker_open" for u in inv.attribution["evidence_used"])


def test_removing_the_breaker_evidence_changes_the_answer():
    """同一組事實拿掉熔斷證據：模型回到「上游自己回錯」（壞版本最常見），證明那筆證據真的在起作用。"""
    with_cb, _ = run(BREAKER_TOPO, ["order"], BREAKER_FACTS, BREAKER_EVIDENCE)
    without, _ = run(BREAKER_TOPO, ["order"], BREAKER_FACTS)
    assert_valid(without)
    assert top(with_cb) == "inventory" and top(without) == "order"
    assert without[0].attribution["subtype"] == "error"
    assert score(with_cb, "order") < 0.5 * score(without, "order")
    # LLM 明確說「沒有熔斷」（absent）時，上游自身故障的機率比沒提到更高
    refuted, _ = run(BREAKER_TOPO, ["order"], BREAKER_FACTS, [E("circuit_breaker_open", "order", 0.9, state="absent")])
    assert score(refuted, "order") > score(without, "order")


def test_low_confidence_breaker_evidence_moves_the_needle_less():
    strong, _ = run(BREAKER_TOPO, ["order"], BREAKER_FACTS, BREAKER_EVIDENCE)
    weak, _ = run(BREAKER_TOPO, ["order"], BREAKER_FACTS, [E("circuit_breaker_open", "order", 0.6)])
    none, _ = run(BREAKER_TOPO, ["order"], BREAKER_FACTS)
    assert score(none, "inventory") < score(weak, "inventory") < score(strong, "inventory")


# ── 場景 16：上游 gw 2 秒逾時放棄、下游 order 的 DB 等待跑 15 秒（子 span 比父 span 晚結束 13 秒）──
TIMEOUT_TOPO = [("gw", "order")]
TIMEOUT_FACTS = [
    F("entry_failure_rate", "gw", 0.4, "ratio", "incident", before=0.0),
    F("entry_latency", "gw", 1900, "ms", before=100.0),
    F("wait_delta", "gw", 1900, "ms", resource="order"),
    F("self_time_delta", "gw", 2, "ms"),
    F("failed_without_failed_child", "gw", 0.9, "ratio", "incident"),
    F("entry_latency", "order", 14800, "ms", before=60.0),
    F("wait_delta", "order", 14700, "ms", resource="orders-db"),
    F("self_time_delta", "order", 5, "ms"),
    F("child_outlives_parent", "order", 13000, "ms", parent="gw"),
]
TIMEOUT_EVIDENCE = [E("client_timeout", "gw", 0.9, quote="Read timed out after 2000 ms")]


def test_timeout_blames_the_downstream_that_kept_running_not_the_upstream_that_gave_up():
    cands, _ = run(TIMEOUT_TOPO, ["gw", "order"], TIMEOUT_FACTS, TIMEOUT_EVIDENCE)
    assert_valid(cands)
    assert top(cands) == "order"
    assert cands[0].attribution["subtype"] == "dependency" and cands[0].dependency == "orders-db"
    assert cands[0].score > 0.8
    assert score(cands, "gw") < 0.05


def test_child_outlives_parent_is_what_separates_timeout_from_upstream_failing_on_its_own():
    """拿掉 child_outlives_parent 與 client_timeout：order 仍是第一（它自己的 15 秒等待就夠了），
    但上游被判成「逾時放棄」的機率要明顯下降——這兩筆證據是在講呼叫端行為。"""
    full, ctx_full = run(TIMEOUT_TOPO, ["gw", "order"], TIMEOUT_FACTS, TIMEOUT_EVIDENCE)
    bare, ctx_bare = run(TIMEOUT_TOPO, ["gw", "order"], TIMEOUT_FACTS[:-1])
    assert top(full) == top(bare) == "order"
    full_timeout = _role(ctx_full, "gw")["timeout"]
    bare_timeout = _role(ctx_bare, "gw")["timeout"]
    assert full_timeout > 0.8 and full_timeout > bare_timeout + 0.2


def _role(ctx, entity):
    """從候選的歸因拿不到非候選實體的角色，直接重算一次後驗。"""
    from aiops.stages.rca.base import graph_edges
    from aiops.stages.rca.bayes.model import infer
    from aiops.stages.rca.bayes.observe import collect

    edges = graph_edges(ctx)
    g = Graph.build(sorted({n for e in edges for n in e}), edges)
    obs = collect(ctx.rca_facts, ctx.rca_evidence, g.callees, g.callers)
    weights = {e: 1.0 for e in sorted({o.entity for o in obs} | set(g.nodes))}
    return infer(obs, g, weights).roles[entity]


# ── 場景 21：order ↔ inventory 循環呼叫，gw 呼叫 order ──
CYCLE_TOPO = [("gw", "order"), ("order", "inventory"), ("inventory", "order")]


def test_cycle_does_not_crash_and_points_to_the_member_with_its_own_evidence():
    facts = [
        F("entry_latency", "gw", 800, "ms", before=50.0), F("wait_delta", "gw", 780, "ms", resource="order"),
        F("self_time_delta", "gw", 1, "ms"),
        F("entry_latency", "order", 760, "ms", before=40.0), F("wait_delta", "order", 750, "ms", resource="inventory"),
        F("self_time_delta", "order", 3, "ms"),
        F("entry_latency", "inventory", 700, "ms", before=30.0),
        F("entry_failure_rate", "inventory", 0.3, "ratio", "incident", before=0.0),
        F("failed_without_failed_child", "inventory", 0.8, "ratio", "incident"),
    ]
    evidence = [E("connection_pool_exhausted", "inventory", 0.9, quote="HikariPool-1 - Connection is not available")]
    cands, _ = run(CYCLE_TOPO, ["gw", "order", "inventory"], facts, evidence)
    assert_valid(cands)
    assert top(cands) == "inventory" and cands[0].attribution["subtype"] == "conn_pool"
    assert cands[0].attribution["cycle"] == ["inventory", "order"]
    assert score(cands, "inventory") > 0.7


def test_symmetric_cycle_splits_evenly_instead_of_flipping_blame():
    """環上兩邊症狀一模一樣：機率必須一樣（不能因為走訪順序把鍋甩給其中一邊）。"""
    facts = [F(k, e, v, "ms", before=10.0) if k == "entry_latency" else F(k, e, v, "ms")
             for e in ("a", "b") for k, v in (("entry_latency", 500), ("self_time_delta", 300))]
    cands, _ = run([("a", "b"), ("b", "a")], ["a", "b"], facts)
    assert_valid(cands)
    assert abs(score(cands, "a") - score(cands, "b")) < 1e-9
    assert [c.entity for c in cands][:2] == ["a", "b"]  # 同分依名稱


def test_tarjan_is_iterative_and_handles_long_chains_and_big_cycles():
    n = 5000  # 遞迴版本在預設遞迴上限（1000）就會炸
    chain = {(f"s{i}", f"s{i + 1}") for i in range(n)}
    comp = strongly_connected([f"s{i}" for i in range(n + 1)], chain)
    assert len(set(comp.values())) == n + 1
    ring = chain | {(f"s{n}", "s0")}
    comp = strongly_connected([f"s{i}" for i in range(n + 1)], ring)
    assert len(set(comp.values())) == 1
    g = Graph.build([f"s{i}" for i in range(n + 1)], ring)
    assert g.hops("s0", "s4000") == 0 and g.cycles() == [sorted(f"s{i}" for i in range(n + 1))]


def test_scc_numbering_is_independent_of_input_order():
    nodes = ["c", "a", "d", "b"]
    edges = {("a", "b"), ("b", "a"), ("c", "d")}
    assert strongly_connected(nodes, edges) == strongly_connected(sorted(nodes), edges)
    assert strongly_connected(nodes, edges)["a"] == strongly_connected(nodes, edges)["b"]


# ── 場景 22 vs CPU 飽和：同樣是「自身耗時上升＋呼叫端跟著慢」，差在有沒有 GC 證據 ──
SLOW_TOPO = [("gw", "pay")]
SLOW_FACTS = [
    F("entry_latency", "gw", 900, "ms", before=60.0), F("wait_delta", "gw", 880, "ms", resource="pay"),
    F("self_time_delta", "gw", 2, "ms"),
    F("entry_latency", "pay", 870, "ms", before=40.0), F("self_time_delta", "pay", 850, "ms", before=30.0),
]


def test_gc_and_cpu_get_different_subtypes():
    gc, _ = run(SLOW_TOPO, ["gw", "pay"], SLOW_FACTS, [E("gc_pause", "pay", 0.9, quote="Pause Full (G1 Evacuation Pause) 1843ms")])
    cpu, _ = run(SLOW_TOPO, ["gw", "pay"], SLOW_FACTS)
    assert top(gc) == top(cpu) == "pay"
    assert gc[0].attribution["subtype"] == "gc" and cpu[0].attribution["subtype"] == "cpu"
    # GC 證據之下 gc 是子型第一、而且明顯壓過 cpu（實際 0.46 vs 0.24：cpu 先驗較高、自身耗時兩者都解釋得了）
    sub = gc[0].attribution["posterior"]["self"]
    assert sub["gc"] > 1.5 * sub["cpu"]
    # LLM 明確否認 GC：CPU 的占比比「沒提到」更高
    denied, _ = run(SLOW_TOPO, ["gw", "pay"], SLOW_FACTS, [E("gc_pause", "pay", 0.9, state="absent")])
    assert denied[0].attribution["posterior"]["self"]["cpu"] > cpu[0].attribution["posterior"]["self"]["cpu"]


# ── 場景 18：鎖車隊。三個服務都在等同一個 DB 上的鎖 ──
LOCK_TOPO = [("a", "orders-db"), ("b", "orders-db"), ("c", "orders-db")]
LOCK_FACTS = [f for s in ("a", "b", "c") for f in (
    F("entry_latency", s, 1200, "ms", before=40.0), F("wait_delta", s, 1180, "ms", resource="orders-db"),
    F("self_time_delta", s, 3, "ms"))]
LOCK_EVIDENCE = [E("lock_wait", s, 0.8, quote="Lock wait timeout exceeded") for s in ("a", "b", "c")]


def test_lock_convoy_blames_the_shared_lock_holder_not_one_of_the_waiters():
    cands, _ = run(LOCK_TOPO, ["a", "b", "c"], LOCK_FACTS, LOCK_EVIDENCE + [E("deadlock", "orders-db", 0.8)])
    assert_valid(cands)
    assert top(cands) == "orders-db" and cands[0].attribution["subtype"] == "lock"
    assert score(cands, "orders-db") > 0.8


def test_lock_convoy_without_db_side_evidence_still_prefers_the_shared_resource():
    cands, _ = run(LOCK_TOPO, ["a", "b", "c"], LOCK_FACTS, LOCK_EVIDENCE)
    assert top(cands) == "orders-db"
    assert all(score(cands, s) < score(cands, "orders-db") for s in ("a", "b", "c"))


# ── 證據不足 ──

def test_no_facts_no_evidence_returns_empty_with_note():
    cands, ctx = run(BREAKER_TOPO, ["order"], [])
    assert cands == [] and "證據不足" in ctx.rca_root_note


def test_nothing_significant_returns_empty_with_note():
    cands, ctx = run(BREAKER_TOPO, ["order"], [F("self_time_delta", "order", 3, "ms"), F("entry_latency", "order", 5, "ms")])
    assert cands == [] and "顯著門檻" in ctx.rca_root_note


def test_one_vague_symptom_is_not_enough_to_name_a_root():
    """只有一個服務的日誌樣板暴增、其他什麼都沒有：不指認（不准為了有答案硬湊）。"""
    cands, ctx = run([("gw", "pay")], ["pay"], [F("log_template_surge", "pay", 5.0, "x")])
    assert cands == [] and "主要來自先驗" in ctx.rca_root_note


def test_undetermined_status_returns_empty():
    cands, ctx = run(BREAKER_TOPO, ["order"], BREAKER_FACTS, BREAKER_EVIDENCE, status="undetermined")
    assert cands == [] and "無法判定" in ctx.rca_root_note


# ── 確定性 ──

def test_same_input_same_output_regardless_of_order():
    topo, facts, ev = LOCK_TOPO, LOCK_FACTS, LOCK_EVIDENCE + [E("deadlock", "orders-db", 0.8)]
    a, _ = run(topo, ["a", "b", "c"], facts, ev)
    b, _ = run(topo, ["a", "b", "c"], facts, ev)
    assert a == b and a
    rng = random.Random(7)
    for _ in range(5):
        shuffled, _ = run(rng.sample(topo, len(topo)), rng.sample(["a", "b", "c"], 3), rng.sample(facts, len(facts)),
                          rng.sample(ev, len(ev)))
        assert shuffled == a


@pytest.mark.xfail(strict=True, reason="死區：單一根因假設。兩個互不相干的故障同時發生（逾時＋鎖車隊），"
                                       "沒有任何單一假設能同時解釋兩邊，UNKNOWN 勝出 → 回空清單，兩個真根因都沒被指認")
def test_two_independent_faults_at_once_are_both_named():
    cands, _ = run(TIMEOUT_TOPO + LOCK_TOPO, ["gw", "order", "a", "b", "c"], TIMEOUT_FACTS + LOCK_FACTS,
                   TIMEOUT_EVIDENCE + LOCK_EVIDENCE + [E("deadlock", "orders-db", 0.8)])
    assert {"order", "orders-db"} <= {c.entity for c in cands}


def test_duplicate_facts_are_not_counted_twice():
    once, _ = run(SLOW_TOPO, ["gw", "pay"], SLOW_FACTS, [E("gc_pause", "pay", 0.9)])
    twice, _ = run(SLOW_TOPO, ["gw", "pay"], SLOW_FACTS + SLOW_FACTS, [E("gc_pause", "pay", 0.9)] * 3)
    assert once == twice


# ── 載入期參數檢查 ──

@pytest.mark.parametrize("params", [{"min_posterior": 1.5}, {"min_posterior": -0.1}, {"top_k": 0},
                                    {"top_k": 1.5}, {"report_floor": 0.5, "min_posterior": 0.3}, {"min_posterior": "x"}])
def test_bad_params_fail_at_load_time(params):
    with pytest.raises(WorkflowError):
        OperatorRegistry.create("rca.root_cause.bayes", params)
