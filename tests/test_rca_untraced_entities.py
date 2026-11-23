"""事件群的實體沒有被追蹤（純指標的產線設備）、但 trace 來源裡有別的服務的 span：對這個事件群來說就是沒有 trace，
根因演算法要退回關係圖推測，不是回「入口請求不足、無法判定」。

2026-11-20 在演練叢集實測到的：同一台 Grafana 有 rca-mes-api／rca-wip-worker 的 trace，產線模擬的
mq-broker／cim-eap／amhs-mcs 只有指標；設了 trace 資料源之後，產線事故的 RCA 從「拓撲推測、第一名 mq-broker」
變成「無法判定」。
"""
from datetime import datetime, timedelta, timezone

from aiops.engine.context import PipelineContext, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident

T = datetime(2026, 11, 20, 12, 8, tzinfo=timezone.utc)


def _ctx(spans, traces_failed=False) -> PipelineContext:
    ctx = PipelineContext(pipeline="rca_report", now=T + timedelta(minutes=10))
    ctx.incidents = [Incident([AnomalyEvent("mq-broker", "depth", T, 9.0), AnomalyEvent("cim-eap", "timeouts", T + timedelta(minutes=3), 9.0),
                               AnomalyEvent("amhs-mcs", "aborts", T + timedelta(minutes=6), 9.0)])]
    ctx.topology = [("cim-eap", "mq-broker"), ("amhs-mcs", "cim-eap")]  # CIM 依賴 MQ、AMHS 依賴 CIM
    ctx.rca_spans, ctx.rca_traces_failed = list(spans), traces_failed
    return ctx


def _other_services_spans(n=200):
    """別的服務（有被追蹤）的 span：事件前後都有、數量很夠。"""
    out = []
    for i in range(n):
        ts = T - timedelta(minutes=14) + timedelta(seconds=7 * i)
        out += [SpanRecord(f"t{i}", f"a{i}", None, "rca-mes-api", ts, 90.0, False),
                SpanRecord(f"t{i}", f"b{i}", f"a{i}", "rca-wip-worker", ts, 50.0, False)]
    return out


def _rank(ctx):
    return [c.entity for c in OperatorRegistry.create("rca.root_cause.evidence", {}).rank(ctx.incidents[0], ctx)]


def test_untraced_entities_fall_back_to_the_graph_even_when_other_services_have_spans():
    ctx = _ctx(_other_services_spans())
    assert _rank(ctx)[0] == "mq-broker"
    assert ctx.rca_conclusion_type == "topology_inference"
    assert "事件裡的實體在 trace 裡沒有任何 span" in ctx.rca_root_note and "推測" in ctx.rca_root_note


def test_it_is_the_same_answer_as_having_no_trace_source_at_all():
    with_other, without = _ctx(_other_services_spans()), _ctx([])
    assert _rank(with_other) == _rank(without) and with_other.rca_conclusion_type == without.rca_conclusion_type
    assert "這一輪沒有 trace 證據" in without.rca_root_note


def test_a_failed_trace_source_still_refuses_to_guess():
    """trace 來源這一輪讀取失敗：證據是被吃掉的，不是本來就沒有——仍然不用關係圖硬猜。"""
    ctx = _ctx([], traces_failed=True)
    assert _rank(ctx) == [] and ctx.rca_conclusion_type == "undetermined"


def test_entities_that_are_traced_are_still_judged_from_their_spans():
    """事件裡只要有實體被追蹤，就照原本的證據路徑（這裡入口請求夠、自身耗時沒變，不會退回關係圖）。"""
    ctx = _ctx(_other_services_spans())
    ctx.incidents = [Incident([AnomalyEvent("rca-wip-worker", "latency", T, 9.0), AnomalyEvent("mq-broker", "depth", T, 9.0)])]
    _rank(ctx)
    assert ctx.rca_conclusion_type != "topology_inference"
