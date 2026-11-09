"""Span 區間聯集（docs/HARDENING_DESIGN.md 第 2 節）：自身 = 耗時 − app 子 span 加總 − 其他子 span 區間（夾進父區間）的聯集。

以前是「耗時 − 子 span 耗時加總，下限 0」：asyncio.gather 並行打兩個下游時重疊的部分被扣兩次，自身耗時被扣成 0。
"""
from datetime import datetime, timedelta, timezone

import pytest

from aiops.engine.context import PipelineContext, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.rca.spans import call_chain, self_times, self_times_detail, window_stats

T = datetime(2026, 11, 3, 15, 0, tzinfo=timezone.utc)


def _at(ms: float) -> datetime:
    return T + timedelta(milliseconds=ms)


def _parent(ms=100.0, start=0.0, sid="p", svc="ord-svc"):
    return SpanRecord("t", sid, None, svc, _at(start), ms)


def _child(sid, start, ms, kind="db", parent="p", svc="ord-svc", resource=""):
    return SpanRecord("t", sid, parent, svc, _at(start), ms, kind=kind, subtype="", resource=resource or sid)


def test_parallel_children_are_subtracted_once():
    spans = [_parent(100), _child("inventory-db", 0, 60), _child("payment-gw", 0, 60, kind="external")]
    selfs, skewed = self_times_detail(spans)
    assert selfs["p"] == pytest.approx(40.0)  # 改版前：100 − 120 → 夾成 0
    assert selfs["inventory-db"] == selfs["payment-gw"] == pytest.approx(60.0) and not skewed


def test_partially_overlapping_children_count_the_overlap_once():
    spans = [_parent(100), _child("a", 10, 40), _child("b", 30, 40), _child("c", 90, 5)]
    assert self_times(spans)["p"] == pytest.approx(100 - (60 + 5))  # [10,70] ∪ [90,95]


def test_sequential_children_are_the_same_as_the_old_sum():
    spans = [_parent(100), _child("a", 0, 40), _child("b", 40, 50)]
    selfs, skewed = self_times_detail(spans)
    assert selfs["p"] == pytest.approx(10.0) and not skewed  # 跟以前一樣：100 − 40 − 50


def test_sub_millisecond_overlap_from_timestamp_truncation_is_treated_as_sequential():
    """@timestamp 是毫秒精度：依序呼叫的第二個子 span 起點被截掉 0.6 ms，看起來跟第一個重疊——不算並行。"""
    spans = [_parent(100), _child("a", 0, 40.6), _child("b", 40, 50)]
    assert self_times(spans)["p"] == pytest.approx(100 - 40.6 - 50)


def test_sequential_children_longer_than_the_parent_are_clamped_and_counted_as_clock_skew():
    """兩個子依序各 60、父只有 100：物理上不可能（時鐘偏移）。第二個子超出父區間的 20 ms 不扣，自身 0，記一筆偏移。"""
    spans = [_parent(100), _child("a", 0, 60), _child("b", 60, 60)]
    selfs, skewed = self_times_detail(spans)
    assert selfs["p"] == 0.0 and skewed == {"p"}


def test_child_running_past_the_parent_only_subtracts_the_part_inside():
    spans = [_parent(100), _child("late", 80, 50)]
    selfs, skewed = self_times_detail(spans)
    assert selfs["p"] == pytest.approx(80.0)  # 只扣 [80, 100]；改版前扣 50 -> 50
    assert skewed == {"p"}


def test_child_starting_before_the_parent_is_clamped_to_zero_and_counted():
    spans = [_parent(100), _child("early", -30, 200)]
    selfs, skewed = self_times_detail(spans)
    assert selfs["p"] == 0.0 and skewed == {"p"}
    st = window_stats(spans, T - timedelta(seconds=1), T + timedelta(minutes=1))["ord-svc"]
    assert st.clock_skew_spans == 1


def test_child_within_one_millisecond_of_the_parent_edge_is_not_clock_skew():
    spans = [_parent(100), _child("a", -0.5, 100.8)]
    selfs, skewed = self_times_detail(spans)
    assert selfs["p"] == 0.0 and not skewed


def test_app_spans_are_summed_not_merged():
    """span.type = app 是服務自己的程式（請求自己的執行緒），跟 I/O 先後發生：加總，不做聯集。
    真實 11/3 資料：app span [0, 9.2]、外部呼叫 [4, 142.5]，照字面聯集會把重疊的 5 ms 算成「自身」。"""
    spans = [_parent(151.2), _child("app", 0, 9.2, kind="app"), _child("tax-gw", 4, 138.5, kind="external")]
    assert self_times(spans)["p"] == pytest.approx(3.5)


def test_window_stats_clock_skew_defaults_to_zero_and_counts_per_service():
    spans = [_parent(100), _child("a", 0, 60), _child("b", 0, 60),
             _parent(50, start=200, sid="q", svc="cart-api"), _child("x", 190, 80, parent="q", svc="cart-api")]
    st = window_stats(spans, T - timedelta(seconds=1), T + timedelta(minutes=1))
    assert st["ord-svc"].clock_skew_spans == 0 and st["ord-svc"].self_ms_per_request == pytest.approx(40 + 120)
    assert st["ord-svc"].app_ms_per_request == pytest.approx(40.0)
    assert st["cart-api"].clock_skew_spans == 1


def test_call_chain_prints_the_union_self_time():
    spans = [_parent(100), _child("inventory-db", 0, 60), _child("payment-gw", 0, 60, kind="external")]
    chain = call_chain(spans, "t")
    assert [(d, s.span_id, round(own, 1)) for d, s, own in chain] == [(0, "p", 40.0), (1, "inventory-db", 60.0), (1, "payment-gw", 60.0)]


def _section(spans):
    ctx = PipelineContext(pipeline="rca", now=T + timedelta(hours=1))
    ctx.incidents, ctx.incidents_built, ctx.rca_spans = [Incident([AnomalyEvent("ord-svc", "latency", T, 6.0)])], True, spans
    return OperatorRegistry.create("rca.section.trace_evidence", {}).execute(ctx).rca_sections[0].body


def test_trace_evidence_prints_clock_skew_only_when_there_is_some():
    clean = [_parent(100), _child("inventory-db", 0, 60), _child("payment-gw", 0, 60, kind="external")]
    assert "時鐘偏移" not in _section(clean)
    skewed = clean + [_parent(100, start=1000, sid="p2"), _child("early", 970, 200, parent="p2")]
    assert "時鐘偏移：事件期間有子 span 超出父 span 的時間範圍，超出的部分沒算進扣除——ord-svc 1 筆" in _section(skewed)
