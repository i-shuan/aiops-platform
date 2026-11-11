"""rca.facts（認知型 RCA 證據層）：每種事實一個正向、一個資料不足；確定性；呼叫圖有環不崩；沒資料回空清單。

span 用手捏的最小呼叫樹，數字都能心算：事件期間 = [12:00, 12:10)、基準期 = [11:45, 12:00)。
"""
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from aiops.engine.context import LogRecord, MqEdge, PipelineContext, SpanRecord
from aiops.engine.errors import WorkflowError
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident

UTC = timezone.utc
T = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)   # 事件開始
BASE = T - timedelta(minutes=15)               # 主撈取窗開頭
MIN = 20


class Spans:
    """最小 span 產生器：每次 req() 是一筆入口請求，kids 是 (服務或資源, 起點偏移 ms, 耗時 ms, 選項) 的同步子 span。"""

    def __init__(self):
        self.spans: list[SpanRecord] = []
        self.n = 0

    def _id(self) -> str:
        self.n += 1
        return f"s{self.n:05d}"

    def add(self, svc, ts, dur, parent=None, trace=None, **kw) -> SpanRecord:
        s = SpanRecord(trace or f"t{self.n:05d}", self._id(), parent, svc, ts, float(dur), **kw)
        self.spans.append(s)
        return s

    def req(self, svc, ts, dur, error=False, kids=(), parent=None, trace=None):
        root = self.add(svc, ts, dur, parent=parent, trace=trace, error=error)
        for kid in kids:
            name, off, kdur, opts = (*kid, {})[:4]
            opts = dict(opts)
            if opts.pop("resource", False):  # 資源葉節點：服務名是呼叫端自己
                self.add(svc, ts + timedelta(milliseconds=off), kdur, parent=root.span_id, trace=root.trace_id,
                         kind="db", subtype="oracle", resource=name, **opts)
            else:
                self.add(name, ts + timedelta(milliseconds=off), kdur, parent=root.span_id, trace=root.trace_id, **opts)
        return root

    def many(self, svc, lo, count, dur, step_s=20, **kw):
        for i in range(count):
            self.req(svc, lo + timedelta(seconds=step_s * i), dur, **kw)


def _incident(*entities) -> Incident:
    return Incident([AnomalyEvent(e, "latency", T + timedelta(minutes=i), 5.0) for e in entities for i in range(10)])


def _run(spans=(), logs=(), logs_base=(), minutes=(10.0, 15.0), mq=(), series=None, entities=("a",), **params):
    ctx = PipelineContext(pipeline="rca", now=T + timedelta(minutes=10))
    ctx.incidents = [_incident(*entities)]
    ctx.rca_spans = list(spans)
    ctx.rca_main_start = BASE if spans else None
    ctx.rca_logs, ctx.rca_logs_baseline, ctx.rca_log_window_minutes = list(logs), list(logs_base), minutes
    ctx.rca_mq = list(mq)
    ctx.rca_series = series or {}
    OperatorRegistry.create("rca.facts", params).execute(ctx)
    return ctx


def _of(ctx, kind, entity=None, **detail):
    return [f for f in ctx.rca_facts if f.kind == kind and (entity is None or f.entity == entity)
            and all(f.detail.get(k) == v for k, v in detail.items())]


def _skips(ctx) -> list[str]:
    return [a for a in ctx.audit if a.startswith("rca.facts:skip:")]


# ───────────── 入口耗時、入口失敗率、自身耗時增量 ─────────────
def _slow_a(base_n=30, inc_n=30) -> Spans:
    b = Spans()
    b.many("a", BASE, base_n, 10)
    b.many("a", T, inc_n, 50, error=False)
    return b


def test_entry_latency_and_self_time_delta():
    ctx = _run(_slow_a().spans)
    lat, = _of(ctx, "entry_latency", "a")
    assert lat.window == "incident" and lat.unit == "ms" and lat.value == pytest.approx(50)
    assert lat.detail == {"before": pytest.approx(10), "during": pytest.approx(50), "n": 30, "n_before": 30}
    st, = _of(ctx, "self_time_delta", "a")
    assert st.window == "delta" and st.value == pytest.approx(40) and st.detail["n"] == 30 and st.detail["n_before"] == 30
    assert st.source == "stages.rca.facts.spans"


def test_entry_facts_skipped_when_incident_has_too_few_requests():
    ctx = _run(_slow_a(inc_n=5).spans)
    assert not _of(ctx, "entry_latency", "a") and not _of(ctx, "entry_failure_rate", "a") and not _of(ctx, "self_time_delta", "a")
    assert any("entry:a:事件期間入口 5 筆" in s for s in _skips(ctx))


def test_self_time_delta_skipped_without_enough_baseline_but_incident_facts_stay():
    ctx = _run(_slow_a(base_n=5).spans)
    assert not _of(ctx, "self_time_delta", "a")
    lat, = _of(ctx, "entry_latency", "a")
    assert lat.detail["before"] is None and lat.detail["n_before"] == 5  # 有就照實說幾筆，比例不算
    assert any(s.startswith("rca.facts:skip:delta:a:") for s in _skips(ctx))


def test_entry_failure_rate():
    b = Spans()
    b.many("a", BASE, 30, 10)
    for i in range(40):
        b.req("a", T + timedelta(seconds=10 * i), 10, error=i % 4 == 0)
    f, = _of(_run(b.spans), "entry_failure_rate", "a")
    assert f.value == pytest.approx(0.25) and f.detail["failures"] == 10 and f.detail["before"] == 0.0 and f.detail["n"] == 40


# ───────────── 等待增量 ─────────────
def _waits(base_n=30) -> Spans:
    b = Spans()
    for i in range(base_n):
        b.req("a", BASE + timedelta(seconds=20 * i), 20, kids=[("oracle", 5, 5, {"resource": True})])
    for i in range(30):
        b.req("a", T + timedelta(seconds=20 * i), 120, kids=[("oracle", 5, 100, {"resource": True, "error": i < 3})])
    return b


def test_wait_delta_on_a_resource():
    w, = _of(_run(_waits().spans), "wait_delta", "a", target="oracle")
    assert w.value == pytest.approx(95) and w.detail["before"] == pytest.approx(5) and w.detail["during"] == pytest.approx(100)
    assert w.detail["target_kind"] == "db/oracle" and w.detail["failures_before"] == 0 and w.detail["failures_during"] == 3
    st, = _of(_run(_waits().spans), "self_time_delta", "a")
    assert st.detail["app_before"] == pytest.approx(15) and st.detail["app_during"] == pytest.approx(20)


def test_wait_delta_skipped_without_baseline():
    assert not _of(_run(_waits(base_n=3).spans), "wait_delta")


# ───────────── 失敗而子 span 沒失敗 ─────────────
def test_failed_without_failed_child():
    b = Spans()
    b.many("a", BASE, 30, 10, kids=[("b", 1, 5)])
    for i in range(30):  # a 全失敗；前 10 筆底下的 b 也失敗、後 20 筆 b 成功
        b.req("a", T + timedelta(seconds=20 * i), 10, error=True, kids=[("b", 1, 5, {"error": i < 10})])
    ctx = _run(b.spans, entities=("a", "b"))
    f, = _of(ctx, "failed_without_failed_child", "a")
    assert f.value == pytest.approx(20 / 30) and f.detail["count"] == 20 and f.detail["failures"] == 30
    assert f.detail["share_of_failures"] == pytest.approx(20 / 30)
    fb, = _of(ctx, "failed_without_failed_child", "b")  # b 自己就是葉節點：失敗的 10 筆底下都沒有子 span
    assert fb.value == pytest.approx(10 / 30)
    assert all("own" not in k and "skew" not in k for x in ctx.rca_facts for k in x.detail)  # 不帶「自己的錯」「時鐘偏移」這類解讀


def test_failed_without_failed_child_skipped_when_few_requests():
    b = Spans()
    b.many("a", BASE, 30, 10)
    b.many("a", T, 3, 10, error=True)
    assert not _of(_run(b.spans), "failed_without_failed_child")


# ───────────── 流量：入口流量、呼叫邊流量、扇出 ─────────────
def _fanout(before_calls=1, during_calls=3, base_n=30, inc_n=30) -> Spans:
    b = Spans()
    for lo, n, k in ((BASE, base_n, before_calls), (T, inc_n, during_calls)):
        for i in range(n):
            b.req("a", lo + timedelta(seconds=20 * i), 50, kids=[("b", 2 + 10 * j, 5) for j in range(k)])
    return b


def test_fanout_ratio_and_traffic_on_the_edge():
    ctx = _run(_fanout().spans, entities=("a", "b"))
    fo, = _of(ctx, "fanout_ratio", "a", callee="b")
    assert fo.value == pytest.approx(3) and fo.detail["before"] == pytest.approx(1) and fo.detail["during"] == pytest.approx(3)
    assert fo.detail["calls_before"] == 30 and fo.detail["calls_during"] == 90
    td, = _of(ctx, "traffic_drop", "a", callee="b")  # 每分鐘：基準 30/15 = 2，事件 90/10 = 9
    assert td.value == pytest.approx(4.5) and td.detail["before_per_min"] == pytest.approx(2) and td.detail["during_per_min"] == pytest.approx(9)
    ir, = _of(ctx, "inbound_rate", "b")
    assert ir.value == pytest.approx(4.5) and ir.detail["by_caller"] == {"a": {"before": 30, "during": 90}}
    ia, = _of(ctx, "inbound_rate", "a")
    assert ia.detail["by_caller"] == {"<external>": {"before": 30, "during": 30}}


def test_traffic_drop_to_zero_is_a_fact_circuit_breaker_shape():
    """呼叫端不再打下游（熔斷、逾時切斷的樣子）：流量掉到 0 照實輸出 0，不判斷原因。"""
    ctx = _run(_fanout(before_calls=1, during_calls=0).spans, entities=("a",))
    td, = _of(ctx, "traffic_drop", "a", callee="b")
    assert td.value == 0.0 and td.detail["calls_during"] == 0
    fo, = _of(ctx, "fanout_ratio", "a", callee="b")
    assert fo.value == 0.0


def test_new_call_path_has_no_traffic_or_fanout_fact():
    """事件前這條邊不到 min_requests 次（新呼叫路徑）：算不出倍數，不產出。"""
    ctx = _run(_fanout(before_calls=0, during_calls=2).spans, entities=("a", "b"))
    assert not _of(ctx, "fanout_ratio") and not _of(ctx, "traffic_drop") and not _of(ctx, "inbound_rate", "b")
    assert any("edge:a->b:基準期呼叫 0 次" in s for s in _skips(ctx))


def test_inbound_rate_skipped_without_baseline():
    b = Spans()
    b.many("a", T, 30, 10)  # span 從事件開始才有
    ctx = _run(b.spans)
    assert not _of(ctx, "inbound_rate") and not _of(ctx, "self_time_delta")
    assert _of(ctx, "entry_latency", "a")


# ───────────── 子 span 超出父 span ─────────────
def _outlives(late_ms=13000.0) -> Spans:
    b = Spans()
    b.many("a", BASE, 30, 10)
    for i in range(30):
        b.req("a", T + timedelta(seconds=20 * i), 100, kids=[("b", 10, 90 + (late_ms if i < 4 else 0))])
    return b


def test_child_outlives_parent_reports_milliseconds_only():
    ctx = _run(_outlives().spans, entities=("a", "b"))
    f, = _of(ctx, "child_outlives_parent", "a")
    assert f.window == "incident" and f.unit == "ms" and f.value == pytest.approx(13000)
    assert f.detail["parent"] == "a" and f.detail["child"] == "b" and f.detail["count"] == 4 and f.detail["n"] == 30
    assert set(f.detail["sample"]) == {"trace_id", "parent_span", "child_span"}
    assert not _of(ctx, "child_precedes_parent")


def test_child_inside_parent_or_within_truncation_tolerance_is_no_fact():
    assert not _of(_run(_outlives(late_ms=0.0).spans, entities=("a",)), "child_outlives_parent")
    assert not _of(_run(_outlives(late_ms=0.5).spans, entities=("a",)), "child_outlives_parent")  # 毫秒截斷不算


def test_child_precedes_parent():
    b = Spans()
    b.many("a", BASE, 30, 10)
    for i in range(30):
        b.req("a", T + timedelta(seconds=20 * i), 100, kids=[("b", -250 if i == 0 else 10, 50)])
    f, = _of(_run(b.spans), "child_precedes_parent", "a")
    assert f.value == pytest.approx(250) and f.detail["count"] == 1


def test_async_consumer_is_not_a_child_that_outlives_its_producer():
    """訊息消費者 20 分鐘後才處理，是佇列延遲，不是子 span 超出父 span（跟舊引擎同一個切法：consumer 不算同步子 span）。"""
    b = Spans()
    b.many("a", BASE, 30, 10)
    for i in range(30):
        root = b.req("a", T + timedelta(seconds=20 * i), 10)
        b.add("c", root.ts + timedelta(minutes=20), 5, parent=root.span_id, trace=root.trace_id, consumer=True)
    assert not _of(_run(b.spans), "child_outlives_parent")


# ───────────── 日誌樣板 ─────────────
def _log(svc, minute, msg, n=1):
    return [LogRecord(T + timedelta(minutes=minute, seconds=i), svc, msg, "SQLException", f"tr{minute}{i}") for i in range(n)]


def test_log_template_surge():
    during = _log("a", 1, "lock timeout on row 123", 30) + _log("a", 2, "lock timeout on row 456", 10)
    base = _log("a", -10, "lock timeout on row 9", 3)
    ctx = _run(logs=during, logs_base=base, minutes=(10.0, 15.0))
    f, = _of(ctx, "log_template_surge", "a")
    # 事件 40 筆／10 分 = 4／分；基準 3 筆／15 分 = 0.2／分 -> 20 倍
    assert f.value == pytest.approx(20) and f.detail["template"] == "lock timeout on row <NUM>"
    assert f.detail["n"] == 40 and f.detail["new"] is False and f.detail["sampled"] is False


def test_log_template_surge_skipped_when_too_few_or_window_unknown():
    assert not _of(_run(logs=_log("a", 1, "boom", 2)), "log_template_surge")  # < min_log_count
    ctx = _run(logs=_log("a", 1, "boom", 30), minutes=(10.0, 0.0))
    assert not ctx.rca_facts and any("日誌窗口長度不明" in s for s in _skips(ctx))


# ───────────── 訊息佇列延遲 ─────────────
def _edge(messages=50) -> MqEdge:
    return MqEdge("order-api", "billing", "orders", messages, 600_000.0, 1_200_000.0, T - timedelta(minutes=20), T,
                  "parent", lag_ms_p95_before=300.0, failed=2)


def test_mq_lag():
    f, = _of(_run(mq=[_edge()]), "mq_lag", "billing")
    assert f.value == 1_200_000.0 and f.unit == "ms" and f.detail["producer"] == "order-api" and f.detail["before"] == 300.0
    assert f.detail["n"] == 50 and "backlog" not in f.detail  # 算不算積壓是判斷，不放進事實


def test_mq_lag_skipped_when_too_few_messages():
    ctx = _run(mq=[_edge(messages=2)], min_messages=5)
    assert not ctx.rca_facts and any(s.startswith("rca.facts:skip:mq_lag:billing:") for s in _skips(ctx))


# ───────────── 訊息生產者在發布時段的數字 ─────────────
def _producer_spans(slow_ms=400.0) -> list[SpanRecord]:
    """order-api：發布時窗（T−20 分 ～ T）每筆等 orders-db slow_ms；它之前等長的一段（T−40 ～ T−20）等 20 ms。"""
    sp = Spans()
    pub = T - timedelta(minutes=20)
    sp.many("order-api", pub - timedelta(minutes=20), 40, 50, kids=[("orders-db", 5, 20, {"resource": True})])
    sp.many("order-api", pub, 40, 30 + slow_ms, kids=[("orders-db", 5, slow_ms, {"resource": True})])
    return sp.spans


def test_producer_window_compares_the_publish_window_with_the_stretch_before_it():
    ctx = _run(spans=_producer_spans(), mq=[_edge()], entities=("billing",))
    own, = _of(ctx, "producer_self_time_delta", "order-api")
    wait, = _of(ctx, "producer_wait_delta", "order-api", target="orders-db")
    assert own.window == wait.window == "publish" and own.detail["consumer"] == "billing" and own.detail["queue"] == "orders"
    assert round(own.value) == 380 and round(wait.value) == 380                 # 發布時段每筆多等 380 ms
    assert (round(wait.detail["before"]), round(wait.detail["during"])) == (20, 400)
    assert (own.detail["n_before"], own.detail["n"]) == (40, 40) and wait.detail["target_kind"] == "db/oracle"
    assert "anomalous" not in own.detail and "abnormal" not in own.detail      # 只陳述數字，有沒有異常是判斷


def test_producer_window_skipped_without_enough_producer_requests():
    ctx = _run(spans=_producer_spans()[:10], mq=[_edge()], entities=("billing",))
    assert not _of(ctx, "producer_self_time_delta")
    assert any(s.startswith("rca.facts:skip:producer_window:order-api:") for s in _skips(ctx))


def test_producer_window_needs_a_known_producer_and_is_counted_once():
    from dataclasses import replace
    unknown = replace(_edge(), producer="")                     # 只有 message.age：不知道是誰發的
    assert not _of(_run(spans=_producer_spans(), mq=[unknown], entities=("billing",)), "producer_self_time_delta")
    twice = [_edge(), replace(_edge(), consumer="shipping")]    # 同一個生產者、同一個發布時窗發給兩個消費者
    assert len(_of(_run(spans=_producer_spans(), mq=twice, entities=("billing",)), "producer_self_time_delta")) == 1


# ───────────── 原始時序位移 ─────────────
def _series(n_before=10, n_during=10):
    ts = [T - timedelta(minutes=n_before - i) for i in range(n_before)] + [T + timedelta(minutes=i) for i in range(n_during)]
    return pd.DataFrame({"ts": ts, "value": [10.0] * n_before + [40.0] * n_during})


def test_metric_shift():
    f, = _of(_run(series={("a", "latency_q"): _series()}), "metric_shift", "a")
    assert f.value == pytest.approx(30) and f.detail["metric"] == "latency_q" and f.detail["n"] == 10


def test_metric_shift_skipped_with_too_few_points_or_garbage():
    bad = pd.DataFrame({"ts": [T], "value": [float("nan")]})
    ctx = _run(series={("a", "q"): _series(n_before=2), ("b", "q"): bad, ("c", "q"): pd.DataFrame()})
    assert not _of(ctx, "metric_shift")
    assert len([s for s in _skips(ctx) if "metric_shift" in s]) == 3


# ───────────── 確定性、環、沒有資料、參數 ─────────────
def _everything() -> dict:
    b = _fanout()
    b.spans += _outlives().spans
    b.spans += _waits().spans
    return dict(spans=b.spans, logs=_log("a", 1, "x 1", 10) + _log("b", 1, "y 2", 10), logs_base=_log("a", -5, "x 3", 2),
                mq=[_edge()], series={("a", "q"): _series(), ("b", "q"): _series()}, entities=("a", "b"))


def test_same_input_same_output_even_shuffled():
    first = _run(**_everything()).rca_facts
    again = _run(**_everything()).rca_facts
    data = _everything()
    random.Random(3).shuffle(data["spans"])
    random.Random(4).shuffle(data["logs"])
    shuffled = _run(**data).rca_facts
    assert first == again
    # 逐位元相同：span 先依全欄位排序，浮點加總順序固定（這份資料故意有重複的 span_id，三組產生器各自從 s00001 編號）
    assert shuffled == first
    assert {f.kind for f in first} >= {"entry_latency", "entry_failure_rate", "self_time_delta", "wait_delta", "fanout_ratio",
                                       "traffic_drop", "inbound_rate", "child_outlives_parent", "failed_without_failed_child",
                                       "log_template_surge", "mq_lag", "metric_shift"}


def test_cyclic_calls_do_not_recurse_forever():
    """服務層 A↔B 互相呼叫，再加上 span 層的父子環（x 的父是 y、y 的父是 x，壞掉的資料）：不准無限遞迴、要產出事實。"""
    b = Spans()
    for lo, n in ((BASE, 30), (T, 30)):
        for i in range(n):
            root = b.req("a", lo + timedelta(seconds=20 * i), 50, kids=[("b", 1, 30)])
            kid = b.spans[-1]
            b.add("a", kid.ts + timedelta(milliseconds=1), 10, parent=kid.span_id, trace=root.trace_id)  # b -> a
    b.spans.append(SpanRecord("tz", "x", "y", "a", T, 5.0, error=True))
    b.spans.append(SpanRecord("tz", "y", "x", "b", T, 5.0))
    ctx = _run(b.spans, entities=("a", "b"))
    assert _of(ctx, "fanout_ratio", "a", callee="b") and _of(ctx, "fanout_ratio", "b", callee="a")
    assert _of(ctx, "failed_without_failed_child", "a")


def test_no_traces_no_logs_is_an_empty_list():
    ctx = _run()
    assert ctx.rca_facts == []
    assert "rca.facts:0" in ctx.audit and any("沒有 span" in s for s in _skips(ctx))


@pytest.mark.parametrize("params", [{"min_requests": 0}, {"min_requests": True}, {"min_requests": 2.5},
                                    {"min_log_count": 0}, {"max_templates_per_service": -1}, {"min_messages": 0},
                                    {"min_series_points": "3"}])
def test_bad_params_fail_at_load_time(params):
    with pytest.raises(WorkflowError):
        OperatorRegistry.create("rca.facts", params)


def test_unknown_param_fails_at_load_time():
    with pytest.raises(WorkflowError):
        OperatorRegistry.create("rca.facts", {"min_reqests": 5})


def test_registered_as_batch_operator():
    assert OperatorRegistry.get("rca.facts").scope == "batch"
