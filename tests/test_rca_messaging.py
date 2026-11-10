"""訊息佇列（非同步邊）的 RCA：docs/MQ_QPS_DESIGN.md 第一階段（P0）。

涵蓋：engine/mq.py（發布時間怎麼還原、發布時窗 P5–P95）、stages/rca/spans.py 剪開非同步父子邊的三處算歪、
rca.root_cause.evidence 回到發布時窗看生產者與訊息夾帶失敗轉記、Runbook RB-07、三行摘要、API 回傳形狀。

已知死區（xfail strict）：毒訊息而生產者沒有任何自身症狀——trace 看不到訊息內容，指不到生產者。
撈取、資料源、偵測端合併與拓撲沉澱在 tests/test_messaging_pipeline.py。
"""
import random
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from aiops.engine import mq
from aiops.engine.context import MqEdge, PipelineContext, ReportSection, RootCandidate, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.topology import service_edges
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.enrichment import narrative
from aiops.stages.rca import spans as sp
from aiops.stages.rca.mq import async_attribution, edge_line, is_backlog, lag_text, producer_windows, window_text
from aiops.stages.rca.sections.actions import RUNBOOK_RULES, RunbookFacts, evaluate, signal_of

T = datetime(2026, 11, 3, 14, 0, tzinfo=timezone.utc)
PAD = timedelta(minutes=2)


def at(minutes=0, seconds=0):
    return T + timedelta(minutes=minutes, seconds=seconds)


def produce(i, pub, db_ms=20.0):
    """ord-api 的一筆請求：程式 30 ms、orders-db、最後發一則訊息到 kafka（send span 2 ms，接在 db 之後）。"""
    tid = f"p{i}"
    return [SpanRecord(tid, f"{tid}-e", None, "ord-api", pub, 30.0 + db_ms + 2.0, name="POST /orders"),
            SpanRecord(tid, f"{tid}-db", f"{tid}-e", "ord-api", pub, db_ms, kind="db", subtype="postgresql", resource="orders-db"),
            SpanRecord(tid, f"{tid}-s", f"{tid}-e", "ord-api", pub + timedelta(milliseconds=db_ms), 2.0,
                       kind="messaging", subtype="kafka", resource="kafka/order-events")]


def consume(i, ts, *, src=None, fail=False, how="parent", age_ms=None, work_ms=20.0, db_ms=200.0, db_fail=False,
            queue="order-events"):
    """ship-worker 消費一則訊息：程式 20 ms、ship-db。how = parent（parent 指到 send span）／link（span link）／其他（都沒有）。
    src：這則訊息是哪一筆生產者請求發的（預設同編號）；多個消費者共用同一個 src = 同一個 send span。"""
    src = i if src is None else src
    tid = f"p{src}" if how == "parent" else f"c{i}"
    parent = f"p{src}-s" if how == "parent" else None
    links = ((f"p{src}", f"p{src}-s"),) if how == "link" else ()
    return [SpanRecord(tid, f"c{i}-e", parent, "ship-worker", ts, work_ms + db_ms, error=fail,
                       name="Kafka RECEIVE from order-events", consumer=True, queue=queue, age_ms=age_ms, links=links),
            SpanRecord(tid, f"c{i}-db", f"c{i}-e", "ship-worker", ts, db_ms, error=db_fail,
                       kind="db", subtype="postgresql", resource="ship-db")]


def world(*, slow=True, poison=True, how="parent", delay=20):
    """固定延遲 20 分鐘的佇列：ord-api 每分鐘發 4 則（-60 ～ +20 分），ship-worker 20 分鐘後消費。
    13:40–14:00 發的訊息（事件期間 14:00–14:20 被消費）：slow = ord-api 當時 orders-db 變慢；poison = 其中一半是毒訊息，消費就失敗。"""
    spans, i = [], 0
    for m in range(-60, 20):
        for k in range(4):
            pub = at(m, k * 15)
            bad = -20 <= m < 0
            spans += produce(i, pub, db_ms=400.0 if bad and slow else 20.0)
            spans += consume(i, pub + timedelta(minutes=delay), fail=bad and poison and i % 2 == 0, how=how)
            i += 1
    return spans


def run_rank(spans, entities=("ship-worker",), mq_on=True, **params):
    """事件 14:00–14:19（rank 看到 14:20）、對照段 13:45–14:00；rca_mq 照 ingestion.incident_traces 的算法給。"""
    incident = Incident([AnomalyEvent(e, "errors", at(m), 5.0) for e in entities for m in range(20)])
    ctx = PipelineContext(pipeline="rca", now=at(30))
    ctx.incidents, ctx.incidents_built = [incident], True
    ctx.rca_spans, ctx.rca_main_start = spans, at(-15)
    if mq_on:
        ctx.rca_mq = mq.build_edges(spans, list(entities), at(0), at(20), at(-15), PAD)[0]
    OperatorRegistry.create("rca.root_cause.evidence", params).execute(ctx)
    return ctx.rca_root_candidates, ctx, incident


def scores(ranked):
    return [(c.entity, round(c.score, 2)) for c in ranked]


# ── engine/mq.py：發布時間怎麼還原 ──


def test_percentile_interpolates_and_is_zero_for_no_samples():
    assert mq.percentile([], 0.95) == 0.0 and mq.percentile([7.0], 0.95) == 7.0
    assert mq.percentile([4, 1, 3, 2], 0.5) == 2.5 and mq.percentile([0, 10], 0.05) == 0.5


def test_spread_samples_evenly_instead_of_taking_the_head():
    assert mq.spread(list(range(10)), 5) == [0, 2, 4, 6, 8]
    assert mq.spread([1, 2, 3], 5) == [1, 2, 3] and mq.spread([1, 2, 3], 0) == [1, 2, 3]


def test_publish_time_comes_from_parent_then_link_then_age():
    spans = (produce(1, at(0)) + consume(1, at(5))  # parent 指到 send span
             + produce(2, at(1)) + consume(2, at(6), how="link")  # span link
             + consume(3, at(7), how="none", age_ms=90_000.0)  # 只有 message.age
             + consume(4, at(8), how="none"))  # 什麼都沒有
    consumers = mq.consumer_entries(spans, ["ship-worker"], at(0), at(20))
    assert [c.span_id for c in consumers] == ["c1-e", "c2-e", "c3-e", "c4-e"]
    got, unresolved = mq.messages(spans, consumers)
    assert unresolved == 1
    assert [(m.producer, m.queue, m.linked_by) for m in got] == [
        ("ord-api", "order-events", "parent"), ("ord-api", "order-events", "link"), ("", "order-events", "age")]
    assert got[0].published == at(0) + timedelta(milliseconds=20) and got[0].lag_ms == 5 * 60_000 - 20
    assert got[1].published == at(1) + timedelta(milliseconds=20)
    assert got[2].published == at(7) - timedelta(seconds=90) and got[2].lag_ms == 90_000


def test_parent_wins_over_age_and_queue_falls_back_to_the_send_span_resource():
    # message.age 是消費端自己記的（受它的時鐘影響）；有 parent 就用 send span 的時間
    spans = produce(1, at(0)) + consume(1, at(5), age_ms=1.0, queue="")
    got, _ = mq.messages(spans, mq.consumer_entries(spans, None, at(0), at(20)))
    assert got[0].linked_by == "parent" and got[0].queue == "kafka/order-events" and got[0].lag_ms == 5 * 60_000 - 20


def test_consumer_entries_are_limited_to_incident_entities_and_the_half_open_window():
    spans = consume(1, at(0)) + consume(2, at(20)) + [SpanRecord("x", "x-e", None, "other", at(1), 5.0, consumer=True)]
    assert [c.span_id for c in mq.consumer_entries(spans, ["ship-worker"], at(0), at(20))] == ["c1-e"]
    assert [c.span_id for c in mq.consumer_entries(spans, None, at(0), at(20))] == ["c1-e", "x-e"]


def test_unresolved_trace_ids_are_what_the_second_fetch_has_to_bring_back():
    spans = (consume(1, at(1))  # parent 不在樣本裡 → 要撈它自己那條 trace（生產者跟它同一條）
             + consume(2, at(2), how="link")  # link 指到的 span 不在樣本裡 → 要撈 link 的那條 trace
             + consume(3, at(3), how="link", src=2)  # 同一條 trace 不重複
             + produce(4, at(0)) + consume(4, at(4)))  # 已經對得到：不用撈
    consumers = mq.consumer_entries(spans, None, at(0), at(20))
    assert mq.unresolved_trace_ids(spans, consumers) == ["p1", "p2"]


def test_age_p95_is_none_without_message_age():
    assert mq.age_p95_ms(mq.consumer_entries(consume(1, at(1)), None, at(0), at(20))) is None
    spans = [s for i in range(20) for s in consume(i, at(1, i), how="none", age_ms=1000.0 * (i + 1))]
    assert mq.age_p95_ms(mq.consumer_entries(spans, None, at(0), at(20))) == pytest.approx(19_050.0)


def test_publish_window_is_p5_to_p95_so_one_stuck_message_does_not_stretch_it():
    spans = [s for i in range(40) for s in produce(i, at(-20, i * 15)) + consume(i, at(0, i * 15))]
    spans += produce(99, at(-180)) + consume(99, at(10))  # 卡了 3 小時的一則
    edges, during, unresolved = mq.build_edges(spans, ["ship-worker"], at(0), at(20), at(-15), PAD)
    assert (during, unresolved, len(edges)) == (41, 0, 1)
    edge = edges[0]
    assert (edge.producer, edge.consumer, edge.queue, edge.messages, edge.linked_by) == ("ord-api", "ship-worker", "order-events", 41, "parent")
    # 41 則的發布時間排序後：P5 = 第 3 則（13:40:15）、P95 = 第 39 則（13:49:15），各往外留 2 分鐘；不是從 11:00 開始
    sent = timedelta(milliseconds=20)  # send span 接在 20 ms 的 db 之後
    assert edge.pub_start == at(-22, 15) + sent and edge.pub_end == at(-9, 15) + sent
    assert edge.lag_ms_p50 == pytest.approx(20 * 60_000 - 20) and edge.lag_ms_p95_before is None  # 事件前沒有消費樣本


def test_edge_carries_the_lag_before_the_incident_for_the_same_producer_consumer_queue():
    spans = [s for i in range(30) for s in produce(i, at(-10, i * 10)) + consume(i, at(-10, i * 10 + 1))]  # 事件前：1 秒
    spans += [s for i in range(30, 60) for s in produce(i, at(-3, (i - 30) * 10)) + consume(i, at(5, (i - 30) * 10))]  # 事件中：8 分鐘
    edge = mq.build_edges(spans, ["ship-worker"], at(0), at(20), at(-15), PAD)[0][0]
    assert edge.lag_ms_p95_before == pytest.approx(980.0) and edge.lag_ms_p95 == pytest.approx(8 * 60_000 - 20)
    assert edge.to_dict()["lag_ms"] == {"p50": 479980.0, "p95": 479980.0, "p95_before": 980.0}


def test_edge_failed_counts_only_consumers_that_failed_on_their_own():
    spans = (produce(1, at(-1)) + consume(1, at(1), fail=True)  # 自己失敗
             + produce(2, at(-1)) + consume(2, at(2), fail=True, db_fail=True)  # ship-db 失敗連帶：不是訊息的問題
             + produce(3, at(-1)) + consume(3, at(3)))
    edge = mq.build_edges(spans, ["ship-worker"], at(0), at(20), at(-15), PAD)[0][0]
    assert (edge.messages, edge.failed) == (3, 1)


def test_batch_consumer_with_three_links_is_three_messages_but_one_failure():
    spans = produce(1, at(-3)) + produce(2, at(-2)) + produce(3, at(-1))
    spans.append(SpanRecord("cb", "cb-e", None, "ship-worker", at(5), 220.0, error=True, consumer=True, queue="order-events",
                            links=(("p1", "p1-s"), ("p2", "p2-s"), ("p3", "p3-s"))))
    edges, during, unresolved = mq.build_edges(spans, ["ship-worker"], at(0), at(20), at(-15), PAD)
    assert (during, unresolved) == (1, 0)
    assert (edges[0].messages, edges[0].failed, edges[0].linked_by) == (3, 1, "link")


def test_linked_by_is_the_majority_and_ties_prefer_the_stronger_evidence():
    def edge(parents, links):
        spans = []
        for i in range(parents + links):
            spans += produce(i, at(-1, i)) + consume(i, at(1, i), how="parent" if i < parents else "link")
        return mq.build_edges(spans, ["ship-worker"], at(0), at(20), at(-15), PAD)[0][0]

    assert edge(2, 3).linked_by == "link" and edge(3, 2).linked_by == "parent" and edge(2, 2).linked_by == "parent"


def test_edges_are_split_by_queue_and_sorted_by_message_count():
    spans = []
    for i in range(5):
        spans += produce(i, at(-1, i)) + consume(i, at(1, i), queue="refund-events" if i < 2 else "order-events")
    edges = mq.build_edges(spans, ["ship-worker"], at(0), at(20), at(-15), PAD)[0]
    assert [(e.queue, e.messages) for e in edges] == [("order-events", 3), ("refund-events", 2)]


def test_async_lags_for_topology_work_on_bare_records_and_skip_same_service():
    def rec(span_id, parent_id, service, ts, consumer=False, links=()):
        return SimpleNamespace(span_id=span_id, parent_id=parent_id, service=service, ts=ts, consumer=consumer, links=links)

    records = [rec("a", None, "ord-api", at(0)), rec("a-s", "a", "ord-api", at(0)),
               rec("c1", "a-s", "ship-worker", at(3), consumer=True),
               rec("c2", None, "ship-worker", at(5), consumer=True, links=(("t", "a-s"),)),
               rec("c3", "a-s", "ord-api", at(1), consumer=True),  # 自己發自己收：不是跨服務的邊
               rec("c4", "gone", "ship-worker", at(9), consumer=True),  # 對不到發布端
               rec("d", "a", "pay-svc", at(0))]  # 同步呼叫
    assert mq.async_lags(records) == {("ord-api", "ship-worker"): (pytest.approx(294_000.0), 2)}


def _random_trace(rng, n):
    spans = []
    for i in range(n):
        parent = None if not spans or rng.random() < 0.15 else rng.choice(spans).span_id
        spans.append(SpanRecord("t", f"s{i}", parent, rng.choice(["a", "b", "c"]), at(0, i), 5.0,
                                error=rng.random() < 0.3, consumer=parent is not None and rng.random() < 0.2))
    return spans


@pytest.mark.parametrize("seed", range(20))
def test_own_failed_in_engine_agrees_with_the_subtree_rule_in_stages(seed):
    # engine 不能 import stages，所以「自己造成的失敗」各寫一份；兩邊對同一批 span 必須給同一個答案
    spans = _random_trace(random.Random(seed), 60)
    below = sp._subtree_failed(spans)
    assert mq.own_failed(spans) == {s.span_id for s in spans if s.error and not below[s.span_id]}


def test_own_failed_does_not_count_an_async_consumer_as_a_descendant():
    spans = produce(1, at(0)) + consume(1, at(5), fail=True)
    spans[0].error = True  # 生產者的入口自己失敗；它發的訊息被消費時也失敗
    assert mq.own_failed(spans) == {"p1-e", "c1-e"}


# ── stages/rca/spans.py：剪開非同步父子邊（三處算歪） ──


def test_send_span_stays_a_resource_leaf_with_its_own_wait_time():
    # 算歪 1：消費者掛在 send span 底下 → send 不再是葉節點、kafka 等待時間變 0，資源歸因看不到它
    spans = produce(1, at(0)) + consume(1, at(5))
    assert sp.resource_kinds(spans) == {"orders-db": "db/postgresql", "kafka/order-events": "messaging/kafka", "ship-db": "db/postgresql"}
    stats = sp.window_stats(spans, at(-1), at(1))["ord-api"]
    assert stats.wait_ms_per_request == {"orders-db": 20.0, "kafka/order-events": 2.0}
    assert stats.self_ms_per_request == 52.0 and stats.app_ms_per_request == 30.0


def test_consumer_running_minutes_later_is_not_subtracted_and_is_not_clock_skew():
    # 算歪 2：5 分鐘後才跑的消費者（220 ms）被當成 2 ms send span 的同步子 span → 假的時鐘偏移
    spans = produce(1, at(0)) + consume(1, at(5))
    selfs, skewed = sp.self_times_detail(spans)
    assert selfs["p1-s"] == 2.0 and selfs["p1-e"] == 30.0 and selfs["c1-e"] == 20.0 and skewed == frozenset()
    assert sp.window_stats(spans, at(-1), at(10))["ord-api"].clock_skew_spans == 0


def test_consumer_failure_is_not_the_producers_downstream_failure():
    # 算歪 3：消費者失敗沿 parent 往上算成生產者「子孫有失敗」
    spans = produce(1, at(0)) + consume(1, at(5), fail=True)
    below = sp._subtree_failed(spans)
    assert below["p1-e"] is False and below["p1-s"] is False
    stats = sp.window_stats(spans, at(-1), at(10))
    assert (stats["ord-api"].error_rate, stats["ord-api"].own_error_rate) == (0.0, 0.0)
    assert (stats["ship-worker"].requests, stats["ship-worker"].own_error_rate) == (1, 1.0)


def test_consumer_is_an_entry_even_when_the_producer_is_the_same_service():
    spans = [SpanRecord("t", "a", None, "svc", at(0), 10.0),
             SpanRecord("t", "a-s", "a", "svc", at(0), 2.0, kind="messaging", subtype="kafka", resource="kafka/retry"),
             SpanRecord("t", "c", "a-s", "svc", at(1), 50.0, consumer=True, queue="retry")]
    stats = sp.window_stats(spans, at(-1), at(5))["svc"]
    assert stats.requests == 2 and stats.total_ms_mean == 30.0


def test_inbound_callers_mark_message_consumers_instead_of_calling_them_external():
    spans = produce(1, at(0)) + consume(1, at(5)) + produce(2, at(0)) + consume(2, at(6), how="link") + consume(3, at(7))
    assert sp.inbound_callers(spans, at(-1), at(10)) == {"ord-api": {"": 2}, "ship-worker": {"mq:ord-api": 2, "?": 1}}


def test_sample_trace_can_pick_a_failed_consumer_whose_parent_is_outside_the_window():
    spans = (produce(1, at(-30)) + consume(1, at(5), fail=True)
             + [SpanRecord("slow", "slow-e", None, "ship-worker", at(6), 900.0, name="GET /health")])
    picked = sp.sample_trace(spans, at(0), at(20))
    assert picked.span_id == "c1-e" and sp.sample_trace_reason(picked, spans) == "入口失敗"


def test_service_topology_edges_do_not_treat_a_queue_as_a_synchronous_call():
    spans = produce(1, at(0)) + consume(1, at(5)) + [SpanRecord("p1", "pay", "p1-e", "pay-svc", at(0), 5.0)]
    assert dict(service_edges(spans)) == {("ord-api", "pay-svc"): 1}


# ── stages/rca/mq.py：講給人聽的字與判準 ──


def _edge(lag=1_200_000.0, before=1_200_000.0, producer="ord-api", queue="order-events", linked_by="parent", failed=0):
    return MqEdge(producer, "ship-worker", queue, 80, lag, lag, at(-22), at(0), linked_by, before, failed)


def test_lag_text_units():
    assert [lag_text(v) for v in (None, 291.4, 999.9, 1000, 8100, 119_999, 120_000, 1_200_000)] == [
        "—", "291 ms", "1000 ms", "1.0 秒", "8.1 秒", "120.0 秒", "2.0 分鐘", "20.0 分鐘"]
    assert window_text(at(-22), at(0)) == "13:38–14:00"


def test_backlog_needs_both_an_absolute_lag_and_a_doubling():
    assert is_backlog(_edge(480_000.0, 291.0), 60) is True
    assert is_backlog(_edge(480_000.0, None), 60) is True  # 事件前沒有樣本：只看絕對值
    assert is_backlog(_edge(30_000.0, 291.0), 60) is False  # 沒到下限
    assert is_backlog(_edge(1_200_000.0, 1_200_000.0), 60) is False  # 本來就固定延遲 20 分鐘
    assert is_backlog(_edge(1_200_000.0, 600_000.0), 60) is True


def test_async_attribution_shape():
    info = async_attribution(_edge(480_000.0, 291.0), "consumer", 60)
    assert info == {"role": "consumer", "queue": "order-events", "producer": "ord-api", "consumer": "ship-worker",
                    "lag_ms": {"before": 291.0, "during": 480_000.0}, "lag_text": {"before": "291 ms", "during": "8.0 分鐘"},
                    "pub_window": [at(-22).isoformat(), at(0).isoformat()], "pub_window_text": "13:38–14:00",
                    "linked_by": "parent", "messages": 80, "backlog": True}
    moved = async_attribution(_edge(), "producer", 60, moved_error=0.49326, suspect_payload=True)
    assert moved["moved_error"] == 0.4933 and moved["suspect_payload"] is True and moved["backlog"] is False


def test_edge_line_says_how_the_publish_time_was_recovered():
    assert edge_line(_edge(480_000.0, 291.0)) == ("訊息佇列 order-events：ord-api → ship-worker，80 則；延遲 P95 291 ms→8.0 分鐘；"
                                                  "這批訊息的發布時窗 13:38–14:00（消費者的 parent 指到發送 span）")
    unknown = edge_line(_edge(producer="", queue="", linked_by="age", before=None))
    assert "訊息佇列 （佇列名稱未知）：（生產者未知） → ship-worker" in unknown and "只有 message.age，不知道是誰發的" in unknown and "—→20.0 分鐘" in unknown


def test_producer_windows_compare_the_publish_window_with_the_equal_span_before_it():
    spans = world()
    edge = mq.build_edges(spans, ["ship-worker"], at(0), at(20), at(-15), PAD)[0][0]
    before, during = producer_windows(spans, edge)
    assert (before.requests, during.requests) == (87, 88)
    assert before.self_ms_per_request == pytest.approx(52.0) and during.self_ms_per_request == pytest.approx((80 * 432 + 8 * 52) / 88)
    assert producer_windows(spans, _edge(producer="")) == (None, None)


# ── rca.root_cause.evidence：回到發布時窗看生產者 ──


def test_poison_messages_from_a_degraded_producer_are_traced_back_to_the_publish_window():
    """12b 的單元版：ship-worker 14:00 起失敗一半，訊息是 ord-api 在 13:40–14:00（orders-db 變慢時）發的。"""
    ranked, ctx, _ = run_rank(world())
    assert scores(ranked) == [("ord-api", 1.5), ("ship-worker", 0.0)]
    edge = ctx.rca_mq[0]
    assert (edge.messages, edge.failed, edge.linked_by) == (80, 40, "parent")
    assert window_text(edge.pub_start, edge.pub_end) == "13:38–14:00"  # 真值 13:40–14:00，前後各留 2 分鐘

    producer, consumer = ranked
    assert producer.dependency == "orders-db（db/postgresql）"
    assert producer.attribution["wait_ms"] == {"before": 20.0, "during": pytest.approx(365.5, abs=0.1)}
    info = producer.attribution["async"]
    assert (info["role"], info["consumer"], info["queue"], info["moved_error"], info["backlog"]) == ("producer", "ship-worker", "order-events", 0.5, False)
    assert info["pub_window_text"] == "13:38–14:00" and info["lag_text"] == {"before": "20.0 分鐘", "during": "20.0 分鐘"}
    assert "訊息發布時窗 13:38–14:00" in producer.reason and "不是事件時窗" in producer.reason
    assert "承接 ship-worker 自身錯誤率增量中的 50%" in producer.reason

    assert consumer.dependency == "" and consumer.attribution["async"]["role"] == "consumer"
    assert consumer.attribution["async"]["moved_error"] == 0.5 and "suspect_payload" not in consumer.attribution["async"]
    assert "改記在 ord-api" in consumer.reason and "不能證明訊息內容有問題" in consumer.reason  # 推論要標成推論


def test_span_links_give_the_same_answer_as_parent_ids():
    ranked, ctx, _ = run_rank(world(how="link"))
    assert scores(ranked) == [("ord-api", 1.5), ("ship-worker", 0.0)] and ctx.rca_mq[0].linked_by == "link"


def test_without_messaging_edges_the_ranking_only_sees_the_consumer():
    """對照組（改前的行為）：不回發布時窗，ord-api 在事件時窗裡一切正常，根因只會是消費者。"""
    ranked, _, _ = run_rank(world(), mq_on=False)
    assert scores(ranked) == [("ship-worker", 0.5)] and "async" not in ranked[0].attribution


def test_healthy_producer_is_not_blamed_and_the_report_says_trace_cannot_see_message_content():
    """12e：毒訊息，但生產者在發布時窗沒有任何症狀。不轉記、不指認生產者，但要把「該去抽查訊息」講出來。"""
    ranked, _, _ = run_rank(world(slow=False))
    assert scores(ranked) == [("ship-worker", 0.5)]
    top = ranked[0]
    assert "未見異常，不轉記" in top.reason and "dead-letter queue" in top.reason and "無法排除是 ord-api 發出的內容有問題" in top.reason
    assert top.attribution["async"]["role"] == "consumer" and top.attribution["async"]["suspect_payload"] is True
    assert "moved_error" not in top.attribution["async"]


@pytest.mark.xfail(strict=True, reason="已知死區：毒訊息而生產者沒有自身症狀。trace 只有時間與成敗，看不到訊息內容，"
                                       "無從證明是生產者發的內容有問題（docs/MQ_QPS_DESIGN.md 12e）")
def test_dead_zone_poison_message_from_a_symptom_free_producer_is_not_attributed_to_it():
    ranked, _, _ = run_rank(world(slow=False))
    assert ranked[0].entity == "ord-api"  # 真正的源頭


def test_healthy_producer_and_healthy_consumer_errors_raise_no_payload_suspicion():
    # 消費者只是變慢（沒有失敗）：不准無端懷疑訊息內容
    spans, i = [], 0
    for m in range(-60, 20):
        for k in range(4):
            pub = at(m, k * 15)
            late = m >= -8  # 13:52 之後發的訊息卡了 8 分鐘才被消費，而且那時 ship-db 變慢
            spans += produce(i, pub)
            spans += consume(i, pub + (timedelta(minutes=8) if late else timedelta(seconds=1)), db_ms=600.0 if late else 200.0)
            i += 1
    ranked, ctx, incident = run_rank(spans)
    assert scores(ranked) == [("ship-worker", 1.0)]  # 12a：消費者自己的資源慢，生產者不上榜
    top = ranked[0]
    assert top.dependency == "ship-db（db/postgresql）"
    info = top.attribution["async"]
    assert (info["role"], info["backlog"], info["lag_text"]) == ("consumer", True, {"before": "980 ms", "during": "8.0 分鐘"})
    assert "suspect_payload" not in info and "未見異常，不轉記" in top.reason and "dead-letter" not in top.reason
    # 有資源主導：該查的是 ship-db（RB-02），不是佇列（RB-07）
    ctx.rca_root_candidates = ranked
    assert [r.id for r in evaluate(RunbookFacts.from_ctx(incident, ctx))] == ["RB-02"]


def test_failures_are_moved_only_when_most_of_them_are_message_consumption():
    """轉記門檻：事件期間 ship-worker 自己失敗 160 筆，其中只有 40 筆（25%）是在消費訊息——大部分失敗跟訊息無關，不轉記。"""
    spans = world()
    for j in range(35 * 6):
        ts = at(-15, j * 10)
        spans.append(SpanRecord(f"s{j}", f"s{j}-e", None, "ship-worker", ts, 20.0, error=ts >= at(0), name="POST /ship"))
    ranked, _, _ = run_rank(spans)
    assert scores(ranked) == [("ord-api", 1.0), ("ship-worker", 0.5)]  # 生產者仍因發布時窗的自身耗時上榜，但沒有承接錯誤
    assert all("moved_error" not in c.attribution["async"] for c in ranked)
    assert "改記在" not in ranked[1].reason and "承接" not in ranked[0].reason


def test_async_weight_zero_turns_the_transfer_off_but_still_looks_at_the_publish_window():
    ranked, _, _ = run_rank(world(), async_weight=0)
    assert scores(ranked) == [("ord-api", 1.0), ("ship-worker", 0.5)]
    assert ranked[0].attribution["async"]["role"] == "producer" and "moved_error" not in ranked[0].attribution["async"]


@pytest.mark.parametrize("params", [{"async_weight": 1.5}, {"async_weight": -0.1}, {"min_lag_seconds": -1}])
def test_async_params_are_validated_at_construction(params):
    with pytest.raises(ValueError, match="async_weight"):
        OperatorRegistry.create("rca.root_cause.evidence", params)


def test_age_only_messages_have_no_producer_to_look_back_at_and_say_so():
    spans = []
    for i in range(35 * 4):
        ts = at(-15, i * 15)
        spans += consume(i, ts, how="none", age_ms=1_200_000.0, fail=ts >= at(0) and i % 2 == 0)
    ranked, ctx, _ = run_rank(spans)
    assert scores(ranked) == [("ship-worker", 0.5)]
    info = ranked[0].attribution["async"]
    assert (info["role"], info["producer"], info["linked_by"], info["pub_window_text"]) == ("consumer", "", "age", "13:38–14:00")
    assert "只有 message.age、不知道是誰發的，沒有回頭看生產者" in ranked[0].reason
    assert ctx.rca_mq[0].lag_ms_p95 == 1_200_000.0


def test_producer_with_too_few_requests_in_the_publish_window_is_not_judged():
    spans, i = [], 0
    for n, m in enumerate(range(-60, 20, 2)):  # 生產者 2 分鐘才一筆請求，一筆被 8 個消費者各處理一次
        spans += produce(n, at(m), db_ms=400.0 if -20 <= m < 0 else 20.0)
        for j in range(8):
            spans += consume(i, at(m + 20, j * 10), src=n, fail=-20 <= m < 0 and j % 2 == 0)
            i += 1
    ranked, _, _ = run_rank(spans)
    assert [c.entity for c in ranked] == ["ship-worker"]
    assert "入口請求不足 20 筆，無法比較它當時有沒有異常" in ranked[0].reason and "moved_error" not in ranked[0].attribution["async"]


def test_producer_already_in_the_incident_keeps_its_incident_window_row():
    """生產者本身也在事件群裡（偵測端 alert.messaging_link 併起來的情況）：它那一列看的是事件時窗，
    發布時窗的數字寫進理由，不另開一列、也不重複計分。"""
    ranked, _, _ = run_rank(world(), entities=("ord-api", "ship-worker"))
    assert sorted(c.entity for c in ranked) == ["ord-api", "ship-worker"]
    producer = next(c for c in ranked if c.entity == "ord-api")
    assert "（事件前 15 分鐘）" in producer.reason and "它在訊息發布時窗 13:38–14:00" in producer.reason


# ── Runbook RB-07 與 lag 訊號 ──


def _facts(async_info=None, signals=(), by="", kind="", share=0.0):
    attribution = {"resource": "ship-db" if kind else "", "kind": kind, "subtype": "", "share": share, "by": by,
                   "failures": {"before": 0, "during": 0}}
    if async_info is not None:
        attribution["async"] = async_info
    return RunbookFacts(set(signals), set(), RootCandidate("ship-worker", 1.0, "r", "x" if by else "", attribution), None, "", [], False)


def test_lag_is_a_fifth_signal_class():
    assert signal_of("kafka_consumer_lag", None) == "lag" and signal_of("queue_backlog", None) == "lag"
    assert signal_of("anything", {"signal": "lag"}) == "lag"
    assert signal_of("lag_duration", None) == ""  # 同時像兩類：都不認


def test_rb07_fires_for_a_producer_top_candidate_and_sends_people_to_the_publish_window_first():
    facts = _facts(async_attribution(_edge(), "producer", 60, moved_error=0.5))
    hit = evaluate(facts)[0]
    assert hit.id == "RB-07"
    first, second, third = (a.format(**facts.fmt()) for a in hit.ordered(facts))
    assert first == "回到訊息發布時段 13:38–14:00 查生產者 ord-api 當時的部署、設定變更與它的依賴——不是看消費者出事的那段時間"
    assert second.startswith("抽查失敗訊息的內容與 dead-letter queue")
    assert third == "檢查佇列 order-events 的積壓深度（consumer lag）與消費者並行度／消費速率（事件期間訊息延遲 P95 20.0 分鐘）"


def test_rb07_fires_for_a_backlog_and_checks_the_queue_depth_first():
    facts = _facts(async_attribution(_edge(480_000.0, 291.0), "consumer", 60))
    hit = evaluate(facts)[0]
    assert hit.id == "RB-07" and hit.ordered(facts)[0].startswith("檢查佇列 {queue} 的積壓深度")


def test_rb07_fires_on_the_lag_signal_alone_and_admits_what_trace_did_not_recover():
    facts = _facts(signals={"lag"})
    hit = evaluate(facts)[0]
    assert hit.id == "RB-07"
    text = " ".join(a.format(**facts.fmt()) for a in hit.ordered(facts))
    assert "（佇列名稱未知）" in text and "（trace 未能還原）" in text and "（trace 未能辨識）" in text and "P95 未知" in text


def test_rb07_stays_quiet_when_a_downstream_resource_explains_the_backlog_or_the_delay_is_constant():
    backlog = async_attribution(_edge(480_000.0, 291.0), "consumer", 60)
    assert [r.id for r in evaluate(_facts(backlog, by="latency", kind="db", share=1.0))] == ["RB-02"]
    assert [r.id for r in evaluate(_facts(async_attribution(_edge(), "consumer", 60)))] == ["RB-00"]  # 固定延遲、消費者角色


def test_runbook_priorities_stay_unique_with_rb07_between_rb01_and_rb02():
    order = [r.id for r in sorted(RUNBOOK_RULES, key=lambda r: -r.priority)]
    assert order[:3] == ["RB-01", "RB-07", "RB-02"] and len({r.priority for r in RUNBOOK_RULES}) == len(RUNBOOK_RULES)


# ── 整段：根因 → 建議處置 → 三行摘要 → API 回傳 ──


def _report(spans):
    ranked, ctx, incident = run_rank(spans)
    OperatorRegistry.create("rca.section.suggested_actions", {}).execute(ctx)
    OperatorRegistry.create("enrichment.llm_briefing", {}).execute(ctx)
    return ranked, ctx, incident


def test_report_for_a_poisoning_producer_points_at_the_publish_window_end_to_end():
    _, ctx, incident = _report(world())
    assert (ctx.rca_runbook["id"], ctx.rca_runbook["also"]) == ("RB-07", ["RB-02"])
    assert ctx.rca_runbook["actions"][0].startswith("回到訊息發布時段 13:38–14:00 查生產者 ord-api")
    assert ctx.llm_briefing_generated_by == "template"
    b = ctx.llm_briefing
    assert b["summary_line"].startswith("【一句話根因】ord-api 等待 orders-db（db/postgresql）20.0→365.5 ms")
    assert "ord-api 是訊息生產者，以上是訊息發布時窗 13:38–14:00 的數字（ship-worker 事後才消費到，延遲 P95 20.0 分鐘）" in b["summary_line"]
    assert b["action_line"].startswith("【立即止血動作】[RB-07] 回到訊息發布時段 13:38–14:00 查生產者 ord-api")
    # 消費者的失敗算在它發的訊息上，就不能再寫「排除它本身的程式」
    assert b["verification_line"] == ("【驗證與排除項】ship-worker 處理失敗的訊息是 ord-api 在 13:38–14:00 發的；"
                                      "trace 看不到訊息內容，不能排除 ord-api 當時發出壞訊息，請抽查失敗訊息")
    assert "排除它本身的程式" not in str(b)
    # 模板本身過得了 LLM 回覆的守門（數字、實體、封閉詞表）：LLM 照 context 寫出同樣的話不會被誤拒
    context = narrative.build_context(ctx, incident)
    assert narrative.validate_briefing(b, context, {"ord-api", "ship-worker", "cart-api"}) is None


def test_report_for_the_dead_zone_adds_a_line_telling_people_to_inspect_failed_messages():
    _, ctx, _ = _report(world(slow=False))
    assert ctx.rca_runbook["id"] == "RB-03"  # 主處方仍是消費者自己的程式：trace 分不出是它的 bug 還是訊息內容，兩邊都要查
    body = ctx.rca_sections[-1].body
    line = next(line for line in body.splitlines() if "dead-letter queue" in line)
    assert "ship-worker 處理佇列 order-events 的訊息失敗，而生產者 ord-api 在發布時段 13:38–14:00 沒有症狀" in line
    assert "核對 ord-api 當時的部署與設定變更" in line


def test_api_result_carries_messaging_edges_only_when_there_are_any():
    _, ctx, _ = _report(world())
    messaging = ctx.to_result()["root_cause"]["messaging"]
    assert messaging["note"] == "" and len(messaging["edges"]) == 1
    edge = messaging["edges"][0]
    assert {k: edge[k] for k in ("producer", "consumer", "queue", "messages", "linked_by", "failed")} == {
        "producer": "ord-api", "consumer": "ship-worker", "queue": "order-events", "messages": 80, "linked_by": "parent", "failed": 40}
    assert edge["lag_ms"]["p95"] == pytest.approx(1_199_980.0, abs=400) and len(edge["pub_window"]) == 2

    _, plain, _ = run_rank(world(), mq_on=False)
    plain.rca_sections = [ReportSection("x", "x", "x")]
    assert "messaging" not in plain.to_result()["root_cause"]  # 沒有 messaging 資料：回傳形狀跟以前一樣
    plain.rca_mq_note = "偵測到 messaging consumer，但…"
    assert plain.to_result()["root_cause"]["messaging"] == {"note": "偵測到 messaging consumer，但…", "edges": []}


def test_trace_evidence_and_root_cause_sections_show_the_queue_and_the_producer_window():
    _, ctx, _ = _report(world())
    OperatorRegistry.create("rca.section.trace_evidence", {}).execute(ctx)
    body = ctx.rca_sections[-1].body
    assert "訊息佇列 order-events：ord-api → ship-worker，80 則；延遲 P95 20.0 分鐘→20.0 分鐘；這批訊息的發布時窗 13:38–14:00" in body
    assert "ord-api 在發布時窗：請求 87→88；自身耗時 52.0→397.5 ms（+345.5）；自身錯誤率 0%→0%" in body

    note_only = PipelineContext(pipeline="rca", now=at(30))
    note_only.incidents, note_only.incidents_built = [Incident([AnomalyEvent("ship-worker", "errors", at(0), 5.0)])], True
    note_only.rca_mq_note = "偵測到 messaging consumer，但沒有 span link、parent 或 message.age，無法跨時窗"
    OperatorRegistry.create("rca.section.root_cause_candidates", {}).execute(note_only)
    assert "無法跨時窗" in note_only.rca_sections[-1].body  # 看到消費者卻跨不了時窗：要講，不然值班的人以為生產者看過了
