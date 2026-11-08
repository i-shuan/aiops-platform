"""告警算子：收斂、計分、抑制、心跳結案、通知內容。全部直接用 ctx 測，不經資料庫（證明算子只讀 ctx）。"""
from datetime import datetime, timedelta, timezone

import pytest

from aiops.core.context import LotEdge, OpenIncident, PipelineContext, RecentNotification, Silence
from aiops.core.registry import OperatorRegistry
from aiops.core.types import AnomalyEvent, Incident

T0 = datetime(2026, 11, 8, tzinfo=timezone.utc)


def _ev(entity, minutes=0, score=3.0, metric="cpu", **attrs):
    return AnomalyEvent(entity, metric, T0 + timedelta(minutes=minutes), score, attrs)


def _ctx(*events, now=T0 + timedelta(hours=1), **kw):
    ctx = PipelineContext(pipeline="wf", now=now)
    ctx.anomalies = list(events)
    for k, v in kw.items():
        setattr(ctx, k, v)
    return ctx


def _run(name, ctx, **params):
    return OperatorRegistry.create(name, params).execute(ctx)


def _entities(ctx):
    return sorted(i.entities for i in ctx.incidents)


# ── 收斂 ──


def test_same_entity_merges_close_events_only():
    ctx = _run("alert.same_entity", _ctx(_ev("a", 0), _ev("a", 5), _ev("a", 60), _ev("b", 1)), window_minutes=10)
    assert _entities(ctx) == [["a"], ["a"], ["b"]]
    assert sorted(len(i.events) for i in ctx.incidents) == [1, 1, 2]


def test_topology_chain_merges_adjacent_services_and_is_a_noop_without_edges():
    events = (_ev("frontend", 1), _ev("cart", 0), _ev("unrelated", 0))
    assert _entities(_run("alert.topology_chain", _ctx(*events))) == [["cart"], ["frontend"], ["unrelated"]]  # 沒拓撲就不併
    ctx = _run("alert.topology_chain", _ctx(*events, topology=[("frontend", "cart")]))
    assert _entities(ctx) == [["cart", "frontend"], ["unrelated"]] and "topology_chain" in ctx.incidents[0].tags


def test_lot_path_chain_respects_min_lots_and_window():
    edges = [LotEdge("LITHO", "ETCH", 3), LotEdge("ETCH", "CMP", 1)]
    events = (_ev("LITHO", 0), _ev("ETCH", 30), _ev("CMP", 40))
    ctx = _run("alert.lot_path_chain", _ctx(*events, lot_edges=edges), min_lots=2)
    assert _entities(ctx) == [["CMP"], ["ETCH", "LITHO"]]  # 流向太少的不併
    far = _run("alert.lot_path_chain", _ctx(_ev("LITHO", 0), _ev("ETCH", 300), lot_edges=edges), window_minutes=60)
    assert _entities(far) == [["ETCH"], ["LITHO"]]


def test_groupers_compose_in_workflow_order():
    ctx = _ctx(_ev("a", 0), _ev("a", 2), _ev("b", 3), topology=[("b", "a")])
    ctx = _run("alert.topology_chain", _run("alert.same_entity", ctx))
    assert _entities(ctx) == [["a", "b"]]


# ── 計分 ──


def test_scorers_boost_and_do_not_touch_other_incidents():
    ctx = _run("alert.same_entity", _ctx(_ev("a", 0, 2.0, metric="latency"), _ev("b", 0, 2.0, metric="disk")))
    ctx = _run("alert.golden_signals", ctx, boost=2.0)
    assert sorted((i.entities[0], i.score) for i in ctx.incidents) == [("a", 4.0), ("b", 2.0)]
    ctx = _run("alert.p0_boost", ctx, entities=["b"], boost=10)
    assert {i.entities[0]: i.score for i in ctx.incidents} == {"a": 4.0, "b": 12.0}
    assert _run("alert.p0_boost", _ctx(_ev("a")), boost=10).incidents[0].score == 3.0  # 沒指定 P0 實體就不加


# ── 抑制 ──


def _silence(**kw):
    return Silence(id=7, kind="maintenance", start=T0 - timedelta(hours=1), end=T0 + timedelta(hours=1), **kw)


def test_silence_needs_every_event_covered_and_respects_scope():
    ctx = _ctx(_ev("a", 0, dept="pay"), _ev("a", 200, dept="pay"), silences=[_silence(entity="a")])
    assert _run("alert.suppress_window", _run("alert.same_entity", ctx, window_minutes=10)).incidents[1].suppressed_by is None
    covered = _run("alert.suppress_window", _run("alert.same_entity", _ctx(_ev("a", 0), silences=[_silence(entity="a")])))
    assert covered.incidents[0].suppressed_by == "silence_window：maintenance#7"
    other = _run("alert.suppress_window", _run("alert.same_entity", _ctx(_ev("a", 0, dept="x"), silences=[_silence(dept="y")])))
    assert other.incidents[0].suppressed_by is None  # 部門不同
    assert _ctx(silences=[_silence()]).is_suppressed is False


def test_dedup_uses_event_time_and_cooldown():
    sent = [RecentNotification("a", T0)]
    ctx = _run("alert.suppress_dedup", _ctx(_ev("a", 10), recent_notifications=sent), cooldown_minutes=30)
    assert ctx.incidents[0].suppressed_by.startswith("dedup")
    later = _run("alert.suppress_dedup", _ctx(_ev("a", 90), recent_notifications=sent), cooldown_minutes=30)
    assert later.incidents[0].suppressed_by is None
    other = _run("alert.suppress_dedup", _ctx(_ev("b", 10), recent_notifications=sent))
    assert other.incidents[0].suppressed_by is None


def test_same_root_cause_suppresses_the_symptom_not_the_cause():
    ctx = _ctx(_ev("frontend", 0, 2.0), _ev("db", 5, 5.0), topology=[("frontend", "db")])
    ctx = _run("alert.suppress_same_root_cause", _run("alert.same_entity", ctx, window_minutes=1))
    by = {i.entities[0]: i.suppressed_by for i in ctx.incidents}
    assert by["db"] is None and by["frontend"].startswith("same_root_cause")
    assert _run("alert.suppress_same_root_cause", _run("alert.same_entity", _ctx(_ev("a"), _ev("b")))).is_suppressed is False


def test_first_suppressor_to_give_a_reason_wins():
    ctx = _ctx(_ev("a", 10), silences=[_silence(entity="a")], recent_notifications=[RecentNotification("a", T0)])
    ctx = _run("alert.suppress_dedup", _run("alert.suppress_window", _run("alert.same_entity", ctx)))
    assert ctx.incidents[0].suppressed_by.startswith("silence_window")


# ── 通知內容 ──


def test_payload_splits_by_dept_uses_uid_and_skips_suppressed():
    ctx = _run("alert.same_entity", _ctx(_ev("a", 0, 11.0, dept="pay"), _ev("a", 1, 4.0, dept="plat"), _ev("z", 0, 1.0)))
    ctx.incidents[0].suppressed_by = None
    ctx = _run("alert.notify_payload", ctx)
    sev = {i.entities[0]: i.severity for i in ctx.incidents}
    assert sev == {"a": "P0", "z": "P3"}
    labels = [n.labels for n in ctx.notifications]
    assert sorted(l["dept"] for l in labels) == ["pay", "plat", "unassigned"]
    assert all("incident_uid" in l and "incident_id" not in l for l in labels)
    assert all(l["incident_uid"].startswith("inc-") for l in labels)

    ctx2 = _ctx(_ev("a"))
    ctx2 = _run("alert.same_entity", ctx2)
    ctx2.incidents[0].suppressed_by = "dedup"
    assert _run("alert.notify_payload", ctx2).notifications == []
    assert ctx2.incidents[0].severity  # 被抑制的也要有嚴重度（要存檔）


def test_two_incidents_never_share_an_uid():
    ctx = _run("alert.same_entity", _ctx(_ev("a"), _ev("b")))
    assert len({i.uid for i in ctx.incidents}) == 2


# ── 心跳與結案 ──


def _open(entity_end_minutes=0, entity="a"):
    inc = Incident([_ev(entity, entity_end_minutes)])
    return OpenIncident(1, inc.uid, "wf", "P1", inc)


def test_heartbeat_resends_with_identical_labels_then_resolves_when_quiet():
    opened = _open()
    first = _run("alert.notify_payload", _run("alert.same_entity", _ctx(_ev("a", 0))))
    quiet = _run("alert.heartbeat", _ctx(open_incidents=[opened], now=T0 + timedelta(minutes=10)), resolve_after_minutes=15)
    assert quiet.resolved_incident_ids == [] and quiet.notifications[0].ends_at is None
    done = _run("alert.heartbeat", _ctx(open_incidents=[opened], now=T0 + timedelta(minutes=30)), resolve_after_minutes=15)
    assert done.resolved_incident_ids == [1] and done.notifications[0].ends_at is not None
    assert done.notifications[0].labels == quiet.notifications[0].labels  # 接收端靠 labels 認同一筆
    assert first.notifications[0].labels["alertname"] == done.notifications[0].labels["alertname"]


def test_new_anomalies_keep_an_incident_open_even_if_it_looks_old():
    opened = _open()
    stored = _run("alert.heartbeat", _ctx(open_incidents=[opened], entity_last_seen={"a": T0 + timedelta(minutes=25)},
                                          now=T0 + timedelta(minutes=30)), resolve_after_minutes=15)
    fresh = _run("alert.heartbeat", _ctx(_ev("a", 28), open_incidents=[opened], now=T0 + timedelta(minutes=30)),
                 resolve_after_minutes=15)
    assert stored.resolved_incident_ids == [] and fresh.resolved_incident_ids == []


def test_heartbeat_with_nothing_open_is_a_noop():
    assert _run("alert.heartbeat", _ctx()).notifications == []


def test_operators_never_mutate_unrelated_context():
    ctx = _ctx(_ev("a"), silences=[_silence()])
    before = (list(ctx.silences), list(ctx.recent_notifications), list(ctx.topology))
    for name in ("alert.same_entity", "alert.suppress_window", "alert.notify_payload"):
        ctx = _run(name, ctx)
    assert (ctx.silences, ctx.recent_notifications, ctx.topology) == before


@pytest.mark.parametrize("name", [n for n in OperatorRegistry.names("alert.")])
def test_every_alert_operator_is_batch_scoped(name):
    assert OperatorRegistry.get(name).scope == "batch"
