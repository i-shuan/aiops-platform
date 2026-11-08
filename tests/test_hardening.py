"""強化項目：訓練排除歷史事故窗、warning／anomaly 兩級告警、Log 樣式比對、Dead Man's Snitch 心跳。"""
from datetime import datetime, timedelta, timezone

import httpx
import pandas as pd
import pytest

from aiops.engine.context import LogRecord, PipelineContext, RecentNotification, Target, TrainingExclusion
from aiops.engine.errors import InfrastructureError, WorkflowError
from aiops.engine.pipeline import NodeSpec, Pipeline, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import ANOMALY, WARNING, AnomalyEvent, Incident
from aiops.interfaces.runner import workflow_path
from aiops.sources.elasticsearch import _log
from aiops.stages.rca.sections.log_patterns import rank_patterns, template_of
from aiops.storage.hooks import StorageHooks
from aiops.storage.repository import Feedbacks, Incidents, Silences

T0 = datetime(2026, 11, 1, tzinfo=timezone.utc)


def _series(n=300, spike_at=None, spike=None):
    values = [50.0 + (i % 5) for i in range(n)]
    if spike_at is not None:
        for i in range(spike_at, spike_at + 60):
            values[i] = spike
    return pd.DataFrame({"ts": pd.date_range(T0, periods=n, freq="60s"), "value": values})


def _ctx(raw, train=None, exclusions=(), entity="svc"):
    ctx = PipelineContext(pipeline="p", now=T0 + timedelta(days=1), target=Target("mock", "q", entity))
    ctx.raw_df, ctx.train_df, ctx.training_exclusions = raw, train, list(exclusions)
    return ctx


# ── 1. 訓練排除歷史事故窗 ──


def test_training_skips_incident_window_so_baseline_is_not_polluted():
    dirty = _series(spike_at=100, spike=500.0)
    window = TrainingExclusion(T0 + timedelta(minutes=95), T0 + timedelta(minutes=165), "incident x")
    clean_ctx = OperatorRegistry.create("detector.zscore").execute(_ctx(_series(30), dirty, [window]))
    polluted = OperatorRegistry.create("detector.zscore").execute(_ctx(_series(30), dirty))
    clean = clean_ctx.new_detector_states["detector.zscore"]
    assert clean.points == 300 - 71 and polluted.new_detector_states["detector.zscore"].points == 300
    assert any("excluded:71pts" in a for a in clean_ctx.audit)
    assert clean.state["std"] < polluted.new_detector_states["detector.zscore"].state["std"] / 10


def test_exclusion_falls_back_to_all_data_when_too_little_is_left():
    train = _series(100)
    window = TrainingExclusion(T0, T0 + timedelta(minutes=80), "incident x")
    ctx = OperatorRegistry.create("detector.zscore").execute(_ctx(_series(30), train, [window]))
    assert ctx.new_detector_states["detector.zscore"].points == 100
    assert any("exclusion-skipped" in a for a in ctx.audit)


def test_exclude_incidents_false_keeps_everything_and_cannot_be_a_target_override():
    window = TrainingExclusion(T0 + timedelta(minutes=95), T0 + timedelta(minutes=165), "x")
    op = OperatorRegistry.create("detector.zscore", {"exclude_incidents": False})
    assert op.execute(_ctx(_series(30), _series(spike_at=100, spike=500.0), [window])).new_detector_states[
        "detector.zscore"].points == 300
    with pytest.raises(TypeError, match="行為參數"):
        op.check_override({"exclude_incidents": True})


def _store_incident(entity, start, pipeline="p", uid="u1"):
    inc = Incident([AnomalyEvent(entity, "q", start, 5.0), AnomalyEvent(entity, "q", start + timedelta(minutes=20), 5.0)],
                   uid=uid)
    Incidents.create(uid, pipeline, inc.to_dict(), "P1", None, pipeline)


def test_hooks_turn_incidents_and_maintenance_into_exclusions_but_not_false_positives():
    _store_incident("svc", T0, uid="real")
    _store_incident("svc", T0 + timedelta(hours=5), uid="fp")
    _store_incident("other", T0, uid="elsewhere")
    _store_incident("svc", T0, pipeline="another", uid="other-pipeline")
    Feedbacks.create({"incident_uid": "fp", "verdict": "false_positive", "text": "誤報", "author": "t"})
    naive = T0.replace(tzinfo=None)
    Silences.create({"kind": "maintenance", "start": naive + timedelta(hours=8), "end": naive + timedelta(hours=9),
                     "entity": "svc", "dept": None, "reason": "換機", "created_by": "t"})
    Silences.create({"kind": "mute", "start": naive, "end": naive + timedelta(hours=1),
                     "entity": "svc", "dept": None, "reason": "只是靜音", "created_by": "t"})
    ctx = _ctx(_series(30))
    StorageHooks().load_target(ctx, [("detect", "detector.zscore")])
    got = sorted((w.reason, w.start, w.end) for w in ctx.training_exclusions)
    assert [r.split(" #")[0] for r, *_ in got] == ["incident real", "maintenance"]
    assert got[0][1] == T0 - timedelta(minutes=10) and got[0][2] == T0 + timedelta(minutes=30)


def test_reused_hooks_reload_the_exclusion_pool_every_round():
    """同一個 StorageHooks 跑兩輪、而且兩輪的 now 一樣（回放固定 --now、或 API 帶同一個 now）：
    第二輪要看到第一輪之後才寫進去的事故，不能沿用上一輪的快取（CLAUDE.md 第 100 節第 3 條）。"""
    hooks = StorageHooks()
    first = _ctx(_series(30))
    hooks.load_batch(first)
    hooks.load_target(first, [("detect", "detector.zscore")])
    assert first.training_exclusions == []
    _store_incident("svc", T0, uid="later")
    second = _ctx(_series(30))
    hooks.load_batch(second)
    hooks.load_target(second, [("detect", "detector.zscore")])
    assert [w.reason for w in second.training_exclusions] == ["incident later"]


# ── 2. warning／anomaly 兩級 ──


def test_warning_ratio_marks_near_threshold_points():
    raw = _series(30)
    train = _series(300)
    mean, std = train["value"].mean(), train["value"].std()
    raw.loc[10, "value"], raw.loc[20, "value"] = mean + 10 * std, mean + 2.5 * std  # 一個越線、一個接近門檻
    ctx = OperatorRegistry.create("detector.zscore", {"threshold": 3.0, "warning_ratio": 0.6}).execute(_ctx(raw, train))
    levels = {e.ts: e.level for e in ctx.anomalies}
    assert levels[raw.loc[10, "ts"].to_pydatetime()] == ANOMALY
    assert levels[raw.loc[20, "ts"].to_pydatetime()] == WARNING
    plain = OperatorRegistry.create("detector.zscore", {"threshold": 3.0}).execute(_ctx(raw, train))
    assert {e.level for e in plain.anomalies} == {ANOMALY}


def test_warning_ratio_is_validated():
    with pytest.raises(WorkflowError, match="0 到 1"):
        OperatorRegistry.create("detector.zscore", {"warning_ratio": 1.2})
    with pytest.raises(WorkflowError, match="threshold"):
        OperatorRegistry.create("detector.rules", {"upper": 80, "warning_ratio": 0.8})


def _vote(*events, runs=("a", "b")):
    ctx = PipelineContext(pipeline="p", now=T0)
    ctx.anomalies, ctx.detector_runs = list(events), list(runs)
    return OperatorRegistry.create("detector.ensemble", {"strategy": "majority"}).execute(ctx).anomalies


def _e(node, level):
    return AnomalyEvent("svc", "q", T0, 4.0, detector_node=node, level=level)


def test_ensemble_two_levels():
    assert [e.level for e in _vote(_e("a", ANOMALY), _e("b", ANOMALY))] == [ANOMALY]
    assert [e.level for e in _vote(_e("a", ANOMALY), _e("b", WARNING))] == [WARNING]
    assert _vote(_e("a", ANOMALY)) == []


def _incident(level):
    return Incident([AnomalyEvent("svc", "q", T0 + timedelta(minutes=10), 7.0, level=level)])


def test_payload_and_dedup_handle_warnings_and_escalation():
    ctx = PipelineContext(pipeline="p", now=T0)
    ctx.incidents = [_incident(WARNING)]
    out = OperatorRegistry.create("alert.notify_payload").execute(ctx)
    assert out.incidents[0].severity == WARNING and out.notifications[0].labels["level"] == WARNING
    quiet = PipelineContext(pipeline="p", now=T0)
    quiet.incidents = [_incident(WARNING)]
    out = OperatorRegistry.create("alert.notify_payload", {"notify_warnings": False}).execute(quiet)
    assert out.notifications == [] and out.incidents[0].suppressed_by.startswith("warning_only")

    for sent_severity, level, suppressed in [(WARNING, ANOMALY, False), (WARNING, WARNING, True), ("P1", ANOMALY, True)]:
        ctx = PipelineContext(pipeline="p", now=T0)
        ctx.incidents = [_incident(level)]
        ctx.recent_notifications = [RecentNotification("svc", T0, severity=sent_severity)]
        out = OperatorRegistry.create("alert.suppress_dedup", {"cooldown_minutes": 30}).execute(ctx)
        assert (out.incidents[0].suppressed_by is not None) == suppressed, (sent_severity, level)


# ── 4. Log 樣式比對 ──


def test_template_normalizes_ids_ips_and_numbers():
    assert template_of("Read timed out after 3012 ms (request 21) to 10.0.3.7:5432") == \
        "Read timed out after <NUM> ms (request <NUM>) to <IP>"
    assert template_of("order 9f1c2e3d-0b4a-4c5d-8e9f-001122334455 trace 4bf92f3577b34da6a3ce929d0e0e4736 v2") == \
        "order <ID> trace <ID> v2"


def _logs(service, msg, n, minute0, err="E"):
    return [LogRecord(T0 + timedelta(minutes=minute0 + i % 10), service, msg.format(i), err, f"t{i}") for i in range(n)]


def test_rank_patterns_puts_new_bursts_first_then_ratio():
    baseline = _logs("pay", "cache miss for key {}", 60, 0, "Cache") + _logs("pay", "slow query {} ms", 6, 0, "DB")
    during = (_logs("pay", "cache miss for key {}", 10, 70, "Cache") + _logs("pay", "slow query {} ms", 20, 70, "DB")
              + _logs("pay", "connection refused to 10.0.0.{}:5432", 12, 70, "Conn"))
    ranked = rank_patterns(during, baseline, (10.0, 60.0))
    assert [p["error_type"] for p in ranked] == ["Conn", "DB", "Cache"]
    assert ranked[0]["new"] and ranked[0]["template"] == "connection refused to <IP>"
    assert ranked[1]["ratio"] == pytest.approx((20 / 10) / (6 / 60))
    assert ranked[2]["ratio"] == pytest.approx(1.0)


def test_a_huge_burst_of_a_known_error_outranks_a_few_brand_new_ones():
    """nov03_cascade 復盤：cart-api 連線池逾時比平常多 902 倍，卻被幾筆「新出現」的樣板擠到第 8～10 名。
    現在新出現的樣板以「正常期間 1 筆」計倍率，跟既有樣板同一把尺排序。"""
    baseline = _logs("cart-api", "connection pool timeout after {} ms", 2, 0, "Pool")
    during = (_logs("cart-api", "connection pool timeout after {} ms", 900, 70, "Pool")
              + _logs("ord-svc", "slow transaction {} ms", 4, 70, "Slow"))  # 新出現，但只有 4 筆
    ranked = rank_patterns(during, baseline, (10.0, 60.0))
    assert [p["error_type"] for p in ranked] == ["Pool", "Slow"]
    assert ranked[0]["ratio"] == pytest.approx((900 / 10) / (2 / 60)) and ranked[1]["new"]


def test_log_section_says_when_logs_were_sampled():
    section = OperatorRegistry.create("rca.section.log_patterns", {})
    ctx = PipelineContext(pipeline="rca", now=T0)
    ctx.rca_logs = [LogRecord(r.ts, r.service, r.message, r.error_type, r.trace_id, 10.0)
                    for r in _logs("pay", "connection refused to 10.0.0.{}:5432", 12, 70, "Conn")]
    ctx.rca_logs_baseline, ctx.rca_log_window_minutes, ctx.rca_logs_sampled = [], (10.0, 60.0), True
    body = section.build(None, ctx)
    assert "事件期間 120 筆" in body and "抽樣" in body and "估計值" in body


def test_log_section_lists_bursts_and_skips_without_logs():
    section = OperatorRegistry.create("rca.section.log_patterns", {"top_n": 2})
    ctx = PipelineContext(pipeline="rca", now=T0)
    assert not section.applies(None, ctx)
    ctx.rca_logs = _logs("pay", "connection refused to 10.0.0.{}:5432", 12, 70, "Conn")
    ctx.rca_logs_baseline, ctx.rca_log_window_minutes = [], (10.0, 60.0)
    body = section.build(None, ctx)
    assert "新出現" in body and "Conn: connection refused to <IP>" in body and "trace t0" in body


def test_es_log_parsing_handles_exception_arrays_and_fallbacks():
    base = {"@timestamp": "2026-11-01T00:00:00Z", "service": {"name": "pay"}, "trace": {"id": "abc"}}
    arr = _log(base | {"error": {"exception": [{"message": "boom 1", "type": "IOError"}]}})
    assert (arr["message"], arr["error_type"], arr["trace_id"]) == ("boom 1", "IOError", "abc")
    assert _log(base | {"error": {"log": {"message": "plain"}}})["message"] == "plain"
    assert _log(base | {"message": "top"})["message"] == "top"
    assert _log(base) is None and _log({"message": "x"}) is None


def test_incident_logs_node_fills_ctx_from_the_data_source():
    ctx = PipelineContext(pipeline="rca", now=T0 + timedelta(hours=2))
    ctx.incidents = [Incident([AnomalyEvent("pay", "q", T0 + timedelta(hours=1), 5.0)])]
    out = OperatorRegistry.create("ingestion.incident_logs", {"source_type": "mock"}).execute(ctx)
    assert out.rca_logs and out.rca_logs_baseline and {r.service for r in out.rca_logs} == {"pay"}
    assert out.rca_log_window_minutes == (10.0, 60.0)
    assert all(r.ts < T0 + timedelta(minutes=55) for r in out.rca_logs_baseline)


# ── 3. Dead Man's Snitch ──


def _watchdog_pipeline():
    nodes = [NodeSpec(name="payload", operator="alert.notify_payload"),
             NodeSpec(name="watchdog", operator="notifier.watchdog", params={"ttl_minutes": 15})]
    return Pipeline(Workflow(name="wd", nodes=nodes))


def test_watchdog_beats_after_persist_with_ttl(sent):
    before = datetime.now(timezone.utc)
    ctx = _watchdog_pipeline().run(now=T0)
    ends = datetime.fromisoformat(ctx.result["heartbeat"]["ends_at"])
    assert timedelta(minutes=15) <= ends - before <= timedelta(minutes=16)  # 牆上時鐘，不是 ctx.now（回放用的時間）
    assert sent == [] and "heartbeat:log" in ctx.audit


def test_watchdog_posts_to_alertmanager(monkeypatch):
    posted = {}

    def fake_post(url, json, headers, timeout):
        posted.update(url=url, body=json)
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setenv("AIOPS_NOTIFIER", "alertmanager")
    monkeypatch.setenv("AIOPS_ALERTMANAGER_URL", "http://am:9093")
    monkeypatch.setattr(httpx, "post", fake_post)
    _watchdog_pipeline().run(now=T0)
    alert = posted["body"][0]
    assert posted["url"] == "http://am:9093/api/v2/alerts"
    assert alert["labels"] == {"alertname": "AIOpsWatchdog", "pipeline": "wd", "severity": "none", "source": "aiops"}
    assert alert["endsAt"] > alert["startsAt"]


def test_watchdog_stays_silent_when_persist_fails(monkeypatch):
    class BrokenHooks(StorageHooks):
        def persist_batch(self, ctx):
            raise InfrastructureError("db down")

    beats = []
    monkeypatch.setattr(OperatorRegistry.get("notifier.watchdog"), "heartbeat",
                        lambda self, ctx, now: beats.append(now))
    p = _watchdog_pipeline()
    p.hooks = BrokenHooks()
    with pytest.raises(InfrastructureError):
        p.run(now=T0)
    assert beats == []


def test_after_persist_nodes_must_come_last():
    with pytest.raises(WorkflowError, match="after_persist"):
        Pipeline(Workflow(name="bad", nodes=[NodeSpec(name="watchdog", operator="notifier.watchdog"),
                                             NodeSpec(name="payload", operator="alert.notify_payload")]))


def test_every_detection_workflow_ends_with_the_watchdog():
    for name in ("it_otel", "it_apm", "factory", "metrics_only"):
        assert load_workflow(workflow_path(name)).nodes[-1].operator == "notifier.watchdog", name
