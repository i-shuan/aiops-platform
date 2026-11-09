"""三個穩定性防呆（docs/PRODUCTION_READINESS.md 第二節查出來的）：

1. 資料斷流不准送心跳：一條 workflow 的序列全部（或超過 max_failed_target_ratio）抓不到資料，這一輪是基礎設施錯誤，
   watchdog 不續命。查詢寫錯（BadRequest）是設定問題，不算斷流。
2. 先寫資料庫、再送通知，寫入是單一交易並重試：推送失敗時事件群已存成進行中，下一輪心跳會補送；
   結案那一輪推送失敗，事件群不會被標成已結案，下一輪會再送一次 endsAt。
3. 冷啟動資料太少就不學：訓練點數 < min_train_points 的偵測節點不訓練、不偵測、不投票；靜態規則不受影響。

每個測試在修正前都會失敗（第 2 項尤其：修正前 11:40 的異常在推送失敗後永遠不會發出）。
"""
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from sqlalchemy.exc import OperationalError

from aiops.engine.context import PipelineContext, Target
from aiops.engine.errors import InfrastructureError
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent
from aiops.interfaces.runner import workflow_path
from aiops.sources.base import InvalidQueryError, NoDataError
from aiops.sources.mock import MockSpec
from aiops.storage import repository
from aiops.storage.hooks import StorageHooks
from aiops.storage.repository import Anomalies, Incidents, Notifications

T0 = datetime(2026, 11, 8, 11, 0, tzinfo=timezone.utc)
SPIKE_AT = T0 + timedelta(minutes=40)


@pytest.fixture
def beats(monkeypatch):
    """記下 watchdog 真的送了幾次心跳（不管出口是 log 還是 alertmanager）。"""
    out: list = []
    cls = OperatorRegistry.get("notifier.watchdog")
    real = cls.heartbeat
    monkeypatch.setattr(cls, "heartbeat", lambda self, ctx, now: (out.append(now), real(self, ctx, now))[1])
    return out


@pytest.fixture
def flaky_source(monkeypatch):
    """mock 資料源加兩種查詢：empty:* 沒資料（NoDataError）、bad:* 查詢寫錯（InvalidQueryError）。"""
    real = MockSpec.fetch_data

    def fetch(self, query, start, end, step_seconds, field_map=None):
        if query.startswith("empty:"):
            raise NoDataError(f"mock 在這段時間沒有資料：{query}")
        if query.startswith("bad:"):
            raise InvalidQueryError(f"查詢寫錯：{query}")
        return real(self, query, start, end, step_seconds, field_map)

    monkeypatch.setattr(MockSpec, "fetch_data", fetch)


def _detection(targets, **workflow_fields):
    wf = load_workflow(workflow_path("metrics_only"))
    data = wf.model_dump() | {"targets": [TargetSpec(source_type="mock", query=q, entity=e) for q, e in targets]} | workflow_fields
    return Pipeline(Workflow.model_validate(data), StorageHooks())


# ── 1. 資料斷流 ──


def test_all_targets_without_data_is_starvation_and_no_heartbeat(flaky_source, beats):
    with pytest.raises(InfrastructureError, match="資料斷流") as e:
        _detection([("empty:a", "a"), ("empty:b", "b")]).run(now=T0)
    assert "2/2" in str(e.value) and "a" in str(e.value) and beats == []


def test_some_targets_without_data_is_only_errors_and_heartbeat_goes_out(flaky_source, beats):
    ctx = _detection([("empty:a", "a"), ("metric_b", "b")]).run(now=T0)
    assert [e.entity for e in ctx.errors] == ["a"] and len(beats) == 1


def test_starvation_ratio_is_configurable(flaky_source, beats):
    with pytest.raises(InfrastructureError, match="資料斷流"):
        _detection([("empty:a", "a"), ("metric_b", "b")], max_failed_target_ratio=0.5).run(now=T0)
    assert beats == []


def test_bad_queries_are_configuration_errors_not_starvation(flaky_source, beats):
    ctx = _detection([("bad:a", "a"), ("bad:b", "b")]).run(now=T0)
    assert len(ctx.errors) == 2 and len(beats) == 1  # 查詢寫錯不是資料源斷流：記錯誤，但這一輪算成功


def test_workflow_without_targets_does_not_divide_by_zero(beats):
    ctx = _detection([]).run(now=T0, targets=[])
    assert ctx.errors == [] and len(beats) == 1


def test_bad_ratio_is_rejected_at_load():
    with pytest.raises(Exception):
        _detection([("metric_a", "a")], max_failed_target_ratio=1.5)


# ── 2. 先寫資料庫、再送通知；單一交易 ──


@OperatorRegistry.register("test.fixed_spike")
class FixedSpike(BaseOperator):
    """每一輪往回看 60 分鐘都會「再偵測到」同一個時間點的異常（真實偵測就是這樣；靠 detector.new_only 去重）。"""

    scope = "target"

    def execute(self, ctx):
        if ctx.now - timedelta(minutes=60) <= SPIKE_AT <= ctx.now:
            ctx.anomalies = [AnomalyEvent(ctx.target.entity, "q", SPIKE_AT, 8.0)]
        return ctx


def _spike_pipeline(resolve_after_minutes=15):
    wf = Workflow(name="spike", targets=[TargetSpec(source_type="mock", query="q", entity="svc")], nodes=[
        NodeSpec(name="detect", operator="test.fixed_spike"),
        NodeSpec(name="new_only", operator="detector.new_only"),
        NodeSpec(name="group", operator="alert.same_entity"),
        NodeSpec(name="absorb_open", operator="alert.absorb_open"),
        NodeSpec(name="dedup", operator="alert.suppress_dedup"),
        NodeSpec(name="payload", operator="alert.notify_payload"),
        NodeSpec(name="heartbeat", operator="alert.heartbeat", params={"resolve_after_minutes": resolve_after_minutes}),
        NodeSpec(name="notify", operator="notifier.auto")])
    return Pipeline(wf, StorageHooks())


def _push_fails(monkeypatch):
    monkeypatch.setenv("AIOPS_NOTIFIER", "alertmanager")
    monkeypatch.setenv("AIOPS_ALERTMANAGER_URL", "http://127.0.0.1:1")  # 沒人在聽


def _push_ok(monkeypatch):
    monkeypatch.setenv("AIOPS_NOTIFIER", "log")


def test_alert_is_delivered_after_the_first_push_fails(monkeypatch, sent):
    _push_fails(monkeypatch)
    with pytest.raises(InfrastructureError):
        _spike_pipeline().run(now=SPIKE_AT + timedelta(minutes=5))
    # 寫在送之前：事件群已經存成進行中；但「已通知」紀錄還沒有，因為根本沒送出去
    assert len(Incidents.list_open("spike")) == 1 and Notifications.since(T0.replace(tzinfo=None)) == []

    _push_ok(monkeypatch)
    _spike_pipeline().run(now=SPIKE_AT + timedelta(minutes=10))
    assert [n.labels["entities"] for n in sent] == ["svc"] and sent[0].ends_at is None  # 下一輪補送了
    assert len(Incidents.list_open("spike")) == 1  # 同一張卡，沒有重開
    assert len(Notifications.since(T0.replace(tzinfo=None))) == 1
    assert len(Anomalies.entity_last_seen("spike", ["svc"])) == 1  # 異常也只存了一次


def test_resolution_is_resent_when_its_push_fails(monkeypatch, sent):
    _push_ok(monkeypatch)
    _spike_pipeline().run(now=SPIKE_AT + timedelta(minutes=5))
    sent.clear()
    _push_fails(monkeypatch)
    with pytest.raises(InfrastructureError):  # 安靜 20 分鐘 -> 這一輪要送 endsAt，但送不出去
        _spike_pipeline().run(now=SPIKE_AT + timedelta(minutes=20))
    row = Incidents.list_open("spike")
    assert len(row) == 1 and row[0]["status"] == "open"  # 結案通知沒送到，就不能算已結案

    _push_ok(monkeypatch)
    ctx = _spike_pipeline().run(now=SPIKE_AT + timedelta(minutes=25))
    assert [n.ends_at is not None for n in sent] == [True] and ctx.resolved_incident_ids
    assert Incidents.list_open("spike") == []


def test_batch_persist_is_one_transaction_that_retries(monkeypatch, sent, db_retry_waits):
    _push_ok(monkeypatch)
    real = repository.Incidents.create
    calls = {"n": 0}

    def flaky_create(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OperationalError("INSERT", {}, Exception("connection reset"))
        return real(*a, **kw)

    monkeypatch.setattr(repository.Incidents, "create", staticmethod(flaky_create))
    ctx = _spike_pipeline().run(now=SPIKE_AT + timedelta(minutes=5))
    assert calls["n"] == 2 and db_retry_waits == [0.5]
    assert len(Incidents.list_open("spike")) == 1 and len(ctx.incident_ids) == 1  # 重試沒有寫兩份、ctx 沒被第一次失敗污染
    assert len(Anomalies.entity_last_seen("spike", ["svc"])) == 1 and len(sent) == 1


def test_batch_persist_gives_up_cleanly_when_the_database_stays_down(monkeypatch, sent, db_retry_waits):
    _push_ok(monkeypatch)

    def broken(*a, **kw):
        raise OperationalError("INSERT", {}, Exception("db gone"))

    monkeypatch.setattr(repository.Incidents, "create", staticmethod(broken))
    with pytest.raises(InfrastructureError, match="資料庫"):
        _spike_pipeline().run(now=SPIKE_AT + timedelta(minutes=5))
    assert db_retry_waits == [0.5, 1.5]
    # 單一交易：異常與事件群一起回復，沒有半套資料；通知也沒送（notifier 在寫入之後）
    assert Anomalies.entity_last_seen("spike", ["svc"]) == {} and Incidents.list_open("spike") == [] and sent == []


# ── 3. 冷啟動資料太少就不學 ──


def _ctx(raw, train=None):
    ctx = PipelineContext(pipeline="p", now=T0, target=Target("mock", "q", "svc"))
    ctx.raw_df, ctx.train_df = raw, train
    return ctx


def _frame(n, spike=None):
    values = [50.0 + (i % 3) for i in range(n)]
    if spike is not None:
        values[-1] = spike
    return pd.DataFrame({"ts": pd.date_range(T0, periods=n, freq="60s"), "value": values})


def test_too_few_training_points_means_no_training_no_votes_no_alerts():
    ctx = OperatorRegistry.create("detector.zscore").execute(_ctx(_frame(20, spike=500.0)))
    assert ctx.detector_runs == [] and ctx.new_detector_states == {} and ctx.anomalies == []
    assert any("too-few-points:20<60" in a for a in ctx.audit)
    ctx = OperatorRegistry.create("detector.zscore").execute(_ctx(_frame(20, spike=500.0), train=_frame(45)))
    assert ctx.detector_runs == [] and any("too-few-points:45<60" in a for a in ctx.audit)


def test_min_train_points_is_a_node_param_with_default_60():
    ok = OperatorRegistry.create("detector.zscore", {"min_train_points": 10}).execute(_ctx(_frame(20, spike=500.0)))
    assert ok.detector_runs == ["detector.zscore"] and ok.anomalies
    assert OperatorRegistry.create("detector.zscore").execute(_ctx(_frame(60, spike=500.0))).detector_runs
    with pytest.raises(TypeError, match="行為參數"):
        OperatorRegistry.create("detector.zscore").check_override({"min_train_points": 5})


def test_static_rules_do_not_need_training_and_still_fire_on_one_point():
    ctx = OperatorRegistry.create("detector.rules", {"upper": 80}).execute(_ctx(_frame(1, spike=500.0)))
    assert ctx.detector_runs == ["detector.rules"] and len(ctx.anomalies) == 1


def test_abstaining_detector_is_not_counted_by_the_ensemble():
    ctx = _ctx(_frame(20, spike=500.0))
    for node, op, params in (("z_cold", "detector.zscore", {}), ("rules", "detector.rules", {"upper": 80})):
        o = OperatorRegistry.create(op, params)
        o.node_name = node
        ctx = o.execute(ctx)
    vote = OperatorRegistry.create("detector.ensemble", {"strategy": "majority"})
    vote.node_name = "vote"
    out = vote.execute(ctx)
    assert ctx.detector_runs == ["rules"] and [e.voted_by for e in out.anomalies] == [("rules",)]  # 1 個參與者就它說了算
