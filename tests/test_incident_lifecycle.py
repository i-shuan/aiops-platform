"""告警生命週期：一場事故從初發到結案只有一個 incident_uid、一張動態卡。

用真的 Pipeline + StorageHooks（SQLite），每 5 分鐘跑一輪，異常點由測試直接餵（等於排程每輪偵測到的結果）。
「Alertmanager 上的卡」用它的規則算：整組 labels 相同 = 同一則；帶 endsAt 的那次 = 關掉。
"""
from datetime import datetime, timedelta, timezone

import pytest

from aiops.engine.context import OpenIncident, PipelineContext
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import WARNING, AnomalyEvent, Incident
from aiops.interfaces.runner import workflow_path
from aiops.stages.notification.transport import LogTransport
from aiops.storage.hooks import StorageHooks
from aiops.storage.repository import Incidents

T0 = datetime(2026, 11, 3, 14, 0, tzinfo=timezone.utc)
FEED: dict[str, list[AnomalyEvent]] = {}


@OperatorRegistry.register("test.lifecycle_feed")
class Feed(BaseOperator):
    """這一輪 (now-5分, now] 的異常點。"""

    scope = "target"

    def execute(self, ctx):
        lo = ctx.now - timedelta(minutes=5)
        ctx.anomalies = [e for e in FEED.get(ctx.target.entity, []) if lo < e.ts <= ctx.now]
        return ctx


def _workflow(entities=("svc",), topology=None):
    wf = Workflow(name="life", targets=[TargetSpec(source_type="mock", query="q", entity=e) for e in entities], nodes=[
        NodeSpec(name="feed", operator="test.lifecycle_feed"),
        NodeSpec(name="group_entity", operator="alert.same_entity"),
        NodeSpec(name="group_topology", operator="alert.topology_chain"),
        NodeSpec(name="absorb_open", operator="alert.absorb_open"),
        NodeSpec(name="suppress_dedup", operator="alert.suppress_dedup", params={"cooldown_minutes": 30}),
        NodeSpec(name="payload", operator="alert.notify_payload"),
        NodeSpec(name="heartbeat", operator="alert.heartbeat", params={"resolve_after_minutes": 15}),
        NodeSpec(name="notify", operator="notifier.auto")])
    wf.topology = topology
    return wf


def _ev(entity, minute, score=5.0, value=None, level="anomaly"):
    return AnomalyEvent(entity, "latency", T0 + timedelta(minutes=minute), score,
                        value=value if value is not None else 100.0 + score, level=level)


def _replay(minutes, wf=None):
    """跑 0..minutes 分鐘，每 5 分鐘一輪。回傳每輪送出的通知 [(輪次時間, [Notification])]。"""
    wf, hooks, rounds = wf or _workflow(), StorageHooks(), []
    for m in range(5, minutes + 1, 5):
        LogTransport.sent.clear()
        Pipeline(wf, hooks).run(now=T0 + timedelta(minutes=m))
        rounds.append((m, list(LogTransport.sent)))
    return rounds


def _cards(rounds):
    """Alertmanager 的視角：labels 組合 -> 狀態（firing／resolved）與最後一則。"""
    cards = {}
    for _, sent in rounds:
        for n in sent:
            key = tuple(sorted(n.labels.items()))
            cards[key] = ("resolved" if n.ends_at else "firing", n)
    return cards


def _uids(rounds):
    return {n.labels["incident_uid"] for _, sent in rounds for n in sent}


def test_two_hour_incident_is_one_card_with_rolling_summary():
    """中間有一個分數 9.0 的峰值：預設門檻下 5.0 是 P2、9.0 是 P1，所以這場事故會從 P2 升級到 P1 一次。
    同一個 incident_uid；Alertmanager 的標籤組合 P2、P1 各一組，P2 在升級那一輪被關掉，P1 撐到結案。"""
    FEED.clear()
    FEED["svc"] = [_ev("svc", m, value=100 + m) for m in range(1, 121)]  # 每分鐘都異常，持續 2 小時
    FEED["svc"][60] = _ev("svc", 61, score=9.0, value=900.0)  # 中間的峰值
    rounds = _replay(150)
    assert len(_uids(rounds)) == 1  # 只有一個 incident_uid
    cards = _cards(rounds)
    assert sorted((dict(k)["severity"], state) for k, (state, _) in cards.items()) == [("P1", "resolved"), ("P2", "resolved")]
    esc = dict(rounds)[65]  # 峰值那一點（第 61 分）在 65 分那一輪被看到
    assert [(n.labels["severity"], n.ends_at is not None) for n in esc] == [("P2", True), ("P1", False)]
    assert esc[1].annotations["escalation_path"] == "P2 -> P1" and esc[1].annotations["summary"].startswith("[Escalated from P2]")
    starts = {n.starts_at for _, sent in rounds for n in sent}
    assert starts == {T0 + timedelta(minutes=1)}  # 初發時間鎖死（升級後的新標籤也沿用）
    counts = [int(sent[-1].annotations["event_count"]) for _, sent in rounds if sent]
    assert counts == sorted(counts) and counts[0] == 5 and counts[-1] == 120  # 每輪累計
    mid = dict(rounds)[30][0].annotations
    assert mid["duration_minutes"] == "29" and "已持續 29 分鐘，累計 30 筆" in mid["summary"]
    final = [n for _, sent in rounds for n in sent if n.ends_at][-1]  # 最後一則結案（P1 那組）
    a = final.annotations
    assert final.labels["severity"] == "P1"  # 分數後來回到 5.0 也不降級
    assert (a["first_value"], a["peak_value"], a["latest_value"], a["event_count"]) == ("101", "900", "220", "120")
    assert "已恢復" in a["summary"] and "起跳 101 → 峰值 900" in a["description"]
    assert final.ends_at == T0 + timedelta(minutes=135)  # 最後一筆 120 分 + 安靜 15 分
    rows = Incidents.created_since(datetime(2000, 1, 1))
    assert [r["status"] for r in rows] == ["resolved"]  # 資料庫也只有一筆事件群（不再一輪一筆被冷卻擋下的）


def test_state_does_not_leak_between_rounds_of_a_long_running_loop():
    """常駐迴圈：同一個 Pipeline、同一個 hooks 跑很多輪。incidents_built 是每輪新建的 PipelineContext 的欄位，
    不是類別或模組層級的狀態；第 3 輪才出現的無關實體照樣要建出自己的事件單，不會被前面輪次的旗標吃掉。"""
    FEED.clear()
    FEED["a"] = [_ev("a", m) for m in range(1, 21)]
    FEED["z"] = [_ev("z", m) for m in range(11, 14)]  # 第 3 輪（10 < t <= 15）才出現，跟 a 不相鄰
    wf, hooks = _workflow(("a", "z")), StorageHooks()
    pipeline = Pipeline(wf, hooks)  # 同一個物件跑到底，不重新建立
    seen, contexts = [], []
    for m in range(5, 41, 5):
        LogTransport.sent.clear()
        ctx = pipeline.run(now=T0 + timedelta(minutes=m))
        contexts.append(ctx)
        seen.append({(n.labels["entities"], n.labels["incident_uid"]) for n in LogTransport.sent})
    assert len({id(c) for c in contexts}) == len(contexts)  # 每輪都是新的 context
    assert all(c.incidents_built for c in contexts if c.anomalies or c.open_incidents)
    by_entity = {}
    for round_ in seen:
        for entity, uid in round_:
            by_entity.setdefault(entity, set()).add(uid)
    assert {e: len(u) for e, u in by_entity.items()} == {"a": 1, "z": 1}  # 各自一張卡，z 沒有被吃掉
    assert PipelineContext(pipeline="p", now=T0).incidents_built is False  # 新 context 的預設值


def test_flapping_below_quiet_window_never_resolves_in_between():
    FEED.clear()
    # 異常 10 分鐘、回落 10 分鐘、異常 10 分鐘、回落 12 分鐘、異常 5 分鐘：每次回落都短於 15 分鐘
    minutes = [*range(1, 11), *range(21, 31), *range(43, 48)]
    FEED["svc"] = [_ev("svc", m) for m in minutes]
    rounds = _replay(80)
    resolved_at = [m for m, sent in rounds for n in sent if n.ends_at]
    assert len(_uids(rounds)) == 1 and resolved_at == [65]  # 中間沒有結案，最後一筆 47 分 + 15 分後那一輪才結案
    assert sum(1 for _, sent in rounds for n in sent if n.ends_at) == 1


def test_quiet_longer_than_window_resolves_then_a_new_spike_is_a_new_card():
    FEED.clear()
    FEED["svc"] = [_ev("svc", m) for m in (*range(1, 6), *range(60, 65))]
    rounds = _replay(90)
    assert len(_uids(rounds)) == 2  # 安靜 55 分鐘 = 真的恢復；之後是另一場（冷卻 30 分鐘從最後一筆 5 分算，60 分已過）


def test_escalation_closes_the_old_labels_and_keeps_one_incident():
    FEED.clear()
    FEED["svc"] = [_ev("svc", m, score=4.0) for m in range(1, 21)] + [_ev("svc", m, score=12.0) for m in range(21, 41)] \
        + [_ev("svc", m, score=4.0) for m in range(41, 51)]
    rounds = dict(_replay(80))
    assert {n.labels["severity"] for n in rounds[5]} == {"P2"}
    esc = rounds[25]  # 21 分起分數衝到 12
    old = [n for n in esc if n.labels["severity"] == "P2"]
    new = [n for n in esc if n.labels["severity"] == "P0"]
    assert len(old) == 1 and old[0].ends_at == T0 + timedelta(minutes=25)  # 舊標籤立即關掉
    assert len(new) == 1 and new[0].ends_at is None and new[0].starts_at == T0 + timedelta(minutes=1)
    assert esc.index(old[0]) < esc.index(new[0])  # 先關舊、再開新
    assert old[0].labels["incident_uid"] == new[0].labels["incident_uid"]
    assert new[0].annotations["summary"].startswith("[Escalated from P2] [P0]")
    assert new[0].annotations["escalation_path"] == "P2 -> P0"
    later = rounds[45]  # 分數回到 4：不降級，P2 不會再亮
    assert [n.labels["severity"] for n in later] == ["P0"] and "[Escalated from P2]" in later[0].annotations["summary"]
    cards = _cards(rounds.items())
    assert sorted(state for state, _ in cards.values()) == ["resolved", "resolved"]  # P2、P0 兩組 labels，最後都關掉
    assert sum(1 for state, _ in _cards([(m, s) for m, s in rounds.items() if m <= 45]).values() if state == "firing") == 1


def test_warning_becomes_anomaly_in_place():
    FEED.clear()
    FEED["svc"] = [_ev("svc", m, score=2.5, level=WARNING) for m in range(1, 11)] + \
        [_ev("svc", m, score=4.0) for m in range(11, 21)]
    rounds = dict(_replay(40))
    assert [(n.labels["severity"], n.labels["level"]) for n in rounds[5]] == [("warning", "warning")]
    sent = rounds[15]
    assert [(n.labels["severity"], n.labels["level"], n.ends_at is not None) for n in sent] == \
        [("warning", "warning", True), ("P2", "anomaly", False)]
    assert len({n.labels["incident_uid"] for n in sent}) == 1


def test_absorbing_a_new_entity_supersedes_the_old_label_set():
    FEED.clear()
    FEED["a"] = [_ev("a", m) for m in range(1, 31)]
    FEED["b"] = [_ev("b", m) for m in range(12, 31)]  # 12 分起 b 也出事，跟 a 相鄰
    rounds = dict(_replay(30, _workflow(("a", "b"), topology=[("a", "b")])))
    sent = rounds[15]
    assert [(n.labels["entities"], n.ends_at is not None) for n in sent] == [("a", True), ("a, b", False)]
    assert len(_uids(rounds.items())) == 1


# ── 算子單元 ──


def _open(entity, minutes, uid="inc-open"):
    inc = Incident([_ev(entity, m) for m in minutes], score=5.0, uid=uid)
    return OpenIncident(1, uid, "life", "P2", inc)


def test_absorb_open_merges_overlap_dedupes_and_keeps_the_rest():
    o = _open("a", [1, 2])
    ctx = PipelineContext(pipeline="life", now=T0 + timedelta(minutes=10))
    ctx.open_incidents = [o]
    ctx.incidents = [Incident([_ev("a", 2), _ev("a", 3, score=8.0)], score=8.0), Incident([_ev("z", 3)], score=5.0)]
    out = OperatorRegistry.create("alert.absorb_open").execute(ctx)
    assert [i.entities for i in out.incidents] == [["z"]]
    assert [e.ts.minute for e in o.incident.events] == [1, 2, 3] and o.incident.score == 8.0 and o.incident.uid == "inc-open"
    assert o.changed and [e.ts.minute for e in o.previous[0].events] == [1, 2] and o.previous[1] == "P2"


def test_absorb_open_is_a_noop_without_open_incidents():
    ctx = PipelineContext(pipeline="life", now=T0)
    ctx.incidents = [Incident([_ev("a", 1)])]
    assert len(OperatorRegistry.create("alert.absorb_open").execute(ctx).incidents) == 1


@pytest.mark.parametrize("name", ["it_otel", "it_apm", "factory", "metrics_only"])
def test_builtin_workflows_order_absorb_and_heartbeat(name):
    ops = [n.operator for n in load_workflow(workflow_path(name)).nodes]
    absorb = ops.index("alert.absorb_open")
    assert all(i < absorb for i, op in enumerate(ops) if op.startswith(("alert.same_entity", "alert.topology_chain",
                                                                          "alert.lot_path_chain", "alert.golden_signals",
                                                                          "alert.p0_boost")))
    assert all(i > absorb for i, op in enumerate(ops) if op.startswith("alert.suppress_"))
    assert ops.index("alert.heartbeat") > ops.index("alert.notify_payload")
