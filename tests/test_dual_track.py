"""雙軌制偵測（docs/DUAL_TRACK_DESIGN.md 方案 B）。

軌道一 detector.hard_limit：致命硬上限，單點成立、不投票（detector.ensemble 的 bypass）、alert.fatal_floor 直接拉到 P0。
軌道二：hourly_baseline＋MAD＋quantile_drift 多數決，組成與分母不變。

整合測試用「出貨的 factory.yaml」（統計軌是上面那三票的布林多數決）：只把 fetch 換成固定資料、targets 換成測試序列，
其餘節點與參數原封不動。以前用 it_otel.yaml；it_otel 的統計軌改成 R+SNR（殘差三票＋軟投票）之後，
「單點被 3 過 2 吃掉」的前提不再成立（軟投票下一票夠強就單獨成立），所以布林多數決的雙軌測試改掛 factory，
it_otel 的雙軌接線另外測（test_it_otel_*）。
資料是重尾的：訓練段 N(1000, 20) 混 2% 的 ±400 離群點——標準差被拉到約 60、MAD 不受影響，
所以 +150 的單點只有 MAD 一票（z≈2.5 < 3），正好是「被 3 過 2 多數決吃掉」的形狀。
"""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from aiops.control_plane.learning.tuning import _check_params, _create_proposal
from aiops.engine.context import PipelineContext, RecentNotification, Silence, Target
from aiops.engine.errors import BadRequest, WorkflowError
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, NullHooks, Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.interfaces.runner import workflow_path
from aiops.stages.detectors.hard_limit import HardLimitDetector
from aiops.stages.ingestion.fetch import FetchOperator
from aiops.stages.scoring.score import FatalFloorScorer
from aiops.stages.suppression.suppress import DedupSuppressor, SameRootCauseSuppressor, SilenceWindowSuppressor
from aiops.storage.hooks import StorageHooks

T0 = datetime(2026, 11, 3, 14, 0, tzinfo=timezone.utc)
WINDOW = 60  # 每輪看最近 60 分鐘
BASE = 1000.0


@OperatorRegistry.register("test.dual_frame")
class _Frame(BaseOperator):
    """固定資料。值只跟「距離 T0 第幾分鐘」有關（跨輪次同一分鐘永遠是同一個值），points 是要注入的單點：
    {實體: {分鐘: 值}}。訓練段 2 天：N(1000, 20) 混 2% 的 ±400 離群點（重尾）。"""

    def __init__(self, seed: int = 0, points: dict | None = None):
        self.seed, self.points = int(seed), points or {}

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        rng = np.random.default_rng(self.seed)
        train = BASE + rng.normal(0, 20, 2880)
        tail = rng.random(2880) < 0.02
        train[tail] += rng.choice([-400.0, 400.0], int(tail.sum()))
        live = BASE + np.clip(rng.normal(0, 20, 1440), -45, 45)  # 測試段：乾淨的常態雜訊
        end = int((ctx.now - T0).total_seconds() // 60)
        minutes = list(range(end - WINDOW + 1, end + 1))
        inject = {int(k): v for k, v in self.points.get(ctx.target.entity, {}).items()}
        values = [float(inject.get(m, live[m % 1440])) for m in minutes]
        ctx.train_df = pd.DataFrame({"ts": pd.date_range(T0 - timedelta(days=3), periods=2880, freq="60s"), "value": train})
        ctx.raw_df = pd.DataFrame({"ts": [T0 + timedelta(minutes=m) for m in minutes], "value": values})
        return ctx


@OperatorRegistry.register("test.dual_fetch_probe")
class _FetchProbe(_Frame):
    """跟 ingestion.fetch 用同一個判斷（FetchOperator._needs_training）決定這一輪給不給訓練窗，並把結果記進 audit。"""

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        ctx = super().execute(ctx)
        needed = FetchOperator(train_lookback_minutes=2880)._needs_training(ctx)
        if not needed:
            ctx.train_df = None
        ctx.log(f"probe:train={needed}:{','.join(ctx.baseline_nodes)}")
        return ctx


def _shipped(fatal: dict | None, *, seed=0, points=None, entity="ETCH-01", dual=True, name="sample_factory",
             fetch="test.dual_frame") -> Workflow:
    """出貨的 workflow，fetch 換成固定資料。dual=False：把雙軌制的節點與參數拿掉（等於改動前的 workflow）。"""
    wf = load_workflow(workflow_path(name))
    nodes = []
    for n in wf.nodes:
        if n.operator == "ingestion.fetch":
            n = NodeSpec(name=n.name, operator=fetch, params={"seed": seed, "points": points or {}})
        if not dual:
            if n.operator in ("detector.hard_limit", "alert.fatal_floor"):
                continue
            drop = {"bypass", "escalate_tags", "exempt_tags"}
            n = NodeSpec(name=n.name, operator=n.operator, params={k: v for k, v in n.params.items() if k not in drop})
        nodes.append(n)
    target = {"source_type": "mock", "query": "rf_power", "entity": entity, "attrs": {"signal": "saturation"}}
    if fatal and dual:
        target["overrides"] = {"fatal": fatal}
    return Workflow(name=wf.name, nodes=nodes, targets=[TargetSpec(**target)])


def _run(wf, minute=WINDOW, hooks=None):
    return Pipeline(wf, hooks or NullHooks()).run(now=T0 + timedelta(minutes=minute))


def _at(events, minute, operator=None):
    ts = T0 + timedelta(minutes=minute)
    return [e for e in events if e.ts == ts and (operator is None or e.detector == operator)]


def _vote_line(ctx):
    return next(line for line in ctx.audit if line.startswith("ensemble:vote:"))


# 單點事件的四種形狀：(名稱, 注入的值, 界線)。值都落在統計多數決判不出來的範圍（z≈2.5，只有 MAD 一票）
SINGLE_POINTS = [
    ("微電弧-絕對上限", BASE + 150, {"fatal_ceiling": 1100}),
    ("微電弧-瞬間上升", BASE + 150, {"max_rise": 100}),
    ("瞬斷-絕對下限", BASE - 150, {"fatal_lower": 900}),
    ("瞬斷-瞬間下降", BASE - 150, {"max_drop": 100}),
]


# ── 缺口與修補 ──


@pytest.mark.parametrize("label,value,limits", SINGLE_POINTS, ids=[s[0] for s in SINGLE_POINTS])
def test_without_the_fatal_track_a_single_point_is_eaten_by_the_majority_vote(sent, label, value, limits):
    """改動前的 workflow：單點只拿到 1/3 票，被多數決否決，沒有任何通知。這就是雙軌制要補的缺口。"""
    ctx = _run(_shipped(None, points={"ETCH-01": {40: value}}, dual=False))
    assert _vote_line(ctx) == "ensemble:vote:majority:voters=3:need=2:1->0"
    assert ctx.anomalies == [] and ctx.notifications == [] and sent == []


@pytest.mark.parametrize("label,value,limits", SINGLE_POINTS, ids=[s[0] for s in SINGLE_POINTS])
def test_single_point_bypasses_the_vote_and_goes_straight_to_p0(sent, label, value, limits):
    ctx = _run(_shipped(limits, points={"ETCH-01": {40: value}}))
    assert _vote_line(ctx) == "ensemble:vote:majority:voters=3:need=2:1->0"  # 統計軌照樣否決，分母還是 3
    assert [(e.detector, e.detector_node) for e in ctx.anomalies] == [("detector.hard_limit", "fatal")]
    assert ctx.anomalies[0].ts == T0 + timedelta(minutes=40) and ctx.anomalies[0].value == value
    (inc,) = ctx.incidents
    assert inc.severity == "P0" and inc.score == 10.0 and "fatal:fatal" in inc.tags and not inc.suppressed_by
    (n,) = sent
    assert n.labels["severity"] == "P0" and n.labels["entities"] == "ETCH-01" and n.ends_at is None
    assert n.annotations["fatal"] == "fatal:fatal"
    assert any(line.startswith("fatal:fatal:ETCH-01:") for line in ctx.audit)


def test_single_point_triggers_every_time_across_seeds_positions_and_shapes():
    """100% 觸發：4 種形狀 × 20 組雜訊 × 3 個位置（含「剛剛發生」的最後一點）= 240 次，每一次都要是 P0 通知，
    而且每一次統計軌都沒有判它異常（不是剛好統計也抓到）。"""
    runs = hits = vetoed = 0
    for _label, value, limits in SINGLE_POINTS:
        for seed in range(20):
            for minute in (12, 37, WINDOW):
                ctx = _run(_shipped(limits, seed=seed, points={"ETCH-01": {minute: value}}))
                runs += 1
                p0 = [n for n in ctx.notifications if n.labels["severity"] == "P0" and "fatal" in n.annotations]
                hits += len(p0) == 1 and bool(_at(ctx.anomalies, minute, "detector.hard_limit"))
                vetoed += not _at(ctx.anomalies, minute, "detector.ensemble")
    assert (runs, hits, vetoed) == (240, 240, 240)


def test_statistical_track_is_identical_with_and_without_the_fatal_node():
    """軌道二不受影響：同一份資料（含一段 15 分鐘的真實位移＋一個單點），有沒有 fatal 節點，
    裁決事件逐筆相同、分母都是 3。"""
    compared = 0
    for seed in range(10):
        points = {"ETCH-01": {**{m: BASE + 400 for m in range(20, 35)}, 50: BASE + 150}}
        before = _run(_shipped(None, seed=seed, points=points, dual=False))
        after = _run(_shipped({"fatal_ceiling": 1100}, seed=seed, points=points))
        key = lambda e: (e.entity, e.ts, round(e.score, 9), e.value, e.voted_by, e.level)  # noqa: E731
        stat_before = sorted(key(e) for e in before.anomalies)
        stat_after = sorted(key(e) for e in after.anomalies if e.detector == "detector.ensemble")
        assert stat_before == stat_after and len(stat_before) >= 15
        assert _vote_line(before) == _vote_line(after) and "voters=3:need=2" in _vote_line(after)
        assert {e.voted_by for e in after.anomalies if e.detector == "detector.ensemble"} <= {
            ("detect", "detect_mad"), ("detect", "detect_mad", "detect_qdrift"), ("detect", "detect_qdrift"),
            ("detect_mad", "detect_qdrift")}  # fatal 從來不是投票者
        compared += len(stat_before)
    assert compared >= 150


def test_targets_without_limits_are_untouched_and_the_node_abstains():
    """預設關閉：沒在 overrides.fatal 訂界線的序列，fatal 節點棄權（不進 detector_runs、不產生事件）。"""
    ctx = _run(_shipped(None, points={"ETCH-01": {40: BASE + 150}}))
    assert "detect:fatal:cannot-evaluate:abstain" in ctx.audit
    assert _vote_line(ctx) == "ensemble:vote:majority:voters=3:need=2:1->0"
    assert ctx.anomalies == [] and ctx.notifications == []


@pytest.mark.parametrize("fatal", [None, {"fatal_ceiling": 1100}], ids=["沒訂界線（棄權）", "有界線"])
def test_fatal_node_does_not_make_the_statistical_detectors_retrain_every_round(fatal):
    """真實資料回放抓到的回歸（11/3：75 個異常點掉到 51 個）：fatal 節點不學習、棄權時也不存基準線，
    如果引擎把它算進 ctx.baseline_nodes，ingestion.fetch 會每一輪都抓訓練窗、三個統計偵測器每一輪都重訓，
    把正在發生的事故學成常態。訓練窗只該在第一輪（還沒有基準線）抓一次。"""
    points = {"ETCH-01": {m: BASE + 400 for m in range(50, 80)}}
    pipeline = Pipeline(_shipped(fatal, points=points, fetch="test.dual_fetch_probe"), StorageHooks())
    probes, detected = [], set()
    for minute in (60, 65, 70, 75):
        ctx = pipeline.run(now=T0 + timedelta(minutes=minute))
        probes += [line for line in ctx.audit if line.startswith("probe:")]
        detected |= {e.ts for e in ctx.detected_anomalies if e.detector == "detector.ensemble"}
    nodes = "detect,detect_mad,detect_qdrift"
    assert probes == [f"probe:train=True:{nodes}"] + [f"probe:train=False:{nodes}"] * 3
    # 基準線停在事故前：事故期間 26 分鐘（第 50～75 分）每一分鐘都還是異常，沒有被重訓吃掉
    assert detected >= {T0 + timedelta(minutes=m) for m in range(50, 76)}


@pytest.mark.parametrize("label,value,limits", SINGLE_POINTS, ids=[s[0] for s in SINGLE_POINTS])
def test_it_otel_soft_vote_also_misses_the_single_point_and_the_fatal_track_catches_it(label, value, limits):
    """it_otel 的統計軌是 R+SNR（殘差三票＋軟投票）：同樣的單點（偏離約 2.5 個尺度），軟投票加總也到不了 2 票等值；
    致命軌照樣單點成立、直接 P0。5 組雜訊都一樣。"""
    for seed in range(5):
        before = _run(_shipped(None, seed=seed, points={"ETCH-01": {40: value}}, name="sample_it_otel"))
        assert not _at(before.anomalies, 40) and before.notifications == []
        assert any(line.startswith("soft_vote:vote:voters=3:need=2:") for line in before.audit)
        after = _run(_shipped(limits, seed=seed, points={"ETCH-01": {40: value}}, name="sample_it_otel"))
        assert not _at(after.anomalies, 40, "detector.soft_vote")
        assert [(e.detector, e.detector_node) for e in _at(after.anomalies, 40)] == [("detector.hard_limit", "fatal")]
        assert [n.labels["severity"] for n in after.notifications] == ["P0"]


@pytest.mark.parametrize("name", ["sample_it_otel", "sample_factory"])
def test_shipped_workflows_wire_the_fatal_track(name):
    ctx = _run(_shipped({"fatal_ceiling": 1100}, points={"ETCH-01": {40: BASE + 150}}, name=name))
    assert [n.labels["severity"] for n in ctx.notifications] == ["P0"]


# ── 生命週期：升級、冷卻、抑制 ──


def test_open_incident_escalates_to_p0_in_place_when_the_limit_is_hit(sent):
    """進行中的統計事件群（不到 P0）吸收到致命事件：同一個 incident_uid 原地升級，舊標籤關掉、新標籤 P0。
    出貨的門檻下 11 分鐘的位移本身就會累積到 P0，所以這裡把 P0 門檻拉高到 1000（統計事件停在 P1）；
    alert.fatal_floor 跟著 payload 的門檻走，致命事件一樣直接到 P0。"""
    points = {"ETCH-01": {**{m: BASE + 300 for m in range(50, 61)}, 63: BASE + 700}}
    wf, hooks = _shipped({"fatal_ceiling": 1600}, points=points), StorageHooks()
    for n in wf.nodes:
        if n.operator == "alert.notify_payload":
            n.params["severity_thresholds"] = {"P0": 1000, "P1": 6, "P2": 3, "P3": 0}
    first = _run(wf, 60, hooks)
    (opened,) = sent
    assert opened.labels["severity"] == "P1" and "fatal" not in opened.annotations
    assert not any(e.detector == "detector.hard_limit" for e in first.detected_anomalies)
    sent.clear()
    _run(wf, 65, hooks)
    closed, escalated = sent
    assert closed.ends_at is not None and closed.labels == opened.labels
    assert escalated.labels["severity"] == "P0" and escalated.ends_at is None
    assert escalated.labels["incident_uid"] == opened.labels["incident_uid"]
    assert escalated.annotations["summary"].startswith("[Escalated from P1]")
    assert escalated.annotations["escalation_path"] == "P1 -> P0"
    assert escalated.annotations["fatal"] == "fatal:fatal"


def _incident(entity="svc", minute=40, score=4.0, tags=(), node="fatal"):
    ev = AnomalyEvent(entity, "m", T0 + timedelta(minutes=minute), 2.0, value=1.0,
                      detector="detector.hard_limit", detector_node=node)
    return Incident([ev], score, list(tags))


def _batch(*incidents, **kw):
    ctx = PipelineContext(pipeline="p", now=T0 + timedelta(minutes=60))
    ctx.incidents, ctx.incidents_built = list(incidents), True
    for k, v in kw.items():
        setattr(ctx, k, v)
    return ctx


@pytest.mark.parametrize("last,tags,suppressed", [
    ("P3", ["fatal:fatal"], False),  # 才發過 P3：踩到硬上限是升級，照發
    ("P2", ["fatal:fatal"], False),
    ("P0", ["fatal:fatal"], True),   # 已經發過 P0：照常去重，不洗版
    ("P3", [], True),                # 沒有 fatal 標籤的照舊被冷卻擋下
    ("P3", ["fatality"], True),      # 標籤要完全相同或「fatal:」開頭，不是字首比對
])
def test_cooldown_lets_a_fatal_incident_through_unless_p0_was_already_sent(last, tags, suppressed):
    inc = _incident(tags=tags)
    recent = [RecentNotification(inc.fingerprint, T0 + timedelta(minutes=30), "inc-old", last)]
    DedupSuppressor(cooldown_minutes=30, escalate_tags=["fatal"]).execute(_batch(inc, recent_notifications=recent))
    assert bool(inc.suppressed_by) is suppressed


def test_dedup_without_escalate_tags_behaves_as_before():
    inc = _incident(tags=["fatal:fatal"])
    recent = [RecentNotification(inc.fingerprint, T0 + timedelta(minutes=30), "inc-old", "P3")]
    DedupSuppressor(cooldown_minutes=30).execute(_batch(inc, recent_notifications=recent))
    assert inc.suppressed_by.startswith("dedup")


def test_same_root_cause_does_not_swallow_a_fatal_incident():
    """上游踩到致命硬上限、下游分數更高也在告警：沒有 exempt_tags 時上游被當成症狀吃掉；有的話兩張都發。"""
    def pair():
        return _incident("api", score=10.0, tags=["fatal:fatal"]), _incident("db", score=12.0)

    up, down = pair()
    SameRootCauseSuppressor().execute(_batch(up, down, topology=[("api", "db")]))
    assert up.suppressed_by.startswith("same_root_cause") and not down.suppressed_by
    up, down = pair()
    SameRootCauseSuppressor(exempt_tags=["fatal"]).execute(_batch(up, down, topology=[("api", "db")]))
    assert not up.suppressed_by and not down.suppressed_by


def test_maintenance_window_still_silences_a_fatal_incident():
    """靜音／維護窗是人的明確決定：致命事件一樣被抑制（要不要例外由人去掉靜音，不是偵測器自己決定）。"""
    inc = _incident(tags=["fatal:fatal"], score=10.0)
    silence = Silence(id=7, kind="maintenance", entity="svc", dept=None, start=T0, end=T0 + timedelta(hours=2))
    SilenceWindowSuppressor().execute(_batch(inc, silences=[silence]))
    assert inc.suppressed_by == "silence_window：maintenance#7"


def test_fatal_floor_only_raises_and_only_for_its_nodes():
    low, high, other = _incident(score=2.0), _incident("b", score=14.0), _incident("c", score=2.0, node="detect_mad")
    FatalFloorScorer(nodes=["fatal"]).execute(_batch(low, high, other))
    assert (low.score, high.score, other.score) == (10.0, 14.0, 2.0)
    assert low.tags == ["fatal:fatal"] and high.tags == ["fatal:fatal"] and other.tags == []


def test_fatal_floor_follows_the_payload_thresholds_of_its_workflow():
    wf = _shipped({"fatal_ceiling": 1100}, points={"ETCH-01": {40: BASE + 150}})
    for n in wf.nodes:
        if n.operator == "alert.notify_payload":
            n.params["severity_thresholds"] = {"P0": 50, "P1": 6, "P2": 3, "P3": 0}
    ctx = _run(wf)
    assert ctx.incidents[0].score == 50.0 and ctx.incidents[0].severity == "P0"


# ── 載入期防呆：設定錯在啟動時就報，不延到執行期 ──


def _wf(*nodes):
    specs = [NodeSpec(name="fetch", operator="test.dual_frame")]
    specs += [NodeSpec(name=n, operator=o, params=p) for n, o, p in nodes]
    return Workflow(name="t", nodes=specs, targets=[TargetSpec(source_type="mock", query="a", entity="a")])


MADN = ("mad", "detector.mad", {})
FATAL = ("fatal", "detector.hard_limit", {})
PAYLOAD = ("payload", "alert.notify_payload", {})


@pytest.mark.parametrize("nodes,message", [
    ((MADN, FATAL, ("vote", "detector.ensemble", {})), "不參與表決，必須列進 bypass"),
    ((MADN, FATAL, ("vote", "detector.ensemble", {"nodes": ["mad", "fatal"]})), "不參與表決，必須列進 bypass"),
    ((MADN, FATAL, ("vote", "detector.ensemble", {"nodes": ["fatal"], "bypass": ["fatal"]})), "同時寫在 nodes 與 bypass"),
    ((MADN, FATAL, ("vote", "detector.ensemble", {"bypass": ["fatl"]})), "bypass 裡的 \\['fatl'\\] 不是這條 workflow 的偵測節點"),
    ((MADN, ("vote", "detector.ensemble", {"nodes": ["madd"]})), "nodes 裡的 \\['madd'\\] 不是這條 workflow 的偵測節點"),
    ((MADN, ("vote", "detector.ensemble", {"bypass": "mad"})), "bypass 必須是節點名稱的清單"),
    ((FATAL, ("floor", "alert.fatal_floor", {"nodes": ["fatl"]}), PAYLOAD), "nodes 裡的 \\['fatl'\\] 不是這條 workflow 的偵測節點"),
    ((MADN, ("floor", "alert.fatal_floor", {"nodes": ["mad"]}), PAYLOAD), "參與多數決的統計偵測節點，不能當致命節點"),
    ((FATAL, ("floor", "alert.fatal_floor", {"nodes": ["fatal"]})), "沒有 alert.notify_payload"),
    ((FATAL, PAYLOAD, ("floor", "alert.fatal_floor", {"nodes": ["fatal"]})), "必須排在 payload 前面"),
    ((FATAL, ("absorb", "alert.absorb_open", {}), ("floor", "alert.fatal_floor", {"nodes": ["fatal"]}), PAYLOAD),
     "必須排在 absorb 前面"),
    ((FATAL, ("floor", "alert.fatal_floor", {"nodes": ["fatal"], "severity": "P1"}),
      ("payload", "alert.notify_payload", {"severity_thresholds": {"P0": 10, "P3": 0}})), "severity 'P1' 不在 payload"),
    ((FATAL, ("floor", "alert.fatal_floor", {"nodes": ["fatal"], "severity": "SEV1"}), PAYLOAD), "不是已知的嚴重度"),
    ((FATAL, ("floor", "alert.fatal_floor", {"nodes": []}), PAYLOAD), "nodes 必須是偵測節點名稱的清單"),
    ((FATAL, ("dedup", "alert.suppress_dedup", {"escalate_tags": "fatal"})), "escalate_tags 必須是標籤的清單"),
])
def test_wiring_mistakes_fail_when_the_pipeline_is_built(nodes, message):
    with pytest.raises(WorkflowError, match=message):
        Pipeline(_wf(*nodes))


@pytest.mark.parametrize("override,message", [
    ({"fatal_ceiling": -5, "fatal_lower": 0}, "fatal_lower.*必須小於 fatal_ceiling"),
    ({"max_rise": 0}, "max_rise 必須 > 0"),
    ({"max_drop": -3}, "max_drop 必須 > 0"),
    ({"max_deviation_ratio": -3}, "max_deviation_ratio 必須 > 0"),
    ({"consecutive": 0}, "consecutive 必須是 >= 1 的整數"),
    ({"deviation_window": 2}, "deviation_window 必須是 >= 5 的整數"),
    ({"fatal_ceiling": "high"}, "fatal_ceiling 必須是有限的數值"),
    ({"fatal_ceiling": 2, "plausible_max": 1}, "fatal_ceiling.*必須小於 plausible_max"),
    ({"fatal_ceilling": 2}, "fatal_ceilling"),
    ({"threshold": 3}, "threshold"),
])
def test_bad_limits_in_target_overrides_fail_at_load(override, message):
    wf = _wf(FATAL, ("vote", "detector.ensemble", {"bypass": ["fatal"]}))
    wf.targets = [TargetSpec(source_type="mock", query="a", entity="a", overrides={"fatal": override})]
    with pytest.raises(WorkflowError, match=message):
        Pipeline(wf)


# ── 隔離：後台調參、規則升級器都改不到致命界線 ──


def test_tuning_proposals_for_the_hard_limit_are_rejected_from_every_source():
    with pytest.raises(BadRequest, match="不接受調參提案"):
        _check_params("detector.hard_limit", {"fatal_ceiling": 99999})
    for source in ("manual", "auto", "llm", "rule_promoter"):
        with pytest.raises(BadRequest, match="不接受調參提案"):
            _create_proposal(source, "detector.hard_limit", {"fatal_ceiling": 99999}, "放寬", "someone")


def test_approved_tuning_already_in_the_database_is_ignored():
    """就算資料庫裡已經有一筆核准調參（舊資料、繞過 API 寫進去的），執行時也不採用，並留下紀錄。"""

    class Tuned(NullHooks):
        def load_target(self, ctx, detector_nodes):
            ctx.detector_params = {"fatal": {"fatal_ceiling": 99999.0}}

    ctx = _run(_shipped({"fatal_ceiling": 1100}, points={"ETCH-01": {40: BASE + 150}}), hooks=Tuned())
    assert [n.labels["severity"] for n in ctx.notifications] == ["P0"]
    assert any(line.startswith("params:fatal:tuning-not-accepted:") for line in ctx.audit)


# ── 壞資料 ──


def test_implausible_value_is_not_fatal_but_is_reported():
    """感測器吐 9999：不判致命、不拿來當前後點的微分基準，但寫進 errors 與 audit（不默默吞掉）。"""
    limits = {"fatal_ceiling": 1100, "max_rise": 100, "max_drop": 100, "plausible_max": 5000}
    ctx = _run(_shipped(limits, points={"ETCH-01": {40: 9999.0}}))
    assert not [e for e in ctx.anomalies if e.detector == "detector.hard_limit"]
    assert [(e.entity, e.message) for e in ctx.errors] == [("ETCH-01", "hard_limit:fatal:implausible:1pts:9999")]
    assert "hard_limit:fatal:implausible:1pts:9999" in ctx.audit


def test_notes_and_events_do_not_leak_into_the_next_round_of_a_long_running_loop(sent):
    """常駐迴圈：同一個 Pipeline 物件跑兩輪。壞資料的註記掛在每輪新建的演算法實例上，離開觀測窗後下一輪就沒有；
    已通知過的致命事件也不會因為還在觀測窗內而每輪重發一張新卡。
    （9999 那一點統計軌照樣會判：plausible_max 只管致命軌不把壞值當成踩線，不替統計軌過濾資料。）"""
    wf = _shipped({"fatal_ceiling": 1100, "plausible_max": 5000}, points={"ETCH-01": {20: 9999.0, 40: BASE + 150}})
    pipeline = Pipeline(wf, StorageHooks())
    first = pipeline.run(now=T0 + timedelta(minutes=60))
    assert len(first.errors) == 1
    fatal_cards = [n for n in sent if "fatal" in n.annotations]
    assert [n.labels["severity"] for n in fatal_cards] == ["P0"]
    assert [e.ts for e in first.anomalies if e.detector == "detector.hard_limit"] == [T0 + timedelta(minutes=40)]
    uids = {n.labels["incident_uid"] for n in sent}
    sent.clear()
    second = pipeline.run(now=T0 + timedelta(minutes=65))  # 兩個點都還在觀測窗裡
    assert {n.labels["incident_uid"] for n in sent} <= uids  # 只有既有事件群的續報，沒有新卡
    third = pipeline.run(now=T0 + timedelta(minutes=200))  # 兩個點都離開觀測窗
    assert third.errors == [] and not [e for e in third.detected_anomalies if e.detector == "detector.hard_limit"]
    assert len(second.errors) == 1  # 還在窗內的那一輪照實再報一次（已知行為，見 docstring）


# ── 演算法本身 ──


def _df(values):
    return pd.DataFrame({"ts": pd.date_range(T0, periods=len(values), freq="60s"), "value": values})


def _flags(values, **params):
    algo = HardLimitDetector(**params).fit(_df(values))
    result = algo.detect(_df(values))
    return [i for i, f in enumerate(result.is_anomaly) if f], algo


def test_each_limit_fires_on_exactly_the_offending_point():
    flat = [10.0] * 20
    spike, dip = [*flat[:10], 31.0, *flat[11:]], [*flat[:10], 1.0, *flat[11:]]
    assert _flags(spike, fatal_ceiling=30)[0] == [10]
    assert _flags(spike, fatal_ceiling=31)[0] == []  # 剛好等於界線不算踩線
    assert _flags(dip, fatal_lower=2)[0] == [10]
    assert _flags(spike, max_rise=20)[0] == [10]     # 上升 21；回落那一步是下降，不算
    assert _flags(spike, max_drop=20)[0] == [11]
    assert _flags(dip, max_drop=8)[0] == [10] and _flags(dip, max_rise=8)[0] == [11]
    assert _flags(spike, max_deviation_ratio=2)[0] == [10]  # |31-10| = 2.1 倍中位數
    assert _flags(spike, max_deviation_ratio=2.2)[0] == []
    assert _flags(spike[:8] + [31.0], max_deviation_ratio=2)[0] == [8]
    assert _flags([10.0, 10.0, 31.0], max_deviation_ratio=2)[0] == []  # 前面不到 5 個有效點：不判


def test_consecutive_applies_to_absolute_limits_only():
    values = [10.0] * 5 + [40.0] + [10.0] * 5 + [40.0, 40.0] + [10.0] * 5
    assert _flags(values, fatal_ceiling=30)[0] == [5, 11, 12]
    assert _flags(values, fatal_ceiling=30, consecutive=2)[0] == [12]
    assert _flags(values, fatal_ceiling=30, consecutive=2, max_rise=20)[0] == [5, 11, 12]


def test_score_is_one_plus_the_overshoot_and_survives_infinity():
    algo = HardLimitDetector(fatal_ceiling=100).fit(_df([1.0]))
    scores = list(algo.detect(_df([100.0, 150.0, float("inf"), float("nan")])).scores)
    assert scores[:2] == [0.0, 1.5] and scores[2] == 1e6 and scores[3] == 0.0


def test_implausible_points_are_masked_from_every_check():
    values = [10.0] * 10 + [9999.0] + [10.0] * 9
    flags, algo = _flags(values, fatal_ceiling=30, max_rise=5, max_drop=5, max_deviation_ratio=2, plausible_max=1000)
    assert flags == [] and algo.notes == ["implausible:1pts:9999"]
    flags, algo = _flags([10.0] * 10 + [-5.0] + [10.0] * 9, fatal_lower=0, max_drop=5, plausible_min=-1)
    assert flags == [] and algo.notes == ["implausible:1pts:-5"]


def test_no_limit_means_abstain_not_a_clean_bill_of_health():
    assert HardLimitDetector().can_evaluate(_df([1.0])) is False
    assert HardLimitDetector(plausible_max=5).can_evaluate(_df([1.0])) is False  # 只有壞資料界線不算有界線
    assert HardLimitDetector(max_drop=5).can_evaluate(_df([1.0])) is True
    assert (HardLimitDetector.votes, HardLimitDetector.accepts_tuning, HardLimitDetector.requires_training) == (False,) * 3
    assert HardLimitDetector.tunable == {}


# ── 已知死區（CLAUDE.md 第 100 節第 4 條：照實留存，不換波形粉飾）──


@pytest.mark.xfail(strict=True, reason="W7：沒訂硬上限的序列沒有軌道一，單點仍被多數決吃掉；界線必須由人訂，系統不會自己猜")
def test_dead_zone_single_point_on_a_target_without_any_limit():
    ctx = _run(_shipped(None, points={"ETCH-01": {40: BASE + 150}}))
    assert ctx.notifications


@pytest.mark.xfail(strict=True, reason="取樣死區：兩次取樣之間（60 秒內）發生又結束的尖峰，資料源給的值沒踩線，硬上限看不到")
def test_dead_zone_spike_between_two_scrapes():
    # 真實波形在第 40 分 20 秒衝到 1500、第 40 分 40 秒回到 1000；每分鐘取一次的序列裡第 40、41 分都是正常值
    ctx = _run(_shipped({"fatal_ceiling": 1100}, points={"ETCH-01": {40: BASE + 5.0, 41: BASE - 3.0}}))
    assert ctx.notifications


@pytest.mark.xfail(strict=True, reason="比例死區：前面的中位數是 0（閒置的計數器）時偏離倍數沒有意義，不判；這種序列要用 fatal_ceiling 或 max_rise")
def test_dead_zone_deviation_ratio_on_a_zero_baseline():
    assert _flags([0.0] * 10 + [500.0], max_deviation_ratio=20)[0] == [10]


@pytest.mark.xfail(strict=True, reason="界線抖動：值在界線上下來回時每次越線都是一筆事件（同一張卡累計，不另開卡），硬上限本身不做遲滯")
def test_dead_zone_jitter_around_the_limit_is_not_collapsed_into_one_event():
    values = [10.0] * 5 + [31.0, 29.0, 31.0, 29.0, 31.0] + [10.0] * 5
    assert _flags(values, fatal_ceiling=30)[0] == [5]
