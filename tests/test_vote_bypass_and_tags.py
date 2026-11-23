"""雙軌制偵測（docs/DUAL_TRACK_DESIGN.md 方案 B）拆掉之後留下來的部分。

致命軌（detector.hard_limit＋alert.fatal_floor）、布林多數決（detector.ensemble）、軟投票（detector.soft_vote，
2026-11-20）都已移除，它們自己的測試（含 bypass 接線檢查）跟著刪掉；檔名沿用。
這個檔只剩雙軌制當初加進來、現在仍然存在的通用機制：
  - 抑制算子的標籤例外：alert.suppress_dedup 的 escalate_tags、alert.suppress_same_root_cause 的 exempt_tags
    （平台內已經沒有算子會打 fatal 標籤，這裡用它當任意標籤）；靜音窗不看標籤。
  - 單點事件：以前是死區（軟投票加總不到 2 票等值，xfail）；表決移除後殘差突變節點單獨成立，現在抓得到。
"""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from aiops.engine.context import PipelineContext, RecentNotification, Silence
from aiops.engine.errors import WorkflowError
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, NullHooks, Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.interfaces.runner import workflow_path
from aiops.stages.suppression.suppress import DedupSuppressor, SameRootCauseSuppressor, SilenceWindowSuppressor

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


def _shipped(name: str, *, seed=0, points=None, entity="ETCH-01") -> Workflow:
    """偵測管線樣本，fetch 換成固定資料、targets 換成一條測試序列，其餘節點與參數原封不動。"""
    wf = load_workflow(workflow_path(name))
    nodes = [NodeSpec(name=n.name, operator="test.dual_frame", params={"seed": seed, "points": points or {}})
             if n.operator == "ingestion.fetch" else n for n in wf.nodes]
    target = {"source_type": "mock", "query": "rf_power", "entity": entity, "attrs": {"signal": "saturation"}}
    return Workflow(name=wf.name, nodes=nodes, targets=[TargetSpec(**target)])


def _run(wf, minute=WINDOW, hooks=None):
    return Pipeline(wf, hooks or NullHooks()).run(now=T0 + timedelta(minutes=minute))


# ── 抑制算子的標籤例外 ──


def _incident(entity="svc", minute=40, score=4.0, tags=(), node="solo"):
    ev = AnomalyEvent(entity, "m", T0 + timedelta(minutes=minute), 2.0, value=1.0,
                      detector="detector.sparse_count", detector_node=node)
    return Incident([ev], score, list(tags))


def _batch(*incidents, **kw):
    ctx = PipelineContext(pipeline="p", now=T0 + timedelta(minutes=60))
    ctx.incidents, ctx.incidents_built = list(incidents), True
    for k, v in kw.items():
        setattr(ctx, k, v)
    return ctx


@pytest.mark.parametrize("last,tags,suppressed", [
    ("P3", ["fatal:fatal"], False),  # 才發過 P3：帶升級標籤的是升級，照發
    ("P2", ["fatal:fatal"], False),
    ("P0", ["fatal:fatal"], True),   # 已經發過 P0：照常去重，不洗版
    ("P3", [], True),                # 沒有標籤的照舊被冷卻擋下
    ("P3", ["fatality"], True),      # 標籤要完全相同或「fatal:」開頭，不是字首比對
])
def test_cooldown_lets_a_tagged_incident_through_unless_p0_was_already_sent(last, tags, suppressed):
    inc = _incident(tags=tags)
    recent = [RecentNotification(inc.fingerprint, T0 + timedelta(minutes=30), "inc-old", last)]
    DedupSuppressor(cooldown_minutes=30, escalate_tags=["fatal"]).execute(_batch(inc, recent_notifications=recent))
    assert bool(inc.suppressed_by) is suppressed


def test_dedup_without_escalate_tags_behaves_as_before():
    inc = _incident(tags=["fatal:fatal"])
    recent = [RecentNotification(inc.fingerprint, T0 + timedelta(minutes=30), "inc-old", "P3")]
    DedupSuppressor(cooldown_minutes=30).execute(_batch(inc, recent_notifications=recent))
    assert inc.suppressed_by.startswith("dedup")


def test_same_root_cause_does_not_swallow_an_exempt_incident():
    """上游帶例外標籤、下游分數更高也在告警：沒有 exempt_tags 時上游被當成症狀吃掉；有的話兩張都發。"""
    def pair():
        return _incident("api", score=10.0, tags=["fatal:fatal"]), _incident("db", score=12.0)

    up, down = pair()
    SameRootCauseSuppressor().execute(_batch(up, down, topology=[("api", "db")]))
    assert up.suppressed_by.startswith("same_root_cause") and not down.suppressed_by
    up, down = pair()
    SameRootCauseSuppressor(exempt_tags=["fatal"]).execute(_batch(up, down, topology=[("api", "db")]))
    assert not up.suppressed_by and not down.suppressed_by


def test_maintenance_window_still_silences_a_tagged_incident():
    """靜音／維護窗是人的明確決定：帶標籤的事件一樣被抑制（要不要例外由人去掉靜音，不是偵測器自己決定）。"""
    inc = _incident(tags=["fatal:fatal"], score=10.0)
    silence = Silence(id=7, kind="maintenance", entity="svc", dept=None, start=T0, end=T0 + timedelta(hours=2))
    SilenceWindowSuppressor().execute(_batch(inc, silences=[silence]))
    assert inc.suppressed_by == "silence_window：maintenance#7"


# ── 載入期防呆：設定錯在啟動時就報，不延到執行期 ──


def _wf(*nodes):
    specs = [NodeSpec(name="fetch", operator="test.dual_frame")]
    specs += [NodeSpec(name=n, operator=o, params=p) for n, o, p in nodes]
    return Workflow(name="t", nodes=specs, targets=[TargetSpec(source_type="mock", query="a", entity="a")])


VOTER = ("hourly", "detector.hourly_baseline", {"snr": {"p_fa": 0.001}})


@pytest.mark.parametrize("nodes,message", [
    ((VOTER, ("dedup", "alert.suppress_dedup", {"escalate_tags": "fatal"})), "escalate_tags 必須是標籤的清單"),
])
def test_wiring_mistakes_fail_when_the_pipeline_is_built(nodes, message):
    with pytest.raises(WorkflowError, match=message):
        Pipeline(_wf(*nodes))


# ── 單點事件（以前的死區）──


@pytest.mark.parametrize("name", ["sample_it_otel", "sample_factory"])
@pytest.mark.parametrize("value", [BASE + 150, BASE - 150], ids=["單點尖峰", "單點瞬斷"])
def test_single_point_event_is_now_caught_by_the_residual_shift_node_alone(name, value):
    """以前是 xfail(strict=True) 的單點死區：重尾序列上偏離 150 的單點，軟投票加總到不了 2 票等值，沒有任何通知
    （致命硬上限那一軌更早就移除了）。2026-11-20 移除軟投票之後，殘差突變節點（detect_rshift）單獨成立：
    第 40、41 分鐘（跳上去、跳回來）各一筆，+150 是 4.53／4.54 分、−150 是 5.64／5.64 分（門檻 3.29），有通知。
    死區不存在了，xfail 標記拿掉。原本的斷言 ctx.notifications 沒動；另一條「audit 有 soft_vote:vote:voters=3」
    是在確認三票都評估過，表決節點沒了，改成確認事件來自哪個節點。
    同一份資料沒有注入時（一整天、每小時一輪共 24 輪）改版前後都是 0 則通知、0 個告警分鐘（scratch 量的）。"""
    ctx = _run(_shipped(name, points={"ETCH-01": {40: value}}))
    assert sorted((e.detector_node, int((e.ts - T0).total_seconds() // 60)) for e in ctx.anomalies) == [
        ("detect_rshift", 40), ("detect_rshift", 41)]
    assert ctx.notifications


@pytest.mark.parametrize("name", ["sample_it_otel", "sample_factory"])
def test_the_same_series_without_the_injected_point_sends_nothing(name):
    """對照組：沒有注入的同一段資料沒有事件、沒有通知（上一個測試的通知是注入造成的，不是偵測段本來就在叫）。"""
    ctx = _run(_shipped(name))
    assert ctx.anomalies == [] and not ctx.notifications
