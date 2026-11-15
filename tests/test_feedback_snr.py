"""R+SNR（it_otel）的誤報回饋閉環：false_positive -> 門檻倍率提案 -> 整段回測＋黃金事故防護罩 -> 人核准 -> 生效。

合成序列 synth:svc 依「時間戳」產生（同一分鐘從哪個時間窗查都是同一個值）：mes-api 的形狀（台北平日 09–19 點 291、
其餘 135，週末 145／75），雜訊 N(0, 9)。疊上：
  FP1／FP2：上班時段 +54（約 6 個雜訊單位）5 分鐘的小凸起——R+SNR 會告警，人判定是誤報（FP2 在核准之後才發生）
  GOLDEN：上班時段 ×0.9 持續 60 分鐘（真的掉量：持續低一成，約 3.2 個雜訊單位，主要靠漂移票）
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from aiops.control_plane.learning import snr_feedback
from aiops.control_plane.learning.segment import Segment, snr_nodes
from aiops.engine.errors import BadRequest, Conflict
from aiops.engine.pipeline import Pipeline, TargetSpec, load_workflow
from aiops.engine.requests import FeedbackPropose, FeedbackSubmit, ProposalApprove, ProposalBacktest, ProposalCreate
from aiops.interfaces.runner import run, workflow_path
from aiops.sources.mock import MockSpec, _splitmix
from aiops.stages.detectors.base import THRESHOLD_SCALE, layer_params
from aiops.stages.detectors.residual_chronic import ResidualChronicDetector
from aiops.storage.hooks import StorageHooks
from aiops.storage.repository import ActiveConfigs, Incidents

TPE = ZoneInfo("Asia/Taipei")
PIPELINE = "sample_it_otel"
QUERY = "synth:svc"


def _tpe(s):
    return pd.Timestamp(s, tz=TPE).tz_convert("UTC").to_pydatetime()


FP1 = (_tpe("2026-11-10 14:00"), _tpe("2026-11-10 14:04"))   # 週二
GOLDEN = (_tpe("2026-11-11 11:00"), _tpe("2026-11-11 11:59"))  # 週三
FP2 = (_tpe("2026-11-12 15:00"), _tpe("2026-11-12 15:04"))   # 週四：核准之後才發生的同類誤報


def _level(ts):
    loc = pd.DatetimeIndex(ts).tz_convert(TPE)
    office, weekend = (loc.hour >= 9) & (loc.hour < 19), loc.day_of_week >= 5
    return np.where(weekend, np.where(office, 145.0, 75.0), np.where(office, 291.0, 135.0))


def _synth(ts):
    ts = pd.DatetimeIndex(ts)
    epoch_min = (ts.as_unit("s").asi8 // 60).astype(np.uint64)
    u1 = (_splitmix(epoch_min, 3) + 0.5) / 2.0**53
    u2 = (_splitmix(epoch_min, 5) + 0.5) / 2.0**53
    v = _level(ts) + 9 * np.sqrt(-2 * np.log(u1)) * np.cos(2 * np.pi * u2)
    for start, end in (FP1, FP2):
        v = v + 54.0 * ((ts >= start) & (ts <= end))
    v = np.where((ts >= GOLDEN[0]) & (ts <= GOLDEN[1]), v * 0.9, v)
    return np.round(v)


@pytest.fixture(autouse=True)
def synth_source(monkeypatch):
    real = MockSpec.fetch_data

    def fetch(self, query, start, end, step, field_map=None):
        if query != QUERY:
            return real(self, query, start, end, step, field_map)
        ts = pd.date_range(pd.Timestamp(start).ceil(f"{step}s"), end, freq=f"{step}s")
        return pd.DataFrame({"ts": ts, "value": _synth(ts)})

    monkeypatch.setattr(MockSpec, "fetch_data", fetch)


TARGET = TargetSpec(source_type="mock", query=QUERY, entity="svc")


def _detect(pipeline: Pipeline, at) -> list:
    """正式偵測跑一輪（it_otel 原樣，StorageHooks：基準線、事件群、調參都走資料庫）。回傳這一輪寫進資料庫的異常。"""
    return pipeline.run(now=at + timedelta(minutes=1), targets=[TARGET]).detected_anomalies


def _incident_at(window) -> dict:
    rows = [r for r in Incidents.created_since(datetime(2000, 1, 1))
            if any(window[0] <= pd.Timestamp(e["ts"]).to_pydatetime() <= window[1] + timedelta(minutes=30)
                   for e in r["incident"]["events"])]
    assert rows, f"{window} 沒有事件群"
    return rows[0]


def _feedback(uid, verdict):
    return run("admin_feedback_submit", request=FeedbackSubmit(
        incident_uid=uid, verdict=verdict, text="凸起只是報表批次，不是事故" if verdict == "false_positive" else "真的掉量",
        author="amy"))


def _backtest(pid, end):
    req = ProposalBacktest(proposal_id=pid, metric={"source_type": "mock", "query": QUERY}, entity="svc",
                           time_range={"start": end - timedelta(days=8), "end": end}, train_ratio=0.875)
    return run("admin_proposal_backtest", request=req, targets=req.to_targets(), now=end)["proposal"]


@pytest.fixture
def pipeline():
    return Pipeline(load_workflow(workflow_path(PIPELINE)), StorageHooks())


def _seed(pipeline):
    """線上偵測：FP1 與 GOLDEN 各跑一輪，人標 FP1 誤報、GOLDEN 真異常（進黃金事故庫）。"""
    fp_events = _detect(pipeline, FP1[1] + timedelta(minutes=5))
    golden_events = _detect(pipeline, GOLDEN[1])
    assert fp_events and golden_events, "R+SNR 應該對 FP1 與 GOLDEN 都告警（測試前提）"
    fp, golden = _incident_at(FP1), _incident_at(GOLDEN)
    fb = _feedback(fp["uid"], "false_positive")["feedback"]
    _feedback(golden["uid"], "true_positive")
    return fb


# ── 端到端：誤報 -> 提案 -> 回測 -> 核准 -> 同類誤報不再告警，黃金事故仍抓得到 ──


def test_it_otel_snr_nodes_are_detected():
    assert snr_nodes(PIPELINE, "detector.residual_chronic") == ["detect_rchronic"]
    assert snr_feedback.has_snr(PIPELINE) and not snr_feedback.has_snr("sample_factory")


def test_false_positive_feedback_produces_an_approvable_scale_that_silences_the_same_kind_of_false_alarm(pipeline):
    fb = _seed(pipeline)
    res = run("admin_feedback_propose", request=FeedbackPropose(feedback_id=fb["id"], proposer="bot", scope="entity"))
    proposal = res["proposal"]
    assert res["generated_by"] == "segment_bisect" and proposal["entity"] == "svc" and proposal["pipeline"] == PIPELINE
    scale = proposal["params"][THRESHOLD_SCALE]
    assert 1.0 < scale <= 10.0 and proposal["detector_name"] in snr_feedback.segment_detectors(PIPELINE)
    best = next(c for c in res["snr"]["candidates"] if c.get("detector") == proposal["detector_name"])
    # 只調一個偵測器：FP1 的告警分鐘變少（軟投票下另外兩票在凸起的第一分鐘仍可能湊到 2 票，照實記在 reason）
    assert best["fp_minutes_after"] < best["fp_minutes_before"] and scale <= best["golden_safe_max"]
    assert f"{best['fp_minutes_before']} -> {best['fp_minutes_after']} 分鐘" in proposal["reason"]

    bt = _backtest(proposal["id"], FP1[1] + timedelta(hours=2))
    b = bt["backtest"]
    assert bt["status"] == "backtested" and b["mode"] == "segment"
    assert b["proposed_anomalies"] < b["current_anomalies"]  # 測試段（含 FP1）告警分鐘變少
    safety = b["safety"]
    assert safety["passed"] and safety["checked"] == 1 and [c["incident_uid"] for c in safety["caught"]]

    approved = run("admin_proposal_approve", request=ProposalApprove(proposal_id=proposal["id"], approver="lead"))
    assert approved["proposal"]["status"] == "approved"
    assert ActiveConfigs.get(proposal["detector_name"], PIPELINE, "svc")["params"][THRESHOLD_SCALE] == scale

    # 生效：核准之後的線上偵測載入這份調參；兩天後同樣形狀的 FP2 不再告警
    ctx = pipeline.run(now=FP2[1] + timedelta(minutes=6), targets=[TARGET])
    assert any(line.startswith("params:") and proposal["detector_name"] in line for line in ctx.audit)
    assert any(f":x{scale:g}" in line for line in ctx.audit)
    window = [e for e in ctx.detected_anomalies if FP2[0] <= e.ts <= FP2[1]]
    assert window == []


def test_without_the_approval_the_same_false_alarm_fires_again(pipeline):
    """對照組：沒有核准調參時，FP2 照樣告警（上一個測試的「不再告警」是調參造成的，不是資料本來就安靜）。"""
    _seed(pipeline)
    ctx = pipeline.run(now=FP2[1] + timedelta(minutes=6), targets=[TARGET])
    assert [e for e in ctx.detected_anomalies if FP2[0] <= e.ts <= FP2[1]]


def test_a_scale_that_would_lose_the_golden_incident_is_rejected_and_cannot_be_approved(pipeline):
    _seed(pipeline)
    pid = run("admin_proposal_create", request=ProposalCreate(
        source="manual", detector="detector.residual_chronic", params={THRESHOLD_SCALE: 10.0}, proposer="bob",
        pipeline=PIPELINE, entity="svc"))["proposal"]["id"]
    bt = _backtest(pid, FP1[1] + timedelta(hours=2))
    assert bt["status"] == "rejected"
    missed = bt["backtest"]["safety"]["missed_golden"]
    assert len(missed) == 1 and missed[0]["current_anomalies"] > 0 and missed[0]["proposed_anomalies"] == 0
    with pytest.raises(Conflict, match="黃金事故"):
        run("admin_proposal_approve", request=ProposalApprove(proposal_id=pid, approver="lead"))
    assert ActiveConfigs.get("detector.residual_chronic", PIPELINE, "svc") is None


def test_the_proposer_never_goes_past_the_golden_safe_limit(pipeline):
    """提案本身就以黃金事故為上限：二分搜出的倍率 < 會讓 GOLDEN 消失的倍率（整段重跑驗）。"""
    fb = _seed(pipeline)
    res = run("admin_feedback_propose", request=FeedbackPropose(feedback_id=fb["id"], proposer="bot", scope="entity",
                                                                detector="detector.residual_chronic"))
    entry = res["snr"]["candidates"][0]
    assert entry["detector"] == "detector.residual_chronic" and entry["scale"] <= entry["golden_safe_max"] < 10.0


# ── 規則與防呆 ──


def test_threshold_scale_only_goes_up_and_needs_a_threshold():
    with pytest.raises(BadRequest):
        run("admin_proposal_create", request=ProposalCreate(
            source="manual", detector="detector.residual_chronic", params={THRESHOLD_SCALE: 0.5}, proposer="x",
            pipeline=PIPELINE, entity="svc"))
    with pytest.raises(BadRequest):
        run("admin_proposal_create", request=ProposalCreate(
            source="manual", detector="detector.hard_limit", params={THRESHOLD_SCALE: 2.0}, proposer="x",
            pipeline=PIPELINE, entity="svc"))
    assert ResidualChronicDetector(threshold_scale=2.5).threshold_scale == 2.5


def test_yaml_written_threshold_scale_is_locked_against_tuning():
    merged = layer_params(ResidualChronicDetector, {THRESHOLD_SCALE: 4.0}, {THRESHOLD_SCALE: 1.5}, {})
    assert merged[THRESHOLD_SCALE] == 1.5
    assert layer_params(ResidualChronicDetector, {THRESHOLD_SCALE: 4.0}, {}, {})[THRESHOLD_SCALE] == 4.0
    assert layer_params(ResidualChronicDetector, {THRESHOLD_SCALE: 4.0}, {}, {THRESHOLD_SCALE: 2.0})[THRESHOLD_SCALE] == 2.0


def test_missed_feedback_on_an_r_snr_pipeline_is_refused_with_a_reason(pipeline):
    _detect(pipeline, FP1[1] + timedelta(minutes=5))
    fb = _feedback(_incident_at(FP1)["uid"], "false_positive")["feedback"]
    missed = run("admin_feedback_submit", request=FeedbackSubmit(
        verdict="missed", text="這段沒告警", author="amy", pipeline=PIPELINE, entity="svc",
        time_range={"start": GOLDEN[0], "end": GOLDEN[1]}))["feedback"]
    with pytest.raises(BadRequest, match="只處理誤報"):
        run("admin_feedback_propose", request=FeedbackPropose(feedback_id=missed["id"], proposer="bot", scope="entity"))
    assert fb["verdict"] == "false_positive"


def test_segment_replay_reuses_the_trained_baseline_across_scales():
    end = FP1[1]
    ts = pd.date_range(end - timedelta(days=7, hours=2), end, freq="60s")
    df = pd.DataFrame({"ts": ts, "value": _synth(ts)})
    seg = Segment(PIPELINE, "svc", "mock", QUERY, df, FP1[0], FP1[1], [])
    first = seg.run({})
    assert first.alerts > 0 and seg.states  # 第一次訓練並記住基準線
    states = dict(seg.states)
    quiet = seg.run({"detector.hourly_baseline": {THRESHOLD_SCALE: 10.0}, "detector.residual_shift": {THRESHOLD_SCALE: 10.0},
                     "detector.residual_chronic": {THRESHOLD_SCALE: 10.0}})
    assert quiet.alerts < first.alerts and seg.states.keys() == states.keys()
