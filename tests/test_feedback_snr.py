"""節點開了 snr 的管線（sample_it_otel）的回饋與提案：
- 誤報回饋不再自動產生把偵測調鈍的提案（2026-11-20 移除「誤報 -> 門檻倍率 threshold_scale 提案」整條路）；
- 人工提案照舊走整段回測＋黃金事故防護罩；
- 資料庫裡舊的 threshold_scale／底噪／尾巴校正調參被略過，不會讓偵測失敗。

合成序列 synth:svc 依「時間戳」產生（同一分鐘從哪個時間窗查都是同一個值）：mes-api 的形狀（台北平日 09–19 點 291、
其餘 135，週末 145／75），雜訊 N(0, 9)。疊上：
  FP1／FP2：上班時段 +54（約 6 個雜訊單位）5 分鐘的小凸起——會告警，人判定是誤報（FP2 是兩天後同樣形狀的凸起）
  GOLDEN：上班時段 ×0.9 持續 60 分鐘（真的掉量：持續低一成，約 3.2 個雜訊單位）
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from aiops.control_plane.learning.segment import snr_nodes
from aiops.engine.errors import BadRequest
from aiops.engine.pipeline import Pipeline, TargetSpec, load_workflow
from aiops.engine.requests import FeedbackPropose, FeedbackSubmit, ProposalBacktest, ProposalCreate
from aiops.interfaces.runner import run, workflow_path
from aiops.sources.mock import MockSpec, _splitmix
from aiops.stages.detectors.base import layer_params
from aiops.stages.detectors.residual_chronic import ResidualChronicDetector
from aiops.storage.hooks import StorageHooks
from aiops.storage.repository import ActiveConfigs, Feedbacks, Incidents

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
    assert fp_events and golden_events, "FP1 與 GOLDEN 都應該告警（測試前提）"
    fp, golden = _incident_at(FP1), _incident_at(GOLDEN)
    fb = _feedback(fp["uid"], "false_positive")["feedback"]
    _feedback(golden["uid"], "true_positive")
    return fb


def test_it_otel_snr_nodes_are_detected():
    assert snr_nodes(PIPELINE, "detector.residual_chronic") == ["detect_rchronic"]
    assert snr_nodes("sample_metrics_only", "detector.hourly_baseline") == []  # 沒開 snr 的管線


# ── 誤報回饋不再自動把偵測調鈍 ──


def test_false_positive_feedback_no_longer_generates_a_desensitizing_proposal(pipeline):
    """2026-11-20 以前：這裡會產生一個 threshold_scale > 1 的提案（整段重跑二分搜），核准後同類誤報不再告警。
    現在：回饋照樣記錄，但 admin.feedback_propose 直接拒絕，不產生提案、不改現行參數。"""
    fb = _seed(pipeline)
    for scope in ("entity", "pipeline", "global"):
        with pytest.raises(BadRequest, match="誤報回饋不再自動產生") as e:
            run("admin_feedback_propose", request=FeedbackPropose(feedback_id=fb["id"], proposer="bot", scope=scope,
                                                                  detector="detector.residual_chronic"))
        assert "admin.proposal_create" in str(e.value)  # 訊息要說人還能怎麼做
    assert Feedbacks.get(fb["id"])["proposal_id"] is None
    assert all(ActiveConfigs.get(d, PIPELINE, "svc") is None
               for d in ("detector.hourly_baseline", "detector.residual_shift", "detector.residual_chronic"))


def test_the_same_kind_of_false_alarm_fires_again_after_false_positive_feedback(pipeline):
    """標了誤報之後，兩天後同樣形狀的 FP2 照樣告警：回饋不會讓偵測變鈍（以前核准倍率提案後這裡會安靜）。"""
    _seed(pipeline)
    ctx = pipeline.run(now=FP2[1] + timedelta(minutes=6), targets=[TARGET])
    assert [e for e in ctx.detected_anomalies if FP2[0] <= e.ts <= FP2[1]]


# ── 人工提案：整段回測＋黃金事故防護罩照舊 ──


def test_a_manual_proposal_on_an_snr_node_is_backtested_on_the_whole_segment_with_the_golden_guard(pipeline):
    _seed(pipeline)
    pid = run("admin_proposal_create", request=ProposalCreate(
        source="manual", detector="detector.residual_chronic", params={"window": 45}, proposer="bob",
        pipeline=PIPELINE, entity="svc"))["proposal"]["id"]
    bt = _backtest(pid, FP1[1] + timedelta(hours=2))
    b = bt["backtest"]
    assert bt["status"] == "backtested" and b["mode"] == "segment"
    assert b["current_anomalies"] > 0 and b["proposed_anomalies"] > 0  # 測試段含 FP1：整段裡任何一個節點越線都算
    safety = b["safety"]
    assert safety["passed"] and safety["checked"] == 1 and [c["incident_uid"] for c in safety["caught"]]


# ── 已移除的參數：寫了就報錯；資料庫裡舊的調參被略過 ──


# min_scale_ratio 原本也在這張清單裡；2026-11-20 同一天依實測結果放回（相對底噪，base.MIN_SCALE_RATIO_TUNABLE），
# 現在是合法的可調參數，所以從清單拿掉。其餘被移除的參數一個都沒少。
@pytest.mark.parametrize("removed", [{"threshold_scale": 2.0}, {"min_scale": 0.5},
                                     {"fpr": 0.01}, {"max_tail_factor": 5.0}, {"min_run": 10}, {"min_train_points": 10}])
def test_removed_parameters_are_rejected_in_yaml_and_in_proposals(removed):
    with pytest.raises(TypeError, match="沒有參數"):
        ResidualChronicDetector(**removed)
    with pytest.raises(BadRequest):
        run("admin_proposal_create", request=ProposalCreate(
            source="manual", detector="detector.residual_chronic", params=removed, proposer="x",
            pipeline=PIPELINE, entity="svc"))


def test_stale_tuned_keys_from_removed_mechanisms_are_ignored_not_fatal(pipeline):
    """active_detector_configs 裡以前核准的 threshold_scale／絕對底噪／尾巴校正：略過並記 log，偵測照常。
    min_scale_ratio 原本也在 stale 裡（當時是已移除的參數）；2026-11-20 放回後它是合法的調參、會被套用，
    所以從 stale 拿掉，log 斷言的鍵清單跟著少一個；它「會被套用」另外斷言在下面第二行。"""
    stale = {"threshold_scale": 4.0, "min_scale": 50.0, "fpr": 0.05, "max_tail_factor": 10.0}
    assert layer_params(ResidualChronicDetector, stale, {}, {}) == {}
    assert layer_params(ResidualChronicDetector, {**stale, "min_scale_ratio": 0.5}, {}, {}) == {"min_scale_ratio": 0.5}
    for det in ("detector.hourly_baseline", "detector.residual_shift", "detector.residual_chronic"):
        assert ActiveConfigs.set(det, stale, "auto", 0, expected_version=0, pipeline=PIPELINE, entity="svc") is not None
    ctx = pipeline.run(now=FP1[1] + timedelta(minutes=6), targets=[TARGET])
    assert not ctx.errors
    assert any(line == "params:detect_rchronic:ignored-unknown:fpr,max_tail_factor,min_scale,threshold_scale"
               for line in ctx.audit)
    assert [e for e in ctx.detected_anomalies if FP1[0] <= e.ts <= FP1[1]]  # 照常偵測（×4 的倍率沒有被套用）


def test_missed_feedback_on_an_snr_node_is_refused_with_a_reason(pipeline):
    """開了 snr 的節點門檻由 YAML 的 p_fa 換算，調參改 threshold 不會生效：漏報提案照舊拒絕並說明要改哪裡。"""
    missed = run("admin_feedback_submit", request=FeedbackSubmit(
        verdict="missed", text="這段沒告警", author="amy", pipeline=PIPELINE, entity="svc",
        time_range={"start": GOLDEN[0], "end": GOLDEN[1]}))["feedback"]
    with pytest.raises(BadRequest, match="snr.p_fa"):
        run("admin_feedback_propose", request=FeedbackPropose(feedback_id=missed["id"], proposer="bot", scope="entity",
                                                              detector="detector.residual_chronic"))
