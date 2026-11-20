"""序列 SNR、由 p_fa 換算門檻、逐點 SNR 證據與軟投票（detector.soft_vote）。四組回放數字在 docs/RESIDUAL_SNR_ENSEMBLE.md。"""
import json
import math
from datetime import timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from aiops.engine.context import DetectorEvidence, PipelineContext, Target
from aiops.engine.errors import WorkflowError, WorkflowParamError
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import ANOMALY, AnomalyEvent
from scripts.backtest.variants import detection_nodes
from aiops.stages.detectors.base import (SNR_STATE_KEY, DetectorOperator, gaussian_threshold, series_snr,
                                         snr_threshold)

TZ = "Asia/Taipei"
TPE = ZoneInfo(TZ)
START = pd.Timestamp("2026-10-12 00:00", tz=TPE).tz_convert("UTC")  # 週一
SNR = {"p_fa": 0.001, "tz": TZ}


def _level(ts):
    loc = ts.tz_convert(TPE)
    office, weekend = (loc.hour >= 9) & (loc.hour < 19), loc.day_of_week >= 5
    return np.where(weekend, np.where(office, 145.0, 75.0), np.where(office, 291.0, 135.0))


def _series(days=14, noise=9.0, seed=0):
    ts = pd.date_range(START, periods=days * 1440, freq="60s")
    rng = np.random.default_rng(seed)
    return pd.DataFrame({"ts": ts, "value": np.round(_level(ts) + rng.normal(0, noise, len(ts))) if noise else _level(ts)})


@pytest.fixture(scope="module")
def train():
    return _series()


# ── 序列 SNR ──


def test_series_snr_is_seasonal_variance_over_residual_noise(train):
    stats = series_snr(train, TPE, 30)
    expected_signal = float(np.var(_level(pd.DatetimeIndex(train["ts"]))))
    assert stats["status"] == "ok" and not stats["noise_floored"]
    assert stats["signal_var"] == pytest.approx(expected_signal, rel=0.02)
    assert stats["noise_sigma"] == pytest.approx(9.0, rel=0.1)
    assert stats["snr_db"] == pytest.approx(10 * math.log10(expected_signal / 81.0), abs=0.5)
    noisier = series_snr(_series(noise=27.0), TPE, 30)
    assert noisier["snr_db"] == pytest.approx(stats["snr_db"] - 10 * math.log10(9), abs=0.5)  # 雜訊 ×3 -> 少 9.5 dB


def test_zero_noise_uses_the_floor_instead_of_dividing_by_zero():
    df = _series(noise=0.0)
    stats = series_snr(df, TPE, 30)
    # 底噪 = max(整數序列 1, |中位數| × 1%)
    assert stats["noise_floored"] and stats["noise_sigma"] == max(1.0, 0.01 * float(df["value"].median()))
    assert math.isfinite(stats["snr_db"]) and json.dumps(stats)


def test_insufficient_and_flat_series_report_no_snr():
    short = series_snr(_series(days=1), TPE, 10_000)
    assert short["status"] == "insufficient" and short["snr_db"] is None
    flat = series_snr(pd.DataFrame({"ts": _series(days=7)["ts"], "value": 50.0}), TPE, 30)
    assert flat["status"] == "flat" and flat["snr_db"] is None and flat["noise_floored"]
    assert json.dumps([short, flat])


# ── 門檻映射 ──


def test_gaussian_threshold_is_the_two_sided_normal_quantile():
    assert gaussian_threshold(0.05) == pytest.approx(1.95996, abs=1e-4)
    assert gaussian_threshold(0.001) == pytest.approx(3.29053, abs=1e-4)
    assert gaussian_threshold(1e-5) > gaussian_threshold(1e-3)


class _Unwhitened(DetectorOperator):
    """沒白化的測試替身（不註冊）：分數 = 0.6745 × |值 − 全段中位數| ÷ MAD，季節訊號沒扣掉。
    內建的沒白化偵測器（mad、quantile_drift）移除後，snr_threshold 的經驗分位數那條路只剩 sparse_count 走得到，
    而它的分數是機率的對數、沒有「季節擺盪撐大門檻」可量，所以用替身固定那條路的行為。"""

    algorithm = "unwhitened"
    tunable = {"threshold": (3.5, 1.0, 20.0)}

    def fit(self, df):
        self.median_ = float(df["value"].median())
        self.mad_ = float((df["value"] - self.median_).abs().median())
        self.fitted = True
        return self

    def get_state(self):
        return {"median": self.median_, "mad": self.mad_}

    def load_state(self, state):
        self.median_, self.mad_ = float(state["median"]), float(state["mad"])
        self.fitted = True
        return self

    def detect(self, df):
        scores = 0.6745 * (df["value"] - self.median_).abs() / self.mad_
        return self._result(df, scores, scores > self.param("threshold"))


class _NoThreshold(_Unwhitened):
    """沒有 threshold 的測試替身（不註冊）：內建偵測器現在都有 threshold，snr 的這個檢查只能用替身碰到。"""

    tunable = {}


def test_whitened_nodes_get_the_gaussian_threshold_and_unwhitened_ones_measure_the_seasonal_swing(train):
    hourly = OperatorRegistry.create("detector.hourly_baseline", {"tz": TZ, "min_weeks": 1}).fit(train)
    assert snr_threshold(hourly, train, 0.001) == (pytest.approx(3.29053, abs=1e-4), "gaussian")
    raw = _Unwhitened().fit(train)
    tau, source = snr_threshold(raw, train, 0.001)
    assert source == "empirical" and tau > 1.5 * gaussian_threshold(0.001)  # 上班時段整段離中位數很遠，門檻被撐大
    noise_only = pd.DataFrame({"ts": train["ts"], "value": np.random.default_rng(4).normal(100, 9, len(train))})
    flat = _Unwhitened().fit(noise_only)
    tau_flat, _ = snr_threshold(flat, noise_only, 0.001)
    assert tau_flat == pytest.approx(gaussian_threshold(0.001), rel=0.15)  # 沒有季節訊號：跟常態分位數差不多


# ── 節點層：證據、狀態、預設不變 ──


def _ctx(df, **kw):
    ctx = PipelineContext(pipeline="p", now=df["ts"].iloc[-1].to_pydatetime() + timedelta(minutes=1),
                          target=Target("mock", "q", "svc", {}))
    ctx.raw_df = df.reset_index(drop=True)
    for k, v in kw.items():
        setattr(ctx, k, v)
    return ctx


def test_snr_node_emits_evidence_and_stores_stats_with_the_baseline(train):
    window = _series(days=15).iloc[-60:]
    op = OperatorRegistry.create("detector.residual_chronic", {"tz": TZ, "snr": SNR})
    ctx = op.execute(_ctx(window, train_df=train))
    ev = ctx.detector_evidence[op.state_key]
    assert isinstance(ev, DetectorEvidence) and len(ev.snr) == 60 and ev.threshold_source == "gaussian"
    assert ev.threshold == pytest.approx(3.29053, abs=1e-4) and ev.series_snr_db > 10
    state = ctx.new_detector_states[op.state_key]
    stored = json.loads(json.dumps(state.state))
    assert stored[SNR_STATE_KEY]["threshold"] == ev.threshold
    # 下一輪載入同一份基準線：證據一模一樣（演算法的 load_state 看不到 "_snr" 那層）
    state.state = stored
    again = op.execute(_ctx(window, detector_states={op.state_key: state}))
    assert again.detector_evidence[op.state_key] == ev


def test_old_baseline_without_snr_stats_falls_back_to_the_configured_threshold(train):
    window = _series(days=15).iloc[-60:]
    plain = OperatorRegistry.create("detector.hourly_baseline", {"tz": TZ, "min_weeks": 1})
    old_state = plain.execute(_ctx(window, train_df=train)).new_detector_states[plain.state_key]
    snr_op = OperatorRegistry.create("detector.hourly_baseline", {"tz": TZ, "min_weeks": 1, "snr": SNR})
    ev = snr_op.execute(_ctx(window, detector_states={snr_op.state_key: old_state})).detector_evidence[snr_op.state_key]
    assert ev.threshold_source == "fallback" and ev.threshold == 3.0 and ev.series_snr_db is None


@pytest.mark.parametrize("name,params", [("detector.hourly_baseline", {"tz": TZ, "min_weeks": 1}),
                                         ("detector.residual_shift", {"tz": TZ})])
def test_without_snr_nothing_changes(train, name, params):
    """沒開 snr：沒有證據、state 沒有 "_snr"、事件跟以前一樣（開了 snr 也不會改到沒開的節點）。"""
    window = _series(days=15).iloc[-60:]
    ctx = OperatorRegistry.create(name, params).execute(_ctx(window, train_df=train))
    assert ctx.detector_evidence == {}
    assert all(SNR_STATE_KEY not in s.state for s in ctx.new_detector_states.values())
    algo = OperatorRegistry.create(name, params).fit(train)
    expected = [t for t, f in zip(algo.detect(window.reset_index(drop=True)).timestamps,
                                  algo.detect(window.reset_index(drop=True)).is_anomaly) if f]
    assert sorted(e.ts.isoformat() for e in ctx.anomalies) == sorted(pd.Timestamp(t).isoformat() for t in expected)


# ── 載入期檢查 ──


@pytest.mark.parametrize("name,params,error", [
    ("detector.hourly_baseline", {"snr": {"p_fa": 0}}, WorkflowParamError),
    ("detector.hourly_baseline", {"snr": {"p_fa": 0.6}}, WorkflowParamError),
    ("detector.hourly_baseline", {"snr": {"tz": TZ}}, WorkflowParamError),  # 沒給 p_fa
    ("detector.hourly_baseline", {"snr": {"p_fa": 0.001, "pfa": 1}}, WorkflowError),
    ("detector.hourly_baseline", {"snr": 0.001}, WorkflowError),
    ("detector.hourly_baseline", {"snr": {"p_fa": 0.001, "tz": "Mars/Olympus"}}, WorkflowError),
    ("detector.hourly_baseline", {"snr": {"p_fa": 0.001, "min_slot_points": 1}}, WorkflowError),
])
def test_bad_snr_params_fail_at_load_time(name, params, error):
    with pytest.raises(error):
        OperatorRegistry.create(name, params)


def test_snr_needs_a_threshold_to_replace():
    _NoThreshold()  # 不開 snr：合法
    with pytest.raises(TypeError, match="沒有 threshold，不能用 snr 換算門檻"):
        _NoThreshold(snr={"p_fa": 0.001})


def test_snr_cannot_be_set_per_target():
    with pytest.raises(TypeError):
        OperatorRegistry.create("detector.hourly_baseline").check_override({"snr": SNR})


# ── 軟投票 ──


def _vote_ctx(weights: dict[str, list[float]], threshold=2.0):
    ts = pd.date_range(START, periods=len(next(iter(weights.values()))), freq="60s")
    ctx = _ctx(pd.DataFrame({"ts": ts, "value": 100.0}))
    stamps = [t.isoformat() for t in ts]
    for node, w in weights.items():
        ctx.detector_runs.append(node)
        ctx.detector_evidence[node] = DetectorEvidence(stamps, [x * threshold for x in w], threshold, "gaussian", 0.001)
        for t, x in zip(ts, w):
            if x > 1:
                ctx.anomalies.append(AnomalyEvent("svc", "q", t.to_pydatetime(), x * threshold, {}, 100.0,
                                                  "detector.x", node, "mock", (), ANOMALY))
    return ctx


def test_soft_vote_adds_weights_in_units_of_the_threshold():
    vote = OperatorRegistry.create("detector.soft_vote")
    vote.node_name = "vote"
    ctx = vote.execute(_vote_ctx({"a": [1.0, 0.7, 2.3, 0.9, 0.0],
                                  "b": [1.0, 0.7, 0.0, 0.9, 0.0],
                                  "c": [0.0, 0.7, 0.0, 0.0, 0.0]}))
    fired = sorted((e.ts.minute, e.voted_by) for e in ctx.anomalies)
    # 1+1 成立；0.7×3=2.1 成立（布林多數決一票都不算）；單票 2.3 倍成立；0.9+0.9=1.8 不成立
    assert fired == [(0, ("a", "b")), (1, ("a", "b", "c")), (2, ("a",))]
    assert all(e.detector == "detector.soft_vote" for e in ctx.anomalies)  # 成員事件拿掉，只留裁決


def test_soft_vote_keeps_bypass_events_and_needs_snr_on_every_voter():
    vote = OperatorRegistry.create("detector.soft_vote", {"bypass": ["solo"]})
    vote.node_name = "vote"
    ctx = _vote_ctx({"a": [0.5], "solo": [5.0]})
    out = vote.execute(ctx)
    assert [e.detector_node for e in out.anomalies] == ["solo"]  # solo 不進權重，自己的事件原樣留下

    def workflow(nodes):
        return Workflow(name="sv", targets=[TargetSpec(source_type="mock", query="q", entity="svc")], nodes=[
            NodeSpec(name="fetch", operator="ingestion.fetch"), *nodes])

    Pipeline(workflow([NodeSpec(name="m", operator="detector.hourly_baseline", params={"snr": SNR}),
                       NodeSpec(name="vote", operator="detector.soft_vote")]))
    with pytest.raises(WorkflowError, match="沒有開 snr"):
        Pipeline(workflow([NodeSpec(name="m", operator="detector.hourly_baseline"),
                           NodeSpec(name="vote", operator="detector.soft_vote")]))
    with pytest.raises(WorkflowError, match="bypass"):  # 不投票的節點（votes = False）沒列進 bypass
        Pipeline(workflow([NodeSpec(name="f", operator="detector.sparse_count"),
                           NodeSpec(name="vote", operator="detector.soft_vote")]))
    with pytest.raises(WorkflowParamError):
        OperatorRegistry.create("detector.soft_vote", {"votes": 0})


def test_snr_variant_loads():
    """R+SNR 是 sample_it_otel 原樣（對照變體 A+SNR 用的 mad／quantile_drift 已移除）。"""
    wf = Workflow(name="sv", targets=[TargetSpec(source_type="mock", query="q", entity="svc")],
                  nodes=detection_nodes("snr_test.frame", "R+SNR"))
    pipe = Pipeline(wf)
    vote = next(op for n, op in pipe.target_nodes if n.operator == "detector.soft_vote")
    voters = [op for n, op in pipe.target_nodes if getattr(op, "has_baseline", False) and n.name not in vote.bypass]
    assert len(voters) == 3 and all(op.snr["p_fa"] == 0.001 for op in voters)
    assert vote.bypass == {"detect_drift_solo"}  # 旁路漂移票不投票


# ── 低流量序列的輕微下降（ES 回放的 report-svc mild） ──


class _Frames:
    raw = None
    train = None


@OperatorRegistry.register("snr_test.frame")
class _Frame(BaseOperator):
    scope = "target"

    def execute(self, ctx):
        ctx.raw_df, ctx.train_df = _Frames.raw, _Frames.train
        return ctx


def _low_traffic_mild():
    """report-svc 的形狀：上班時段約 40 筆／分、其餘 15，週末更低，Poisson 雜訊；訓練 14 天，測試週某個週三中午低兩成 60 分。"""
    ts = pd.date_range(START, periods=21 * 1440, freq="60s")
    loc = ts.tz_convert(TPE)
    office, weekend = (loc.hour >= 9) & (loc.hour < 19), loc.day_of_week >= 5
    base = np.where(weekend, np.where(office, 20.0, 8.0), np.where(office, 40.0, 15.0))
    df = pd.DataFrame({"ts": ts, "value": np.random.default_rng(3).poisson(base).astype(float)})
    train, test = df.iloc[:14 * 1440].reset_index(drop=True), df.iloc[14 * 1440:].reset_index(drop=True)
    tloc = test["ts"].dt.tz_convert(TPE)
    s = int(np.flatnonzero(((tloc.dt.day_of_week == 2) & (tloc.dt.hour == 12) & (tloc.dt.minute == 0)).to_numpy())[0])
    test.loc[s:s + 59, "value"] = np.round(test.loc[s:s + 59, "value"] * 0.8)
    return train, test.iloc[s:s + 60].reset_index(drop=True)


def _alerts(variant: str, train, window) -> int:
    """跑變體的偵測段一輪：裁決（soft_vote）∪ bypass 節點的告警分鐘數。"""
    _Frames.raw, _Frames.train = window, train
    wf = Workflow(name="snr_test", targets=[TargetSpec(source_type="mock", query="rpm", entity="svc")],
                  nodes=detection_nodes("snr_test.frame", variant))
    batch = Pipeline(wf).run(now=window["ts"].iloc[-1].to_pydatetime() + timedelta(seconds=30), strict=True)
    return len({e.ts for e in batch.anomalies if e.level == ANOMALY})


def test_residual_snr_catches_the_low_traffic_mild_drop():
    train, window = _low_traffic_mild()
    assert _alerts("R+SNR", train, window) >= 10
