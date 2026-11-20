"""序列 SNR、由 p_fa 換算門檻（節點參數 snr）。軟投票（detector.soft_vote）與逐點證據（ctx.detector_evidence）已於
2026-11-20 移除，它們的測試跟著刪掉；檔名沿用。回放數字（移除前量的）在 docs/RESIDUAL_SNR_ENSEMBLE.md。"""
import json
import math
from datetime import timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from aiops.engine.context import PipelineContext, Target
from aiops.engine.errors import WorkflowError, WorkflowParamError
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import Pipeline, TargetSpec, Workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import ANOMALY
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


def test_zero_noise_falls_back_to_one_instead_of_dividing_by_zero():
    df = _series(noise=0.0)
    stats = series_snr(df, TPE, 30)
    # 殘差 MAD = 0：σ 用 guard_scale 的 1.0（以前是底噪 max(整數序列 1, |中位數| × 1%) = 1.35；底噪已移除）
    assert stats["noise_floored"] and stats["noise_sigma"] == 1.0
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


# ── 節點層：門檻、狀態、預設不變 ──


def _ctx(df, **kw):
    ctx = PipelineContext(pipeline="p", now=df["ts"].iloc[-1].to_pydatetime() + timedelta(minutes=1),
                          target=Target("mock", "q", "svc", {}))
    ctx.raw_df = df.reset_index(drop=True)
    for k, v in kw.items():
        setattr(ctx, k, v)
    return ctx


def _events(ctx):
    return sorted((e.ts, e.score, e.level) for e in ctx.anomalies)


def test_snr_node_uses_the_p_fa_threshold_and_stores_stats_with_the_baseline(train):
    window = _series(days=15).iloc[-60:].copy()
    window.iloc[-20:, window.columns.get_loc("value")] -= 40.0  # 最後 20 分鐘掉 40（約 4.4 個雜訊單位）：要有事件可比
    op = OperatorRegistry.create("detector.residual_chronic", {"tz": TZ, "snr": SNR})
    ctx = op.execute(_ctx(window, train_df=train))
    state = ctx.new_detector_states[op.state_key]
    stored = json.loads(json.dumps(state.state))
    assert stored[SNR_STATE_KEY]["threshold"] == pytest.approx(3.29053, abs=1e-4)
    assert stored[SNR_STATE_KEY]["threshold_source"] == "gaussian" and stored[SNR_STATE_KEY]["snr_db"] > 10
    assert any(line.startswith(f"snr:{op.state_key}:db=") and line.endswith(":threshold=3.291:gaussian") for line in ctx.audit)
    # 事件就是「分數 > p_fa 換算的門檻」的點（不是演算法預設的 threshold 3）
    scores = ctx.anomaly_result.scores
    assert ctx.anomalies and len(ctx.anomalies) == sum(s > stored[SNR_STATE_KEY]["threshold"] for s in scores)
    # 下一輪載入同一份基準線：事件一模一樣（演算法的 load_state 看不到 "_snr" 那層）
    state.state = stored
    again = op.execute(_ctx(window, detector_states={op.state_key: state}))
    assert _events(again) == _events(ctx) and again.anomaly_result.scores == scores


def test_old_baseline_without_snr_stats_falls_back_to_the_configured_threshold(train):
    window = _series(days=15).iloc[-60:]
    plain = OperatorRegistry.create("detector.hourly_baseline", {"tz": TZ, "min_weeks": 1})
    old_state = plain.execute(_ctx(window, train_df=train)).new_detector_states[plain.state_key]
    snr_op = OperatorRegistry.create("detector.hourly_baseline", {"tz": TZ, "min_weeks": 1, "snr": SNR})
    ctx = snr_op.execute(_ctx(window, detector_states={snr_op.state_key: old_state}))
    assert f"snr:{snr_op.state_key}:db=n/a:threshold=3.0:fallback" in ctx.audit


@pytest.mark.parametrize("name,params", [("detector.hourly_baseline", {"tz": TZ, "min_weeks": 1}),
                                         ("detector.residual_shift", {"tz": TZ})])
def test_without_snr_nothing_changes(train, name, params):
    """沒開 snr：state 沒有 "_snr"、事件就是演算法自己的 threshold 判的（開了 snr 也不會改到沒開的節點）。"""
    window = _series(days=15).iloc[-60:]
    ctx = OperatorRegistry.create(name, params).execute(_ctx(window, train_df=train))
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


# ── 樣本管線的偵測段 ──


def test_sample_detection_segment_has_three_snr_nodes_and_no_vote_node():
    """sample_it_otel 的偵測段：三個節點都開 snr，各自成立；沒有表決節點、沒有旁路漂移票（2026-11-20 移除）。"""
    wf = Workflow(name="sv", targets=[TargetSpec(source_type="mock", query="q", entity="svc")],
                  nodes=detection_nodes("snr_test.frame"))
    pipe = Pipeline(wf)
    assert [(n.name, n.operator) for n, _ in pipe.target_nodes[1:]] == [
        ("detect", "detector.hourly_baseline"), ("detect_rshift", "detector.residual_shift"),
        ("detect_rchronic", "detector.residual_chronic")]
    assert all(op.snr["p_fa"] == 0.001 for _n, op in pipe.target_nodes[1:])
    with pytest.raises(WorkflowError):
        OperatorRegistry.create("detector.soft_vote")  # 算子不存在了


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


def _alerts(train, window) -> int:
    """跑樣本管線的偵測段一輪：任何一個偵測節點越線的分鐘數。"""
    _Frames.raw, _Frames.train = window, train
    wf = Workflow(name="snr_test", targets=[TargetSpec(source_type="mock", query="rpm", entity="svc")],
                  nodes=detection_nodes("snr_test.frame"))
    batch = Pipeline(wf).run(now=window["ts"].iloc[-1].to_pydatetime() + timedelta(seconds=30), strict=True)
    return len({e.ts for e in batch.anomalies if e.level == ANOMALY})


def test_residual_snr_catches_the_low_traffic_mild_drop():
    train, window = _low_traffic_mild()
    assert _alerts(train, window) >= 10
