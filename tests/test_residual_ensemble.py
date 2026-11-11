"""殘差白化＋自適應門檻的三票（detector.residual_shift／residual_chronic，管線 it_otel_residual）。

合成序列照 ES 回測裡 mes-api 的形狀做：台北時間平日 09:00–18:59 約 291、其餘 135，週末 145／75，分鐘雜訊 std 9。
ES 6 條序列的回放數字在 docs/RESIDUAL_SNR_ENSEMBLE.md（scripts/backtest/residual_ensemble.py）。
"""
import json
from datetime import timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from aiops.engine.errors import WorkflowError, WorkflowParamError
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import ANOMALY
from aiops.interfaces.runner import workflow_path
from aiops.stages.detectors.base import (calibrated_scale, fit_seasonal_profile, noise_stats, seasonal_baseline,
                                         seasonal_residual)

TZ = "Asia/Taipei"
TPE = ZoneInfo(TZ)
START = pd.Timestamp("2026-10-12 00:00", tz=TPE).tz_convert("UTC")  # 週一 00:00（台北）


def _level(ts: pd.DatetimeIndex, day=291.0, night=135.0, weekend_day=145.0, weekend_night=75.0) -> np.ndarray:
    loc = ts.tz_convert(TPE)
    office = (loc.hour >= 9) & (loc.hour < 19)
    weekend = loc.day_of_week >= 5
    return np.where(weekend, np.where(office, weekend_day, weekend_night), np.where(office, day, night)).astype(float)


def _series(days: int, seed: int = 0, noise: float = 9.0, start=START, poisson=False, **level) -> pd.DataFrame:
    ts = pd.date_range(start, periods=days * 1440, freq="60s")
    base = _level(ts, **level)
    rng = np.random.default_rng(seed)
    values = rng.poisson(base).astype(float) if poisson else np.round(base + rng.normal(0, noise, len(ts)))
    return pd.DataFrame({"ts": ts, "value": values})


def _split(df: pd.DataFrame, train_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    cut = df["ts"].iloc[0] + pd.Timedelta(days=train_days)
    return df[df["ts"] < cut].reset_index(drop=True), df[df["ts"] >= cut].reset_index(drop=True)


def _near_switch(ts: pd.Series, pad: int = 5) -> np.ndarray:
    loc = ts.dt.tz_convert(TPE)
    minute = (loc.dt.hour * 60 + loc.dt.minute).to_numpy()
    return (np.abs(minute - 9 * 60) <= pad) | (np.abs(minute - 19 * 60) <= pad)


def _weekday_noon(test: pd.DataFrame) -> int:
    loc = test["ts"].dt.tz_convert(TPE)
    return int(np.flatnonzero(((loc.dt.day_of_week == 2) & (loc.dt.hour == 12) & (loc.dt.minute == 0)).to_numpy())[0])


def _create(name, **params):
    return OperatorRegistry.create(name, {"tz": TZ, **params})


@pytest.fixture(scope="module")
def three_weeks():
    return _split(_series(21), 14)


# ── 白化 ──


def test_whitening_removes_the_daily_steps_and_weekend(three_weeks):
    train, test = three_weeks
    profile, train_resid = fit_seasonal_profile(train, TPE, 30)
    resid = seasonal_residual(test, profile)
    assert profile["slots_ready"] == 168
    # 原值的標準差被日夜／週末撐到 80 以上；扣掉時段基準後只剩分鐘雜訊（std 9）
    assert test["value"].std() > 60 and 7 < resid.std() < 11 and 7 < train_resid.std() < 11
    # 09:00／19:00 前後的相鄰差跟其他時間一樣小：每天固定的切換被扣掉了
    jumps = np.abs(np.diff(resid))
    assert np.abs(np.diff(test["value"].to_numpy()))[_near_switch(test["ts"])[1:]].max() > 100
    assert jumps[_near_switch(test["ts"])[1:]].max() < 6 * 9


def test_training_residual_is_leave_one_week_out_when_there_are_two_weeks():
    """14 天：每個點扣的是「別週同時段」的平均，量到的雜訊不會小得不真實；7 天只有一週可用，只能含自己。"""
    two = _series(14)
    one = _series(7)
    p2, r2 = fit_seasonal_profile(two, TPE, 30)
    p1, r1 = fit_seasonal_profile(one, TPE, 30)
    assert p2["lowo_share"] == 1.0 and p1["lowo_share"] == 0.0
    # 含自己的平均吃掉了一部分雜訊（每格 60 點：std 約小 1/120），留一週的不會
    assert r2.std() > r1.std()


def test_whitening_falls_back_slot_then_hour_then_global():
    """訓練只有週一、二：週六的時段沒資料 -> 退一天中的小時；小時也不夠（min_slot_points 很大）-> 全段平均。"""
    train = _series(2)
    profile, _ = fit_seasonal_profile(train, TPE, 30)
    saturday_10 = pd.Series([pd.Timestamp("2026-10-17 10:30", tz=TPE).tz_convert("UTC")])
    monday_10 = pd.Series([pd.Timestamp("2026-10-12 10:30", tz=TPE).tz_convert("UTC")])
    hour_mean = profile["hours"]["10"][0]
    assert profile["slots_ready"] == 48 and profile["hours_ready"] == 24
    assert seasonal_baseline(saturday_10, profile)[0] == pytest.approx(hour_mean)
    assert seasonal_baseline(monday_10, profile)[0] == pytest.approx(profile["slots"]["10"][0])
    sparse, _ = fit_seasonal_profile(train, TPE, 10_000)
    assert sparse["slots_ready"] == sparse["hours_ready"] == 0
    assert seasonal_baseline(saturday_10, sparse)[0] == pytest.approx(train["value"].mean())


# ── 自適應尺度 ──


def test_adaptive_scale_tracks_each_series_own_noise():
    """同形狀、雜訊差 10 倍的兩條序列：分母自己差 10 倍，同一個 threshold 對兩條都適用，不用手調。"""
    rng = np.random.default_rng(1)
    quiet, loud = rng.normal(0, 3, 20000), rng.normal(0, 30, 20000)
    kw = dict(min_scale=0.0, min_scale_ratio=0.0, level=0.0, integer=False)
    s_quiet = calibrated_scale(noise_stats(quiet, 0.001), 4.0, 3.0, **kw)
    s_loud = calibrated_scale(noise_stats(loud, 0.001), 4.0, 3.0, **kw)
    assert s_loud / s_quiet == pytest.approx(10, rel=0.1)
    # 常態雜訊：0.999 分位數約 3.3σ < 門檻 4σ，尾巴校正不介入，分母就是穩健 σ
    assert s_quiet == pytest.approx(3.0, rel=0.05)


def test_heavy_tails_widen_the_scale_but_only_up_to_the_cap():
    rng = np.random.default_rng(2)
    heavy = rng.standard_t(2, 20000)
    stats = noise_stats(heavy, 0.001)
    kw = dict(min_scale=0.0, min_scale_ratio=0.0, level=0.0, integer=False)
    capped = calibrated_scale(stats, 4.0, 3.0, **kw)
    loose = calibrated_scale(stats, 4.0, 20.0, **kw)
    assert stats["tail_q"] / 4.0 > 3 * stats["noise"]  # 尾巴比常態肥很多
    assert capped == pytest.approx(3 * stats["noise"])  # 上限 σ × max_tail_factor：寧可吵也不失明
    assert loose == pytest.approx(stats["tail_q"] / 4.0)
    # 訓練期最多 fpr 比例的點會超過門檻（沒被上限截掉時）
    assert np.mean(np.abs(heavy) / loose > 4.0) <= 0.001 + 1e-9


def test_flat_training_falls_to_the_noise_floor_instead_of_learning_the_jump():
    """MAD 為 0（幾乎不動，只有零星跳點）：不退標準差（會被跳點自己撐大），直接用底噪，跟 detector.mad 一樣。"""
    flat = np.r_[np.zeros(500), 40.0, np.zeros(500)]
    stats = noise_stats(flat, 0.001)
    assert stats["noise"] == 0.0
    assert calibrated_scale(stats, 4.0, 3.0, min_scale=0.0, min_scale_ratio=0.01, level=50.0, integer=True) == 1.0
    assert calibrated_scale(stats, 4.0, 3.0, min_scale=2.5, min_scale_ratio=0.01, level=50.0, integer=True) == 2.5


# ── 兩個算子 ──


def test_residual_shift_is_quiet_at_the_daily_switches_where_plain_shift_fires(three_weeks):
    train, test = three_weeks
    near = _near_switch(test["ts"])
    rshift = _create("detector.residual_shift").fit(train)
    plain = OperatorRegistry.create("detector.shift").fit(train)
    r_flags = np.array(rshift.detect(test).is_anomaly)
    p_flags = np.array(plain.detect(test).is_anomaly)
    assert p_flags[near].sum() >= 10  # 原本的 shift：每個平日 09:00／19:00 都舉牌
    assert r_flags[near].sum() == 0
    assert r_flags.mean() < 0.002


def test_residual_shift_flags_a_cliff_at_its_edge(three_weeks):
    train, test = three_weeks
    s = _weekday_noon(test)
    cliff = test.copy()
    cliff.loc[s:s + 19, "value"] *= 0.3
    flags = np.array(_create("detector.residual_shift").fit(train).detect(cliff).is_anomaly)
    assert flags[s] and flags[s + 20]  # 掉下去、回來
    assert not flags[s + 2:s + 19].any()  # 新水位不再報（持續偏離交給 residual_chronic）


def test_residual_chronic_is_quiet_on_a_normal_week_and_catches_a_mild_sustained_drop(three_weeks):
    train, test = three_weeks
    rchronic = _create("detector.residual_chronic").fit(train)
    normal = np.array(rchronic.detect(test).is_anomaly)
    assert normal.mean() < 0.002 and normal[_near_switch(test["ts"])].sum() == 0
    s = _weekday_noon(test)
    mild = test.copy()
    mild.loc[s:s + 59, "value"] *= 0.8
    flags = np.array(rchronic.detect(mild).is_anomaly)
    assert flags[s:s + 60].sum() >= 50


def test_plain_chronic_flags_whole_office_hours_on_the_same_data(three_weeks):
    """對照組（問題本身）：chronic 拿滾動平均跟整段訓練平均比，上班時段整段都偏離。"""
    train, test = three_weeks
    flags = np.array(OperatorRegistry.create("detector.chronic").fit(train).detect(test).is_anomaly)
    assert flags.mean() > 0.3


@pytest.mark.parametrize("name", ["detector.residual_shift", "detector.residual_chronic"])
def test_state_round_trip_on_seasonal_data(three_weeks, name):
    train, test = three_weeks
    trained = _create(name).fit(train)
    state = json.loads(json.dumps(trained.get_state()))
    assert state["profile"]["tz"] == TZ and state["noise"]["points"] > 10_000
    restored = _create(name).load_state(state)
    assert restored.detect(test).to_dict() == trained.detect(test).to_dict()


def test_threshold_change_applies_without_retraining(three_weeks):
    """threshold、max_tail_factor、底噪在 detect 時才套；fpr 要等重訓（分位數是在訓練資料上量的）。"""
    train, test = three_weeks
    state = _create("detector.residual_chronic").fit(train).get_state()
    low = _create("detector.residual_chronic", threshold=1.0).load_state(state)
    high = _create("detector.residual_chronic", threshold=10.0).load_state(state)
    assert sum(low.detect(test).is_anomaly) > sum(high.detect(test).is_anomaly)


def test_residual_chronic_keeps_the_trained_window_until_retrain(three_weeks):
    train, test = three_weeks
    state = _create("detector.residual_chronic").fit(train).get_state()
    algo = _create("detector.residual_chronic", window=10.0).load_state(state)
    result = algo.detect(test.iloc[:120].reset_index(drop=True))
    assert algo.notes == ["window-changed:30->10:until-retrain"]
    assert result.scores[28] == 0 and result.scores[29] != 0  # 照舊的 30 點窗口


def test_min_run_needs_the_drift_to_persist(three_weeks):
    """min_run：滾動平均連續越線 N 點才成立（不投票的旁路漂移票用）；沒撐滿的點分數歸 0。"""
    train, test = three_weeks
    s = _weekday_noon(test)
    mild = test.copy()
    mild.loc[s:s + 59, "value"] *= 0.8
    window = mild.iloc[s:s + 60].reset_index(drop=True)
    state = _create("detector.residual_chronic").fit(train).get_state()
    every = np.array(_create("detector.residual_chronic").load_state(state).detect(window).is_anomaly)
    persist = _create("detector.residual_chronic", min_run=10).load_state(state).detect(window)
    flags = np.array(persist.is_anomaly)
    first = int(np.argmax(every))
    assert every.any() and flags.any()
    assert int(np.argmax(flags)) == first + 9  # 第 10 個連續越線點才成立
    assert not any(persist.scores[:first + 9])


# ── 載入期參數檢查 ──


@pytest.mark.parametrize("name,params,error", [
    ("detector.residual_shift", {"threshold": 0.5}, WorkflowParamError),
    ("detector.residual_shift", {"fpr": 0.5}, WorkflowParamError),
    ("detector.residual_shift", {"max_tail_factor": 0.5}, WorkflowParamError),
    ("detector.residual_shift", {"tz": "Mars/Olympus"}, WorkflowError),
    ("detector.residual_shift", {"min_slot_points": 1}, WorkflowError),
    ("detector.residual_shift", {"ref_points": 0}, WorkflowError),
    ("detector.residual_shift", {"windw": 3}, WorkflowError),
    ("detector.residual_chronic", {"window": 2}, WorkflowParamError),
    ("detector.residual_chronic", {"fpr": 0.0}, WorkflowParamError),
    ("detector.residual_chronic", {"tz": "nowhere"}, WorkflowError),
    ("detector.residual_chronic", {"min_run": 0}, WorkflowError),
])
def test_bad_params_fail_at_load_time(name, params, error):
    with pytest.raises(error):
        OperatorRegistry.create(name, params)


def test_target_override_is_range_checked_at_load_time():
    op = _create("detector.residual_chronic")
    op.check_override({"threshold": 4.0, "min_scale": 0.5})
    with pytest.raises(ValueError):
        op.check_override({"fpr": 1.0})
    with pytest.raises(TypeError):
        op.check_override({"tz": "UTC", "bogus": 1})


# ── 管線 ──


def _detection_nodes(name: str) -> list[NodeSpec]:
    nodes = load_workflow(workflow_path(name)).nodes
    stop = [n.operator for n in nodes].index("detector.ensemble")
    return [NodeSpec(name="fetch", operator="residual_test.frame"), *nodes[1:stop + 1]]


def test_it_otel_residual_loads_with_three_complementary_votes_and_fatal_bypass():
    pipe = Pipeline(load_workflow(workflow_path("it_otel_residual")))
    ops = {n.name: n.operator for n, _ in pipe.target_nodes}
    vote = next(op for n, op in pipe.target_nodes if n.operator == "detector.ensemble")
    voters = [n.name for n, op in pipe.target_nodes if getattr(op, "has_baseline", False) and n.name not in vote.bypass]
    assert [ops[v] for v in voters] == ["detector.hourly_baseline", "detector.residual_shift",
                                        "detector.residual_chronic"]
    assert vote.strategy == "majority" and vote.bypass == {"fatal", "detect_drift_solo"}
    solo = next(op for n, op in pipe.target_nodes if n.name == "detect_drift_solo")
    assert solo.operator_name == "detector.residual_chronic" and solo.min_run > 1
    # 原本的 it_otel 不動：還是 hourly + mad + quantile_drift
    original = [n.operator for n in load_workflow(workflow_path("it_otel")).nodes]
    assert {"detector.mad", "detector.quantile_drift"} <= set(original)
    assert "detector.residual_shift" not in original


class _Frames:
    raw: pd.DataFrame | None = None
    train: pd.DataFrame | None = None


@OperatorRegistry.register("residual_test.frame")
class _Frame(BaseOperator):
    scope = "target"

    def execute(self, ctx):
        ctx.raw_df, ctx.train_df = _Frames.raw, _Frames.train
        return ctx


def _majority_minutes(train: pd.DataFrame, window: pd.DataFrame) -> tuple[int, set]:
    """跑 it_otel_residual 的偵測段一輪（60 分鐘），回傳 (裁決通過的分鐘數, 投過票的節點)。"""
    _Frames.raw, _Frames.train = window.reset_index(drop=True), train
    wf = Workflow(name="residual_test", nodes=_detection_nodes("it_otel_residual"),
                  targets=[TargetSpec(source_type="mock", query="rpm", entity="svc")])
    batch = Pipeline(wf).run(now=window["ts"].iloc[-1].to_pydatetime() + timedelta(seconds=30), strict=True)
    verdicts = [e for e in batch.anomalies if e.detector == "detector.ensemble" and e.level == ANOMALY]
    return len(verdicts), {n for e in verdicts for n in e.voted_by}


def _alert_minutes(train: pd.DataFrame, window: pd.DataFrame) -> int:
    """這一輪會發告警的分鐘：裁決通過的 ∪ bypass 節點（旁路漂移票、致命硬上限）自己的事件。"""
    _Frames.raw, _Frames.train = window.reset_index(drop=True), train
    wf = Workflow(name="residual_test", nodes=_detection_nodes("it_otel_residual"),
                  targets=[TargetSpec(source_type="mock", query="rpm", entity="svc")])
    batch = Pipeline(wf).run(now=window["ts"].iloc[-1].to_pydatetime() + timedelta(seconds=30), strict=True)
    return len({e.ts for e in batch.anomalies if e.level == ANOMALY})


def test_pipeline_majority_is_silent_across_the_0900_switch_and_catches_a_cliff(three_weeks):
    train, test = three_weeks
    loc = test["ts"].dt.tz_convert(TPE)
    nine = int(np.flatnonzero(((loc.dt.day_of_week == 1) & (loc.dt.hour == 9) & (loc.dt.minute == 0)).to_numpy())[0])
    quiet, _ = _majority_minutes(train, test.iloc[nine - 30:nine + 30])
    assert quiet == 0
    s = _weekday_noon(test)
    cliff = test.copy()
    cliff.loc[s:s + 19, "value"] *= 0.3
    caught, voters = _majority_minutes(train, cliff.iloc[s - 40:s + 20])
    assert caught >= 19 and {"detect", "detect_rchronic"} <= voters


def _low_traffic_mild():
    """report-svc 的形狀：上班時段約 40 筆／分、其餘 15，週末更低，Poisson 雜訊（std 約 6）。訓練 14 天。"""
    train, test = _split(_series(21, seed=3, poisson=True, day=40.0, night=15.0, weekend_day=20.0,
                                 weekend_night=8.0), 14)
    s = _weekday_noon(test)
    mild = test.copy()
    mild.loc[s:s + 59, "value"] = np.round(mild.loc[s:s + 59, "value"] * 0.8)
    return train, mild.iloc[s:s + 60]


def test_mild_drop_on_a_busy_series_reaches_majority(three_weeks):
    """對照組：291 筆／分、雜訊 9 的序列，持續低兩成（-58）時 hourly 與 residual_chronic 都投票。"""
    train, test = three_weeks
    s = _weekday_noon(test)
    mild = test.copy()
    mild.loc[s:s + 59, "value"] *= 0.8
    caught, voters = _majority_minutes(train, mild.iloc[s:s + 60])
    assert caught >= 25 and {"detect", "detect_rchronic"} <= voters


@pytest.mark.xfail(strict=True, reason="多數決的死區（ES 回測 report-svc mild 0/60；告警由旁路漂移票補上，見下一個測試）：低流量、雜訊大的序列持續低兩成（約 -8 筆／分，"
                                        "約 1.3 個時段標準差）只有 residual_chronic 看得到；hourly_baseline 與 residual_shift "
                                        "都不投票，三票多數決過不了。")
def test_mild_drop_on_a_low_traffic_noisy_series_reaches_majority():
    train, window = _low_traffic_mild()
    caught, _ = _majority_minutes(train, window)
    assert caught >= 30


def test_low_traffic_mild_drop_is_caught_by_the_solo_drift_track():
    """不漏報：多數決過不了（上面的 xfail），旁路漂移票（min_run 分鐘持續偏離）單獨發告警。"""
    train, window = _low_traffic_mild()
    assert _alert_minutes(train, window) >= 10


def test_solo_drift_track_stays_quiet_on_a_normal_day(three_weeks):
    train, test = three_weeks
    s = _weekday_noon(test)
    assert _alert_minutes(train, test.iloc[s:s + 60]) == 0


def test_low_traffic_mild_drop_is_seen_by_the_drift_vote_alone():
    """死區的原因釘在這裡：漂移票其實看到了，是多數決需要第二票。"""
    train, window = _low_traffic_mild()
    rchronic = _create("detector.residual_chronic").fit(train)
    hourly = OperatorRegistry.create("detector.hourly_baseline", {"tz": TZ, "min_weeks": 1}).fit(train)
    assert sum(rchronic.detect(window.reset_index(drop=True)).is_anomaly) >= 15
    assert sum(hourly.detect(window.reset_index(drop=True)).is_anomaly) <= 5
