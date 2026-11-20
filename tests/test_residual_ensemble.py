"""殘差白化的偵測器（detector.residual_shift／residual_chronic）。管線測試跑 sample_it_otel 的偵測段：
三個節點開 snr、各自成立，任何一個越線就是告警（2026-11-20 移除軟投票 detector.soft_vote、旁路漂移票、min_run、
尾巴校正 fpr／max_tail_factor、分母底噪；那些機制自己的測試跟著刪掉，其餘照新行為改寫，改了哪些數字寫在各測試）。

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
from scripts.backtest.variants import detection_nodes
from aiops.stages.detectors.base import (fit_seasonal_profile, guard_scale, noise_stats, seasonal_baseline,
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


# ── 雜訊尺度 ──


def test_noise_scale_tracks_each_series_own_noise():
    """同形狀、雜訊差 10 倍的兩條序列：分母（σ = 1.4826 × MAD）自己差 10 倍，同一個 threshold 對兩條都適用，不用手調。"""
    rng = np.random.default_rng(1)
    quiet, loud = rng.normal(0, 3, 20000), rng.normal(0, 30, 20000)
    s_quiet, s_loud = noise_stats(quiet)["noise"], noise_stats(loud)["noise"]
    assert s_loud / s_quiet == pytest.approx(10, rel=0.1)
    assert s_quiet == pytest.approx(3.0, rel=0.05)  # 常態雜訊：穩健 σ 就是標準差


def test_heavy_tails_are_no_longer_absorbed_by_the_scale():
    """尾巴比常態肥的雜訊（自由度 2 的 t 分布）：分母只有穩健 σ，沒有尾巴校正。
    改版前：σ* = min(max(σ, Q_0.999 / 4), 3σ) = 3σ（撞到上限）；max_tail_factor 放寬到 20 時 σ* = Q_0.999 / 4，
    訓練資料自己超過門檻 4 的點 <= 0.1%（fpr）。
    改版後：分母 = σ。訓練資料自己超過門檻 4 的比例照實量在下面（OVER_NOW），並跟改版前預設（上限 3σ）的比例並列。
    這是移除尾巴校正的直接後果。"""
    rng = np.random.default_rng(2)
    heavy = rng.standard_t(2, 20000)
    stats = noise_stats(heavy)
    assert set(stats) == {"noise", "points"}  # 不再量 1−fpr 分位數
    over_now = float(np.mean(np.abs(heavy) / guard_scale(stats["noise"]) > 4.0))
    over_before = float(np.mean(np.abs(heavy) / (3 * stats["noise"]) > 4.0))  # 改版前預設：分母撞到上限 3σ
    assert over_now == pytest.approx(OVER_NOW, abs=0.0005) and over_before == pytest.approx(OVER_BEFORE, abs=0.0005)
    assert over_now > 5 * over_before


OVER_NOW, OVER_BEFORE = 0.0397, 0.00405  # 量到的值：改版後 3.97% 的點越線，改版前（上限 3σ）0.41%，約 10 倍


def test_flat_training_gets_scale_one_instead_of_learning_the_jump():
    """MAD 為 0（幾乎不動，只有零星跳點）：不退標準差（會被跳點自己撐大），分母用 guard_scale 的 1.0（只防除以 0）。
    改版前這裡落到底噪（整數序列 1、或 min_scale 2.5）；底噪已移除，1.0 不是底噪、只是不除以 0。"""
    flat = np.r_[np.zeros(500), 40.0, np.zeros(500)]
    stats = noise_stats(flat)
    assert stats["noise"] == 0.0 and guard_scale(stats["noise"]) == 1.0


def test_old_baselines_with_tail_and_floor_bookkeeping_still_load(three_weeks):
    """舊基準線的 noise 多帶 tail_q／fpr，state 多帶 level／integer：都不讀，分數跟新訓練的一樣（σ 本來就存在 noise 裡）。"""
    train, test = three_weeks
    for name in ("detector.residual_shift", "detector.residual_chronic"):
        fresh = _create(name).fit(train)
        old = json.loads(json.dumps(fresh.get_state()))
        old["noise"].update(tail_q=123.4, fpr=0.001)
        old.update(level=291.0, integer=True)
        assert _create(name).load_state(old).detect(test).to_dict() == fresh.detect(test).to_dict()


# ── 兩個算子 ──


def test_residual_shift_is_quiet_at_the_daily_switches_where_plain_shift_fires(three_weeks):
    train, test = three_weeks
    near = _near_switch(test["ts"])
    rshift = _create("detector.residual_shift").fit(train)
    r_flags = np.array(rshift.detect(test).is_anomaly)
    # 對照組：原始值的一階跳變（相鄰差 ÷ 訓練期相鄰差的標準差 > 4，沒有白化）
    diff_std = train["value"].diff().dropna().std(ddof=0)
    p_flags = (test["value"].diff().fillna(0.0).abs() / diff_std > 4).to_numpy()
    assert p_flags[near].sum() >= 10  # 原值的跳變：每個平日 09:00／19:00 都舉牌
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
    """對照組（問題本身）：原始值的滾動 30 點平均跟整段訓練平均比（尺度 std/√30，門檻 3），上班時段整段都偏離。"""
    train, test = three_weeks
    mean, std = train["value"].mean(), train["value"].std(ddof=0)
    rolling = test["value"].rolling(30, min_periods=30).mean()
    flags = ((rolling - mean).abs() / (std / np.sqrt(30)) > 3).to_numpy()
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
    """threshold 在 detect 時才套：改了不用等重訓。"""
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


# ── 載入期參數檢查 ──


@pytest.mark.parametrize("name,params,error", [
    ("detector.residual_shift", {"threshold": 0.5}, WorkflowParamError),
    ("detector.residual_shift", {"fpr": 0.01}, WorkflowError),             # 已移除的參數：寫了就報錯
    ("detector.residual_shift", {"max_tail_factor": 5.0}, WorkflowError),
    ("detector.residual_shift", {"tz": "Mars/Olympus"}, WorkflowError),
    ("detector.residual_shift", {"min_slot_points": 1}, WorkflowError),
    ("detector.residual_shift", {"ref_points": 0}, WorkflowError),
    ("detector.residual_shift", {"windw": 3}, WorkflowError),
    ("detector.residual_chronic", {"window": 2}, WorkflowParamError),
    ("detector.residual_chronic", {"threshold": 0.5}, WorkflowParamError),
    ("detector.residual_chronic", {"tz": "nowhere"}, WorkflowError),
    ("detector.residual_chronic", {"min_run": 10}, WorkflowError),         # 已移除的參數
])
def test_bad_params_fail_at_load_time(name, params, error):
    with pytest.raises(error):
        OperatorRegistry.create(name, params)


def test_target_override_is_range_checked_at_load_time():
    op = _create("detector.residual_chronic")
    op.check_override({"threshold": 4.0, "window": 20})
    with pytest.raises(ValueError):
        op.check_override({"window": 1.0})
    with pytest.raises(TypeError):
        op.check_override({"min_scale": 0.5})  # 已移除的參數
    with pytest.raises(TypeError):
        op.check_override({"tz": "UTC", "bogus": 1})


# ── 管線 ──


def _detection_nodes() -> list[NodeSpec]:
    return detection_nodes("residual_test.frame")  # sample_it_otel 的偵測段原樣


def test_it_otel_detects_with_three_independent_nodes_and_no_vote():
    """sample_it_otel 的偵測段：三個互補的節點（週期、殘差突變、殘差漂移），各自成立；沒有表決節點、沒有旁路漂移票。"""
    pipe = Pipeline(load_workflow(workflow_path("sample_it_otel")))
    detectors = [(n.name, n.operator) for n, op in pipe.target_nodes if getattr(op, "has_baseline", False)]
    assert detectors == [("detect", "detector.hourly_baseline"), ("detect_rshift", "detector.residual_shift"),
                         ("detect_rchronic", "detector.residual_chronic")]
    assert not [n.name for n, _ in pipe.target_nodes if "vote" in n.operator or "ensemble" in n.operator]
    # 測試用的偵測段跟出貨樣本是同一份（只有 fetch 換成固定資料）
    assert [(n.name, n.operator) for n in _detection_nodes()[1:]] == detectors


class _Frames:
    raw: pd.DataFrame | None = None
    train: pd.DataFrame | None = None


@OperatorRegistry.register("residual_test.frame")
class _Frame(BaseOperator):
    scope = "target"

    def execute(self, ctx):
        ctx.raw_df, ctx.train_df = _Frames.raw, _Frames.train
        return ctx


def _run(train: pd.DataFrame, window: pd.DataFrame):
    _Frames.raw, _Frames.train = window.reset_index(drop=True), train
    wf = Workflow(name="residual_test", nodes=_detection_nodes(),
                  targets=[TargetSpec(source_type="mock", query="rpm", entity="svc")])
    return Pipeline(wf).run(now=window["ts"].iloc[-1].to_pydatetime() + timedelta(seconds=30), strict=True)


def _alert_minutes(train: pd.DataFrame, window: pd.DataFrame, node: str | None = None) -> int:
    """跑偵測段一輪（60 分鐘），這一輪會發告警的分鐘：任何一個偵測節點越線。node：只算那個節點自己的事件。"""
    batch = _run(train, window)
    return len({e.ts for e in batch.anomalies if e.level == ANOMALY and node in (None, e.detector_node)})


def _firing_nodes(train: pd.DataFrame, window: pd.DataFrame) -> set:
    return {e.detector_node for e in _run(train, window).anomalies if e.level == ANOMALY}


def test_pipeline_is_silent_across_the_0900_switch_and_catches_a_cliff(three_weeks):
    """改寫：以前數的是軟投票裁決通過的分鐘，現在數任何節點越線的分鐘。斷言的數字沒動；
    這份資料上 09:00 前後一小時改版前後都是 0 分鐘，斷崖都是 20 分鐘（改版前靠三票加總，現在 hourly 與漂移各自 20 分鐘）。"""
    train, test = three_weeks
    loc = test["ts"].dt.tz_convert(TPE)
    nine = int(np.flatnonzero(((loc.dt.day_of_week == 1) & (loc.dt.hour == 9) & (loc.dt.minute == 0)).to_numpy())[0])
    assert _alert_minutes(train, test.iloc[nine - 30:nine + 30]) == 0
    s = _weekday_noon(test)
    cliff = test.copy()
    cliff.loc[s:s + 19, "value"] *= 0.3
    window = cliff.iloc[s - 40:s + 20]
    assert _alert_minutes(train, window) >= 19 and {"detect", "detect_rchronic"} <= _firing_nodes(train, window)


def _low_traffic_mild():
    """report-svc 的形狀：上班時段約 40 筆／分、其餘 15，週末更低，Poisson 雜訊（std 約 6）。訓練 14 天。"""
    train, test = _split(_series(21, seed=3, poisson=True, day=40.0, night=15.0, weekend_day=20.0,
                                 weekend_night=8.0), 14)
    s = _weekday_noon(test)
    mild = test.copy()
    mild.loc[s:s + 59, "value"] = np.round(mild.loc[s:s + 59, "value"] * 0.8)
    return train, mild.iloc[s:s + 60]


def test_mild_drop_on_a_busy_series_is_caught(three_weeks):
    """291 筆／分、雜訊 9 的序列，持續低兩成（-58）：hourly 與 residual_chronic 都越線。
    告警分鐘：改版前 49／60（軟投票加總到 2 票等值的分鐘），改版後 60／60（hourly 單獨 60、漂移 31）。斷言的數字沒動。"""
    train, test = three_weeks
    s = _weekday_noon(test)
    mild = test.copy()
    mild.loc[s:s + 59, "value"] *= 0.8
    window = mild.iloc[s:s + 60]
    assert _alert_minutes(train, window) >= 25 and {"detect", "detect_rchronic"} <= _firing_nodes(train, window)


def test_mild_drop_on_a_low_traffic_noisy_series_is_caught():
    """以前是 xfail(strict=True) 的死區：低流量、雜訊大的序列持續低兩成（約 -8 筆／分）主要只有 residual_chronic 看得到，
    軟投票 60 分鐘裡只有 13 分鐘加總到 2 票等值（要靠旁路漂移票補到 28 分鐘）。
    移除軟投票之後漂移節點單獨成立，這份合成資料上 31／60 分鐘告警（全部來自 detect_rchronic），死區不存在了，
    xfail 標記拿掉；門檻 >= 30 是原本那個 xfail 測試的斷言，沒有動。"""
    train, window = _low_traffic_mild()
    assert _alert_minutes(train, window) >= 30
    assert _alert_minutes(train, window, node="detect_rchronic") >= 30


def test_normal_noon_hour_stays_quiet(three_weeks):
    """正常的一小時（測試週週三 12:00–12:59）沒有告警：改版前 0 分鐘，改版後也是 0 分鐘（這份資料上量到的）。
    原本的名稱是 test_solo_drift_track_stays_quiet_on_a_normal_day（旁路漂移票已移除），斷言沒動。"""
    train, test = three_weeks
    s = _weekday_noon(test)
    assert _alert_minutes(train, test.iloc[s:s + 60]) == 0


def test_low_traffic_mild_drop_is_seen_by_the_drift_detector_alone():
    """低流量輕微下降只有漂移偵測器看得到（hourly 逐點看不到）：所以以前要兩票等值的裁決會漏。"""
    train, window = _low_traffic_mild()
    rchronic = _create("detector.residual_chronic").fit(train)
    hourly = OperatorRegistry.create("detector.hourly_baseline", {"tz": TZ, "min_weeks": 1}).fit(train)
    assert sum(rchronic.detect(window.reset_index(drop=True)).is_anomaly) >= 15
    assert sum(hourly.detect(window.reset_index(drop=True)).is_anomaly) <= 5


# ── 正常週有多吵（移除減少誤報的機制之後，照實量） ──


def _normal_week_minutes(train, test) -> tuple[int, dict]:
    """三個節點用 p_fa = 0.001 換算的門檻（3.29）對整個正常的測試週判一次：任何一個越線的分鐘數，與各節點自己的分鐘數。"""
    from aiops.stages.detectors.base import gaussian_threshold

    z = gaussian_threshold(0.001)
    flags = {
        "detect": _create("detector.hourly_baseline", min_weeks=1, threshold=z).fit(train).detect(test).is_anomaly,
        "detect_rshift": _create("detector.residual_shift", threshold=z).fit(train).detect(test).is_anomaly,
        "detect_rchronic": _create("detector.residual_chronic", threshold=z).fit(train).detect(test).is_anomaly,
    }
    union = np.logical_or.reduce([np.array(f) for f in flags.values()])
    return int(union.sum()), {k: int(sum(f)) for k, f in flags.items()}


@pytest.mark.parametrize("label,series_kw,train_days,expected", [
    # 改版前（軟投票 2 票等值 ∪ 旁路漂移票 min_run 10，尾巴校正與底噪都在）：29 分鐘／週
    ("busy-14d", dict(days=21), 14, (31, {"detect": 20, "detect_rshift": 12, "detect_rchronic": 2})),
    # 改版前：20 分鐘／週（各票單獨：hourly 20、突變 6、漂移 1）
    ("low-traffic-14d", dict(days=21, seed=3, poisson=True, day=40.0, night=15.0, weekend_day=20.0, weekend_night=8.0), 14,
     (133, {"detect": 20, "detect_rshift": 72, "detect_rchronic": 46})),
    # 改版前：93 分鐘／週（訓練只有 7 天，每個時段只看過一次，雜訊量得偏小）
    ("busy-7d", dict(days=14, seed=5), 7, (290, {"detect": 19, "detect_rshift": 6, "detect_rchronic": 268})),
    # 改版前：110 分鐘／週
    ("low-traffic-7d", dict(days=14, seed=7, poisson=True, day=40.0, night=15.0, weekend_day=20.0, weekend_night=8.0), 7,
     (681, {"detect": 17, "detect_rshift": 108, "detect_rchronic": 579})),
])
def test_normal_week_alert_minutes_after_removing_the_false_alarm_mechanisms(label, series_kw, train_days, expected):
    """沒有注入任何異常的合成測試週（7 天 = 10080 分鐘）會有幾分鐘告警。這不是「該有的值」，是移除軟投票、min_run、
    尾巴校正、分母底噪之後量到的現況，釘住是為了之後有人動到分母或門檻時看得到變化。改版前的數字寫在各案例上面
    （scratch 腳本在同一份合成資料上量的：軟投票用各節點分數 / 3.29 加總 >= 2 重算，不是跑完整管線）。
    只在合成資料上量過，沒有用真實資料重跑（docs/RESIDUAL_SNR_ENSEMBLE.md 的 ES 數字是移除前的）。"""
    train, test = _split(_series(**series_kw), train_days)
    assert _normal_week_minutes(train, test) == expected
