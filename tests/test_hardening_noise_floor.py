"""分母只防除以 0（base.guard_scale）：量到的尺度有限而且 > 0 就照用，否則退到下一個候選，都沒有就 1.0。

2026-11-20 以前這個檔測的是分母底噪（docs/HARDENING_DESIGN.md 第 1 節）：
分母 = max(scale, min_scale, |中心| × min_scale_ratio, 整數序列 1)，hourly_baseline 另外有「時段 std 至少全段的一成」。
那些下限是減少誤報的機制，已經全部移除（決定：偵測階段要最敏感）。這裡留下的測試照實記錄移除後的行為——
幾個以前「不報」的情況現在會報，改版前後的數字寫在各測試的 docstring，沒有為了讓它們安靜而改輸入。
"""
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import guard_scale, is_integer_series

T0 = datetime(2026, 11, 2, tzinfo=timezone.utc)


def _df(values, start=T0):
    return pd.DataFrame({"ts": [start + timedelta(minutes=i) for i in range(len(values))], "value": [float(v) for v in values]})


def _failures(n=600, seed=3):
    """每分鐘失敗數：90% 是 0，其餘 1～2 筆。"""
    rng = np.random.default_rng(seed)
    return np.where(rng.random(n) < 0.9, 0, rng.integers(1, 3, n))


# ── 共用函式 ──

def test_guard_scale_uses_the_measured_scale_whenever_it_is_positive():
    assert guard_scale(3.0) == 3.0
    assert guard_scale(1e-9) == 1e-9  # 再小也照用：不墊高
    assert guard_scale(0.0, 0.4) == 0.4  # 第一個不是正數：退到下一個候選
    assert guard_scale(float("nan"), float("inf"), 2.5) == 2.5
    assert guard_scale(0.0) == 1.0 and guard_scale(0.0, 0.0) == 1.0 and guard_scale(None, -1.0) == 1.0  # 都沒有：1.0


def test_integer_series_detection():
    assert is_integer_series(pd.Series([0.0, 1.0, 2.0]))
    assert not is_integer_series(pd.Series([0.0, 1.5]))
    assert not is_integer_series(pd.Series([1.0, np.nan]))
    assert not is_integer_series(pd.Series([], dtype=float))


# ── 失敗數序列：沒有「整數序列分母至少 1」之後，平常的 2 筆失敗就越線 ──

def test_two_failures_on_a_sparse_failure_count_series_now_cross_the_line():
    """改版前（整數序列分母至少 1 筆）：分數 [0.125, 0.875, 1.875, 0.125, 39.875]，只有 40 筆那點越線；
    訓練資料自己重跑 600 點裡 0 點越線。
    改版後（分母 = 量到的全段 std 0.4428）：分數 [0.2823, 1.9762, 4.2347, 0.2823, 90.0589]，2 筆失敗（4.23 > 3）也越線；
    訓練資料自己重跑 600 點裡 26 點越線（訓練期所有「2 筆」的分鐘）。這是移除底噪的直接後果，照實記錄。"""
    train = _failures()
    mean, std = float(train.mean()), float(train.std())
    assert mean == 0.125 and 0 < std < 0.5
    algo = OperatorRegistry.get("detector.hourly_baseline")().fit(_df(train))
    assert algo.buckets_ready == 0 and algo.global_ == (mean, pytest.approx(std))  # 10 小時的訓練：整條退回全段統計
    r = algo.detect(_df([0, 1, 2, 0, 40]))
    assert r.scores == [0.2823, 1.9762, 4.2347, 0.2823, 90.0589]
    assert r.is_anomaly == [False, False, True, False, True]
    on_train = algo.detect(_df(train))
    assert sum(on_train.is_anomaly) == int((train == 2).sum()) == 26


def test_baseline_stores_the_measured_std_and_no_integer_flag():
    cls = OperatorRegistry.get("detector.hourly_baseline")
    train = _failures()
    state = json.loads(json.dumps(cls().fit(_df(train)).get_state()))
    assert state["global"][1] == pytest.approx(float(train.std())) and "integer" not in state


def test_loads_old_baselines_including_ones_carrying_the_removed_integer_key():
    """舊版基準線：全段 std 是 0（或趨近 0），有的多帶 integer。都載得進來。
    std = 0：分母退到 1.0，102 -> 2.0 分（改版前相對下限 100 × 1% = 1，剛好同一個數）。
    std = 1e-6：改版前同樣墊到 1 -> 2.0 分；改版後照量到的 1e-6 -> 2,000,000 分。"""
    cls = OperatorRegistry.get("detector.hourly_baseline")
    for extra in ({}, {"integer": True}):
        zero = cls().load_state({"buckets": {}, "global": [100.0, 0.0], **extra})
        assert zero.detect(_df([100.0, 102.0])).scores == [0.0, pytest.approx(2.0)]
        tiny = cls().load_state({"buckets": {}, "global": [100.0, 1e-6], **extra})
        assert tiny.detect(_df([100.0, 102.0])).scores == [0.0, pytest.approx(2_000_000.0)]


# ── 常數序列：std = 0 時不除以 0、也不全盲 ──

def test_fallback_on_a_constant_series_is_not_blind_and_does_not_divide_by_zero():
    algo = OperatorRegistry.get("detector.hourly_baseline")().fit(_df([100.0] * 120))
    assert algo.buckets_ready == 0 and algo.global_[1] == 0.0  # 退回全段統計，全段 std = 0
    r = algo.detect(_df([100.0, 100.5, 150.0]))
    assert r.scores == [0.0, 0.5, 50.0]  # 分母 = guard_scale 的 1.0（改版前底噪剛好也是 1，數字沒變）
    assert r.is_anomaly == [False, False, True]


def test_hourly_baseline_on_a_constant_series_is_not_blind():
    days = 14 * 1440
    algo = OperatorRegistry.get("detector.hourly_baseline")(min_weeks=1).fit(_df([20.0] * days))
    r = algo.detect(_df([20.0, 21.0, 60.0], start=T0 + timedelta(days=14)))
    assert r.scores == [0.0, 1.0, 40.0] and r.is_anomaly == [False, False, True]  # 時段 std 0 -> 全段 std 0 -> 1.0


# ── 幾乎不動的大數值序列：沒有相對下限之後，很小的變動分數就很高 ──

def test_a_tiny_measured_std_is_used_as_is_on_a_large_valued_series():
    """序列在 100 萬上下 ±0.5（std 0.5）。
    改版前（相對下限 = 中心的 1% = 10,000）：+20,000 -> 2.0 分不報、+40,000 -> 4.0 分報；+2 是 0.0002 分。
    改版後（分母 = 量到的 0.5）：+20,000 -> 40,000 分、+40,000 -> 80,000 分、+2 -> 4.0 分，三個都報。"""
    vals = [1_000_000.25 + (0.5 if i % 2 else -0.5) for i in range(120)]  # 非整數、std 0.5、中心 100 萬
    algo = OperatorRegistry.get("detector.hourly_baseline")().fit(_df(vals))
    r = algo.detect(_df([1_000_000.25 + 20_000, 1_000_000.25 + 40_000, 1_000_000.25 + 2]))
    assert r.scores == [pytest.approx(40_000.0), pytest.approx(80_000.0), pytest.approx(4.0)]
    assert r.is_anomaly == [True, True, True]


def test_scores_on_an_ordinary_series_are_plain_z_scores():
    rng = np.random.default_rng(5)
    vals = 1000 + rng.normal(0, 80, 600)
    test = 1000 + rng.normal(0, 80, 60)
    r = OperatorRegistry.get("detector.hourly_baseline")().fit(_df(vals)).detect(_df(test))
    expected = np.abs(test - vals.mean()) / vals.std()
    assert r.scores == pytest.approx(expected.round(4).tolist()) and max(r.scores) > 1.0


# ── 已移除的參數寫了就報錯 ──

@pytest.mark.parametrize("name", ["detector.hourly_baseline", "detector.residual_shift", "detector.residual_chronic"])
@pytest.mark.parametrize("removed", [{"min_scale": 0.5}, {"min_scale_ratio": 0.0}])
def test_floor_params_no_longer_exist(name, removed):
    with pytest.raises(TypeError, match="沒有參數"):
        OperatorRegistry.get(name)(**removed)


def test_sample_pipeline_no_longer_overrides_min_scale():
    from aiops.engine.pipeline import Pipeline, TargetSpec, load_workflow
    from aiops.interfaces.runner import workflow_path

    wf = load_workflow(workflow_path("sample_it_otel"))
    assert not [t.overrides for t in wf.targets if t.overrides]
    Pipeline(wf)  # 合法
    bad = wf.model_copy(update={"targets": [TargetSpec(source_type="mock", query="q", entity="x",
                                                       overrides={"detect_rshift": {"min_scale": 0.5}})]})
    with pytest.raises(Exception, match="min_scale"):
        Pipeline(bad)
