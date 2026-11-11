"""稀疏流量底噪（docs/HARDENING_DESIGN.md 第 1 節）：分母 = max(scale, min_scale, |中心| × min_scale_ratio, 整數序列 1)。

以前 detector.mad 在 MAD = 0 時分母是 1e-6：失敗數序列 90% 是 0，平常的 2 筆失敗就是 1,349,000 分（混沌場景 5 的誤報）；
detector.zscore 在 std = 0 時分數全 0（永遠不報）。這裡用合成序列鎖住新行為，真實資料零迴歸記在設計稿「實測結果」。
"""
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import SCALE_FLOOR_TUNABLE, is_integer_series, scale_floor

FLOORED = ["detector.mad", "detector.zscore", "detector.hourly_baseline", "detector.quantile_drift"]
T0 = datetime(2026, 11, 2, tzinfo=timezone.utc)


def _df(values, start=T0):
    return pd.DataFrame({"ts": [start + timedelta(minutes=i) for i in range(len(values))], "value": [float(v) for v in values]})


def _failures(n=600, seed=3):
    """每分鐘失敗數：90% 是 0，其餘 1～2 筆。"""
    rng = np.random.default_rng(seed)
    return np.where(rng.random(n) < 0.9, 0, rng.integers(1, 3, n))


# ── 共用函式 ──

def test_scale_floor_takes_the_largest_of_the_four_floors():
    ints = pd.Series([0, 0, 0, 1, 2])
    assert scale_floor(0.0, ints, 0.0, 0.01) == 1.0  # 整數序列至少 1 個單位
    assert scale_floor(0.0, ints, 5.0, 0.01) == 5.0  # 絕對下限
    assert scale_floor(3.0, ints, 0.0, 0.01) == 3.0  # 算出來的 scale 本來就夠大：不動
    floats = pd.Series([1_000_000.0, 1_000_000.5, 999_999.5])
    assert scale_floor(0.4, floats, 0.0, 0.01) == pytest.approx(10_000.0)  # 相對下限 = |中位數| × 1%
    assert scale_floor(0.4, floats, 0.0, 0.0) == 0.4  # 相對下限關掉
    assert scale_floor(0.0, None, 0.0, 0.01, center=-200.0, integer=False) == pytest.approx(2.0)  # 中心取絕對值


def test_scale_floor_falls_back_to_one_when_every_floor_is_zero():
    # 中心 0、非整數、兩個參數都關：跟 quantile_drift／compare 既有的最後一層一樣退回 1.0，不除以 0
    assert scale_floor(0.0, pd.Series([0.0, 0.25, 0.0]), 0.0, 0.0) == 1.0
    assert scale_floor(float("nan"), None, 0.0, 0.0, center=0.0, integer=False) == 1.0


def test_integer_series_detection():
    assert is_integer_series(pd.Series([0.0, 1.0, 2.0]))
    assert not is_integer_series(pd.Series([0.0, 1.5]))
    assert not is_integer_series(pd.Series([1.0, np.nan]))
    assert not is_integer_series(pd.Series([], dtype=float))


# ── 失敗數序列：2 筆失敗不再是百萬分 ──

def test_mad_score_on_a_failure_count_series_is_bounded():
    train = _failures()
    assert float(np.median(np.abs(train - np.median(train)))) == 0.0  # MAD 真的是 0（以前分母 = 1e-6）
    algo = OperatorRegistry.get("detector.mad")().fit(_df(train))
    r = algo.detect(_df([0, 1, 2, 0, 40]))
    assert r.scores[:4] == [0.0, 0.6745, 1.349, 0.0]  # 2 筆失敗 = 1.349 分；改版前是 1,349,000 分
    assert r.is_anomaly == [False, False, False, False, True]  # 真的爆量（40 筆）照樣抓到：26.98 分
    assert r.scores[4] == pytest.approx(26.98)


def test_mad_min_scale_from_target_overrides_takes_effect_without_retraining():
    """基準線存原始 MAD，底噪在 detect 才套：後台調 min_scale 不用等基準線重訓。"""
    cls = OperatorRegistry.get("detector.mad")
    state = json.loads(json.dumps(cls().fit(_df(_failures())).get_state()))
    assert state["mad"] == 0.0 and state["integer"] is True
    loose = cls(min_scale=10.0).load_state(state).detect(_df([40]))
    assert loose.scores == [2.698] and loose.is_anomaly == [False]


def test_mad_loads_a_baseline_saved_before_the_floor_existed():
    """舊版存的是 mad = 1e-6、沒有 integer：載入後套絕對與相對下限，不再爆分。"""
    algo = OperatorRegistry.get("detector.mad")().load_state({"median": 100.0, "mad": 1e-6})
    r = algo.detect(_df([100.0, 102.0]))
    assert r.scores == [0.0, pytest.approx(1.349)]  # 分母 = 100 × 1% = 1


# ── 常數序列：zscore／hourly_baseline 不再全盲 ──

def test_zscore_on_a_constant_series_is_no_longer_blind():
    algo = OperatorRegistry.get("detector.zscore")().fit(_df([100.0] * 120))
    assert algo.std_ == 0.0
    r = algo.detect(_df([100.0, 100.5, 150.0]))
    assert r.scores == [0.0, 0.5, 50.0]  # 分母 = 整數序列 1 與 100 × 1% 取大 = 1
    assert r.is_anomaly == [False, False, True]  # 改版前三個都是 0 分、永遠不報


def test_zscore_relative_floor_is_one_percent_of_the_center():
    vals = [1_000_000.25 + (0.5 if i % 2 else -0.5) for i in range(120)]  # 非整數、std 0.5、中心 100 萬
    algo = OperatorRegistry.get("detector.zscore")().fit(_df(vals))
    r = algo.detect(_df([1_000_000.25 + 20_000, 1_000_000.25 + 40_000]))
    assert r.scores == [pytest.approx(2.0), pytest.approx(4.0)]  # 分母 = 10,000（不是 0.5）
    assert r.is_anomaly == [False, True]
    zero = OperatorRegistry.get("detector.zscore")(min_scale_ratio=0.0).fit(_df(vals)).detect(_df([1_000_000.25 + 20_000]))
    assert zero.scores == [pytest.approx(40_000.0)]  # 相對下限關掉：回到 std 0.5


def test_hourly_baseline_on_a_constant_series_is_no_longer_blind():
    days = 14 * 1440
    algo = OperatorRegistry.get("detector.hourly_baseline")(min_weeks=1).fit(_df([20.0] * days))
    r = algo.detect(_df([20.0, 21.0, 60.0], start=T0 + timedelta(days=14)))
    assert r.scores == [0.0, 1.0, 40.0] and r.is_anomaly == [False, False, True]  # 改版前全 0


def test_normal_series_scores_are_untouched_by_the_floor():
    """scale 遠大於中心的 1% 時一分不變：真實 11/3 零迴歸的合成版。"""
    rng = np.random.default_rng(5)
    vals = 1000 + rng.normal(0, 80, 600)
    test = _df(1000 + rng.normal(0, 80, 60))
    for name in ("detector.mad", "detector.zscore"):
        cls = OperatorRegistry.get(name)
        a = cls().fit(_df(vals)).detect(test)
        b = cls(min_scale=0.0, min_scale_ratio=0.0).fit(_df(vals)).detect(test)
        assert a.scores == b.scores, name


# ── tunable 範圍 ──

@pytest.mark.parametrize("name", FLOORED)
def test_floor_params_are_tunable_with_sane_ranges(name):
    cls = OperatorRegistry.get(name)
    assert {k: cls.tunable[k] for k in SCALE_FLOOR_TUNABLE} == {"min_scale": (0.0, 0.0, 1e12), "min_scale_ratio": (0.01, 0.0, 1.0)}
    extra = {"tz": "UTC"} if name == "detector.hourly_baseline" else {}
    for bad in ({"min_scale": -1.0}, {"min_scale": 2e12}, {"min_scale_ratio": -0.1}, {"min_scale_ratio": 1.5}):
        with pytest.raises(ValueError):
            cls(**extra, **bad).validate_params()
    cls(**extra, min_scale=5000.0, min_scale_ratio=0.0).validate_params()


def test_it_otel_example_target_overrides_min_scale():
    """範例 workflow 的 target overrides 示範 min_scale，載入期 check_override 會驗證範圍。"""
    from aiops.engine.pipeline import Pipeline, TargetSpec, load_workflow
    from aiops.interfaces.runner import workflow_path

    wf = load_workflow(workflow_path("it_otel"))
    overrides = [t.overrides for t in wf.targets if t.overrides]
    assert any("min_scale" in o.get("detect_rshift", {}) for o in overrides)
    Pipeline(wf)  # 合法
    bad = wf.model_copy(update={"targets": [TargetSpec(source_type="mock", query="q", entity="x",
                                                       overrides={"detect_rshift": {"min_scale": -5}})]})
    with pytest.raises(Exception, match="min_scale"):
        Pipeline(bad)
