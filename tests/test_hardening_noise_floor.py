"""分母 = max(量到的尺度, |水位| × min_scale_ratio)（base.floored_scale）；兩者都是 0 才退 1.0，只防除以 0。

2026-11-20 以前這個檔測的是分母底噪（docs/HARDENING_DESIGN.md 第 1 節）：
分母 = max(scale, min_scale, |中心| × min_scale_ratio, 整數序列 1)，hourly_baseline 另外有「時段 std 至少全段的一成」。
那些下限是減少誤報的機制，當天先全部移除（決定：偵測階段要最敏感）；同一天真實資料實測之後只放回一個——
相對底噪 min_scale_ratio（預設 1%，0 = 關掉；來源與實測數字在 base.MIN_SCALE_RATIO_TUNABLE 的註解）。
絕對下限 min_scale、整數序列至少 1、時段 std 至少全段一成都沒有放回來，寫了照樣報錯。

所以這個檔的測試分三種：
- 沒放回來的下限：照實記錄移除後的行為（幾個以前「不報」的情況現在會報），數字沒有因為放回相對底噪而變。
- 被相對底噪改到數字的測試：斷言改成新的算式，改之前 -> 改之後寫在各測試的 docstring；同一個測試另外用
  min_scale_ratio: 0 把原本「沒有底噪」的斷言原樣留著，最敏感的那條路仍然有測試。沒有為了過測試改輸入。
- 放回的相對底噪自己的測試（檔案最後一段）。
"""
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from aiops.engine.errors import WorkflowError
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import MIN_SCALE_RATIO_TUNABLE, floored_scale, guard_scale, is_integer_series

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
    """舊版基準線：全段 std 是 0（或趨近 0），有的多帶 integer。都載得進來（integer 不讀）。
    hourly_baseline 的水位是時段／全段平均，本來就存在基準線裡，所以舊基準線載進來就有相對底噪，不用等重訓。

    放回相對底噪（預設 1%）之後斷言改了：
    std = 0   ：改之前分母退到 1.0 -> 102 是 2.0 分；改之後分母 = 100 × 1% = 1.0 -> 一樣 2.0 分（數字剛好相同）。
    std = 1e-6：改之前照量到的 1e-6 -> 2,000,000 分；改之後分母 = max(1e-6, 1.0) = 1.0 -> 2.0 分。
    min_scale_ratio: 0：改之前的兩個數字原樣成立（2.0、2,000,000）。"""
    cls = OperatorRegistry.get("detector.hourly_baseline")
    for extra in ({}, {"integer": True}):
        zero = {"buckets": {}, "global": [100.0, 0.0], **extra}
        tiny = {"buckets": {}, "global": [100.0, 1e-6], **extra}
        assert cls().load_state(zero).detect(_df([100.0, 102.0])).scores == [0.0, pytest.approx(2.0)]
        assert cls().load_state(tiny).detect(_df([100.0, 102.0])).scores == [0.0, pytest.approx(2.0)]
        off = dict(min_scale_ratio=0)
        assert cls(**off).load_state(zero).detect(_df([100.0, 102.0])).scores == [0.0, pytest.approx(2.0)]
        assert cls(**off).load_state(tiny).detect(_df([100.0, 102.0])).scores == [0.0, pytest.approx(2_000_000.0)]


# ── 常數序列：std = 0 時不除以 0、也不全盲 ──

def test_fallback_on_a_constant_series_is_not_blind_and_does_not_divide_by_zero():
    algo = OperatorRegistry.get("detector.hourly_baseline")().fit(_df([100.0] * 120))
    assert algo.buckets_ready == 0 and algo.global_[1] == 0.0  # 退回全段統計，全段 std = 0
    r = algo.detect(_df([100.0, 100.5, 150.0]))
    # 分母 = 相對底噪 100 × 1% = 1.0。斷言的數字沒動：放回之前是「都是 0 退 1.0」，剛好同一個數
    assert r.scores == [0.0, 0.5, 50.0]
    assert r.is_anomaly == [False, False, True]
    off = OperatorRegistry.get("detector.hourly_baseline")(min_scale_ratio=0).fit(_df([100.0] * 120))
    assert off.detect(_df([100.0, 100.5, 150.0])).scores == [0.0, 0.5, 50.0]  # 關掉底噪：量到的 0 -> 退 1.0


def test_hourly_baseline_on_a_constant_series_is_not_blind():
    """常數 20 訓練 14 天（時段 std 0、全段 std 0）。放回相對底噪之後斷言改了：
    改之前（都是 0 -> 分母 1.0）      ：[20, 21, 60] -> [0.0, 1.0, 40.0] 分，只有 60 越線。
    改之後（分母 = 時段平均 20 × 1% = 0.2）：[0.0, 5.0, 200.0] 分，21（偏 5%）也越線。
    水位小於 100 的常數序列，相對底噪比原本的 1.0 小，所以放回底噪之後這裡反而更靈敏；
    min_scale_ratio: 0 時改之前的斷言原樣成立。"""
    days = 14 * 1440
    cls = OperatorRegistry.get("detector.hourly_baseline")
    test = _df([20.0, 21.0, 60.0], start=T0 + timedelta(days=14))
    r = cls(min_weeks=1).fit(_df([20.0] * days)).detect(test)
    assert r.scores == [0.0, 5.0, 200.0] and r.is_anomaly == [False, True, True]
    r = cls(min_weeks=1, min_scale_ratio=0).fit(_df([20.0] * days)).detect(test)
    assert r.scores == [0.0, 1.0, 40.0] and r.is_anomaly == [False, False, True]  # 時段 std 0 -> 全段 std 0 -> 1.0


# ── 幾乎不動的大數值序列：預設分母是水位的 1%；關掉相對底噪才照量到的極小 std ──

def test_a_tiny_measured_std_is_floored_at_one_percent_of_the_level_and_used_as_is_when_the_floor_is_off():
    """序列在 100 萬上下 ±0.5（std 0.5）。原本的名稱是 test_a_tiny_measured_std_is_used_as_is_on_a_large_valued_series，
    放回相對底噪之後預設行為變了，改成兩種設定都斷言：

    預設（min_scale_ratio 1%）：分母 = max(0.5, 1,000,000.25 × 1%) = 10,000.0025。
      +20,000 -> 2.0 分不報、+40,000 -> 4.0 分報、+2 -> 0.0002 分不報（就是 2026-11-20 移除底噪之前的數字）。
    min_scale_ratio: 0（最敏感，原本這個測試釘的行為）：分母 = 量到的 0.5。
      +20,000 -> 40,000 分、+40,000 -> 80,000 分、+2 -> 4.0 分，三個都報。斷言原樣保留。"""
    vals = [1_000_000.25 + (0.5 if i % 2 else -0.5) for i in range(120)]  # 非整數、std 0.5、中心 100 萬
    test = _df([1_000_000.25 + 20_000, 1_000_000.25 + 40_000, 1_000_000.25 + 2])
    cls = OperatorRegistry.get("detector.hourly_baseline")
    floored = cls().fit(_df(vals))
    assert floored.global_ == (pytest.approx(1_000_000.25), pytest.approx(0.5))
    r = floored.detect(test)
    assert r.scores == [pytest.approx(2.0, abs=1e-4), pytest.approx(4.0, abs=1e-4), pytest.approx(0.0002, abs=1e-4)]
    assert r.is_anomaly == [False, True, False]
    r = cls(min_scale_ratio=0).fit(_df(vals)).detect(test)
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

# min_scale_ratio 原本也在 removed 裡（{"min_scale_ratio": 0.0}）；2026-11-20 放回後是合法參數，從這裡拿掉，
# 改由檔案最後的 test_min_scale_ratio_is_tunable_* 測它的範圍。絕對下限 min_scale 沒有放回來，照樣報錯。
@pytest.mark.parametrize("name", ["detector.hourly_baseline", "detector.residual_shift", "detector.residual_chronic"])
@pytest.mark.parametrize("removed", [{"min_scale": 0.5}])
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


# ── 放回的相對底噪（min_scale_ratio，預設 1%）──

FLOORED = ("detector.hourly_baseline", "detector.residual_shift", "detector.residual_chronic")


def test_floored_scale_is_the_larger_of_the_measured_scale_and_the_relative_floor():
    assert floored_scale(100.0, 0.01, 3.0) == 3.0  # 量到的比較大：照量到的
    assert floored_scale(100.0, 0.01, 1e-6) == 1.0  # 量到的極小：水位 100 × 1% = 1.0
    assert floored_scale(-100.0, 0.01, 1e-6) == 1.0  # 負的水位取絕對值
    assert floored_scale(None, 0.01, 1e-6) == 1e-6  # 不知道水位（舊基準線）：沒有相對底噪，照量到的
    assert floored_scale(100.0, 0.0, 1e-6) == 1e-6  # ratio 0 = 關掉
    assert floored_scale(100.0, 0.01, 0.0, 0.4) == 1.0 and floored_scale(10.0, 0.01, 0.0, 0.4) == 0.4  # 候選依序取第一個正數
    # 都是 0（或都不能用）才退 1.0，只防除以 0
    assert floored_scale(0.0, 0.01, 0.0) == 1.0 and floored_scale(None, 0.01) == 1.0 and floored_scale(None, 0.01, None, 0.0) == 1.0
    assert floored_scale(100.0, 0.0, 0.0) == 1.0
    # 不是有限正數的候選跳過；水位不是有限值就當成不知道
    assert floored_scale(100.0, 0.01, float("nan"), float("inf"), -2.0, 2.5) == 2.5
    assert floored_scale(100.0, 0.01, float("nan"), None) == 1.0  # 候選都不能用：只剩底噪 1.0
    assert floored_scale(float("nan"), 0.01, 0.3) == 0.3 and floored_scale(float("inf"), 0.01, 0.3) == 0.3
    assert floored_scale(5.0, 0.01, 0.0) == 0.05  # 有底噪就不退 1.0：水位小於 100 時分母比 1.0 小


@pytest.mark.parametrize("name", FLOORED)
def test_min_scale_ratio_is_tunable_from_zero_to_one_and_defaults_to_one_percent(name):
    cls = OperatorRegistry.get(name)
    assert MIN_SCALE_RATIO_TUNABLE == {"min_scale_ratio": (0.01, 0.0, 1.0)}
    assert cls.tunable["min_scale_ratio"] == (0.01, 0.0, 1.0) and cls().param("min_scale_ratio") == 0.01
    assert cls(min_scale_ratio=0).param("min_scale_ratio") == 0.0 and cls(min_scale_ratio=1).param("min_scale_ratio") == 1.0
    for bad in (-0.1, 1.5):
        with pytest.raises(ValueError, match="min_scale_ratio=.*超出範圍"):
            cls(min_scale_ratio=bad)
        wf = Workflow(name="t", nodes=[NodeSpec(name="fetch", operator="ingestion.fetch"),
                                       NodeSpec(name="detect", operator=name, params={"min_scale_ratio": bad})],
                      targets=[TargetSpec(source_type="mock", query="a", entity="a")])
        with pytest.raises(WorkflowError, match="min_scale_ratio"):  # 載入期就擋，不是等到第一輪偵測
            Pipeline(wf)
        override = Workflow(name="t", nodes=[NodeSpec(name="fetch", operator="ingestion.fetch"), NodeSpec(name="detect", operator=name)],
                            targets=[TargetSpec(source_type="mock", query="a", entity="a",
                                                overrides={"detect": {"min_scale_ratio": bad}})])
        with pytest.raises(WorkflowError, match="min_scale_ratio"):
            Pipeline(override)


def test_only_the_three_baseline_detectors_have_min_scale_ratio():
    have = {name for name in OperatorRegistry.names("detector.")
            if "min_scale_ratio" in getattr(OperatorRegistry.get(name), "tunable", {})}
    assert have == set(FLOORED)
    sparse = OperatorRegistry.get("detector.sparse_count")
    assert "min_scale_ratio" not in sparse.tunable and "min_scale_ratio" not in sparse.other_params
    with pytest.raises(TypeError, match="沒有參數"):
        sparse(min_scale_ratio=0.01)


# 很穩定的序列：延遲 0.1 秒、分鐘雜訊 std 1e-4（水位的 0.1%）。這是放回相對底噪的原因（真實資料上故障後的正常值
# 0.0988 秒在 residual_chronic 得到 11,247 分）；下面是同一種形狀的合成序列，種子固定，數字是在這份資料上量到的。
STABLE_LEVEL, STABLE_NOISE = 0.1, 1e-4


def _stable():
    """回傳 (訓練 600 分鐘, 隔天同時段的 120 分鐘正常值)。"""
    rng = np.random.default_rng(11)
    return STABLE_LEVEL + rng.normal(0, STABLE_NOISE, 600), STABLE_LEVEL + rng.normal(0, STABLE_NOISE, 120)


def _shifted(quiet, delta):
    """前 60 分鐘正常，後 60 分鐘整段墊高 delta（夠 residual_chronic 的 30 點滾動窗湊滿）。"""
    values = quiet.copy()
    values[60:] += delta
    return _df(values, start=T0 + timedelta(days=1))


@pytest.mark.parametrize("name,threshold", [("detector.hourly_baseline", 3.0), ("detector.residual_shift", 4.0),
                                            ("detector.residual_chronic", 3.0)])
def test_a_wiggle_within_one_percent_of_a_very_stable_series_is_not_flagged_but_a_ten_percent_shift_is(name, threshold):
    """預設（min_scale_ratio 1%）：分母 = max(量到的 σ, 0.1 × 1% = 0.001)，量到的 σ 都遠小於 0.001。
      +0.8%（0.1000 -> 0.1008）整段：分數最高 1.04／1.02／0.82（hourly／突變／漂移），不報。
      +10%（-> 0.11）：分數 10.24／10.22／10.02，報（hourly 60 分鐘都報、突變只報跳上去那一點、漂移窗口過半之後報）。
      正常的 120 分鐘：0 點越線。

    死區（CLAUDE.md 第 100 節第 4 條，照實記錄）：預設之下分數 = 偏離 / (水位 × 1%)，所以偏離要超過
    水位 × 1% × threshold 才看得到——hourly 與漂移是水位的 3%、突變是 4%。+2%（-> 0.102）分數 2.24／2.22／2.02，三個都不報。
    要看到就在節點 YAML 把 min_scale_ratio 調小或設 0。"""
    train, quiet = _stable()
    algo = OperatorRegistry.get(name)().fit(_df(train))
    assert algo.param("threshold") == threshold
    normal = algo.detect(_shifted(quiet, 0.0))
    assert not any(normal.is_anomaly)
    wiggle = algo.detect(_shifted(quiet, 0.0008))
    assert not any(wiggle.is_anomaly) and max(wiggle.scores) < 1.1
    dead_zone = algo.detect(_shifted(quiet, 0.002))
    assert not any(dead_zone.is_anomaly) and 2.0 < max(dead_zone.scores) < 2.3  # 死區：+2% 看不到
    shift = algo.detect(_shifted(quiet, 0.01))
    assert max(shift.scores) == pytest.approx(10.0, abs=0.3)
    assert not any(shift.is_anomaly[:60]) and shift.is_anomaly[-1 if name.endswith('chronic') else 60]
    expected_minutes = {"detector.hourly_baseline": 60, "detector.residual_shift": 1, "detector.residual_chronic": 51}[name]
    assert sum(shift.is_anomaly) == expected_minutes


@pytest.mark.parametrize("name,sigma,peak,flagged", [
    ("detector.hourly_baseline", 9.674e-05, 10.75, 60),
    ("detector.residual_shift", 1.3745e-04, 7.44, 1),
    ("detector.residual_chronic", 1.1163e-05, 73.22, 59),
])
def test_without_the_relative_floor_the_same_wiggle_scores_far_above_threshold(name, sigma, peak, flagged):
    """min_scale_ratio: 0 = 放回之前的行為：分母就是量到的 σ（hourly 9.7e-5、突變 1.37e-4、漂移的滾動平均 1.1e-5）。
    同一段 +0.8% 的變動得到 10.75／7.44／73.22 分（門檻 3／4／3），hourly 60 分鐘、漂移 59 分鐘都在報——
    事件群就是這樣一直有新異常、結不了案。預設 1% 時同一段是 1.04／1.02／0.82 分（上一個測試）。"""
    train, quiet = _stable()
    algo = OperatorRegistry.get(name)(min_scale_ratio=0).fit(_df(train))
    measured = algo.global_[1] if name == "detector.hourly_baseline" else algo.noise_["noise"]
    assert measured == pytest.approx(sigma, rel=1e-3) and measured < STABLE_LEVEL * 0.01 / 5
    wiggle = algo.detect(_shifted(quiet, 0.0008))
    assert max(wiggle.scores) == pytest.approx(peak, abs=0.01) and max(wiggle.scores) > 1.8 * algo.param("threshold")
    assert sum(wiggle.is_anomaly) == flagged and not any(wiggle.is_anomaly[:60])
    assert sum(algo.detect(_shifted(quiet, 0.01)).is_anomaly) >= flagged  # +10% 當然也報


@pytest.mark.parametrize("name", ["detector.residual_shift", "detector.residual_chronic"])
def test_residual_baselines_store_the_level_and_one_saved_without_it_has_no_relative_floor(name):
    """residual_shift／residual_chronic 的水位（訓練期中位數）跟基準線一起存；存檔讀回來分數完全一樣。
    放回相對底噪之前存的基準線沒有 level：載得進來、照樣判，但沒有相對底噪（跟 min_scale_ratio: 0 完全一樣），
    下次重訓才有——也就是那段期間很穩定的序列仍然會像放回之前一樣吵。"""
    train, quiet = _stable()
    cls = OperatorRegistry.get(name)
    fresh = cls().fit(_df(train))
    state = json.loads(json.dumps(fresh.get_state()))
    assert state["level"] == pytest.approx(float(np.median(train))) and state["level"] == pytest.approx(0.1, abs=1e-4)
    loaded = cls().load_state(state)
    assert loaded.level_ == fresh.level_ and loaded.get_state()["level"] == state["level"]
    for delta in (0.0008, 0.01):
        assert loaded.detect(_shifted(quiet, delta)).to_dict() == fresh.detect(_shifted(quiet, delta)).to_dict()

    for old_state in ({k: v for k, v in state.items() if k != "level"}, {**state, "level": None}):
        old = cls().load_state(old_state)
        assert old.level_ is None
        no_floor = cls(min_scale_ratio=0).fit(_df(train))
        for delta in (0.0, 0.0008, 0.01):
            assert old.detect(_shifted(quiet, delta)).to_dict() == no_floor.detect(_shifted(quiet, delta)).to_dict()
        assert any(old.detect(_shifted(quiet, 0.01)).is_anomaly[60:])  # 明顯的偏移照樣抓得到
        assert any(old.detect(_shifted(quiet, 0.0008)).is_anomaly[60:])  # 1% 以內的變動也報：沒有相對底噪
        assert not any(fresh.detect(_shifted(quiet, 0.0008)).is_anomaly)
