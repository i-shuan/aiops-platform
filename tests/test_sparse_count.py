"""detector.sparse_count：稀疏計數序列的滾動窗累積筆數檢定（docs/SPARSE_COUNT.md）。"""
import math

import numpy as np
import pandas as pd
import pytest

from aiops.stages.detectors.hourly_baseline import HourlyBaselineDetector
from aiops.stages.detectors.sparse_count import SparseCountDetector, dispersion, tail_probability

WEEK = 7 * 1440


def frame(values, start="2027-01-04") -> pd.DataFrame:
    values = np.asarray(values, dtype=float)
    return pd.DataFrame({"ts": pd.date_range(start, periods=len(values), freq="min", tz="UTC"), "value": values})


def poisson(rate: float, n: int, seed: int) -> np.ndarray:
    return np.random.default_rng(seed).poisson(rate, n).astype(float)


def flagged(det, values) -> np.ndarray:
    return np.asarray(det.detect(frame(values, "2027-01-11")).is_anomaly)


def test_tail_probability_is_a_proper_distribution_tail():
    shape, n, w = 5.5, 1000.0, 30.0
    p0 = (n / (n + w)) ** shape  # P(K = 0)
    assert tail_probability(0, shape, n, w) == 1.0
    assert math.isclose(tail_probability(1, shape, n, w), 1 - p0, rel_tol=1e-9)
    tails = [tail_probability(k, shape, n, w) for k in range(0, 12)]
    assert all(a > b for a, b in zip(tails, tails[1:]))  # 越多筆越不可能


def test_less_training_evidence_means_a_fatter_tail():
    """同樣的發生率，訓練期越短越不敢判：發生率估不準的不確定性在預測分布裡。"""
    short = tail_probability(4, 3 + 0.5, 1000.0, 30.0)
    long = tail_probability(4, 30 + 0.5, 10000.0, 30.0)
    assert short > long


def test_sustained_one_per_minute_on_a_sparse_series_is_caught():
    train = poisson(0.003, WEEK, seed=1)  # 一週約 30 筆
    now = np.zeros(60)
    now[40:] = 1  # 每分鐘 1 筆，持續 20 分鐘（前 30 點要先湊滿一個窗口，所以從第 40 點開始）
    det = SparseCountDetector().fit(frame(train))
    hit = flagged(det, now)
    assert hit.any()
    assert int(np.argmax(hit)) - 40 <= 5  # 開始後幾分鐘內：累積到第 4 筆就越線


def test_pointwise_detector_now_flags_every_single_failure_on_a_sparse_series():
    """原本接在上一個測試後面，斷言「逐點的 hourly_baseline 看每分鐘都只是 1：約 1 分（分母是整數序列的底噪 1 筆），
    門檻 3，20 分鐘一分鐘都不報」，用來說明為什麼需要 sparse_count。
    分母底噪移除後這個前提不成立了：分母是量到的全段 std（0.0434），1 筆 = 23 分，持續失敗的 20 分鐘全部越線；
    代價是平常的每一筆零星失敗也越線——同一個發生率再抽 4 週（40320 分鐘、120 個有失敗的分鐘）：改版前 0 分鐘、改版後 120 分鐘。
    照實記錄；逐點偵測器掛在稀疏計數序列上現在就是這麼吵，這種序列該用 sparse_count（上下的測試）。"""
    train = poisson(0.003, WEEK, seed=1)
    now = np.zeros(60)
    now[40:] = 1
    pointwise = HourlyBaselineDetector().fit(frame(train))
    assert pointwise.buckets_ready == 0 and pointwise.global_[1] == pytest.approx(0.0434, abs=1e-4)
    result = pointwise.detect(frame(now, "2027-01-11"))
    assert sum(result.is_anomaly) == 20 and max(result.scores) == pytest.approx(23.01, abs=0.01)  # 改版前：0 分鐘、0.998 分
    normal = poisson(0.003, 4 * WEEK, seed=3)
    assert sum(pointwise.detect(frame(normal, "2027-01-11")).is_anomaly) == int((normal > 0).sum()) == 120  # 改版前：0


def test_fresh_data_at_the_training_rate_rarely_alarms():
    """同一個發生率再抽 4 週：越線的分鐘很少。這是模型自己的一致性（Poisson 抽的資料），不是真實資料的誤報率。
    min_events 移除前後這裡都是 0 分鐘（另外三組沒拿來調過的種子 12/13、22/23、32/33 也都是 0，scratch 量的）：
    訓練期有 30 筆左右的序列，零星 1～2 筆在模型上本來就不顯著，min_events 擋到的是「訓練期 0 筆」的序列（見下）。"""
    det = SparseCountDetector().fit(frame(poisson(0.003, WEEK, seed=2)))
    hit = flagged(det, poisson(0.003, 4 * WEEK, seed=3))
    episodes = int((np.diff(np.concatenate([[0], hit.astype(int)])) == 1).sum())
    assert episodes <= 2, episodes


def test_bursty_series_is_not_alarmed_by_one_more_burst():
    """成群出現（一次失敗帶 4 筆）：訓練期就是這樣，再來一群不算異常；過度離散把它吸收掉。"""
    train = np.zeros(WEEK)
    train[np.arange(200, WEEK, 480)] = 4  # 每 8 小時一群 4 筆
    det = SparseCountDetector().fit(frame(train))
    assert det.dispersion_ > 3
    now = np.zeros(60)
    now[30] = 4
    assert not flagged(det, now).any()
    now[30:] = 4  # 每分鐘一群、持續半小時：這就不是平常了
    assert flagged(det, now).any()


def test_dispersion_is_one_for_poisson_like_data_and_never_below_one():
    assert dispersion(np.zeros(WEEK), 30) == 1.0
    even = np.zeros(WEEK)
    even[::30] = 1  # 每個窗口剛好 1 筆：比 Poisson 還規律，仍然回 1
    assert dispersion(even, 30) == 1.0
    assert dispersion(poisson(0.01, 4 * WEEK, seed=4), 30) < 1.3


def test_on_a_series_with_no_history_two_failures_alarm_and_one_does_not():
    """原本是 test_never_seen_before_needs_min_events：訓練期 0 筆的序列，窗口內 2 筆不報（min_events 預設 3）、3 筆才報。
    min_events 已移除，有幾筆就照模型算：1 筆 2.83 分（< 4，不報）、2 筆 5.48 分（報）、3 筆 8.09 分（報）。
    改版前：1 筆 0 分、2 筆 0 分、3 筆 8.09 分。"""
    det = SparseCountDetector().fit(frame(np.zeros(WEEK)))
    one, two, three = np.zeros(60), np.zeros(60), np.zeros(60)
    one[40] = 1
    two[[40, 45]] = 1
    three[[40, 45, 50]] = 1
    scores = [max(det.detect(frame(x, "2027-01-11")).scores) for x in (one, two, three)]
    assert scores == [pytest.approx(2.8283), pytest.approx(5.4808), pytest.approx(8.0876)]
    assert not flagged(det, one).any() and flagged(det, two).any() and flagged(det, three).any()


def test_one_benign_burst_on_a_series_with_no_history_does_alarm():
    """已知的誤報來源，照實留著：訓練期完全沒有失敗的序列，第一次出現一群 3 筆（一次失敗重試三次）就會越線。
    模型沒有任何依據說這是平常事；要壓掉只能靠更長的訓練期（min_events 已移除，2 筆就會越線，見上一個測試）。"""
    det = SparseCountDetector().fit(frame(np.zeros(WEEK)))
    now = np.zeros(60)
    now[40] = 3
    assert flagged(det, now).any()


def test_dense_or_non_count_series_are_not_judged():
    dense = SparseCountDetector().fit(frame(poisson(5.0, WEEK, seed=5)))
    assert not flagged(dense, np.full(60, 50.0)).any()
    floats = SparseCountDetector().fit(frame(np.random.default_rng(6).normal(120.0, 5.0, WEEK)))
    assert not flagged(floats, np.full(60, 500.5)).any()
    # 不適用是常態：不准記成錯誤（notes 會進 ctx.errors，每條延遲序列每一輪都多一筆）
    assert dense.notes == [] and floats.notes == []
    assert dense.get_state()["zero_fraction"] < 0.5 and floats.get_state()["countable"] is False
    changed = SparseCountDetector().fit(frame(np.zeros(WEEK)))
    assert not flagged(changed, np.full(60, 0.5)).any()  # 訓練期是計數、現在出現小數：資料變了，要有人知道
    assert changed.notes == ["sparse-count:not-a-count-series-anymore"]
    assert not SparseCountDetector().can_evaluate(frame(np.zeros(10)))  # 湊不滿窗口：棄權


def test_state_round_trip_and_window_change_waits_for_retrain():
    det = SparseCountDetector().fit(frame(poisson(0.003, WEEK, seed=7)))
    now = np.zeros(60)
    now[20:] = 1
    again = SparseCountDetector().load_state(det.get_state())
    assert list(flagged(again, now)) == list(flagged(det, now))
    changed = SparseCountDetector(window=10).load_state(det.get_state())
    assert list(flagged(changed, now)) == list(flagged(det, now))
    assert "window-changed:30->10:until-retrain" in changed.notes


def test_bad_params_fail_at_construction():
    with pytest.raises(TypeError, match="沒有參數"):
        SparseCountDetector(min_events=3)  # 已移除的參數：寫了就報錯
    with pytest.raises(ValueError):
        SparseCountDetector(threshold=0.5)


def test_every_run_fails_but_runs_are_rare_is_now_caught():
    """以前是 xfail(strict=True) 的死區：每 20 分鐘跑一次的工作全數失敗，30 分鐘窗只會看到 1～2 筆，累積不到 min_events
    （3）筆，永遠不判。min_events 移除後窗口裡有 2 筆的分鐘就越線（5.48 分 > 4）：60 分鐘裡 11 分鐘告警，死區不存在了，
    xfail 標記拿掉；資料與斷言沒有動。"""
    det = SparseCountDetector().fit(frame(np.zeros(WEEK)))
    now = np.zeros(60)
    now[::20] = 1  # 每 20 分鐘一筆，全部失敗
    assert flagged(det, now).any()
