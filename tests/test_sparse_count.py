"""detector.sparse_count：稀疏計數序列的滾動窗累積筆數檢定（docs/SPARSE_COUNT.md）。"""
import math

import numpy as np
import pandas as pd
import pytest

from aiops.engine.pipeline import Pipeline, Workflow, WorkflowError
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


def test_sustained_one_per_minute_on_a_sparse_series_is_caught_and_pointwise_misses_it():
    train = poisson(0.003, WEEK, seed=1)  # 一週約 30 筆
    now = np.zeros(60)
    now[40:] = 1  # 每分鐘 1 筆，持續 20 分鐘（前 30 點要先湊滿一個窗口，所以從第 40 點開始）
    det = SparseCountDetector().fit(frame(train))
    hit = flagged(det, now)
    assert hit.any()
    assert int(np.argmax(hit)) - 40 <= 5  # 開始後幾分鐘內：累積到第 4 筆就越線
    # 逐點的偵測器（hourly_baseline；訓練只有一週，整條退回全段平均與標準差）看每分鐘都只是「1」：約 1 分，門檻 3
    pointwise = HourlyBaselineDetector().fit(frame(train))
    assert pointwise.buckets_ready == 0
    assert not any(pointwise.detect(frame(now, "2027-01-11")).is_anomaly)


def test_fresh_data_at_the_training_rate_rarely_alarms():
    """同一個發生率再抽 4 週：越線的分鐘很少。這是模型自己的一致性（Poisson 抽的資料），不是真實資料的誤報率。"""
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


def test_never_seen_before_needs_min_events():
    det = SparseCountDetector().fit(frame(np.zeros(WEEK)))
    two, three = np.zeros(60), np.zeros(60)
    two[[40, 45]] = 1
    three[[40, 45, 50]] = 1
    assert not flagged(det, two).any()  # 模型上已經很顯著（p 約 3e-6），但 1～2 筆不發卡
    assert flagged(det, three).any()
    loose = SparseCountDetector(min_events=2).fit(frame(np.zeros(WEEK)))
    assert flagged(loose, two).any()


def test_one_benign_burst_on_a_series_with_no_history_does_alarm():
    """已知的誤報來源，照實留著：訓練期完全沒有失敗的序列，第一次出現一群 3 筆（一次失敗重試三次）就會越線。
    模型沒有任何依據說這是平常事；要壓掉只能靠更長的訓練期或把 min_events 調高。"""
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
    for bad in ({"min_events": 0}, {"min_events": 2.5}, {"min_events": True}):
        with pytest.raises(TypeError):
            SparseCountDetector(**bad)
    with pytest.raises(ValueError):
        SparseCountDetector(threshold=0.5)


def _workflow(bypass: list[str]) -> Workflow:
    return Workflow.model_validate({"name": "t", "nodes": [
        {"name": "fetch", "operator": "ingestion.fetch"},
        {"name": "detect", "operator": "detector.hourly_baseline", "params": {"snr": {"p_fa": 0.001}}},
        {"name": "sparse", "operator": "detector.sparse_count"},
        {"name": "vote", "operator": "detector.soft_vote", "params": {"bypass": bypass}},
    ], "targets": [{"source_type": "mock", "query": "errors", "entity": "a"}]})


def test_it_does_not_vote_and_must_be_listed_in_bypass():
    assert SparseCountDetector.votes is False
    Pipeline(_workflow(["sparse"]))
    with pytest.raises(WorkflowError, match="sparse（detector.sparse_count）不參與表決，必須列進 bypass"):
        Pipeline(_workflow([]))


@pytest.mark.xfail(strict=True, reason="死區：窗口內累積不到 min_events 筆。每 20 分鐘跑一次的工作全數失敗，"
                                       "30 分鐘窗只會看到 1～2 筆，永遠不判；要把 window 開到 60 以上才看得到")
def test_dead_zone_every_run_fails_but_runs_are_rare():
    det = SparseCountDetector().fit(frame(np.zeros(WEEK)))
    now = np.zeros(60)
    now[::20] = 1  # 每 20 分鐘一筆，全部失敗
    assert flagged(det, now).any()
