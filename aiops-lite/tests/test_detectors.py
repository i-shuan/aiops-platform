"""偵測器：合成序列（有日夜週期的指標、幾乎不動的指標、稀疏計數）。這些不是真實產線資料。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from src import detectors

TZ = "Asia/Taipei"
START = datetime(2026, 10, 26, tzinfo=timezone.utc)  # 週一


def series(days: float, fn, start: datetime = START, seed: int = 0) -> pd.DataFrame:
    n = int(days * 1440)
    ts = pd.date_range(start, periods=n, freq="1min", tz="UTC")
    minutes = np.arange(n) + int((start - START).total_seconds() // 60)
    rng = np.random.default_rng(seed + minutes[0])
    return pd.DataFrame({"ts": ts, "value": fn(minutes, rng)})


def daily(minutes, rng):
    """上班時段高、夜間低的流量型指標，雜訊 σ = 3。"""
    return 100 + 40 * np.sin(2 * np.pi * minutes / 1440) + rng.normal(0, 3, len(minutes))


def trained(fn=daily, days=21):
    state = detectors.fit_all(series(days, fn), TZ)
    assert state is not None
    return json.loads(json.dumps(state))  # 基準線要能 JSON 存檔再讀回來


def hour_after(days: float, fn, minutes: int = 60) -> pd.DataFrame:
    return series(minutes / 1440, fn, start=START + timedelta(days=days))


def test_too_few_training_points_means_no_baseline():
    assert detectors.fit_all(series(59 / 1440, daily), TZ) is None
    assert detectors.fit_all(series(60 / 1440, daily), TZ) is not None


def test_normal_week_after_training_is_quiet():
    """留出驗證：訓練 3 週，第 4 週（沒有拿來學）整週逐小時判，誤報的點數比例要很低。"""
    state = trained()
    flagged = total = 0
    for h in range(7 * 24):
        df = series(1 / 24, daily, start=START + timedelta(days=21, hours=h))
        flagged += len(detectors.detect_all(state, df))
        total += len(df)
    assert total == 10080 and flagged / total < 0.002, f"誤報 {flagged}/{total}"


def test_spike_is_detected():
    state = trained()

    def spike(minutes, rng):
        v = daily(minutes, rng)
        v[40:50] += 60  # 20σ 的尖峰，持續 10 分鐘
        return v

    found = detectors.detect_all(state, hour_after(21, spike))
    hit = [a for a in found if 40 <= (a.ts - (START + timedelta(days=21))).total_seconds() / 60 < 50]
    assert len(hit) >= 1 and "hourly_baseline" in hit[0].detectors and len(found) - len(hit) <= 1


def test_sustained_level_shift_is_detected_for_its_whole_duration():
    state = trained()

    def shifted(minutes, rng):
        v = daily(minutes, rng)
        v[10:] += 30  # 10σ 的水位跳升，之後一直維持
        return v

    found = detectors.detect_all(state, hour_after(21, shifted))
    assert len(found) >= 15
    assert {d for a in found for d in a.detectors} >= {"hourly_baseline", "residual_chronic"}


def test_moderate_sustained_offset_is_mostly_outvoted_known_dead_zone():
    """死區（照實留著）：持續但只有 +2σ 的偏移，residual_chronic 幾乎整段都舉牌，但它只有一票；
    只有 hourly_baseline 剛好也被雜訊推過 3σ 的那幾個點才成立。所以慢性的小幅偏移只會零星報出來，
    不會像水位跳升那樣整段都報——多數決壓誤報的代價。"""
    state = trained()
    df = hour_after(21, lambda m, r: daily(m, r) + 6)
    _scores, chronic = detectors.DETECTORS[2].score(state["residual_chronic"], state, df)
    found = detectors.detect_all(state, df)
    assert int(chronic.sum()) >= 20 and len(found) <= int(chronic.sum()) // 2


@pytest.mark.xfail(strict=True, reason="死區：持續 +1σ 的偏移，residual_chronic 看得到但多數決不成立")
def test_small_sustained_offset_is_not_reported_as_a_run():
    state = trained()
    found = detectors.detect_all(state, hour_after(21, lambda m, r: daily(m, r) + 3))
    assert len(found) >= 5


def test_daily_cycle_itself_is_not_an_anomaly_but_the_same_value_at_the_wrong_hour_is():
    state = trained()
    peak = hour_after(21 + 6 / 24, daily)           # 每天的高峰時段：值很高但正常
    assert detectors.detect_all(state, peak) == []
    wrong = hour_after(21 + 18 / 24, lambda m, r: daily(m, r) + 80)  # 低谷時段出現高峰的值
    assert len(detectors.detect_all(state, wrong)) >= 30


def test_one_percent_noise_floor_keeps_a_nearly_flat_series_quiet():
    flat = lambda m, r: 1000 + r.normal(0, 0.001, len(m))   # 幾乎不動（σ 只有水位的百萬分之一）
    state = trained(flat)
    wiggle = hour_after(21, lambda m, r: flat(m, r) + np.where(m % 7 == 0, 5.0, 0.0))  # 0.5% 的抖動
    assert detectors.detect_all(state, wiggle) == []
    jump = hour_after(21, lambda m, r: flat(m, r) + np.where(np.arange(len(m)) >= 30, 100.0, 0.0))  # 10% 的跳升
    assert detectors.detect_all(state, jump)


def test_sparse_counter_burst_is_detected_without_votes():
    sparse = lambda m, r: (r.random(len(m)) < 0.001).astype(float)   # 一天約 1.4 筆
    state = trained(sparse)
    assert state["sparse_count"]["applies"] is True
    burst = hour_after(21, lambda m, r: np.where(np.arange(len(m)) >= 20, 1.0, 0.0))  # 每分鐘 1 筆、連續 40 分鐘
    found = detectors.detect_all(state, burst)
    assert found and all("sparse_count" in a.detectors for a in found)


def test_one_or_two_events_on_a_sparse_counter_are_not_reported():
    sparse = lambda m, r: (r.random(len(m)) < 0.001).astype(float)
    state = trained(sparse)
    two = hour_after(21, lambda m, r: np.where(np.isin(np.arange(len(m)), [50, 55]), 1.0, 0.0))
    assert detectors.detect_all(state, two) == []


def test_sparse_count_does_not_apply_to_ordinary_series():
    assert trained()["sparse_count"] == {"applies": False}


def test_one_week_of_training_falls_back_to_global_statistics_without_crashing():
    state = trained(days=7)
    slots = state["hourly_baseline"]["buckets"]
    assert all(occ < 2 for _m, _s, _n, occ in slots.values())  # 每個時段都只看過一次：全部退回全段統計
    spike = hour_after(7, lambda m, r: daily(m, r) + np.where(np.arange(len(m)) == 30, 300.0, 0.0))
    assert detectors.detect_all(state, spike)


def test_short_window_means_abstain_not_veto():
    """不滿 30 點時 residual_chronic 算不出分數：棄權，剩兩票都舉牌就成立。"""
    state = trained()
    short = hour_after(21, lambda m, r: daily(m, r) + np.where(np.arange(len(m)) == 10, 200.0, 0.0), minutes=20)
    assert len(detectors.detect_all(state, short)) == 1


def test_new_only_drops_what_was_already_reported():
    t = START
    found = [detectors.Anomaly(t + timedelta(minutes=i), 1.0, 5.0, ("hourly_baseline",)) for i in range(5)]
    assert detectors.new_only(found, None) == found
    assert [a.ts for a in detectors.new_only(found, t + timedelta(minutes=2))] == [f.ts for f in found[3:]]
    assert detectors.new_only(found, t + timedelta(minutes=4)) == []


def test_tail_probability_matches_direct_summation():
    det = detectors.SparseCount()
    p0 = det.tail_probability(0, 2.5, 1000.0)
    p1 = det.tail_probability(1, 2.5, 1000.0)
    assert p0 == 1.0 and p1 == pytest.approx(1 - (1000 / 1030) ** 2.5, rel=1e-9)
