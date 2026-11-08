"""cross_correlation（preprocess=hybrid）的已知限制。這些是演算法本身的死區／假訊號，不是測試資料造得不好：
每個案例都有一個「現況」測試（釘住目前行為，改了演算法就會被叫醒）與一個 strict xfail（寫出我們期望的行為）。
哪天演算法真的修好，xfail 會變成 XPASS 而失敗，提醒把它升格成一般測試。數字見 README「RCA」一節。
"""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from aiops.engine.context import PipelineContext
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident

T = datetime(2026, 11, 3, 15, 0, tzinfo=timezone.utc)
N = 120
TS = pd.date_range(T, periods=N, freq="60s")


def _step(rng, start, length, shift=0):
    x = np.zeros(N)
    x[start + shift:start + shift + length] = 10
    return x + rng.normal(0, 0.5, N)


def _rank(a, b, a_events, b_events, preprocess="hybrid"):
    inc = Incident([AnomalyEvent("a", "m", T + timedelta(minutes=m), 5.0) for m in a_events] +
                   [AnomalyEvent("b", "m", T + timedelta(minutes=m), 5.0) for m in b_events])
    ctx = PipelineContext(pipeline="p", now=T)
    ctx.rca_series = {("a", "m"): pd.DataFrame({"ts": TS, "value": a}), ("b", "m"): pd.DataFrame({"ts": TS, "value": b})}
    return OperatorRegistry.create("rca.root_cause.cross_correlation", {"preprocess": preprocess}).rank(inc, ctx)


def _leader(ranked):
    return [c.entity for c in ranked]


# ── 能用的基本情況（對照組）──


def test_staggered_step_with_real_lead_is_found():
    rng = np.random.default_rng(1)
    a, b = _step(rng, 40, 40), _step(rng, 40, 40, shift=3)  # b 真的晚 3 點，偵測器也晚 3 分標出
    # 這組有雜訊的小階梯 hybrid 與 diff 都找得到（diff 的 r 較低，0.78 vs 1.00）；不要拿它推論 diff 在高原事件的表現
    assert _leader(_rank(a, b, range(40, 80), range(43, 83), "hybrid")) == ["a"]
    assert _leader(_rank(a, b, range(40, 80), range(43, 83), "diff")) == ["a"]


# ── 死區 1：短事故／毛刺，遮罩內不到 min_mask_points（20）個點，該實體不參與 ──


def _spike_case():
    rng = np.random.default_rng(2)
    return _step(rng, 40, 4), _step(rng, 40, 4, shift=3), range(40, 44), range(43, 47)


def test_short_spike_is_excluded_by_min_mask_points():
    a, b, ea, eb = _spike_case()
    assert _rank(a, b, ea, eb) == []  # 現況：遮罩內只有 14 個點 < 20，兩個實體都不參與


@pytest.mark.xfail(strict=True, reason="死區：4 分鐘的毛刺遮罩內只有 14 點，低於 min_mask_points=20，hybrid 無法判斷先後")
def test_short_spike_lead_should_be_detectable():
    a, b, ea, eb = _spike_case()
    assert _leader(_rank(a, b, ea, eb)) == ["a"]


# ── 死區 2：事故還在持續，高原延伸到遮罩結束之後 + 兩個實體事件時間相同 -> 遮罩切口是共同的台階 ──
# 這是我當初（為了讓測試過而）換成隨機漫步之前的原始資料：斜坡升到 20、高原一路維持到時序結束（第 79 分），
# 事件標在第 20～59 分，遮罩（前後各擴 5 分）在第 64 分結束，把仍在持續的高原從中間切斷。


def _plateau_past_the_mask_case():
    wave = [0.0] * 20 + [float(i) for i in range(20)] + [20.0] * 40  # 80 點
    a = wave + [20.0] * (N - 80)
    b = ([0.0] * 3 + wave[:-3]) + [20.0] * (N - 80)  # b 晚 3 點
    return np.array(a), np.array(b), range(20, 60), range(20, 60)


def test_plateau_cut_by_the_mask_makes_the_cut_dominate_and_lag_zero():
    a, b, ea, eb = _plateau_past_the_mask_case()
    assert _rank(a, b, ea, eb) == []  # 現況：切口讓 lag=0（r≈0.98），lag 0 的配對不列入，真實的 3 點領先被蓋掉


@pytest.mark.xfail(strict=True, reason="死區：仍在持續的高原被遮罩從中間切斷，切口的共同台階主導相關性，波形上真實的 3 點領先被蓋掉")
def test_real_lead_should_survive_a_plateau_cut_by_the_mask():
    a, b, ea, eb = _plateau_past_the_mask_case()
    assert _leader(_rank(a, b, ea, eb)) == ["a"]


def test_same_waveform_is_found_when_the_events_cover_the_whole_plateau():
    """對照：同一組波形，事件涵蓋到高原結束就沒有切口，hybrid 正常（所以死區是遮罩切斷，不是波形本身）。"""
    a, b, _, _ = _plateau_past_the_mask_case()
    assert _leader(_rank(a, b, range(20, 110), range(23, 113))) == ["a"]


# ── 假訊號：波形其實同時變化，只有偵測器標出事件的時間差 > mask_expand_minutes（5）-> 憑空造出領先者 ──


def _simultaneous_but_detected_late_case(gap):
    rng = np.random.default_rng(11)
    a, b = _step(rng, 40, 40), _step(rng, 40, 40)  # 波形完全同步，真實領先 = 0
    return a, b, range(40, 80), range(40 + gap, 80 + gap)


def test_late_detection_alone_can_fabricate_a_leader():
    a, b, ea, eb = _simultaneous_but_detected_late_case(8)
    assert _leader(_rank(a, b, ea, eb)) == ["a"]  # 現況（不該有的假訊號）：偵測晚 8 分鐘就被當成「a 領先 b」
    assert _rank(a, b, ea, eb, "diff") == []  # diff 不吃遮罩，沒有這個問題


@pytest.mark.xfail(strict=True, reason="假訊號：兩個波形同步，只因偵測器標出事件的時間差 8 分鐘（> mask_expand 5）就報出領先者")
def test_simultaneous_waveforms_should_not_produce_a_leader():
    a, b, ea, eb = _simultaneous_but_detected_late_case(8)
    assert _rank(a, b, ea, eb) == []
