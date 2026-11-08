"""rca.root_cause.cross_correlation 的 hybrid 前處理（異常門控遮罩）：高原型事件、日夜偽相關、參數。"""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from aiops.engine.context import PipelineContext
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.rca.algorithms.causal import anomaly_mask, masked_robust_z

T0 = datetime(2026, 11, 3, 14, 0, tzinfo=timezone.utc)
MINUTES = 150


def _envelope(minutes: np.ndarray, onset: int, ramp=10, plateau=60, decay=15) -> np.ndarray:
    """緩升 -> 高原 -> 衰減，值在 0..1。"""
    t = minutes - onset
    out = np.zeros_like(t, dtype=float)
    out[(t >= 0) & (t < ramp)] = t[(t >= 0) & (t < ramp)] / ramp
    out[(t >= ramp) & (t < ramp + plateau)] = 1.0
    d = (t >= ramp + plateau) & (t < ramp + plateau + decay)
    out[d] = 1 - (t[d] - ramp - plateau) / decay
    return out


def _cascade(onsets: dict[str, int | None], seed=7, amplitude=200.0, noise=10.0):
    """各服務共用同一個包絡線、只差起點（lag）；onset=None 的服務沒出事（只有雜訊）。回傳 (incident, ctx)。"""
    rng = np.random.default_rng(seed)
    minutes = np.arange(MINUTES)
    ts = [T0 + timedelta(minutes=int(m)) for m in minutes]
    ctx = PipelineContext(pipeline="rca_report", now=ts[-1])
    events = []
    for ent, onset in onsets.items():
        env = _envelope(minutes, onset) if onset is not None else np.zeros(MINUTES)
        value = 100 + amplitude * env + rng.normal(0, noise, MINUTES)
        ctx.rca_series[(ent, "lat")] = pd.DataFrame({"ts": pd.to_datetime(ts), "value": value})
        events += [AnomalyEvent(ent, "lat", ts[i], 5.0) for i in np.where(env >= 0.5)[0]]  # 偵測器標出的異常點
    if not events:
        events = [AnomalyEvent("quiet", "lat", ts[0], 1.0)]
    ctx.incidents = [Incident(events)]
    return ctx.incidents[0], ctx


def _rank(incident, ctx, **params):
    return OperatorRegistry.create("rca.root_cause.cross_correlation", {"min_corr": 0.5, **params}).rank(incident, ctx)


def test_plateau_cascade_diff_misses_it_but_hybrid_finds_the_leader_and_the_lags():
    inc, ctx = _cascade({"cart-api": 20, "ord-svc": 25, "bill-svc": 32})
    assert _rank(inc, ctx, max_lag_points=20) == []  # 差分只剩邊緣，被雜訊淹沒
    ranked = _rank(inc, ctx, max_lag_points=20, preprocess="hybrid")
    assert ranked[0].entity == "cart-api"
    assert "ord-svc（5 點" in ranked[0].reason and "bill-svc（12 點" in ranked[0].reason


def test_a_service_without_anomaly_events_is_all_zero_and_never_leads_or_follows():
    inc, ctx = _cascade({"cart-api": 20, "ord-svc": 25, "inv-svc": None})
    ranked = _rank(inc, ctx, max_lag_points=20, preprocess="hybrid")
    assert [c.entity for c in ranked] == ["cart-api"] and "inv-svc" not in ranked[0].reason


def test_unrelated_daily_cycles_do_not_lead_each_other_under_hybrid():
    """兩條各自有日夜週期（相位差 30 分鐘）、但沒有共同事件的序列：差分會看到假的「領先」，hybrid 不會。"""
    minutes = np.arange(24 * 60)
    ts = pd.to_datetime([T0 + timedelta(minutes=int(m)) for m in minutes])
    ctx = PipelineContext(pipeline="rca_report", now=T0)
    for ent, phase in (("web", 0), ("batch", 30)):
        ctx.rca_series[(ent, "lat")] = pd.DataFrame({"ts": ts, "value": 100 + 50 * np.sin(2 * np.pi * (minutes - phase) / 1440)})
    # 各自只有一個不相干時間的單點異常
    incident = Incident([AnomalyEvent("web", "lat", ts[100].to_pydatetime(), 4.0),
                         AnomalyEvent("batch", "lat", ts[900].to_pydatetime(), 4.0)])
    ctx.incidents = [incident]
    assert _rank(incident, ctx, max_lag_points=40)  # 差分：日夜週期互相「領先」，假相關
    assert _rank(incident, ctx, max_lag_points=40, preprocess="hybrid") == []


def test_entities_with_only_a_few_anomaly_points_do_not_take_part():
    """只有零星異常點：遮罩很短，等於拿十來個雜訊點互比，很容易隨機超過門檻，所以不讓它參與。"""
    minutes = np.arange(100)
    ts = pd.to_datetime([T0 + timedelta(minutes=int(m)) for m in minutes])
    rng = np.random.default_rng(3)
    ctx = PipelineContext(pipeline="rca_report", now=T0)
    events = []
    for ent in ("a", "b", "c", "d", "e"):
        ctx.rca_series[(ent, "lat")] = pd.DataFrame({"ts": ts, "value": 50 + rng.normal(0, 2, 100)})
        events.append(AnomalyEvent(ent, "lat", ts[50].to_pydatetime(), 3.0))  # 每個實體同一時間 1 個異常點
    incident = Incident(events)
    loose = OperatorRegistry.create("rca.root_cause.cross_correlation",
                                    {"preprocess": "hybrid", "max_lag_points": 10, "min_mask_points": 1})
    assert loose.rank(incident, ctx)  # 沒有防線：純雜訊也排得出「根因」
    assert _rank(incident, ctx, max_lag_points=10, preprocess="hybrid") == []  # 預設 20 點的防線擋掉


def test_mask_expands_by_minutes_not_by_points():
    idx = pd.to_datetime([T0 + timedelta(minutes=m) for m in range(30)])
    mask = anomaly_mask(idx, [T0 + timedelta(minutes=15)], expand_minutes=5)
    assert list(np.where(mask)[0]) == list(range(10, 21))  # 15 前後各 5 分鐘
    assert not anomaly_mask(idx, [], 5).any()


def test_baseline_comes_from_points_outside_the_mask():
    """事件佔了一大段：median／MAD 若用整段算，基準會被事件拉高；用遮罩外的平常點才對。"""
    idx = pd.to_datetime([T0 + timedelta(minutes=m) for m in range(60)])
    values = np.r_[np.full(20, 100.0), np.full(40, 300.0)] + np.tile([0.0, 2.0], 30)
    events = [T0 + timedelta(minutes=m) for m in range(25, 60)]
    z = masked_robust_z(pd.DataFrame({"ts": idx, "value": values}), events, expand_minutes=5)
    assert (z.iloc[:20] == 0).all()  # 遮罩外歸零
    assert z.iloc[30] > 50  # 遮罩內是相對「平常 100」的大偏離；若基準用整段（中位數 300）這裡會接近 0


def test_default_is_still_diff_and_matches_explicit_diff():
    inc, ctx = _cascade({"cart-api": 20, "ord-svc": 22}, amplitude=600, noise=2)
    default = _rank(inc, ctx, max_lag_points=10)
    explicit = _rank(inc, ctx, max_lag_points=10, preprocess="diff")
    assert default, "要有候選才算真的比到東西"
    assert [(c.entity, c.score, c.reason) for c in default] == [(c.entity, c.score, c.reason) for c in explicit]


@pytest.mark.parametrize("params", [{"preprocess": "level"}, {"preprocess": "hybrid", "mask_expand_minutes": -1},
                                    {"preprocess": "hybrid", "min_mask_points": 0}])
def test_bad_params_fail_at_load_time(params):
    from aiops.engine.errors import WorkflowError

    with pytest.raises((ValueError, WorkflowError)):
        OperatorRegistry.create("rca.root_cause.cross_correlation", params)
