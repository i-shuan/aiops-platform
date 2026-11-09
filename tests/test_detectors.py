"""偵測器算子：契約（自動涵蓋每一個已註冊的偵測器）、各演算法的行為與邊界、execute 與 ctx 的互動。"""
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from aiops.engine.context import DetectorState, PeerRef, PipelineContext, Target
from aiops.engine.errors import BadRequest
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator

T0 = datetime(2026, 11, 8, tzinfo=timezone.utc)
DETECTORS = [n for n in OperatorRegistry.names("detector.")
             if issubclass(OperatorRegistry.get(n), DetectorOperator)]
SAMPLE = {"detector.rules": {"upper": 80}}
# 滾動平均／滾動分位數會稀釋單點尖峰，刻意的（它們抓的是持續偏移與尾端漂移）
IGNORES_SINGLE_SPIKE = {"detector.chronic", "detector.quantile_drift"}


def _df(values, **cols):
    return pd.DataFrame({"ts": pd.date_range(T0, periods=len(values), freq="60s"), "value": values, **cols})


SPIKE = [50.0] * 30 + [90.0] + [50.0] * 29


def _algo(name, **extra):
    return OperatorRegistry.get(name)(**{**SAMPLE.get(name, {}), **extra})


def _frame_for(name):
    cls = OperatorRegistry.get(name)
    lags = {f"lag_{m}": 50.0 for m in _algo(name).required_lags()}  # 同比／環比：過去同一時刻的值
    return _df(SPIKE, peer_0=50.0, peer_1=51.0, **lags) if cls.requires_peers else _df(SPIKE, **lags)


def _ctx(df, entity="svc", peers=0, **kw):
    ctx = PipelineContext(pipeline="p", now=T0 + timedelta(hours=2),
                          target=Target("mock", "q", entity, {"dept": "d"}, [PeerRef("mock", f"p{i}") for i in range(peers)]))
    ctx.raw_df = df
    for k, v in kw.items():
        setattr(ctx, k, v)
    return ctx


# ── 契約：每個偵測器都要遵守 ──


@pytest.mark.parametrize("name", DETECTORS)
def test_detector_contract(name):
    algo, df = _algo(name), _frame_for(name)
    algo.validate_params()
    with pytest.raises(RuntimeError):
        algo.detect(df)  # 沒 fit 不能 detect
    assert algo.fit(df) is algo
    result = algo.detect(df)
    assert len(result.is_anomaly) == len(result.scores) == len(df) and result.algorithm
    assert result.is_anomaly[30] != (name in IGNORES_SINGLE_SPIKE), f"{name} 對尖峰的反應不符預期"


@pytest.mark.parametrize("name", DETECTORS)
def test_state_round_trips_through_json(name):
    df = _frame_for(name)
    trained = _algo(name).fit(df)
    restored = _algo(name).load_state(json.loads(json.dumps(trained.get_state())))
    assert restored.fitted and restored.detect(df).to_dict() == trained.detect(df).to_dict()


@pytest.mark.parametrize("name", DETECTORS)
def test_tunable_ranges_are_sane(name):
    cls = OperatorRegistry.get(name)
    for key, (default, low, high) in cls.tunable.items():
        assert low <= default <= high, f"{name}.{key}"
        with pytest.raises(ValueError):
            cls(**{**SAMPLE.get(name, {}), key: high + 1}).validate_params()


def test_misspelled_param_is_rejected_at_construction():
    with pytest.raises(TypeError, match="沒有參數"):
        OperatorRegistry.get("detector.zscore")(treshold=3)


# ── 演算法行為 ──


def test_zscore_threshold_param():
    assert _algo("detector.zscore", threshold=3.0).fit(_df(SPIKE)).detect(_df(SPIKE)).is_anomaly[30]
    assert not any(_algo("detector.zscore", threshold=10.0).fit(_df(SPIKE)).detect(_df(SPIKE)).is_anomaly)


def test_mad_resists_outliers_in_the_training_data():
    """訓練資料裡混了幾個大尖峰：zscore 的標準差被拉大、後來的尖峰漏掉；MAD 不受影響。"""
    rng = np.random.default_rng(1)
    train = rng.normal(50, 1, 300)
    train[::20] = 500  # 5% 的離群值
    test = np.full(60, 50.0)
    test[30] = 70.0
    z = _algo("detector.zscore").fit(_df(train.tolist())).detect(_df(test.tolist()))
    mad = _algo("detector.mad").fit(_df(train.tolist())).detect(_df(test.tolist()))
    assert not z.is_anomaly[30] and mad.is_anomaly[30]


def test_mad_constant_training_does_not_divide_by_zero():
    r = _algo("detector.mad").fit(_df([5.0] * 30)).detect(_df([5.0] * 29 + [6.0]))
    assert r.is_anomaly[29] and np.isfinite(r.scores).all()


def test_shift_constant_series_is_quiet_and_only_the_jump_point_is_flagged():
    assert not any(_algo("detector.shift").fit(_df([5.0] * 30)).detect(_df([5.0] * 30)).is_anomaly)
    noisy = [50 + (i % 3) * 0.5 for i in range(60)]
    step = noisy[:30] + [v + 20 for v in noisy[30:]]
    flagged = [i for i, f in enumerate(_algo("detector.shift").fit(_df(noisy)).detect(_df(step)).is_anomaly) if f]
    assert flagged == [30]  # 跳上去之後是新水位，不再報


def test_chronic_catches_small_sustained_drift_that_zscore_misses():
    base = [50 + (i % 5 - 2) * 0.5 for i in range(120)]
    drift = base[:60] + [v + 1.0 for v in base[60:]]
    assert not any(_algo("detector.zscore").fit(_df(base)).detect(_df(drift)).is_anomaly)
    assert any(_algo("detector.chronic", window=20).fit(_df(base)).detect(_df(drift)).is_anomaly)
    drop = base[:60] + [v - 1.0 for v in base[60:]]
    assert any(_algo("detector.chronic", window=20).fit(_df(base)).detect(_df(drop)).is_anomaly)


def test_chronic_edge_cases():
    assert not any(_algo("detector.chronic", window=30).fit(_df([50.0, 51.0, 49.0])).detect(_df([80.0] * 10)).is_anomaly)
    assert not any(_algo("detector.chronic", window=5).fit(_df([50.0] * 40)).detect(_df([50.0] * 40)).is_anomaly)


def test_peer_ignores_shared_spike_and_catches_lone_deviation():
    base = [50.0] * 60
    fit_df = _df(base, peer_0=[50.5] * 60)
    shared = _df(SPIKE, peer_0=SPIKE)
    lone = _df(SPIKE, peer_0=base)
    assert not any(_algo("detector.peer").fit(fit_df).detect(shared).is_anomaly)
    assert _algo("detector.peer").fit(fit_df).detect(lone).is_anomaly[30]


def test_peer_tolerates_missing_points_and_a_single_crazy_peer():
    peers = [50.0] * 60
    peers[10] = np.nan
    assert not any(_algo("detector.peer").fit(_df([50.0] * 60, peer_0=peers, peer_1=[50.5] * 60))
                   .detect(_df([50.0] * 60, peer_0=peers, peer_1=[50.5] * 60)).is_anomaly)
    train = _df([50.0] * 60, peer_0=[50.0] * 60, peer_1=[51.0] * 60, peer_2=[49.0] * 60)
    crazy = _df([50.0] * 60, peer_0=[50.0] * 60, peer_1=[90.0] * 60, peer_2=[49.0] * 60)
    assert not any(_algo("detector.peer").fit(train).detect(crazy).is_anomaly)  # 中位數不被一個壞同儕帶偏
    with pytest.raises(ValueError):
        _algo("detector.peer").fit(_df([50.0] * 10))


def test_rules_need_consecutive_breaches_and_bounds():
    det = _algo("detector.rules", consecutive=3).fit(_df([0.0]))
    assert det.detect(_df([50, 85, 50, 85, 85, 85, 50])).is_anomaly == [False] * 5 + [True, False]
    with pytest.raises(ValueError):
        OperatorRegistry.get("detector.rules")().validate_params()
    assert OperatorRegistry.get("detector.rules").tunable == {}  # 人訂的規則不開放自動調參


# ── execute：跟 ctx 的互動 ──


def test_execute_needs_fetch_first_and_peers_when_required():
    with pytest.raises(BadRequest, match="ingestion.fetch"):
        OperatorRegistry.create("detector.zscore").execute(PipelineContext("p", T0, Target("mock", "q", "e")))
    with pytest.raises(BadRequest, match="同儕"):
        OperatorRegistry.create("detector.peer").execute(_ctx(_df(SPIKE, peer_0=50.0)))


def test_execute_emits_events_and_saves_new_baseline():
    ctx = OperatorRegistry.create("detector.zscore").execute(_ctx(_df(SPIKE)))
    assert [(e.entity, e.attrs, e.value) for e in ctx.anomalies] == [("svc", {"dept": "d"}, 90.0)]
    assert "detector.zscore" in ctx.new_detector_states and ctx.anomaly_result.count == 1


def test_saved_baseline_is_used_instead_of_refitting():
    """已存的基準線學的是「正常 50」：用這一輪資料重 fit 會把 90 當成常態（全是 90 的窗），用已存的就抓得到。"""
    state = DetectorState({"mean": 50.0, "std": 1.0}, T0)
    all_high = _df([90.0] * 60)  # 60 點：剛好夠第一次訓練（min_train_points 預設 60），少於這個數會不學、不判
    assert not any(OperatorRegistry.create("detector.zscore").execute(_ctx(all_high)).anomaly_result.is_anomaly)
    ctx = OperatorRegistry.create("detector.zscore").execute(_ctx(all_high, detector_states={"detector.zscore": state}))
    assert all(ctx.anomaly_result.is_anomaly) and ctx.new_detector_states == {}


def test_train_window_retrains_and_records_the_window():
    train = _df([50.0 + (i % 3) for i in range(200)])
    ctx = OperatorRegistry.create("detector.zscore").execute(_ctx(_df(SPIKE), train_df=train))
    st = ctx.new_detector_states["detector.zscore"]
    assert st.points == 200 and st.train_start == train["ts"].min().to_pydatetime()


def test_param_precedence_workflow_over_tuned_over_default():
    df = _df(SPIKE)
    tuned = {"detector.zscore": {"threshold": 10.0}}
    assert not OperatorRegistry.create("detector.zscore").execute(_ctx(df, detector_params=tuned)).anomalies  # 核准的現行參數
    assert OperatorRegistry.create("detector.zscore", {"threshold": 3.0}).execute(_ctx(df, detector_params=tuned)).anomalies  # YAML 優先
    assert OperatorRegistry.create("detector.zscore").execute(_ctx(df)).anomalies  # 預設值


def test_out_of_range_tuned_params_are_rejected():
    with pytest.raises(BadRequest, match="超出範圍"):
        OperatorRegistry.create("detector.zscore").execute(_ctx(_df(SPIKE), detector_params={"detector.zscore": {"threshold": 99}}))


def test_one_operator_instance_is_safe_across_targets():
    """同一個算子實例連續處理兩條序列：第二條不能沿用第一條學到的基準線。"""
    op = OperatorRegistry.create("detector.zscore")
    low, high = _df([10.0 + (i % 2) for i in range(60)]), _df([500.0 + (i % 2) for i in range(60)])
    op.execute(_ctx(low))
    assert not op.execute(_ctx(high)).anomalies  # 500 是 high 自己的常態


def test_new_only_filters_by_last_stored_anomaly():
    op = OperatorRegistry.create("detector.new_only")
    ctx = OperatorRegistry.create("detector.zscore").execute(_ctx(_df(SPIKE + [90.0] * 5)))
    older = len(ctx.anomalies)
    ctx.last_anomaly_ts = T0 + timedelta(minutes=30)  # 第 30 分鐘那筆已經存過
    assert older > len(op.execute(ctx).anomalies) and all(a.ts > ctx.last_anomaly_ts for a in ctx.anomalies)
