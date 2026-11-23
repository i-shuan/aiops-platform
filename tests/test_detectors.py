"""偵測器算子：契約（自動涵蓋每一個已註冊的偵測器）、各演算法的行為與邊界、execute 與 ctx 的互動。"""
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

import peer_probe  # 測試專用的 requires_peers 偵測器（tests/ 在 sys.path 上）
from aiops.engine.context import DetectorState, PeerRef, PipelineContext, Target
from aiops.engine.errors import BadRequest
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator

T0 = datetime(2026, 11, 8, tzinfo=timezone.utc)
DETECTORS = [n for n in OperatorRegistry.names("detector.")
             if issubclass(OperatorRegistry.get(n), DetectorOperator)]
# 滾動平均會稀釋單點尖峰，刻意的（它抓的是持續偏移）
# sparse_count 只判稀疏計數序列（非負整數、訓練期一半以上是 0），契約測試的序列水位在 50 附近、不是計數，它刻意不做事
IGNORES_SINGLE_SPIKE = {"detector.residual_chronic", "detector.sparse_count"}


def _df(values, **cols):
    return pd.DataFrame({"ts": pd.date_range(T0, periods=len(values), freq="60s"), "value": values, **cols})


SPIKE = [50.0] * 30 + [90.0] + [50.0] * 29


def _algo(name, **extra):
    return OperatorRegistry.get(name)(**extra)


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
            cls(**{key: high + 1}).validate_params()


def test_misspelled_param_is_rejected_at_construction():
    with pytest.raises(TypeError, match="沒有參數"):
        OperatorRegistry.get("detector.hourly_baseline")(treshold=3)


# ── 演算法行為 ──


def test_threshold_param():
    """60 點不到兩週：hourly_baseline 退回全段平均與標準差，尖峰約 7.7 個標準差。"""
    assert _algo("detector.hourly_baseline", threshold=3.0).fit(_df(SPIKE)).detect(_df(SPIKE)).is_anomaly[30]
    assert not any(_algo("detector.hourly_baseline", threshold=10.0).fit(_df(SPIKE)).detect(_df(SPIKE)).is_anomaly)


def test_constant_training_does_not_divide_by_zero():
    # 訓練期完全不變（std = 0）。2026-11-20 放回相對底噪（min_scale_ratio，預設 1%）之後斷言改了：
    # 改之前（分母只防除以 0，退到 1.0）：5→6 是 1.0 分、不是異常；5→15 是 10.0 分。
    # 改之後（分母 = max(量到的 0, |平均 5| × 1%) = 0.05）：5→6 是 20.0 分、是異常；5→15 是 200.0 分。
    # 水位小於 100 的常數序列，相對底噪（水位的 1%）比原本的 1.0 小，所以分數變高——這是 floored_scale 的定義
    # （有底噪就不再退 1.0），不是為了過測試調的輸入；輸入序列沒有動。
    r = _algo("detector.hourly_baseline").fit(_df([5.0] * 30)).detect(_df([5.0] * 28 + [6.0, 15.0]))
    assert np.isfinite(r.scores).all() and r.scores[28] == 20.0 and r.is_anomaly[28]
    assert r.is_anomaly[29] and r.scores[29] == 200.0
    # min_scale_ratio: 0（關掉相對底噪）= 改之前的行為：量到的尺度與底噪都是 0 才退 1.0，原本的斷言原樣保留
    r = _algo("detector.hourly_baseline", min_scale_ratio=0).fit(_df([5.0] * 30)).detect(_df([5.0] * 28 + [6.0, 15.0]))
    assert np.isfinite(r.scores).all() and r.scores[28] == 1.0 and not r.is_anomaly[28]
    assert r.is_anomaly[29] and r.scores[29] == 10.0


# ── execute：跟 ctx 的互動 ──


def test_execute_needs_fetch_first_and_peers_when_required():
    with pytest.raises(BadRequest, match="ingestion.fetch"):
        OperatorRegistry.create("detector.hourly_baseline").execute(PipelineContext("p", T0, Target("mock", "q", "e")))
    with pytest.raises(BadRequest, match="同儕"):
        OperatorRegistry.create(peer_probe.NAME).execute(_ctx(_df(SPIKE, peer_0=50.0)))  # target 沒有 peers


def test_execute_emits_events_and_saves_new_baseline():
    ctx = OperatorRegistry.create("detector.hourly_baseline").execute(_ctx(_df(SPIKE)))
    assert [(e.entity, e.attrs, e.value) for e in ctx.anomalies] == [("svc", {"dept": "d"}, 90.0)]
    assert "detector.hourly_baseline" in ctx.new_detector_states and ctx.anomaly_result.count == 1


def test_saved_baseline_is_used_instead_of_refitting():
    """已存的基準線學的是「正常 50」：用這一輪資料重 fit 會把 90 當成常態（全是 90 的窗），用已存的就抓得到。"""
    state = DetectorState({"buckets": {}, "global": [50.0, 1.0]}, T0)
    all_high = _df([90.0] * 60)
    assert not any(OperatorRegistry.create("detector.hourly_baseline").execute(_ctx(all_high)).anomaly_result.is_anomaly)
    ctx = OperatorRegistry.create("detector.hourly_baseline").execute(_ctx(all_high, detector_states={"detector.hourly_baseline": state}))
    assert all(ctx.anomaly_result.is_anomaly) and ctx.new_detector_states == {}


def test_train_window_retrains_and_records_the_window():
    train = _df([50.0 + (i % 3) for i in range(200)])
    ctx = OperatorRegistry.create("detector.hourly_baseline").execute(_ctx(_df(SPIKE), train_df=train))
    st = ctx.new_detector_states["detector.hourly_baseline"]
    assert st.points == 200 and st.train_start == train["ts"].min().to_pydatetime()


def test_param_precedence_workflow_over_tuned_over_default():
    df = _df(SPIKE)
    tuned = {"detector.hourly_baseline": {"threshold": 10.0}}
    assert not OperatorRegistry.create("detector.hourly_baseline").execute(_ctx(df, detector_params=tuned)).anomalies  # 核准的現行參數
    assert OperatorRegistry.create("detector.hourly_baseline", {"threshold": 3.0}).execute(_ctx(df, detector_params=tuned)).anomalies  # YAML 優先
    assert OperatorRegistry.create("detector.hourly_baseline").execute(_ctx(df)).anomalies  # 預設值


def test_out_of_range_tuned_params_are_rejected():
    with pytest.raises(BadRequest, match="超出範圍"):
        OperatorRegistry.create("detector.hourly_baseline").execute(_ctx(_df(SPIKE), detector_params={"detector.hourly_baseline": {"threshold": 99}}))


def test_one_operator_instance_is_safe_across_targets():
    """同一個算子實例連續處理兩條序列：第二條不能沿用第一條學到的基準線。"""
    op = OperatorRegistry.create("detector.hourly_baseline")
    low, high = _df([10.0 + (i % 2) for i in range(60)]), _df([500.0 + (i % 2) for i in range(60)])
    op.execute(_ctx(low))
    assert not op.execute(_ctx(high)).anomalies  # 500 是 high 自己的常態


def test_new_only_filters_by_last_stored_anomaly():
    op = OperatorRegistry.create("detector.new_only")
    ctx = OperatorRegistry.create("detector.hourly_baseline").execute(_ctx(_df(SPIKE + [90.0] * 5)))
    older = len(ctx.anomalies)
    ctx.last_anomaly_ts = T0 + timedelta(minutes=30)  # 第 30 分鐘那筆已經存過
    assert older > len(op.execute(ctx).anomalies) and all(a.ts > ctx.last_anomaly_ts for a in ctx.anomalies)
