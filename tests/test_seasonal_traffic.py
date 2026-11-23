"""有日夜／週末週期的流量序列：週期基準線（detector.hourly_baseline）單獨判（docs/SEASONAL_TRAFFIC_BACKTEST.md）。

資料是合成的，形狀照 ES 裡 mes-api 實際的每小時均值：上班時段（台北 09:00–18:59）平日 290、其餘 135，週末 145／75，
分鐘雜訊 std 約 9。把回測看到的現象固定下來：週期基準線單獨判（threshold 5）不誤報，而且抓得到尖峰時段流量掉到三成。

原本還有一個對照測試：非週期偵測器（detector.mad／quantile_drift，拿整段資料的中位數與 MAD 當基準）在這種兩階梯波形上
把平日上班時段整段判成異常，多數決（detector.ensemble）跟著誤報。那三個算子都已移除，對照測試跟著刪掉。
"""
import warnings

import numpy as np
import pandas as pd
import pytest

from aiops.engine.registry import OperatorRegistry
from aiops.interfaces.runner import load_all

TZ = "Asia/Taipei"


def _series(seed: int = 11) -> tuple[pd.DataFrame, pd.Timestamp]:
    """前 14 天訓練、後 7 天測試的每分鐘請求數。週一 00:00（台北）開始。"""
    rng = np.random.default_rng(seed)
    t0 = pd.Timestamp("2026-10-12 00:00", tz=TZ)
    idx = pd.date_range(t0, periods=21 * 1440, freq="1min")
    office = (idx.hour >= 9) & (idx.hour <= 18)
    level = np.where(idx.dayofweek >= 5, np.where(office, 145.0, 75.0), np.where(office, 291.0, 135.0))
    value = np.maximum(level + rng.normal(0, 9.0, len(idx)), 0.0).round()
    df = pd.DataFrame({"ts": idx.tz_convert("UTC"), "value": value})
    return df, (t0 + pd.Timedelta(days=14)).tz_convert("UTC")


def _flags(detectors: dict, test: pd.DataFrame) -> dict[str, np.ndarray]:
    """每 5 分鐘一輪、每輪看最近 60 分鐘（跟 it_otel 的 fetch.lookback_minutes: 60 一樣）。"""
    out = {k: np.zeros(len(test), bool) for k in detectors}
    for i in range(59, len(test), 5):
        win = test.iloc[i - 59:i + 1]
        for k, d in detectors.items():
            out[k][i - 59:i + 1] |= np.array(d.detect(win).is_anomaly, bool)
    return out


@pytest.fixture(scope="module")
def world():
    warnings.simplefilter("ignore")
    load_all()
    df, split = _series()
    train, test = df[df["ts"] < split], df[df["ts"] >= split].reset_index(drop=True)
    hourly = OperatorRegistry.create("detector.hourly_baseline", {"tz": TZ, "min_weeks": 1, "threshold": 5})
    hourly.fit(train)
    local = test["ts"].dt.tz_convert(TZ)
    # 週二 12:00 起 20 分鐘，流量掉到三成
    start = int(np.flatnonzero(((local.dt.dayofweek == 1) & (local.dt.hour == 12) & (local.dt.minute == 0)).to_numpy())[0])
    dropped = test.copy()
    dropped.loc[start:start + 19, "value"] *= 0.3
    win = np.zeros(len(test), bool)
    win[start:start + 20] = True
    return {"dets": {"hourly": hourly}, "test": test, "dropped": dropped, "win": win}


def test_seasonal_baseline_alone_has_almost_no_false_alarms_with_threshold_5(world):
    f = _flags(world["dets"], world["test"])["hourly"]
    assert f.sum() <= 10, f"一週 {int(f.sum())} 分鐘誤報"


def test_seasonal_baseline_alone_catches_a_drop_at_peak(world):
    f = _flags(world["dets"], world["dropped"])
    assert (f["hourly"] & world["win"]).sum() == 20               # 尖峰掉到三成：20 分鐘全抓到
    assert f["hourly"][world["win"]].argmax() == 0                # 第一個點就成立（延遲 0 分鐘）
    assert (f["hourly"] & ~world["win"]).sum() <= 10
