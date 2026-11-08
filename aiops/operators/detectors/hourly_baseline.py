"""168 時段週期基準線：一週 7 天 × 24 小時，每個時段各自學平均與標準差。

適合有明顯日週期／週末效應的指標（流量、交易量、登入數）：週一早上九點跟週日凌晨三點不該用同一條基準線。
時段依 tz 參數換算成當地時間（預設 UTC；台灣請設 Asia/Taipei，不然「上班時間」會偏 8 小時）。
某個時段的訓練點太少（< min_points）就退回全段的平均與標準差。
訓練資料至少要一週才有意義：workflow 的 ingestion.fetch 請把 train_lookback_minutes 設成 10080 以上。
"""
from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import numpy as np
import pandas as pd

from aiops.core.context import AnomalyResult
from aiops.core.registry import OperatorRegistry
from aiops.operators.detectors.base import DetectorOperator

HOURS_PER_WEEK = 168


@OperatorRegistry.register("detector.hourly_baseline")
class HourlyBaselineDetector(DetectorOperator):
    """params: threshold（偏離幾個該時段標準差）、tz（時區，預設 UTC）、min_points（時段最少訓練點，預設 3）。"""

    algorithm = "hourly_baseline"
    tunable = {"threshold": (3.0, 1.0, 10.0)}
    other_params = frozenset({"tz", "min_points"})

    def __init__(self, **params: Any):
        super().__init__(**params)
        try:
            self.tz = ZoneInfo(str(params.get("tz", "UTC")))
        except (ZoneInfoNotFoundError, ValueError) as e:
            raise TypeError(f"detector.hourly_baseline 的 tz 不是合法時區：{params.get('tz')!r}") from e
        self.min_points = int(params.get("min_points", 3))
        if self.min_points < 2:
            raise TypeError("detector.hourly_baseline 的 min_points 至少要 2")

    def _bucket(self, ts: pd.Series) -> np.ndarray:
        t = pd.to_datetime(ts)
        t = t.dt.tz_localize("UTC") if t.dt.tz is None else t
        local = t.dt.tz_convert(self.tz)
        return (local.dt.dayofweek * 24 + local.dt.hour).to_numpy()

    def fit(self, df: pd.DataFrame) -> HourlyBaselineDetector:
        values = df["value"].astype(float)
        stats = values.groupby(self._bucket(df["ts"])).agg(["mean", "count"])
        stds = values.groupby(self._bucket(df["ts"])).std(ddof=0)
        self.buckets_ = {int(b): (float(stats.at[b, "mean"]), float(stds.at[b]), int(stats.at[b, "count"]))
                         for b in stats.index}
        self.global_ = (float(values.mean()), float(values.std(ddof=0)))
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"buckets": {str(b): list(v) for b, v in self.buckets_.items()}, "global": list(self.global_),
                "tz": str(self.tz)}

    def load_state(self, state: dict) -> HourlyBaselineDetector:
        self.buckets_ = {int(b): (float(m), float(s), int(n)) for b, (m, s, n) in state["buckets"].items()}
        self.global_ = (float(state["global"][0]), float(state["global"][1]))
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        g_mean, g_std = self.global_
        floor = max(g_std * 0.1, 1e-9)  # 某時段剛好完全不變時，避免除以 0 把小雜訊放大成異常
        means, stds = [], []
        for b in self._bucket(df["ts"]):
            m, s, n = self.buckets_.get(int(b), (g_mean, g_std, 0))
            if n < self.min_points:
                m, s = g_mean, g_std
            means.append(m)
            stds.append(max(s, floor))
        scores = ((df["value"].astype(float) - np.array(means)) / np.array(stds)).abs()
        if g_std == 0:
            scores = scores * 0.0
        return self._result(df, scores, scores > self.param("threshold"))
