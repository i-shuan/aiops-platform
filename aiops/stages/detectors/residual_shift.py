"""殘差突變票：看「扣掉週期基準後的殘差」的相鄰差。

原始值的相鄰差（已移除的 detector.shift）每天 09:00／19:00 的流量階梯切換都會舉牌；這裡先白化（base.fit_seasonal_profile：
原值 − 該 週幾×小時 的訓練平均），每天固定的切換在殘差裡相鄰差接近 0，只有「這個時段不該有的斷崖」才會跳。
門檻依序列自己的殘差雜訊伸縮（base.noise_stats／calibrated_scale）：分數 = |Δe| / σ*，σ* 由訓練期 |Δe| 的
MAD 與 1−fpr 分位數決定，threshold 的意義不變。跳上去之後是新水位，不再報（持續偏離交給 residual_chronic）。
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import (RESIDUAL_TUNABLE, SCALE_FLOOR_TUNABLE, DetectorOperator, calibrated_scale,
                                         fit_seasonal_profile, is_integer_series, noise_stats, parse_tz,
                                         seasonal_residual)


@OperatorRegistry.register("detector.residual_shift")
class ResidualShiftDetector(DetectorOperator):
    """params: threshold（預設 4）、fpr、max_tail_factor、min_scale／min_scale_ratio（tunable）；
    tz（時段用的時區，比照 hourly_baseline，預設 UTC）、min_slot_points（時段至少幾個訓練點才用它，預設 30）。"""

    algorithm = "residual_shift"
    whitened = True  # 分數對時段基準算（開 snr 時門檻用常態分位數）
    tunable = {"threshold": (4.0, 1.0, 20.0), **RESIDUAL_TUNABLE, **SCALE_FLOOR_TUNABLE}
    other_params = frozenset({"tz", "min_slot_points", "ref_points"})

    def __init__(self, **params: Any):
        super().__init__(**params)
        self.tz = parse_tz(params.get("tz", "UTC"), "detector.residual_shift")
        self.min_slot_points = int(params.get("min_slot_points", 30))
        if self.min_slot_points < 2:
            raise TypeError("detector.residual_shift 的 min_slot_points 至少要 2")
        self.ref_points = int(params.get("ref_points", 1))
        if not 1 <= self.ref_points <= 60:
            raise TypeError("detector.residual_shift 的 ref_points 要在 1 到 60 之間（1 = 只跟前一分鐘比）")

    def _jump(self, resid) -> pd.Series:
        """殘差減「前 ref_points 點殘差的中位數」。ref_points=1 就是相鄰差；用中位數當參考水位，
        前面有一個雜訊點不會讓下一點看起來像跳變，斷崖後前幾分鐘（參考窗還沒被新水位佔過半）都會舉牌。"""
        e = pd.Series(resid, dtype=float)
        ref = e.shift(1).rolling(self.ref_points, min_periods=1).median()
        return (e - ref).fillna(0.0)

    def fit(self, df: pd.DataFrame) -> ResidualShiftDetector:
        self.profile_, resid = fit_seasonal_profile(df, self.tz, self.min_slot_points)
        self.noise_ = noise_stats(self._jump(resid).to_numpy()[1:], self.param("fpr"))
        values = df["value"]
        self.level_ = float(values.median())
        self.integer_ = is_integer_series(values)
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"profile": self.profile_, "noise": self.noise_, "level": self.level_, "integer": self.integer_}

    def load_state(self, state: dict) -> ResidualShiftDetector:
        self.profile_, self.noise_ = dict(state["profile"]), dict(state["noise"])
        self.level_, self.integer_ = float(state["level"]), bool(state.get("integer", False))
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        scale = calibrated_scale(self.noise_, self.param("threshold"), self.param("max_tail_factor"),
                                 min_scale=self.param("min_scale"), min_scale_ratio=self.param("min_scale_ratio"),
                                 level=self.level_, integer=self.integer_)
        jump = self._jump(seasonal_residual(df, self.profile_))
        jump.index = df.index
        scores = jump.abs() / scale
        return self._result(df, scores, scores > self.param("threshold"))
