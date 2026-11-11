"""殘差漂移票：看「扣掉週期基準後的殘差」的滾動平均是否持續偏離 0。

detector.chronic 拿滾動平均跟「整段訓練期平均」比，流量有日夜週期時，每個上班時段都整段偏離（單獨每週標四、五千分鐘）。
這裡先白化（base.fit_seasonal_profile），殘差的滾動平均在正常時段應該在 0 附近；15 分鐘慢滑、持續低兩成一小時，
殘差會一路偏離，滾動平均跟著累積。

為什麼用滾動平均不用 CUSUM：偵測器每一輪只看到這一輪的資料（lookback 60 分鐘），跨輪不能留狀態——CUSUM 的累積和
每輪都要從窗口開頭重算，等於「窗口內的累積和」，還多一個漂移量 k 要定；而且 CUSUM 的分數在訓練期的分布跟起算點有關，
沒辦法像滾動平均那樣直接在訓練期殘差上量出「平常會偏到哪裡」來校正門檻。滾動平均記憶長度固定（window），
回測的 warmup_points／can_evaluate 也沿用 chronic 的寫法。代價是要湊滿 window 個點：慢滑要幾分鐘才累積到門檻。

門檻依序列自己的殘差雜訊伸縮（base.noise_stats／calibrated_scale）：分數 = |滾動平均(e)| / σ*，σ* 是在訓練期
「滾動平均(e)」本身的分布上量的（MAD 與 1−fpr 分位數），殘差的自相關與時段基準的週間誤差都已經算進去。
底噪不套「整數序列至少 1」：那條是給 MAD 為 0 的稀疏計數用的，30 個整數點的平均解析度是 1/30，持續少 0.5 筆／分也有意義；
min_scale 與 |水位| × min_scale_ratio 照套。
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import (RESIDUAL_TUNABLE, SCALE_FLOOR_TUNABLE, DetectorOperator, calibrated_scale,
                                         fit_seasonal_profile, noise_stats, parse_tz,
                                         seasonal_residual)


@OperatorRegistry.register("detector.residual_chronic")
class ResidualChronicDetector(DetectorOperator):
    """params: threshold（預設 3，同 detector.chronic）、window（滾動窗點數，預設 30，同 chronic）、fpr、max_tail_factor、
    min_scale／min_scale_ratio（tunable）；tz、min_slot_points（同 residual_shift）。"""

    algorithm = "residual_chronic"
    tunable = {"threshold": (3.0, 1.0, 10.0), "window": (30.0, 3.0, 1440.0), **RESIDUAL_TUNABLE, **SCALE_FLOOR_TUNABLE}
    other_params = frozenset({"tz", "min_slot_points"})

    def __init__(self, **params: Any):
        super().__init__(**params)
        self.tz = parse_tz(params.get("tz", "UTC"), "detector.residual_chronic")
        self.min_slot_points = int(params.get("min_slot_points", 30))
        if self.min_slot_points < 2:
            raise TypeError("detector.residual_chronic 的 min_slot_points 至少要 2")

    def _rolling(self, resid) -> pd.Series:
        w = int(self.param("window"))
        return pd.Series(resid).rolling(w, min_periods=w).mean()

    def fit(self, df: pd.DataFrame) -> ResidualChronicDetector:
        self.profile_, resid = fit_seasonal_profile(df, self.tz, self.min_slot_points)
        rolling = self._rolling(resid)
        if rolling.notna().sum() < 2:  # 訓練資料比窗口還短：退回逐點殘差的分布（尺度偏大、偏鈍，不會更吵）
            rolling = pd.Series(resid)
        self.noise_ = noise_stats(rolling.to_numpy(), self.param("fpr"))
        self.window_ = int(self.param("window"))
        self.level_ = float(df["value"].median())
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"profile": self.profile_, "noise": self.noise_, "window": self.window_, "level": self.level_}

    def load_state(self, state: dict) -> ResidualChronicDetector:
        self.profile_, self.noise_ = dict(state["profile"]), dict(state["noise"])
        self.window_ = int(state["window"])
        self.level_ = float(state["level"])
        self.fitted = True
        return self

    def warmup_points(self) -> int:
        return int(self.param("window")) - 1

    def can_evaluate(self, df: pd.DataFrame) -> bool:
        return len(df) >= int(self.param("window"))  # 湊不滿就算不出分數：棄權，不投反對票

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        if int(self.param("window")) != self.window_:
            # 後台改了 window：雜訊尺度是在舊窗口的滾動平均上量的，換窗口後尺度不對。照舊窗口判，下次重訓才換
            self.notes.append(f"window-changed:{self.window_}->{int(self.param('window'))}:until-retrain")
            self.params = {**self.params, "window": self.window_}
        scale = calibrated_scale(self.noise_, self.param("threshold"), self.param("max_tail_factor"),
                                 min_scale=self.param("min_scale"), min_scale_ratio=self.param("min_scale_ratio"),
                                 level=self.level_, integer=False)
        rolling = self._rolling(seasonal_residual(df, self.profile_))
        rolling.index = df.index
        scores = (rolling.abs() / scale).fillna(0.0)
        return self._result(df, scores, scores > self.param("threshold"))
