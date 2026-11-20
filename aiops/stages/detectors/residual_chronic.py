"""殘差漂移票：看「扣掉週期基準後的殘差」的滾動平均是否持續偏離 0。

滾動平均跟「整段訓練期平均」比（已移除的 detector.chronic），流量有日夜週期時，每個上班時段都整段偏離（單獨每週標四、五千分鐘）。
這裡先白化（base.fit_seasonal_profile），殘差的滾動平均在正常時段應該在 0 附近；15 分鐘慢滑、持續低兩成一小時，
殘差會一路偏離，滾動平均跟著累積。

為什麼用滾動平均不用 CUSUM：偵測器每一輪只看到這一輪的資料（lookback 60 分鐘），跨輪不能留狀態——CUSUM 的累積和
每輪都要從窗口開頭重算，等於「窗口內的累積和」，還多一個漂移量 k 要定；而且 CUSUM 的分數在訓練期的分布跟起算點有關，
沒辦法像滾動平均那樣直接在訓練期殘差上量出「平常會偏到哪裡」來校正門檻。滾動平均記憶長度固定（window），
回測的 warmup_points／can_evaluate 也沿用 chronic 的寫法。代價是要湊滿 window 個點：慢滑要幾分鐘才累積到門檻。

分數 = |滾動平均(e)| / σ，σ = 1.4826 × MAD，在訓練期「滾動平均(e)」本身的分布上量（base.noise_stats），
殘差的自相關與時段基準的週間誤差都已經算進去。分母不小於序列水位的 min_scale_ratio（預設 1%）；σ 與水位都是 0 才退到 1.0（base.floored_scale）。
滾動平均一越線就成立（連續越線 min_run 個點才算的條件已於 2026-11-20 移除）。
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import (DetectorOperator, MIN_SCALE_RATIO_TUNABLE, fit_seasonal_profile, floored_scale, noise_stats, parse_tz,
                                         seasonal_residual)


@OperatorRegistry.register("detector.residual_chronic")
class ResidualChronicDetector(DetectorOperator):
    """params: threshold（預設 3）、window（滾動窗點數，預設 30）（tunable）；tz、min_slot_points（同 residual_shift）。"""

    algorithm = "residual_chronic"
    whitened = True  # 分數對時段基準算（開 snr 時門檻用常態分位數）
    tunable = {"threshold": (3.0, 1.0, 10.0), "window": (30.0, 3.0, 1440.0), **MIN_SCALE_RATIO_TUNABLE}
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
        if rolling.notna().sum() < 2:  # 訓練資料比窗口還短，量不到滾動平均的分布：用逐點殘差的分布（尺度偏大）
            rolling = pd.Series(resid)
        self.noise_ = noise_stats(rolling.to_numpy())
        self.window_ = int(self.param("window"))
        self.level_ = float(df["value"].median())  # 序列水位：相對底噪用（base.floored_scale）
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"profile": self.profile_, "noise": self.noise_, "window": self.window_, "level": self.level_}

    def load_state(self, state: dict) -> ResidualChronicDetector:
        # 舊基準線 noise 裡多帶的 tail_q、fpr（已移除的尾巴校正）不讀
        self.profile_, self.noise_ = dict(state["profile"]), {"noise": float(state["noise"]["noise"])}
        # 沒記水位的基準線（放回相對底噪之前存的）：level 是 None，相對底噪等下次重訓才有
        self.level_ = float(state["level"]) if state.get("level") is not None else None
        self.window_ = int(state["window"])
        self.fitted = True
        return self

    def warmup_points(self) -> int:
        return int(self.param("window")) - 1

    def can_evaluate(self, df: pd.DataFrame) -> bool:
        return len(df) >= int(self.param("window"))  # 湊不滿就算不出分數：棄權

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        if int(self.param("window")) != self.window_:
            # 後台改了 window：雜訊尺度是在舊窗口的滾動平均上量的，換窗口後尺度不對。照舊窗口判，下次重訓才換
            self.notes.append(f"window-changed:{self.window_}->{int(self.param('window'))}:until-retrain")
            self.params = {**self.params, "window": self.window_}
        scale = floored_scale(self.level_, self.param("min_scale_ratio"), self.noise_["noise"])
        rolling = self._rolling(seasonal_residual(df, self.profile_))
        rolling.index = df.index
        scores = (rolling.abs() / scale).fillna(0.0)
        return self._result(df, scores, scores > self.param("threshold"))
