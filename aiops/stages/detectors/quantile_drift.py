"""分位數漂移：盯尾端（p95／p99）而不是平均。

延遲這類指標常常平均沒變、但最慢的那 5% 越來越慢；平均型的偵測器（zscore、chronic）看不出來。
做法：滾動 window 個點算 quantile 分位數，訓練期學「滾動分位數平常落在哪」（中位數與 MAD），
偵測時看現在的滾動分位數偏離平常幾倍。需要先湊滿 window 個點（warmup_points）。
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator

_MAD_TO_STD = 1.4826


@OperatorRegistry.register("detector.quantile_drift")
class QuantileDriftDetector(DetectorOperator):
    """params: threshold、window（滾動窗點數）、quantile（預設 0.95）。"""

    algorithm = "quantile_drift"
    tunable = {"threshold": (3.0, 1.0, 10.0), "window": (30.0, 5.0, 1440.0)}
    other_params = frozenset({"quantile"})

    def __init__(self, **params: Any):
        super().__init__(**params)
        q = float(params.get("quantile", 0.95))
        if not 0 < q < 1:
            raise TypeError("detector.quantile_drift 的 quantile 必須在 0 到 1 之間（不含）")
        self.q = q

    def _rolling(self, df: pd.DataFrame) -> pd.Series:
        w = int(self.param("window"))
        return df["value"].astype(float).rolling(w, min_periods=w).quantile(self.q)

    def warmup_points(self) -> int:
        return int(self.param("window")) - 1

    def fit(self, df: pd.DataFrame) -> QuantileDriftDetector:
        r = self._rolling(df).dropna()
        if len(r) < 2:  # 訓練資料比窗口還短：退回整段的分位數
            r = pd.Series([float(df["value"].quantile(self.q))])
        self.center_ = float(r.median())
        scale = float((r - self.center_).abs().median() * _MAD_TO_STD)
        self.scale_ = scale or float(r.std(ddof=0) or 0) or abs(self.center_) * 0.01 or 1.0
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"center": self.center_, "scale": self.scale_, "quantile": self.q}

    def load_state(self, state: dict) -> QuantileDriftDetector:
        self.center_, self.scale_ = float(state["center"]), float(state["scale"])
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        scores = ((self._rolling(df) - self.center_).abs() / self.scale_).fillna(0.0)
        return self._result(df, scores, scores > self.param("threshold"))
