import math

import pandas as pd

from aiops.plugins.base import AnomalyResult, BaseDetector
from aiops.plugins.registry import DetectorRegistry


@DetectorRegistry.register
class ChronicDetector(BaseDetector):
    """慢性病：每個點都不算離譜（zscore 抓不到），但一段時間的平均持續偏離基準。

    用滾動平均跟訓練期平均比，尺度是「平均值的標準誤」std / sqrt(window)，所以小幅但持續的偏移也抓得到。
    params: threshold、window（幾個點的滾動窗）。
    """

    name = "chronic"
    tunable = {"threshold": (3.0, 1.0, 10.0), "window": (30.0, 3.0, 1440.0)}

    def fit(self, df: pd.DataFrame) -> "ChronicDetector":
        self.mean_ = float(df["value"].mean())
        self.std_ = float(df["value"].std(ddof=0))
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"mean": self.mean_, "std": self.std_}

    def load_state(self, state: dict) -> "ChronicDetector":
        self.mean_, self.std_ = float(state["mean"]), float(state["std"])
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        window = int(self.param("window"))
        rolling = df["value"].rolling(window, min_periods=window).mean()
        stderr = self.std_ / math.sqrt(window) if self.std_ else 0.0
        scores = ((rolling - self.mean_).abs() / stderr).fillna(0.0) if stderr else rolling.fillna(0.0) * 0.0
        return AnomalyResult(
            timestamps=[t.isoformat() for t in df["ts"]],
            values=df["value"].tolist(),
            scores=scores.round(4).tolist(),
            is_anomaly=(scores > self.param("threshold")).tolist(),
        )
