import math

import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator


@OperatorRegistry.register("detector.chronic")
class ChronicDetector(DetectorOperator):
    """慢性病：每個點都不算離譜（zscore 抓不到），但一段時間的平均持續偏離基準。

    用滾動平均跟訓練期平均比，尺度是「平均值的標準誤」std / sqrt(window)，所以小幅但持續的偏移也抓得到。
    單點尖峰會被滾動平均稀釋，這是刻意的。params: threshold、window（幾個點的滾動窗）。
    """

    algorithm = "chronic"
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

    def warmup_points(self) -> int:
        return int(self.param("window")) - 1  # 滾動平均要湊滿 window 個點才有值

    def can_evaluate(self, df: pd.DataFrame) -> bool:
        return len(df) >= int(self.param("window"))  # 湊不滿就沒有任何分數：棄權，不投反對票

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        window = int(self.param("window"))
        rolling = df["value"].rolling(window, min_periods=window).mean()
        stderr = self.std_ / math.sqrt(window) if self.std_ else 0.0
        scores = ((rolling - self.mean_).abs() / stderr).fillna(0.0) if stderr else rolling.fillna(0.0) * 0.0
        return self._result(df, scores, scores > self.param("threshold"))
