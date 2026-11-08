import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator


@OperatorRegistry.register("detector.zscore")
class ZScoreDetector(DetectorOperator):
    """偏離訓練期平均幾個標準差。params: threshold。"""

    algorithm = "zscore"
    tunable = {"threshold": (3.0, 1.0, 10.0)}

    def fit(self, df: pd.DataFrame) -> "ZScoreDetector":
        self.mean_ = float(df["value"].mean())
        self.std_ = float(df["value"].std(ddof=0))
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"mean": self.mean_, "std": self.std_}

    def load_state(self, state: dict) -> "ZScoreDetector":
        self.mean_, self.std_ = float(state["mean"]), float(state["std"])
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        scores = ((df["value"] - self.mean_) / self.std_).abs() if self.std_ else df["value"] * 0.0
        return self._result(df, scores, scores > self.param("threshold"))
