import pandas as pd

from aiops.plugins.base import AnomalyResult, BaseDetector
from aiops.plugins.registry import DetectorRegistry


@DetectorRegistry.register
class ZScoreDetector(BaseDetector):
    """偏離訓練期平均幾個標準差。params: threshold。"""

    name = "zscore"
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
        return AnomalyResult(
            timestamps=[t.isoformat() for t in df["ts"]],
            values=df["value"].tolist(),
            scores=scores.round(4).tolist(),
            is_anomaly=(scores > self.param("threshold")).tolist(),
        )
