import pandas as pd

from aiops.plugins.base import AnomalyResult, BaseDetector
from aiops.plugins.registry import DetectorRegistry


@DetectorRegistry.register
class ZScoreDetector(BaseDetector):
    """偏離訓練期平均幾個標準差。params: threshold（預設 3.0）。"""

    name = "zscore"

    def fit(self, df: pd.DataFrame) -> "ZScoreDetector":
        self.mean_ = float(df["value"].mean())
        self.std_ = float(df["value"].std(ddof=0))
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        threshold = float(self.params.get("threshold", 3.0))
        scores = ((df["value"] - self.mean_) / self.std_).abs() if self.std_ else df["value"] * 0.0
        return AnomalyResult(
            timestamps=[t.isoformat() for t in df["ts"]],
            values=df["value"].tolist(),
            scores=scores.round(4).tolist(),
            is_anomaly=(scores > threshold).tolist(),
        )
