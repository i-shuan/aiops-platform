import pandas as pd

from aiops.plugins.base import AnomalyResult, BaseDetector
from aiops.plugins.registry import DetectorRegistry


@DetectorRegistry.register
class ZScoreDetector(BaseDetector):
    name = "zscore"

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        threshold = float(self.params.get("threshold", 3.0))
        std = df["value"].std(ddof=0)
        mean = df["value"].mean()
        scores = ((df["value"] - mean) / std).abs() if std else df["value"] * 0.0
        return AnomalyResult(
            timestamps=[t.isoformat() for t in df["ts"]],
            values=df["value"].tolist(),
            scores=scores.round(4).tolist(),
            is_anomaly=(scores > threshold).tolist(),
        )
