import pandas as pd

from aiops.plugins.base import AnomalyResult, BaseDetector
from aiops.plugins.registry import DetectorRegistry


@DetectorRegistry.register
class ShiftDetector(BaseDetector):
    """一階跳變：相鄰兩點差值超過訓練期差值標準差的幾倍。params: threshold（預設 4.0）。"""

    name = "shift"

    def fit(self, df: pd.DataFrame) -> "ShiftDetector":
        self.diff_std_ = float(df["value"].diff().dropna().std(ddof=0))
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"diff_std": self.diff_std_}

    def load_state(self, state: dict) -> "ShiftDetector":
        self.diff_std_ = float(state["diff_std"])
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        threshold = float(self.params.get("threshold", 4.0))
        diffs = df["value"].diff().fillna(0.0)
        scores = (diffs / self.diff_std_).abs() if self.diff_std_ else diffs * 0.0
        return AnomalyResult(
            timestamps=[t.isoformat() for t in df["ts"]],
            values=df["value"].tolist(),
            scores=scores.round(4).tolist(),
            is_anomaly=(scores > threshold).tolist(),
        )
