import pandas as pd

from aiops.core.context import AnomalyResult
from aiops.core.registry import OperatorRegistry
from aiops.operators.detectors.base import DetectorOperator


@OperatorRegistry.register("detector.shift")
class ShiftDetector(DetectorOperator):
    """一階跳變：相鄰兩點差值超過訓練期差值標準差的幾倍。params: threshold。跳上去之後是新水位，不再報。"""

    algorithm = "shift"
    tunable = {"threshold": (4.0, 1.0, 20.0)}

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
        diffs = df["value"].diff().fillna(0.0)
        scores = (diffs / self.diff_std_).abs() if self.diff_std_ else diffs * 0.0
        return self._result(df, scores, scores > self.param("threshold"))
