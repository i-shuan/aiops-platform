import pandas as pd

from aiops.plugins.base import AnomalyResult, BaseDetector
from aiops.plugins.registry import DetectorRegistry


@DetectorRegistry.register
class StaticRuleDetector(BaseDetector):
    """靜態規則：超過上限或低於下限、而且連續 N 點才算（例如 CPU > 90 連續 5 分鐘）。不學習，fit 只是標記可用。

    params: upper、lower（至少要給一個）、consecutive（預設 1）。
    規則值是人訂的，不開放自動調參（tunable 為空），要改請走人工提案。
    """

    name = "rules"

    def validate_params(self) -> None:
        if self.params.get("upper") is None and self.params.get("lower") is None:
            raise ValueError("rules 偵測器至少要給 upper 或 lower")
        if int(self.params.get("consecutive", 1)) < 1:
            raise ValueError("consecutive 必須 >= 1")

    def fit(self, df: pd.DataFrame) -> "StaticRuleDetector":
        self.fitted = True
        return self

    def get_state(self) -> dict:
        return {}

    def load_state(self, state: dict) -> "StaticRuleDetector":
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        self.validate_params()
        upper, lower = self.params.get("upper"), self.params.get("lower")
        v = df["value"]
        excess = pd.Series(0.0, index=v.index)
        if upper is not None:
            excess = excess.where(v <= upper, (v - upper) / max(abs(float(upper)), 1e-9))
        if lower is not None:
            excess = excess.where(v >= lower, (lower - v) / max(abs(float(lower)), 1e-9))
        breach = excess > 0
        # 連續違規的長度：遇到沒違規就歸零
        run = breach.groupby((~breach).cumsum()).cumsum()
        flagged = run >= int(self.params.get("consecutive", 1))
        scores = (excess + breach.astype(float)).where(flagged, 0.0)  # 違規點分數 = 1 + 超出比例
        return AnomalyResult(
            timestamps=[t.isoformat() for t in df["ts"]],
            values=v.tolist(),
            scores=scores.round(4).tolist(),
            is_anomaly=flagged.tolist(),
        )
