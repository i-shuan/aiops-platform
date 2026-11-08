import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator


@OperatorRegistry.register("detector.rules")
class StaticRuleDetector(DetectorOperator):
    """靜態規則：超過上限或低於下限、而且連續 N 點才算（例如 CPU > 90 連續 5 分鐘）。不學習。

    params: upper、lower（至少要給一個）、consecutive（預設 1）、require_rule（預設 true）。
    規則值是人訂的，不開放自動調參（tunable 為空），要改請走人工提案。
    require_rule=false：上下限只來自核准的規則提案（例如規則升級器產生的），沒有規則的序列就不判、不報錯。
    """

    algorithm = "rules"
    other_params = frozenset({"upper", "lower", "consecutive", "require_rule"})

    @property
    def has_rule(self) -> bool:
        return self.params.get("upper") is not None or self.params.get("lower") is not None

    def validate_params(self) -> None:
        if not self.has_rule and self.params.get("require_rule", True):
            raise ValueError("detector.rules 至少要給 upper 或 lower")
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
        if not self.has_rule:  # require_rule=false 且這條序列沒有核准的規則：不判
            return self._result(df, excess, excess > 0)
        if upper is not None:
            excess = excess.where(v <= upper, (v - upper) / max(abs(float(upper)), 1e-9))
        if lower is not None:
            excess = excess.where(v >= lower, (lower - v) / max(abs(float(lower)), 1e-9))
        breach = excess > 0
        # 連續違規的長度：遇到沒違規就歸零
        run = breach.groupby((~breach).cumsum()).cumsum()
        flagged = run >= int(self.params.get("consecutive", 1))
        scores = (excess + breach.astype(float)).where(flagged, 0.0)  # 違規點分數 = 1 + 超出比例
        return self._result(df, scores, flagged)
