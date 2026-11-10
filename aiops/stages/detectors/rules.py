import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.params import number
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator, limit_breach


@OperatorRegistry.register("detector.rules")
class StaticRuleDetector(DetectorOperator):
    """靜態規則：超過上限或低於下限、而且連續 N 點才算（例如 CPU > 90 連續 5 分鐘）。不學習。

    params: upper、lower（至少要給一個）、consecutive（預設 1）、require_rule（預設 true）。
    規則值是人訂的，不開放自動調參（tunable 為空），要改請走人工提案。
    require_rule=false：上下限只來自核准的規則提案（例如規則升級器產生的），沒有規則的序列就不判、不報錯。
    """

    algorithm = "rules"
    other_params = frozenset({"upper", "lower", "consecutive", "require_rule"})
    requires_training = False  # 上下限是人訂的，不用歷史資料；服務剛上線只有一個點也照判，給統計算子還不能學的時候兜底

    @property
    def has_rule(self) -> bool:
        return self.params.get("upper") is not None or self.params.get("lower") is not None

    def can_evaluate(self, df: pd.DataFrame) -> bool:
        return self.has_rule  # require_rule=false 又沒有規則：不判也不投票（棄權），不是「看過了沒異常」

    def check_ranges(self) -> None:
        bounds = {}
        for key in ("upper", "lower"):
            if self.params.get(key) is not None:
                bounds[key] = number(f"detector.rules 的 {key}", self.params[key])
        if len(bounds) == 2 and bounds["lower"] >= bounds["upper"]:
            raise ValueError(f"detector.rules 的 lower（{bounds['lower']:g}）必須小於 upper（{bounds['upper']:g}）")
        consecutive = self.params.get("consecutive", 1)
        if isinstance(consecutive, bool) or not isinstance(consecutive, int) or consecutive < 1:
            raise ValueError(f"consecutive 必須 >= 1 的整數，不是 {consecutive!r}")
        if not isinstance(self.params.get("require_rule", True), bool):
            raise ValueError(f"require_rule 只能是 true／false，不是 {self.params['require_rule']!r}")

    def validate_params(self) -> None:
        self.check_ranges()
        # 「有沒有規則」要等各層合併完才知道（規則可以來自 target overrides 或核准的提案），所以不在建立時檢查
        if not self.has_rule and self.params.get("require_rule", True):
            raise ValueError("detector.rules 至少要給 upper 或 lower")

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
        if not self.has_rule:  # require_rule=false 且這條序列沒有核准的規則：不判
            none = pd.Series(0.0, index=df.index)
            return self._result(df, none, none > 0)
        # 違規點分數 = 1 + 超出比例；連續違規滿 consecutive 點才成立
        scores, flagged = limit_breach(df["value"], self.params.get("upper"), self.params.get("lower"),
                                       int(self.params.get("consecutive", 1)))
        return self._result(df, scores, flagged)
