import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import SCALE_FLOOR_TUNABLE, DetectorOperator, is_integer_series, scale_floor


@OperatorRegistry.register("detector.zscore")
class ZScoreDetector(DetectorOperator):
    """偏離訓練期平均幾個標準差。params: threshold、min_scale／min_scale_ratio（分母底噪，見 base.scale_floor）。

    訓練期完全不變（std = 0）時以前分數全 0（永遠不報）；現在分母退到底噪：常數序列突然跳一大格照樣抓得到。
    """

    algorithm = "zscore"
    tunable = {"threshold": (3.0, 1.0, 10.0), **SCALE_FLOOR_TUNABLE}

    def fit(self, df: pd.DataFrame) -> "ZScoreDetector":
        self.mean_ = float(df["value"].mean())
        self.std_ = float(df["value"].std(ddof=0))
        self.integer_ = is_integer_series(df["value"])
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"mean": self.mean_, "std": self.std_, "integer": self.integer_}

    def load_state(self, state: dict) -> "ZScoreDetector":
        self.mean_, self.std_ = float(state["mean"]), float(state["std"])
        self.integer_ = bool(state.get("integer", False))
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        scale = scale_floor(self.std_, None, self.param("min_scale"), self.param("min_scale_ratio"),
                            center=self.mean_, integer=self.integer_)
        scores = ((df["value"] - self.mean_) / scale).abs()
        return self._result(df, scores, scores > self.param("threshold"))
