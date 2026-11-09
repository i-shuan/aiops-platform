import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import SCALE_FLOOR_TUNABLE, DetectorOperator, is_integer_series, scale_floor

_CONSISTENCY = 0.6745  # 讓 MAD 在常態分布下跟標準差同尺度的常數


@OperatorRegistry.register("detector.mad")
class MADDetector(DetectorOperator):
    """Modified Z-Score（中位數與 MAD）：離群值不會把基準線拉歪，比 zscore 耐尖峰。
    params: threshold、min_scale／min_scale_ratio（分母底噪，見 base.scale_floor）。

    跟 zscore 一樣先 fit 再 detect：基準線來自訓練期，而不是這一輪資料自己（否則異常本身會被當成常態）。
    基準線存原始 MAD，底噪在 detect 時才套：後台調了 min_scale 不用等重訓就生效。
    """

    algorithm = "mad"
    tunable = {"threshold": (3.5, 1.0, 20.0), **SCALE_FLOOR_TUNABLE}

    def fit(self, df: pd.DataFrame) -> "MADDetector":
        values = df["value"]
        self.median_ = float(values.median())
        self.mad_ = float((values - self.median_).abs().median())
        self.integer_ = is_integer_series(values)  # 失敗數這類計數序列：分母至少 1 筆
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"median": self.median_, "mad": self.mad_, "integer": self.integer_}

    def load_state(self, state: dict) -> "MADDetector":
        self.median_, self.mad_ = float(state["median"]), float(state["mad"])
        self.integer_ = bool(state.get("integer", False))  # 舊版基準線沒記：只套絕對與相對下限
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        scale = scale_floor(self.mad_, None, self.param("min_scale"), self.param("min_scale_ratio"),
                            center=self.median_, integer=self.integer_)
        scores = _CONSISTENCY * (df["value"] - self.median_).abs() / scale
        return self._result(df, scores, scores > self.param("threshold"))
