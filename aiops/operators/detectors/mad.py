import pandas as pd

from aiops.core.context import AnomalyResult
from aiops.core.registry import OperatorRegistry
from aiops.operators.detectors.base import DetectorOperator

_CONSISTENCY = 0.6745  # 讓 MAD 在常態分布下跟標準差同尺度的常數


@OperatorRegistry.register("detector.mad")
class MADDetector(DetectorOperator):
    """Modified Z-Score（中位數與 MAD）：離群值不會把基準線拉歪，比 zscore 耐尖峰。params: threshold。

    跟 zscore 一樣先 fit 再 detect：基準線來自訓練期，而不是這一輪資料自己（否則異常本身會被當成常態）。
    """

    algorithm = "mad"
    tunable = {"threshold": (3.5, 1.0, 20.0)}

    def fit(self, df: pd.DataFrame) -> "MADDetector":
        values = df["value"]
        self.median_ = float(values.median())
        mad = float((values - self.median_).abs().median())
        self.mad_ = mad if mad != 0 else 1e-6  # 訓練期完全不變時避免除以 0
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"median": self.median_, "mad": self.mad_}

    def load_state(self, state: dict) -> "MADDetector":
        self.median_, self.mad_ = float(state["median"]), float(state["mad"])
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        scores = _CONSISTENCY * (df["value"] - self.median_).abs() / self.mad_
        return self._result(df, scores, scores > self.param("threshold"))
