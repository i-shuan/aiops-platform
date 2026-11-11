"""測試專用的同儕偵測器（requires_peers = True）。

正式的同儕偵測器已移除，但引擎的同儕路徑（target 的 peers／peer_group、ingestion.fetch 的 peer_* 欄位、
基底「沒有同儕就棄權／報錯」、training_points 只算同儕有值的列、回測要帶 peers）還在。這個算子只是讓那些路徑有東西可以測；
註冊成 test.* 名稱，不進 tests/test_detectors.py 的 detector.* 契約清單。

算法：自己跟同儕中位數的差，跟訓練期這個差的中位數比，尺度是差的 MAD（MAD 為 0 時用 1）。
"""
import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator

NAME = "test.peer_probe"
_MAD_TO_STD = 1.4826


def _deviation(df: pd.DataFrame) -> pd.Series:
    peers = df[[c for c in df.columns if c.startswith("peer_")]]
    if peers.shape[1] == 0:
        raise ValueError("需要至少一個 peer_* 欄位")
    return df["value"] - peers.median(axis=1, skipna=True)


@OperatorRegistry.register(NAME)
class PeerProbe(DetectorOperator):
    algorithm = "peer_probe"
    requires_peers = True
    tunable = {"threshold": (4.0, 1.0, 20.0)}

    def training_points(self, df: pd.DataFrame) -> int:
        return int(_deviation(df).notna().sum())  # 同儕那一刻沒值的列不算訓練點

    def fit(self, df: pd.DataFrame) -> "PeerProbe":
        d = _deviation(df).dropna()
        self.center_ = float(d.median())
        self.mad_ = float((d - self.center_).abs().median() * _MAD_TO_STD)
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"center": self.center_, "mad": self.mad_}

    def load_state(self, state: dict) -> "PeerProbe":
        self.center_, self.mad_ = float(state["center"]), float(state["mad"])
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        scores = ((_deviation(df) - self.center_).abs() / (self.mad_ or 1.0)).fillna(0.0)
        return self._result(df, scores, scores > self.param("threshold"))
