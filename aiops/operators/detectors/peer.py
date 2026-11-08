import pandas as pd

from aiops.core.context import AnomalyResult
from aiops.core.registry import OperatorRegistry
from aiops.operators.detectors.base import DetectorOperator

_MAD_TO_STD = 1.4826


def _deviation(df: pd.DataFrame) -> pd.Series:
    peers = df[[c for c in df.columns if c.startswith("peer_")]]
    if peers.shape[1] == 0:
        raise ValueError("peer 偵測器需要至少一個 peer_* 欄位")
    return df["value"] - peers.median(axis=1, skipna=True)


@OperatorRegistry.register("detector.peer")
class PeerDetector(DetectorOperator):
    """同儕比對：跟同一群（同服務其他 Pod、同型號其他機台）同時間的中位數比。

    大家一起變高（例如流量尖峰）不算異常；只有自己偏離同儕、而且偏得比平常多才算。
    target 要有 peers。params: threshold（偏離量是平常偏離幅度 MAD 的幾倍）。
    """

    algorithm = "peer"
    requires_peers = True
    tunable = {"threshold": (4.0, 1.0, 20.0)}

    def fit(self, df: pd.DataFrame) -> "PeerDetector":
        d = _deviation(df).dropna()
        self.center_ = float(d.median())
        self.mad_ = float((d - self.center_).abs().median() * _MAD_TO_STD)
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"center": self.center_, "mad": self.mad_}

    def load_state(self, state: dict) -> "PeerDetector":
        self.center_, self.mad_ = float(state["center"]), float(state["mad"])
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        d = _deviation(df)
        # 平常完全一致（MAD=0）時，用 1 當尺度，避免除以 0
        scores = ((d - self.center_).abs() / (self.mad_ or 1.0)).fillna(0.0)
        return self._result(df, scores, scores > self.param("threshold"))
