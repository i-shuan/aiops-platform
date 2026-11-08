"""外掛抽象契約：領域層只依賴這裡，不 import 具體演算法。"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class AnomalyResult:
    timestamps: list
    values: list
    scores: list
    is_anomaly: list

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamps": self.timestamps,
            "values": self.values,
            "scores": self.scores,
            "is_anomaly": self.is_anomaly,
            "anomaly_count": int(sum(self.is_anomaly)),
        }


class BaseDetector(ABC):
    name: str = ""

    def __init__(self, params: dict[str, Any] | None = None):
        self.params = params or {}

    @abstractmethod
    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        """df 欄位固定為 ts, value。"""


class BaseLLMProvider(ABC):
    name: str = ""

    @abstractmethod
    def complete(self, prompt: str) -> str: ...
