"""同比／環比：跟「同一時刻往前 N 分鐘」的自己比。

- detector.dod（日環比）：跟昨天同一時刻比，預設 offset_minutes=1440
- detector.wow（週同比）：跟上週同一時刻比，預設 offset_minutes=10080

先算差值（mode=diff：現在 − 過去；mode=ratio：(現在 − 過去) / |過去|），
訓練期學「平常差多少」（中位數與 MAD），偵測時看這次的差值偏離平常幾倍。
所以昨天本來就比今天高一點的固定落差不會被當成異常，只有「差得比平常多」才算。

注意：日環比在「週一 vs 週日」「週六 vs 週五」這種跨週末的日子，落差本來就大，會把「平常差多少」撐寬、變遲鈍。
有週末效應的指標請用 detector.wow（或 detector.hourly_baseline）。

workflow 的 ingestion.fetch 必須設 lags_minutes 包含 offset_minutes（例如 [1440]），
fetch 才會準備 lag_<分鐘> 欄位；訓練資料也要多往前抓 offset 那麼久。
"""
from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.errors import BadRequest
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator

_MAD_TO_STD = 1.4826


class _LagCompareDetector(DetectorOperator):
    default_offset: ClassVar[int] = 0
    tunable = {"threshold": (4.0, 1.0, 20.0)}
    other_params = frozenset({"offset_minutes", "mode"})

    def __init__(self, **params: Any):
        super().__init__(**params)
        if self.offset <= 0:
            raise TypeError(f"{self.operator_name} 的 offset_minutes 必須大於 0")
        if self.mode not in ("diff", "ratio"):
            raise TypeError(f"{self.operator_name} 的 mode 只能是 diff 或 ratio")

    @property
    def offset(self) -> int:
        return int(self.params.get("offset_minutes", self.default_offset))

    @property
    def mode(self) -> str:
        return str(self.params.get("mode", "diff"))

    def required_lags(self) -> list[int]:
        return [self.offset]

    def _delta(self, df: pd.DataFrame) -> pd.Series:
        past = df[f"lag_{self.offset}"].astype(float)
        now = df["value"].astype(float)
        if self.mode == "ratio":
            return (now - past) / past.abs().clip(lower=1e-9)
        return now - past

    def fit(self, df: pd.DataFrame) -> _LagCompareDetector:
        if f"lag_{self.offset}" not in df.columns:
            raise BadRequest(f"{self.operator_name} 訓練資料沒有 lag_{self.offset}（fetch 要設 lags_minutes）")
        d = self._delta(df).dropna()
        if d.empty:
            raise BadRequest(f"{self.operator_name} 訓練期間沒有 {self.offset} 分鐘前的資料可以比")
        self.center_ = float(d.median())
        scale = float((d - self.center_).abs().median() * _MAD_TO_STD)
        self.scale_ = scale or float(d.std(ddof=0)) or 1.0
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"center": self.center_, "scale": self.scale_, "offset_minutes": self.offset, "mode": self.mode}

    def load_state(self, state: dict) -> _LagCompareDetector:
        self.center_, self.scale_ = float(state["center"]), float(state["scale"])
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        scores = ((self._delta(df) - self.center_).abs() / self.scale_).fillna(0.0)  # 過去沒資料的點不判
        return self._result(df, scores, scores > self.param("threshold"))


@OperatorRegistry.register("detector.dod")
class DayOverDayDetector(_LagCompareDetector):
    """日環比：跟昨天同一時刻比。params: threshold、offset_minutes（預設 1440）、mode（diff／ratio）。"""

    algorithm = "dod"
    default_offset = 1440


@OperatorRegistry.register("detector.wow")
class WeekOverWeekDetector(_LagCompareDetector):
    """週同比：跟上週同一時刻比。params: threshold、offset_minutes（預設 10080）、mode（diff／ratio）。"""

    algorithm = "wow"
    default_offset = 10080
