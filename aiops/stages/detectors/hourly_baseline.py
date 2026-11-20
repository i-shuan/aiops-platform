"""168 時段週期基準線：一週 7 天 × 24 小時，每個時段各自學平均與標準差。

適合有明顯日週期／週末效應的指標（流量、交易量、登入數）：週一早上九點跟週日凌晨三點不該用同一條基準線。
時段依 tz 參數換算成當地時間（預設 UTC；台灣請設 Asia/Taipei，不然「上班時間」會偏 8 小時）。

冷啟動與資料不足時的安全回退（退回「全段的平均與標準差」，就是一般的 z-score）：
- 某個時段的訓練點太少（< min_points）
- 某個時段只看過不到 min_weeks 次（預設 2 次）：每個「週幾幾點」只看過一次的話，標準差只反映那一小時內的雜訊，
  不含週與週之間的變化，會被低估，平常的波動就會被當成異常。真實資料回放（7 天訓練）就是這樣吵起來的。
**流量類指標建議累積 3~4 週再信任它**；訓練窗口只有 7 天時，每個時段都只看過一次，整條基準線會全部退回全段統計。
訓練窗口夠長的設定：ingestion.fetch 的 train_lookback_minutes 設 20160（2 週）以上；Prometheus 一次查詢上限約 11000 點，
抓 2 週以上要把 step_seconds 調大（例如 300 秒）。
"""
from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import numpy as np
import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator, guard_scale

HOURS_PER_WEEK = 168


@OperatorRegistry.register("detector.hourly_baseline")
class HourlyBaselineDetector(DetectorOperator):
    """params: threshold（偏離幾個該時段標準差）、tz（時區，預設 UTC）、
    min_points（時段最少訓練點，預設 3）、min_weeks（時段至少要看過幾次完整的一週，預設 2）。

    分母就是該時段量到的標準差，沒有下限（「全段 std 的一成」與 min_scale 底噪已於 2026-11-20 移除）；
    只防除以 0：時段 std 不是正數就用全段 std，全段也不是就 1.0（base.guard_scale）。"""

    algorithm = "hourly_baseline"
    whitened = True  # 分數對時段基準算（開 snr 時門檻用常態分位數）
    tunable = {"threshold": (3.0, 1.0, 10.0)}
    other_params = frozenset({"tz", "min_points", "min_weeks"})

    def __init__(self, **params: Any):
        super().__init__(**params)
        try:
            self.tz = ZoneInfo(str(params.get("tz", "UTC")))
        except (ZoneInfoNotFoundError, ValueError) as e:
            raise TypeError(f"detector.hourly_baseline 的 tz 不是合法時區：{params.get('tz')!r}") from e
        self.min_points = int(params.get("min_points", 3))
        if self.min_points < 2:
            raise TypeError("detector.hourly_baseline 的 min_points 至少要 2")
        self.min_weeks = float(params.get("min_weeks", 2))
        if self.min_weeks < 1:
            raise TypeError("detector.hourly_baseline 的 min_weeks 至少要 1（要完全不退回請設 1，但變異會被低估）")

    def _local(self, ts: pd.Series) -> pd.Series:
        t = pd.to_datetime(ts)
        t = t.dt.tz_localize("UTC") if t.dt.tz is None else t
        return t.dt.tz_convert(self.tz)

    def _bucket(self, ts: pd.Series) -> np.ndarray:
        local = self._local(ts)
        return (local.dt.dayofweek * 24 + local.dt.hour).to_numpy()

    def _ready(self, n: int, occurrences: float) -> bool:
        return n >= self.min_points and occurrences >= self.min_weeks - 1e-9

    def fit(self, df: pd.DataFrame) -> HourlyBaselineDetector:
        local = self._local(df["ts"])
        frame = pd.DataFrame({"b": (local.dt.dayofweek * 24 + local.dt.hour).to_numpy(),
                              "d": local.dt.strftime("%Y-%m-%d").to_numpy(),
                              "v": df["value"].astype(float).to_numpy()})
        # 一個「週幾幾點」完整出現一次大約有幾個點（取各次出現中最多的）；用它把點數換算成「看過幾週」，
        # 這樣訓練窗口剛好從某小時中間開始／結束時，被切成兩半的那一小時合起來只算一次
        full_slot = max(int(frame.groupby(["b", "d"]).size().max()), 1)
        agg = frame.groupby("b")["v"].agg(mean="mean", std=lambda s: float(s.std(ddof=0)), n="size")
        self.buckets_ = {int(b): (float(r["mean"]), float(r["std"]), int(r["n"]), round(float(r["n"]) / full_slot, 2))
                         for b, r in agg.iterrows()}
        values = frame["v"]
        self.global_ = (float(values.mean()), float(values.std(ddof=0)))
        self.fitted = True
        return self

    @property
    def buckets_ready(self) -> int:
        """有幾個時段（168 個裡）已經夠格用自己的基準線；其餘退回全段統計。"""
        return sum(1 for (_m, _s, n, occ) in self.buckets_.values() if self._ready(n, occ))

    def get_state(self) -> dict:
        self._require_fitted()
        return {"buckets": {str(b): list(v) for b, v in self.buckets_.items()}, "global": list(self.global_),
                "tz": str(self.tz), "buckets_ready": self.buckets_ready, "min_weeks": self.min_weeks}

    def load_state(self, state: dict) -> HourlyBaselineDetector:
        # 舊版存的每個時段只有 (平均, 標準差, 點數)，沒有「看過幾週」：保守當成 1 週，會退回全段統計。
        # 舊基準線多帶的 integer 鍵（已移除的整數序列底噪）不讀
        self.buckets_ = {int(b): (float(v[0]), float(v[1]), int(v[2]), float(v[3]) if len(v) > 3 else 1.0)
                         for b, v in state["buckets"].items()}
        self.global_ = (float(state["global"][0]), float(state["global"][1]))
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        g_mean, g_std = self.global_
        means, stds = [], []
        for b in self._bucket(df["ts"]):
            m, s, n, occ = self.buckets_.get(int(b), (g_mean, g_std, 0, 0.0))
            if not self._ready(n, occ):
                m, s = g_mean, g_std  # 回退：全段 z-score
            means.append(m)
            stds.append(guard_scale(s, g_std))  # 只防除以 0：時段 std -> 全段 std -> 1.0
        scores = ((df["value"].astype(float) - np.array(means)) / np.array(stds)).abs()
        return self._result(df, scores, scores > self.param("threshold"))
