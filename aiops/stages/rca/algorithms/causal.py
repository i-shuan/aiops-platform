"""時序互相關（Time-lagged cross-correlation）：看誰的波動「領先」別人。

對事件時段每兩個不同實體的時序（先做前處理、再標準化），在 −max_lag ~ +max_lag 個點之間找相關係數
絕對值最大的位移：A 領先 B k 個點（k > 0）且 |r| >= min_corr，就記 A 一次「領先」，分數加 |r|。
領先別人越多、越明確的實體排越前面。

前處理（preprocess）：
- diff（預設）：一階差分去趨勢。對「一個個尖峰」有效，但事件是「整段水位抬高」（高原）時，差分只剩進出的兩個邊緣，
  相關性會被稀釋（產生器的標準答案資料：差分後 r 只有 0.2~0.35，不差分是 0.98~0.99）。
- hybrid：異常門控遮罩。每個實體只在「自己被標成異常的時間點前後 mask_expand_minutes 分鐘」保留穩健 z 分數
  （median／MAD 用遮罩外的平常點算），其他時間一律歸零，不做差分。高原波形完整保留；平常時段大家都是 0，
  日夜週期之類的共同起伏不會變成假相關。遮罩來源是事件群裡同實體（優先同指標）的異常事件；沒有異常事件的
  實體整條是 0，不參與比較。遮罩內少於 min_mask_points 個點的實體也不參與：只有零星幾個異常點時，等於拿十來個
  雜訊點去比、再在一堆位移裡挑最大的相關，很容易隨機超過門檻（真實資料的無事件對照組：沒有這道防線時 40/40 誤判）。

這是相關，不是嚴格的因果：兩條序列同時被第三者影響也可能看起來有先後。報告會寫出依據（領先誰、幾個點、r 值）讓人判斷。
需要前面有 ingestion.incident_series 節點把時序抓進 ctx.rca_series；沒有時序就不給候選（段落略過）。
use_topology=true 時只比較傳播圖上相鄰的實體（少很多誤判，但需要拓撲）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from aiops.engine.context import RootCandidate
from aiops.engine.registry import OperatorRegistry
from aiops.stages.rca.base import RootCauseOperator, first_seen, graph_edges


def best_lag(x: np.ndarray, y: np.ndarray, max_lag: int) -> tuple[int, float]:
    """回傳 (lag, r)：lag > 0 代表 x 領先 y lag 個點。r 是該位移下的皮爾森相關係數。"""
    best = (0, 0.0)
    for lag in range(-max_lag, max_lag + 1):
        if lag > 0:
            a, b = x[:-lag], y[lag:]
        elif lag < 0:
            a, b = x[-lag:], y[:lag]
        else:
            a, b = x, y
        if len(a) < 3 or np.std(a) == 0 or np.std(b) == 0:
            continue
        r = float(np.corrcoef(a, b)[0, 1])
        if np.isfinite(r) and abs(r) > abs(best[1]) + 1e-12:
            best = (lag, r)
    return best


PREPROCESS = ("diff", "hybrid")
MIN_BASELINE_POINTS = 5  # 遮罩外至少要有這麼多點才用來算 median／MAD，不然退回整段


def _series(df: pd.DataFrame) -> pd.Series:
    return df.set_index("ts")["value"].astype(float).sort_index()


def _prepared(df: pd.DataFrame) -> pd.Series:
    return _series(df).diff().dropna()


def _epoch_seconds(values) -> np.ndarray:
    return pd.DatetimeIndex(pd.to_datetime(values, utc=True)).as_unit("s").asi8


def anomaly_mask(index: pd.Index, event_times: list, expand_minutes: float) -> np.ndarray:
    """index 的每個時間點，是否落在任一異常時間點前後 expand_minutes 分鐘內。"""
    if not event_times or len(index) == 0:
        return np.zeros(len(index), dtype=bool)
    ts = _epoch_seconds(index)
    events = _epoch_seconds(event_times)
    window = expand_minutes * 60
    return (np.abs(ts[:, None] - events[None, :]) <= window).any(axis=1)


def masked_robust_z(df: pd.DataFrame, event_times: list, expand_minutes: float) -> pd.Series:
    """hybrid 前處理：遮罩內是穩健 z 分數，遮罩外是 0。median／MAD 用遮罩外的點算（平常的水位與波動），
    遮罩外的點太少（事件佔滿視窗）才退回整段。"""
    s = _series(df)
    mask = anomaly_mask(s.index, event_times, expand_minutes)
    base = s[~mask] if (~mask).sum() >= MIN_BASELINE_POINTS else s
    med = float(base.median())
    scale = 1.4826 * float((base - med).abs().median())
    if not scale:
        scale = float(base.std(ddof=0)) or 1.0
    z = (s - med) / scale
    return z.where(mask, 0.0)


def _event_times(incident, entity: str, metric: str) -> list:
    same_metric = [e.ts for e in incident.events if e.entity == entity and e.metric == metric]
    return same_metric or [e.ts for e in incident.events if e.entity == entity]


@OperatorRegistry.register("rca.root_cause.cross_correlation")
class CrossCorrelationRootCause(RootCauseOperator):
    """params: max_lag_points（預設 10）、min_corr（預設 0.5）、use_topology（預設 false）、
    preprocess（diff 預設｜hybrid）、mask_expand_minutes（hybrid 用，預設 5）、
    min_mask_points（hybrid 用，遮罩內至少幾個點才參與，預設 20）。"""

    def __init__(self, max_lag_points: int = 10, min_corr: float = 0.5, use_topology: bool = False,
                 preprocess: str = "diff", mask_expand_minutes: float = 5, min_mask_points: int = 20):
        if max_lag_points < 1 or not 0 < min_corr <= 1:
            raise ValueError("max_lag_points 至少 1；min_corr 必須在 0 到 1 之間")
        if preprocess not in PREPROCESS:
            raise ValueError(f"preprocess 只能是 {', '.join(PREPROCESS)}，不是 {preprocess!r}")
        if mask_expand_minutes < 0 or min_mask_points < 1:
            raise ValueError("mask_expand_minutes 不能小於 0；min_mask_points 至少 1")
        self.max_lag, self.min_corr, self.use_topology = int(max_lag_points), float(min_corr), bool(use_topology)
        self.preprocess, self.mask_expand = preprocess, float(mask_expand_minutes)
        self.min_mask_points = int(min_mask_points)

    def _prepare(self, incident, key: tuple[str, str], df: pd.DataFrame) -> pd.Series | None:
        if self.preprocess == "hybrid":
            z = masked_robust_z(df, _event_times(incident, *key), self.mask_expand)
            return z if int((z != 0).sum()) >= self.min_mask_points else None  # 遮罩太短：樣本不夠，不參與
        return _prepared(df)

    def rank(self, incident, ctx):
        prepared = {key: self._prepare(incident, key, df) for key, df in ctx.rca_series.items()
                    if key[0] in set(incident.entities)}
        series = {key: s for key, s in prepared.items() if s is not None}
        if len({e for e, _ in series}) < 2:
            return []  # 至少要兩個實體的時序才有先後可比
        adjacent = {frozenset(e) for e in graph_edges(ctx)} if self.use_topology else None
        credit: dict[str, float] = {}
        evidence: dict[str, list[str]] = {}
        keys = sorted(series)
        for i, ka in enumerate(keys):
            for kb in keys[i + 1:]:
                if ka[0] == kb[0]:
                    continue
                if adjacent is not None and frozenset((ka[0], kb[0])) not in adjacent:
                    continue
                joined = pd.concat([series[ka], series[kb]], axis=1, join="inner").dropna()
                if len(joined) < max(10, 2 * self.max_lag + 3):
                    continue
                x, y = joined.iloc[:, 0].to_numpy(), joined.iloc[:, 1].to_numpy()
                x, y = (x - x.mean()) / (x.std() or 1), (y - y.mean()) / (y.std() or 1)
                lag, r = best_lag(x, y, self.max_lag)
                if lag == 0 or abs(r) < self.min_corr:
                    continue
                leader, follower = (ka, kb) if lag > 0 else (kb, ka)
                credit[leader[0]] = credit.get(leader[0], 0.0) + abs(r)
                evidence.setdefault(leader[0], []).append(f"{follower[0]}（{abs(lag)} 點，r={r:.2f}）")
        if not credit:
            return []
        seen = first_seen(incident)
        ordered = sorted(credit, key=lambda e: (-credit[e], seen.get(e, (incident.start, 0))[0]))
        return [RootCandidate(e, credit[e], f"波動領先 {', '.join(evidence[e])}；累計 {credit[e]:.2f}")
                for e in ordered]
