"""時序互相關（Time-lagged cross-correlation）：看誰的波動「領先」別人。

對事件時段每兩個不同實體的原始時序（先做一階差分去趨勢、再標準化），在 −max_lag ~ +max_lag 個點之間找相關係數
絕對值最大的位移：A 領先 B k 個點（k > 0）且 |r| >= min_corr，就記 A 一次「領先」，分數加 |r|。
領先別人越多、越明確的實體排越前面。

這是相關，不是嚴格的因果：兩條序列同時被第三者影響也可能看起來有先後。報告會寫出依據（領先誰、幾個點、r 值）讓人判斷。
需要前面有 ingestion.incident_series 節點把時序抓進 ctx.rca_series；沒有時序就不給候選（段落略過）。
use_topology=true 時只比較傳播圖上相鄰的實體（少很多誤判，但需要拓撲）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from aiops.core.context import RootCandidate
from aiops.core.registry import OperatorRegistry
from aiops.operators.rca.base import RootCauseOperator, first_seen, graph_edges


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


def _prepared(df: pd.DataFrame) -> pd.Series:
    s = df.set_index("ts")["value"].astype(float).sort_index()
    return s.diff().dropna()


@OperatorRegistry.register("rca.root_cause.cross_correlation")
class CrossCorrelationRootCause(RootCauseOperator):
    """params: max_lag_points（預設 10）、min_corr（預設 0.5）、use_topology（預設 false）。"""

    def __init__(self, max_lag_points: int = 10, min_corr: float = 0.5, use_topology: bool = False):
        if max_lag_points < 1 or not 0 < min_corr <= 1:
            raise ValueError("max_lag_points 至少 1；min_corr 必須在 0 到 1 之間")
        self.max_lag, self.min_corr, self.use_topology = int(max_lag_points), float(min_corr), bool(use_topology)

    def rank(self, incident, ctx):
        series = {key: _prepared(df) for key, df in ctx.rca_series.items() if key[0] in set(incident.entities)}
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
