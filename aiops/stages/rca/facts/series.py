"""事件群的原始時序（ctx.rca_series，ingestion.incident_series 抓的）-> 事實（metric_shift）：事件期間平均 − 事件前平均。

只算位移，不判斷誰先動（那是互相關／因果算子的事）；NaN、inf 丟掉，剩下的點數不夠就不輸出。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from aiops.engine.rca_types import Fact
from aiops.stages.rca.facts.windows import Windows

SOURCE = "stages.rca.facts.series"


def series_facts(series: dict[tuple[str, str], pd.DataFrame], w: Windows, min_points: int, skips: list[str]) -> list[Fact]:
    out: list[Fact] = []
    for (entity, metric), df in sorted(series.items(), key=lambda kv: kv[0]):
        if df is None or df.empty or not {"ts", "value"} <= set(df.columns):
            skips.append(f"metric_shift:{entity}:時序是空的或缺 ts／value 欄位")
            continue
        ts = pd.to_datetime(df["ts"], utc=True)
        values = pd.to_numeric(df["value"], errors="coerce").astype(float)
        ok = pd.Series(np.isfinite(values.to_numpy()), index=values.index)
        before = values[ok & (ts < w.start)]
        during = values[ok & (ts >= w.start) & (ts < w.end)]
        if len(before) < min_points or len(during) < min_points:
            skips.append(f"metric_shift:{entity}:點數 {len(before)}→{len(during)}，有一邊 < {min_points}")
            continue
        b, d = float(before.mean()), float(during.mean())
        out.append(Fact("metric_shift", entity, d - b, "", "delta",
                        {"metric": metric, "before": b, "during": d, "before_std": float(before.std(ddof=0)),
                         "during_max": float(during.max()), "n": int(len(during)), "n_before": int(len(before))}, SOURCE))
    return out
