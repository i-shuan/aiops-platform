"""Q4：偵測器數值合約。演算法層直接呼叫 fit／get_state／detect（不經 execute），輸入：
長度 0、長度 1、全常數 5、全 0、含 NaN、含 +Inf。偵測段一律 10 點：前 9 點 = 訓練的典型值、最後一點 +1（小台階），
NaN／Inf 案例的偵測段也含 NaN／Inf。

另外檢查 get_state() 能不能嚴格 JSON 序列化（allow_nan=False；PostgreSQL 的 json／jsonb 不收 NaN／Infinity）。

用法：python scripts/probe/q4_numeric_contract.py [--root <aiops 原始碼根>]
"""
from __future__ import annotations

import json
import math
import os
import sys
import warnings

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402

warnings.filterwarnings("ignore")


def main() -> int:
    use_root(sys.argv)
    import numpy as np
    import pandas as pd

    from aiops.engine.registry import OperatorRegistry
    for m in ("mad", "zscore", "hourly_baseline", "quantile_drift", "compare", "peer", "rules"):
        __import__(f"aiops.stages.detectors.{m}")

    def frame(values):
        ts = pd.date_range("2026-11-02", periods=len(values), freq="1min", tz="UTC")
        df = pd.DataFrame({"ts": ts, "value": np.asarray(values, dtype=float)})
        df["lag_1440"] = df["value"]  # dod：昨天同一時刻 = 今天（差值 0）
        df["peer_0"] = df["value"]  # peer：同儕跟自己一樣（偏離 0）
        return df

    rng = np.random.default_rng(7)
    noisy = 100 + rng.normal(0, 1, 100)
    nan_v = noisy.copy(); nan_v[[3, 50, 97]] = np.nan
    inf_v = noisy.copy(); inf_v[50] = np.inf
    cases = {
        "len0": ([], []),
        "len1": ([5.0], [5.0] * 9 + [6.0]),
        "const5": ([5.0] * 100, [5.0] * 9 + [6.0]),
        "zeros": ([0.0] * 100, [0.0] * 9 + [1.0]),
        "nan": (nan_v, [100.0] * 7 + [np.nan, 100.0, 101.0]),
        "inf": (inf_v, [100.0] * 7 + [np.inf, 100.0, 101.0]),
    }
    detectors = {
        "detector.mad": {}, "detector.zscore": {}, "detector.hourly_baseline": {},
        "detector.quantile_drift": {"window": 5}, "detector.dod": {}, "detector.peer": {},
        "detector.rules": {"upper": 5.5},
    }
    for name, params in detectors.items():
        print(f"── {name} {params}")
        for case, (train_v, test_v) in cases.items():
            algo = OperatorRegistry.create(name, dict(params))
            train, test = frame(train_v), frame(test_v)
            try:
                algo.fit(train)
                state = algo.get_state()
            except Exception as e:  # noqa: BLE001
                print(f"   {case:7} fit RAISES {type(e).__name__}: {str(e)[:70]}")
                continue
            short = {k: (round(v, 6) if isinstance(v, float) else v) for k, v in state.items()
                     if k not in ("buckets", "tz")}
            try:
                json.dumps(state, allow_nan=False)
                js = "ok"
            except ValueError:
                js = "NaN/Inf!"
            if test.empty:
                print(f"   {case:7} fit ok state={short} json={js} detect: (無偵測段)")
                continue
            try:
                r = algo.detect(test)
                sc = r.scores
                nan_n = sum(1 for s in sc if isinstance(s, float) and math.isnan(s))
                finite = [s for s in sc if isinstance(s, float) and math.isfinite(s)]
                inf_n = sum(1 for s in sc if isinstance(s, float) and math.isinf(s))
                print(f"   {case:7} fit ok state={short} json={js} detect: flagged={r.count}/10 "
                      f"last_score={sc[-1]} max_finite={max(finite) if finite else None} nan={nan_n} inf={inf_n}")
            except Exception as e:  # noqa: BLE001
                print(f"   {case:7} fit ok state={short} json={js} detect RAISES {type(e).__name__}: {str(e)[:70]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
