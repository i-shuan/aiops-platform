"""誤報回饋的分位數反推門檻（admin.feedback_propose 用；取代固定 ×1.2）。

    新門檻 = max(S_max × 1.05, Q_99.9)，夾在偵測器 tunable 範圍內

- S_max：誤報事件群時窗內，這個偵測器（用現行生效參數）的最高異常分數。訓練只用時窗之前的資料（跟回測同一套，
  不偷看答案）。門檻要比 S_max 再高 5%，這一場誤報才會真的消失；只乘 1.2 的固定規則不管 S_max 是 3.6 還是 12，
  可能調了也沒用（還是越線），也可能一次調過頭。
- Q_99.9：過去 30 天（時窗之前）同一個偵測器的分數分佈的 99.9% 分位數，扣掉跟正式偵測同一套的訓練排除窗
  （這個實體的歷史事故、維護窗）與經驗庫裡的黃金事故——那些時段本來就該越線，算進「正常分數」會把門檻墊高。
  每天用前一個訓練窗（該管線 ingestion.fetch 的 train_lookback_minutes）重新 fit 再算當天分數（walk-forward），
  跟線上「基準線每天重訓」同一個節奏。99.9% = 每 1000 個正常點最多 1 點越線（每分鐘一點 ≈ 每天 1.4 點）；
  它是下限：S_max × 1.05 若還低於日常的千分之一尾端，代表門檻本來就太敏感，只調過 S_max 下一場還會再誤報。
- 前提：偵測器以「分數 > threshold」判異常（mad、zscore、hourly_baseline…都是），分數跟門檻同尺度。

資料撈不到（資料源已沒有那段、訓練點不足）時回傳原因字串，呼叫端退回固定規則並把原因寫進 reason。
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from aiops.control_plane.learning.replay import (
    Series,
    exclusion_windows,
    fetch_settings,
    fetch_span,
    replay_window,
    series_context,
)
from aiops.engine.context import PipelineContext
from aiops.engine.errors import NoDataError
from aiops.stages.detectors.base import DetectorOperator

QUANTILE = 0.999
LOOKBACK_DAYS = 30
MARGIN = 1.05
MIN_SCORES = 1000  # 少於這麼多個正常分數，99.9% 分位數只是最大值的雜訊，不用


def _ceil4(x: float) -> float:
    return math.ceil(x * 10000) / 10000 if math.isfinite(x) else x


def _in_windows(ts: pd.Timestamp, windows) -> bool:
    return any(pd.Timestamp(w.start) <= ts <= pd.Timestamp(w.end) for w in windows)


def infer(ctx: PipelineContext, cls: type[DetectorOperator], params: dict[str, Any], series: list[Series],
          pipeline: str, entity: str, start: datetime, end: datetime) -> dict[str, Any] | str:
    """回傳 {s_max, q, target, ...}；S_max 算不出來回傳原因字串（Q 算不出來只記 note，用 S_max × 1.05）。"""
    train_minutes, step = fetch_settings(ctx, pipeline)
    probe = cls(**params)
    lags, warmup = probe.required_lags(), probe.warmup_points()
    train_span = timedelta(minutes=train_minutes) + timedelta(seconds=step * warmup)
    exclusions = exclusion_windows(ctx, pipeline, entity, end)
    s_max, reasons, normal_scores, days = None, [], [], 0
    per_series = []
    for s in series:
        sctx = series_context(ctx, s, pipeline)
        # (a) 誤報時窗的最高分
        try:
            df = fetch_span(sctx, start - train_span, end, step, lags)
        except NoDataError as e:
            reasons.append(f"{s.query}：{e}")
            continue
        rep = replay_window(sctx, cls, params, df, start, end, exclusions)
        if isinstance(rep, str) or rep.max_score is None:
            reasons.append(f"{s.query}：{rep if isinstance(rep, str) else '時窗內沒有分數'}")
            continue
        s_max = rep.max_score if s_max is None else max(s_max, rep.max_score)
        # (b) 過去 30 天的正常分數：逐日 walk-forward
        q_start = start - timedelta(days=LOOKBACK_DAYS)
        try:
            hist = fetch_span(sctx, q_start - train_span, start, step, lags)
        except NoDataError as e:
            hist = None
            reasons.append(f"{s.query}：過去 {LOOKBACK_DAYS} 天沒有資料，不算分位數（{e}）")
        n_before = len(normal_scores)
        day = q_start
        while hist is not None and day < start:
            day_end = min(day + timedelta(days=1), start)
            ts = pd.to_datetime(hist["ts"], utc=True)
            chunk = hist[(ts >= pd.Timestamp(day - train_span)) & (ts < pd.Timestamp(day_end))]
            r = replay_window(sctx, cls, params, chunk.reset_index(drop=True), day,
                              day_end - timedelta(microseconds=1), exclusions)
            if not isinstance(r, str):
                days += 1
                normal_scores += [v for t, v in zip(r.ts, r.scores) if math.isfinite(v) and not _in_windows(t, exclusions)]
            day = day_end
        per_series.append({**s.to_dict(), "s_max": round(rep.max_score, 4), "window_points": len(rep.scores),
                           "normal_points": len(normal_scores) - n_before})
    if s_max is None:
        return "；".join(reasons) or "沒有可重跑的序列"
    q = float(np.quantile(normal_scores, QUANTILE)) if len(normal_scores) >= MIN_SCORES else None
    out = {"s_max": round(s_max, 4), "q": round(q, 4) if q is not None else None, "quantile": QUANTILE,
           "q_points": len(normal_scores), "q_days": days, "lookback_days": LOOKBACK_DAYS, "margin": MARGIN,
           "excluded_windows": len(exclusions), "series": per_series,
           # 無條件進位到小數 4 位、而且用回報出去的（四捨五入後的）s_max 算：門檻永遠 >= 報表上的 s_max × 1.05，
           # 不會因為四捨五入少了 0.0001 讓「剛好等於誤報分數 ×1.05」的點又越線（分數小到兩位數時才看得到這個誤差）
           "target": _ceil4(max(round(s_max, 4) * MARGIN, round(q, 4) if q is not None else -math.inf))}
    if q is None:
        out["note"] = f"過去 {LOOKBACK_DAYS} 天只有 {len(normal_scores)} 個正常分數（< {MIN_SCORES}），不算分位數，只用 S_max × {MARGIN}"
    if reasons:
        out["unchecked_series"] = reasons
    return out
