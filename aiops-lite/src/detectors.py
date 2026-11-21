"""異常偵測：季節基準（週幾×小時）+ 四個演算法 + new_only 去重。純函式，不碰網路與資料庫。

  hourly_baseline   每個「週幾幾點」各自的平均與標準差；時段看不滿 2 週就退回全段統計
  residual_shift    扣掉季節基準後的殘差「相鄰差」：這個時段不該有的斷崖
  residual_chronic  殘差的滾動平均持續偏離 0：慢滑、持續偏低／偏高
  sparse_count      平常幾乎都是 0 的計數序列，看最近 window 點累積幾筆（Gamma-Poisson 上尾機率）
  new_only          只留比上次已報的時間更新的點（重跑同一段不重複報）

裁決：前三個是「統計票」，同一個時間點要超過一半的參與者舉牌才算異常（只剩一個參與者時它說了算；
算不出分數的是棄權，不是反對票）。sparse_count 的分數是機率的對數、單位不同，不投票，越線就單獨成立。

1% 相對底噪（MIN_SCALE_RATIO）：分母至少是 |序列水位| 的 1%，整數序列至少 1 個單位——幾乎不動的序列不會因為
小數點後的抖動被放大成異常。

演算法與常數移植自封存的 aiops-platform（aiops/stages/detectors/），驗證狀態沿用該處的說明：門檻只在合成資料與
模擬紀錄上回放過，**沒有用真實產線資料驗證過（未驗證泛化）**。
已知死區：sparse_count 對「每天固定時段才發生」的稀疏事件會在那個時段誤報或變鈍；窗口內只有 1～2 筆不判；
基準線上線未滿 2 週時 hourly_baseline 整條退回全段統計，日夜週期明顯的序列在這段期間偏鈍。
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

STATE_VERSION = 1
MIN_TRAIN_POINTS = 60     # 訓練點數不到這個數不學、不判（幾十個點學到的基準線不可信）
MIN_SCALE_RATIO = 0.01    # 1% 相對底噪
MIN_SLOT_POINTS = 30      # 季節基準：一格至少幾個訓練點才用它，不夠就退到「小時」再退到全段
MAD_TO_STD = 1.4826
FPR = 0.001               # 訓練期允許超過門檻的點比例
MAX_TAIL_FACTOR = 3.0     # 尾巴校正最多把雜訊尺度放大幾倍（防訓練期混進事故後整條序列失明）
WEEK = pd.Timedelta(days=7)


@dataclass(frozen=True)
class Anomaly:
    ts: datetime
    value: float
    score: float
    detectors: tuple[str, ...]


# ── 共用：底噪、季節基準、自適應尺度 ──

def is_integer_series(values: Any) -> bool:
    arr = np.asarray(values, dtype=float)
    return bool(arr.size) and bool(np.isfinite(arr).all()) and bool((arr == np.round(arr)).all())


def scale_floor(scale: float, center: float, integer: bool) -> float:
    """分母保底：max(算出來的尺度, |中心| × 1%, 整數序列的 1 個單位)；全部是 0 時退回 1.0（寧可偏鈍也不除以 0）。"""
    floor = max(abs(float(center)) * MIN_SCALE_RATIO, 1.0 if integer else 0.0)
    out = max(float(scale) if np.isfinite(scale) else 0.0, floor)
    return out if out > 0 else 1.0


def _slots(ts: pd.Series, tz: str) -> tuple[np.ndarray, np.ndarray]:
    local = pd.to_datetime(ts, utc=True).dt.tz_convert(ZoneInfo(tz))
    return (local.dt.day_of_week * 24 + local.dt.hour).to_numpy(), local.dt.hour.to_numpy()


def seasonal_baseline(ts: pd.Series, profile: dict[str, Any]) -> np.ndarray:
    """每個時間點該扣的基準：週幾×小時 → 小時 → 全段（點數不到 MIN_SLOT_POINTS 就往下一層退）。"""
    slot, hour = _slots(ts, profile["tz"])
    slots, hours, fallback = profile["slots"], profile["hours"], float(profile["global"])
    out = np.empty(len(slot))
    for i, (s, h) in enumerate(zip(slot, hour)):
        m, n = slots.get(str(int(s)), (0.0, 0))
        if n < MIN_SLOT_POINTS:
            m, n = hours.get(str(int(h)), (0.0, 0))
            if n < MIN_SLOT_POINTS:
                m = fallback
        out[i] = m
    return out


def fit_profile(df: pd.DataFrame, tz: str) -> tuple[dict[str, Any], np.ndarray]:
    """學 週幾×小時 的平均，回傳 (profile, 訓練期殘差)。訓練期殘差用「留一週」算：某格有別週的資料時，
    每個點扣的是同一格別週的平均，不含自己——不然訓練殘差小得不真實，學到的雜訊尺度偏小，一換週就誤報。"""
    slot, hour = _slots(df["ts"], tz)
    v = df["value"].astype(float).to_numpy()
    ts = pd.to_datetime(df["ts"], utc=True)
    week = ((ts - ts.min()) // WEEK).to_numpy().astype(int)
    frame = pd.DataFrame({"s": slot, "h": hour, "w": week, "v": v})
    by_slot = frame.groupby("s")["v"].agg(["sum", "count"])
    by_slot_week = frame.groupby(["s", "w"])["v"].agg(["sum", "count"])
    by_hour = frame.groupby("h")["v"].agg(["mean", "count"])
    profile = {
        "tz": tz,
        "slots": {str(int(s)): [float(r["sum"] / r["count"]), int(r["count"])] for s, r in by_slot.iterrows()},
        "hours": {str(int(h)): [float(r["mean"]), int(r["count"])] for h, r in by_hour.iterrows()},
        "global": float(v.mean()),
    }
    base = seasonal_baseline(df["ts"], profile)
    s_sum, s_cnt = by_slot["sum"].reindex(slot).to_numpy(), by_slot["count"].reindex(slot).to_numpy()
    sw = by_slot_week.reindex(pd.MultiIndex.from_arrays([slot, week]))
    other_cnt = s_cnt - sw["count"].to_numpy()
    lowo = (other_cnt >= MIN_SLOT_POINTS) & (s_cnt >= MIN_SLOT_POINTS)
    with np.errstate(invalid="ignore", divide="ignore"):
        other_mean = (s_sum - sw["sum"].to_numpy()) / other_cnt
    return profile, v - np.where(lowo, other_mean, base)


def noise_stats(stat: np.ndarray) -> dict[str, float]:
    """訓練期統計量的雜訊尺度 σ（1.4826 × MAD，不被尖峰拉大）與 |統計量| 的 1−FPR 分位數。"""
    raw = np.asarray(stat, dtype=float)
    raw = raw[np.isfinite(raw)]
    if not len(raw):
        return {"noise": 0.0, "tail_q": 0.0}
    noise = float(np.median(np.abs(raw - np.median(raw))) * MAD_TO_STD)
    return {"noise": noise, "tail_q": float(np.quantile(np.abs(raw), 1.0 - FPR))}


def calibrated_scale(stats: dict[str, float], threshold: float, level: float, integer: bool) -> float:
    """σ* = clip(Q_{1−fpr} / threshold, 下限 σ, 上限 σ × MAX_TAIL_FACTOR)，再套底噪。
    意思是「訓練期最多只有 FPR 比例的點會超過門檻」：尾巴比常態肥的序列分母自動放大。"""
    noise, tail = float(stats["noise"]), float(stats["tail_q"]) / threshold
    return scale_floor(min(max(noise, tail), noise * MAX_TAIL_FACTOR), level, integer)


# ── 四個演算法：fit(df, ctx) -> state；score(state, ctx, df) -> (分數, 越線) 或 None（棄權）──
# ctx 是 fit_all 算好的共用資訊：{"tz", "profile", "level", "integer"} 與訓練期殘差 resid（只有 fit 時有）。

class HourlyBaseline:
    name, votes, threshold = "hourly_baseline", True, 3.0
    min_points, min_weeks = 3, 2.0  # 時段至少幾個點、至少看過幾次完整的一週才用自己的基準線

    def fit(self, df: pd.DataFrame, ctx: dict[str, Any], resid: np.ndarray) -> dict[str, Any]:
        local = pd.to_datetime(df["ts"], utc=True).dt.tz_convert(ZoneInfo(ctx["tz"]))
        frame = pd.DataFrame({"b": (local.dt.day_of_week * 24 + local.dt.hour).to_numpy(),
                              "d": local.dt.strftime("%Y-%m-%d").to_numpy(),
                              "v": df["value"].astype(float).to_numpy()})
        # 一個「週幾幾點」完整出現一次大約有幾個點；用它把點數換算成「看過幾週」
        full_slot = max(int(frame.groupby(["b", "d"]).size().max()), 1)
        agg = frame.groupby("b")["v"].agg(mean="mean", std=lambda s: float(s.std(ddof=0)), n="size")
        return {"buckets": {str(int(b)): [float(r["mean"]), float(r["std"]), int(r["n"]),
                                          round(float(r["n"]) / full_slot, 2)] for b, r in agg.iterrows()},
                "global": [float(frame["v"].mean()), float(frame["v"].std(ddof=0))]}

    def score(self, state: dict[str, Any], ctx: dict[str, Any], df: pd.DataFrame):
        g_mean, g_std = state["global"]
        slot, _ = _slots(df["ts"], ctx["tz"])
        means, stds = np.empty(len(slot)), np.empty(len(slot))
        for i, b in enumerate(slot):
            m, s, n, occ = state["buckets"].get(str(int(b)), (g_mean, g_std, 0, 0.0))
            if n < self.min_points or occ < self.min_weeks - 1e-9:
                m, s = g_mean, g_std  # 退回全段統計（等同 z-score，不會比它更吵）
            means[i] = m
            stds[i] = max(s, g_std * 0.1, scale_floor(0.0, m, ctx["integer"]))
        scores = np.abs(df["value"].astype(float).to_numpy() - means) / stds
        return scores, scores > self.threshold


class ResidualShift:
    name, votes, threshold = "residual_shift", True, 4.0

    @staticmethod
    def _jump(resid: np.ndarray) -> np.ndarray:
        return np.nan_to_num(pd.Series(resid, dtype=float).diff().to_numpy(), nan=0.0)

    def fit(self, df: pd.DataFrame, ctx: dict[str, Any], resid: np.ndarray) -> dict[str, Any]:
        return noise_stats(self._jump(resid)[1:])

    def score(self, state: dict[str, Any], ctx: dict[str, Any], df: pd.DataFrame):
        scale = calibrated_scale(state, self.threshold, ctx["level"], ctx["integer"])
        resid = df["value"].astype(float).to_numpy() - seasonal_baseline(df["ts"], ctx["profile"])
        scores = np.abs(self._jump(resid)) / scale
        return scores, scores > self.threshold


class ResidualChronic:
    name, votes, threshold, window = "residual_chronic", True, 3.0, 30

    def _rolling(self, resid: np.ndarray) -> pd.Series:
        return pd.Series(resid, dtype=float).rolling(self.window, min_periods=self.window).mean()

    def fit(self, df: pd.DataFrame, ctx: dict[str, Any], resid: np.ndarray) -> dict[str, Any]:
        rolling = self._rolling(resid)
        if rolling.notna().sum() < 2:  # 訓練資料比窗口短：退回逐點殘差（尺度偏大、偏鈍，不會更吵）
            rolling = pd.Series(resid)
        return {**noise_stats(rolling.to_numpy()), "window": self.window}

    def score(self, state: dict[str, Any], ctx: dict[str, Any], df: pd.DataFrame):
        if len(df) < self.window:
            return None  # 湊不滿一個窗口算不出分數：棄權
        # 30 個整數點的平均解析度是 1/30，底噪不套「整數至少 1」
        scale = calibrated_scale(state, self.threshold, ctx["level"], False)
        resid = df["value"].astype(float).to_numpy() - seasonal_baseline(df["ts"], ctx["profile"])
        scores = np.nan_to_num((self._rolling(resid).abs() / scale).to_numpy(), nan=0.0)
        return scores, scores > self.threshold


class SparseCount:
    """threshold 4 = 訓練期的發生率下，這種累積量出現的機率低於萬分之一（依模型算的，不是量出來的）。
    min_events 3 是作業上的取捨（一次失敗加一次重試常見 1～2 筆），不是統計量，未驗證泛化。"""
    name, votes, threshold, window, min_events = "sparse_count", False, 4.0, 30, 3
    prior, max_score = 0.5, 300.0  # Jeffreys 先驗；機率小到浮點數表示不了時的分數上限

    @staticmethod
    def _countable(values: np.ndarray) -> bool:
        return is_integer_series(values) and bool((values >= 0).all())

    def fit(self, df: pd.DataFrame, ctx: dict[str, Any], resid: np.ndarray) -> dict[str, Any]:
        values = df["value"].to_numpy(dtype=float)
        if not self._countable(values) or float((values == 0).mean()) < 0.5:
            return {"applies": False}  # 不是「一半以上是 0 的非負整數序列」：這一票不適用
        blocks, phi = len(values) // self.window, 1.0
        if blocks >= 2:  # 過度離散 φ：不重疊窗口總數的 變異數 / 平均（Poisson 是 1），成群出現的序列不會因為一群就越線
            sums = values[:blocks * self.window].reshape(blocks, self.window).sum(axis=1)
            if sums.mean() > 0:
                phi = max(1.0, float(sums.var(ddof=1)) / float(sums.mean()))
        return {"applies": True, "points": int(len(values)), "events": float(values.sum()), "dispersion": phi}

    def tail_probability(self, k: int, shape: float, n: float) -> float:
        """負二項上尾 P(K >= k)：發生率 ~ Gamma(shape, n)，K ~ Poisson(發生率 × window)。逐項加（不用 1 − 下尾）。"""
        if k <= 0:
            return 1.0
        w = float(self.window)
        q, log_p = w / (n + w), math.log(n / (n + w))
        total, j = 0.0, k
        while True:
            term = math.exp(math.lgamma(j + shape) - math.lgamma(j + 1) - math.lgamma(shape)
                            + shape * log_p + j * math.log(q))
            total += term
            ratio = q * (j + shape) / (j + 1)
            if ratio < 1 and term * ratio / (1 - ratio) <= total * 1e-12:
                return min(1.0, total)
            j += 1
            if j - k > 100000:
                raise ArithmeticError("負二項上尾沒有收斂")

    def score(self, state: dict[str, Any], ctx: dict[str, Any], df: pd.DataFrame):
        values = df["value"].to_numpy(dtype=float)
        if not state.get("applies") or len(values) < self.window or not self._countable(values):
            return None
        phi, shape = state["dispersion"], state["events"] / state["dispersion"] + self.prior
        counts = pd.Series(values).rolling(self.window, min_periods=self.window).sum().to_numpy()
        cache: dict[int, float] = {}
        scores = np.zeros(len(values))
        for i, k in enumerate(counts):
            if not np.isfinite(k) or k < self.min_events:
                continue
            k_eff = int(k // phi)  # 成群的序列：幾筆算一群，往下取整（偏保守）
            if k_eff not in cache:
                p = self.tail_probability(k_eff, shape, float(state["points"]))
                cache[k_eff] = self.max_score if p <= 0 else min(self.max_score, -math.log10(p))
            scores[i] = cache[k_eff]
        return scores, scores > self.threshold


DETECTORS = (HourlyBaseline(), ResidualShift(), ResidualChronic(), SparseCount())  # 無狀態：基準線都在 state 裡


def fit_all(df: pd.DataFrame, tz: str) -> dict[str, Any] | None:
    """用訓練窗（欄位 ts、value）學基準線，回傳可以 JSON 存檔的 state；點數不足回 None（不學、不判）。"""
    if len(df) < MIN_TRAIN_POINTS:
        return None
    profile, resid = fit_profile(df, tz)
    values = df["value"].astype(float)
    state: dict[str, Any] = {"version": STATE_VERSION, "tz": tz, "points": int(len(df)), "profile": profile,
                             "level": float(values.median()), "integer": is_integer_series(values)}
    for det in DETECTORS:
        state[det.name] = det.fit(df, state, resid)
    return state


def detect_all(state: dict[str, Any], df: pd.DataFrame) -> list[Anomaly]:
    """對這一輪的資料判異常。統計票多數決（超過一半的參與者）；不投票的偵測器越線就單獨成立。"""
    if df.empty:
        return []
    votes = np.zeros(len(df), dtype=int)
    peak = np.zeros(len(df))
    names: list[list[str]] = [[] for _ in range(len(df))]
    solo: list[tuple[str, np.ndarray, np.ndarray]] = []
    participants = 0
    for det in DETECTORS:
        result = det.score(state[det.name], state, df)
        if result is None:
            continue
        scores, flagged = result
        if not det.votes:
            solo.append((det.name, scores, flagged))
            continue
        participants += 1
        votes += flagged.astype(int)
        for i in np.flatnonzero(flagged):
            names[i].append(det.name)
            peak[i] = max(peak[i], scores[i])
    out = []
    ts, values = pd.to_datetime(df["ts"], utc=True), df["value"].astype(float).to_numpy()
    for i in range(len(df)):
        passed = participants > 0 and votes[i] * 2 > participants
        who = list(names[i]) if passed else []
        score = float(peak[i]) if passed else 0.0
        for name, scores, flagged in solo:
            if flagged[i]:
                who.append(name)
                score = max(score, float(scores[i]))
        if who:
            out.append(Anomaly(ts.iloc[i].to_pydatetime(), float(values[i]), round(score, 4), tuple(who)))
    return out


def new_only(anomalies: list[Anomaly], last_ts: datetime | None) -> list[Anomaly]:
    """只留比這條規則上次已報的時間更新的異常：每輪都重看最近一小時，不重複報、不重複計數。"""
    return anomalies if last_ts is None else [a for a in anomalies if a.ts > last_ts]
