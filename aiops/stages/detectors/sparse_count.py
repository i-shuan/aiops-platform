"""稀疏計數：平常幾乎都是 0 的計數序列（低流量服務的失敗數），看「最近 window 個點一共幾筆」是不是比訓練期的發生率多太多。

為什麼需要：這種序列的中位數與 MAD 都是 0，逐點偵測量不到雜訊尺度（分母只剩防除以 0 的 1.0，base.guard_scale），
分數約等於單一分鐘的筆數——每分鐘 1 筆、連續半小時，逐點看永遠是「1」，但訓練期一週才幾筆。逐點看不到的是「累積筆數」。

做法（計數資料的標準模型，跟服務、指標名稱無關）：
  訓練期 n 個點一共 S 筆 -> 發生率的後驗是 Gamma(S + 0.5, n)（Jeffreys 先驗；S = 0 也有定義）。
  「接下來 window 個點出現 k 筆以上」的預測分布是負二項（Gamma-Poisson），發生率估不準的不確定性已經算在裡面：
  訓練期越短、筆數越少，尾巴越厚，越不容易判異常。
  分數 = −log10 P(K >= k)。threshold 4 = 訓練期的發生率下，這種累積量出現的機率低於萬分之一。
  成群出現（一次失敗帶好幾筆）：用訓練期不重疊窗口的「變異數 / 平均」估過度離散 φ（至少 1），筆數與 S 都除以 φ
  再檢定（quasi-Poisson 的做法），成群的序列不會因為一群就越線。

只判「變多」：計數掉到 0 是流量類的問題，交給其他偵測器。

什麼序列才判（其他序列分數一律 0；不適用是常態，不記成錯誤，判準存在基準線的 state 裡查得到）：
  - 非負整數序列；
  - 訓練期一半以上的點是 0（中位數 0 = MAD 也是 0，正是逐點偵測退化的範圍；不稀疏的序列其他偵測器本來就看得到）。

分數是機率的對數，跟其他偵測器「雜訊尺度的倍數」不同單位；事件單獨成立（平台已經沒有表決節點）。

常數的來源（§100.5）：
  - threshold 預設 4（p < 1e-4）：30 點的窗每天約 48 個不重疊窗口，模型成立時每條序列約 200 天誤報一次。
    這是依模型算的，不是量出來的；真實計數的過度離散、作息只靠 φ 修正。
  - 沒有最少筆數的門檻（min_events 已於 2026-11-20 移除）：窗口裡只要有 1 筆就照模型算分數。訓練期 0 筆的序列
    出現 1～2 筆在模型上就可能越線（一次失敗加一次重試也會發），這是決定好的取捨；移除後沒有重新量測。
  - 稀疏判準「一半以上是 0」：中位數為 0 的定義，沒有調過。
  驗證資料與留出驗證見 docs/SPARSE_COUNT.md；沒有用真實資料驗證過。

死區（照實寫）：
  - 有作息的稀疏事件（每天固定時段才會失敗的排程）：這裡的發生率是整段平均，固定時段的群聚只靠 φ 吸收，
    吸收不了時會在那個時段誤報；反過來 φ 被拉大後，其他時段變鈍。
  - 訓練期本來就一直在失敗的序列（不稀疏）：不判。
"""
from __future__ import annotations

import math
import numpy as np
import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator, is_integer_series

PRIOR = 0.5       # Jeffreys 先驗（Poisson 發生率）
SPARSE_ZERO_FRACTION = 0.5  # 訓練期 0 的比例到這裡才算稀疏（= 中位數是 0）
MAX_SCORE = 300.0  # 機率小到浮點數表示不了時的上限


def tail_probability(k: int, shape: float, n: float, window: float) -> float:
    """負二項的上尾 P(K >= k)：發生率 ~ Gamma(shape, n)，K | 發生率 ~ Poisson(發生率 × window)。
    直接把上尾逐項加起來（不用 1 − 下尾：小機率會被捨入吃掉）。"""
    if k <= 0:
        return 1.0
    q = window / (n + window)  # 每一筆「落在這個窗口」的機率
    log_p = math.log(n / (n + window))

    def log_pmf(j: int) -> float:
        return math.lgamma(j + shape) - math.lgamma(j + 1) - math.lgamma(shape) + shape * log_p + j * math.log(q)

    total, j = 0.0, k
    while True:
        term = math.exp(log_pmf(j))
        total += term
        # 過了眾數之後每一項至少乘 q(j+shape)/(j+1)：比值夠小、而且這一項已經不影響總和就停
        ratio = q * (j + shape) / (j + 1)
        if ratio < 1 and term * ratio / (1 - ratio) <= total * 1e-12:
            break
        j += 1
        if j - k > 100000:  # 理論上到不了（比值最後一定小於 1）；到了就丟錯，不默默回一個不準的值
            raise ArithmeticError("負二項上尾沒有收斂")
    return min(1.0, total)


def dispersion(values: np.ndarray, window: int) -> float:
    """過度離散 φ：訓練期切成不重疊的 window 段，段內總數的 變異數 / 平均（Poisson 是 1）。至少 1；段數不到 2 或全是 0 回 1。"""
    blocks = len(values) // window
    if blocks < 2:
        return 1.0
    sums = values[:blocks * window].reshape(blocks, window).sum(axis=1)
    mean = float(sums.mean())
    return max(1.0, float(sums.var(ddof=1)) / mean) if mean > 0 else 1.0


@OperatorRegistry.register("detector.sparse_count")
class SparseCountDetector(DetectorOperator):
    """params: threshold（−log10 機率，預設 4）、window（滾動窗點數，預設 30）。"""

    algorithm = "sparse_count"
    tunable = {"threshold": (4.0, 2.0, 12.0), "window": (30.0, 5.0, 1440.0)}

    def _window(self) -> int:
        return int(self.param("window"))

    def fit(self, df: pd.DataFrame) -> SparseCountDetector:
        values = df["value"].to_numpy(dtype=float)
        countable = is_integer_series(values) and bool((values >= 0).all())
        self.window_ = self._window()
        self.points_ = int(len(values))
        self.events_ = float(values.sum()) if countable else 0.0
        self.zero_fraction_ = float((values == 0).mean()) if countable else 0.0
        self.dispersion_ = dispersion(values, self.window_) if countable else 1.0
        self.countable_ = countable
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"window": self.window_, "points": self.points_, "events": self.events_,
                "zero_fraction": self.zero_fraction_, "dispersion": self.dispersion_, "countable": self.countable_}

    def load_state(self, state: dict) -> SparseCountDetector:
        self.window_, self.points_ = int(state["window"]), int(state["points"])
        self.events_, self.zero_fraction_ = float(state["events"]), float(state["zero_fraction"])
        self.dispersion_, self.countable_ = float(state["dispersion"]), bool(state["countable"])
        self.fitted = True
        return self

    def warmup_points(self) -> int:
        return self._window() - 1

    def can_evaluate(self, df: pd.DataFrame) -> bool:
        return len(df) >= self._window()  # 湊不滿一個窗口就算不出累積筆數：棄權

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        zeros = pd.Series(0.0, index=df.index)
        values = df["value"].to_numpy(dtype=float)
        if not self.countable_ or self.zero_fraction_ < SPARSE_ZERO_FRACTION:
            return self._result(df, zeros, zeros > 0)  # 不是稀疏計數序列：不適用
        if not (is_integer_series(values) and bool((values >= 0).all())):
            # 訓練期是計數、這一輪卻出現小數或負值：資料變了，不判，而且要有人知道
            self.notes.append("sparse-count:not-a-count-series-anymore")
            return self._result(df, zeros, zeros > 0)
        if self._window() != self.window_:
            # 過度離散是在舊窗口的段上量的：照舊窗口判，下次重訓才換（跟 residual_chronic 同一個做法）
            self.notes.append(f"window-changed:{self.window_}->{self._window()}:until-retrain")
            self.params = {**self.params, "window": self.window_}
        w, phi = self.window_, self.dispersion_
        counts = pd.Series(values).rolling(w, min_periods=w).sum()
        shape = self.events_ / phi + PRIOR
        cache: dict[int, float] = {}
        scores = np.zeros(len(values))
        for i, k in enumerate(counts.to_numpy()):
            if not np.isfinite(k) or k < 1:
                continue  # 窗口還沒湊滿、或窗口裡一筆都沒有：0 分
            k_eff = int(k // phi)  # 成群的序列：幾筆算「一群」，往下取整（偏保守）
            if k_eff not in cache:
                p = tail_probability(k_eff, shape, float(self.points_), float(w))
                cache[k_eff] = MAX_SCORE if p <= 0 else min(MAX_SCORE, -math.log10(p))
            scores[i] = cache[k_eff]
        out = pd.Series(scores, index=df.index)
        return self._result(df, out, out > self.param("threshold"))
