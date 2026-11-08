"""基準曲線與「系統狀態」：每個服務、每一分鐘的期望值，全部由這裡算一次。

SystemState 是唯一的故障狀態函數：metrics（整段 2 週的解析模型）跟 traces（只在事件附近逐筆走訪呼叫樹）都讀同一份陣列，
所以事件期間兩邊對得起來：metrics 的每分鐘平均延遲 = trace 走訪的期望耗時，錯誤率、請求量也一樣。

每個服務每分鐘的量（索引 i = 從 window.start 起第幾分鐘）：
- rate[s][i]     期望請求數／分鐘：入口服務 = 日夜曲線 × 週末係數；下游 = 上游請求數 × 呼叫機率 × 重試倍數（一路乘下去）
- self_ms[s][i]  自己的處理時間平均（基準 × 每分鐘獨立的高斯抖動 + 故障偏移 × I(t−lag)）
- own_err[s][i]  自己出錯的機率（接近 0 的截斷常態 + 偶發單點毛刺 + 故障錯誤率 × I(t−lag)）
- mult[s][i]     對下游服務的呼叫次數倍數（重試風暴，平常是 1）
- e2e_ms[s][i]   這個服務一次 transaction 的期望總耗時 = self_ms + Σ 呼叫機率 × 倍數 × (網路 + 下游 e2e)（下游先算）
- e2e_err[s][i]  一次 transaction 失敗的機率 = 1 − (1 − own_err) × Π(1 − 呼叫機率 × 下游 e2e_err)（下游失敗會往上傳）
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .faults import Scenario
from .topology import Topology

SELF_SIGMA = 0.35       # 處理時間的對數常態 sigma（右偏長尾）
EXTERNAL_SIGMA = 0.30   # 外部依賴呼叫耗時的 sigma
METRIC_SIGMA = 0.40     # metrics 直方圖裡每筆請求總耗時的 sigma
JITTER = 0.03           # 每分鐘處理時間的獨立高斯抖動（相對）
BASE_ERR_MEAN, BASE_ERR_SD = 0.002, 0.0015
SPIKE_PROB, SPIKE_LOW, SPIKE_HIGH = 1 / 1500, 0.05, 0.12   # 偶發單點錯誤率毛刺：每分鐘機率、幅度範圍
WEEKEND_FACTOR = 0.4


def lognormal_mu(mean: np.ndarray | float, sigma: float):
    """對數常態要「平均」剛好等於 mean 時的 mu（median = mean / e^(σ²/2)）。"""
    return np.log(np.maximum(mean, 1e-6)) - sigma * sigma / 2


def diurnal(ts_utc: pd.DatetimeIndex, tz: str) -> np.ndarray:
    """入口流量的形狀（0~1 再乘基準）：當地 04:00 最低、16:00 最高；週末 × 0.4。"""
    local = ts_utc.tz_convert(tz)
    h = local.hour.to_numpy() + local.minute.to_numpy() / 60.0
    shape = 0.5 - 0.5 * np.cos(2 * math.pi * (h - 4.0) / 24.0)
    weekend = np.where(local.day_of_week.to_numpy() >= 5, WEEKEND_FACTOR, 1.0)
    return (0.15 + 0.85 * shape) * weekend


def _rng(seed: int, *keys: int) -> np.random.Generator:
    return np.random.default_rng([seed, *keys])


@dataclass
class SystemState:
    scenario: Scenario
    topo: Topology
    ts: pd.DatetimeIndex
    rate: dict[str, np.ndarray]
    self_ms: dict[str, np.ndarray]
    own_err: dict[str, np.ndarray]
    mult: dict[str, np.ndarray]
    e2e_ms: dict[str, np.ndarray]
    e2e_err: dict[str, np.ndarray]
    spikes: dict[str, np.ndarray]          # 單點毛刺發生在哪幾分鐘（索引），給驗證用

    def index(self, epoch_seconds: float) -> int:
        i = int((epoch_seconds - self.scenario.window.start.timestamp()) // 60)
        return min(max(i, 0), len(self.ts) - 1)

    def baseline_e2e_ms(self, service: str) -> float:
        """沒有故障時的期望總耗時（判斷「延遲超標」用）。"""
        return float(np.median(self.e2e_ms[service]))


def build_state(scenario: Scenario, topo: Topology) -> SystemState:
    n = scenario.window.minutes
    ts = pd.date_range(scenario.window.start, periods=n, freq="1min")
    minutes = np.arange(n, dtype=float)
    names = sorted(topo.services)
    sid = {name: k for k, name in enumerate(names)}

    self_ms, own_err, mult, spikes = {}, {}, {}, {}
    for name in names:
        svc = topo.services[name]
        r = _rng(scenario.seed, 1, sid[name])
        jitter = 1 + JITTER * r.standard_normal(n)
        base_err = np.clip(BASE_ERR_MEAN + BASE_ERR_SD * r.standard_normal(n), 0, None)
        spike_at = np.flatnonzero(r.random(n) < SPIKE_PROB)
        base_err[spike_at] = r.uniform(SPIKE_LOW, SPIKE_HIGH, len(spike_at))
        self_ms[name] = svc.self_ms * jitter
        own_err[name] = base_err
        mult[name] = np.ones(n)
        spikes[name] = spike_at

    start = scenario.window.start.timestamp()
    for inc in scenario.incidents:
        since_start = minutes - (inc.start.timestamp() - start) / 60.0
        for f in inc.faults:
            env = inc.envelope(since_start - f.lag_minutes)
            self_ms[f.entity] = self_ms[f.entity] + f.latency_shift_ms * env
            own_err[f.entity] = own_err[f.entity] + f.trace_error_rate * env
            mult[f.entity] = mult[f.entity] + (f.call_multiplier - 1) * env
    own_err = {k: np.clip(v, 0, 0.95) for k, v in own_err.items()}

    entry = scenario.base_rate_per_minute * diurnal(ts, scenario.tz)
    rate = {name: np.zeros(n) for name in names}
    rate[topo.entry] = entry
    order = topo.order()                    # 下游 -> 上游
    for name in reversed(order):            # 上游先算，請求量往下游乘
        for c in topo.service_calls(name):
            rate[c.callee] = rate[c.callee] + rate[name] * c.prob * mult[name]

    e2e_ms, e2e_err = {}, {}
    for name in order:                      # 下游先算，耗時與錯誤往上游加
        total = self_ms[name].copy()
        ok = 1 - own_err[name]
        for c in topo.services[name].calls:
            if topo.is_service(c.callee):
                total = total + c.prob * mult[name] * (c.base_ms + e2e_ms[c.callee])
                ok = ok * (1 - c.prob * e2e_err[c.callee])
            else:
                total = total + c.prob * c.base_ms
        e2e_ms[name], e2e_err[name] = total, 1 - ok
    return SystemState(scenario, topo, ts, rate, self_ms, own_err, mult, e2e_ms, e2e_err, spikes)
