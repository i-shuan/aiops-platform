"""時間窗與 PromQL 的共用工具：純函式，不連線、不讀設定（依賴方向跟 grouping 同一層，誰都可以用）。

  Window       一張事故的時間窗：事故期間 [start, end]，事故前的基線 [start - baseline, start)。日誌的事故前次數比較、
               service graph 的延遲基線、要看哪一段 service graph，全部用同一個定義
  increase／quantile／mean   組 PromQL：照 (client, server, …) 分組的增量、直方圖的第幾百分位、平均（總和 ÷ 次數）
  string       系統名稱放進 PromQL 字串之前的跳脫
  round4、ratio              給 LLM 看的數字：四位有效數字；倍數（基線是 0 或沒有就不算）

只做算術，沒有任何「算不算異常」的門檻。
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta

MAX_RANGE_MINUTES = 1440   # 查詢的區間最長一天：事故拖很久時不要一次掃太多資料


@dataclass(frozen=True)
class Window:
    start: datetime             # 事故期間的起點（事故開始前再往前留一段緩衝：規則有 for 的等待時間，錯誤常比告警早）
    end: datetime
    baseline_minutes: int = 0   # 事故前看多久當基線；0 = 不需要基線

    @classmethod
    def of_incident(cls, started_at: datetime, last_seen_at: datetime, now: datetime, *, padding_minutes: int,
                    baseline_minutes: int = 0) -> "Window":
        return cls(started_at - timedelta(minutes=padding_minutes), max(last_seen_at, now), baseline_minutes)

    @property
    def baseline_start(self) -> datetime:
        return self.start - timedelta(minutes=self.baseline_minutes)

    @property
    def minutes(self) -> int:
        """事故期間換成查詢用的分鐘數：無條件進位、至少 1、最多 MAX_RANGE_MINUTES。"""
        return range_minutes(self.start, self.end)


def range_minutes(start: datetime, end: datetime) -> int:
    return min(MAX_RANGE_MINUTES, max(1, math.ceil((end - start).total_seconds() / 60)))


def increase(metric: str, by: str, minutes: int) -> str:
    return f"sum by ({by}) (increase({metric}[{minutes}m]))"


def quantile(q: float, histogram: str, by: str, minutes: int) -> str:
    """histogram 是直方圖的名稱（不含 _bucket）。結果是在桶裡線性內插出來的：桶寬粗時不準。"""
    return f"histogram_quantile({q}, sum by ({by}, le) (increase({histogram}_bucket[{minutes}m])))"


def mean(histogram: str, by: str, minutes: int) -> str:
    """平均：總和的增量 ÷ 次數的增量。不受直方圖桶寬的影響。"""
    return f"{increase(histogram + '_sum', by, minutes)} / {increase(histogram + '_count', by, minutes)}"


def string(value: str) -> str:
    """把系統名稱放進 PromQL 雙引號字串之前的跳脫。系統名稱來自告警的標籤（外部輸入），不跳脫的話名稱裡的引號
    可以把查詢截斷、接上別的條件。"""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def round4(value: float) -> float:
    return float(f"{value:.4g}")   # 四位有效數字：給 LLM 看走勢夠用，不會被一長串小數淹沒


def ratio(value: float | None, baseline: float | None) -> float | None:
    """事故期間 ÷ 基線，四位有效數字。用原始值算（不要拿四捨五入過的值相除）。基線是 0 或沒有就不算。"""
    return round4(value / baseline) if value is not None and baseline else None
