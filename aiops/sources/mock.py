import zlib
from datetime import datetime

import numpy as np
import pandas as pd

from aiops.sources.base import BaseDataSourceSpec

SEASONAL_PREFIX = "seasonal:"
MOCK_GROUP_SIZE = 5


def _splitmix(x: np.ndarray, seed: int) -> np.ndarray:
    """splitmix64：把連續整數打散成看起來隨機的 64 位元值；回傳高 53 位元（可直接轉成 [0, 1) 的浮點）。"""
    with np.errstate(over="ignore"):
        z = (x + np.uint64(seed)) * np.uint64(0x9E3779B97F4A7C15)
        z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        z = z ^ (z >> np.uint64(31))
    return (z >> np.uint64(11)).astype(np.float64)


def _seasonal(query: str, ts: pd.DatetimeIndex) -> np.ndarray:
    """依「時間戳」決定的假資料（同一時刻不管從哪個時間窗查都是同一個值），有日週期與週末差異。
    同比、環比、168 時段基準線需要這種資料；一般 query 的假資料是依「時間窗內位置」產生的，跨窗不一致。"""
    seed = zlib.crc32(query.encode())
    # 先轉成秒：pandas 的內部單位可能是 ns 或 us（版本不同），直接用 asi8 會算錯分鐘
    epoch_min = (pd.DatetimeIndex(ts).as_unit("s").asi8 // 60).astype(np.uint64)
    # 決定性的雜訊：只看 (seed, 分鐘)，splitmix64 雜湊成均勻分布，再用 Box–Muller 轉成常態
    u1 = (_splitmix(epoch_min, seed) + 0.5) / 2.0**53
    u2 = (_splitmix(epoch_min, seed ^ 0x5DEECE66D) + 0.5) / 2.0**53
    noise = np.sqrt(-2 * np.log(u1)) * np.cos(2 * np.pi * u2)
    hour = ts.hour + ts.minute / 60
    weekend = np.isin(ts.dayofweek, (5, 6))
    return 50 + 15 * np.sin(2 * np.pi * (hour - 6) / 24) - 10 * weekend + 2 * noise


class MockSpec(BaseDataSourceSpec):
    """開發用假資料，讓流程不依賴真實監控源就能跑。同一 query 結果固定。

    - 一般 query：常態雜訊加一個尖峰（在時間窗正中央）
    - seasonal:<名稱>：依時間戳決定的日週期資料（沒有尖峰），給同比、環比、168 時段基準線用
    - fetch_group：回傳 MOCK_GROUP_SIZE 個成員（<by>-0、<by>-1…），成員 0 帶尖峰、其他沒有
    """

    source_type = "mock"

    def fetch_data(self, query: str, start: datetime, end: datetime, step_seconds: int,
                   field_map: dict[str, str] | None = None) -> pd.DataFrame:
        ts = pd.date_range(start, end, freq=f"{step_seconds}s")
        if query.startswith(SEASONAL_PREFIX):
            return pd.DataFrame({"ts": ts, "value": _seasonal(query, ts)})
        rng = np.random.default_rng(zlib.crc32(query.encode()))
        values = rng.normal(50, 2, len(ts))
        if len(values) > 10:
            values[len(values) // 2] += 40
        return pd.DataFrame({"ts": ts, "value": values})

    def fetch_group(self, query: str, start: datetime, end: datetime, step_seconds: int, by: str,
                    field_map: dict[str, str] | None = None) -> dict[str, pd.DataFrame]:
        ts = pd.date_range(start, end, freq=f"{step_seconds}s")
        members = {}
        for i in range(MOCK_GROUP_SIZE):
            rng = np.random.default_rng(zlib.crc32(f"{query}|{by}|{i}".encode()))
            values = rng.normal(50, 2, len(ts))
            if i == 0 and len(values) > 10:
                values[len(values) // 2] += 40
            members[f"{by}-{i}"] = pd.DataFrame({"ts": ts, "value": values})
        return members

    def check(self) -> dict:
        return {"ok": True, "detail": "假資料，永遠可用"}
