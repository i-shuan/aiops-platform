import zlib
from datetime import datetime

import numpy as np
import pandas as pd

from aiops.datasource_specs.base import BaseDataSourceSpec


class MockSpec(BaseDataSourceSpec):
    """開發用：產生含一個尖峰的假時序，讓流程不依賴真實監控源就能跑。同一 query 結果固定。"""

    source_type = "mock"

    def fetch_data(self, query: str, start: datetime, end: datetime, step_seconds: int,
                   field_map: dict[str, str] | None = None) -> pd.DataFrame:
        ts = pd.date_range(start, end, freq=f"{step_seconds}s")
        rng = np.random.default_rng(zlib.crc32(query.encode()))
        values = rng.normal(50, 2, len(ts))
        if len(values) > 10:
            values[len(values) // 2] += 40
        return pd.DataFrame({"ts": ts, "value": values})

    def check(self) -> dict:
        return {"ok": True, "detail": "假資料，永遠可用"}
