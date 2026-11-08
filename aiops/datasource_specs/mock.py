from datetime import datetime

import numpy as np
import pandas as pd

from aiops.datasource_specs.base import BaseDataSourceSpec


class MockSpec(BaseDataSourceSpec):
    """開發用：產生含幾個尖峰的假時序，讓流程不依賴真實監控源就能跑。"""

    source_type = "mock"

    def fetch_data(self, query: str, start: datetime, end: datetime, step_seconds: int) -> pd.DataFrame:
        ts = pd.date_range(start, end, freq=f"{step_seconds}s")
        rng = np.random.default_rng(abs(hash(query)) % 2**32)
        values = rng.normal(50, 2, len(ts))
        if len(values) > 10:
            values[len(values) // 2] += 40
        return pd.DataFrame({"ts": ts, "value": values})
