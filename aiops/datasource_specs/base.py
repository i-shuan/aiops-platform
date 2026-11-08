from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd


class BaseDataSourceSpec(ABC):
    """仿 db_engine_specs：各資料源方言，統一輸出 DataFrame(ts, value)。"""

    source_type: str = ""

    @abstractmethod
    def fetch_data(self, query: str, start: datetime, end: datetime, step_seconds: int) -> pd.DataFrame: ...
