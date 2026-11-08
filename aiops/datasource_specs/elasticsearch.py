import os
from datetime import datetime
from typing import Any

import httpx
import pandas as pd

from aiops.datasource_specs import _http
from aiops.datasource_specs.base import BaseDataSourceSpec, InvalidQueryError, NoDataError
from aiops.metric_query.elasticsearch import bucket_value, build_search_body, parse_es_query
from aiops.metric_query.window import align_window


class ElasticsearchSpec(BaseDataSourceSpec):
    """Elasticsearch：date_histogram 降採樣成時序。query 是 JSON（格式見 metric_query/elasticsearch.py）。

    環境變數：AIOPS_ES_URL（必填）、AIOPS_ES_API_KEY（選填，送 Authorization: ApiKey <key>）。
    只讀：只呼叫 _search 與根路徑，不寫任何索引。
    """

    source_type = "elasticsearch"

    def __init__(self, base_url: str, api_key: str | None = None, client: httpx.Client | None = None,
                 timeout: float = 15.0):
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"ApiKey {api_key}"} if api_key else {}
        self.client = client or httpx.Client(timeout=timeout)

    @classmethod
    def from_env(cls) -> "ElasticsearchSpec":
        return cls(_http.env_required("AIOPS_ES_URL"), os.environ.get("AIOPS_ES_API_KEY") or None)

    @classmethod
    def configured(cls) -> bool:
        return bool(os.environ.get("AIOPS_ES_URL", "").strip())

    def fetch_data(self, query: str, start: datetime, end: datetime, step_seconds: int) -> pd.DataFrame:
        try:
            q = parse_es_query(query)
        except ValueError as e:
            raise InvalidQueryError(str(e)) from e
        s, e = align_window(start, end, step_seconds)
        resp = _http.request(
            self.client, self.source_type, "POST", f"{self.base_url}/{q.index}/_search", headers=self.headers,
            json=build_search_body(q, s, e, step_seconds),
        )
        payload = resp.json() if resp.content else {}
        if not resp.is_success:
            reason = (payload.get("error") or {}).get("reason") if isinstance(payload.get("error"), dict) else None
            raise InvalidQueryError(f"elasticsearch 查詢失敗（HTTP {resp.status_code}）：{reason or resp.text[:200]}")

        buckets = (payload.get("aggregations") or {}).get("series", {}).get("buckets", [])
        points = [(b["key"], bucket_value(q, b)) for b in buckets]
        points = [(t, v) for t, v in points if v is not None]
        if not points:
            raise NoDataError(f"elasticsearch 在這段時間沒有資料：index={q.index}")
        return pd.DataFrame({
            "ts": pd.to_datetime([t for t, _ in points], unit="ms", utc=True),
            "value": [v for _, v in points],
        })

    def check(self) -> dict[str, Any]:
        return _http.check(self.client, self.source_type, f"{self.base_url}/", headers=self.headers)
