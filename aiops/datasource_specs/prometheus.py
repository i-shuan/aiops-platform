import math
import os
from datetime import datetime
from typing import Any

import httpx
import pandas as pd

from aiops.datasource_specs import _http
from aiops.datasource_specs.base import BaseDataSourceSpec, InvalidQueryError, NoDataError
from aiops.metric_query.promql import validate_promql
from aiops.metric_query.window import align_window


class PrometheusSpec(BaseDataSourceSpec):
    """Prometheus HTTP API：GET /api/v1/query_range。查詢必須只回一條序列（多條請先 sum／avg by 聚合）。

    環境變數：AIOPS_PROMETHEUS_URL（必填）、AIOPS_PROMETHEUS_TOKEN（選填，Bearer）。
    """

    source_type = "prometheus"
    env_prefix = "AIOPS_PROMETHEUS"
    health_path = "/-/ready"

    def __init__(self, base_url: str, token: str | None = None, client: httpx.Client | None = None,
                 timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.client = client or httpx.Client(timeout=timeout)

    @classmethod
    def from_env(cls) -> "PrometheusSpec":
        return cls(_http.env_required(f"{cls.env_prefix}_URL"), os.environ.get(f"{cls.env_prefix}_TOKEN") or None)

    @classmethod
    def configured(cls) -> bool:
        return bool(os.environ.get(f"{cls.env_prefix}_URL", "").strip())

    def _query_range(self, query: str, start: datetime, end: datetime, step_seconds: int) -> tuple[str, list[dict]]:
        try:
            q = validate_promql(query)
        except ValueError as e:
            raise InvalidQueryError(str(e)) from e
        s, e = align_window(start, end, step_seconds)
        resp = _http.request(
            self.client, self.source_type, "GET", f"{self.base_url}/api/v1/query_range", headers=self.headers,
            params={"query": q, "start": s.timestamp(), "end": e.timestamp(), "step": step_seconds},
        )
        payload = resp.json() if resp.content else {}
        if not resp.is_success or payload.get("status") != "success":
            raise InvalidQueryError(f"{self.source_type} 查詢失敗：{payload.get('error') or resp.text[:200]}")
        result = payload.get("data", {}).get("result", [])
        if not result:
            raise NoDataError(f"{self.source_type} 在這段時間沒有資料：{q}")
        return q, result

    @staticmethod
    def _frame(series: dict) -> pd.DataFrame | None:
        points = [(float(t), float(v)) for t, v in series.get("values", [])]
        points = [(t, v) for t, v in points if math.isfinite(v)]
        if not points:
            return None
        return pd.DataFrame({
            "ts": pd.to_datetime([t for t, _ in points], unit="s", utc=True),
            "value": [v for _, v in points],
        })

    def fetch_data(self, query: str, start: datetime, end: datetime, step_seconds: int,
                   field_map: dict[str, str] | None = None) -> pd.DataFrame:
        # PromQL 本身就指定好序列與標籤，不需要欄位對照；收下但不使用
        q, result = self._query_range(query, start, end, step_seconds)
        if len(result) > 1:
            raise InvalidQueryError(f"查詢回傳 {len(result)} 條序列，請用 sum／avg by 聚合成一條：{q}")
        df = self._frame(result[0])
        if df is None:
            raise NoDataError(f"{self.source_type} 在這段時間只有 NaN／Inf：{q}")
        return df

    def fetch_group(self, query: str, start: datetime, end: datetime, step_seconds: int, by: str,
                    field_map: dict[str, str] | None = None) -> dict[str, pd.DataFrame]:
        """一個查詢撈回整群（例如同 service 的所有 pod），依 by 標籤分成員。全是 NaN 的成員略過。"""
        q, result = self._query_range(query, start, end, step_seconds)
        members: dict[str, pd.DataFrame] = {}
        for series in result:
            labels = series.get("metric", {})
            if by not in labels:
                raise InvalidQueryError(f"有序列沒有 {by} 標籤，無法分組：{labels}（查詢：{q}）")
            if labels[by] in members:
                raise InvalidQueryError(f"{by}={labels[by]} 對到多條序列，請先 sum／avg by ({by}) 聚合：{q}")
            df = self._frame(series)
            if df is not None:
                members[labels[by]] = df
        if not members:
            raise NoDataError(f"{self.source_type} 在這段時間只有 NaN／Inf：{q}")
        return members

    def check(self) -> dict[str, Any]:
        return _http.check(self.client, self.source_type, f"{self.base_url}{self.health_path}", headers=self.headers)


class VictoriaMetricsSpec(PrometheusSpec):
    """VictoriaMetrics 相容 Prometheus 查詢 API。環境變數：AIOPS_VICTORIAMETRICS_URL、AIOPS_VICTORIAMETRICS_TOKEN。"""

    source_type = "victoriametrics"
    env_prefix = "AIOPS_VICTORIAMETRICS"
    health_path = "/health"
