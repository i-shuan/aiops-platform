from collections.abc import Callable
from datetime import datetime
from typing import Any

import pandas as pd

from aiops.commands.base import CommandError
from aiops.datasource_specs.base import BaseDataSourceSpec, InvalidQueryError, NoDataError
from aiops.datasource_specs.elasticsearch import ElasticsearchSpec
from aiops.datasource_specs.mock import MockSpec
from aiops.datasource_specs.prometheus import PrometheusSpec, VictoriaMetricsSpec
from aiops.infrastructure.errors import InfrastructureError
from aiops.metric_query.trace import parse_trace_query
from aiops.metric_query.window import align_window, to_utc

# 新增資料源 = 寫一個 Spec 並加進這裡
_SPECS: dict[str, type[BaseDataSourceSpec]] = {
    spec.source_type: spec for spec in (MockSpec, PrometheusSpec, VictoriaMetricsSpec, ElasticsearchSpec)
}
# 平台內部來源：資料是平台自己合成並存起來的（例如 trace 合成出的每服務時序），由領域傳入讀取函式
TRACE_SOURCE = "trace"

# 型別：讀已核准欄位對照的函式（source_type -> 對照或 None）、讀 trace 時序的函式（服務, 起, 訖 -> 列）
FieldMapLoader = Callable[[str], dict[str, str] | None]
TraceRowsLoader = Callable[[str, datetime, datetime], list[dict[str, Any]]]


def get_datasource_spec(source_type: str) -> BaseDataSourceSpec:
    if source_type not in _SPECS:
        raise CommandError(f"未知的資料源: {source_type}（可用：{', '.join(sorted([*_SPECS, TRACE_SOURCE]))}）")
    return _SPECS[source_type].from_env()


def frame_from_rows(rows: list[dict[str, Any]], value_key: str, start: datetime, end: datetime,
                    step_seconds: int) -> pd.DataFrame:
    """平台自己存的時序列 -> DataFrame(ts, value)，時間對齊到 step、只取窗內。"""
    s, e = align_window(start, end, step_seconds)
    points = sorted((to_utc(r["ts"]), float(r[value_key])) for r in rows if s <= to_utc(r["ts"]) <= e)
    if not points:
        raise NoDataError(f"{TRACE_SOURCE} 在這段時間沒有資料")
    return pd.DataFrame({"ts": pd.to_datetime([t for t, _ in points], utc=True), "value": [v for _, v in points]})


def _fetch_one(source_type: str, query: str, start: datetime, end: datetime, step_seconds: int,
               field_map_for: FieldMapLoader | None, trace_rows: TraceRowsLoader | None) -> pd.DataFrame:
    if source_type == TRACE_SOURCE:
        try:
            service, field = parse_trace_query(query)
        except ValueError as e:
            raise InvalidQueryError(str(e)) from e
        if trace_rows is None:
            raise CommandError("這個流程不支援 trace 來源")
        return frame_from_rows(trace_rows(service, start, end), field, start, end, step_seconds)
    field_map = field_map_for(source_type) if field_map_for else None
    return get_datasource_spec(source_type).fetch_data(query, start, end, step_seconds, field_map)


def fetch_frame(source_type: str, query: str, start: datetime, end: datetime, step_seconds: int,
                peers: list[tuple[str, str]] | None = None, *, field_map_for: FieldMapLoader | None = None,
                trace_rows: TraceRowsLoader | None = None) -> pd.DataFrame:
    """抓主序列；有 peers（[(source_type, query), ...]）就各抓一條，依時間對齊成 peer_0、peer_1… 欄位。

    field_map_for、trace_rows 由領域用自己的 *ReadDAO 傳入：資料源層不 import models、不查資料庫。
    """
    df = _fetch_one(source_type, query, start, end, step_seconds, field_map_for, trace_rows)
    for i, (peer_source, peer_query) in enumerate(peers or []):
        peer = _fetch_one(peer_source, peer_query, start, end, step_seconds, field_map_for, trace_rows)
        df = df.merge(peer.rename(columns={"value": f"peer_{i}"}), on="ts", how="left")
    return df


def check_sources() -> list[dict[str, Any]]:
    """逐一檢查每個資料源：有沒有設定、連不連得上。沒設定的不算失敗，只標示出來。"""
    results = []
    for source_type, spec_cls in sorted(_SPECS.items()):
        if not spec_cls.configured():
            results.append({"source_type": source_type, "configured": False, "ok": None, "detail": "未設定"})
            continue
        try:
            outcome = spec_cls.from_env().check()
        except InfrastructureError as e:
            outcome = {"ok": False, "detail": str(e)}
        results.append({"source_type": source_type, "configured": True, **outcome})
    return results
