from datetime import datetime
from typing import Any

import pandas as pd

from aiops.commands.base import CommandError
from aiops.datasource_specs.base import BaseDataSourceSpec
from aiops.datasource_specs.elasticsearch import ElasticsearchSpec
from aiops.datasource_specs.mock import MockSpec
from aiops.datasource_specs.prometheus import PrometheusSpec, VictoriaMetricsSpec
from aiops.infrastructure.errors import InfrastructureError

# 新增資料源 = 寫一個 Spec 並加進這裡
_SPECS: dict[str, type[BaseDataSourceSpec]] = {
    spec.source_type: spec for spec in (MockSpec, PrometheusSpec, VictoriaMetricsSpec, ElasticsearchSpec)
}


def get_datasource_spec(source_type: str) -> BaseDataSourceSpec:
    if source_type not in _SPECS:
        raise CommandError(f"未知的資料源: {source_type}（可用：{', '.join(sorted(_SPECS))}）")
    return _SPECS[source_type].from_env()


def fetch_frame(source_type: str, query: str, start: datetime, end: datetime, step_seconds: int,
                peers: list[tuple[str, str]] | None = None) -> pd.DataFrame:
    """抓主序列；有 peers（[(source_type, query), ...]）就各抓一條，依時間對齊成 peer_0、peer_1… 欄位。"""
    df = get_datasource_spec(source_type).fetch_data(query, start, end, step_seconds)
    for i, (peer_source, peer_query) in enumerate(peers or []):
        peer = get_datasource_spec(peer_source).fetch_data(peer_query, start, end, step_seconds)
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
