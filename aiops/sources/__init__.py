"""外部資料源：只管「連到外部系統、把語法轉成標準 DataFrame(ts, value)」。

不 import models、stages、control_plane；不處理平台自己合成的資料（例如 trace 時序，那是 stages/ingestion/fetch.py 的事）。
已核准的欄位對照由呼叫端（引擎載入到 ctx）傳進來，這一層自己不查資料庫。
"""
from typing import Any

from aiops.engine.errors import BadRequest, InfrastructureError
from aiops.sources.base import BaseDataSourceSpec
from aiops.sources.elasticsearch import ElasticsearchSpec
from aiops.sources.mock import MockSpec
from aiops.sources.prometheus import PrometheusSpec, VictoriaMetricsSpec

# 新增資料源 = 寫一個 Spec 並加進這裡
_SPECS: dict[str, type[BaseDataSourceSpec]] = {
    spec.source_type: spec for spec in (MockSpec, PrometheusSpec, VictoriaMetricsSpec, ElasticsearchSpec)
}


def source_types() -> list[str]:
    return sorted(_SPECS)


def is_configured(source_type: str) -> bool:
    """這個資料源有沒有設定（例如 AIOPS_ES_URL）。未知的資料源回 False。"""
    return source_type in _SPECS and _SPECS[source_type].configured()


def get_datasource_spec(source_type: str) -> BaseDataSourceSpec:
    if source_type not in _SPECS:
        raise BadRequest(f"未知的資料源: {source_type}（可用：{', '.join(sorted(_SPECS))}）")
    return _SPECS[source_type].from_env()


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
