from typing import Any

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
