from aiops.commands.base import CommandError
from aiops.datasource_specs.base import BaseDataSourceSpec
from aiops.datasource_specs.mock import MockSpec

# 新增資料源 = 寫一個 Spec 並加進這裡（之後 prometheus / victoriametrics / elasticsearch 同理）
_SPECS: dict[str, type[BaseDataSourceSpec]] = {
    MockSpec.source_type: MockSpec,
}


def get_datasource_spec(source_type: str) -> BaseDataSourceSpec:
    if source_type not in _SPECS:
        raise CommandError(f"未知的資料源: {source_type}")
    return _SPECS[source_type]()
