from typing import Any

from aiops.models import core


class MappingDAO:
    @staticmethod
    def create(row: dict[str, Any]) -> dict[str, Any]:
        return core.field_mappings.insert(row)

    @staticmethod
    def get(mapping_id: int) -> dict[str, Any] | None:
        return core.field_mappings.get(mapping_id)

    @staticmethod
    def update(mapping_id: int, **fields: Any) -> dict[str, Any]:
        return core.field_mappings.update(mapping_id, **fields)

    @staticmethod
    def list_by(source_type: str, status: str) -> list[dict[str, Any]]:
        return core.field_mappings.list(source_type=source_type, status=status)
