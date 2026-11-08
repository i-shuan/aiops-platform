from typing import Any

from aiops.models import crud
from aiops.models.core import ExternalAnomalyRow, FieldMappingRow


class MappingDAO:
    @staticmethod
    def create(row: dict[str, Any]) -> dict[str, Any]:
        return crud.create(FieldMappingRow, **row)

    @staticmethod
    def get(mapping_id: int) -> dict[str, Any] | None:
        return crud.get(FieldMappingRow, mapping_id)

    @staticmethod
    def update(mapping_id: int, **fields: Any) -> dict[str, Any]:
        return crud.update(FieldMappingRow, mapping_id, **fields)

    @staticmethod
    def list_by(source_type: str, status: str) -> list[dict[str, Any]]:
        return crud.find(FieldMappingRow, source_type=source_type, status=status)


class ExternalAnomalyDAO:
    @staticmethod
    def existing_ids(source: str, external_ids: list[str]) -> set[str]:
        rows = crud.find_in(ExternalAnomalyRow, "external_id", external_ids)
        return {r["external_id"] for r in rows if r["source"] == source}

    @staticmethod
    def create_many(rows: list[dict[str, Any]]) -> int:
        return crud.create_many(ExternalAnomalyRow, rows)
