from typing import Any

from aiops.models import crud
from aiops.models.core import (
    ExternalAnomalyRow,
    FieldMappingRow,
    LotPathRow,
    ServiceEdgeRow,
    TraceSeriesRow,
    utc_now,
)


class TraceSeriesDAO:
    @staticmethod
    def upsert_many(rows: list[dict[str, Any]]) -> int:
        """同一服務同一時間格只留一筆，再合成一次就用最新一批覆蓋。"""
        for r in rows:
            crud.upsert(TraceSeriesRow, {"service": r["service"], "ts": r["ts"]},
                        count=r["count"], error_rate=r["error_rate"], p95_ms=r["p95_ms"])
        return len(rows)


class ServiceEdgeDAO:
    @staticmethod
    def add_calls(edges: dict[tuple[str, str], int]) -> int:
        for (caller, callee), calls in edges.items():
            existing = crud.find_one(ServiceEdgeRow, caller=caller, callee=callee)
            crud.upsert(ServiceEdgeRow, {"caller": caller, "callee": callee},
                        calls=(existing["calls"] if existing else 0) + calls, last_seen=utc_now())
        return len(edges)


class LotPathDAO:
    @staticmethod
    def upsert_many(lots: list[dict[str, Any]]) -> int:
        """以 lot_id 為準：同一個 Lot 再合成一次就換成最新的路徑（不會重複計算流向）。"""
        for lot in lots:
            crud.upsert(LotPathRow, {"lot_id": lot["lot_id"]}, path=lot["path"], start=lot["start"], end=lot["end"])
        return len(lots)


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
