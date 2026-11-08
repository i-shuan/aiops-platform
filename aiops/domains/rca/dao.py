from datetime import datetime
from typing import Any

from aiops.models import crud
from aiops.models.core import IncidentRow, LotPathRow, ServiceEdgeRow, TraceSeriesRow


class IncidentReadDAO:
    """跨領域唯讀：incidents 屬於 alerts。"""

    @staticmethod
    def get(incident_id: int) -> dict[str, Any] | None:
        return crud.get(IncidentRow, incident_id)


class ServiceEdgeReadDAO:
    """跨領域唯讀：從 trace 推導出的服務呼叫關係（屬於 ingestion）。"""

    @staticmethod
    def all() -> list[dict[str, Any]]:
        return crud.find(ServiceEdgeRow)


class LotPathReadDAO:
    """跨領域唯讀：已合成的 Lot 路徑（屬於 ingestion）。"""

    @staticmethod
    def paths() -> list[list[str]]:
        return [r["path"] for r in crud.find(LotPathRow)]


class TraceSeriesReadDAO:
    """跨領域唯讀：trace 合成出的每服務時序（屬於 ingestion），給 trace 佐證段落用。"""

    @staticmethod
    def for_services(services: list[str], start: datetime, end: datetime) -> list[dict[str, Any]]:
        return crud.find_between(TraceSeriesRow, "ts", start, end, in_column="service", in_values=services)
