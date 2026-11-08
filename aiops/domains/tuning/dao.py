from typing import Any

from aiops.models import crud
from datetime import datetime

from aiops.models.core import (
    ActiveDetectorConfigRow,
    FeedbackRow,
    FieldMappingRow,
    IncidentRow,
    TraceSeriesRow,
    TuningProposalRow,
)


class FieldMappingReadDAO:
    """跨領域唯讀：回測抓資料時用已核准的欄位對照（屬於 ingestion）。"""

    @staticmethod
    def approved_fields(source_type: str) -> dict[str, str] | None:
        row = crud.find_one(FieldMappingRow, source_type=source_type, status="approved")
        return row["fields"] if row else None


class TraceSeriesReadDAO:
    """跨領域唯讀：回測 trace 來源時用（屬於 ingestion）。"""

    @staticmethod
    def window(service: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        return crud.find_between(TraceSeriesRow, "ts", start, end, in_column="service", in_values=[service])


class FeedbackDAO:
    @staticmethod
    def create(row: dict[str, Any]) -> dict[str, Any]:
        return crud.create(FeedbackRow, **row)

    @staticmethod
    def get(feedback_id: int) -> dict[str, Any] | None:
        return crud.get(FeedbackRow, feedback_id)

    @staticmethod
    def link_proposal(feedback_id: int, proposal_id: int) -> dict[str, Any]:
        return crud.update(FeedbackRow, feedback_id, proposal_id=proposal_id)


class IncidentReadDAO:
    """跨領域唯讀：incidents 屬於 alerts。回饋要指到真的事件群。"""

    @staticmethod
    def get_by_uid(uid: str) -> dict[str, Any] | None:
        return crud.find_one(IncidentRow, uid=uid)


class ProposalDAO:
    @staticmethod
    def create(row: dict[str, Any]) -> dict[str, Any]:
        return crud.create(TuningProposalRow, **row)

    @staticmethod
    def get(proposal_id: int) -> dict[str, Any] | None:
        return crud.get(TuningProposalRow, proposal_id)

    @staticmethod
    def update(proposal_id: int, **fields: Any) -> dict[str, Any]:
        return crud.update(TuningProposalRow, proposal_id, **fields)


class ActiveConfigDAO:
    """偵測器現行參數。整個系統只有 ApproveTuningCommand 會呼叫 set()。"""

    @staticmethod
    def get(detector_name: str) -> dict[str, Any] | None:
        return crud.find_one(ActiveDetectorConfigRow, detector_name=detector_name)

    @staticmethod
    def version(detector_name: str) -> int:
        row = ActiveConfigDAO.get(detector_name)
        return row["version"] if row else 0

    @staticmethod
    def set(detector_name: str, params: dict[str, Any], source: str, proposal_id: int) -> dict[str, Any]:
        return crud.upsert(
            ActiveDetectorConfigRow,
            {"detector_name": detector_name},
            version=ActiveConfigDAO.version(detector_name) + 1,
            params=params,
            source=source,
            proposal_id=proposal_id,
        )
