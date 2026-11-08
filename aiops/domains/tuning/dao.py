from typing import Any

from aiops.models import crud
from aiops.models.core import ActiveDetectorConfigRow, FeedbackRow, IncidentRow, TuningProposalRow


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
