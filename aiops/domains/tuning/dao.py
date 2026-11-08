from typing import Any

from aiops.models import core


class ProposalDAO:
    @staticmethod
    def create(row: dict[str, Any]) -> dict[str, Any]:
        return core.tuning_proposals.insert(row)

    @staticmethod
    def get(proposal_id: int) -> dict[str, Any] | None:
        return core.tuning_proposals.get(proposal_id)

    @staticmethod
    def update(proposal_id: int, **fields: Any) -> dict[str, Any]:
        return core.tuning_proposals.update(proposal_id, **fields)


class ActiveConfigDAO:
    """偵測器現行參數。整個系統只有 ApproveTuningCommand 會呼叫 set()。"""

    @staticmethod
    def get(detector_name: str) -> dict[str, Any] | None:
        rows = core.active_detector_configs.list(detector_name=detector_name)
        return rows[0] if rows else None

    @staticmethod
    def version(detector_name: str) -> int:
        row = ActiveConfigDAO.get(detector_name)
        return row["version"] if row else 0

    @staticmethod
    def set(detector_name: str, params: dict[str, Any], source: str, proposal_id: int) -> dict[str, Any]:
        fields = {"params": params, "source": source, "proposal_id": proposal_id}
        row = ActiveConfigDAO.get(detector_name)
        if row is None:
            return core.active_detector_configs.insert({"detector_name": detector_name, "version": 1, **fields})
        return core.active_detector_configs.update(row["id"], version=row["version"] + 1, **fields)
