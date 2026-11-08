from typing import Any

from aiops.models import crud
from aiops.models.core import IncidentRow


class IncidentReadDAO:
    """跨領域唯讀：incidents 屬於 alerts。"""

    @staticmethod
    def get(incident_id: int) -> dict[str, Any] | None:
        return crud.get(IncidentRow, incident_id)
