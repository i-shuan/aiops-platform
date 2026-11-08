from typing import Any

from aiops.models import core


class IncidentReadDAO:
    @staticmethod
    def get(incident_id: int) -> dict[str, Any] | None:
        return core.incidents.get(incident_id)
