from typing import Any

from aiops.models import core


class IncidentDAO:
    @staticmethod
    def create(preset: str, incident: dict[str, Any]) -> dict[str, Any]:
        return core.incidents.insert({"preset": preset, "incident": incident})
