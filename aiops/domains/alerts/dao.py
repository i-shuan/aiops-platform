from datetime import datetime
from typing import Any

from aiops.models import core


class IncidentDAO:
    @staticmethod
    def create(preset: str, incident: dict[str, Any], severity: str, suppressed_by: str | None) -> dict[str, Any]:
        return core.incidents.insert(
            {"preset": preset, "incident": incident, "severity": severity, "suppressed_by": suppressed_by}
        )


class SilenceDAO:
    @staticmethod
    def create(row: dict[str, Any]) -> dict[str, Any]:
        return core.silences.insert(row)

    @staticmethod
    def list_all() -> list[dict[str, Any]]:
        return core.silences.list()


class NotificationDAO:
    @staticmethod
    def record(incident_id: int, fingerprint: str, severity: str, last_event_ts: datetime, count: int) -> dict:
        return core.notifications.insert(
            {
                "incident_id": incident_id,
                "fingerprint": fingerprint,
                "severity": severity,
                "last_event_ts": last_event_ts,
                "count": count,
            }
        )

    @staticmethod
    def list_all() -> list[dict[str, Any]]:
        return core.notifications.list()
