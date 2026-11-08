from datetime import datetime
from typing import Any

from aiops.models import crud
from aiops.models.core import (
    AlertWatermarkRow,
    AnomalyRow,
    ExternalAnomalyRow,
    IncidentRow,
    NotificationRow,
    SilenceRow,
)


class IncidentDAO:
    @staticmethod
    def create(preset: str, incident: dict[str, Any], severity: str, suppressed_by: str | None,
               pipeline: str | None = None) -> dict[str, Any]:
        return crud.create(IncidentRow, preset=preset, incident=incident, severity=severity,
                           suppressed_by=suppressed_by, pipeline=pipeline,
                           status="suppressed" if suppressed_by else "open")

    @staticmethod
    def list_open(pipeline: str) -> list[dict[str, Any]]:
        return crud.find(IncidentRow, pipeline=pipeline, status="open")

    @staticmethod
    def get_by_uid(uid: str) -> dict[str, Any] | None:
        return crud.find_one(IncidentRow, uid=uid)

    @staticmethod
    def mark_resolved(incident_id: int, resolved_at: datetime) -> dict[str, Any]:
        return crud.update(IncidentRow, incident_id, status="resolved", resolved_at=resolved_at)


class SilenceDAO:
    @staticmethod
    def create(fields: dict[str, Any]) -> dict[str, Any]:
        return crud.create(SilenceRow, **fields)

    @staticmethod
    def list_all() -> list[dict[str, Any]]:
        return crud.find(SilenceRow)


class NotificationDAO:
    @staticmethod
    def record(incident_id: int, fingerprint: str, severity: str, last_event_ts: datetime, count: int) -> dict:
        return crud.create(
            NotificationRow,
            incident_id=incident_id,
            fingerprint=fingerprint,
            severity=severity,
            last_event_ts=last_event_ts,
            count=count,
        )

    @staticmethod
    def list_for(fingerprints: list[str]) -> list[dict[str, Any]]:
        return crud.find_in(NotificationRow, "fingerprint", fingerprints)


class AlertWatermarkDAO:
    @staticmethod
    def get(consumer: str) -> int:
        row = crud.get(AlertWatermarkRow, consumer)
        return row["last_anomaly_id"] if row else 0

    @staticmethod
    def set(consumer: str, last_anomaly_id: int) -> dict[str, Any]:
        return crud.upsert(AlertWatermarkRow, {"consumer": consumer}, last_anomaly_id=last_anomaly_id)


class AnomalyReadDAO:
    """跨領域唯讀：anomalies 屬於 detection。alerts 只依 id 往後讀，讀到哪記在自己的 alert_watermarks。"""

    @staticmethod
    def list_after(pipeline: str, last_id: int, limit: int) -> list[dict[str, Any]]:
        return crud.find(AnomalyRow, after_id=last_id, limit=limit, pipeline=pipeline)

    @staticmethod
    def latest_ts(pipeline: str, entity: str) -> datetime | None:
        return crud.max_of(AnomalyRow, "ts", pipeline=pipeline, entity=entity)


class ExternalAnomalyReadDAO:
    """跨領域唯讀：external_anomalies 屬於 ingestion（外部系統推來的異常）。游標獨立記在 alert_watermarks。"""

    @staticmethod
    def list_after(pipeline: str, last_id: int, limit: int) -> list[dict[str, Any]]:
        return crud.find(ExternalAnomalyRow, after_id=last_id, limit=limit, pipeline=pipeline)

    @staticmethod
    def latest_ts(pipeline: str, entity: str) -> datetime | None:
        return crud.max_of(ExternalAnomalyRow, "ts", pipeline=pipeline, entity=entity)
