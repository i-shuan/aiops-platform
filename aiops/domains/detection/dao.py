from datetime import datetime
from typing import Any

from aiops.models import crud
from aiops.models.core import ActiveDetectorConfigRow, AnomalyRow, DetectorModelRow, utc_now


class DetectorConfigReadDAO:
    """跨領域唯讀：偵測只讀 tuning 擁有的現行參數；寫入只能經 tuning 的核准流程。"""

    @staticmethod
    def get_params(detector_name: str) -> dict[str, Any] | None:
        row = crud.find_one(ActiveDetectorConfigRow, detector_name=detector_name)
        return row["params"] if row else None


class AnomalyDAO:
    @staticmethod
    def latest_ts(pipeline: str, entity: str, metric: str) -> datetime | None:
        return crud.max_of(AnomalyRow, "ts", pipeline=pipeline, entity=entity, metric=metric)

    @staticmethod
    def create_many(rows: list[dict[str, Any]]) -> int:
        return crud.create_many(AnomalyRow, rows)


class DetectorModelDAO:
    @staticmethod
    def get(pipeline: str, entity: str, metric: str, detector_name: str) -> dict[str, Any] | None:
        return crud.find_one(DetectorModelRow, pipeline=pipeline, entity=entity, metric=metric,
                             detector_name=detector_name)

    @staticmethod
    def save(pipeline: str, entity: str, metric: str, detector_name: str, state: dict[str, Any],
             train_start: datetime, train_end: datetime, points: int,
             trained_at: datetime | None = None) -> dict[str, Any]:
        return crud.upsert(
            DetectorModelRow,
            {"pipeline": pipeline, "entity": entity, "metric": metric, "detector_name": detector_name},
            state=state, train_start=train_start, train_end=train_end, points=points,
            trained_at=trained_at or utc_now(),
        )
