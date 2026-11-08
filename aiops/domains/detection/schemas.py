from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class MetricTarget(BaseModel):
    source_type: str = "mock"
    query: str


class TimeRange(BaseModel):
    start: datetime
    end: datetime
    step_seconds: int = Field(60, gt=0)


class DetectionRunRequest(BaseModel):
    metric: MetricTarget
    time_range: TimeRange
    detector_name: str = "zscore"
    params: dict[str, Any] = Field(default_factory=dict)
