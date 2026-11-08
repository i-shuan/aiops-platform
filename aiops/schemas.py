"""跨領域共用的請求型別（只有純資料形狀，沒有業務邏輯）。"""
from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class MetricTarget(BaseModel):
    source_type: str = "mock"
    query: str


class TimeRange(BaseModel):
    start: datetime
    end: datetime
    step_seconds: int = Field(60, gt=0)

    @model_validator(mode="after")
    def _start_before_end(self):
        if self.start >= self.end:
            raise ValueError("start 必須早於 end")
        return self
