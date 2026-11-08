from datetime import datetime

from pydantic import BaseModel, Field


class MappingSubmitRequest(BaseModel):
    source_type: str
    # 來源欄位 -> 標準欄位，例如 {"@timestamp": "ts", "cpu.pct": "value"}
    fields: dict[str, str] = Field(min_length=1)
    submitter: str


class MappingApproveRequest(BaseModel):
    approver: str


class SpanIn(BaseModel):
    trace_id: str
    service: str
    start: datetime
    duration_ms: float = Field(ge=0)
    error: bool = False


class TraceMergeRequest(BaseModel):
    spans: list[SpanIn] = Field(min_length=1)
    step_seconds: int = Field(60, gt=0)
