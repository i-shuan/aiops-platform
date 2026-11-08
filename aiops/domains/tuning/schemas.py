from typing import Any, Literal

from pydantic import BaseModel, Field

from aiops.schemas import MetricTarget, TimeRange


class ProposalCreateRequest(BaseModel):
    # auto：自動調優；llm：從回饋文字抽出；manual：人工調整
    source: Literal["auto", "llm", "manual"]
    detector_name: str
    params: dict[str, Any] = Field(default_factory=dict)
    reason: str = ""
    proposer: str


class BacktestRequest(BaseModel):
    metric: MetricTarget
    time_range: TimeRange


class ApproveRequest(BaseModel):
    approver: str
