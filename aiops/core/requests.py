"""API 與手動動作的請求型別（Pydantic）。API 層與 admin 算子共用，所以放在 core。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from aiops.core.pipeline import PeerSpec, TargetSpec


class TimeRangeIn(BaseModel):
    start: datetime
    end: datetime
    step_seconds: int = Field(60, gt=0)

    @model_validator(mode="after")
    def _ordered(self):
        if self.start >= self.end:
            raise ValueError("start 必須早於 end")
        return self


class MetricIn(BaseModel):
    source_type: str = "mock"
    query: str


# ── 執行 workflow ──


class RunWorkflowRequest(BaseModel):
    workflow: str
    # 不給就用 workflow 檔裡的 targets
    targets: list[TargetSpec] | None = None
    # 回放用：不給就用現在
    now: datetime | None = None


class RcaRequest(BaseModel):
    incident_id: int
    workflow: str = "rca_report"


class IdRef(BaseModel):
    id: int


class SourceRef(BaseModel):
    source_type: str


class UidRef(BaseModel):
    uid: str


# ── 靜音與事件群 ──


class SilenceCreate(BaseModel):
    kind: Literal["mute", "maintenance", "exclusion"]
    start: datetime
    end: datetime
    entity: str | None = None
    dept: str | None = None
    reason: str
    created_by: str

    @model_validator(mode="after")
    def _ordered(self):
        if self.start >= self.end:
            raise ValueError("start 必須早於 end")
        return self


# ── 欄位對照 ──


class MappingSubmit(BaseModel):
    source_type: str
    # 來源欄位 -> 標準欄位（ts／value／entity）
    fields: dict[str, str] = Field(min_length=1)
    submitter: str


class MappingApprove(BaseModel):
    mapping_id: int
    approver: str


# ── 匯入與合成 ──


class SpanIn(BaseModel):
    trace_id: str
    service: str
    start: datetime
    duration_ms: float = Field(ge=0)
    error: bool = False
    span_id: str | None = None
    parent_id: str | None = None


class TraceMerge(BaseModel):
    spans: list[SpanIn] = Field(min_length=1)
    step_seconds: int = Field(60, gt=0)
    persist: bool = True


class LotStepIn(BaseModel):
    lot_id: str
    station: str
    equipment: str
    start: datetime
    end: datetime
    recipe: str | None = None

    @model_validator(mode="after")
    def _ordered(self):
        if self.start > self.end:
            raise ValueError(f"lot {self.lot_id} 在 {self.station} 的 start 晚於 end")
        return self


class LotPathsMerge(BaseModel):
    steps: list[LotStepIn] = Field(min_length=1, max_length=50000)
    persist: bool = True


class ExternalAnomalyIn(BaseModel):
    external_id: str
    entity: str
    metric: str
    ts: datetime
    score: float = Field(ge=0)
    value: float | None = None
    attrs: dict[str, Any] = Field(default_factory=dict)


class IngestAnomalies(BaseModel):
    pipeline: str
    source: str
    anomalies: list[ExternalAnomalyIn] = Field(min_length=1, max_length=5000)


# ── 調參 ──


class ProposalCreate(BaseModel):
    source: Literal["auto", "llm", "manual"]
    detector: str  # 算子名，例如 detector.zscore
    params: dict[str, Any] = Field(default_factory=dict)
    reason: str = ""
    proposer: str


class ProposalBacktest(BaseModel):
    proposal_id: int
    metric: MetricIn
    time_range: TimeRangeIn
    peers: list[MetricIn] = Field(default_factory=list)

    def to_targets(self) -> list:
        """回測的 target（引擎載入欄位對照與 trace 時序用）。放在這裡，API 層就不必寫轉換邏輯。"""
        return [TargetSpec(source_type=self.metric.source_type, query=self.metric.query,
                           peers=[PeerSpec(source_type=p.source_type, query=p.query) for p in self.peers])]


class ProposalApprove(BaseModel):
    proposal_id: int
    approver: str


class FeedbackSubmit(BaseModel):
    incident_uid: str
    verdict: Literal["false_positive", "true_positive", "missed"]
    detector: str | None = None
    text: str = Field(min_length=1, max_length=4000)
    author: str


class FeedbackPropose(BaseModel):
    feedback_id: int
    proposer: str
    detector: str | None = None
