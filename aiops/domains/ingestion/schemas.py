from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator


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
    # 有父子關係才推得出服務呼叫拓撲（父 span 的服務呼叫了這個 span 的服務）
    span_id: str | None = None
    parent_id: str | None = None


class TraceMergeRequest(BaseModel):
    spans: list[SpanIn] = Field(min_length=1)
    step_seconds: int = Field(60, gt=0)
    # True：合成結果存起來（偵測的 trace 來源、RCA 的 trace 佐證、自動拓撲都讀這裡）
    persist: bool = True


# ── 工廠 Lot 履歷 ──


class LotStepIn(BaseModel):
    lot_id: str
    station: str
    equipment: str
    start: datetime
    end: datetime
    recipe: str | None = None

    @model_validator(mode="after")
    def _start_not_after_end(self):
        if self.start > self.end:
            raise ValueError(f"lot {self.lot_id} 在 {self.station} 的 start 晚於 end")
        return self


class LotPathsMergeRequest(BaseModel):
    steps: list[LotStepIn] = Field(min_length=1, max_length=50000)
    # True：路徑存起來（alerts 的 lot_path_chain、rca 的根因推論會自動讀）
    persist: bool = True


# ── 外部異常接收 ──


class ExternalAnomalyIn(BaseModel):
    # 來源系統自己的 id；同一 source 重送會被當成重複
    external_id: str
    entity: str
    metric: str
    ts: datetime
    score: float = Field(ge=0)
    value: float | None = None
    # 慣例鍵：dept（通知路由用）
    attrs: dict[str, Any] = Field(default_factory=dict)


class IngestAnomaliesRequest(BaseModel):
    # 要進哪一條告警管線（跟排程 job 名稱相同才會被那個 job 處理）
    pipeline: str
    # 來源系統名稱，例如 fab-fdc
    source: str
    anomalies: list[ExternalAnomalyIn] = Field(min_length=1, max_length=5000)
