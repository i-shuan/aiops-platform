"""API 與手動動作的請求型別（Pydantic）。API 層與控制面（admin）算子共用，所以放在 engine。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from aiops.engine.pipeline import PeerGroupSpec, PeerSpec, TargetSpec


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


# API 只能用內建 workflow 的名稱（不能給路徑）；runner.workflow_path 會再檢查一次
WorkflowName = Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=128)


class RunWorkflowRequest(BaseModel):
    workflow: str = WorkflowName
    # 不給就用 workflow 檔裡的 targets
    targets: list[TargetSpec] | None = None
    # 回放用：不給就用現在
    now: datetime | None = None


class RcaRequest(BaseModel):
    incident_id: int
    workflow: str = Field("rca_report", pattern=r"^[A-Za-z0-9_-]+$", max_length=128)


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
    # 這個提案改哪一層："*" = 不限定。只給 pipeline = 這條管線的所有實體；兩個都給 = 只改這條管線的這個實體
    pipeline: str = Field("*", min_length=1, max_length=128)
    entity: str = Field("*", min_length=1, max_length=256)


class ProposalBacktest(BaseModel):
    proposal_id: int
    metric: MetricIn
    time_range: TimeRangeIn
    peers: list[MetricIn] = Field(default_factory=list)
    # 動態同儕（跟 workflow target 的 peer_group 一樣）；query 裡的 ${屬性} 用 attrs 代入，自己是 attrs[by] 或 entity
    peer_group: PeerGroupSpec | None = None
    entity: str | None = None
    attrs: dict[str, Any] = Field(default_factory=dict)
    # 前面這個比例拿來訓練、後面拿來測試（只算測試段的異常），避免「用同一段資料訓練又評分」高估效果
    train_ratio: float = Field(0.7, ge=0.1, le=0.9)

    def to_targets(self) -> list:
        """回測的 target（引擎載入欄位對照與 trace 時序用）。放在這裡，API 層就不必寫轉換邏輯。"""
        return [TargetSpec(source_type=self.metric.source_type, query=self.metric.query, entity=self.entity,
                           attrs=dict(self.attrs), peer_group=self.peer_group,
                           peers=[PeerSpec(source_type=p.source_type, query=p.query) for p in self.peers])]


class ProposalApprove(BaseModel):
    proposal_id: int
    approver: str


class FeedbackSubmit(BaseModel):
    """兩種回饋：
    - 針對某個事件群：給 incident_uid（誤報、確認、或「有告警但太晚／太輕」的漏報）
    - 真正的漏報（平台根本沒告警）：不給 incident_uid，改給 pipeline + entity + time_range（事情實際發生的時段）

    確認是真異常（true_positive）時可以順便寫 resolution（當時怎麼處理）與 root_cause，
    這件事就會進歷史事故經驗庫，之後 RCA 遇到類似事故會列出來。
    """

    incident_uid: str | None = None
    verdict: Literal["false_positive", "true_positive", "missed"]
    detector: str | None = None
    text: str = Field(min_length=1, max_length=4000)
    author: str
    pipeline: str | None = Field(None, pattern=r"^[A-Za-z0-9_-]+$", max_length=128)
    entity: str | None = Field(None, min_length=1, max_length=256)
    time_range: TimeRangeIn | None = None
    resolution: str | None = Field(None, min_length=1, max_length=4000)
    root_cause: str | None = Field(None, max_length=1000)

    @model_validator(mode="after")
    def _shape(self):
        if self.incident_uid is None:
            if self.verdict != "missed":
                raise ValueError("沒有 incident_uid 的回饋只能是 missed（真正的漏報）")
            if not (self.pipeline and self.entity and self.time_range):
                raise ValueError("漏報補錄要給 pipeline、entity 與 time_range（事情發生的時段）")
        if self.resolution is not None and self.verdict != "true_positive":
            raise ValueError("resolution（處置）只能寫在確認為真異常（true_positive）的回饋上")
        if self.root_cause and self.resolution is None:
            raise ValueError("寫了 root_cause 也要寫 resolution，經驗庫才有處置可以參考")
        return self


class FeedbackPropose(BaseModel):
    feedback_id: int
    proposer: str
    detector: str | None = None
    # 提案要改哪一層（管線取自事件群本身）：
    # entity = 只改這條管線的這個實體（事件群只有一個實體時可省略 entity）；pipeline = 這條管線全部；global = 全平台
    scope: Literal["entity", "pipeline", "global"] = "pipeline"
    entity: str | None = None


class TuningMetricsQuery(BaseModel):
    """調參成效報表：每個核准過的調參，在核准前後各 window_days 天的事件群與誤報數。只看最近 lookback_days 天內核准的。"""

    window_days: int = Field(7, ge=1, le=90)
    lookback_days: int = Field(90, ge=1, le=365)


class RulePromote(BaseModel):
    """規則升級：同一層（偵測器 × 管線 × 實體）在 window_days 天內被核准調參 >= min_approvals 次，
    就用它目前的基準線產生一個 detector.rules 靜態規則的提案（仍要回測、核准才生效）。"""

    min_approvals: int = Field(3, ge=2, le=50)
    window_days: int = Field(30, ge=1, le=365)
    consecutive: int = Field(3, ge=1, le=60)
    proposer: str = "rule_promoter"
