"""PipelineContext：貫穿整條 Pipeline 的強型別資料載體。

節點之間只透過它傳遞資料，不透過資料庫、也不傳鬆散的 dict。
引擎在跑之前把「要先存起來、之後被讀進流程」的輸入（靜音、冷卻紀錄、欄位對照、偵測基準線…）載入這裡；
算子只讀 ctx，自己不碰 DB。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd
from pydantic import BaseModel

from aiops.core.types import AnomalyEvent, Incident, Notification


@dataclass
class PeerRef:
    source_type: str
    query: str


@dataclass
class Target:
    """要偵測的一條序列。同儕（peers）給 peer 偵測器用。"""

    source_type: str
    query: str
    entity: str
    attrs: dict[str, Any] = field(default_factory=dict)
    peers: list[PeerRef] = field(default_factory=list)


@dataclass
class AnomalyResult:
    algorithm: str
    timestamps: list[str]
    values: list[float]
    scores: list[float]
    is_anomaly: list[bool]

    @property
    def latest_is_anomaly(self) -> bool:
        return bool(self.is_anomaly[-1]) if self.is_anomaly else False

    @property
    def count(self) -> int:
        return int(sum(self.is_anomaly))

    def to_dict(self) -> dict[str, Any]:
        return {"algorithm": self.algorithm, "timestamps": self.timestamps, "values": self.values,
                "scores": self.scores, "is_anomaly": self.is_anomaly, "anomaly_count": self.count}


@dataclass
class DetectorState:
    """批次訓練保存的基準線。"""

    state: dict[str, Any]
    trained_at: datetime
    train_start: datetime | None = None
    train_end: datetime | None = None
    points: int = 0
    version: int = 0  # 讀取當時的資料庫版本（0 = 資料庫還沒有這一筆）；寫回時用來偵測別人是否已經更新
    detector: str = ""  # 算子名（例如 detector.zscore）。ctx 裡的 key 是節點名稱，寫回時兩個都要


@dataclass
class Silence:
    id: int
    kind: str
    start: datetime
    end: datetime
    entity: str | None = None
    dept: str | None = None


@dataclass
class RecentNotification:
    fingerprint: str
    last_event_ts: datetime
    incident_uid: str = ""


@dataclass
class LotEdge:
    upstream: str
    downstream: str
    lots: int


@dataclass
class TraceRow:
    service: str
    ts: datetime
    count: int
    error_rate: float
    p95_ms: float


@dataclass
class OpenIncident:
    """已通知、還在進行中的事件群（心跳與結案用）。"""

    id: int
    uid: str
    preset: str
    severity: str
    incident: Incident


@dataclass
class ReportSection:
    key: str
    title: str
    body: str

    def to_dict(self) -> dict[str, str]:
        return {"key": self.key, "title": self.title, "body": self.body}


@dataclass
class TargetError:
    entity: str
    message: str


@dataclass
class PipelineContext:
    pipeline: str
    now: datetime
    target: Target | None = None  # None = 批次層（所有 target 的結果合併後）

    # ── 載入的輸入（引擎在跑之前載入；算子只讀）──
    field_maps: dict[str, dict[str, str]] = field(default_factory=dict)  # source_type -> 已核准對照（來源欄 -> 標準欄）
    trace_rows: dict[str, list[TraceRow]] = field(default_factory=dict)  # 服務 -> trace 合成時序
    # 節點名稱 -> tuning 核准的現行參數（已依 管線×實體 > 管線 > 實體 > 全域 挑出最具體的一筆）
    detector_params: dict[str, dict[str, Any]] = field(default_factory=dict)
    detector_states: dict[str, DetectorState] = field(default_factory=dict)  # 節點名稱 -> 基準線
    last_anomaly_ts: datetime | None = None  # 這條序列最後一筆已存的異常時間（只處理比它新的）
    silences: list[Silence] = field(default_factory=list)
    recent_notifications: list[RecentNotification] = field(default_factory=list)
    topology: list[tuple[str, str]] = field(default_factory=list)  # (caller, callee)
    lot_edges: list[LotEdge] = field(default_factory=list)
    # Workflow 明確給了（含給空清單）就不要再從 trace／Lot 履歷自動推導
    topology_explicit: bool = False
    lot_edges_explicit: bool = False
    external_anomaly_ids: list[int] = field(default_factory=list)  # 這一輪處理掉的外部匯入異常
    open_incidents: list[OpenIncident] = field(default_factory=list)
    entity_last_seen: dict[str, datetime] = field(default_factory=dict)  # 實體 -> 最近一次異常

    # ── 流動的資料 ──
    raw_df: pd.DataFrame | None = None  # 欄位 ts、value（有同儕時再加 peer_0、peer_1…）
    train_df: pd.DataFrame | None = None
    anomaly_result: AnomalyResult | None = None
    new_detector_states: dict[str, DetectorState] = field(default_factory=dict)  # 節點名稱 -> 這次新訓練的，最後才存
    anomalies: list[AnomalyEvent] = field(default_factory=list)
    incidents: list[Incident] = field(default_factory=list)
    incident_ids: dict[str, int] = field(default_factory=dict)  # 事件群 fingerprint -> 已存的 id（持久化後填）
    rca_sections: list[ReportSection] = field(default_factory=list)
    rca_skipped: list[str] = field(default_factory=list)
    llm_summary: str | None = None
    llm_claims: list[dict[str, Any]] = field(default_factory=list)
    llm_generated_by: str = "template"
    llm_rejected: str | None = None
    notifications: list[Notification] = field(default_factory=list)
    resolved_incident_ids: list[int] = field(default_factory=list)
    errors: list[TargetError] = field(default_factory=list)
    audit: list[str] = field(default_factory=list)

    # ── 手動動作（API）：請求與結果 ──
    incident_id: int | None = None  # RCA 等針對單一事件群的流程
    request: BaseModel | None = None
    result: dict[str, Any] = field(default_factory=dict)

    @property
    def is_suppressed(self) -> bool:
        return any(i.suppressed_by for i in self.incidents)

    def log(self, message: str) -> None:
        self.audit.append(message)

    def to_result(self) -> dict[str, Any]:
        """API 回傳用：這一輪的結果摘要。手動動作的結果（result）原樣帶出。"""
        out: dict[str, Any] = {"pipeline": self.pipeline, **self.result}
        if self.incidents:
            out["incidents"] = [
                {"id": self.incident_ids.get(i.fingerprint), "uid": i.uid, "severity": i.severity,
                 "suppressed_by": i.suppressed_by, **i.to_dict()}
                for i in self.incidents
            ]
        if self.anomalies or self.notifications or self.incidents:
            out.update(anomalies=len(self.anomalies), notifications=len(self.notifications),
                       resolved_incidents=len(self.resolved_incident_ids))
        if self.rca_sections or self.rca_skipped:
            out.update(sections=[s.to_dict() for s in self.rca_sections], skipped=self.rca_skipped,
                       narrative={"summary": self.llm_summary, "claims": self.llm_claims,
                                  "generated_by": self.llm_generated_by, "rejected": self.llm_rejected})
        if self.errors:
            out["errors"] = [{"entity": e.entity, "message": e.message} for e in self.errors]
        return out
