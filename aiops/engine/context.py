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

from aiops.engine.types import AnomalyEvent, Incident, Notification


@dataclass
class PeerRef:
    source_type: str
    query: str


@dataclass
class PeerGroup:
    """動態同儕：一個查詢撈回整群序列，依 by 標籤分成各成員；自己以外的成員就是同儕。

    query 可以用 ${屬性名} 引用 target 的 attrs（例如 cpu{service="${service}"}），同一份 YAML 套到不同服務。
    自己是哪個成員：attrs[by]，沒有就用 entity。
    """

    query: str
    by: str
    source_type: str = ""  # 空 = 跟 target 同一個資料源


@dataclass
class Target:
    """要偵測的一條序列。同儕給 peer 偵測器用：peers 是寫死的名單，peer_group 是依標籤動態聚合。

    detectors：這條序列要跑哪些偵測節點（節點名稱或算子名稱；None = 全部）。引擎依它決定跳過哪些節點。
    overrides：節點名稱 -> 參數，蓋過節點 YAML 的 params（最優先）。
    """

    source_type: str
    query: str
    entity: str
    attrs: dict[str, Any] = field(default_factory=dict)
    peers: list[PeerRef] = field(default_factory=list)
    peer_group: PeerGroup | None = None
    detectors: list[str] | None = None
    overrides: dict[str, dict[str, Any]] = field(default_factory=dict)


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
    # 非 JSON 的模型本體（例如數值陣列）。只允許安全格式（npz，不允許 pickle），沒有就是 None
    artifact: bytes | None = None
    artifact_format: str = ""


@dataclass
class RootCandidate:
    """RCA 根因候選：由 rca.root_cause.* 算子產生，報告段落只負責呈現。"""

    entity: str
    score: float
    reason: str


@dataclass
class KnowledgeCase:
    """歷史事故經驗庫的一筆（確認為真異常、且有寫處置的事件群）。"""

    incident_uid: str
    pipeline: str
    entities: list[str]
    metrics: list[str]
    signals: list[str]
    resolution: str
    root_cause: str
    created_at: datetime


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
    severity: str = ""  # 上次送出的等級（warning 或 P0～P3）：warning 之後升級成異常要照發，不被冷卻擋掉


@dataclass
class TrainingExclusion:
    """訓練基準線時要排除的時段（歷史事故、維護窗）。不排除的話，上週當機那幾小時會把基準線拉高，
    之後同一個時段再出事就被吃掉。"""

    start: datetime
    end: datetime
    reason: str


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
class SpanRecord:
    """RCA 現抓的一個 span（或 transaction）。只留還原呼叫關係與佐證需要的欄位。"""

    trace_id: str
    span_id: str
    parent_id: str | None
    service: str
    ts: datetime
    duration_ms: float
    error: bool = False


@dataclass
class LogRecord:
    """RCA 現抓的一筆錯誤日誌（只留比對樣板需要的欄位）。"""

    ts: datetime
    service: str
    message: str
    error_type: str = ""
    trace_id: str = ""
    # 抽樣權重：日誌超過上限時依時間分桶抽樣，一筆代表同時段的幾筆（= 該時段總筆數 ÷ 抽到的筆數）；沒抽樣就是 1
    weight: float = 1.0


@dataclass
class OpenIncident:
    """已通知、還在進行中的事件群（吸收新異常、升級、心跳與結案用）。

    一場事故從頭到尾只有這一個 uid：後續的異常由 alert.absorb_open 併進 incident，嚴重度由 alert.notify_payload
    只升不降地更新。previous 是這一輪第一次被改之前的 (事件群, 嚴重度)，心跳拿它重建上次送出的標籤：
    標籤變了（嚴重度、等級、實體）就先對舊標籤送 endsAt 關掉，再送新標籤，Alertmanager 上不會兩張同時掛著。
    """

    id: int
    uid: str
    preset: str
    severity: str
    incident: Incident
    changed: bool = False  # 這一輪被更新過：持久化時寫回資料庫
    previous: tuple[Incident, str] | None = None

    def remember(self) -> None:
        """第一次改之前留一份快照（同一輪改兩次也只留最早那份＝上次真的送出去的）。"""
        if self.previous is None:
            self.previous = (Incident.from_dict(self.incident.to_dict(), uid=self.uid), self.severity)
        self.changed = True


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
    # 這條序列訓練基準線時要排除的時段（這個實體的歷史事故、維護窗；被標成誤報的事故不排除）
    training_exclusions: list[TrainingExclusion] = field(default_factory=list)
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
    knowledge_cases: list[KnowledgeCase] = field(default_factory=list)  # RCA：可比對的歷史事故

    # ── 流動的資料 ──
    # 欄位 ts、value；有同儕時加 peer_0、peer_1…；fetch 設了 lags_minutes 時加 lag_<分鐘>（同一時刻往前 N 分鐘的值）
    raw_df: pd.DataFrame | None = None
    train_df: pd.DataFrame | None = None
    peer_members: list[str] = field(default_factory=list)  # 動態同儕：peer_0、peer_1… 依序是哪些成員
    detector_runs: list[str] = field(default_factory=list)  # 這條序列跑過的偵測節點（裁決算子用來知道誰沒投票）
    anomaly_result: AnomalyResult | None = None
    new_detector_states: dict[str, DetectorState] = field(default_factory=dict)  # 節點名稱 -> 這次新訓練的，最後才存
    anomalies: list[AnomalyEvent] = field(default_factory=list)
    incidents: list[Incident] = field(default_factory=list)
    incidents_built: bool = False  # ensure_incidents 建過了：之後清單變空也不再從 anomalies 重建
    incident_ids: dict[str, int] = field(default_factory=dict)  # 事件群 fingerprint -> 已存的 id（持久化後填）
    rca_root_candidates: list[RootCandidate] = field(default_factory=list)  # 由 rca.root_cause.* 排序好
    rca_root_algorithm: str = ""
    # 根因演算法的結論說明，例如「證據不足」：沒給候選時報告要講清楚為什麼，不是讓段落默默消失
    rca_root_note: str = ""
    # incident_traces 有設定 trace 來源、但這一輪連不上或讀失敗（例如 ES 重啟）。跟「這個場景本來就沒有 trace」（工廠）要分開：
    # 前者證據被吃掉，根因算子不該拿拓撲硬猜；後者退回拓撲推測是正常做法
    rca_traces_failed: bool = False
    # RCA 重抓的事件時段原始時序：(實體, 指標) -> DataFrame(ts, value)。給互相關等需要原始資料的演算法
    rca_series: dict[tuple[str, str], pd.DataFrame] = field(default_factory=dict)
    # RCA 隨需現抓的事件前後 span（ingestion.incident_traces），給 rca.build_dynamic_topology 還原當下的呼叫關係
    rca_spans: list[SpanRecord] = field(default_factory=list)
    # RCA 隨需現抓的錯誤日誌（ingestion.incident_logs）：事件期間與事件前的正常期間，給 rca.section.log_patterns 比對
    rca_logs: list[LogRecord] = field(default_factory=list)
    rca_logs_baseline: list[LogRecord] = field(default_factory=list)
    rca_logs_sampled: bool = False  # 日誌超過撈取上限，是抽樣加權的估計值（rca.section.log_patterns 要在報告裡寫明）
    rca_log_window_minutes: tuple[float, float] = (0.0, 0.0)  # (事件期間, 正常期間) 的長度，算每分鐘頻率用
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
    incident_pipeline: str = ""  # 那個事件群是哪條管線產生的（RCA 的 ctx.pipeline 是報告流程自己的名字）
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
                       root_cause={"algorithm": self.rca_root_algorithm, "note": self.rca_root_note,
                                   "candidates": [{"entity": c.entity, "score": round(c.score, 4), "reason": c.reason}
                                                  for c in self.rca_root_candidates]},
                       narrative={"summary": self.llm_summary, "claims": self.llm_claims,
                                  "generated_by": self.llm_generated_by, "rejected": self.llm_rejected})
        if self.errors:
            out["errors"] = [{"entity": e.entity, "message": e.message} for e in self.errors]
        return out
