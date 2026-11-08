"""所有資料表（ORM）。節點之間不經過資料庫：這些表只在 Pipeline 前面載入輸入、後面寫入最終結果，以及手動動作（admin 算子）管理自己的狀態。

時間一律存成「UTC、不帶時區」（crud 寫入時自動轉換）；讀出來的 naive datetime 就是 UTC。
資料表結構目前由 create_all() 建立；上正式環境前要改用 Alembic 做 migration。
"""
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, DateTime, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from aiops.storage.db import Base, create_all, drop_all


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utc_naive(value: Any) -> Any:
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def new_incident_uid() -> str:
    """給外部（通知、Alertmanager 分組）用的全域唯一識別。整數 id 會因資料庫重建從 1 重來，不能拿去外面用。"""
    return f"inc-{uuid.uuid4().hex}"


def row_to_dict(row: Any) -> dict[str, Any]:
    return {attr.key: getattr(row, attr.key) for attr in row.__mapper__.column_attrs}


# ── alerts ──


class IncidentRow(Base):
    __tablename__ = "incidents"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # 對外識別（通知 label incident_uid）；id 只在平台內部用（RCA 查詢、通知紀錄）
    uid: Mapped[str] = mapped_column(String(40), unique=True, default=new_incident_uid)
    preset: Mapped[str] = mapped_column(String(64))
    incident: Mapped[dict] = mapped_column(JSON)
    severity: Mapped[str] = mapped_column(String(8))
    suppressed_by: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    # 來自哪條排程管線（直接呼叫 /alerts/process 的為 None，不會自動心跳與結案）
    pipeline: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    # open：已通知、還在發生；resolved：已結案（送過 endsAt）；suppressed：被抑制、沒通知
    status: Mapped[str] = mapped_column(String(16), default="open", index=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # 建立事件群當下用的拓撲／Lot 流向（只留跟這件事有關的邊）：RCA 優先用它，報告才跟當時的收斂結果一致、可重現
    context: Mapped[dict] = mapped_column(JSON, default=dict)


class SilenceRow(Base):
    __tablename__ = "silences"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    kind: Mapped[str] = mapped_column(String(16))
    start: Mapped[datetime] = mapped_column(DateTime)
    end: Mapped[datetime] = mapped_column(DateTime)
    entity: Mapped[str | None] = mapped_column(String(256), nullable=True)
    dept: Mapped[str | None] = mapped_column(String(128), nullable=True)
    reason: Mapped[str] = mapped_column(String(1024))
    created_by: Mapped[str] = mapped_column(String(128))


class NotificationRow(Base):
    __tablename__ = "notifications"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[int] = mapped_column(Integer)
    fingerprint: Mapped[str] = mapped_column(String(1024), index=True)
    severity: Mapped[str] = mapped_column(String(8))
    last_event_ts: Mapped[datetime] = mapped_column(DateTime)
    count: Mapped[int] = mapped_column(Integer)


# ── detection ──


class AnomalyRow(Base):
    """偵測結果（只增不改）。由 persist_target 寫入。"""

    __tablename__ = "anomalies"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline: Mapped[str] = mapped_column(String(128), index=True)
    entity: Mapped[str] = mapped_column(String(256))
    metric: Mapped[str] = mapped_column(String(512))
    ts: Mapped[datetime] = mapped_column(DateTime)
    value: Mapped[float] = mapped_column(Float)
    score: Mapped[float] = mapped_column(Float)
    detector_name: Mapped[str] = mapped_column(String(64))
    attrs: Mapped[dict] = mapped_column(JSON, default=dict)


class DetectorModelRow(Base):
    """批次訓練後保存的偵測器狀態（基準線參數）。一條管線 × 實體 × 指標 × 偵測器一筆。"""

    __tablename__ = "detector_models"
    __table_args__ = (UniqueConstraint("pipeline", "entity", "metric", "detector_name"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline: Mapped[str] = mapped_column(String(128))
    entity: Mapped[str] = mapped_column(String(256))
    metric: Mapped[str] = mapped_column(String(512))
    detector_name: Mapped[str] = mapped_column(String(64))
    state: Mapped[dict] = mapped_column(JSON)
    train_start: Mapped[datetime] = mapped_column(DateTime)
    train_end: Mapped[datetime] = mapped_column(DateTime)
    points: Mapped[int] = mapped_column(Integer)
    trained_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    # 樂觀鎖：寫入時必須帶讀取當時的版本，版本已被別人推進就不覆蓋
    version: Mapped[int] = mapped_column(Integer, default=1, server_default="1")


# ── ingestion ──


class FieldMappingRow(Base):
    __tablename__ = "field_mappings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_type: Mapped[str] = mapped_column(String(64), index=True)
    fields: Mapped[dict] = mapped_column(JSON)
    submitter: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(16))
    approved_by: Mapped[str | None] = mapped_column(String(128), nullable=True)


class ExternalAnomalyRow(Base):
    """外部系統（例如工廠 FDC）推來的異常。只增不改；同一來源的 external_id 只收一次。

    由 load_batch 取出，跟偵測產生的 anomalies 一起進告警管線。
    """

    __tablename__ = "external_anomalies"
    __table_args__ = (UniqueConstraint("source", "external_id"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline: Mapped[str] = mapped_column(String(128), index=True)
    source: Mapped[str] = mapped_column(String(64))
    external_id: Mapped[str] = mapped_column(String(256))
    entity: Mapped[str] = mapped_column(String(256))
    metric: Mapped[str] = mapped_column(String(512))
    ts: Mapped[datetime] = mapped_column(DateTime)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    score: Mapped[float] = mapped_column(Float)
    attrs: Mapped[dict] = mapped_column(JSON, default=dict)
    received_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    # pending：還沒進過告警流程；processed：已被某一輪 Pipeline 處理
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)


class TraceSeriesRow(Base):
    """trace 合成出的每服務時序（MergeTracesCommand 寫入）。偵測用 source_type=trace 讀，RCA 的 trace_evidence 自動讀。

    同一個服務、同一個時間格只有一筆；同一格再合成一次會被最新一批覆蓋。
    """

    __tablename__ = "trace_series"
    __table_args__ = (UniqueConstraint("service", "ts"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    service: Mapped[str] = mapped_column(String(256), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime)
    count: Mapped[int] = mapped_column(Integer)
    error_rate: Mapped[float] = mapped_column(Float)
    p95_ms: Mapped[float] = mapped_column(Float)


class ServiceEdgeRow(Base):
    """從 trace 的父子 span 推導出的服務呼叫關係（caller 呼叫 callee）。alerts、rca 自動拿來當 topology。"""

    __tablename__ = "service_edges"
    __table_args__ = (UniqueConstraint("caller", "callee"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    caller: Mapped[str] = mapped_column(String(256))
    callee: Mapped[str] = mapped_column(String(256))
    calls: Mapped[int] = mapped_column(Integer, default=0)
    last_seen: Mapped[datetime] = mapped_column(DateTime)


class LotPathRow(Base):
    """每個 Lot 依時間排好的機台路徑（MergeLotPathsCommand 寫入）。alerts、rca 從這裡推導機台間流向（lot_edges）。"""

    __tablename__ = "lot_paths"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lot_id: Mapped[str] = mapped_column(String(128), unique=True)
    path: Mapped[list] = mapped_column(JSON)
    start: Mapped[datetime] = mapped_column(DateTime)
    end: Mapped[datetime] = mapped_column(DateTime)


# ── tuning ──


class TuningProposalRow(Base):
    __tablename__ = "tuning_proposals"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(16))
    detector_name: Mapped[str] = mapped_column(String(64))
    params: Mapped[dict] = mapped_column(JSON)
    reason: Mapped[str] = mapped_column(String(2048), default="")
    proposer: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(16))
    base_version: Mapped[int] = mapped_column(Integer)
    backtest: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    approved_by: Mapped[str | None] = mapped_column(String(128), nullable=True)


class FeedbackRow(Base):
    """維運人員對事件群的回饋（誤報、確認、漏報）。可以轉成調參提案，但只能經提案 -> 回測 -> 核准生效。"""

    __tablename__ = "feedback"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_uid: Mapped[str] = mapped_column(String(40), index=True)
    verdict: Mapped[str] = mapped_column(String(16))
    detector_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    text: Mapped[str] = mapped_column(String(4000))
    author: Mapped[str] = mapped_column(String(128))
    proposal_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class ActiveDetectorConfigRow(Base):
    __tablename__ = "active_detector_configs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    detector_name: Mapped[str] = mapped_column(String(64), unique=True)
    version: Mapped[int] = mapped_column(Integer)
    params: Mapped[dict] = mapped_column(JSON)
    source: Mapped[str] = mapped_column(String(16))
    proposal_id: Mapped[int] = mapped_column(Integer)


def reset_all() -> None:
    """測試用：清空並重建所有資料表。"""
    drop_all()
    create_all()
