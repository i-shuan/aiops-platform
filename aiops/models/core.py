"""所有資料表（ORM）。每張表都要在 TABLE_OWNERS 登記擁有者領域；只有擁有者能寫。

時間一律存成「UTC、不帶時區」（crud 寫入時自動轉換）；讀出來的 naive datetime 就是 UTC。
資料表結構目前由 create_all() 建立；上正式環境前要改用 Alembic 做 migration。
"""
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, DateTime, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from aiops.models.db import Base, create_all, drop_all


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


class AlertWatermarkRow(Base):
    """alerts 讀 anomalies 讀到哪裡（每條管線一個游標）。alerts 不能改 detection 的表，所以進度記在自己這裡。"""

    __tablename__ = "alert_watermarks"
    consumer: Mapped[str] = mapped_column(String(256), primary_key=True)
    last_anomaly_id: Mapped[int] = mapped_column(Integer, default=0)


# ── detection ──


class AnomalyRow(Base):
    """偵測結果（只增不改）。alerts 透過 AnomalyReadDAO 依 id 往後讀。"""

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

    alerts 透過 ExternalAnomalyReadDAO 依 id 往後讀，跟偵測產生的 anomalies 一起進告警管線。
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


class ActiveDetectorConfigRow(Base):
    __tablename__ = "active_detector_configs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    detector_name: Mapped[str] = mapped_column(String(64), unique=True)
    version: Mapped[int] = mapped_column(Integer)
    params: Mapped[dict] = mapped_column(JSON)
    source: Mapped[str] = mapped_column(String(16))
    proposal_id: Mapped[int] = mapped_column(Integer)


# 每張表只有擁有者領域能寫；其他領域只能透過自己 dao.py 裡的 *ReadDAO 唯讀（tests/test_architecture.py 檢查）
TABLE_OWNERS = {
    "incidents": "alerts",
    "silences": "alerts",
    "notifications": "alerts",
    "alert_watermarks": "alerts",
    "anomalies": "detection",
    "detector_models": "detection",
    "field_mappings": "ingestion",
    "external_anomalies": "ingestion",
    "tuning_proposals": "tuning",
    "active_detector_configs": "tuning",
}


def reset_all() -> None:
    """測試用：清空並重建所有資料表。"""
    drop_all()
    create_all()
