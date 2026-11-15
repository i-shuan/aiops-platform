"""所有資料表（ORM）。節點之間不經過資料庫：這些表只在 Pipeline 前面載入輸入、後面寫入最終結果，以及手動動作（admin 算子）管理自己的狀態。

時間一律存成「UTC、不帶時區」（crud 寫入時自動轉換）；讀出來的 naive datetime 就是 UTC。
資料表結構目前由 create_all() 建立；上正式環境前要改用 Alembic 做 migration。
"""
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, DateTime, Float, Index, Integer, LargeBinary, String, UniqueConstraint
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
    # 最近一次 RCA 的結論等級（engine/rca_types.ConclusionType）與產生時間；'' / NULL = 還沒跑過 RCA（含加欄位之前的舊列）。
    # 每次重跑 RCA 覆蓋。migrations/postgres/0010
    rca_conclusion_type: Mapped[str] = mapped_column(String(32), default="", server_default="")
    rca_concluded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


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
    """偵測結果（只增不改）。由 hooks.persist_batch 跟事件群在同一筆交易寫入（不是 persist_target）。
    detector.ensemble 的裁決事件只存裁決節點（detector_node = 裁決節點名稱），投票者 voted_by 只在事件群快照裡。

    唯一鍵：一個偵測節點對一條序列的一個時間點只有一筆。同一輪被重跑時 Anomalies.create_many 撞到就跳過（留先寫的），
    不會寫出兩份。既有的 PostgreSQL 靠 migrations/postgres/0008 補這個鍵。"""

    __tablename__ = "anomalies"
    __table_args__ = (UniqueConstraint("pipeline", "entity", "metric", "ts", "detector_node", name="uq_anomalies_key"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline: Mapped[str] = mapped_column(String(128), index=True)
    entity: Mapped[str] = mapped_column(String(256))
    metric: Mapped[str] = mapped_column(String(512))
    ts: Mapped[datetime] = mapped_column(DateTime)
    value: Mapped[float] = mapped_column(Float)
    score: Mapped[float] = mapped_column(Float)
    detector_name: Mapped[str] = mapped_column(String(64))  # 算子名，例如 detector.zscore（外部匯入的異常不經這裡）
    detector_node: Mapped[str] = mapped_column(String(128), default="", server_default="")  # Workflow 節點名稱
    # anomaly（越過門檻）或 warning（接近門檻的預警）
    level: Mapped[str] = mapped_column(String(16), default="anomaly", server_default="anomaly")
    attrs: Mapped[dict] = mapped_column(JSON, default=dict)


class DetectorModelRow(Base):
    """批次訓練後保存的偵測器狀態（基準線參數）。一條管線 × 實體 × 指標 × 偵測器 × 節點一筆。
    同一個演算法在同一條管線掛兩次（例如短窗、長窗）各自有自己的基準線。"""

    __tablename__ = "detector_models"
    __table_args__ = (UniqueConstraint("pipeline", "entity", "metric", "detector_name", "node",
                                       name="uq_detector_models_key"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline: Mapped[str] = mapped_column(String(128))
    entity: Mapped[str] = mapped_column(String(256))
    metric: Mapped[str] = mapped_column(String(512))
    detector_name: Mapped[str] = mapped_column(String(64))
    node: Mapped[str] = mapped_column(String(128), default="", server_default="")
    state: Mapped[dict] = mapped_column(JSON)
    train_start: Mapped[datetime] = mapped_column(DateTime)
    train_end: Mapped[datetime] = mapped_column(DateTime)
    points: Mapped[int] = mapped_column(Integer)
    trained_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    # 樂觀鎖：寫入時必須帶讀取當時的版本，版本已被別人推進就不覆蓋
    version: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    # 非 JSON 的模型本體（選用）。格式只允許安全的 npz（載入時禁止 pickle）；空字串 = 沒有
    artifact_format: Mapped[str] = mapped_column(String(16), default="", server_default="")
    artifact: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)


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
    """從 trace 的父子 span 推導出的服務呼叫關係（caller 呼叫 callee）。alerts、rca 自動拿來當 topology。

    寫入者：admin.topology_refresh（排程從 trace 來源沉澱）與 admin.merge_traces（批次匯入），都走 ServiceEdges.add_calls。
    last_seen：最近一次被刷新／匯入看到的時間（牆鐘，不是 trace 本身的時間）；NULL = 欄位加上之前就存在的舊列，
    不知道多久沒出現，prune 不刪它（migrations/postgres/0006）。"""

    __tablename__ = "service_edges"
    __table_args__ = (UniqueConstraint("environment", "caller", "callee", name="uq_service_edges_env_pair"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # 環境範圍（Workflow.environment；'' = 不分環境）。唯一鍵含它：正式與測試的同名服務各有自己的邊（migrations/postgres/0009）
    environment: Mapped[str] = mapped_column(String(64), default="", server_default="")
    caller: Mapped[str] = mapped_column(String(256))
    callee: Mapped[str] = mapped_column(String(256))
    calls: Mapped[int] = mapped_column(Integer, default=0)
    last_seen: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)


class ServiceAsyncEdgeRow(Base):
    """從 trace 推導出的非同步邊（producer 發訊息、consumer 消費）與最近一次看到的延遲 P95。偵測的 alert.messaging_link 拿來
    把「生產者先出事、消費者晚一點才出事」的兩群併成一件事。

    為什麼不放進 service_edges：那張表的 (caller, callee) 會被當成同步呼叫、傳播方向是 callee -> caller；
    非同步邊的方向剛好相反（生產者出事 -> 消費者晚點出事），放同一張表舊版程式會把它讀成同步拓撲。
    寫入者：admin.topology_refresh（AsyncEdges.record）。messages 累加；lag_ms_p95 是最近一次刷新那個窗的值（覆蓋，不平均）。
    migrations/postgres/0007。"""

    __tablename__ = "service_async_edges"
    __table_args__ = (UniqueConstraint("environment", "producer", "consumer", name="uq_service_async_edges_env_pair"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # 環境範圍（Workflow.environment；'' = 不分環境）。唯一鍵含它：正式與測試的同名服務各有自己的邊（migrations/postgres/0009）
    environment: Mapped[str] = mapped_column(String(64), default="", server_default="")
    producer: Mapped[str] = mapped_column(String(256))
    consumer: Mapped[str] = mapped_column(String(256))
    messages: Mapped[int] = mapped_column(Integer, default=0)
    lag_ms_p95: Mapped[float] = mapped_column(Float, default=0.0)
    last_seen: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)


class LotPathRow(Base):
    """每個 Lot 依時間排好的機台路徑（MergeLotPathsCommand 寫入）。alerts、rca 從這裡推導機台間流向（lot_edges）。"""

    __tablename__ = "lot_paths"
    __table_args__ = (UniqueConstraint("environment", "lot_id", name="uq_lot_paths_env_lot"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # 環境範圍（Workflow.environment；'' = 不分環境）。唯一鍵含它：正式與測試的同名服務各有自己的邊（migrations/postgres/0009）
    environment: Mapped[str] = mapped_column(String(64), default="", server_default="")
    lot_id: Mapped[str] = mapped_column(String(128))
    path: Mapped[list] = mapped_column(JSON)
    start: Mapped[datetime] = mapped_column(DateTime)
    end: Mapped[datetime] = mapped_column(DateTime)


# ── tuning ──


class TuningProposalRow(Base):
    __tablename__ = "tuning_proposals"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(16))
    detector_name: Mapped[str] = mapped_column(String(64))
    # 提案要改哪一層的設定（"*" = 不限定）。核准時寫進同一層
    pipeline: Mapped[str] = mapped_column(String(128), default="*", server_default="*")
    entity: Mapped[str] = mapped_column(String(256), default="*", server_default="*")
    params: Mapped[dict] = mapped_column(JSON)
    reason: Mapped[str] = mapped_column(String(2048), default="")
    proposer: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(16))
    base_version: Mapped[int] = mapped_column(Integer)
    backtest: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    approved_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)  # 調參成效報表用來切前後
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class FeedbackRow(Base):
    """維運人員的回饋（誤報、確認、漏報）。可以轉成調參提案，但只能經提案 -> 回測 -> 核准生效。

    兩種：針對某個事件群（incident_uid）；或真正的漏報——平台根本沒產生事件群，
    這時 incident_uid 為空，改記 pipeline + entity + 發生時段（time_start、time_end）。
    """

    __tablename__ = "feedback"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_uid: Mapped[str | None] = mapped_column(String(40), index=True, nullable=True)
    pipeline: Mapped[str | None] = mapped_column(String(128), nullable=True)
    entity: Mapped[str | None] = mapped_column(String(256), nullable=True)
    time_start: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    time_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    verdict: Mapped[str] = mapped_column(String(16))
    detector_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    text: Mapped[str] = mapped_column(String(4000))
    author: Mapped[str] = mapped_column(String(128))
    proposal_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class ActiveDetectorConfigRow(Base):
    """偵測器現行參數，分層：(偵測器, 管線, 實體)，"*" = 不限定。
    三個獨立欄位組成唯一鍵（不組字串），實體名稱裡有冒號也不會歧義。
    套用時取最具體的一層：管線×實體 > 管線 > 實體 > 全域（engine/params.pick_most_specific）。"""

    __tablename__ = "active_detector_configs"
    __table_args__ = (UniqueConstraint("detector_name", "pipeline", "entity", name="uq_active_detector_configs_scope"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    detector_name: Mapped[str] = mapped_column(String(64))
    pipeline: Mapped[str] = mapped_column(String(128), default="*", server_default="*")
    entity: Mapped[str] = mapped_column(String(256), default="*", server_default="*")
    version: Mapped[int] = mapped_column(Integer)
    params: Mapped[dict] = mapped_column(JSON)
    source: Mapped[str] = mapped_column(String(16))
    proposal_id: Mapped[int] = mapped_column(Integer)


class IncidentKnowledgeRow(Base):
    """歷史事故經驗庫：維運確認過的事件群（true_positive 一律寫入，處置可以之後再補）。兩種人在用：

    - RCA「歷史相似案例」：只拿有寫處置（resolution 非空）的列，列出當時怎麼處理
    - 調參回測的黃金事故防護罩（admin.proposal_backtest）：verdict = true_positive 的列 = 黃金事故，
      提案參數重跑時窗內一個異常都抓不到就擋下

    特徵簽章（entities、metrics、signals）在寫入當下從事件群抽出來存好，查詢時不必再解析事件內容。
    entities 維持 JSON 清單（一個事件群可以有多個實體），實體過濾在 Python 端做（Knowledge.golden）：
    黃金事故是人工確認的，量級是幾百到幾千筆，(pipeline, window_start) 索引先把範圍縮小就夠了。
    context：根因下鑽、範例 trace id、主要錯誤、tags 這類擴充資訊（PostgreSQL 上是 JSONB + GIN，見 migration 0005）。
    """

    __tablename__ = "incident_knowledge"
    __table_args__ = (
        Index("ix_incident_knowledge_pipeline_window", "pipeline", "window_start"),
        Index("ix_incident_knowledge_verdict_window", "verdict", "window_start"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_uid: Mapped[str] = mapped_column(String(40), unique=True)
    pipeline: Mapped[str] = mapped_column(String(128), index=True, default="")
    entities: Mapped[list] = mapped_column(JSON)
    metrics: Mapped[list] = mapped_column(JSON)
    signals: Mapped[list] = mapped_column(JSON)
    resolution: Mapped[str] = mapped_column(String(4000), default="", server_default="")  # 空字串 = 還沒寫處置
    root_cause: Mapped[str] = mapped_column(String(1000), default="")
    author: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    # true_positive = 黃金事故；同一個事件群後來被改判 false_positive 就降級（不刪，留紀錄）
    verdict: Mapped[str] = mapped_column(String(16), default="true_positive", server_default="true_positive")
    severity: Mapped[str] = mapped_column(String(8), default="", server_default="")
    # 事件群的時窗（事件群 start／end，UTC）。黃金事故回測就重跑這一段
    window_start: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    window_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    context: Mapped[dict] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now, onupdate=utc_now)


def reset_all() -> None:
    """測試用：清空並重建所有資料表。"""
    drop_all()
    create_all()
