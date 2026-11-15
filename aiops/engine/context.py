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

from aiops.engine.graph import Edge, ServiceGraph
from aiops.engine.health import RunCheck, StreamCheck
from aiops.engine.rca_types import ConclusionType, EvidenceItem, Fact, RcaStatus
from aiops.engine.types import AnomalyEvent, GrafanaLinks, Incident, Notification


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
class DetectorEvidence:
    """偵測節點開了 snr（節點參數）時交出的逐點證據：給 detector.soft_vote 軟投票、之後也能當連續證據餵給機率模型。

    snr[i] = 這個節點的統計量在第 i 點的偏離 / 該統計量的雜訊尺度（也就是節點自己的分數，連續值、不是布林）。
    threshold 跟 snr 同單位：由目標虛警率 p_fa 換算（threshold_source 說是怎麼換的，見 stages/detectors/base.snr_threshold）。
    series_snr_db：序列層 SNR（訓練期 168 格季節基準的變異數 / 殘差雜訊變異數），資料不足時是 None。"""

    timestamps: list[str]
    snr: list[float]
    threshold: float
    threshold_source: str  # gaussian／empirical／fallback
    p_fa: float
    series_snr_db: float | None = None


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
    # 下鑽歸因：這個服務的增量主要花在等哪個下游資源（例如 "tax-gw:443（external/http）"）。候選實體仍是服務，
    # 拓撲、傳播路徙、標準答案都不變；這一欄只告訴人「該去查的不是這個服務的程式，是它等的東西」。空字串 = 沒有
    dependency: str = ""
    # 給程式判斷用的結構化版本（Runbook 規則、LLM context）：resource、kind、subtype、self_ms{before,during}、
    # wait_ms{before,during}、failures{before,during}、share（等待增量 ÷ 自身耗時增量，0～1）、by（"latency"／"errors"／""）。
    # 有資源葉節點就填（share 不夠主導時 by 為空、dependency 為空字串，但數字照給）；沒有資源葉節點就是 {}
    attribution: dict[str, Any] = field(default_factory=dict)


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
    # 以下是選填（資料源沒給就是預設值，所有統計退回只看服務；mock 資料源與既有測試都不帶）
    kind: str = ""          # span.type：""（入口 transaction）／app／db／external／messaging…
    subtype: str = ""       # span.subtype：redis／http／postgresql
    resource: str = ""      # span.destination.service.resource：tax-gw:443、redis
    status_code: int = 0    # http.response.status_code；0 = 沒有
    name: str = ""          # transaction.name：POST /invoice（只給報告文字用）
    # 訊息佇列（Elastic APM messaging）：消費者 transaction 才有值。非同步父子邊跟同步呼叫要分開算（見 stages/rca/spans.py）
    consumer: bool = False  # transaction.type = messaging：這個入口是「從佇列拿到訊息」，不是被同步呼叫
    queue: str = ""         # transaction.message.queue.name
    age_ms: float | None = None  # transaction.message.age.ms：訊息在佇列裡等了多久；None = 資料源沒給
    links: tuple[tuple[str, str], ...] = ()  # span.links：批次消費時指回各個生產者 span 的 (trace_id, span_id)
    # 取樣權重：這個 span 代表幾個實際的 span（APM representative_count、OTel tracestate 的 adjusted count）；
    # 沒取樣資訊就是 1。比例、平均、流量用它加權；「樣本夠不夠」的門檻仍看實際撈到幾筆（見 stages/rca/spans.py）
    weight: float = 1.0
    environment: str = ""  # 部署環境（deployment.environment／service.environment）；資料源沒給就是空字串


@dataclass(frozen=True)
class MqEdge:
    """RCA 從 span 還原的一條非同步邊（生產者 -> 佇列 -> 消費者）。延遲 = 消費時間 − 發布時間（T_pub）。

    pub_start／pub_end 是事件期間被消費的那批訊息「當初是什麼時候發布的」（T_pub 的 P5～P95，前後各留緩衝）：
    根因算子拿這個時窗去看生產者，而不是拿事件時窗（消費者 12:20 才出事，訊息可能是 12:00 發的）。
    linked_by：T_pub 怎麼來的——parent（消費者的 parent.id 指到 send span）、link（span.links）、
    age（只有 message.age，知道什麼時候發的但不知道是誰發的，producer 是空字串）。
    """

    producer: str
    consumer: str
    queue: str
    messages: int
    lag_ms_p50: float
    lag_ms_p95: float
    pub_start: datetime
    pub_end: datetime
    linked_by: str
    # 事件前（基準窗）同一條邊的延遲 P95；沒有樣本就是 None。報告印「延遲 X→Y」用
    lag_ms_p95_before: float | None = None
    failed: int = 0  # 這批訊息裡消費者「自己失敗」（不是下游連帶）的入口筆數（訊息夾帶失敗轉記用）

    def to_dict(self) -> dict[str, Any]:
        return {"producer": self.producer, "consumer": self.consumer, "queue": self.queue, "messages": self.messages,
                "lag_ms": {"p50": round(self.lag_ms_p50, 1), "p95": round(self.lag_ms_p95, 1),
                           "p95_before": None if self.lag_ms_p95_before is None else round(self.lag_ms_p95_before, 1)},
                "pub_window": [self.pub_start.isoformat(), self.pub_end.isoformat()],
                "linked_by": self.linked_by, "failed": self.failed}


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
    # 管線所屬環境（Workflow.environment；空字串 = 不分）。storage 只載入這個環境的關係圖；RCA 改用事件群快照記的環境
    environment: str = ""

    # ── 載入的輸入（引擎在跑之前載入；算子只讀）──
    field_maps: dict[str, dict[str, str]] = field(default_factory=dict)  # source_type -> 已核准對照（來源欄 -> 標準欄）
    trace_rows: dict[str, list[TraceRow]] = field(default_factory=dict)  # 服務 -> trace 合成時序
    # source_type = platform 的原始紀錄（engine/platform_series.py）：查詢字串 -> [(時間, 狀態)]。StorageHooks.load_target 載入，
    # ingestion.fetch 換成每格筆數。只有看門狗管線（pipelines/watchdog/）用得到
    platform_rows: dict[str, list[tuple[datetime, str]]] = field(default_factory=dict)
    # 節點名稱 -> tuning 核准的現行參數（已依 管線×實體 > 管線 > 實體 > 全域 挑出最具體的一筆）
    detector_params: dict[str, dict[str, Any]] = field(default_factory=dict)
    detector_states: dict[str, DetectorState] = field(default_factory=dict)  # 節點名稱 -> 基準線
    # 這條序列這一輪真的會跑、而且會存基準線的偵測節點（引擎依路由算好）。ingestion.fetch 只在這些節點裡有人
    # 還沒有基準線或基準線到期時才抓訓練窗：全部被跳過的序列不抓（沒有人會學）
    baseline_nodes: list[str] = field(default_factory=list)
    last_anomaly_ts: datetime | None = None  # 這條序列最後一筆已存的異常時間（只處理比它新的）
    # 這條序列訓練基準線時要排除的時段（這個實體的歷史事故、維護窗；被標成誤報的事故不排除）
    training_exclusions: list[TrainingExclusion] = field(default_factory=list)
    silences: list[Silence] = field(default_factory=list)
    recent_notifications: list[RecentNotification] = field(default_factory=list)
    # 關係圖的載入格式（YAML、資料表、事件群快照都是這個形狀）。算子不直接讀這三個欄位，一律問 ctx.graph
    topology: list[tuple[str, str]] = field(default_factory=list)  # (caller, callee)
    lot_edges: list[LotEdge] = field(default_factory=list)
    # 非同步邊（訊息佇列）：(生產者, 消費者) -> 延遲 P95（毫秒）。由 admin.topology_refresh 沉澱、引擎載入；
    # alert.messaging_link 用它把「生產者先出事、消費者隔一段延遲才出事」的兩群併成一群
    async_edges: dict[tuple[str, str], float] = field(default_factory=dict)
    # Workflow 明確給了（含給空清單）就不要再從 trace／Lot 履歷自動推導
    # 宣告的邊，每條自帶型別（call／supply／flow／async）：declared_edges 是這條 Workflow 的 graph 寫的，
    # imported_edges 是 admin.graph_import 匯進資料庫、引擎依環境載入的（RCA 時是事件群建立當下的快照）。
    # 跟上面三個欄位一樣只是載入與存檔的格式，算子一律問 ctx.graph
    declared_edges: list[Edge] = field(default_factory=list)
    imported_edges: list[Edge] = field(default_factory=list)
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
    # 開了 snr 的偵測節點交出的逐點證據（節點名稱 -> 證據）；沒開的節點不會出現在這裡
    detector_evidence: dict[str, DetectorEvidence] = field(default_factory=dict)
    new_detector_states: dict[str, DetectorState] = field(default_factory=dict)  # 節點名稱 -> 這次新訓練的，最後才存
    anomalies: list[AnomalyEvent] = field(default_factory=list)
    # 這一輪各序列偵測出來、要寫進資料庫的異常（由引擎彙整；不含從資料庫載入的外部匯入異常，那些另有自己的表）。
    # 跟通知一起在寫入階段落地，不再每條序列先寫：送通知失敗時整輪一起回復，下一輪才會重新偵測到同一批
    detected_anomalies: list[AnomalyEvent] = field(default_factory=list)
    incidents: list[Incident] = field(default_factory=list)
    incidents_built: bool = False  # ensure_incidents 建過了：之後清單變空也不再從 anomalies 重建
    incident_ids: dict[str, int] = field(default_factory=dict)  # 事件群 fingerprint -> 已存的 id（持久化後填）
    # 認知型 RCA（docs/COGNITIVE_RCA_DESIGN.md）：事實 → LLM 抽出的證據 → 根因判斷（rca.root_cause.llm）。status 為 undetermined 時報告照出但沒有根因結論
    rca_facts: list[Fact] = field(default_factory=list)
    rca_evidence: list[EvidenceItem] = field(default_factory=list)
    rca_status: RcaStatus = "ok"
    rca_undetermined_reason: str = ""  # 連不上、逾時、回傳格式不合、驗證全數被拒 …
    rca_root_candidates: list[RootCandidate] = field(default_factory=list)  # 由 rca.root_cause.* 排序好
    rca_root_algorithm: str = ""
    # 根因演算法的結論說明，例如「證據不足」：沒給候選時報告要講清楚為什麼，不是讓段落默默消失
    rca_root_note: str = ""
    # 根因結論的等級（engine/rca_types.ConclusionType）：根因節點執行時由 RootCauseOperator 依演算法的宣告填；
    # 沒有根因節點、或沒有候選就是 undetermined。reason 是「為什麼是這一級」的說明（只有需要解釋的等級才有）
    rca_conclusion_type: ConclusionType = "undetermined"
    rca_conclusion_reason: str = ""
    # incident_traces 有設定 trace 來源、但這一輪連不上或讀失敗（例如 ES 重啟）。跟「這個場景本來就沒有 trace」（工廠）要分開：
    # 前者證據被吃掉，根因算子不該拿拓撲硬猜；後者退回拓撲推測是正常做法
    rca_traces_failed: bool = False
    # incident_traces 的事件前基準窗：(設定的分鐘, 實際用的分鐘)。入口請求不足時會往前擴展，報告要照實標出來；None = 沒撈 trace
    rca_trace_lookback: tuple[int, int] | None = None
    # follow_messaging 補撈了更早的 span（生產者發布時段）時，主撈取窗裡最早那個 span 的時間：事件前基準從這裡算起，
    # 不是從補撈回來的最早那個 span。None = 沒有補撈（基準就是手上最早的 span）
    rca_main_start: datetime | None = None
    # RCA 重抓的事件時段原始時序：(實體, 指標) -> DataFrame(ts, value)。給互相關等需要原始資料的演算法
    rca_series: dict[tuple[str, str], pd.DataFrame] = field(default_factory=dict)
    # RCA 隨需現抓的事件前後 span（ingestion.incident_traces），給 rca.build_dynamic_topology 還原當下的呼叫關係
    rca_spans: list[SpanRecord] = field(default_factory=list)
    # 事件實體當消費者的非同步邊（ingestion.incident_traces 的 follow_messaging 還原）；沒有 messaging 資料就是空的
    rca_mq: list[MqEdge] = field(default_factory=list)
    # 看到 messaging 消費者但跨不了時窗的原因（沒有 link／parent／age、延遲超過上限、補撈失敗）：報告要照實寫
    rca_mq_note: str = ""
    # RCA 隨需現抓的錯誤日誌（ingestion.incident_logs）：事件期間與事件前的正常期間，給 rca.section.log_patterns 比對
    rca_logs: list[LogRecord] = field(default_factory=list)
    rca_logs_baseline: list[LogRecord] = field(default_factory=list)
    rca_logs_sampled: bool = False  # 日誌超過撈取上限，是抽樣加權的估計值（rca.section.log_patterns 要在報告裡寫明）
    rca_log_window_minutes: tuple[float, float] = (0.0, 0.0)  # (事件期間, 正常期間) 的長度，算每分鐘頻率用
    rca_sections: list[ReportSection] = field(default_factory=list)
    rca_skipped: list[str] = field(default_factory=list)
    # rca.section.suggested_actions 命中的故障特徵（Runbook）：{"id", "name", "actions": [...]}；沒命中任何一條就是 None。
    # 給 enrichment.llm_briefing 當 context，也讓 API 的呼叫端不用去解析段落文字
    rca_runbook: dict[str, Any] | None = None
    llm_summary: str | None = None
    llm_claims: list[dict[str, Any]] = field(default_factory=list)
    llm_generated_by: str = "template"
    llm_rejected: str | None = None
    # enrichment.llm_briefing 的三行止血摘要：{"summary_line", "action_line", "verification_line"}；模板或 LLM 寫的，
    # generated_by 說明是哪個；LLM 回文不過驗證時 rejected 寫原因並退回模板
    llm_briefing: dict[str, str] | None = None
    llm_briefing_generated_by: str = "template"
    llm_briefing_rejected: str | None = None
    notifications: list[Notification] = field(default_factory=list)
    grafana_links: GrafanaLinks | None = None  # alert.notify_payload 組好，alert.heartbeat 續命時沿用
    resolved_incident_ids: list[int] = field(default_factory=list)
    errors: list[TargetError] = field(default_factory=list)
    audit: list[str] = field(default_factory=list)
    # 資料健康度（engine/health.py）。每輪新建的 ctx 上，跑完由 hooks.record_health 寫進資料庫：
    # stream_seen_before：（序列層，引擎載入）這條資料流已經有「第一次收到資料」的紀錄，用來分「斷流」與「從未有過資料」
    # stream_checks：（批次層）這一輪每條資料流的檢查結果；run_check：這一輪整條管線的執行結果（None = 這一輪不記）
    # health_retention_days：健康度紀錄留幾天（Workflow.health_retention_days）；None = 這一輪不清舊紀錄
    health_retention_days: int | None = None
    stream_seen_before: bool = False
    stream_checks: list[StreamCheck] = field(default_factory=list)
    run_check: RunCheck | None = None

    # ── 手動動作（API）：請求與結果 ──
    incident_id: int | None = None  # RCA 等針對單一事件群的流程
    incident_pipeline: str = ""  # 那個事件群是哪條管線產生的（RCA 的 ctx.pipeline 是報告流程自己的名字）
    # 那個事件群的環境（建立時寫進快照；舊快照沒有就沿用 ctx.environment）。RCA 的關係圖照它載入
    incident_environment: str = ""
    request: BaseModel | None = None
    result: dict[str, Any] = field(default_factory=dict)

    @property
    def graph(self) -> ServiceGraph:
        """實體關係圖（engine/graph.py）。每次從當下的 topology／lot_edges／async_edges 現算、不快取：
        rca.build_dynamic_topology 會在一輪中途改 topology，快取會讓後面的節點讀到舊圖。"""
        return ServiceGraph.from_parts(self.topology, self.lot_edges, self.async_edges,
                                       [*self.declared_edges, *self.imported_edges])

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
            root_cause: dict[str, Any] = {"algorithm": self.rca_root_algorithm, "note": self.rca_root_note,
                                          "conclusion_type": self.rca_conclusion_type,
                                          "conclusion_reason": self.rca_conclusion_reason,
                                          "candidates": [{"entity": c.entity, "score": round(c.score, 4), "reason": c.reason,
                                                          "dependency": c.dependency, "attribution": c.attribution}
                                                         for c in self.rca_root_candidates]}
            if self.rca_mq or self.rca_mq_note:  # 沒有 messaging 資料時回傳的形狀跟以前一樣
                root_cause["messaging"] = {"note": self.rca_mq_note, "edges": [e.to_dict() for e in self.rca_mq]}
            if self.rca_facts or self.rca_evidence or self.rca_status != "ok":  # 舊引擎沒有這些，回傳形狀不變
                root_cause["status"] = self.rca_status
                root_cause["undetermined_reason"] = self.rca_undetermined_reason
                out["facts"] = [{"kind": f.kind, "entity": f.entity, "value": f.value, "unit": f.unit,
                                 "window": f.window, "detail": f.detail} for f in self.rca_facts]
                out["evidence"] = [{"kind": e.kind, "entity": e.entity, "state": e.state, "confidence": e.confidence,
                                    "quote": e.quote, "log_ts": e.log_ts} for e in self.rca_evidence]
            out.update(sections=[s.to_dict() for s in self.rca_sections], skipped=self.rca_skipped,
                       root_cause=root_cause,
                       runbook=self.rca_runbook,
                       narrative={"summary": self.llm_summary, "claims": self.llm_claims,
                                  "generated_by": self.llm_generated_by, "rejected": self.llm_rejected,
                                  "briefing": self.llm_briefing, "briefing_generated_by": self.llm_briefing_generated_by,
                                  "briefing_rejected": self.llm_briefing_rejected})
        if self.errors:
            out["errors"] = [{"entity": e.entity, "message": e.message} for e in self.errors]
        return out
