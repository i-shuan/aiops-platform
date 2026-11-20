"""API 與手動動作的請求型別（Pydantic）。API 層與控制面（admin）算子共用，所以放在 engine。

全部繼承 StrictModel：請求裡多了不認得的欄位（window_day 少了 s、threshold 放錯層）回 422，不會默默用預設值跑完。"""
from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from aiops.engine.pipeline import EnvironmentName, GraphEdgeSpec, PeerGroupSpec, PeerSpec, SourceType, StrictModel, TargetSpec


class TimeRangeIn(StrictModel):
    start: datetime
    end: datetime
    step_seconds: int = Field(60, gt=0)

    @model_validator(mode="after")
    def _ordered(self):
        if self.start >= self.end:
            raise ValueError("start 必須早於 end")
        return self


class MetricIn(StrictModel):
    source_type: SourceType  # 必填：漏寫回 400，不再默默退回 mock 假資料
    query: str


# ── 執行 workflow ──


# API 只能用內建 workflow 的名稱（不能給路徑）；runner.workflow_path 會再檢查一次
WorkflowName = Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=128)


class RunWorkflowRequest(StrictModel):
    workflow: str = WorkflowName
    # 不給就用 workflow 檔裡的 targets
    targets: list[TargetSpec] | None = None
    # 回放用：不給就用現在
    now: datetime | None = None


class RcaRequest(StrictModel):
    incident_id: int
    # 不給 = 依事件群所屬偵測管線的 rca_workflow 決定（interfaces/runner.rca_workflow_for）
    workflow: str | None = Field(None, pattern=r"^[A-Za-z0-9_-]+$", max_length=128)


class IdRef(StrictModel):
    id: int


class SourceRef(StrictModel):
    source_type: str


class UidRef(StrictModel):
    uid: str


# ── 靜音與事件群 ──


class SilenceCreate(StrictModel):
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


class MappingSubmit(StrictModel):
    source_type: str
    # 來源欄位 -> 標準欄位（ts／value／entity）
    fields: dict[str, str] = Field(min_length=1)
    submitter: str


class MappingApprove(StrictModel):
    mapping_id: int
    approver: str


# ── 匯入與合成 ──


class SpanIn(StrictModel):
    trace_id: str
    service: str
    start: datetime
    duration_ms: float = Field(ge=0)
    error: bool = False
    span_id: str | None = None
    parent_id: str | None = None


class TraceMerge(StrictModel):
    spans: list[SpanIn] = Field(min_length=1)
    step_seconds: int = Field(60, gt=0)
    persist: bool = True
    environment: EnvironmentName = ""  # 推導出的呼叫邊寫進哪個環境（= 讀它的管線的 Workflow.environment）；trace 時序不分環境


class TopologyRefresh(StrictModel):
    """admin.topology_refresh：從 trace 來源撈 [end − lookback_minutes, end] 的 span，把服務呼叫邊沉澱進 service_edges。"""

    source_type: SourceType = "elasticsearch"
    lookback_minutes: int = Field(60, gt=0, le=24 * 60)
    max_spans: int = Field(20000, gt=0, le=200000)
    prune_days: int = Field(7, gt=0)  # last_seen 早於「現在 − prune_days」的邊移除
    end: datetime | None = None  # 回放歷史用；不給 = 現在
    # 邊寫進哪個環境、prune 只刪哪個環境（= 讀它的管線的 Workflow.environment；'' = 不分）。
    # 設了環境時，撈 span 也只撈這個環境的（deployment.environment／service.environment；查詢時濾一次、回來再濾一次）
    environment: EnvironmentName = ""


class GraphImport(StrictModel):
    """admin.graph_import：匯入宣告的關係邊（CMDB、設備清冊、供電／管線圖）。每條邊必寫 kind。
    mode = replace：這個環境的宣告邊整份換成這一批（CMDB 同步用，edges 可以是空的 = 清空）；merge：加進去、同一條邊更新 weight。
    只動宣告的邊：trace 沉澱的 service_edges、Lot 履歷推導的流向都不受影響。"""

    edges: list[GraphEdgeSpec] = Field(max_length=50000)
    mode: Literal["replace", "merge"]
    environment: EnvironmentName = ""
    source: str = Field("", max_length=128)  # 這一批是哪裡來的（cmdb、手動…），只記錄不參與判斷


class LotStepIn(StrictModel):
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


class LotPathsMerge(StrictModel):
    steps: list[LotStepIn] = Field(min_length=1, max_length=50000)
    persist: bool = True
    environment: EnvironmentName = ""  # 路徑寫進哪個環境（= 讀它的工廠管線的 Workflow.environment）


class ExternalAnomalyIn(StrictModel):
    external_id: str
    entity: str
    metric: str
    ts: datetime
    score: float = Field(ge=0, allow_inf_nan=False)  # Infinity 過得了 ge=0：在 API 入口就擋，不要進資料庫
    value: float | None = Field(default=None, allow_inf_nan=False)
    attrs: dict[str, Any] = Field(default_factory=dict)


class IngestAnomalies(StrictModel):
    pipeline: str
    source: str
    anomalies: list[ExternalAnomalyIn] = Field(min_length=1, max_length=5000)


# Grafana 告警 webhook 的網址參數：要進哪條管線（跟 workflow 名稱同一套白名單）、來源代號（哪個團隊；外部異常以 來源 + external_id 去重）
PIPELINE_NAME = r"^[A-Za-z0-9_-]+$"
SOURCE_NAME = r"^[A-Za-z0-9_.-]+$"


class GrafanaAlertIngest(StrictModel):
    """admin.ingest_grafana_alerts：各團隊自己的 Grafana 告警，經 webhook 通知管道推進來。

    payload 是 Grafana 送來的原樣（第三方格式）：不用嚴格模型解析，因為 Grafana 每個版本都可能多欄位，嚴格模型會讓
    對方升級 Grafana 之後告警全部進不來。算子只讀它需要的欄位（alerts 裡的 status、labels、fingerprint、startsAt、
    generatorURL、valueString、values），每一筆都檢查型別，格式不對的列在結果的 rejected 裡。"""

    pipeline: str = Field(pattern=PIPELINE_NAME, max_length=128)
    source: str = Field(pattern=SOURCE_NAME, max_length=64)
    payload: dict[str, Any]


# ── 調參 ──


class ProposalCreate(StrictModel):
    source: Literal["auto", "llm", "manual"]
    detector: str  # 算子名，例如 detector.zscore
    params: dict[str, Any] = Field(default_factory=dict)
    reason: str = ""
    proposer: str
    # 這個提案改哪一層："*" = 不限定。只給 pipeline = 這條管線的所有實體；兩個都給 = 只改這條管線的這個實體
    pipeline: str = Field("*", min_length=1, max_length=128)
    entity: str = Field("*", min_length=1, max_length=256)


class ProposalBacktest(StrictModel):
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


class ProposalApprove(StrictModel):
    proposal_id: int
    approver: str


class ReviewIn(StrictModel):
    """對一張事故卡的分項覆核：平台做的四件事（收斂、RCA、結案、嚴重度）各自對不對。

    每一項都是選填：None = 這一項沒看（不算進分母），"ok" = 看過、是對的。「沒填」跟「對」分開記，
    之後算準確率才有分母（只收「錯了」的話，沒人抱怨跟沒人看分不出來）。
    細節欄位只能跟對應的判定一起給（寫了 unrelated_entities 卻沒說 grouping=merged_wrong 是填錯，直接拒絕）。
    這裡只收資料：任何一項都不會自動改設定（control_plane/learning/review.py）。"""

    # 收斂：merged_wrong = 不相干的告警被併進這張卡；split_wrong = 同一件事被拆成好幾張卡
    grouping: Literal["ok", "merged_wrong", "split_wrong"] | None = None
    # RCA：wrong_root = 指錯根因；missed_root = 平台說無法判定（或只給症狀位置），但其實找得到
    rca: Literal["ok", "wrong_root", "missed_root"] | None = None
    # 結案：early = 故障還在卡就結案了
    resolve: Literal["ok", "early"] | None = None
    severity: Literal["ok", "too_high", "too_low"] | None = None
    unrelated_entities: list[Annotated[str, Field(min_length=1, max_length=256)]] = Field(default_factory=list, max_length=50)
    same_as: list[Annotated[str, Field(min_length=1, max_length=40)]] = Field(default_factory=list, max_length=50)
    actual_root_entity: str | None = Field(None, min_length=1, max_length=256)

    @model_validator(mode="after")
    def _shape(self):
        if not any((self.grouping, self.rca, self.resolve, self.severity)):
            raise ValueError("review 至少要填一項（grouping、rca、resolve、severity）；都沒看就不要帶 review")
        if self.unrelated_entities and self.grouping != "merged_wrong":
            raise ValueError("unrelated_entities（不該併進來的實體）只能跟 grouping=merged_wrong 一起給")
        if self.same_as and self.grouping != "split_wrong":
            raise ValueError("same_as（其實是同一件事的其他事件群）只能跟 grouping=split_wrong 一起給")
        if self.actual_root_entity and self.rca not in ("wrong_root", "missed_root"):
            raise ValueError("actual_root_entity（實際的根因）只能跟 rca=wrong_root 或 missed_root 一起給")
        return self

    def to_dict(self) -> dict[str, Any]:
        """只留有填的項目（沒看的不存成 null：之後查「有沒有看過這一項」就是鍵在不在）。"""
        return {k: v for k, v in self.model_dump().items() if v not in (None, [])}


class FeedbackSubmit(StrictModel):
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
    # true_positive 的擴充資訊（根因下鑽、範例 trace id、主要錯誤、tags），存進經驗庫的 context
    context: dict[str, Any] = Field(default_factory=dict)
    # 分項覆核（收斂、RCA、結案、嚴重度各自對不對）：只能針對事件群
    review: ReviewIn | None = None

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
        if self.context and (self.verdict != "true_positive" or self.incident_uid is None):
            raise ValueError("context 只能寫在針對事件群、確認為真異常（true_positive）的回饋上")
        if self.review is not None and self.incident_uid is None:
            raise ValueError("review（分項覆核）只能寫在針對事件群的回饋上：漏報補錄沒有事故卡可以覆核")
        return self


class FeedbackPropose(StrictModel):
    feedback_id: int
    proposer: str
    detector: str | None = None
    # 提案要改哪一層（管線取自事件群本身）：
    # entity = 只改這條管線的這個實體（事件群只有一個實體時可省略 entity）；pipeline = 這條管線全部；global = 全平台
    scope: Literal["entity", "pipeline", "global"] = "pipeline"
    entity: str | None = None


class SeriesProfile(StrictModel):
    """序列體檢（admin.series_profile）：撈 days 天的序列，算 24h／168h 自相關、偏態、峰度。
    沒有日週期也沒有週週期時，產生「把 detector.hourly_baseline 從這個 target 排除」的建議（advisory，要改 YAML 才生效）。
    end 不給 = 現在；回放歷史資料時給資料的最後時間。"""

    metric: MetricIn
    pipeline: str = Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=128)
    entity: str = Field(min_length=1, max_length=256)
    days: int = Field(14, ge=8, le=60)  # 至少 8 天：168h 的自相關要有一整週的配對
    step_seconds: int = Field(300, ge=60, le=3600)
    end: datetime | None = None
    proposer: str = "series_profiler"

    def to_targets(self) -> list:
        return [TargetSpec(source_type=self.metric.source_type, query=self.metric.query, entity=self.entity)]


class TuningMetricsQuery(StrictModel):
    """調參成效報表：每個核准過的調參，在核准前後各 window_days 天的事件群與誤報數。只看最近 lookback_days 天內核准的。"""

    window_days: int = Field(7, ge=1, le=90)
    lookback_days: int = Field(90, ge=1, le=365)


class ReviewMetricsQuery(StrictModel):
    """覆核統計：最近 lookback_days 天收到的分項覆核，每一項看過幾張、對幾張、錯在哪。pipeline 不給 = 全部。"""

    lookback_days: int = Field(90, ge=1, le=365)
    pipeline: str | None = Field(None, pattern=r"^[A-Za-z0-9_-]+$", max_length=128)


class RulePromote(StrictModel):
    """規則升級：同一層（偵測器 × 管線 × 實體）在 window_days 天內被核准調參 >= min_approvals 次，
    就用它目前的基準線產生一個 detector.rules 靜態規則的提案（仍要回測、核准才生效）。"""

    min_approvals: int = Field(3, ge=2, le=50)
    window_days: int = Field(30, ge=1, le=365)
    consecutive: int = Field(3, ge=1, le=60)
    proposer: str = "rule_promoter"
