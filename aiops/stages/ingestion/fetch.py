"""抓資料的算子。整個 stages/ 只有這個檔案可以碰 sources（tests/test_architecture.py 檢查）。

- ingestion.fetch：依 ctx.target 抓這一輪要偵測的資料放進 ctx.raw_df（需要重訓時另外抓較長的歷史放進 ctx.train_df）
  - 同儕：peers 寫死的名單 -> peer_0、peer_1…；peer_group 依標籤動態聚合整群 -> 接在後面，成員名單記在 ctx.peer_members
  - lags_minutes：同一時刻往前 N 分鐘的值 -> lag_<N> 欄位（給同比、環比偵測器）
- ingestion.incident_series：RCA 用，依事件群裡每筆異常的 (資料源, 指標) 重抓事件時段的原始時序 -> ctx.rca_series
- ingestion.incident_traces：RCA 用，隨需撈事件前後的 span -> ctx.rca_spans（給 rca.build_dynamic_topology 還原呼叫關係）。
  日常偵測完全不撈 trace（trace 量通常是指標的 10 倍以上），只有出 RCA 報告時才去撈那一小段

外部資料源走 sources，已核准的欄位對照由引擎載入 ctx.field_maps。
source_type=trace：平台自己合成並存起來的 trace 時序（引擎載入 ctx.trace_rows），query 寫「服務/欄位」。
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from string import Template

import pandas as pd

from aiops.engine.context import LogRecord, PipelineContext, SpanRecord, TargetError
from aiops.engine.errors import BadRequest, InfrastructureError, NoDataError, NotFound
from aiops.engine.mq import age_p95_ms, build_edges, consumer_entries, spread, unresolved_trace_ids
from aiops.engine.params import positive
from aiops.engine.platform_series import PLATFORM_SOURCE, bucket_counts, parse_platform_query, selects
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import as_utc
from aiops.sources import get_datasource_spec, is_configured
from aiops.sources.base import canonical_service, in_environment
from aiops.sources.query.trace import parse_trace_query
from aiops.sources.query.window import align_window, to_utc
from aiops.engine.operator import BaseOperator

logger = logging.getLogger("aiops.ingestion")


def _spelled_like_entities(entities: list[str]):
    """資料源回來的服務名已正規化（去空白、小寫）；換回事件實體自己的寫法，下游（拓撲、根因）用實體名比對才對得上。"""
    aliases = {canonical_service(e): e for e in entities}
    return lambda service: aliases.get(canonical_service(service), canonical_service(service))

TRACE_SOURCE = "trace"


def _trace_frame(ctx: PipelineContext, query: str, start: datetime, end: datetime, step: int) -> pd.DataFrame:
    try:
        service, field = parse_trace_query(query)
    except ValueError as e:
        raise BadRequest(str(e)) from e
    s, e = align_window(start, end, step)
    points = sorted((to_utc(r.ts), float(getattr(r, field))) for r in ctx.trace_rows.get(service, [])
                    if s <= to_utc(r.ts) <= e)
    if not points:
        raise NoDataError(f"{TRACE_SOURCE} 在這段時間沒有 {service} 的資料")
    return pd.DataFrame({"ts": pd.to_datetime([t for t, _ in points], utc=True), "value": [v for _, v in points]})


def _field_map_kw(ctx: PipelineContext, source_type: str) -> dict:
    """已核准的欄位對照（span.*／log.* 標準欄會蓋過資料模型的預設欄位，見 sources/es_models.py）。
    沒有對照時不帶這個參數：自訂資料源（只實作舊簽名的 fetch_spans／fetch_logs）照樣能用。"""
    fields = ctx.field_maps.get(source_type)
    return {"field_map": fields} if fields else {}


def _rca_environment(ctx: PipelineContext) -> str:
    """RCA 撈 trace／日誌要用哪個部署環境：事件群自己的（建立時寫進快照），沒有就用這條管線的。空字串 = 不分。"""
    return ctx.incident_environment or ctx.environment


def _scope_kw(ctx: PipelineContext, source_type: str) -> dict:
    """欄位對照＋部署環境。兩個都是有才帶：只實作舊簽名的自訂資料源照樣能用。"""
    env = _rca_environment(ctx)
    return {**_field_map_kw(ctx, source_type), **({"environment": env} if env else {})}


def _platform_frame(ctx: PipelineContext, query: str, start: datetime, end: datetime, step: int) -> pd.DataFrame:
    """source_type = platform：平台自己的紀錄（StorageHooks 載進 ctx.platform_rows）換成每格筆數（engine/platform_series.py）。
    0 也是資料：查得到的範圍內一筆都沒有，就是一排 0。"""
    try:
        q = parse_platform_query(query)
    except ValueError as e:
        raise BadRequest(str(e)) from e
    if query not in ctx.platform_rows:
        raise NoDataError(f"{PLATFORM_SOURCE} 的紀錄沒有載入（{query}）：只有用 StorageHooks 跑的管線讀得到平台自己的紀錄")
    times = [ts for ts, status in ctx.platform_rows[query] if selects(q, status)]
    frame = bucket_counts(times, to_utc(start), to_utc(end), step, as_utc(ctx.now))
    if frame.empty:
        raise NoDataError(f"{query}：這段時間還沒有任何一格完整過完")
    return frame


def fetch_series(ctx: PipelineContext, source_type: str, query: str, start: datetime, end: datetime,
                 step: int) -> pd.DataFrame:
    """抓一條序列 -> DataFrame(ts, value)。"""
    if source_type == TRACE_SOURCE:
        return _trace_frame(ctx, query, start, end, step)
    if source_type == PLATFORM_SOURCE:
        return _platform_frame(ctx, query, start, end, step)
    return get_datasource_spec(source_type).fetch_data(query, start, end, step, ctx.field_maps.get(source_type))


def _group(ctx: PipelineContext, start: datetime, end: datetime, step: int) -> tuple[list[str], list[pd.DataFrame]]:
    """動態同儕：撈回整群、找出自己、回傳 (其他成員名單, 其他成員的序列)。"""
    t = ctx.target
    assert t is not None and t.peer_group is not None
    g = t.peer_group
    source_type = g.source_type or t.source_type
    if source_type in (TRACE_SOURCE, PLATFORM_SOURCE):
        raise BadRequest(f"{source_type} 資料源不支援 peer_group，請改用 peers")
    try:
        query = Template(g.query).substitute({k: str(v) for k, v in t.attrs.items()})
    except (KeyError, ValueError) as e:
        raise BadRequest(f"peer_group.query 引用了 target attrs 沒有的屬性：{e}") from e
    members = get_datasource_spec(source_type).fetch_group(query, start, end, step, g.by,
                                                           ctx.field_maps.get(source_type))
    me = str(t.attrs.get(g.by, t.entity))
    if me not in members:
        raise BadRequest(f"peer_group 撈回的成員裡找不到自己（{g.by}={me}）：{sorted(members)[:10]}")
    others = sorted(m for m in members if m != me)
    if not others:
        # 單一副本的服務：資料是有的，只是沒有同儕可以比。不是「沒資料」（不算斷流），也不擋住其他偵測節點；
        # 記成這條序列的錯誤讓人看到，同儕偵測器看到沒有 peer_* 欄位會棄權
        msg = f"peer_group 只撈到自己（{g.by}={me}），沒有同儕可以比；需要同儕的偵測器（requires_peers）這一輪棄權"
        if all(e.message != msg for e in ctx.errors):  # 偵測窗與訓練窗各抓一次，只記一筆
            ctx.errors.append(TargetError(t.entity, msg))
    return others, [members[m] for m in others]


def fetch_target_frame(ctx: PipelineContext, start: datetime, end: datetime, step: int,
                       lags_minutes: tuple[int, ...] | list[int] = ()) -> pd.DataFrame:
    """依 ctx.target（含同儕、往前位移）抓一段資料。抓資料與調參回測共用同一條路徑，回測才跟實際偵測用一樣的資料。"""
    t = ctx.target
    assert t is not None
    df = fetch_series(ctx, t.source_type, t.query, start, end, step)
    peers = [fetch_series(ctx, p.source_type, p.query, start, end, step) for p in t.peers]
    if t.peer_group is not None:
        members, group_frames = _group(ctx, start, end, step)
        ctx.peer_members = [*(p.query for p in t.peers), *members]
        peers += group_frames
    for i, p in enumerate(peers):
        df = df.merge(p.rename(columns={"value": f"peer_{i}"}), on="ts", how="left")
    for minutes in lags_minutes:
        shift = timedelta(minutes=int(minutes))
        past = fetch_series(ctx, t.source_type, t.query, start - shift, end - shift, step)
        past = past.assign(ts=past["ts"] + shift).rename(columns={"value": f"lag_{int(minutes)}"})
        df = df.merge(past, on="ts", how="left")
    return df


@OperatorRegistry.register("ingestion.fetch")
class FetchOperator(BaseOperator):
    def __init__(self, lookback_minutes: int = 60, step_seconds: int = 60,
                 train_lookback_minutes: int | None = None, retrain_minutes: int = 1440,
                 lags_minutes: list[int] | None = None):
        for key, value in (("lookback_minutes", lookback_minutes), ("step_seconds", step_seconds),
                           ("retrain_minutes", retrain_minutes), ("train_lookback_minutes", train_lookback_minutes)):
            if key == "train_lookback_minutes" and value is None:
                continue  # 不寫 = 不抓訓練窗
            positive(key, value)
        lags = [int(m) for m in (lags_minutes or [])]
        if any(m <= 0 for m in lags):
            raise ValueError("lags_minutes 必須都大於 0")
        if any((m * 60) % step_seconds for m in lags):
            raise ValueError("lags_minutes 換成秒必須是 step_seconds 的整數倍，位移後的點才對得上")
        self.lookback_minutes = lookback_minutes
        self.step_seconds = step_seconds
        self.train_lookback_minutes = train_lookback_minutes
        self.retrain_minutes = retrain_minutes
        self.lags_minutes = sorted(set(lags))

    def _needs_training(self, ctx: PipelineContext) -> bool:
        """這一輪要不要多抓一段訓練窗：只看引擎說這條序列會跑的基準線節點（ctx.baseline_nodes）。
        全部被路由跳過（或這條管線沒有會存基準線的節點）就不抓：抓了 1440 點也沒有人會學。"""
        if not self.train_lookback_minutes or not ctx.baseline_nodes:
            return False
        age = timedelta(minutes=self.retrain_minutes)
        for node in ctx.baseline_nodes:
            state = ctx.detector_states.get(node)
            if state is None or as_utc(ctx.now) - state.trained_at >= age:
                return True  # 還沒有基準線、或到期要重訓
        return False

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        epoch = int(as_utc(ctx.now).timestamp())
        end = datetime.fromtimestamp(epoch - epoch % self.step_seconds, tz=as_utc(ctx.now).tzinfo)
        ctx.raw_df = fetch_target_frame(ctx, end - timedelta(minutes=self.lookback_minutes), end, self.step_seconds,
                                        self.lags_minutes)
        if self._needs_training(ctx):
            ctx.train_df = fetch_target_frame(ctx, end - timedelta(minutes=self.train_lookback_minutes), end,
                                              self.step_seconds, self.lags_minutes)
        return ctx


@OperatorRegistry.register("ingestion.incident_series")
class IncidentSeriesFetch(BaseOperator):
    """RCA 用：依事件群每筆異常的 (資料源, 指標) 重抓 [開始 - before, 結束 + after] 的原始時序放進 ctx.rca_series。

    外部匯入或舊版沒記資料源的異常略過；某條抓不到（沒資料、查詢錯）列在 result["series_fetch"]["skipped"] 並略過。
    資料源連不上（InfrastructureError，已在 sources/_http 重試過）也只略過、寫進 errors，不讓整份 RCA 報告失敗：
    跟 incident_traces、incident_logs 一樣，這是 RCA 的旁路證據，拿不到就少一段，報告照出（傳播路徑、時間軸用既有資料）。
    同一個資料源連不上一次後，剩下用它的序列直接略過，不再一條條等逾時。
    （偵測流程的 ingestion.fetch 不在此列：拿不到資料就判斷不了，那一輪照規則失敗，由 notifier.watchdog 的心跳斷掉來通知。）
    params: before_minutes、after_minutes、step_seconds、max_series（避免一個大事件群把資料源打爆）。
    保留給新 RCA 引擎餵時序事實（ctx.rca_series -> rca/facts/series.py），目前沒有掛在任何管線上。
    """

    scope = "batch"

    def __init__(self, before_minutes: int = 30, after_minutes: int = 10, step_seconds: int = 60,
                 max_series: int = 20):
        if min(before_minutes, after_minutes) < 0 or step_seconds <= 0 or max_series <= 0:
            raise ValueError("before／after_minutes 不能小於 0；step_seconds、max_series 必須大於 0")
        self.before = timedelta(minutes=before_minutes)
        self.after = timedelta(minutes=after_minutes)
        self.step = step_seconds
        self.max_series = max_series

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not ctx.incidents:
            raise NotFound("沒有可以抓時序的事件群")
        incident = ctx.incidents[0]
        start, end = as_utc(incident.start) - self.before, as_utc(incident.end) + self.after
        wanted = sorted({(e.source_type, e.metric, e.entity) for e in incident.events if e.source_type})
        if len(wanted) > self.max_series:
            ctx.log(f"incident_series:truncated:{len(wanted)}->{self.max_series}")
            wanted = wanted[: self.max_series]
        skipped = []
        down: dict[str, str] = {}  # 這一次已確認連不上的資料源 -> 原因
        for source_type, metric, entity in wanted:
            if source_type in down:
                skipped.append({"entity": entity, "metric": metric, "reason": f"{source_type} 連不上（同一資料源前面已失敗）"})
                continue
            try:
                ctx.rca_series[(entity, metric)] = fetch_series(ctx, source_type, metric, start, end, self.step)
            except InfrastructureError as e:
                down[source_type] = str(e)
                logger.warning("incident_series：%s 連不上，略過用它的時序：%s", source_type, e)
                skipped.append({"entity": entity, "metric": metric, "reason": str(e)})
                ctx.errors.append(TargetError(f"node:{self.node_name or 'incident_series'}",
                                              f"{source_type} 連不上，RCA 不含用它的時序：{e}"))
            except (NotFound, BadRequest) as e:
                # 少一條序列，根因演算法就可能排錯：一律列在報告裡；查詢不合法是設定錯誤，另外記進 errors
                ctx.log(f"incident_series:skip:{entity}:{e}")
                skipped.append({"entity": entity, "metric": metric, "reason": str(e)})
                if isinstance(e, BadRequest):
                    logger.warning("incident_series：%s 的 %s 查詢不合法：%s", entity, metric, e)
                    ctx.errors.append(TargetError(f"node:{self.node_name or 'incident_series'}",
                                                  f"{entity} 的 {metric} 查詢不合法，RCA 不含這條時序：{e}"))
        ctx.result["series_fetch"] = {"fetched": len(ctx.rca_series), "skipped": skipped}
        return ctx


# 事件時間點的桶寬：一個點代表它起算的這一分鐘（跟 stages/rca 的事件期間 [start, end + 1 分鐘) 同一個定義；ingestion 不 import rca）
EVENT_BUCKET = timedelta(minutes=1)


@OperatorRegistry.register("ingestion.incident_traces")
class IncidentTracesFetch(BaseOperator):
    """RCA 用：隨需撈 [事件開始 - before, 事件最後一點那一分鐘的結尾 + after] 的 span 放進 ctx.rca_spans。

    事件的時間點是分鐘桶的起點：最後一點 03:04 代表 [03:04, 03:05)。根因演算法與 facts 的「事件期間」是
    [開始, 結束 + 1 分鐘)，撈取要蓋到同一個結尾。以前只撈到最後一點的起點，最後一分鐘沒撈；外部告警的事故卡
    開始＝結束（只有一個點），事件期間就一筆 span 都沒有（docs/E2E_DRILL.md 問題 6）。

    只在跑 RCA 報告時執行，日常偵測不碰 trace。這是旁路佐證，撈不到不該讓整份報告失敗：
    資料源沒設定 -> 略過（記在 audit）；連不上、查詢錯 -> 記進 errors 並略過，報告照出（拓撲就只用既有的）。
    params: source_type（預設 grafana）、before_minutes（預設 15）、after_minutes（預設 0）、
            max_spans（上限，預設 20000，超過就截斷並註記）、only_incident_services（預設 false：撈這段時間所有服務，
            才看得到事件實體跟沒出事的服務之間的關係；true 只撈事件裡的服務，量小但可能漏掉中間的服務）、
            max_before_minutes（預設 0 = 不擴展；設了就是基準窗自適應回溯的上限）、
            min_entry_spans（預設 20；要跟 rca.root_cause.evidence 的 min_requests 一致，兩邊各自宣告、不互相 import）、
            follow_messaging（預設 false；見下）、max_lag_minutes（預設 120）、max_linked_traces（預設 200）、
            min_consumer_spans（預設 5）、mq_pad_minutes（預設 2）。

    基準窗自適應回溯：事件裡任一實體在事件前 [start − before, start) 的入口 span 不到 min_entry_spans，
    而事件期間夠（擴基準窗才有用；期間也不夠就留給根因算子寫「證據不足」），就把基準窗加倍（15 → 30 → 60）直到
    max_before_minutes；每一步只撈多出來的前段 [start − new, start − old)，窗口維持連續、不跨週。
    越長的基準越鈍（事故前的緩慢劣化會被算成「正常」），所以只擴到剛好夠。實際用了幾分鐘記在 ctx.rca_trace_lookback，
    報告段落照實標出來。

    訊息佇列跨時窗（follow_messaging: true 才做；關著的時候行為跟沒有這段程式一樣）：
    事件實體在事件期間有 >= min_consumer_spans 筆訊息消費者入口時，這批訊息是更早之前發的，生產者有沒有問題要回到
    發布的那段時間看。做法：
      1. 消費者記的訊息年齡（message.age）P95 超過 max_lag_minutes -> 不撈，ctx.rca_mq_note 寫「延遲超過上限」
      2. 消費者的 parent／span.links 指到的生產者 span 不在手上的，挑至多 max_linked_traces 條 trace（等距挑，不是拿最早的），
         用 fetch_linked_spans 補回 [事件開始 − max_lag, 事件結束] 內的那幾條 trace
      3. engine/mq.build_edges 算每條 (生產者, 消費者, 佇列) 的延遲與生產者時窗（發布時間 P5~P95 ± mq_pad）-> ctx.rca_mq
      4. 生產者時窗＋它前面一段等長基準如果早於主撈取窗，再補撈那一段「所有服務」的 span（只撈生產者的話，
         它的同步下游不在樣本裡，等下游的時間會被算成自身耗時）
    補撈的量另計、合計不超過 max_spans（主撈取在尖峰時常常剛好用滿 max_spans，共用額度等於永遠補不到）。
    這一段任何一步失敗都只記 errors 與 ctx.rca_mq_note，主撈取的 span 照用、報告照出。
    """

    scope = "batch"

    def __init__(self, source_type: str = "grafana", before_minutes: int = 15, after_minutes: int = 0,
                 max_spans: int = 20000, only_incident_services: bool = False, max_before_minutes: int = 0,
                 min_entry_spans: int = 20, follow_messaging: bool = False, max_lag_minutes: int = 120,
                 max_linked_traces: int = 200, min_consumer_spans: int = 5, mq_pad_minutes: int = 2):
        if min(before_minutes, after_minutes, max_before_minutes) < 0 or max_spans <= 0 or min_entry_spans < 1:
            raise ValueError("before／after／max_before_minutes 不能小於 0；max_spans 必須大於 0；min_entry_spans 至少 1")
        if max_before_minutes and max_before_minutes < before_minutes:
            raise ValueError("max_before_minutes 要大於等於 before_minutes（0 = 不擴展）")
        if not isinstance(follow_messaging, bool):
            raise ValueError("follow_messaging 只能是 true／false")
        if max_lag_minutes <= 0 or max_linked_traces <= 0 or min_consumer_spans < 1 or mq_pad_minutes < 0:
            raise ValueError("max_lag_minutes、max_linked_traces 必須大於 0；min_consumer_spans 至少 1；mq_pad_minutes 不能小於 0")
        self.follow_messaging = follow_messaging
        self.max_lag, self.mq_pad = timedelta(minutes=max_lag_minutes), timedelta(minutes=mq_pad_minutes)
        self.max_linked_traces, self.min_consumer_spans = int(max_linked_traces), int(min_consumer_spans)
        self.source_type = source_type
        self.before, self.after = timedelta(minutes=before_minutes), timedelta(minutes=after_minutes)
        self.max_spans = int(max_spans)
        self.only_incident_services = bool(only_incident_services)
        self.max_before = timedelta(minutes=max_before_minutes) if max_before_minutes else None
        self.min_entry_spans = int(min_entry_spans)

    @staticmethod
    def _record(s: dict, spell) -> SpanRecord:
        return SpanRecord(s["trace_id"], s["span_id"], s.get("parent_id"), spell(s["service"]), s["ts"],
                          float(s.get("duration_ms") or 0.0), bool(s.get("error")),
                          kind=str(s.get("kind") or ""), subtype=str(s.get("subtype") or ""),
                          resource=str(s.get("resource") or ""), status_code=int(s.get("status_code") or 0),
                          name=str(s.get("name") or ""), consumer=bool(s.get("consumer")), queue=str(s.get("queue") or ""),
                          age_ms=s.get("age_ms"), links=tuple((str(t), str(i)) for t, i in s.get("links") or ()),
                          weight=float(s.get("weight") or 1.0), environment=str(s.get("environment") or ""))

    @staticmethod
    def _entry_counts(spans: list[SpanRecord], entities: list[str], lo: datetime, hi: datetime) -> dict[str, int]:
        """[lo, hi) 內每個事件實體的入口 span 數（沒有父、父不是同一個服務、或是訊息消費者）。跟 stages/rca/spans 同一個定義，但 ingestion 不 import rca。"""
        service_of = {s.span_id: s.service for s in spans}
        counts = {e: 0 for e in entities}
        for s in spans:
            if s.service in counts and lo <= as_utc(s.ts) < hi and (s.parent_id is None or service_of.get(s.parent_id) != s.service or s.consumer):
                counts[s.service] += 1
        return counts

    def _short_entities(self, spans, entities, inc_start, used, end) -> list[str]:
        before = self._entry_counts(spans, entities, inc_start - used, inc_start)
        during = self._entry_counts(spans, entities, inc_start, end)
        return [f"{e} {before[e]} 筆" for e in entities if before[e] < self.min_entry_spans <= during[e]]

    def _skip(self, ctx: PipelineContext, why: str, as_error: bool) -> PipelineContext:
        ctx.log(f"incident_traces:skip:{why}")
        ctx.result["trace_fetch"] = {"source_type": self.source_type, "spans": 0, "skipped": why}
        if as_error:
            ctx.rca_traces_failed = True  # 有設定卻撈不到：根因算子要知道證據是被吃掉的，不是本來就沒有
            ctx.errors.append(TargetError(f"node:{self.node_name or 'incident_traces'}", f"撈 trace 失敗，拓撲只用既有的：{why}"))
        return ctx

    def _follow_messaging(self, ctx: PipelineContext, spec, spans: list[SpanRecord], entities: list[str], spell,
                          services: list[str] | None, inc_start: datetime, start: datetime, end: datetime,
                          used: timedelta) -> tuple[list[SpanRecord], dict]:
        """訊息佇列跨時窗（見類別說明）。回 (併好補撈結果的 span, 寫進 trace_fetch.mq 的摘要)。"""
        hi = end  # execute 已經把最後一點那一分鐘算進 end
        consumers = consumer_entries(spans, entities, inc_start, hi)
        info: dict = {"consumers": len(consumers), "linked_traces": 0, "extra_spans": 0, "queries": 0, "edges": 0}
        if len(consumers) < self.min_consumer_spans:
            ctx.log(f"incident_traces:mq:skip:consumers={len(consumers)}<{self.min_consumer_spans}")
            return spans, info
        lag_minutes = int(self.max_lag.total_seconds() // 60)
        age = age_p95_ms(consumers)
        if age is not None and age > self.max_lag.total_seconds() * 1000:
            ctx.rca_mq_note = (f"訊息延遲超過上限：消費者記的訊息年齡 P95 約 {age / 60000:.0f} 分鐘，超過 max_lag_minutes（{lag_minutes}），"
                               "不往回撈生產者那段時間的 trace；請直接查佇列積壓與生產者當時的狀況")
            ctx.log("incident_traces:mq:over-max-lag")
            return spans, {**info, "skipped": "over_max_lag", "age_p95_minutes": round(age / 60000, 1)}
        budget = self.max_spans
        seen = {s.span_id for s in spans}

        def merge(raw: list[dict]) -> int:
            fresh = [self._record(s, spell) for s in raw if s["span_id"] not in seen]
            seen.update(s.span_id for s in fresh)
            spans.extend(fresh)
            return len(fresh)

        wanted = unresolved_trace_ids(spans, consumers)
        if wanted:
            picked = spread(wanted, self.max_linked_traces)
            raw, cut = spec.fetch_linked_spans(picked, inc_start - self.max_lag, end, budget, **_field_map_kw(ctx, self.source_type))
            added = merge(raw)
            budget -= added
            info.update(linked_traces=len(picked), linked_candidates=len(wanted), extra_spans=added,
                        queries=1, truncated=bool(cut))
        base_start = inc_start - used
        edges, _, unresolved = build_edges(spans, entities, inc_start, hi, base_start, self.mq_pad)
        if not edges:
            ctx.rca_mq_note = ("偵測到 messaging consumer，但沒有 span link、parent 或 message.age，無法跨時窗"
                               f"（事件期間 {len(consumers)} 筆消費者入口都對不到發布時間；往回只找 {lag_minutes} 分鐘）")
            ctx.log("incident_traces:mq:unlinked")
            return spans, {**info, "unresolved": unresolved}
        # 生產者時窗＋前面等長的基準早於主撈取窗的部分：補撈所有服務，生產者的自身耗時才扣得掉它等下游的時間
        known = [e for e in edges if e.producer]
        if known and budget > 0:
            floor = inc_start - self.max_lag - used
            need_lo = max(min(e.pub_start - (e.pub_end - e.pub_start) for e in known), floor)
            need_hi = min(max(e.pub_end for e in known), start)
            if need_lo < need_hi:
                raw, cut = spec.fetch_spans(need_lo, need_hi, services, budget, **_scope_kw(ctx, self.source_type))
                raw = in_environment(raw, _rca_environment(ctx))
                added = merge(raw)
                info.update(extra_spans=info["extra_spans"] + added, queries=info["queries"] + 1,
                            truncated=bool(info.get("truncated") or cut),
                            pub_fetch=[need_lo.isoformat(), need_hi.isoformat()])
                edges, _, unresolved = build_edges(spans, entities, inc_start, hi, base_start, self.mq_pad)
        ctx.rca_mq = edges
        info.update(edges=len(edges), unresolved=unresolved)
        ctx.log(f"incident_traces:mq:{len(edges)}edges:+{info['extra_spans']}spans")
        return spans, info

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not ctx.incidents:
            raise NotFound("沒有可以撈 trace 的事件群")
        incident = ctx.incidents[0]
        if not is_configured(self.source_type, "traces"):
            return self._skip(ctx, f"{self.source_type} 沒有設定 trace 來源", as_error=False)
        inc_start = as_utc(incident.start)
        start, end = inc_start - self.before, as_utc(incident.end) + EVENT_BUCKET + self.after
        services = sorted(set(incident.entities)) if self.only_incident_services else None
        entities = sorted(set(incident.entities))
        spell = _spelled_like_entities(incident.entities)
        used, extended_for = self.before, ""
        try:
            spec = get_datasource_spec(self.source_type)
            raw, truncated = spec.fetch_spans(start, end, services, self.max_spans, **_scope_kw(ctx, self.source_type))
            raw = in_environment(raw, _rca_environment(ctx))
            spans = [self._record(s, spell) for s in raw]
            # 基準窗自適應回溯：入口請求不夠就往前加倍撈，只撈多出來的前段，窗口連續
            while self.max_before is not None and used < self.max_before and len(spans) < self.max_spans:
                short = self._short_entities(spans, entities, inc_start, used, end)
                if not short:
                    break
                new = min(used * 2, self.max_before)
                extra, cut = spec.fetch_spans(inc_start - new, inc_start - used, services, self.max_spans - len(spans),
                                              **_scope_kw(ctx, self.source_type))
                extra = in_environment(extra, _rca_environment(ctx))
                seen = {s.span_id for s in spans}
                spans = [self._record(s, spell) for s in extra if s["span_id"] not in seen] + spans
                truncated = truncated or cut
                extended_for = extended_for or f"{'、'.join(short)}（門檻 {self.min_entry_spans} 筆）"
                ctx.log(f"incident_traces:extend:{int(used.total_seconds() // 60)}->{int(new.total_seconds() // 60)}:{';'.join(short)}")
                used, start = new, inc_start - new
        except (InfrastructureError, BadRequest, NotFound) as e:
            return self._skip(ctx, str(e), as_error=True)
        except Exception as e:  # noqa: BLE001  旁路佐證：資料長得出乎意料也只略過這一段，不能讓整份報告失敗
            logger.warning("incident_traces 解析失敗，略過拓撲佐證：%s: %s", type(e).__name__, e)
            return self._skip(ctx, f"{type(e).__name__}: {e}", as_error=True)
        mq = None
        if self.follow_messaging:
            main = list(spans)
            main_first = min((as_utc(s.ts) for s in main), default=None)
            try:
                spans, mq = self._follow_messaging(ctx, spec, spans, entities, spell, services, inc_start, start, end, used)
            except Exception as e:  # noqa: BLE001  旁路的旁路：補撈失敗只少了生產者那一段，主撈取的 span 照用
                why = str(e) if isinstance(e, (InfrastructureError, BadRequest, NotFound)) else f"{type(e).__name__}: {e}"
                logger.warning("incident_traces 訊息佇列跨時窗失敗，只用事件時窗的 span：%s", why)
                spans, mq = main, {"skipped": why}
                ctx.rca_mq = []
                ctx.rca_mq_note = f"訊息佇列跨時窗撈取失敗，沒有回到生產者發布時段比對：{why}"
                ctx.errors.append(TargetError(f"node:{self.node_name or 'incident_traces'}",
                                              f"訊息佇列跨時窗撈取失敗，根因只看事件時窗：{why}"))
            if len(spans) > len(main):  # 補撈了更早的 span：事件前基準仍從主撈取窗算起
                ctx.rca_main_start = main_first
        ctx.rca_spans = spans
        before_min, used_min = int(self.before.total_seconds() // 60), int(used.total_seconds() // 60)
        ctx.rca_trace_lookback = (before_min, used_min)
        ctx.result["trace_fetch"] = {"source_type": self.source_type, "spans": len(ctx.rca_spans),
                                     "truncated": truncated, "start": start.isoformat(), "end": end.isoformat(),
                                     "services": sorted({s.service for s in ctx.rca_spans}),
                                     "before_minutes": before_min, "before_minutes_used": used_min, "extended_for": extended_for}
        if mq is not None:
            ctx.result["trace_fetch"]["mq"] = mq
        ctx.log(f"incident_traces:{len(ctx.rca_spans)}{':truncated' if truncated else ''}")
        return ctx


@OperatorRegistry.register("ingestion.incident_logs")
class IncidentLogsFetch(BaseOperator):
    """RCA 用：隨需撈事件期間與事件前正常期間的錯誤日誌，給 rca.section.log_patterns 比對「哪種例外暴增」。

    事件期間 = [事件開始 - before, 事件結束 + after]；正常期間 = 事件期間開始前 baseline_minutes 分鐘（不重疊）。
    只撈事件裡的服務（日誌量大，撈全部沒意義）。跟 incident_traces 一樣是旁路佐證：
    資料源沒設定 -> 略過；連不上、查詢錯 -> 記進 errors 並略過，報告照出。
    params: source_type（預設 grafana）、before_minutes（預設 5）、after_minutes（預設 5）、
            baseline_minutes（預設 60）、max_logs（每段上限，預設 5000）。
    """

    scope = "batch"

    def __init__(self, source_type: str = "grafana", before_minutes: int = 5, after_minutes: int = 5,
                 baseline_minutes: int = 60, max_logs: int = 5000):
        if min(before_minutes, after_minutes) < 0 or baseline_minutes <= 0 or max_logs <= 0:
            raise ValueError("before／after_minutes 不能小於 0；baseline_minutes、max_logs 必須大於 0")
        self.source_type = source_type
        self.before, self.after = timedelta(minutes=before_minutes), timedelta(minutes=after_minutes)
        self.baseline = timedelta(minutes=baseline_minutes)
        self.max_logs = int(max_logs)

    def _skip(self, ctx: PipelineContext, why: str, as_error: bool) -> PipelineContext:
        ctx.log(f"incident_logs:skip:{why}")
        ctx.result["log_fetch"] = {"source_type": self.source_type, "logs": 0, "skipped": why}
        if as_error:
            ctx.errors.append(TargetError(f"node:{self.node_name or 'incident_logs'}", f"撈日誌失敗，報告不含日誌比對：{why}"))
        return ctx

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not ctx.incidents:
            raise NotFound("沒有可以撈日誌的事件群")
        incident = ctx.incidents[0]
        if not is_configured(self.source_type, "logs"):
            return self._skip(ctx, f"{self.source_type} 沒有設定日誌來源", as_error=False)
        start, end = as_utc(incident.start) - self.before, as_utc(incident.end) + self.after
        base_start = start - self.baseline
        services = sorted(set(incident.entities))
        spell = _spelled_like_entities(incident.entities)

        def record(r):
            return LogRecord(r["ts"], spell(r["service"]), r["message"], r.get("error_type", ""), r.get("trace_id", ""),
                             float(r.get("weight", 1.0)))

        try:
            spec = get_datasource_spec(self.source_type)
            fm = _scope_kw(ctx, self.source_type)
            during, cut1 = spec.fetch_logs(start, end, services, self.max_logs, **fm)
            before, cut2 = spec.fetch_logs(base_start, start, services, self.max_logs, **fm)
            during, before = in_environment(during, _rca_environment(ctx)), in_environment(before, _rca_environment(ctx))
            logs = [record(r) for r in during]
            # 正常期間的查詢區間跟事件期間在 start 那一刻相接，剛好落在 start 的那筆歸事件期間
            baseline = [record(r) for r in before if as_utc(r["ts"]) < start]
        except (InfrastructureError, BadRequest, NotFound) as e:
            return self._skip(ctx, str(e), as_error=True)
        except Exception as e:  # noqa: BLE001  旁路佐證：日誌長得出乎意料也只略過日誌段落，不能讓整份 RCA 報告失敗
            logger.warning("incident_logs 解析失敗，略過日誌比對：%s: %s", type(e).__name__, e)
            return self._skip(ctx, f"{type(e).__name__}: {e}", as_error=True)
        ctx.rca_logs, ctx.rca_logs_baseline = logs, baseline
        ctx.rca_logs_sampled = bool(cut1 or cut2)
        ctx.rca_log_window_minutes = ((end - start).total_seconds() / 60, self.baseline.total_seconds() / 60)
        ctx.result["log_fetch"] = {"source_type": self.source_type, "logs": len(ctx.rca_logs),
                                   "baseline_logs": len(ctx.rca_logs_baseline), "truncated": cut1 or cut2,
                                   "window": [start.isoformat(), end.isoformat()],
                                   "baseline_window": [base_start.isoformat(), start.isoformat()]}
        ctx.log(f"incident_logs:{len(ctx.rca_logs)}+{len(ctx.rca_logs_baseline)}baseline")
        return ctx


def grafana_targets(pipeline: str) -> list[dict[str, Any]]:
    """Workflow.targets_from: grafana 的偵測對象：Grafana 上標了 aiops_detect: <pipeline> 的告警規則。
    回傳 TargetSpec 的欄位（由組裝點 interfaces/runner 建成 TargetSpec）。Grafana 沒設定或連不上丟 InfrastructureError：
    不知道要看哪些序列時，這一輪要失敗給人看，不能當成「沒有對象、沒事」。"""
    return get_datasource_spec("grafana").detection_targets(pipeline)
