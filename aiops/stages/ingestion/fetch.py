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

from datetime import datetime, timedelta
from string import Template

import pandas as pd

from aiops.engine.context import LogRecord, PipelineContext, SpanRecord, TargetError
from aiops.engine.errors import BadRequest, InfrastructureError, NotFound
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import as_utc
from aiops.sources import get_datasource_spec, is_configured
from aiops.sources.query.trace import parse_trace_query
from aiops.sources.query.window import align_window, to_utc
from aiops.engine.operator import BaseOperator

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
        raise NotFound(f"{TRACE_SOURCE} 在這段時間沒有 {service} 的資料")
    return pd.DataFrame({"ts": pd.to_datetime([t for t, _ in points], utc=True), "value": [v for _, v in points]})


def fetch_series(ctx: PipelineContext, source_type: str, query: str, start: datetime, end: datetime,
                 step: int) -> pd.DataFrame:
    """抓一條序列 -> DataFrame(ts, value)。"""
    if source_type == TRACE_SOURCE:
        return _trace_frame(ctx, query, start, end, step)
    return get_datasource_spec(source_type).fetch_data(query, start, end, step, ctx.field_maps.get(source_type))


def _group(ctx: PipelineContext, start: datetime, end: datetime, step: int) -> tuple[list[str], list[pd.DataFrame]]:
    """動態同儕：撈回整群、找出自己、回傳 (其他成員名單, 其他成員的序列)。"""
    t = ctx.target
    assert t is not None and t.peer_group is not None
    g = t.peer_group
    source_type = g.source_type or t.source_type
    if source_type == TRACE_SOURCE:
        raise BadRequest("trace 資料源不支援 peer_group，請改用 peers")
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
        raise NotFound(f"peer_group 只撈到自己（{g.by}={me}），沒有同儕可以比")
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
        if lookback_minutes <= 0 or step_seconds <= 0:
            raise ValueError("lookback_minutes、step_seconds 必須大於 0")
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
        if not self.train_lookback_minutes:
            return False
        if not ctx.detector_states:
            return True
        age = timedelta(minutes=self.retrain_minutes)
        return any(as_utc(ctx.now) - st.trained_at >= age for st in ctx.detector_states.values())

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

    外部匯入或舊版沒記資料源的異常略過；某條抓不到（沒資料、查詢錯）只記在 audit 並略過，
    資料源連不上（InfrastructureError）照常往上丟（要不阻斷報告就在 YAML 把這個節點標 optional）。
    params: before_minutes、after_minutes、step_seconds、max_series（避免一個大事件群把資料源打爆）。
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
        for source_type, metric, entity in wanted:
            try:
                ctx.rca_series[(entity, metric)] = fetch_series(ctx, source_type, metric, start, end, self.step)
            except (NotFound, BadRequest) as e:
                ctx.log(f"incident_series:skip:{entity}:{e}")
        return ctx


@OperatorRegistry.register("ingestion.incident_traces")
class IncidentTracesFetch(BaseOperator):
    """RCA 用：隨需撈 [事件開始 - before, 事件結束 + after] 的 span 放進 ctx.rca_spans。

    只在跑 RCA 報告時執行，日常偵測不碰 trace。這是旁路佐證，撈不到不該讓整份報告失敗：
    資料源沒設定 -> 略過（記在 audit）；連不上、查詢錯 -> 記進 errors 並略過，報告照出（拓撲就只用既有的）。
    params: source_type（預設 elasticsearch）、before_minutes（預設 15）、after_minutes（預設 0）、
            max_spans（上限，預設 20000，超過就截斷並註記）、only_incident_services（預設 false：撈這段時間所有服務，
            才看得到事件實體跟沒出事的服務之間的關係；true 只撈事件裡的服務，量小但可能漏掉中間的服務）。
    """

    scope = "batch"

    def __init__(self, source_type: str = "elasticsearch", before_minutes: int = 15, after_minutes: int = 0,
                 max_spans: int = 20000, only_incident_services: bool = False):
        if min(before_minutes, after_minutes) < 0 or max_spans <= 0:
            raise ValueError("before／after_minutes 不能小於 0；max_spans 必須大於 0")
        self.source_type = source_type
        self.before, self.after = timedelta(minutes=before_minutes), timedelta(minutes=after_minutes)
        self.max_spans = int(max_spans)
        self.only_incident_services = bool(only_incident_services)

    def _skip(self, ctx: PipelineContext, why: str, as_error: bool) -> PipelineContext:
        ctx.log(f"incident_traces:skip:{why}")
        ctx.result["trace_fetch"] = {"source_type": self.source_type, "spans": 0, "skipped": why}
        if as_error:
            ctx.errors.append(TargetError(f"node:{self.node_name or 'incident_traces'}", f"撈 trace 失敗，拓撲只用既有的：{why}"))
        return ctx

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if not ctx.incidents:
            raise NotFound("沒有可以撈 trace 的事件群")
        incident = ctx.incidents[0]
        if not is_configured(self.source_type):
            return self._skip(ctx, f"{self.source_type} 沒有設定", as_error=False)
        start, end = as_utc(incident.start) - self.before, as_utc(incident.end) + self.after
        services = sorted(set(incident.entities)) if self.only_incident_services else None
        try:
            raw, truncated = get_datasource_spec(self.source_type).fetch_spans(start, end, services, self.max_spans)
        except (InfrastructureError, BadRequest, NotFound) as e:
            return self._skip(ctx, str(e), as_error=True)
        ctx.rca_spans = [SpanRecord(s["trace_id"], s["span_id"], s.get("parent_id"), s["service"], s["ts"],
                                    float(s.get("duration_ms") or 0.0), bool(s.get("error"))) for s in raw]
        ctx.result["trace_fetch"] = {"source_type": self.source_type, "spans": len(ctx.rca_spans),
                                     "truncated": truncated, "start": start.isoformat(), "end": end.isoformat(),
                                     "services": sorted({s.service for s in ctx.rca_spans})}
        ctx.log(f"incident_traces:{len(ctx.rca_spans)}{':truncated' if truncated else ''}")
        return ctx


@OperatorRegistry.register("ingestion.incident_logs")
class IncidentLogsFetch(BaseOperator):
    """RCA 用：隨需撈事件期間與事件前正常期間的錯誤日誌，給 rca.section.log_patterns 比對「哪種例外暴增」。

    事件期間 = [事件開始 - before, 事件結束 + after]；正常期間 = 事件期間開始前 baseline_minutes 分鐘（不重疊）。
    只撈事件裡的服務（日誌量大，撈全部沒意義）。跟 incident_traces 一樣是旁路佐證：
    資料源沒設定 -> 略過；連不上、查詢錯 -> 記進 errors 並略過，報告照出。
    params: source_type（預設 elasticsearch）、before_minutes（預設 5）、after_minutes（預設 5）、
            baseline_minutes（預設 60）、max_logs（每段上限，預設 5000）。
    """

    scope = "batch"

    def __init__(self, source_type: str = "elasticsearch", before_minutes: int = 5, after_minutes: int = 5,
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
        if not is_configured(self.source_type):
            return self._skip(ctx, f"{self.source_type} 沒有設定", as_error=False)
        start, end = as_utc(incident.start) - self.before, as_utc(incident.end) + self.after
        base_start = start - self.baseline
        services = sorted(set(incident.entities))
        try:
            spec = get_datasource_spec(self.source_type)
            during, cut1 = spec.fetch_logs(start, end, services, self.max_logs)
            before, cut2 = spec.fetch_logs(base_start, start, services, self.max_logs)
        except (InfrastructureError, BadRequest, NotFound) as e:
            return self._skip(ctx, str(e), as_error=True)
        ctx.rca_logs = [LogRecord(r["ts"], r["service"], r["message"], r.get("error_type", ""), r.get("trace_id", ""))
                        for r in during]
        # 正常期間的查詢區間跟事件期間在 start 那一刻相接，剛好落在 start 的那筆歸事件期間
        ctx.rca_logs_baseline = [LogRecord(r["ts"], r["service"], r["message"], r.get("error_type", ""),
                                           r.get("trace_id", "")) for r in before if as_utc(r["ts"]) < start]
        ctx.rca_log_window_minutes = ((end - start).total_seconds() / 60, self.baseline.total_seconds() / 60)
        ctx.result["log_fetch"] = {"source_type": self.source_type, "logs": len(ctx.rca_logs),
                                   "baseline_logs": len(ctx.rca_logs_baseline), "truncated": cut1 or cut2,
                                   "window": [start.isoformat(), end.isoformat()],
                                   "baseline_window": [base_start.isoformat(), start.isoformat()]}
        ctx.log(f"incident_logs:{len(ctx.rca_logs)}+{len(ctx.rca_logs_baseline)}baseline")
        return ctx
