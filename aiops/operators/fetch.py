"""抓資料的算子。整個 operators/ 只有這個檔案可以碰 datasource_specs（tests/test_architecture.py 檢查）。

- ingestion.fetch：依 ctx.target 抓這一輪要偵測的資料放進 ctx.raw_df（需要重訓時另外抓較長的歷史放進 ctx.train_df）
  - 同儕：peers 寫死的名單 -> peer_0、peer_1…；peer_group 依標籤動態聚合整群 -> 接在後面，成員名單記在 ctx.peer_members
  - lags_minutes：同一時刻往前 N 分鐘的值 -> lag_<N> 欄位（給同比、環比偵測器）
- ingestion.incident_series：RCA 用，依事件群裡每筆異常的 (資料源, 指標) 重抓事件時段的原始時序 -> ctx.rca_series

外部資料源走 datasource_specs，已核准的欄位對照由引擎載入 ctx.field_maps。
source_type=trace：平台自己合成並存起來的 trace 時序（引擎載入 ctx.trace_rows），query 寫「服務/欄位」。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from string import Template

import pandas as pd

from aiops.core.context import PipelineContext
from aiops.core.errors import BadRequest, NotFound
from aiops.core.registry import OperatorRegistry
from aiops.core.types import as_utc
from aiops.datasource_specs import get_datasource_spec
from aiops.metric_query.trace import parse_trace_query
from aiops.metric_query.window import align_window, to_utc
from aiops.operators.base import BaseOperator

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
