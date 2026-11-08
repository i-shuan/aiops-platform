"""ingestion.fetch：依 ctx.target 抓這一輪要偵測的資料（含同儕序列）放進 ctx.raw_df。

- 外部資料源（prometheus／victoriametrics／elasticsearch／mock）走 datasource_specs，已核准的欄位對照由引擎載入 ctx.field_maps
- source_type=trace：平台自己合成並存起來的 trace 時序（引擎載入 ctx.trace_rows），query 寫「服務/欄位」
需要（重新）訓練時，另外抓較長的歷史放進 ctx.train_df。
"""
from __future__ import annotations

from datetime import datetime, timedelta

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


def _series(ctx: PipelineContext, source_type: str, query: str, start: datetime, end: datetime, step: int) -> pd.DataFrame:
    if source_type == TRACE_SOURCE:
        return _trace_frame(ctx, query, start, end, step)
    return get_datasource_spec(source_type).fetch_data(query, start, end, step, ctx.field_maps.get(source_type))


def fetch_target_frame(ctx: PipelineContext, start: datetime, end: datetime, step: int) -> pd.DataFrame:
    """依 ctx.target（含同儕）抓一段資料。抓資料與調參回測共用同一條路徑，回測才跟實際偵測用一樣的資料。"""
    t = ctx.target
    assert t is not None
    df = _series(ctx, t.source_type, t.query, start, end, step)
    for i, peer in enumerate(t.peers):
        p = _series(ctx, peer.source_type, peer.query, start, end, step)
        df = df.merge(p.rename(columns={"value": f"peer_{i}"}), on="ts", how="left")
    return df


@OperatorRegistry.register("ingestion.fetch")
class FetchOperator(BaseOperator):
    def __init__(self, lookback_minutes: int = 60, step_seconds: int = 60,
                 train_lookback_minutes: int | None = None, retrain_minutes: int = 1440):
        if lookback_minutes <= 0 or step_seconds <= 0:
            raise ValueError("lookback_minutes、step_seconds 必須大於 0")
        self.lookback_minutes = lookback_minutes
        self.step_seconds = step_seconds
        self.train_lookback_minutes = train_lookback_minutes
        self.retrain_minutes = retrain_minutes

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
        ctx.raw_df = fetch_target_frame(ctx, end - timedelta(minutes=self.lookback_minutes), end, self.step_seconds)
        if self._needs_training(ctx):
            ctx.train_df = fetch_target_frame(ctx, end - timedelta(minutes=self.train_lookback_minutes), end,
                                              self.step_seconds)
        return ctx
