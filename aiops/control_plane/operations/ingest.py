"""資料匯入與合成：外部異常接收、trace 合成（含服務呼叫關係）、Lot 履歷合成。"""
from __future__ import annotations

from collections import Counter, defaultdict

import pandas as pd

from aiops.engine.context import PipelineContext
from aiops.engine.errors import BadRequest
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import IngestAnomalies, LotPathsMerge, LotStepIn, SpanIn, TraceMerge
from aiops.engine.topology import lot_edges_from_paths
from aiops.engine.operator import BaseOperator
from aiops.storage.repository import ExternalAnomalies, LotPaths, ServiceEdges, Traces


@OperatorRegistry.register("admin.ingest_anomalies")
class IngestAnomaliesOp(BaseOperator):
    """接收外部系統推來的異常（例如工廠 FDC），先存起來，下一輪 Pipeline 開頭載入、跟偵測到的異常一起進告警流程。
    冪等：同一 source 的 external_id 只收一次，重送只會算成 duplicates。
    """

    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = ctx.request
        if not isinstance(req, IngestAnomalies):
            raise BadRequest("admin.ingest_anomalies 需要 IngestAnomalies 請求")
        ids = [a.external_id for a in req.anomalies]
        if len(ids) != len(set(ids)):
            raise BadRequest("同一批裡 external_id 重複")
        seen = ExternalAnomalies.existing_ids(req.source, ids)
        rows = [{"pipeline": req.pipeline, "source": req.source, **a.model_dump()}
                for a in req.anomalies if a.external_id not in seen]
        ctx.result = {"pipeline": req.pipeline, "source": req.source,
                      "accepted": ExternalAnomalies.create_many(rows), "duplicates": len(seen)}
        return ctx


def _service_edges(spans: list[SpanIn]) -> Counter:
    """父 span 的服務 -> 子 span 的服務 = 一次跨服務呼叫（同服務內的呼叫不算）。"""
    service_of = {s.span_id: s.service for s in spans if s.span_id}
    return Counter((service_of[s.parent_id], s.service) for s in spans
                   if s.parent_id in service_of and service_of[s.parent_id] != s.service)


@OperatorRegistry.register("admin.merge_traces")
class MergeTraces(BaseOperator):
    """把 span 轉成每個服務的標準時序（count、error_rate、p95_ms），並推導服務呼叫關係。

    存起來之後：偵測可用 source_type=trace（query「服務/欄位」）讀；RCA 的 trace 佐證自動載入；
    沒明確給拓撲時，引擎自動用這裡推導出的呼叫關係。
    """

    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = ctx.request
        if not isinstance(req, TraceMerge):
            raise BadRequest("admin.merge_traces 需要 TraceMerge 請求")
        df = pd.DataFrame([s.model_dump() for s in req.spans])
        df["ts"] = pd.to_datetime(df["start"], utc=True).dt.floor(f"{req.step_seconds}s")
        out = (df.groupby(["service", "ts"])
               .agg(count=("trace_id", "size"), error_rate=("error", "mean"),
                    p95_ms=("duration_ms", lambda s: s.quantile(0.95)))
               .reset_index())
        series = [{"service": r.service, "ts": r.ts.to_pydatetime(), "count": int(r.count),
                   "error_rate": round(float(r.error_rate), 4), "p95_ms": round(float(r.p95_ms), 2)}
                  for r in out.itertuples()]
        edges = _service_edges(req.spans)
        if req.persist:
            Traces.upsert_series(series)
            ServiceEdges.add_calls(dict(edges))
        ctx.result = {
            "series": [{**s, "ts": s["ts"].isoformat()} for s in series],
            "edges": [{"caller": a, "callee": b, "calls": n} for (a, b), n in sorted(edges.items())],
            "persisted": req.persist,
        }
        return ctx


def _minutes(step: LotStepIn) -> float:
    return round((step.end - step.start).total_seconds() / 60, 2)


@OperatorRegistry.register("admin.merge_lot_paths")
class MergeLotPaths(BaseOperator):
    """工廠 Lot 履歷合成：每個 Lot 的站點紀錄依時間排成一條路徑，並統計機台流向（lot_edges）。
    存起來之後，引擎在沒明確給 lot_edges 時自動從已存的路徑推導。Lot 流向是 上游 -> 下游，
    跟 IT 的 topology（[caller, callee]）方向相反，所以放 lot_edges，不要塞進 topology。
    """

    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = ctx.request
        if not isinstance(req, LotPathsMerge):
            raise BadRequest("admin.merge_lot_paths 需要 LotPathsMerge 請求")
        by_lot: dict[str, list[LotStepIn]] = defaultdict(list)
        for step in req.steps:
            by_lot[step.lot_id].append(step)
        lots = []
        for lot_id in sorted(by_lot):
            steps = sorted(by_lot[lot_id], key=lambda s: (s.start, s.end))
            end = max(s.end for s in steps)
            lots.append({
                "lot_id": lot_id, "path": [s.equipment for s in steps], "stations": [s.station for s in steps],
                "start": steps[0].start, "end": end,
                "total_minutes": round((end - steps[0].start).total_seconds() / 60, 2),
                # 同一 Lot 前一站還沒結束、下一站就開始：資料品質問題，標出來但不丟掉
                "overlapping_steps": sum(1 for a, b in zip(steps, steps[1:]) if b.start < a.end),
                "steps": [{"station": s.station, "equipment": s.equipment, "recipe": s.recipe,
                           "start": s.start.isoformat(), "end": s.end.isoformat(), "minutes": _minutes(s)}
                          for s in steps],
            })
        if req.persist:
            LotPaths.upsert_many(lots)
        ctx.result = {
            "lots": [{**lot, "start": lot["start"].isoformat(), "end": lot["end"].isoformat()} for lot in lots],
            "lot_edges": [{"from": e.upstream, "to": e.downstream, "lots": e.lots}
                          for e in lot_edges_from_paths([lot["path"] for lot in lots])],
            "persisted": req.persist,
        }
        return ctx
