from collections import Counter

import pandas as pd

from aiops.commands.base import BaseCommand
from aiops.domains.ingestion.dao import ServiceEdgeDAO, TraceSeriesDAO
from aiops.domains.ingestion.schemas import SpanIn, TraceMergeRequest


def _service_edges(spans: list[SpanIn]) -> Counter:
    """父 span 的服務 -> 子 span 的服務 = 一次跨服務呼叫（同服務內的呼叫不算）。"""
    service_of = {s.span_id: s.service for s in spans if s.span_id}
    return Counter(
        (service_of[s.parent_id], s.service)
        for s in spans
        if s.parent_id in service_of and service_of[s.parent_id] != s.service
    )


class MergeTracesCommand(BaseCommand):
    """把 span 轉成每個服務的標準時序（count、error_rate、p95_ms），並推導服務呼叫關係。

    存起來之後：
    - 偵測可以用 source_type=trace、query「服務/欄位」直接讀（trace 跟一般指標走同一個偵測器）
    - RCA 的 trace 佐證段落自動讀，不用呼叫端貼
    - alerts／rca 沒給 topology 時，自動用這裡推導出的呼叫關係
    """

    def __init__(self, req: TraceMergeRequest):
        self.req = req

    def validate(self) -> None:
        pass  # span 形狀已由 schema 驗證

    def run(self) -> dict:
        self.validate()
        df = pd.DataFrame([s.model_dump() for s in self.req.spans])
        df["ts"] = pd.to_datetime(df["start"], utc=True).dt.floor(f"{self.req.step_seconds}s")
        out = (
            df.groupby(["service", "ts"])
            .agg(
                count=("trace_id", "size"),
                error_rate=("error", "mean"),
                p95_ms=("duration_ms", lambda s: s.quantile(0.95)),
            )
            .reset_index()
        )
        series = [
            {"service": r.service, "ts": r.ts.to_pydatetime(), "count": int(r.count),
             "error_rate": round(float(r.error_rate), 4), "p95_ms": round(float(r.p95_ms), 2)}
            for r in out.itertuples()
        ]
        edges = _service_edges(self.req.spans)
        if self.req.persist:
            TraceSeriesDAO.upsert_many(series)
            ServiceEdgeDAO.add_calls(dict(edges))
        return {
            "series": [{**s, "ts": s["ts"].isoformat()} for s in series],
            "edges": [{"caller": a, "callee": b, "calls": n} for (a, b), n in sorted(edges.items())],
            "persisted": self.req.persist,
        }
