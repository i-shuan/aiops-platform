import pandas as pd

from aiops.commands.base import BaseCommand
from aiops.domains.ingestion.schemas import TraceMergeRequest


class MergeTracesCommand(BaseCommand):
    """把 span 轉成每個服務的標準時序（count、error_rate、p95_ms）。

    trace 不另開一套 train / detect：轉完的時序跟一般指標一樣交給 Detector 外掛。
    """

    def __init__(self, req: TraceMergeRequest):
        self.req = req

    def validate(self) -> None:
        pass  # span 形狀已由 schema 驗證

    def run(self) -> list[dict]:
        self.validate()
        df = pd.DataFrame([s.model_dump() for s in self.req.spans])
        df["ts"] = pd.to_datetime(df["start"]).dt.floor(f"{self.req.step_seconds}s")
        out = (
            df.groupby(["service", "ts"])
            .agg(
                count=("trace_id", "size"),
                error_rate=("error", "mean"),
                p95_ms=("duration_ms", lambda s: s.quantile(0.95)),
            )
            .reset_index()
        )
        return [
            {
                "service": r.service,
                "ts": r.ts.isoformat(),
                "count": int(r.count),
                "error_rate": round(float(r.error_rate), 4),
                "p95_ms": round(float(r.p95_ms), 2),
            }
            for r in out.itertuples()
        ]
