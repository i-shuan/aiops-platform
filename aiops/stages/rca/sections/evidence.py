"""佐證段落：trace 合成出的錯誤率與延遲。"""
from __future__ import annotations

from datetime import timedelta

from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import as_utc
from aiops.stages.rca.base import RcaSection

_TRACE_MARGIN = timedelta(minutes=10)


def _trace_rows(incident, ctx):
    start, end = as_utc(incident.start) - _TRACE_MARGIN, as_utc(incident.end) + _TRACE_MARGIN
    return [r for s in incident.entities for r in ctx.trace_rows.get(s, []) if start <= as_utc(r.ts) <= end]


@OperatorRegistry.register("rca.section.trace_evidence")
class TraceEvidence(RcaSection):
    """IT 場景：用 trace 合成出的每服務錯誤率與 p95 佐證。載入的 trace 時序沒有相關資料就略過。"""

    key, title = "trace_evidence", "Trace 佐證"

    def applies(self, incident, ctx):
        return bool(_trace_rows(incident, ctx))

    def build(self, incident, ctx):
        by_service: dict[str, list] = {}
        for r in _trace_rows(incident, ctx):
            by_service.setdefault(r.service, []).append(r)
        lines = []
        for service in sorted(by_service):
            rows = by_service[service]
            worst_err = max(rows, key=lambda r: r.error_rate)
            worst_p95 = max(rows, key=lambda r: r.p95_ms)
            lines.append(f"{service}：請求 {sum(r.count for r in rows)} 次；"
                         f"錯誤率最高 {worst_err.error_rate:.0%}（{worst_err.ts.isoformat()}）；"
                         f"p95 最高 {worst_p95.p95_ms:.0f} ms（{worst_p95.ts.isoformat()}）")
        return "\n".join(lines)
