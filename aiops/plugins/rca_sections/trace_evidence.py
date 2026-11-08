from datetime import datetime, timedelta

from aiops.plugins.base import BaseSectionBuilder, Incident, ReportSection, as_utc
from aiops.plugins.registry import SectionRegistry

_MARGIN = timedelta(minutes=10)


def _rows(incident: Incident, context: dict) -> list[dict]:
    """事件群時間前後 10 分鐘內、屬於事件群實體的 trace 時序（MergeTracesCommand 的輸出格式）。"""
    start, end = as_utc(incident.start) - _MARGIN, as_utc(incident.end) + _MARGIN
    entities = set(incident.entities)
    return [
        r for r in context.get("trace_series", [])
        if r.get("service") in entities and start <= as_utc(datetime.fromisoformat(r["ts"])) <= end
    ]


@SectionRegistry.register
class TraceEvidenceSection(BaseSectionBuilder):
    """IT 場景：用 trace 合成出的每服務錯誤率與 p95 佐證。context["trace_series"] 沒有相關資料就略過。"""

    name = "trace_evidence"
    title = "Trace 佐證"

    def applies(self, incident: Incident, context: dict) -> bool:
        return bool(_rows(incident, context))

    def build(self, incident: Incident, context: dict) -> ReportSection:
        by_service: dict[str, list[dict]] = {}
        for r in _rows(incident, context):
            by_service.setdefault(r["service"], []).append(r)
        lines = []
        for service in sorted(by_service):
            rows = by_service[service]
            worst_err = max(rows, key=lambda r: r["error_rate"])
            worst_p95 = max(rows, key=lambda r: r["p95_ms"])
            lines.append(
                f"{service}：請求 {sum(r['count'] for r in rows)} 次；"
                f"錯誤率最高 {worst_err['error_rate']:.0%}（{worst_err['ts']}）；"
                f"p95 最高 {worst_p95['p95_ms']:.0f} ms（{worst_p95['ts']}）"
            )
        return ReportSection(self.name, self.title, "\n".join(lines))
