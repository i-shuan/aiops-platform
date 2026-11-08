"""所有場景都適用的報告段落。"""
from collections import Counter

from aiops.plugins.base import BaseSectionBuilder, Incident, ReportSection
from aiops.plugins.registry import SectionRegistry


@SectionRegistry.register
class SummarySection(BaseSectionBuilder):
    name = "summary"
    title = "摘要"

    def build(self, incident: Incident, context: dict) -> ReportSection:
        body = (
            f"{len(incident.entities)} 個實體、{len(incident.events)} 筆異常，"
            f"{incident.start.isoformat()} ~ {incident.end.isoformat()}，分數 {incident.score:.2f}"
        )
        if incident.tags:
            body += f"；收斂方式：{', '.join(incident.tags)}"
        return ReportSection(self.name, self.title, body)


@SectionRegistry.register
class TimelineSection(BaseSectionBuilder):
    name = "timeline"
    title = "時間軸"

    def build(self, incident: Incident, context: dict) -> ReportSection:
        lines = [f"{e.ts.isoformat()} {e.entity} {e.metric} score={e.score:.2f}" for e in incident.events]
        return ReportSection(self.name, self.title, "\n".join(lines))


@SectionRegistry.register
class AffectedEntitiesSection(BaseSectionBuilder):
    name = "affected_entities"
    title = "受影響實體"

    def build(self, incident: Incident, context: dict) -> ReportSection:
        counts = Counter(e.entity for e in incident.events)
        lines = [f"{entity}：{n} 筆" for entity, n in counts.most_common()]
        return ReportSection(self.name, self.title, "\n".join(lines))
