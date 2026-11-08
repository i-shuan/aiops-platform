"""概況段落：摘要、時間軸、受影響實體、黃金訊號。只看事件群本身，任何場景都適用（黃金訊號除外）。"""
from __future__ import annotations

from collections import Counter

from aiops.core.registry import OperatorRegistry
from aiops.core.types import Incident
from aiops.operators.rca.base import RcaSection

GOLDEN_SIGNALS = ("latency", "errors", "traffic", "saturation")


def golden_events(incident: Incident):
    return [e for e in incident.events if e.metric in GOLDEN_SIGNALS or e.attrs.get("signal") in GOLDEN_SIGNALS]


@OperatorRegistry.register("rca.section.summary")
class Summary(RcaSection):
    key, title = "summary", "摘要"

    def build(self, incident, ctx):
        body = (f"{len(incident.entities)} 個實體、{len(incident.events)} 筆異常，"
                f"{incident.start.isoformat()} ~ {incident.end.isoformat()}，分數 {incident.score:.2f}")
        if incident.tags:
            body += f"；收斂方式：{', '.join(incident.tags)}"
        return body


@OperatorRegistry.register("rca.section.timeline")
class Timeline(RcaSection):
    key, title = "timeline", "時間軸"

    def build(self, incident, ctx):
        return "\n".join(f"{e.ts.isoformat()} {e.entity} {e.metric} score={e.score:.2f}"
                         + (f" [{e.detector_node or e.detector}]" if e.detector else "")
                         for e in incident.events)


@OperatorRegistry.register("rca.section.affected_entities")
class AffectedEntities(RcaSection):
    key, title = "affected_entities", "受影響實體"

    def build(self, incident, ctx):
        counts = Counter(e.entity for e in incident.events)
        return "\n".join(f"{entity}：{n} 筆" for entity, n in counts.most_common())


@OperatorRegistry.register("rca.section.golden_signals")
class GoldenSignals(RcaSection):
    """IT 場景專用：只有事件群真的含黃金訊號時才產生。"""

    key, title = "golden_signals", "黃金訊號"

    def applies(self, incident, ctx):
        return bool(golden_events(incident))

    def build(self, incident, ctx):
        return "\n".join(f"{e.entity} {e.attrs.get('signal', e.metric)} score={e.score:.2f}"
                         for e in golden_events(incident))
