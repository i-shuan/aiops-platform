"""通知內容由平台決定：嚴重度、標題、摘要、路由標籤。接收端（Alertmanager）只照 labels 分流。

labels 完全由事件群內容決定，所以心跳重送與結案時重建出來的 labels 跟第一次一模一樣，接收端才認得是同一筆。
對外只用全域唯一的 incident_uid，絕不放整數 id（整數 id 在資料庫重建後會重複）。
"""
from __future__ import annotations

from datetime import datetime

from aiops.engine.context import PipelineContext
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import Incident, Notification
from aiops.stages._common import ensure_incidents
from aiops.engine.operator import BaseOperator

UNASSIGNED_DEPT = "unassigned"
DEFAULT_SEVERITY = {"P0": 10.0, "P1": 6.0, "P2": 3.0, "P3": 0.0}


def severity_for(score: float, thresholds: dict[str, float]) -> str:
    for level, minimum in sorted(thresholds.items(), key=lambda kv: kv[1], reverse=True):
        if score >= minimum:
            return level
    return min(thresholds, key=thresholds.get)


def build_notifications(incident: Incident, preset: str, severity: str, ends_at: datetime | None = None) -> list[Notification]:
    """一個部門一則，接收端才能用 dept 標籤直接分送；沒帶部門的歸 unassigned。"""
    entities = ", ".join(incident.entities)
    title = f"[{severity}] {entities} 異常（{len(incident.events)} 筆）"
    description = (
        f"{incident.start.isoformat()} ~ {incident.end.isoformat()}，分數 {incident.score:.2f}"
        + (f"；收斂方式：{', '.join(incident.tags)}" if incident.tags else "")
    )
    return [
        Notification(
            labels={"alertname": "aiops_incident", "incident_uid": incident.uid, "severity": severity,
                    "preset": preset, "dept": dept, "entities": entities},
            annotations={"summary": title, "description": description},
            starts_at=incident.start,
            ends_at=ends_at,
        )
        for dept in (incident.depts or [UNASSIGNED_DEPT])
    ]


@OperatorRegistry.register("alert.notify_payload")
class NotifyPayload(BaseOperator):
    """替每個事件群定嚴重度，並為沒被抑制的事件群產生通知（放進 ctx.notifications，由 notifier 算子送出）。
    params: severity_thresholds（分數 >= 門檻就是該等級，例如 {P0: 10, P1: 6, P2: 3, P3: 0}）。
    """

    scope = "batch"

    def __init__(self, severity_thresholds: dict[str, float] | None = None):
        self.thresholds = {k: float(v) for k, v in (severity_thresholds or DEFAULT_SEVERITY).items()}

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        for inc in ensure_incidents(ctx):
            inc.severity = severity_for(inc.score, self.thresholds)
            if not inc.suppressed_by:
                ctx.notifications.extend(build_notifications(inc, ctx.pipeline, inc.severity))
        return ctx
