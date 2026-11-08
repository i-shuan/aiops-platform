"""通知內容由平台決定：嚴重度、標題、摘要、路由標籤。接收端只照 labels 分流。"""
from aiops.notifiers.base import Notification
from aiops.plugins.base import Incident

UNASSIGNED_DEPT = "unassigned"


def severity_for(score: float, thresholds: tuple[tuple[str, float], ...]) -> str:
    for level, minimum in sorted(thresholds, key=lambda t: t[1], reverse=True):
        if score >= minimum:
            return level
    return thresholds[-1][0]


def build_notifications(incident_id: int, incident: Incident, preset: str, severity: str) -> list[Notification]:
    """一個部門一則，接收端才能用 dept 標籤直接分送；沒帶部門的歸 unassigned。"""
    entities = ", ".join(incident.entities)
    title = f"[{severity}] {entities} 異常（{len(incident.events)} 筆）"
    description = (
        f"{incident.start.isoformat()} ~ {incident.end.isoformat()}，分數 {incident.score:.2f}"
        + (f"；收斂方式：{', '.join(incident.tags)}" if incident.tags else "")
    )
    return [
        Notification(
            labels={
                "alertname": "aiops_incident",
                "incident_id": str(incident_id),
                "severity": severity,
                "preset": preset,
                "dept": dept,
                "entities": entities,
            },
            annotations={"summary": title, "description": description},
            starts_at=incident.start,
        )
        for dept in (incident.depts or [UNASSIGNED_DEPT])
    ]
