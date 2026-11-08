"""通知內容由平台決定：嚴重度、標題、摘要、路由標籤。接收端只照 labels 分流。"""
from datetime import datetime

from aiops.notifiers.base import Notification
from aiops.plugins.base import Incident

UNASSIGNED_DEPT = "unassigned"


def severity_for(score: float, thresholds: tuple[tuple[str, float], ...]) -> str:
    for level, minimum in sorted(thresholds, key=lambda t: t[1], reverse=True):
        if score >= minimum:
            return level
    return thresholds[-1][0]


def build_notifications(incident_uid: str, incident: Incident, preset: str, severity: str,
                        ends_at: datetime | None = None) -> list[Notification]:
    """一個部門一則，接收端才能用 dept 標籤直接分送；沒帶部門的歸 unassigned。

    對外只用全域唯一的 incident_uid：整數 id 會因資料庫重建從 1 重來，
    拿去當 label 會讓 Alertmanager 把不相干的事件群分在同一組。
    labels 完全由事件群內容決定，所以心跳重送與結案時重建出來的 labels 跟第一次一模一樣，接收端才認得是同一筆。
    """
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
                "incident_uid": incident_uid,
                "severity": severity,
                "preset": preset,
                "dept": dept,
                "entities": entities,
            },
            annotations={"summary": title, "description": description},
            starts_at=incident.start,
            ends_at=ends_at,
        )
        for dept in (incident.depts or [UNASSIGNED_DEPT])
    ]
