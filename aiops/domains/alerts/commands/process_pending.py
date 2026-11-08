from aiops.commands.base import BaseCommand
from aiops.domains.alerts.commands.pipeline import AlertPipeline, check_preset
from aiops.domains.alerts.dao import AlertWatermarkDAO, AnomalyReadDAO
from aiops.domains.alerts.schemas import ProcessPendingRequest
from aiops.plugins.base import AnomalyEvent
from aiops.presets import get_preset


class ProcessPendingAnomaliesCommand(BaseCommand):
    """讀這條管線還沒處理的異常（anomalies，屬於 detection）跑告警管線，處理完推進自己的游標。

    不 import detection：異常透過 AnomalyReadDAO 唯讀取得，進度記在 alerts 自己的 alert_watermarks。
    推送失敗（InfrastructureError）時游標不前進，下次會再處理同一批。
    """

    def __init__(self, req: ProcessPendingRequest):
        self.req = req

    def validate(self) -> None:
        self.preset = get_preset(self.req.preset)
        check_preset(self.preset)

    def run(self) -> dict:
        self.validate()
        consumer = f"pipeline:{self.req.pipeline}"
        last = AlertWatermarkDAO.get(consumer)
        rows = AnomalyReadDAO.list_after(self.req.pipeline, last, self.req.limit)
        if not rows:
            return {"pipeline": self.req.pipeline, "processed_anomalies": 0, "last_anomaly_id": last, "incidents": []}

        events = [AnomalyEvent(r["entity"], r["metric"], r["ts"], r["score"], r["attrs"] or {}) for r in rows]
        incidents = AlertPipeline(self.preset, self.req.context, self.req.plugin_params).run(events)
        AlertWatermarkDAO.set(consumer, rows[-1]["id"])
        return {"pipeline": self.req.pipeline, "processed_anomalies": len(rows),
                "last_anomaly_id": rows[-1]["id"], "incidents": incidents}
