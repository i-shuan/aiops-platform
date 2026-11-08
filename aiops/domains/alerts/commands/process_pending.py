from aiops.commands.base import BaseCommand
from aiops.domains.alerts.commands.pipeline import AlertPipeline, check_preset
from aiops.domains.alerts.dao import AlertWatermarkDAO, AnomalyReadDAO, ExternalAnomalyReadDAO
from aiops.domains.alerts.schemas import ProcessPendingRequest
from aiops.plugins.base import AnomalyEvent
from aiops.presets import get_preset

# 兩種來源各自一個游標：偵測產生的（detection.anomalies）、外部推來的（ingestion.external_anomalies）
_SOURCES = (("detected", AnomalyReadDAO, ""), ("external", ExternalAnomalyReadDAO, ":external"))


class ProcessPendingAnomaliesCommand(BaseCommand):
    """讀這條管線還沒處理的異常（偵測的 + 外部推來的），一起跑告警管線，處理完推進各自的游標。

    不 import detection／ingestion：異常透過 *ReadDAO 唯讀取得，進度記在 alerts 自己的 alert_watermarks。
    推送失敗（InfrastructureError）時游標不前進，下次會再處理同一批。
    """

    def __init__(self, req: ProcessPendingRequest):
        self.req = req

    def validate(self) -> None:
        self.preset = get_preset(self.req.preset)
        check_preset(self.preset)

    def run(self) -> dict:
        self.validate()
        base = f"pipeline:{self.req.pipeline}"
        batches = []
        for label, dao, suffix in _SOURCES:
            consumer = base + suffix
            rows = dao.list_after(self.req.pipeline, AlertWatermarkDAO.get(consumer), self.req.limit)
            batches.append((label, consumer, rows))

        counts = {label: len(rows) for label, _, rows in batches}
        events = [AnomalyEvent(r["entity"], r["metric"], r["ts"], r["score"], r["attrs"] or {})
                  for _, _, rows in batches for r in rows]
        if not events:
            return {"pipeline": self.req.pipeline, "processed_anomalies": 0, **counts, "incidents": []}

        incidents = AlertPipeline(self.preset, self.req.context, self.req.plugin_params).run(events)
        for _, consumer, rows in batches:
            if rows:
                AlertWatermarkDAO.set(consumer, rows[-1]["id"])
        return {"pipeline": self.req.pipeline, "processed_anomalies": len(events), **counts, "incidents": incidents}
