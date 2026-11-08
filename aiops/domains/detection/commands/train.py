from datetime import datetime, timedelta

from aiops.commands.base import BaseCommand, CommandError
from aiops.datasource_specs import get_datasource_spec
from aiops.domains.detection.dao import DetectorModelDAO
from aiops.domains.detection.schemas import DetectionTrainRequest
from aiops.plugins.base import as_utc
from aiops.plugins.registry import DetectorRegistry


class TrainDetectorCommand(BaseCommand):
    """批次訓練：抓一段較長的歷史資料 fit，保存狀態（基準線參數）。偵測時直接載入，不用每次重 fit。"""

    def __init__(self, req: DetectionTrainRequest, now: datetime | None = None):
        self.req = req
        self.now = now

    def validate(self) -> None:
        if not DetectorRegistry.has(self.req.detector_name):
            raise CommandError(f"未知的檢測外掛: {self.req.detector_name}")

    def run(self) -> dict:
        self.validate()
        req = self.req
        entity = req.entity or req.metric.query
        key = (req.pipeline, entity, req.metric.query, req.detector_name)
        # 訓練時間與「是否過期」用同一個時鐘：排程傳入的 now（補跑、回放時才不會判斷錯），沒有就用系統時間
        now = as_utc(self.now or datetime.now().astimezone())

        if req.if_older_than_minutes is not None:
            existing = DetectorModelDAO.get(*key)
            if existing and now - as_utc(existing["trained_at"]) < timedelta(minutes=req.if_older_than_minutes):
                return {"trained": False, "reason": "模型仍在有效期內", "model_id": existing["id"],
                        "trained_at": existing["trained_at"]}

        tr = req.train_time_range
        df = get_datasource_spec(req.metric.source_type).fetch_data(req.metric.query, tr.start, tr.end, tr.step_seconds)
        detector = DetectorRegistry.get(req.detector_name)().fit(df)
        saved = DetectorModelDAO.save(*key, detector.get_state(), tr.start, tr.end, len(df), trained_at=now)
        return {"trained": True, "model_id": saved["id"], "points": len(df), "trained_at": saved["trained_at"]}
