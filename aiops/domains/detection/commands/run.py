from aiops.commands.base import BaseCommand, CommandError
from aiops.datasource_specs import get_datasource_spec
from aiops.domains.detection.schemas import DetectionRunRequest
from aiops.plugins.registry import DetectorRegistry


class RunDetectionCommand(BaseCommand):
    """組裝流程：DataSourceSpec 抓資料 -> Registry 取偵測器 -> detect。不寫演算法。"""

    def __init__(self, req: DetectionRunRequest):
        self.req = req

    def validate(self) -> None:
        if not DetectorRegistry.has(self.req.detector_name):
            raise CommandError(f"未知的檢測外掛: {self.req.detector_name}")
        if self.req.time_range.start >= self.req.time_range.end:
            raise CommandError("start 必須早於 end")

    def run(self) -> dict:
        self.validate()
        tr, metric = self.req.time_range, self.req.metric
        df = get_datasource_spec(metric.source_type).fetch_data(
            metric.query, tr.start, tr.end, tr.step_seconds
        )
        detector = DetectorRegistry.get(self.req.detector_name)(self.req.params)
        return detector.detect(df).to_dict()
