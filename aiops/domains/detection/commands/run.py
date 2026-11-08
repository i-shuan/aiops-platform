from aiops.commands.base import BaseCommand, CommandError
from aiops.datasource_specs import get_datasource_spec
from aiops.domains.detection.dao import DetectorConfigReadDAO
from aiops.domains.detection.schemas import DetectionRunRequest
from aiops.plugins.registry import DetectorRegistry
from aiops.presets import get_preset
from aiops.schemas import TimeRange


class RunDetectionCommand(BaseCommand):
    """抓資料 -> fit -> detect。演算法由 Registry 動態取得，這裡不寫任何演算法。"""

    def __init__(self, req: DetectionRunRequest):
        self.req = req

    def validate(self) -> None:
        name = self.req.detector_name
        if not DetectorRegistry.has(name):
            raise CommandError(f"未知的檢測外掛: {name}")
        if self.req.preset and name not in get_preset(self.req.preset).detectors:
            raise CommandError(f"preset {self.req.preset} 未啟用偵測器 {name}")

    def _fetch(self, tr: TimeRange):
        metric = self.req.metric
        return get_datasource_spec(metric.source_type).fetch_data(metric.query, tr.start, tr.end, tr.step_seconds)

    def run(self) -> dict:
        self.validate()
        name = self.req.detector_name
        params = self.req.params if self.req.params is not None else (DetectorConfigReadDAO.get_params(name) or {})
        df = self._fetch(self.req.time_range)
        train_df = self._fetch(self.req.train_time_range) if self.req.train_time_range else df
        detector = DetectorRegistry.get(name)(params).fit(train_df)
        return {**detector.detect(df).to_dict(), "detector_name": name, "params": params}
