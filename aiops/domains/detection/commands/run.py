import pandas as pd

from aiops.commands.base import BaseCommand, CommandError
from aiops.datasource_specs import get_datasource_spec
from aiops.domains.detection.dao import AnomalyDAO, DetectorConfigReadDAO, DetectorModelDAO
from aiops.domains.detection.schemas import DetectionRunRequest
from aiops.plugins.base import AnomalyResult, BaseDetector, as_utc
from aiops.plugins.registry import DetectorRegistry
from aiops.presets import get_preset
from aiops.schemas import TimeRange


class RunDetectionCommand(BaseCommand):
    """抓資料 -> （載入保存的模型或 fit）-> detect ->（有管線名稱時）把新異常寫進 anomalies。

    演算法由 Registry 動態取得，這裡不寫任何演算法；也不呼叫 alerts，告警由 tasks 另外觸發。
    """

    def __init__(self, req: DetectionRunRequest):
        self.req = req

    def validate(self) -> None:
        name = self.req.detector_name
        if not DetectorRegistry.has(name):
            raise CommandError(f"未知的檢測外掛: {name}")
        if self.req.preset and name not in get_preset(self.req.preset).detectors:
            raise CommandError(f"preset {self.req.preset} 未啟用偵測器 {name}")

    def _fetch(self, tr: TimeRange) -> pd.DataFrame:
        metric = self.req.metric
        return get_datasource_spec(metric.source_type).fetch_data(metric.query, tr.start, tr.end, tr.step_seconds)

    def _detector(self, params: dict, df: pd.DataFrame, entity: str) -> tuple[BaseDetector, str]:
        req = self.req
        detector = DetectorRegistry.get(req.detector_name)(params)
        if req.train_time_range:
            return detector.fit(self._fetch(req.train_time_range)), "fitted"
        if req.pipeline:
            model = DetectorModelDAO.get(req.pipeline, entity, req.metric.query, req.detector_name)
            if model:
                return detector.load_state(model["state"]), "saved"
            detector.fit(df)
            tr = req.time_range
            DetectorModelDAO.save(req.pipeline, entity, req.metric.query, req.detector_name,
                                  detector.get_state(), tr.start, tr.end, len(df))
            return detector, "fitted_and_saved"
        return detector.fit(df), "fitted"

    def _persist(self, df: pd.DataFrame, result: AnomalyResult, entity: str) -> int:
        """只寫比上次更新的異常點，重跑同一段時間不會重複寫入。"""
        req = self.req
        latest = AnomalyDAO.latest_ts(req.pipeline, entity, req.metric.query)
        rows = [
            {"pipeline": req.pipeline, "entity": entity, "metric": req.metric.query, "ts": ts.to_pydatetime(),
             "value": float(value), "score": float(score), "detector_name": req.detector_name, "attrs": req.attrs}
            for ts, value, score, flagged in zip(df["ts"], result.values, result.scores, result.is_anomaly)
            if flagged and (latest is None or as_utc(ts.to_pydatetime()) > as_utc(latest))
        ]
        return AnomalyDAO.create_many(rows)

    def run(self) -> dict:
        self.validate()
        req = self.req
        name = req.detector_name
        entity = req.entity or req.metric.query
        params = req.params if req.params is not None else (DetectorConfigReadDAO.get_params(name) or {})
        df = self._fetch(req.time_range)
        detector, model = self._detector(params, df, entity)
        result = detector.detect(df)
        out = {**result.to_dict(), "detector_name": name, "params": params, "model": model, "entity": entity}
        if req.pipeline:
            out["persisted"] = self._persist(df, result, entity)
        return out
