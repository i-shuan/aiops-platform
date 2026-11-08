"""detection 內部共用：建立偵測器並檢查參數、準備資料（含同儕序列）。"""
import pandas as pd

from aiops.commands.base import CommandError
from aiops.datasource_specs import fetch_frame
from aiops.plugins.base import BaseDetector
from aiops.plugins.registry import DetectorRegistry
from aiops.schemas import MetricTarget, TimeRange


def build_detector(name: str, params: dict, peers: list[MetricTarget], check_params: bool = True) -> BaseDetector:
    """check_params=False 給批次訓練用：訓練只學基準線，偵測參數（例如 rules 的上下限）到偵測時才檢查。"""
    if not DetectorRegistry.has(name):
        raise CommandError(f"未知的檢測外掛: {name}")
    detector = DetectorRegistry.get(name)(params)
    if detector.requires_peers and not peers:
        raise CommandError(f"{name} 偵測器需要 peers（同儕序列）")
    if check_params:
        try:
            detector.validate_params()
        except ValueError as e:
            raise CommandError(str(e)) from e
    return detector


def load_frame(metric: MetricTarget, peers: list[MetricTarget], tr: TimeRange) -> pd.DataFrame:
    return fetch_frame(metric.source_type, metric.query, tr.start, tr.end, tr.step_seconds,
                       [(p.source_type, p.query) for p in peers])
