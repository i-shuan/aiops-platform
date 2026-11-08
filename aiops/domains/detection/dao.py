from typing import Any

from aiops.models import core


class DetectorConfigReadDAO:
    """跨領域唯讀：偵測只讀 tuning 擁有的現行參數；寫入只能經 tuning 的核准流程。"""

    @staticmethod
    def get_params(detector_name: str) -> dict[str, Any] | None:
        rows = core.active_detector_configs.list(detector_name=detector_name)
        return rows[0]["params"] if rows else None
