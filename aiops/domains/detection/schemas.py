from typing import Any

from pydantic import BaseModel

from aiops.schemas import MetricTarget, TimeRange


class DetectionRunRequest(BaseModel):
    metric: MetricTarget
    time_range: TimeRange
    # 不給就用 time_range 同一段資料 fit
    train_time_range: TimeRange | None = None
    detector_name: str = "zscore"
    # None = 用 tuning 核准後的現行參數；{} = 外掛預設值
    params: dict[str, Any] | None = None
    # 給了就檢查此偵測器是否在該場景啟用
    preset: str | None = None
