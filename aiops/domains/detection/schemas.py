from typing import Any

from pydantic import BaseModel, Field

from aiops.schemas import MetricTarget, TimeRange


class DetectionRunRequest(BaseModel):
    metric: MetricTarget
    time_range: TimeRange
    # 給了就用這段資料重新 fit（不用保存的模型）
    train_time_range: TimeRange | None = None
    detector_name: str = "zscore"
    # None = 用 tuning 核准後的現行參數；{} = 外掛預設值
    params: dict[str, Any] | None = None
    # 給了就檢查此偵測器是否在該場景啟用
    preset: str | None = None
    # 異常事件的實體名稱；不給就用 metric.query
    entity: str | None = None
    # 帶進異常事件的屬性，例如 {"dept": "payments"}（通知依 dept 路由）
    attrs: dict[str, Any] = Field(default_factory=dict)
    # 給了就：優先載入這條管線保存的模型（沒有就 fit 並保存），並把「新的」異常寫進 anomalies
    pipeline: str | None = None


class DetectionTrainRequest(BaseModel):
    metric: MetricTarget
    train_time_range: TimeRange
    detector_name: str = "zscore"
    pipeline: str
    entity: str | None = None
    # 給了就：現有模型比這個分鐘數新就不重訓
    if_older_than_minutes: int | None = Field(None, gt=0)
