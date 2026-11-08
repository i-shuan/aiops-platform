from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class AnomalyEventIn(BaseModel):
    entity: str
    metric: str
    ts: datetime
    score: float = Field(ge=0)
    attrs: dict[str, Any] = Field(default_factory=dict)


class ProcessAlertsRequest(BaseModel):
    preset: str
    events: list[AnomalyEventIn] = Field(min_length=1)
    # 給外掛用的場景資料：topology（邊清單）、p0_entities 等
    context: dict[str, Any] = Field(default_factory=dict)
    # 依外掛名稱覆寫參數，例如 {"same_entity": {"window_minutes": 5}}
    plugin_params: dict[str, dict[str, Any]] = Field(default_factory=dict)
