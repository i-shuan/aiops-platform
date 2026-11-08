from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class AnomalyEventIn(BaseModel):
    entity: str
    metric: str
    ts: datetime
    score: float = Field(ge=0)
    # 慣例鍵：dept（通知路由用的部門）、signal（黃金訊號種類）
    attrs: dict[str, Any] = Field(default_factory=dict)


class ProcessAlertsRequest(BaseModel):
    preset: str
    events: list[AnomalyEventIn] = Field(min_length=1)
    # 給外掛用的場景資料：topology（邊清單 [caller, callee]）、p0_entities 等
    context: dict[str, Any] = Field(default_factory=dict)
    # 依外掛名稱覆寫參數，例如 {"dedup": {"cooldown_minutes": 60}}
    plugin_params: dict[str, dict[str, Any]] = Field(default_factory=dict)


class SilenceCreateRequest(BaseModel):
    # mute：臨時靜音；maintenance：維護窗口；exclusion：人工排除時段（回饋調整）
    kind: Literal["mute", "maintenance", "exclusion"]
    start: datetime
    end: datetime
    # 兩個都空 = 全部靜音
    entity: str | None = None
    dept: str | None = None
    reason: str
    created_by: str

    @model_validator(mode="after")
    def _start_before_end(self):
        if self.start >= self.end:
            raise ValueError("start 必須早於 end")
        return self
