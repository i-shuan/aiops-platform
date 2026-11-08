from typing import Any

from pydantic import BaseModel, Field


class GenerateRCARequest(BaseModel):
    incident_id: int
    preset: str
    context: dict[str, Any] = Field(default_factory=dict)
