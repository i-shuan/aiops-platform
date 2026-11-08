from typing import Any

from pydantic import BaseModel, Field


class GenerateRCARequest(BaseModel):
    incident_id: int
    preset: str
    # 給段落用的場景資料：topology（[caller, callee]）、lot_edges（Lot 履歷流向）、trace_series（trace 合成結果）、p0_entities
    context: dict[str, Any] = Field(default_factory=dict)
    # False：不呼叫 LLM，總結直接用模板
    narrative: bool = True
