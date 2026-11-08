"""排程工作設定（YAML）。一個 job = 一條管線：同一組目標指標、同一個 preset、同一個偵測器。"""
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator


class JobTarget(BaseModel):
    source_type: str = "mock"
    query: str
    entity: str | None = None
    attrs: dict[str, Any] = Field(default_factory=dict)


class Job(BaseModel):
    name: str
    preset: str
    detector_name: str = "zscore"
    # None = 用 tuning 核准後的現行參數
    params: dict[str, Any] | None = None
    step_seconds: int = Field(60, gt=0)
    # 每次偵測看最近多久
    lookback_minutes: int = Field(60, gt=0)
    # 批次訓練用多久的歷史
    train_lookback_minutes: int = Field(1440, gt=0)
    # 模型超過這麼久才重訓
    retrain_minutes: int = Field(1440, gt=0)
    targets: list[JobTarget] = Field(min_length=1)
    context: dict[str, Any] = Field(default_factory=dict)
    plugin_params: dict[str, dict[str, Any]] = Field(default_factory=dict)


class JobsFile(BaseModel):
    jobs: list[Job] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_names(self):
        names = [j.name for j in self.jobs]
        if len(names) != len(set(names)):
            raise ValueError(f"job 名稱不可重複：{names}")
        return self


def load_jobs(path: str | Path) -> list[Job]:
    return JobsFile.model_validate(yaml.safe_load(Path(path).read_text(encoding="utf-8"))).jobs
