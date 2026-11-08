"""組裝點：把 workflow 定義、算子、storage 的載入與寫入點接在一起，並提供給 API 與排程呼叫。

這裡是唯一同時認識 core、operators、storage 的地方；其他層各自只認識自己的鄰居。
"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from aiops.core.context import PipelineContext
from aiops.core.errors import BadRequest, NotFound
from aiops.core.pipeline import Pipeline, TargetSpec, load_workflow
from aiops.operators import load_all
from aiops.storage.hooks import StorageHooks

WORKFLOW_DIR = Path(__file__).parent / "workflows"
_NAME = re.compile(r"^[A-Za-z0-9_\-]+$")


def workflow_path(name_or_path: str) -> Path:
    """內建 workflow 用名字（例如 it_otel）；自己的檔案用路徑。名字只准英數底線，避免被當成路徑穿越。"""
    if name_or_path.endswith((".yaml", ".yml")) or "/" in name_or_path or "\\" in name_or_path:
        path = Path(name_or_path)
    elif _NAME.match(name_or_path):
        path = WORKFLOW_DIR / f"{name_or_path}.yaml"
    else:
        raise BadRequest(f"不合法的 workflow 名稱：{name_or_path}")
    if not path.is_file():
        raise NotFound(f"找不到 workflow：{name_or_path}")
    return path


def build_pipeline(name_or_path: str) -> Pipeline:
    load_all()
    return Pipeline(load_workflow(workflow_path(name_or_path)), StorageHooks())


def run_context(workflow: str, *, request: BaseModel | None = None, incident_id: int | None = None,
                targets: list[TargetSpec] | None = None, now: datetime | None = None,
                strict: bool = True) -> PipelineContext:
    return build_pipeline(workflow).run(now=now, targets=targets, incident_id=incident_id, request=request,
                                        strict=strict)


def run(workflow: str, **kwargs: Any) -> dict[str, Any]:
    """跑一個 workflow，回傳結果摘要（給 API 直接回傳）。"""
    return run_context(workflow, **kwargs).to_result()
