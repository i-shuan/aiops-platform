"""組裝點：把 workflow 定義、算子、storage 的載入與寫入點接在一起，並提供給 API 與排程呼叫。

這裡是唯一同時認識 engine、算子（stages、control_plane）、storage 的地方；其他層各自只認識自己的鄰居。
"""
from __future__ import annotations

import importlib
import pkgutil
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from aiops.engine.context import PipelineContext
from aiops.engine.errors import BadRequest, NotFound, WorkflowError
from aiops.engine.pipeline import Pipeline, TargetSpec, load_workflow
from aiops.storage.hooks import StorageHooks

WORKFLOW_DIR = Path(__file__).parents[1] / "pipelines"
# 算子所在的套件：管線算子（stages）與控制面手動動作（control_plane）
OPERATOR_PACKAGES = ("aiops.stages", "aiops.control_plane")
# 白名單：只准英數、底線、連字號。fullmatch 一次擋掉 /、\、..、空字串、換行與 \x00
WORKFLOW_NAME = r"[A-Za-z0-9_-]+"
_NAME = re.compile(WORKFLOW_NAME)


def load_all() -> None:
    """掃描所有算子套件，載入即完成 @OperatorRegistry.register。"""
    for package_name in OPERATOR_PACKAGES:
        package = importlib.import_module(package_name)
        for module in pkgutil.walk_packages(package.__path__, prefix=f"{package_name}."):
            importlib.import_module(module.name)


def workflow_path(name: str, *, allow_file: bool = False) -> Path:
    """依名稱找內建 workflow：pipelines/ 底下任一子資料夾的 <名稱>.yaml（例如 pipelines/detection/it_otel.yaml）。

    名稱在整個 pipelines/ 底下必須唯一，重複就直接報錯，不猜要用哪一個。
    allow_file=True 只給排程 CLI 用：維運人員在自己機器上指定自己的 YAML 檔。API 一律不開放，
    否則拿到 token 的人就能讓伺服器載入任意位置的檔案。
    """
    if allow_file and name.endswith((".yaml", ".yml")):
        path = Path(name)
        if not path.is_file():
            raise NotFound(f"找不到 workflow 檔：{name}")
        return path
    if not isinstance(name, str) or not _NAME.fullmatch(name):
        raise BadRequest(f"不合法的 workflow 名稱：{name!r}（只能是英數、底線、連字號）")
    # 名稱已過白名單，不含 * ? [ 這類 glob 字元
    hits = sorted(WORKFLOW_DIR.rglob(f"{name}.yaml"))
    if not hits:
        raise NotFound(f"找不到 workflow：{name}")
    if len(hits) > 1:
        found = ", ".join(p.relative_to(WORKFLOW_DIR).as_posix() for p in hits)
        raise WorkflowError(f"workflow 名稱 {name} 重複：{found}")
    path = hits[0]
    # pipelines/ 裡被放了指向外部的軟連結（檔案本身或中間的子資料夾）
    subdirs = [WORKFLOW_DIR / d for d in path.relative_to(WORKFLOW_DIR).parents if d != Path(".")]
    if any(p.is_symlink() for p in (path, *subdirs)):
        raise BadRequest(f"workflow {name} 是軟連結，拒絕載入")
    if not path.is_file():
        raise NotFound(f"找不到 workflow：{name}")
    if not path.resolve().is_relative_to(WORKFLOW_DIR.resolve()):
        raise BadRequest(f"workflow {name} 不在 {WORKFLOW_DIR} 底下，拒絕載入")
    return path


def build_pipeline(name: str, *, allow_file: bool = False) -> Pipeline:
    load_all()
    return Pipeline(load_workflow(workflow_path(name, allow_file=allow_file)), StorageHooks())


def run_context(workflow: str, *, request: BaseModel | None = None, incident_id: int | None = None,
                targets: list[TargetSpec] | None = None, now: datetime | None = None,
                strict: bool = True, allow_file: bool = False) -> PipelineContext:
    return build_pipeline(workflow, allow_file=allow_file).run(now=now, targets=targets, incident_id=incident_id,
                                                               request=request, strict=strict)


def run(workflow: str, **kwargs: Any) -> dict[str, Any]:
    """跑一個 workflow，回傳結果摘要（給 API 直接回傳）。"""
    return run_context(workflow, **kwargs).to_result()
