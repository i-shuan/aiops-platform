"""組裝點：把 workflow 定義、算子、storage 的載入與寫入點接在一起，並提供給 API 與排程呼叫。

這裡是唯一同時認識 engine、算子（stages、control_plane）、storage 的地方；其他層各自只認識自己的鄰居。
"""
from __future__ import annotations

import importlib
import logging
import pkgutil
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from aiops.engine.context import PipelineContext
from aiops.engine.errors import BadRequest, NotFound, WorkflowError
from aiops.engine.health import run_status
from aiops.engine.pipeline import Pipeline, TargetSpec, load_workflow
from aiops.engine.requests import GrafanaAlertIngest
from aiops.storage.hooks import StorageHooks
from aiops.storage.repository import Incidents

logger = logging.getLogger(__name__)

WORKFLOW_DIR = Path(__file__).parents[1] / "pipelines"
# 算子所在的套件：管線算子（stages）與控制面手動動作（control_plane）
OPERATOR_PACKAGES = ("aiops.stages", "aiops.control_plane")
# 白名單：只准英數、底線、連字號。fullmatch 一次擋掉 /、\、..、空字串、換行與 \x00
WORKFLOW_NAME = r"[A-Za-z0-9_-]+"
_NAME = re.compile(WORKFLOW_NAME)
# 事件群查不到所屬管線、或那條管線沒寫 rca_workflow 時用的 RCA 管線：不需要 LLM、沒有 trace 也能退回拓撲推測
DEFAULT_RCA_WORKFLOW = "rca_report"


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
    workflow = load_workflow(workflow_path(name, allow_file=allow_file))
    if workflow.rca_workflow:
        try:
            workflow_path(workflow.rca_workflow)  # 寫錯名稱在載入期就報錯，不要等到有人按 RCA 才發現
        except NotFound as e:
            raise WorkflowError(f"workflow {workflow.name} 的 rca_workflow 找不到：{workflow.rca_workflow}") from e
    return Pipeline(workflow, StorageHooks())


def rca_workflow_for(incident_id: int, requested: str | None = None) -> str:
    """這個事件群要用哪一條 RCA 管線：呼叫端指定的優先；沒指定就看事件群所屬偵測管線 YAML 的 rca_workflow；
    查不到（事件群不存在、舊資料沒記管線、管線是排程 CLI 用檔案載入的）就用 DEFAULT_RCA_WORKFLOW。
    事件群不存在不在這裡報錯：照常交給管線載入時丟 NotFound（404）。"""
    if requested:
        return requested
    row = Incidents.get(incident_id)
    pipeline = (row or {}).get("pipeline")
    if not pipeline:
        return DEFAULT_RCA_WORKFLOW
    try:
        path = workflow_path(pipeline)
    except (NotFound, BadRequest) as e:
        logger.info("rca_workflow_for: 事件群 %s 的管線 %r 不是內建 workflow（%s），用 %s",
                    incident_id, pipeline, e, DEFAULT_RCA_WORKFLOW)
        return DEFAULT_RCA_WORKFLOW
    return load_workflow(path).rca_workflow or DEFAULT_RCA_WORKFLOW


def run_context(workflow: str, *, request: BaseModel | None = None, incident_id: int | None = None,
                targets: list[TargetSpec] | None = None, now: datetime | None = None,
                strict: bool = True, allow_file: bool = False) -> PipelineContext:
    return build_pipeline(workflow, allow_file=allow_file).run(now=now, targets=targets, incident_id=incident_id,
                                                               request=request, strict=strict)


def run_scheduled(workflow: str, now: datetime | None = None) -> PipelineContext:
    """排程入口（interfaces/cli）跑一輪：某條序列失敗只記在 errors（strict=False），可以用自己的 YAML 檔。
    跟 run_context 的差別只有一件事：管線定義載入失敗（WorkflowError：YAML 寫錯、算子參數不合法）時，這一輪根本沒跑、
    引擎沒機會記，在這裡補記一筆管線層級的 workflow_error（資料健康度），事後才查得到「那段時間管線沒在跑、原因是設定」。
    只有排程補記：API 與手動動作的設定錯誤直接回給呼叫的人，而且維持「報錯之前不碰資料庫」（tests/test_fail_fast.py）。"""
    try:
        pipeline = build_pipeline(workflow, allow_file=True)
    except WorkflowError as e:
        record_load_failure(workflow, e, now)
        raise
    return pipeline.run(now=now, strict=False)


def record_load_failure(workflow: str, error: WorkflowError, now: datetime | None = None) -> None:
    """名稱用呼叫時給的（內建管線 = 檔名 = YAML 的 name；自己的 YAML 檔用檔名，讀不到檔案裡的 name）。
    寫不進去（資料庫連不上）只記 log，照原本的錯誤往外丟。"""
    ctx = PipelineContext(pipeline=Path(workflow).stem[:128], now=now or datetime.now(timezone.utc))
    ctx.run_check = run_status(error, targets=0, failed_entities=[])
    try:
        StorageHooks().record_health(ctx)
    except Exception as e:  # noqa: BLE001
        logger.warning("管線 %s 載入失敗的健康度紀錄寫不進去：%s", ctx.pipeline, e)


def ingest_grafana_alerts(request: GrafanaAlertIngest) -> dict[str, Any]:
    """各團隊 Grafana 的 webhook：先確認網址上的 pipeline 是存在的偵測管線，再交給 admin_ingest_grafana_alerts。
    寫錯的話回 404／400：對方在自己的 Grafana 通知管道上看得到「送達失敗」；不檢查的話告警會一直等在 pending，沒有人知道。"""
    path = workflow_path(request.pipeline)
    if path.parent.name != "detection":
        raise BadRequest(f"{request.pipeline} 不是偵測管線（pipelines/detection/ 底下的），告警不能送進去")
    return run("admin_ingest_grafana_alerts", request=request)


def run(workflow: str, **kwargs: Any) -> dict[str, Any]:
    """跑一個 workflow，回傳結果摘要（給 API 直接回傳）。"""
    return run_context(workflow, **kwargs).to_result()
