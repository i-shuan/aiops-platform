from fastapi import APIRouter, Request

from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import RcaRequest, RunWorkflowRequest
from aiops.interfaces.runner import rca_workflow_for, run, run_workflow_request

router = APIRouter(prefix="/api/v1", tags=["runs"])


@router.post("/runs")
def run_workflow(req: RunWorkflowRequest):
    """帶 targets 只准平台看門狗管線、平台自己的紀錄（其他 403，見 runner.check_detection_entry）。"""
    return {"result": run_workflow_request(req)}


@router.post("/rca")
def generate_rca(req: RcaRequest, request: Request):
    """同一個事件群、同一條 workflow 的報告正在產生時，後到的請求等它並拿同一份（engine/single_flight）：
    10 個人同時按，只撈一次 trace／日誌、只呼叫一次 LLM。"""
    workflow = rca_workflow_for(req.incident_id, req.workflow)
    return {"result": request.app.state.rca_flight.do(
        (workflow, req.incident_id), lambda: run(workflow, incident_id=req.incident_id))}


@router.get("/operators")
def list_operators():
    return {"result": OperatorRegistry.names()}
