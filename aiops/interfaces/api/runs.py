from fastapi import APIRouter, Request

from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import RcaRequest, RunWorkflowRequest
from aiops.interfaces.runner import run

router = APIRouter(prefix="/api/v1", tags=["runs"])


@router.post("/runs")
def run_workflow(req: RunWorkflowRequest):
    return {"result": run(req.workflow, targets=req.targets, now=req.now, strict=False)}


@router.post("/rca")
def generate_rca(req: RcaRequest, request: Request):
    """同一個事件群、同一條 workflow 的報告正在產生時，後到的請求等它並拿同一份（engine/single_flight）：
    10 個人同時按，只撈一次 trace／日誌、只呼叫一次 LLM。"""
    return {"result": request.app.state.rca_flight.do(
        (req.workflow, req.incident_id), lambda: run(req.workflow, incident_id=req.incident_id))}


@router.get("/operators")
def list_operators():
    return {"result": OperatorRegistry.names()}
