from fastapi import APIRouter

from aiops.core.registry import OperatorRegistry
from aiops.core.requests import RcaRequest, RunWorkflowRequest
from aiops.runner import run

router = APIRouter(prefix="/api/v1", tags=["runs"])


@router.post("/runs")
def run_workflow(req: RunWorkflowRequest):
    return {"result": run(req.workflow, targets=req.targets, now=req.now, strict=False)}


@router.post("/rca")
def generate_rca(req: RcaRequest):
    return {"result": run(req.workflow, incident_id=req.incident_id)}


@router.get("/operators")
def list_operators():
    return {"result": OperatorRegistry.names()}
