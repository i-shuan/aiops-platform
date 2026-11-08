from fastapi import APIRouter

from aiops.domains.detection.commands.run import RunDetectionCommand
from aiops.domains.detection.schemas import DetectionRunRequest
from aiops.plugins.registry import DetectorRegistry

router = APIRouter(prefix="/api/v1/detection", tags=["detection"])


@router.post("/run")
def run_detection(req: DetectionRunRequest):
    # API 只負責反序列化與呼叫 Command，不寫業務邏輯
    return {"result": RunDetectionCommand(req).run()}


@router.get("/detectors")
def list_detectors():
    return {"result": DetectorRegistry.names()}
