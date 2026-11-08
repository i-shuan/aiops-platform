from fastapi import APIRouter

from aiops.domains.detection.commands.list_detectors import ListDetectorsCommand
from aiops.domains.detection.commands.run import RunDetectionCommand
from aiops.domains.detection.commands.train import TrainDetectorCommand
from aiops.domains.detection.schemas import DetectionRunRequest, DetectionTrainRequest

router = APIRouter(prefix="/api/v1/detection", tags=["detection"])


@router.post("/run")
def run_detection(req: DetectionRunRequest):
    return {"result": RunDetectionCommand(req).run()}


@router.post("/train")
def train_detector(req: DetectionTrainRequest):
    return {"result": TrainDetectorCommand(req).run()}


@router.get("/detectors")
def list_detectors():
    return {"result": ListDetectorsCommand().run()}
