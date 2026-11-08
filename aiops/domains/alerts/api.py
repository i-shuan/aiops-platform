from fastapi import APIRouter

from aiops.domains.alerts.commands.process import ProcessAlertsCommand
from aiops.domains.alerts.commands.process_pending import ProcessPendingAnomaliesCommand
from aiops.domains.alerts.commands.silences import CreateSilenceCommand, ListSilencesCommand
from aiops.domains.alerts.schemas import ProcessAlertsRequest, ProcessPendingRequest, SilenceCreateRequest

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.post("/process")
def process_alerts(req: ProcessAlertsRequest):
    return {"result": ProcessAlertsCommand(req).run()}


@router.post("/process-pending")
def process_pending(req: ProcessPendingRequest):
    return {"result": ProcessPendingAnomaliesCommand(req).run()}


@router.post("/silences")
def create_silence(req: SilenceCreateRequest):
    return {"result": CreateSilenceCommand(req).run()}


@router.get("/silences")
def list_silences():
    return {"result": ListSilencesCommand().run()}
