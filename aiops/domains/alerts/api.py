from fastapi import APIRouter

from aiops.domains.alerts.commands.process import ProcessAlertsCommand
from aiops.domains.alerts.commands.process_pending import ProcessPendingAnomaliesCommand
from aiops.domains.alerts.commands.silences import CreateSilenceCommand, ListSilencesCommand
from aiops.domains.alerts.commands.sync import ResolveIncidentCommand, SyncOpenIncidentsCommand
from aiops.domains.alerts.schemas import (
    ProcessAlertsRequest,
    ProcessPendingRequest,
    SilenceCreateRequest,
    SyncIncidentsRequest,
)

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.post("/process")
def process_alerts(req: ProcessAlertsRequest):
    return {"result": ProcessAlertsCommand(req).run()}


@router.post("/process-pending")
def process_pending(req: ProcessPendingRequest):
    return {"result": ProcessPendingAnomaliesCommand(req).run()}


@router.post("/sync")
def sync_open_incidents(req: SyncIncidentsRequest):
    return {"result": SyncOpenIncidentsCommand(req).run()}


@router.post("/incidents/{uid}/resolve")
def resolve_incident(uid: str):
    return {"result": ResolveIncidentCommand(uid).run()}


@router.post("/silences")
def create_silence(req: SilenceCreateRequest):
    return {"result": CreateSilenceCommand(req).run()}


@router.get("/silences")
def list_silences():
    return {"result": ListSilencesCommand().run()}
