from fastapi import APIRouter

from aiops.domains.alerts.commands.process import ProcessAlertsCommand
from aiops.domains.alerts.schemas import ProcessAlertsRequest

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.post("/process")
def process_alerts(req: ProcessAlertsRequest):
    return {"result": ProcessAlertsCommand(req).run()}
