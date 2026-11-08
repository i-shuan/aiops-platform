from fastapi import APIRouter

from aiops.core.requests import IdRef, SilenceCreate, UidRef
from aiops.runner import run
from aiops.storage.repository import Silences

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.post("/silences")
def create_silence(req: SilenceCreate):
    return {"result": run("admin_silence_create", request=req)}


@router.get("/silences")
def list_silences():
    return {"result": Silences.list_all()}


@router.get("/incidents/{incident_id}")
def get_incident(incident_id: int):
    return {"result": run("admin_incident_get", request=IdRef(id=incident_id))}


@router.post("/incidents/{uid}/resolve")
def resolve_incident(uid: str):
    return {"result": run("admin_incident_resolve", request=UidRef(uid=uid))}
