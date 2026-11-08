from fastapi import APIRouter

from aiops.engine.requests import IngestAnomalies, LotPathsMerge, MappingApprove, MappingSubmit, SourceRef, TraceMerge
from aiops.interfaces.runner import run

router = APIRouter(prefix="/api/v1/ingestion", tags=["ingestion"])


@router.post("/mappings")
def submit_mapping(req: MappingSubmit):
    return {"result": run("admin_mapping_submit", request=req)}


@router.post("/mappings/approve")
def approve_mapping(req: MappingApprove):
    return {"result": run("admin_mapping_approve", request=req)}


@router.get("/mappings/approved/{source_type}")
def get_approved_mapping(source_type: str):
    return {"result": run("admin_mapping_get_approved", request=SourceRef(source_type=source_type))}


@router.post("/traces/merge")
def merge_traces(req: TraceMerge):
    return {"result": run("admin_merge_traces", request=req)}


@router.post("/lot-paths/merge")
def merge_lot_paths(req: LotPathsMerge):
    return {"result": run("admin_merge_lot_paths", request=req)}


@router.post("/anomalies")
def ingest_anomalies(req: IngestAnomalies):
    return {"result": run("admin_ingest_anomalies", request=req)}


@router.get("/sources/check")
def check_sources():
    return {"result": run("admin_sources_check")}
