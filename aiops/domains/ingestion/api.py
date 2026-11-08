from fastapi import APIRouter

from aiops.domains.ingestion.commands.mappings import (
    ApproveMappingCommand,
    GetApprovedMappingCommand,
    SubmitMappingCommand,
)
from aiops.domains.ingestion.commands.merge_traces import MergeTracesCommand
from aiops.domains.ingestion.schemas import MappingApproveRequest, MappingSubmitRequest, TraceMergeRequest

router = APIRouter(prefix="/api/v1/ingestion", tags=["ingestion"])


@router.post("/mappings")
def submit_mapping(req: MappingSubmitRequest):
    return {"result": SubmitMappingCommand(req).run()}


@router.post("/mappings/{mapping_id}/approve")
def approve_mapping(mapping_id: int, req: MappingApproveRequest):
    return {"result": ApproveMappingCommand(mapping_id, req.approver).run()}


@router.get("/mappings/approved/{source_type}")
def get_approved_mapping(source_type: str):
    return {"result": GetApprovedMappingCommand(source_type).run()}


@router.post("/traces/merge")
def merge_traces(req: TraceMergeRequest):
    return {"result": MergeTracesCommand(req).run()}
