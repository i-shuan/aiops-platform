from fastapi import APIRouter

from aiops.domains.tuning.commands.proposals import (
    ApproveTuningCommand,
    BacktestProposalCommand,
    CreateProposalCommand,
    GetProposalCommand,
)
from aiops.domains.tuning.schemas import ApproveRequest, BacktestRequest, ProposalCreateRequest

router = APIRouter(prefix="/api/v1/tuning", tags=["tuning"])


@router.post("/proposals")
def create_proposal(req: ProposalCreateRequest):
    return {"result": CreateProposalCommand(req).run()}


@router.get("/proposals/{proposal_id}")
def get_proposal(proposal_id: int):
    return {"result": GetProposalCommand(proposal_id).run()}


@router.post("/proposals/{proposal_id}/backtest")
def backtest_proposal(proposal_id: int, req: BacktestRequest):
    return {"result": BacktestProposalCommand(proposal_id, req).run()}


@router.post("/proposals/{proposal_id}/approve")
def approve_proposal(proposal_id: int, req: ApproveRequest):
    return {"result": ApproveTuningCommand(proposal_id, req.approver).run()}
