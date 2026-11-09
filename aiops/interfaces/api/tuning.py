from fastapi import APIRouter, Depends

from aiops.engine.requests import (
    FeedbackPropose,
    FeedbackSubmit,
    IdRef,
    ProposalApprove,
    ProposalBacktest,
    ProposalCreate,
    RulePromote,
    SeriesProfile,
    TuningMetricsQuery,
)
from aiops.interfaces.runner import run

router = APIRouter(prefix="/api/v1/tuning", tags=["tuning"])


@router.post("/proposals")
def create_proposal(req: ProposalCreate):
    return {"result": run("admin_proposal_create", request=req)}


@router.get("/proposals/{proposal_id}")
def get_proposal(proposal_id: int):
    return {"result": run("admin_proposal_get", request=IdRef(id=proposal_id))}


@router.post("/proposals/backtest")
def backtest_proposal(req: ProposalBacktest):
    return {"result": run("admin_proposal_backtest", request=req, targets=req.to_targets(), now=req.time_range.end)}


@router.post("/proposals/approve")
def approve_proposal(req: ProposalApprove):
    return {"result": run("admin_proposal_approve", request=req)}


@router.post("/feedback")
def submit_feedback(req: FeedbackSubmit):
    return {"result": run("admin_feedback_submit", request=req)}


@router.post("/feedback/propose")
def propose_from_feedback(req: FeedbackPropose):
    return {"result": run("admin_feedback_propose", request=req)}


@router.get("/metrics")
def tuning_metrics(req: TuningMetricsQuery = Depends()):
    return {"result": run("admin_tuning_metrics", request=req)}


@router.post("/rules/promote")
def promote_rules(req: RulePromote):
    return {"result": run("admin_rule_promote", request=req)}


@router.post("/profile")
def profile_series(req: SeriesProfile):
    return {"result": run("admin_series_profile", request=req, targets=req.to_targets(), now=req.end)}
