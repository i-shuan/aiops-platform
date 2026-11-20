from fastapi import APIRouter, Depends

from aiops.engine.requests import (
    FeedbackPropose,
    FeedbackSubmit,
    IdRef,
    ProposalApprove,
    ProposalBacktest,
    ProposalCreate,
    ReviewMetricsQuery,
    RulePromote,
    SeriesProfile,
    TuningMetricsQuery,
)
from aiops.interfaces.runner import backtest_proposal as run_backtest, profile_series as run_profile, run

router = APIRouter(prefix="/api/v1/tuning", tags=["tuning"])


@router.post("/proposals")
def create_proposal(req: ProposalCreate):
    return {"result": run("admin_proposal_create", request=req)}


@router.get("/proposals/{proposal_id}")
def get_proposal(proposal_id: int):
    return {"result": run("admin_proposal_get", request=IdRef(id=proposal_id))}


@router.post("/proposals/backtest")
def backtest_proposal(req: ProposalBacktest):
    """只准平台看門狗管線的提案、平台自己的紀錄（其他 403，見 runner.check_detection_entry）。"""
    return {"result": run_backtest(req)}


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


@router.get("/review")
def review_metrics(req: ReviewMetricsQuery = Depends()):
    """分項覆核統計（收斂、RCA、結案、嚴重度各看過幾張、錯在哪）。只是報表，不改任何設定。"""
    return {"result": run("admin_review_metrics", request=req)}


@router.post("/rules/promote")
def promote_rules(req: RulePromote):
    return {"result": run("admin_rule_promote", request=req)}


@router.post("/profile")
def profile_series(req: SeriesProfile):
    """只准平台看門狗管線、平台自己的紀錄（其他 403，見 runner.check_detection_entry）。"""
    return {"result": run_profile(req)}
