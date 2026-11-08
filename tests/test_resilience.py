"""局部降級與併發安全：旁路節點失敗不擋主流程；基準線／現行參數不會被較舊的寫入蓋掉。"""
from datetime import datetime

import pytest

from aiops.storage.repository import ActiveConfigs, DetectorModels, Proposals

T0 = datetime(2026, 11, 8, 0, 0)

from aiops.core.context import PipelineContext
from aiops.core.errors import InfrastructureError, NotFound
from aiops.core.pipeline import NodeSpec, Pipeline, load_workflow
from aiops.core.registry import OperatorRegistry
from aiops.operators.base import BaseOperator
from aiops.runner import workflow_path
from aiops.storage.hooks import StorageHooks


@OperatorRegistry.register("test.boom")
class _Boom(BaseOperator):
    scope = "batch"

    def __init__(self, infra: bool = False, plain: bool = False):
        self.infra, self.plain = infra, plain

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if self.infra:
            raise InfrastructureError("db down")
        raise RuntimeError("boom") if self.plain else NotFound("沒資料")


def _pipeline(*, optional: bool, **params) -> Pipeline:
    """真實的 metrics_only 流程，在「通知內容」前插入一個一定會失敗的批次節點。"""
    wf = load_workflow(workflow_path("metrics_only"))
    at = next(i for i, n in enumerate(wf.nodes) if n.operator == "alert.notify_payload")
    wf.nodes.insert(at, NodeSpec(name="flaky", operator="test.boom", params=params, optional=optional))
    return Pipeline(wf, StorageHooks())


def test_failing_optional_node_is_skipped_and_the_rest_still_persists_and_notifies(now, sent, client):
    ctx = _pipeline(optional=True).run(now=now)
    assert len(ctx.incidents) == 1 and len(sent) == 1  # 後面的 payload／notify 照跑
    assert [e.entity for e in ctx.errors] == ["node:flaky"] and "flaky" in ctx.errors[0].message
    assert "flaky:skipped" in ctx.audit
    assert client.get(f"/api/v1/alerts/incidents/{ctx.incident_ids[ctx.incidents[0].fingerprint]}").status_code == 200
    assert ctx.to_result()["errors"][0]["entity"] == "node:flaky"


def test_optional_node_swallows_unexpected_exceptions_too(now, sent):
    ctx = _pipeline(optional=True, plain=True).run(now=now)
    assert len(sent) == 1 and ctx.errors


def test_failing_required_node_still_aborts_and_nothing_is_persisted(now, sent):
    with pytest.raises(NotFound):
        _pipeline(optional=False).run(now=now)
    assert sent == []


def test_infrastructure_error_in_an_optional_node_still_fails_the_run(now, sent):
    with pytest.raises(InfrastructureError):
        _pipeline(optional=True, infra=True).run(now=now)
    assert sent == []


def test_optional_summary_in_rca_report_workflow_is_marked_optional():
    wf = load_workflow(workflow_path("rca_report"))
    assert {n.name: n.optional for n in wf.nodes} == {"report": False, "summary": True}


# ── 偵測器基準線：樂觀鎖 ──


def _save(state, expected, **kw):
    return DetectorModels.save("p", "e", "m", "detector.zscore", state, T0, T0, 10, expected_version=expected, **kw)


def test_first_insert_race_keeps_the_first_writer():
    first, second = _save({"mean": 1}, 0), _save({"mean": 2}, 0)  # 兩個 worker 都以為「還沒有這一筆」
    assert first["version"] == 1 and second is None
    assert DetectorModels.get("p", "e", "m", "detector.zscore")["state"] == {"mean": 1}


def test_stale_worker_cannot_overwrite_a_newer_baseline():
    _save({"mean": 1}, 0)
    a = _save({"mean": 2}, 1)  # 兩個 worker 都讀到 version 1
    b = _save({"mean": 3}, 1)
    assert a["version"] == 2 and b is None
    row = DetectorModels.get("p", "e", "m", "detector.zscore")
    assert row["state"] == {"mean": 2} and row["version"] == 2
    assert _save({"mean": 4}, 2)["version"] == 3  # 讀到最新版的人可以繼續寫


def test_update_for_a_row_that_does_not_exist_is_a_conflict_not_an_insert():
    assert _save({"mean": 1}, 5) is None
    assert DetectorModels.get("p", "e", "m", "detector.zscore") is None


def test_pipeline_rerun_after_training_bumps_the_version_only_when_it_retrains(now):
    from aiops.runner import run_context

    run_context("metrics_only", now=now, strict=False)
    row = DetectorModels.get("metrics_only", "cpu", "cpu", "detector.zscore") or next(iter(_all_models()), None)
    assert row is not None and row["version"] == 1
    run_context("metrics_only", now=now, strict=False)  # 基準線沒過期：不重訓、不寫、版本不變
    assert _all_models()[0]["version"] == 1


def test_concurrent_first_training_in_the_pipeline_logs_a_conflict_and_keeps_the_winner(now, monkeypatch):
    """兩個 worker 都讀到「沒有基準線」：後寫的那個在 audit 留下衝突紀錄，資料庫裡留下先寫的。"""
    from aiops.storage import hooks

    real_load = hooks.StorageHooks.load_target

    def load_then_other_worker_wins(self, ctx, detector_nodes):
        real_load(self, ctx, detector_nodes)
        ctx.detector_states.clear()  # 這個 worker 讀的時候還沒有
        if not _all_models():
            DetectorModels.save(ctx.pipeline, ctx.target.entity, ctx.target.query, "detector.zscore", {"winner": True},
                                T0, T0, 1, expected_version=0, node="detect")  # 另一個 worker 在這個 worker 寫入前先寫了

    monkeypatch.setattr(hooks.StorageHooks, "load_target", load_then_other_worker_wins)
    from aiops.runner import run_context

    ctx = run_context("metrics_only", now=now, strict=False)
    assert any("conflict-kept-newer" in line for line in ctx.audit)
    assert _all_models()[0]["state"] == {"winner": True}


# ── 現行參數核准：條件式寫入 ──


def test_concurrent_approvals_only_one_wins(now):
    from aiops.core.errors import Conflict
    from aiops.core.requests import ProposalApprove, ProposalBacktest, ProposalCreate
    from aiops.runner import run

    def propose(threshold):
        pid = run("admin_proposal_create", request=ProposalCreate(
            source="manual", detector="detector.zscore", params={"threshold": threshold}, proposer="a"))["proposal"]["id"]
        req = ProposalBacktest(proposal_id=pid, metric={"query": "cpu"}, time_range={
            "start": now.replace(hour=0), "end": now, "step_seconds": 60})
        run("admin_proposal_backtest", request=req, targets=req.to_targets(), now=now)
        return pid

    first, second = propose(2.0), propose(4.0)  # 兩個提案都以 version 0 為基準
    run("admin_proposal_approve", request=ProposalApprove(proposal_id=first, approver="b"))
    with pytest.raises(Conflict, match="已作廢"):
        run("admin_proposal_approve", request=ProposalApprove(proposal_id=second, approver="b"))
    assert ActiveConfigs.get("detector.zscore")["params"] == {"threshold": 2.0}
    assert Proposals.get(second)["status"] == "stale"


def test_active_config_set_is_conditional():
    assert ActiveConfigs.set("detector.mad", {"threshold": 3.0}, "manual", 1, expected_version=0)["version"] == 1
    assert ActiveConfigs.set("detector.mad", {"threshold": 9.0}, "manual", 2, expected_version=0) is None  # 第二個以為還沒有
    assert ActiveConfigs.set("detector.mad", {"threshold": 4.0}, "manual", 3, expected_version=1)["version"] == 2
    assert ActiveConfigs.set("detector.mad", {"threshold": 9.0}, "manual", 4, expected_version=1) is None
    assert ActiveConfigs.get("detector.mad")["params"] == {"threshold": 4.0}


def _all_models():
    from aiops.storage import crud
    from aiops.storage.models import DetectorModelRow

    return crud.find(DetectorModelRow)
