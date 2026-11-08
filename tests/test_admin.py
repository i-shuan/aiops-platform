"""手動動作（admin 算子）：欄位對照審核、調參狀態機、回饋轉提案、匯入。"""
import json

import pytest

from aiops.engine.errors import BadRequest, Conflict, NotFound
from aiops.engine.requests import (
    ExternalAnomalyIn,
    FeedbackPropose,
    FeedbackSubmit,
    IdRef,
    IngestAnomalies,
    MappingApprove,
    MappingSubmit,
    ProposalApprove,
    ProposalBacktest,
    ProposalCreate,
    SourceRef,
    SilenceCreate,
)
from aiops.stages.enrichment.base import BaseLLM, LLMError
from aiops.interfaces.runner import run, run_context
from aiops.storage.repository import ActiveConfigs, Proposals


def _backtest_req(pid, now, train_ratio=0.3):
    # mock 資料的尖峰在序列正中間：前 30% 當訓練段（乾淨），尖峰落在測試段，回測才看得到它
    return ProposalBacktest(proposal_id=pid, metric={"query": "cpu"}, train_ratio=train_ratio, time_range={
        "start": now.replace(hour=0), "end": now, "step_seconds": 60})


def _propose(detector="detector.zscore", params=None, source="auto"):
    return run("admin_proposal_create", request=ProposalCreate(
        source=source, detector=detector, params=params if params is not None else {"threshold": 2.5}, proposer="amy"))["proposal"]["id"]


def _backtest(pid, now):
    req = _backtest_req(pid, now)
    return run("admin_proposal_backtest", request=req, targets=req.to_targets(), now=req.time_range.end)["proposal"]


def _approve(pid):
    return run("admin_proposal_approve", request=ProposalApprove(proposal_id=pid, approver="bob"))


# ── 欄位對照 ──


def test_mapping_approval_supersedes_the_previous_one_and_only_pending_can_be_approved():
    def submit(fields):
        return run("admin_mapping_submit", request=MappingSubmit(source_type="es", fields=fields, submitter="amy"))["mapping"]["id"]

    first = submit({"@timestamp": "ts"})
    run("admin_mapping_approve", request=MappingApprove(mapping_id=first, approver="bob"))
    second = submit({"event.ingested": "ts"})
    run("admin_mapping_approve", request=MappingApprove(mapping_id=second, approver="bob"))
    assert run("admin_mapping_get_approved", request=SourceRef(source_type="es"))["mapping"]["id"] == second
    with pytest.raises(Conflict):
        run("admin_mapping_approve", request=MappingApprove(mapping_id=first, approver="bob"))  # 已被取代
    with pytest.raises(NotFound):
        run("admin_mapping_approve", request=MappingApprove(mapping_id=999, approver="bob"))
    with pytest.raises(NotFound):
        run("admin_mapping_get_approved", request=SourceRef(source_type="nothing"))


def test_admin_operators_reject_a_wrong_request_type():
    with pytest.raises(BadRequest, match="需要"):
        run("admin_mapping_approve", request=IdRef(id=1))


# ── 調參狀態機：提案 -> 回測 -> 核准 ──


def test_proposal_validation():
    with pytest.raises(BadRequest, match="不是偵測器"):
        _propose("alert.same_entity")
    with pytest.raises(BadRequest, match="超出範圍"):
        _propose(params={"threshold": 99})
    with pytest.raises(BadRequest, match="沒有參數"):
        _propose(params={"treshold": 3})
    from aiops.engine.errors import WorkflowError

    with pytest.raises(WorkflowError):
        _propose("detector.nope")


def test_approve_requires_a_backtest_and_then_activates_with_a_version(now):
    pid = _propose()
    with pytest.raises(Conflict, match="必須先回測"):
        _approve(pid)
    bt = _backtest(pid, now)
    assert bt["status"] == "backtested" and bt["backtest"]["points"] > 0
    active = _approve(pid)["active_config"]
    assert active["version"] == 1 and active["params"] == {"threshold": 2.5}
    with pytest.raises(Conflict):
        _backtest(pid, now)  # 核准後不能再回測


def test_a_stale_proposal_cannot_overwrite_a_newer_config(now):
    auto, manual = _propose(params={"threshold": 2.0}), _propose(params={"threshold": 4.0}, source="manual")
    _backtest(manual, now)
    _approve(manual)  # 人工先調好
    _backtest(auto, now)
    with pytest.raises(Conflict, match="已作廢"):
        _approve(auto)
    assert Proposals.get(auto)["status"] == "stale" and ActiveConfigs.get("detector.zscore")["params"] == {"threshold": 4.0}


def test_approved_params_are_used_by_the_next_detection_run(now):
    """預設門檻 3 只抓到 mock 的那個尖峰；核准更敏感的門檻 1.0 後，下一輪就會在尖峰之後多抓到點。"""
    first = run_context("metrics_only", now=now, strict=False)
    spike_ts = max(a.ts for a in first.anomalies)
    assert len(first.anomalies) == 1
    pid = _propose(params={"threshold": 1.0})
    _backtest(pid, now)
    _approve(pid)
    second = run_context("metrics_only", now=now, strict=False)
    assert second.anomalies and all(a.ts > spike_ts for a in second.anomalies)  # new_only：只留尖峰之後的新異常


def test_backtest_compares_current_and_proposed_on_the_same_data(now):
    pid = _propose(params={"threshold": 1.0})
    bt = _backtest(pid, now)["backtest"]
    assert bt["proposed_anomalies"] > bt["current_anomalies"] >= 1  # 門檻 1.0 比預設 3.0 敏感得多


def test_backtest_needs_peers_for_peer_detectors_and_known_proposal(now):
    pid = _propose("detector.peer")
    with pytest.raises(BadRequest, match="peers"):
        _backtest(pid, now)
    with pytest.raises(NotFound):
        _backtest(999, now)


# ── 回饋轉提案 ──


@pytest.fixture
def uid(now):
    return run_context("metrics_only", now=now, strict=False).incidents[0].uid


def _feedback(uid, verdict="false_positive", detector="detector.zscore"):
    return run("admin_feedback_submit", request=FeedbackSubmit(
        incident_uid=uid, verdict=verdict, detector=detector, text="半夜誤報，門檻太敏感", author="amy"))["feedback"]["id"]


def _propose_from(fid):
    return run("admin_feedback_propose", request=FeedbackPropose(feedback_id=fid, proposer="bot"))


def test_feedback_needs_a_real_incident_and_detector():
    with pytest.raises(NotFound):
        run("admin_feedback_submit", request=FeedbackSubmit(incident_uid="inc-nope", verdict="missed", text="x", author="a"))


def test_template_rule_raises_threshold_on_false_positive_and_only_once(uid):
    fid = _feedback(uid)
    out = _propose_from(fid)
    assert out["generated_by"] == "template" and out["proposal"]["params"] == {"threshold": 3.6}
    assert out["proposal"]["status"] == "pending"
    with pytest.raises(Conflict, match="已轉成提案"):
        _propose_from(fid)


def test_true_positive_and_untunable_detectors_do_not_propose(uid):
    with pytest.raises(Conflict, match="真異常"):
        _propose_from(_feedback(uid, verdict="true_positive"))
    with pytest.raises(BadRequest, match="沒有可自動調整"):
        _propose_from(_feedback(uid, detector="detector.rules"))


class _Fake(BaseLLM):
    name = "fake"

    def __init__(self, reply=None, error=None):
        self.reply, self.error = reply, error

    def complete(self, system, prompt):
        if self.error:
            raise LLMError(self.error)
        return json.dumps(self.reply)


def _llm_propose(monkeypatch, uid, reply=None, error=None):
    from aiops.engine.pipeline import Pipeline, load_workflow
    from aiops.interfaces.runner import workflow_path
    from aiops.storage.hooks import StorageHooks

    monkeypatch.setattr("aiops.control_plane.learning.tuning.build_llm", lambda *a, **k: _Fake(reply, error))
    wf = load_workflow(workflow_path("admin_feedback_propose"))
    wf.nodes[0].params["provider"] = "openai"
    req = FeedbackPropose(feedback_id=_feedback(uid), proposer="bot")
    return Pipeline(wf, StorageHooks()).run(request=req, strict=True).result


def test_valid_llm_rule_becomes_an_llm_proposal(monkeypatch, uid):
    out = _llm_propose(monkeypatch, uid, {"params": {"threshold": 4.0}, "reason": "夜間誤報多"})
    assert out["generated_by"] == "llm" and out["proposal"]["source"] == "llm" and out["proposal"]["params"] == {"threshold": 4.0}


@pytest.mark.parametrize("params, reason", [({"threshold": 2.0}, "方向與回饋相反"), ({"threshold": 9.0}, "變動超過"),
                                            ({"window": 5}, "不是可調參數"), ({"threshold": 50}, "超出範圍")])
def test_invalid_llm_rules_fall_back_to_the_template(monkeypatch, uid, params, reason):
    out = _llm_propose(monkeypatch, uid, {"params": params, "reason": "r"})
    assert out["generated_by"] == "template" and reason in out["rejected"] and out["proposal"]["params"] == {"threshold": 3.6}


def test_llm_outage_falls_back_to_the_template(monkeypatch, uid):
    out = _llm_propose(monkeypatch, uid, error="timeout")
    assert out["generated_by"] == "template" and "timeout" in out["rejected"]


# ── 匯入 ──


def test_duplicate_external_ids_inside_one_batch_are_rejected(now):
    a = ExternalAnomalyIn(external_id="x", entity="e", metric="m", ts=now, score=1)
    with pytest.raises(BadRequest, match="重複"):
        run("admin_ingest_anomalies", request=IngestAnomalies(pipeline="p", source="s", anomalies=[a, a]))


def test_silence_window_must_be_ordered(now):
    with pytest.raises(ValueError):
        SilenceCreate(kind="mute", start=now, end=now, reason="x", created_by="a")


def test_sources_check_without_any_configured_source_is_ok(monkeypatch):
    for var in ("AIOPS_PROMETHEUS_URL", "AIOPS_VICTORIAMETRICS_URL", "AIOPS_ES_URL"):
        monkeypatch.delenv(var, raising=False)
    out = run("admin_sources_check")
    assert out["ok"] is True and {s["source_type"] for s in out["sources"]} >= {"mock", "prometheus", "elasticsearch"}
