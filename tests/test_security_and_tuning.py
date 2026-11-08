"""路徑穿越、節點血緣、調參分層、參數合併、回測切段、LLM 註冊表。"""
import shutil
import sys

import pytest

from aiops import runner
from aiops.core.context import PipelineContext
from aiops.core.errors import BadRequest
from aiops.core.params import ANY, deep_merge, pick_most_specific
from aiops.core.pipeline import NodeSpec, Pipeline, load_workflow
from aiops.core.requests import (
    FeedbackPropose,
    FeedbackSubmit,
    ProposalApprove,
    ProposalBacktest,
    ProposalCreate,
)
from aiops.operators.detectors.chronic import ChronicDetector
from aiops.operators.llm import registry
from aiops.operators.llm.base import BaseLLM
from aiops.operators.llm.providers import build_llm
from aiops.runner import run, run_context, workflow_path
from aiops.storage import crud
from aiops.storage.hooks import StorageHooks
from aiops.storage.models import AnomalyRow
from aiops.storage.repository import ActiveConfigs, DetectorModels

ZSCORE = "detector.zscore"


# ── P0-1 路徑穿越 ──


@pytest.mark.parametrize("bad", ["../x", "..", "a/b", "a\\b", "x.yaml", "", "a\x00", "a\n", "a b", "/etc/passwd",
                                 "C:\\Windows\\win.ini"])
def test_workflow_names_outside_the_whitelist_are_rejected(bad):
    with pytest.raises(BadRequest):
        workflow_path(bad)


@pytest.mark.parametrize("bad", ["../x", "a/b", "x.yaml", "", "a\\b", "a\nb"])
def test_api_rejects_path_like_workflow_names(client, bad):
    assert client.post("/api/v1/runs", json={"workflow": bad}).status_code == 422
    assert client.post("/api/v1/rca", json={"incident_id": 1, "workflow": bad}).status_code == 422


def test_builtin_names_still_work():
    assert workflow_path("metrics_only").name == "metrics_only.yaml"


def test_symlink_pointing_outside_is_refused(tmp_path, monkeypatch):
    wf_dir, outside = tmp_path / "wf", tmp_path / "outside.yaml"
    wf_dir.mkdir()
    outside.write_text("name: evil\nnodes: []\n", encoding="utf-8")
    try:
        (wf_dir / "evil.yaml").symlink_to(outside)
    except (OSError, NotImplementedError):
        pytest.skip("這個環境不能建立軟連結")
    monkeypatch.setattr(runner, "WORKFLOW_DIR", wf_dir)
    with pytest.raises(BadRequest, match="軟連結"):
        workflow_path("evil")


def test_symlink_branch_without_needing_symlink_privileges(monkeypatch):
    """Windows 一般帳號建不了軟連結，上面那個測試會略過；這裡直接讓 is_symlink 回 True 驗證防護本身。"""
    from pathlib import Path

    monkeypatch.setattr(Path, "is_symlink", lambda self: self.name == "metrics_only.yaml")
    with pytest.raises(BadRequest, match="軟連結"):
        workflow_path("metrics_only")


def test_resolved_path_outside_the_workflow_dir_is_refused(tmp_path, monkeypatch):
    wf_dir = tmp_path / "wf"
    wf_dir.mkdir()
    (wf_dir / "a.yaml").write_text("name: a\nnodes: []\n", encoding="utf-8")
    monkeypatch.setattr(runner, "WORKFLOW_DIR", wf_dir)
    from pathlib import Path

    real_resolve = Path.resolve
    monkeypatch.setattr(Path, "resolve",
                        lambda self, *a, **k: tmp_path / "elsewhere.yaml" if self.name == "a.yaml" else real_resolve(self, *a, **k))
    with pytest.raises(BadRequest, match="不在"):
        workflow_path("a")


def test_cli_may_load_its_own_file_but_the_api_path_may_not(tmp_path, now):
    custom = tmp_path / "mine.yaml"
    shutil.copy(workflow_path("metrics_only"), custom)
    with pytest.raises(BadRequest):
        workflow_path(str(custom))  # 預設（API 用的）不接受檔案路徑
    assert workflow_path(str(custom), allow_file=True) == custom
    from aiops.tasks.__main__ import run_once

    assert run_once(str(custom), now) == 0


# ── P0-2 節點血緣 ──


def _two_zscore_nodes():
    wf = load_workflow(workflow_path("metrics_only"))
    at = next(i for i, n in enumerate(wf.nodes) if n.name == "detect")
    wf.nodes[at:at + 1] = [NodeSpec(name="detect_a", operator=ZSCORE, params={"threshold": 1.0}),
                           NodeSpec(name="detect_b", operator=ZSCORE, params={"threshold": 9.0})]
    return Pipeline(wf, StorageHooks())


def test_same_algorithm_twice_keeps_separate_baselines_and_lineage(now):
    ctx = _two_zscore_nodes().run(now=now)
    assert {a.detector_node for a in ctx.anomalies} == {"detect_a", "detect_b"}
    assert {a.detector for a in ctx.anomalies} == {ZSCORE}
    for node in ("detect_a", "detect_b"):
        assert DetectorModels.get("metrics_only", "metric_a", "metric_a", ZSCORE, node)["points"] > 0
    rows = crud.find(AnomalyRow)
    assert {r["detector_node"] for r in rows} == {"detect_a", "detect_b"}
    assert {r["detector_name"] for r in rows} == {ZSCORE}  # 算子名，不再是演算法短名或「最後一個偵測器」


def test_each_anomaly_is_stored_with_its_own_detector_when_algorithms_differ(now):
    wf = load_workflow(workflow_path("metrics_only"))
    at = next(i for i, n in enumerate(wf.nodes) if n.name == "detect")
    wf.nodes[at:at + 1] = [NodeSpec(name="z", operator=ZSCORE, params={"threshold": 1.0}),
                           NodeSpec(name="m", operator="detector.mad", params={"threshold": 10.0})]
    Pipeline(wf, StorageHooks()).run(now=now)
    by_node = {}
    for r in crud.find(AnomalyRow):
        by_node.setdefault(r["detector_node"], set()).add(r["detector_name"])
    assert by_node["z"] == {ZSCORE} and by_node["m"] == {"detector.mad"}


def test_anomaly_event_roundtrip_keeps_lineage(now):
    ctx = run_context("metrics_only", now=now, strict=False)
    event = ctx.incidents[0].events[0]
    assert event.detector == ZSCORE and event.detector_node == "detect"
    from aiops.core.types import AnomalyEvent

    again = AnomalyEvent.from_dict(event.to_dict())
    assert (again.detector, again.detector_node) == (ZSCORE, "detect")


# ── P0-3 調參分層 ──


def _set(params, pipeline=ANY, entity=ANY, detector=ZSCORE):
    return ActiveConfigs.set(detector, params, "manual", 1, expected_version=0, pipeline=pipeline, entity=entity)


def _resolve(pipeline, entity):
    row = pick_most_specific(ActiveConfigs.candidates(ZSCORE, pipeline, entity), pipeline, entity)
    return row["params"]["threshold"] if row else None


def test_fallback_order_is_pipeline_entity_then_pipeline_then_entity_then_global():
    assert _resolve("p1", "cpu") is None
    _set({"threshold": 5.0})
    _set({"threshold": 6.0}, entity="cpu")
    _set({"threshold": 7.0}, pipeline="p1")
    _set({"threshold": 8.0}, pipeline="p1", entity="cpu")
    assert _resolve("p1", "cpu") == 8.0  # 管線×實體
    assert _resolve("p1", "other") == 7.0  # 管線
    assert _resolve("p2", "cpu") == 6.0  # 實體
    assert _resolve("p2", "other") == 5.0  # 全域


def test_candidates_come_from_a_single_query(now, monkeypatch):
    _set({"threshold": 5.0})
    calls = []
    real = crud.find_matching
    monkeypatch.setattr(crud, "find_matching", lambda *a, **k: (calls.append(1), real(*a, **k))[1])
    _two_zscore_nodes().run(now=now)  # 兩個節點都是 zscore、一條序列
    assert len(calls) == 1


def test_entity_names_with_colons_do_not_collide():
    _set({"threshold": 6.0}, pipeline="p", entity="ns:pod-1")
    _set({"threshold": 7.0}, pipeline="p", entity="ns")
    assert _resolve("p", "ns:pod-1") == 6.0 and _resolve("p", "ns") == 7.0 and _resolve("p", "pod-1") is None


def test_a_tuned_pipeline_does_not_affect_other_pipelines(now):
    _set({"threshold": 1.0}, pipeline="some_other_pipeline")
    assert len(run_context("metrics_only", now=now, strict=False).anomalies) == 1  # 不受影響：只抓到尖峰


def test_tuning_for_this_pipeline_is_applied(now):
    _set({"threshold": 1.0}, pipeline="metrics_only")
    ctx = run_context("metrics_only", now=now, strict=False)
    assert len(ctx.anomalies) > 1
    assert any(line.startswith("params:detect:detector.zscore@metrics_only/*") for line in ctx.audit)


def test_proposals_for_different_layers_do_not_invalidate_each_other(now):
    def propose(pipeline, entity):
        return run("admin_proposal_create", request=ProposalCreate(
            source="manual", detector=ZSCORE, params={"threshold": 4.0}, proposer="a", pipeline=pipeline,
            entity=entity))["proposal"]["id"]

    def backtest(pid):
        req = ProposalBacktest(proposal_id=pid, metric={"query": "cpu"}, train_ratio=0.3,
                               time_range={"start": now.replace(hour=0), "end": now})
        run("admin_proposal_backtest", request=req, targets=req.to_targets(), now=now)

    entity_level, pipeline_level = propose("p", "cpu"), propose("p", ANY)
    backtest(entity_level)
    backtest(pipeline_level)
    run("admin_proposal_approve", request=ProposalApprove(proposal_id=pipeline_level, approver="b"))
    run("admin_proposal_approve", request=ProposalApprove(proposal_id=entity_level, approver="b"))  # 不同層，不算過期
    assert ActiveConfigs.get(ZSCORE, "p", ANY)["version"] == 1 and ActiveConfigs.get(ZSCORE, "p", "cpu")["version"] == 1


# ── P0-4 參數合併 ──


def test_deep_merge_merges_nested_dicts_and_leaves_inputs_alone():
    base = {"thresholds": {"upper": 3.0, "lower": -3.0}, "window": 10}
    override = {"thresholds": {"upper": 5.0}}
    assert deep_merge(base, override) == {"thresholds": {"upper": 5.0, "lower": -3.0}, "window": 10}
    assert base["thresholds"]["upper"] == 3.0 and override == {"thresholds": {"upper": 5.0}}


def _chronic_ctx(tuned):
    ctx = PipelineContext(pipeline="p", now=None)
    ctx.detector_params["detect"] = tuned
    return ctx


def test_yaml_keys_are_locked_but_other_keys_take_the_tuned_value():
    op = ChronicDetector(threshold=5.0)
    op.node_name = "detect"
    ctx = _chronic_ctx({"threshold": 2.0, "window": 10})
    assert op.effective_params(ctx) == {"threshold": 5.0, "window": 10}  # YAML 沒寫 window，所以吃調參
    assert any("yaml-locked:threshold" in line for line in ctx.audit)


def test_unknown_tuned_keys_are_ignored_instead_of_failing():
    op = ChronicDetector()
    op.node_name = "detect"
    ctx = _chronic_ctx({"threshold": 2.0, "removed_in_new_version": 1})
    assert op.effective_params(ctx) == {"threshold": 2.0}
    assert any("ignored-unknown:removed_in_new_version" in line for line in ctx.audit)


# ── P1-5 回測切段 ──


def _propose(detector=ZSCORE, params=None):
    return run("admin_proposal_create", request=ProposalCreate(
        source="manual", detector=detector, params=params or {"threshold": 1.0}, proposer="a"))["proposal"]["id"]


def _backtest(pid, now, train_ratio, start=None):
    req = ProposalBacktest(proposal_id=pid, metric={"query": "cpu"}, train_ratio=train_ratio,
                           time_range={"start": start or now.replace(hour=0), "end": now})
    return run("admin_proposal_backtest", request=req, targets=req.to_targets(), now=now)["proposal"]["backtest"]


def test_spike_in_the_training_segment_is_not_scored(now):
    # mock 的尖峰在正中間：70% 訓練段把它吃進去了，測試段沒有尖峰
    bt = _backtest(_propose(), now, 0.7)
    assert bt["train_points"] + bt["test_points"] == bt["points"] and bt["test_points"] < bt["train_points"]
    assert bt["current_anomalies"] == 0


def test_spike_in_the_test_segment_is_scored(now):
    bt = _backtest(_propose(), now, 0.3)
    assert bt["current_anomalies"] >= 1 and bt["proposed_anomalies"] > bt["current_anomalies"]
    assert bt["split_at"]


def test_rolling_detectors_get_warmup_points_that_are_not_scored(now):
    bt = _backtest(_propose("detector.chronic", {"threshold": 3.0, "window": 30}), now, 0.3)
    assert bt["warmup_points"] == 29


def test_too_little_data_to_split_is_rejected(now):
    with pytest.raises(BadRequest, match="切不出"):
        _backtest(_propose(), now, 0.1, start=now.replace(hour=11, minute=56))


# ── 回饋 -> 提案的層級 ──


def _feedback_for_incident(now):
    uid = run_context("metrics_only", now=now, strict=False).incidents[0].uid
    return lambda: run("admin_feedback_submit", request=FeedbackSubmit(
        incident_uid=uid, verdict="false_positive", detector=ZSCORE, text="誤報", author="a"))["feedback"]["id"]


def _propose_from(fid, **kw):
    return run("admin_feedback_propose", request=FeedbackPropose(feedback_id=fid, proposer="bot", **kw))["proposal"]


def test_feedback_scopes_pick_the_right_layer(now):
    new_feedback = _feedback_for_incident(now)
    assert _propose_from(new_feedback(), scope="global")["pipeline"] == ANY
    pipeline_level = _propose_from(new_feedback())  # 預設 scope=pipeline
    assert (pipeline_level["pipeline"], pipeline_level["entity"]) == ("metrics_only", ANY)
    entity_level = _propose_from(new_feedback(), scope="entity")  # 事件群只有一個實體，可省略 entity
    assert (entity_level["pipeline"], entity_level["entity"]) == ("metrics_only", "metric_a")


def test_feedback_entity_must_belong_to_the_incident(now):
    new_feedback = _feedback_for_incident(now)
    with pytest.raises(BadRequest, match="不在這個事件群"):
        _propose_from(new_feedback(), scope="entity", entity="somebody-else")


def test_feedback_proposal_starts_from_the_layers_current_params(now):
    _set({"threshold": 4.0}, pipeline="metrics_only")
    proposal = _propose_from(_feedback_for_incident(now)())
    assert proposal["params"]["threshold"] == pytest.approx(4.8)  # 4.0 調高 20%，而不是從預設 3.0 起算
    assert proposal["base_version"] == 1


# ── P1-6 LLM 註冊表 ──


class _Fake(BaseLLM):
    name = "fake"

    def complete(self, system, prompt):
        return "{}"


def test_registry_lists_builtin_providers_without_anyone_importing_them_first():
    assert {"noop", "openai", "anthropic"} <= set(registry.names())


def test_a_new_provider_needs_only_the_decorator(monkeypatch):
    monkeypatch.setattr(registry, "_ENTRIES", dict(registry._ENTRIES))

    @registry.register("fakeprov", requires_model=False)
    def _fake(model, base_url, api_key):
        return _Fake()

    assert "fakeprov" in registry.names()
    assert isinstance(build_llm("fakeprov"), _Fake)


def test_modules_dropped_into_the_folder_are_discovered(tmp_path, monkeypatch):
    import aiops.operators.llm as package

    (tmp_path / "zzz_provider.py").write_text(
        "from aiops.operators.llm.registry import register\n"
        "@register('zzz', requires_model=False)\n"
        "def _zzz(model, base_url, api_key):\n    return None\n", encoding="utf-8")
    monkeypatch.setattr(package, "__path__", [*package.__path__, str(tmp_path)])
    monkeypatch.setattr(registry, "_ENTRIES", dict(registry._ENTRIES))
    monkeypatch.setattr(registry, "_scanned", False)
    monkeypatch.delitem(sys.modules, "aiops.operators.llm.zzz_provider", raising=False)
    assert "zzz" in registry.names()
    monkeypatch.delitem(sys.modules, "aiops.operators.llm.zzz_provider", raising=False)


def test_unknown_provider_error_lists_the_available_ones():
    from aiops.operators.llm.base import LLMError

    with pytest.raises(LLMError, match="noop"):
        build_llm("mystery", "m")
