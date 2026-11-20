"""核心引擎：註冊中心、Workflow 驗證、管線執行順序與錯誤處理。"""
from datetime import datetime, timezone
from pathlib import Path

import pytest

from aiops.engine.context import PipelineContext
from aiops.engine.errors import InfrastructureError, NotFound, WorkflowError
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.operator import BaseOperator

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)
WORKFLOWS = Path(__file__).resolve().parents[1] / "aiops" / "pipelines"


def _wf(*nodes: tuple[str, str, dict], targets=("a", "b")):
    return Workflow(name="t", nodes=[NodeSpec(name=n, operator=o, params=p) for n, o, p in nodes],
                    targets=[TargetSpec(source_type="mock", query=q, entity=q) for q in targets])


# ── 註冊中心 ──


def test_registry_rejects_duplicates_and_non_operators():
    with pytest.raises(TypeError):
        OperatorRegistry.register("detector.hourly_baseline")(type("Another", (BaseOperator,), {"execute": lambda s, c: c}))
    with pytest.raises(TypeError):
        OperatorRegistry.register("x.not_an_operator")(type("NoExecute", (), {}))


def test_unknown_operator_and_unknown_params_fail_at_construction_not_at_runtime():
    with pytest.raises(WorkflowError, match="未註冊"):
        Pipeline(_wf(("n", "detector.nope", {})))
    with pytest.raises(WorkflowError, match="參數"):
        Pipeline(_wf(("n", "detector.hourly_baseline", {"treshold": 3})))  # 拼錯
    with pytest.raises(WorkflowError, match="參數"):
        Pipeline(_wf(("n", "ingestion.fetch", {"lookback": 5})))


def test_target_nodes_must_precede_batch_nodes():
    with pytest.raises(WorkflowError, match="scope"):
        Pipeline(_wf(("g", "alert.same_entity", {}), ("f", "ingestion.fetch", {})))


def test_duplicate_node_names_rejected():
    with pytest.raises(ValueError):
        _wf(("n", "ingestion.fetch", {}), ("n", "detector.hourly_baseline", {}))


def test_every_shipped_workflow_loads_and_builds():
    paths = {p.stem: p for p in WORKFLOWS.rglob("*.yaml")}
    names = sorted(paths)
    assert len(names) >= 20
    for name in names:
        Pipeline(load_workflow(paths[name]))


def test_bad_workflow_file_is_a_workflow_error(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("name: x\nnodes: []\n", encoding="utf-8")
    with pytest.raises(WorkflowError):
        load_workflow(bad)


# ── 引擎 ──


@OperatorRegistry.register("test.mark_target")
class _MarkTarget(BaseOperator):
    def __init__(self, fail_on: str | None = None, infra: bool = False):
        self.fail_on, self.infra = fail_on, infra

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if ctx.target.entity == self.fail_on:
            raise InfrastructureError("db down") if self.infra else NotFound(f"{ctx.target.entity} 沒資料")
        ctx.result = {ctx.target.entity: "ok"}
        return ctx


@OperatorRegistry.register("test.batch_count")
class _BatchCount(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        ctx.result = {**ctx.result, "seen_in_batch": sorted(ctx.result)}
        return ctx


def test_target_nodes_run_per_target_then_batch_nodes_see_all():
    ctx = Pipeline(_wf(("m", "test.mark_target", {}), ("b", "test.batch_count", {}))).run(now=NOW)
    assert ctx.result["seen_in_batch"] == ["a", "b"] and ctx.audit[:2] == ["m:test.mark_target"] * 2


def test_one_failing_target_does_not_stop_the_others_unless_strict():
    pipe = Pipeline(_wf(("m", "test.mark_target", {"fail_on": "a"})))
    ctx = pipe.run(now=NOW)
    assert [e.entity for e in ctx.errors] == ["a"] and ctx.result == {"b": "ok"}
    with pytest.raises(NotFound):
        pipe.run(now=NOW, strict=True)


def test_infrastructure_errors_fail_the_whole_run_even_when_not_strict():
    with pytest.raises(InfrastructureError):
        Pipeline(_wf(("m", "test.mark_target", {"fail_on": "a", "infra": True}))).run(now=NOW)


def test_explicit_empty_topology_means_do_not_derive():
    wf = _wf(("b", "test.batch_count", {}), targets=())  # 只有批次節點：targets 用不到，帶了會在載入期報錯
    wf.topology = []
    ctx = Pipeline(wf).run(now=NOW)
    assert ctx.topology == [] and ctx.topology_explicit is True


def test_incident_without_events_has_no_result_noise():
    assert Pipeline(_wf(("b", "test.batch_count", {}), targets=())).run(now=NOW).to_result() == {
        "pipeline": "t", "seen_in_batch": []}
