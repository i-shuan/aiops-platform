"""RCA 管線依事件群所屬偵測管線分流（docs/CROSS_DOMAIN_CONSISTENCY_AUDIT.md H1、H2）。

有 trace 的 IT 管線預設走 rca_report_llm；沒有 trace 的（工廠、純指標）固定走 rca_report：
新引擎在沒有 span、沒有時序事實時沒東西可判，而且不會退回拓撲／Lot 流向推測。
"""
import pytest

from aiops.engine.errors import WorkflowError
from aiops.engine.pipeline import load_workflow
from aiops.engine.requests import RcaRequest
from aiops.interfaces import runner
from aiops.interfaces.runner import DEFAULT_RCA_WORKFLOW, WORKFLOW_DIR, rca_workflow_for, workflow_path

EXPECTED = {"it_otel": "rca_report_llm", "it_apm": "rca_report_llm", "factory": "rca_report", "metrics_only": "rca_report"}


def _incident(monkeypatch, pipeline):
    monkeypatch.setattr(runner.Incidents, "get", staticmethod(lambda _id: None if pipeline is ... else {"pipeline": pipeline}))


@pytest.mark.parametrize("pipeline,expected", sorted(EXPECTED.items()))
def test_incident_is_routed_by_the_rca_workflow_of_its_detection_pipeline(monkeypatch, pipeline, expected):
    _incident(monkeypatch, pipeline)
    assert rca_workflow_for(1) == expected


def test_every_detection_pipeline_declares_an_rca_workflow_that_exists():
    """新增偵測管線時不准漏寫：沒寫會默默走預設，等於沒有決定過這個場景有沒有 trace。"""
    names = sorted(p.stem for p in (WORKFLOW_DIR / "detection").glob("*.yaml"))
    assert names == sorted(EXPECTED)
    for name in names:
        declared = load_workflow(workflow_path(name)).rca_workflow
        assert declared == EXPECTED[name]
        assert workflow_path(declared).parent.name == "rca"


def test_pipelines_without_traces_never_default_to_the_llm_engine():
    """沒有 trace 的管線走新引擎會一律無法判定，也接不到 topological：分流不准把它們送過去。"""
    for name in ("factory", "metrics_only"):
        rca = load_workflow(workflow_path(load_workflow(workflow_path(name)).rca_workflow))
        root = [n for n in rca.nodes if n.operator.startswith("rca.root_cause.")]
        assert [n.operator for n in root] == ["rca.root_cause.evidence"]
        assert root[0].params.get("fallback", "rca.root_cause.topological") == "rca.root_cause.topological"


def test_an_explicit_workflow_wins_and_does_not_look_up_the_incident(monkeypatch):
    def boom(_id):
        raise AssertionError("指定了 workflow 就不該查事件群")
    monkeypatch.setattr(runner.Incidents, "get", staticmethod(boom))
    assert rca_workflow_for(1, "rca_report") == "rca_report"
    assert RcaRequest(incident_id=1).workflow is None


@pytest.mark.parametrize("pipeline", [..., None, "", "my_custom_pipeline", "../etc/passwd"])
def test_unknown_incident_or_pipeline_falls_back_to_the_engine_that_needs_no_llm(monkeypatch, pipeline):
    """事件群不存在、舊資料沒記管線、管線是排程 CLI 用檔案載入的、名稱不合法：都用不需要 LLM 的那條。"""
    _incident(monkeypatch, pipeline)
    assert rca_workflow_for(1) == DEFAULT_RCA_WORKFLOW == "rca_report"


def test_a_misspelled_rca_workflow_fails_when_the_pipeline_is_built(monkeypatch):
    original = runner.load_workflow

    def misspelled(path):
        wf = original(path)
        wf.rca_workflow = "rca_reprot"
        return wf

    monkeypatch.setattr(runner, "load_workflow", misspelled)
    with pytest.raises(WorkflowError, match="rca_reprot"):
        runner.build_pipeline("it_otel")
