"""認知型 RCA 的報告整合：facts 段落、to_result 的判定狀態與事實、rca_report_cognitive.yaml 載得進來。"""
from datetime import UTC, datetime

from aiops.engine.context import PipelineContext, ReportSection
from aiops.engine.rca_types import EvidenceItem, Fact
from aiops.interfaces.runner import load_all, workflow_path
from aiops.engine.pipeline import load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident

NOW = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)


def _ctx(**kw) -> PipelineContext:
    ctx = PipelineContext(pipeline="rca_report_cognitive", now=NOW)
    ctx.incidents = [Incident(events=[AnomalyEvent(entity="ord-svc", ts=NOW, score=9.0, detector="detector.mad",
                                                   metric="m", value=1.0)])]
    for k, v in kw.items():
        setattr(ctx, k, v)
    return ctx


def _section(ctx: PipelineContext):
    load_all()
    return OperatorRegistry.create("rca.section.facts", None).execute(ctx)


def test_undetermined_section_says_why_and_points_to_raw_evidence():
    ctx = _section(_ctx(rca_status="undetermined", rca_undetermined_reason="llm_timeout：30 秒沒有回應"))
    [sec] = ctx.rca_sections
    assert sec.key == "facts"
    assert "無法判定" in sec.body and "llm_timeout" in sec.body
    assert "trace 佐證" in sec.body and "錯誤日誌" in sec.body


def test_section_lists_facts_and_evidence_without_judging():
    facts = [Fact(kind="entry_failure_rate", entity="ord-svc", value=1.0, unit="", window="incident")]
    ev = [EvidenceItem(kind="circuit_breaker_open", entity="ord-svc", quote="CircuitBreaker 'x' is OPEN")]
    [sec] = _section(_ctx(rca_facts=facts, rca_evidence=ev)).rca_sections
    assert "entry_failure_rate｜ord-svc｜1｜incident" in sec.body
    assert "circuit_breaker_open" in sec.body and "CircuitBreaker 'x' is OPEN" in sec.body


def test_section_skipped_when_nothing_to_show():
    ctx = _section(_ctx())
    assert ctx.rca_sections == [] and ctx.rca_skipped == ["facts"]


def test_to_result_shape_unchanged_for_legacy_and_extended_for_cognitive():
    legacy = _ctx(rca_sections=[ReportSection("summary", "摘要", "x")])
    assert "facts" not in legacy.to_result() and "status" not in legacy.to_result()["root_cause"]
    cog = _ctx(rca_sections=[ReportSection("summary", "摘要", "x")], rca_status="undetermined",
               rca_undetermined_reason="llm_timeout", rca_facts=[Fact(kind="traffic_drop", entity="a", value=0.5)])
    out = cog.to_result()
    assert out["root_cause"]["status"] == "undetermined" and out["root_cause"]["undetermined_reason"] == "llm_timeout"
    assert out["facts"][0]["kind"] == "traffic_drop" and out["evidence"] == []


def test_cognitive_workflow_loads_and_has_the_cognitive_nodes_in_order():
    load_all()
    wf = load_workflow(workflow_path("rca_report_cognitive"))
    ops = [n.operator for n in wf.nodes]
    order = ["ingestion.incident_traces", "ingestion.incident_logs", "rca.facts", "enrichment.extract_evidence",
             "rca.root_cause.bayes", "rca.report", "enrichment.rca_narrator"]
    assert [ops.index(o) for o in order] == sorted(ops.index(o) for o in order)
    assert "rca.root_cause.evidence" not in ops  # 新管線不掛舊判斷層
