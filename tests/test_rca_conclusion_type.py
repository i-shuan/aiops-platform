"""RCA 結論等級（engine/rca_types.ConclusionType）：confirmed／symptom_location／topology_inference／undetermined。

等級由根因演算法自己宣告（RootCauseOperator.conclusion，或 rank() 裡覆寫這一次的），報告第一行、API 回傳、事件群存檔都帶著。
端到端測試走出貨的 rca_report.yaml：事件群存進資料庫、trace 由 mock 資料源給（把 MockSpec.fetch_spans 換成這個檔案捏的 span，
欄位跟真的資料源回傳的 dict 相同）。span 是合成的：這裡驗的是「等級怎麼宣告、怎麼帶出去」與判準的邏輯，不是真實資料上的準確率
（判準的來源與未驗證泛化的聲明在 aiops/stages/rca/conclusion.py）。
"""
import json
import re
from datetime import timedelta
from pathlib import Path

import pytest

from aiops.engine.context import PipelineContext, RootCandidate, SpanRecord
from aiops.engine.pipeline import Pipeline, load_workflow
from aiops.engine.rca_types import CONCLUSION_LABELS, CONCLUSION_TYPES, Fact
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import ExternalAnomalyIn, IdRef, IngestAnomalies
from aiops.engine.types import AnomalyEvent, Incident
from aiops.interfaces.runner import run, workflow_path
from aiops.sources.mock import MockSpec
from aiops.stages.enrichment.base import BaseLLM
from aiops.stages.enrichment.registry import register
from aiops.stages.rca.base import RootCauseOperator
from aiops.stages.rca.spans import own_failures_after_dependency
from aiops.storage import crud
from aiops.storage.hooks import StorageHooks
from aiops.storage.models import IncidentRow
from aiops.storage.repository import Incidents

ROOT = Path(__file__).resolve().parents[1]
PER_MINUTE = 20  # 每分鐘幾條 trace：事件前、事件期間都超過 evidence 的 min_requests（20）
BEFORE_MINUTES = 15


def _traces(now, *, slow_c: bool = False, a_fails: str = "", a_extra_ms: float = 0.0) -> list[dict]:
    """svc-a -> svc-b -> svc-c 的呼叫鏈，事件前 15 分鐘到事件結束。事件期間 = 最後 3 分鐘（跟 _incident 的異常點對齊）。
    slow_c：事件期間 svc-c 自己變慢（10 -> 300 ms）。
    a_fails："after_dependency" = 事件期間 svc-a 一半的請求失敗，而且是 svc-b 成功回應之後才失敗（下游回空、或 svc-a 自己的 bug，
             span 長得一樣）；"at_entry" = 一半的請求在入口就失敗、還沒呼叫任何依賴。
    a_extra_ms：事件期間 svc-a 失敗的請求自己多花的時間（重試／備援計算）。"""
    out: list[dict] = []
    for m in range(BEFORE_MINUTES + 3, 0, -1):
        during = m <= 3
        for k in range(PER_MINUTE):
            ts = now - timedelta(minutes=m) + timedelta(seconds=k * 2)
            tid = f"t{m:02d}{k:02d}"
            fail = during and bool(a_fails) and k % 2 == 0
            c_ms = 300.0 if during and slow_c else 10.0
            a_ms = c_ms + 20.0 + (a_extra_ms if fail else 0.0)

            def span(sid, parent, svc, ms, error=False):
                out.append({"trace_id": tid, "span_id": f"{tid}{sid}", "parent_id": f"{tid}{parent}" if parent else None,
                            "service": svc, "ts": ts, "duration_ms": ms, "error": error})

            span("a", None, "svc-a", a_ms, fail)
            if not (fail and a_fails == "at_entry"):
                span("b", "a", "svc-b", c_ms + 10.0)
                span("c", "b", "svc-c", c_ms)
    return out


@pytest.fixture
def traces(monkeypatch):
    """把 mock 資料源的 trace 換成這個測試給的 span（只回時間窗內的；簽名跟 MockSpec.fetch_spans 一樣）。"""
    box: dict = {"spans": []}

    def fetch_spans(self, start, end, services=None, max_spans=20000, field_map=None, environment=None):
        return [s for s in box["spans"] if start <= s["ts"] <= end], False

    monkeypatch.setattr(MockSpec, "fetch_spans", fetch_spans)
    return box


def _incident(now, *, keep_topology: bool = True) -> int:
    items = [ExternalAnomalyIn(external_id=f"c{i}", entity=e, metric="latency", ts=now - timedelta(minutes=m), score=s)
             for i, (e, m, s) in enumerate([("svc-a", 1, 2), ("svc-b", 2, 3), ("svc-c", 3, 6)])]
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline="it_otel", source="x", anomalies=items))
    wf = load_workflow(workflow_path("it_otel"))
    wf.topology = [("svc-a", "svc-b"), ("svc-b", "svc-c")]
    inc_id = Pipeline(wf, StorageHooks()).run(now=now, targets=[]).to_result()["incidents"][0]["id"]
    if not keep_topology:
        crud.update(IncidentRow, inc_id, context={})
    return inc_id


def _rca(incident_id, now) -> dict:
    wf = load_workflow(workflow_path("rca_report"))
    next(n for n in wf.nodes if n.name == "fetch_traces").params["source_type"] = "mock"
    return Pipeline(wf, StorageHooks()).run(incident_id=incident_id, now=now).to_result()


def _candidates_section(result: dict) -> str:
    return {s["key"]: s["body"] for s in result["sections"]}["root_cause_candidates"]


# ── 端到端：三個等級（加上沒有候選的 undetermined）──


def test_confirmed_when_spans_show_the_candidate_itself_got_slower(now, traces):
    traces["spans"] = _traces(now, slow_c=True)
    inc_id = _incident(now)
    assert Incidents.get(inc_id)["rca_conclusion_type"] == ""  # 還沒跑過 RCA
    out = _rca(inc_id, now)
    rc = out["root_cause"]
    assert rc["candidates"][0]["entity"] == "svc-c" and rc["algorithm"] == "rca.root_cause.evidence"
    assert rc["conclusion_type"] == "confirmed" and rc["conclusion_reason"] == ""
    assert _candidates_section(out).splitlines()[0] == CONCLUSION_LABELS["confirmed"]
    row = Incidents.get(inc_id)  # 事件群存檔帶著等級與時間
    assert row["rca_conclusion_type"] == "confirmed" and row["rca_concluded_at"] == now.replace(tzinfo=None)
    assert run("admin_incident_get", request=IdRef(id=inc_id))["incident"]["rca_conclusion_type"] == "confirmed"


def test_symptom_location_when_the_candidate_only_fails_after_its_dependency_answered_ok(now, traces):
    """下游回 200 但內容不對、與呼叫端自己的 bug，span 逐筆相同（這份資料兩種都可能）：候選仍是 svc-a，但等級是「僅症狀位置」。"""
    traces["spans"] = _traces(now, a_fails="after_dependency")
    inc_id = _incident(now)
    out = _rca(inc_id, now)
    rc = out["root_cause"]
    assert rc["candidates"][0]["entity"] == "svc-a"  # 排名不變：不把候選換成下游
    assert rc["conclusion_type"] == "symptom_location"
    assert "根因未確認" in rc["conclusion_reason"] and "svc-a" in rc["conclusion_reason"]
    lines = _candidates_section(out).splitlines()
    assert lines[0] == CONCLUSION_LABELS["symptom_location"] and "根因未確認" in lines[0]
    assert lines[1] == f"（{rc['conclusion_reason']}）" and lines[2].startswith("1. svc-a：")
    assert Incidents.get(inc_id)["rca_conclusion_type"] == "symptom_location"


def test_failing_before_calling_any_dependency_stays_confirmed(now, traces):
    """對照組：一樣只有自身錯誤，但失敗的請求一個依賴都還沒呼叫到——沒有任何回應內容可以怪，維持 confirmed。"""
    traces["spans"] = _traces(now, a_fails="at_entry")
    rc = _rca(_incident(now), now)["root_cause"]
    assert rc["candidates"][0]["entity"] == "svc-a" and rc["conclusion_type"] == "confirmed"


def test_topology_inference_when_there_is_no_trace(now, traces):
    """沒有 trace：evidence 退回 topological。候選照給，但 API、報告、存檔都標成拓撲推測。"""
    inc_id = _incident(now)
    out = _rca(inc_id, now)
    rc = out["root_cause"]
    assert out["trace_fetch"]["spans"] == 0 and rc["candidates"][0]["entity"] == "svc-c"
    assert rc["algorithm"] == "rca.root_cause.evidence"  # 等級來自 fallback 算子的宣告，不是看這個名稱
    assert rc["conclusion_type"] == "topology_inference"
    assert _candidates_section(out).splitlines()[0] == CONCLUSION_LABELS["topology_inference"]
    assert Incidents.get(inc_id)["rca_conclusion_type"] == "topology_inference"


def test_undetermined_when_there_is_no_candidate(now, traces):
    inc_id = _incident(now, keep_topology=False)  # 沒有 trace、也沒有拓撲：排不出來
    out = _rca(inc_id, now)
    rc = out["root_cause"]
    assert rc["candidates"] == [] and rc["conclusion_type"] == "undetermined"
    lines = _candidates_section(out).splitlines()
    assert lines[0] == CONCLUSION_LABELS["undetermined"] and lines[1].startswith("證據不足")
    assert Incidents.get(inc_id)["rca_conclusion_type"] == "undetermined"


def test_rerunning_rca_overwrites_the_stored_conclusion(now, traces):
    inc_id = _incident(now)
    _rca(inc_id, now)
    assert Incidents.get(inc_id)["rca_conclusion_type"] == "topology_inference"
    traces["spans"] = _traces(now, slow_c=True)  # 後來 trace 補進來了
    _rca(inc_id, now)
    assert Incidents.get(inc_id)["rca_conclusion_type"] == "confirmed"


# ── 判準的邊界與死區 ──


def _stats(now, **kw):
    spans = [SpanRecord(**s) for s in _traces(now, **kw)]
    return own_failures_after_dependency(spans, now - timedelta(minutes=3), now)


def test_own_failures_after_dependency_counts_only_entries_that_already_called_something(now):
    assert _stats(now, a_fails="after_dependency") == {"svc-a": (30.0, 30.0)}
    assert _stats(now, a_fails="at_entry") == {"svc-a": (30.0, 0.0)}
    assert _stats(now, slow_c=True) == {}  # 沒有人自己失敗


def test_a_resource_leaf_counts_as_a_dependency_and_app_spans_do_not(now):
    def spans(kind):
        return [SpanRecord("t", "a", None, "svc-a", now, 20.0, True),
                SpanRecord("t", "x", "a", "svc-a", now, 5.0, False, kind=kind)]

    assert own_failures_after_dependency(spans("db"), now, now + timedelta(minutes=1)) == {"svc-a": (1.0, 1.0)}
    assert own_failures_after_dependency(spans("app"), now, now + timedelta(minutes=1)) == {"svc-a": (1.0, 0.0)}


def test_first_candidate_below_the_significance_threshold_is_only_a_symptom_location(now):
    """分數是相對值：第一名自己沒過顯著門檻（別人過了門檻但分數較低）時，不能標成 confirmed。"""
    op = OperatorRegistry.create("rca.root_cause.evidence", {})
    ctx = PipelineContext(pipeline="rca", now=now)
    stats = type("S", (), {"self_ms_per_request": 100.0})()
    op._conclude(ctx, {"entity": "svc-a", "hit": False, "b": stats}, [], now, now)
    assert ctx.rca_conclusion_type == "symptom_location" and "顯著門檻" in ctx.rca_conclusion_reason


@pytest.mark.xfail(strict=True, reason="死區 a（aiops/stages/rca/conclusion.py）：依賴回空、呼叫端除了出錯還因此變慢"
                                       "（重試、備援計算讓自身耗時過了顯著門檻）時，判準第 1 點不成立，結論仍是 confirmed")
def test_dead_zone_silent_downstream_that_also_slows_the_caller_is_still_confirmed(now, traces):
    traces["spans"] = _traces(now, a_fails="after_dependency", a_extra_ms=200.0)
    rc = _rca(_incident(now), now)["root_cause"]
    assert rc["candidates"][0]["entity"] == "svc-a"
    assert rc["conclusion_type"] == "symptom_location"


# ── LLM 判斷（rca.root_cause.llm）：同一個判準 ──


class _Judge(BaseLLM):
    name = "test-conclusion-judge"

    def __init__(self, model: str):
        self.model = model

    def complete(self, system, prompt):
        cites = {"plain": ["F1"], "with_log": ["F1", "E1"]}[self.model]
        return json.dumps({"candidates": [{"entity": "svc-a", "confidence": 0.7, "subtype": "error", "resource": "",
                                           "reason": "svc-a 自己回錯，下游沒有失敗", "cites": cites}], "note": ""},
                          ensure_ascii=False)


@register("test-conclusion-judge")
def _judge_factory(model, base_url, api_key):
    return _Judge(model)


def _llm_ctx(now, spans: list[dict]) -> PipelineContext:
    from aiops.engine.rca_types import EvidenceItem

    ctx = PipelineContext(pipeline="rca_report_llm", now=now)
    ctx.incidents = [Incident(events=[AnomalyEvent("svc-a", "errors", now - timedelta(minutes=m), 5.0) for m in (3, 2, 1)])]
    ctx.topology = [("svc-a", "svc-b"), ("svc-b", "svc-c")]
    ctx.rca_spans = [SpanRecord(**s) for s in spans]
    ctx.rca_facts = [Fact(kind="failed_without_failed_child", entity="svc-a", value=0.5, unit="ratio")]
    ctx.rca_evidence = [EvidenceItem(kind="oom", entity="svc-a", quote="java.lang.OutOfMemoryError")]
    return ctx


def _judge(ctx, model):
    ctx = OperatorRegistry.create("rca.root_cause.llm", {"provider": "test-conclusion-judge", "model": model}).execute(ctx)
    ctx = OperatorRegistry.create("rca.section.root_cause_candidates", {}).execute(ctx)
    return ctx.to_result()


def test_llm_candidate_backed_by_validated_citations_is_confirmed(now):
    out = _judge(_llm_ctx(now, _traces(now, a_fails="at_entry")), "plain")
    assert out["root_cause"]["candidates"][0]["entity"] == "svc-a" and out["root_cause"]["status"] == "ok"
    assert out["root_cause"]["conclusion_type"] == "confirmed"


def test_llm_candidate_is_downgraded_when_spans_only_show_a_symptom_location(now):
    out = _judge(_llm_ctx(now, _traces(now, a_fails="after_dependency")), "plain")
    assert out["root_cause"]["candidates"][0]["entity"] == "svc-a"
    assert out["root_cause"]["conclusion_type"] == "symptom_location"
    assert out["sections"][0]["body"].splitlines()[0] == CONCLUSION_LABELS["symptom_location"]


def test_llm_candidate_citing_its_own_log_evidence_is_not_downgraded(now):
    """它引用了關於自己的日誌證據（自己的 OOM）：那是 span 以外的直接證據，不降級。"""
    out = _judge(_llm_ctx(now, _traces(now, a_fails="after_dependency")), "with_log")
    assert out["root_cause"]["conclusion_type"] == "confirmed"


def test_llm_failure_is_undetermined(now):
    ctx = _llm_ctx(now, [])
    ctx = OperatorRegistry.create("rca.root_cause.llm", {}).execute(ctx)  # 沒設定供應商
    assert ctx.rca_status == "undetermined" and ctx.rca_conclusion_type == "undetermined"
    ctx = OperatorRegistry.create("rca.section.root_cause_candidates", {}).execute(ctx)
    assert ctx.to_result()["root_cause"]["conclusion_type"] == "undetermined"


# ── 合約：等級由演算法宣告，不看名稱 ──


def test_every_root_cause_algorithm_declares_its_conclusion():
    names = OperatorRegistry.names("rca.root_cause.")
    assert {"rca.root_cause.topological", "rca.root_cause.evidence", "rca.root_cause.llm"} <= set(names)
    for name in names:
        assert OperatorRegistry.get(name).conclusion in ("confirmed", "symptom_location", "topology_inference"), name
    assert OperatorRegistry.get("rca.root_cause.topological").conclusion == "topology_inference"


def test_an_algorithm_without_a_declared_conclusion_cannot_be_defined():
    with pytest.raises(TypeError, match="conclusion"):
        class _NoConclusion(RootCauseOperator):
            def rank(self, incident, ctx):
                return []

    with pytest.raises(TypeError, match="conclusion"):
        class _Undetermined(RootCauseOperator):
            conclusion = "undetermined"  # 沒有候選才是 undetermined，不能當成演算法的等級

            def rank(self, incident, ctx):
                return []


def test_the_algorithm_can_override_the_level_per_run_and_no_candidate_is_always_undetermined(now):
    class _Custom(RootCauseOperator):
        conclusion = "confirmed"
        operator_name = "rca.root_cause._test_custom"

        def __init__(self, mode):
            self.mode = mode

        def rank(self, incident, ctx):
            if self.mode == "override":
                ctx.rca_conclusion_type = "topology_inference"
            if self.mode == "garbage":
                ctx.rca_conclusion_type = "very_sure"
            if self.mode == "empty":
                ctx.rca_conclusion_type = "confirmed"
                return []
            return [RootCandidate("svc-a", 1.0, "r")]

    def level(mode):
        ctx = PipelineContext(pipeline="rca", now=now)
        ctx.incidents = [Incident(events=[AnomalyEvent("svc-a", "latency", now, 5.0)])]
        return _Custom(mode).execute(ctx).rca_conclusion_type

    assert level("plain") == "confirmed" and level("override") == "topology_inference"
    assert level("garbage") == "confirmed"  # 不認得的等級退回宣告的那一級
    assert level("empty") == "undetermined"


def test_report_sections_and_engine_never_branch_on_root_cause_operator_names():
    """等級怎麼來的只有一條路：演算法宣告。報告組裝、段落、引擎裡不准出現任何根因算子的全名（拿名稱判斷等級）。"""
    names = OperatorRegistry.names("rca.root_cause.")
    files = [ROOT / "aiops/stages/rca/report.py", ROOT / "aiops/stages/rca/base.py", ROOT / "aiops/stages/rca/conclusion.py",
             ROOT / "aiops/engine/context.py", ROOT / "aiops/engine/pipeline.py", ROOT / "aiops/engine/rca_types.py",
             ROOT / "aiops/storage/hooks.py", *sorted((ROOT / "aiops/stages/rca/sections").glob("*.py"))]
    for path in files:
        text = path.read_text(encoding="utf-8")
        hits = [n for n in names if re.search(rf'["\']{re.escape(n)}["\']', text)]
        assert not hits, f"{path.name} 用算子名稱 {hits} 判斷"


# ── 相容：沒有這個欄位的舊資料 ──


def test_old_incidents_and_reports_without_the_field_still_read_back(now, traces):
    inc_id = _incident(now)
    row = Incidents.get(inc_id)  # 沒跑過 RCA 的事件群（＝加欄位之前的舊列，migration 回填 ''）
    assert row["rca_conclusion_type"] == "" and row["rca_concluded_at"] is None
    assert run("admin_incident_get", request=IdRef(id=inc_id))["incident"] == row
    # 沒有根因節點的報告（舊的自訂 workflow）：欄位預設是 undetermined，段落不會因為缺欄位而出錯
    wf = load_workflow(workflow_path("rca_report"))
    wf.nodes = [n for n in wf.nodes if n.name in ("report",)]
    out = Pipeline(wf, StorageHooks()).run(incident_id=inc_id, now=now).to_result()
    assert out["root_cause"]["conclusion_type"] == "undetermined" and out["root_cause"]["candidates"] == []
    assert "root_cause_candidates" in out["skipped"]
    assert Incidents.get(inc_id)["rca_conclusion_type"] == ""  # 沒跑根因節點就不寫


def test_migration_adds_the_columns_idempotently_with_a_default_for_old_rows():
    sql = (ROOT / "migrations/postgres/0010_incident_rca_conclusion.sql").read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS rca_conclusion_type VARCHAR(32) NOT NULL DEFAULT ''" in sql
    assert "ADD COLUMN IF NOT EXISTS rca_concluded_at TIMESTAMP" in sql
    columns = IncidentRow.__table__.columns
    assert columns["rca_conclusion_type"].type.length == 32 and columns["rca_conclusion_type"].server_default.arg == ""
    assert columns["rca_concluded_at"].nullable
    assert set(CONCLUSION_TYPES) == set(CONCLUSION_LABELS) and all(len(c) <= 32 for c in CONCLUSION_TYPES)


# ── 摘要模板：推測與症狀位置不准被寫成確定的根因 ──


def _summaries(now, level: str) -> tuple[str, str]:
    """(三行止血摘要的第一行, 戰情摘要模板的第一句)，第一名固定是 svc-a、只換結論等級。"""
    from aiops.stages.enrichment import narrative, rca_narrator

    ctx = PipelineContext(pipeline="rca", now=now)
    ctx.incidents = [Incident(events=[AnomalyEvent("svc-a", "latency", now, 5.0)])]
    ctx.rca_root_candidates = [RootCandidate("svc-a", 1.0, "理由")]
    ctx.rca_conclusion_type = level
    briefing = narrative.briefing_template(narrative.build_context(ctx, ctx.incidents[0]))
    return briefing["summary_line"], rca_narrator.template(ctx, rca_narrator.build_sections(ctx))["summary"]


def test_summary_templates_say_inference_or_unconfirmed_instead_of_root_cause(now):
    line, summary = _summaries(now, "topology_inference")
    assert "推測" in line and "未確認" in line and "自身耗時" not in line  # 沒有 trace，不准寫「自身耗時上升」
    assert summary.startswith("沒有 trace 證據，依拓撲推測的源頭（未確認）：svc-a")
    line, summary = _summaries(now, "symptom_location")
    assert "根因未確認" in line and "svc-a" in line
    assert summary.startswith("根因未確認，症狀出現的位置：svc-a")
    line, summary = _summaries(now, "confirmed")  # 有直接證據的維持原本的寫法
    assert "svc-a 自身耗時或自身錯誤率上升" in line and summary.startswith("最可能的根因：svc-a")
