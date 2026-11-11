"""場景 24（LLM 全掛）的離線版：混沌資料庫暫停期間，用出貨的 rca_report_llm.yaml 驗同一組保證。

跟 scripts/chaos/scen_llm_outage.py 的差別只在資料來源：這裡不跑世界模擬器、不連 ES／PostgreSQL，事件群、拓撲、
證據層事實（ctx.rca_facts）與錯誤日誌（ctx.rca_logs）直接放進 ctx，然後依序執行 YAML 裡 rca.facts 之後的節點
（extract_evidence -> root_cause -> report -> narrator -> briefing），參數原樣、LLM 節點用 scripted_llm.wire 接，
接法跟場景 24 完全一樣。三段撈取（trace、拓撲、日誌）與 rca.facts 本身不在這裡跑（它們各有自己的測試）。

保證（CLAUDE.md §1 第 3 條；斷言跟場景 24 的 _check_undetermined／_check_faithful 對應）：
  LLM 沒設定／連不上／逾時／亂回／幻覺 -> 報告照出（有段落、不丟例外）、rca_status = "undetermined"、
  rca_undetermined_reason 有代碼與原因、沒有任何根因候選（不退回舊規則硬猜）、報告寫「無法判定」、事實照列；
  每個 LLM 節點最多呼叫 1 次（retries 0，不重試）；逾時有生效。
  對照組 faithful：熔斷證據通過驗證（quote 逐字在日誌裡）、狀態 ok、有根因候選。
"""
import time
from datetime import datetime, timedelta, timezone

import pytest

from aiops.engine.context import LogRecord, PipelineContext
from aiops.engine.pipeline import load_workflow
from aiops.engine.rca_types import Fact
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.interfaces.runner import load_all, workflow_path
from aiops.stages.enrichment import evidence_extractor as ex
from scripts.chaos import scripted_llm as LLM

_REAL_SLEEP = time.sleep  # conftest 會把 time.sleep 換成記錄器；slow 模式要真的卡住，逾時才驗得到
NOW = datetime(2026, 11, 9, 12, 30, tzinfo=timezone.utc)
CB = "io.github.resilience4j.circuitbreaker.CallNotPermittedException: CircuitBreaker 'payment-gw' is OPEN and does not permit further calls"
LLM_NODES = set(LLM.LLM_OPERATORS)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    load_all()
    monkeypatch.delenv(ex.PROVIDER_ENV, raising=False)
    LLM.CALLS.clear()


def _ctx() -> PipelineContext:
    ctx = PipelineContext(pipeline="rca_report_llm", now=NOW)
    t = NOW - timedelta(minutes=20)
    ctx.incidents = [Incident(events=[
        AnomalyEvent(entity="ord-svc", metric="errors", ts=t + timedelta(minutes=m), score=8.0, source_type="elasticsearch")
        for m in range(10)])]
    ctx.topology = [("ord-svc", "pay-api")]
    ctx.rca_facts = [
        Fact(kind="entry_failure_rate", entity="ord-svc", value=1.0, window="incident"),
        Fact(kind="wait_delta", entity="ord-svc", value=-18.0, unit="ms", window="incident",
             detail={"target": "payment-gw:443", "target_kind": "external", "before": 20.0, "during": 2.0}),
    ]
    ctx.rca_logs = [LogRecord(t + timedelta(minutes=3, seconds=i), "ord-svc", CB, "CallNotPermittedException") for i in range(5)]
    ctx.rca_logs_baseline = []
    ctx.rca_log_window_minutes = (20.0, 60.0)
    return ctx


def _run(mode: str, timeout: float | None = None) -> tuple[PipelineContext, float]:
    """出貨的 rca_report_llm.yaml，rca.facts 之後的節點依序跑（LLM 節點照場景 24 接成 scripted_llm 的 mode）。"""
    wf = load_workflow(workflow_path("rca_report_llm"))
    if timeout is not None:
        for n in wf.nodes:
            if n.operator in LLM_NODES and "timeout_seconds" in n.params:
                n.params = {**n.params, "timeout_seconds": timeout}
    LLM.wire(wf, mode)
    names = [n.operator for n in wf.nodes]
    nodes = wf.nodes[names.index("rca.facts") + 1:]
    ctx = _ctx()
    t = time.time()
    for n in nodes:
        op = OperatorRegistry.create(n.operator, n.params)
        op.node_name = n.name
        ctx = op.execute(ctx)
    return ctx, time.time() - t


def _report_text(ctx: PipelineContext) -> str:
    return "\n".join(s.body for s in ctx.rca_sections) + "\n" + (ctx.rca_root_note or "")


def _calls_per_node() -> dict[str, int]:
    out: dict[str, int] = {}
    for c in LLM.CALLS:
        out[c["system"][:20]] = out.get(c["system"][:20], 0) + 1
    return out


def _assert_undetermined(ctx: PipelineContext, mode: str):
    assert any(c["mode"] == mode for c in LLM.CALLS), "假 LLM 沒被叫到（LLM 節點沒接上）"
    assert ctx.rca_sections, "報告沒有段落"
    assert ctx.rca_status == "undetermined"
    code, _, why = ctx.rca_undetermined_reason.partition("：")
    assert code.startswith("llm_") and why, f"原因要有代碼與說明：{ctx.rca_undetermined_reason!r}"
    assert ctx.rca_root_candidates == [], "不准編造根因（也不准退回舊規則）"
    assert "無法判定" in _report_text(ctx)
    assert "facts" in {s.key for s in ctx.rca_sections}, "證據層的事實要照列在報告裡"
    assert all(n == 1 for n in _calls_per_node().values()), f"有節點被重試：{_calls_per_node()}"


@pytest.mark.parametrize("mode", ["down", "garbage", "hallucinate"])
def test_llm_outage_keeps_the_report_and_marks_it_undetermined(mode):
    ctx, _ = _run(mode)
    _assert_undetermined(ctx, mode)


def test_llm_not_configured_is_undetermined_without_any_call(monkeypatch):
    wf = load_workflow(workflow_path("rca_report_llm"))
    assert all("provider" not in n.params for n in wf.nodes if n.operator in ("enrichment.extract_evidence",
                                                                             "rca.root_cause.llm"))
    names = [n.operator for n in wf.nodes]
    ctx = _ctx()
    for n in wf.nodes[names.index("rca.facts") + 1:]:
        op = OperatorRegistry.create(n.operator, n.params)
        op.node_name = n.name
        ctx = op.execute(ctx)
    assert ctx.rca_status == "undetermined" and ctx.rca_undetermined_reason.startswith("llm_not_configured")
    assert ctx.rca_root_candidates == [] and ctx.rca_sections and "無法判定" in _report_text(ctx)


def test_llm_timeout_gives_up_at_the_node_timeout_without_retrying(monkeypatch):
    monkeypatch.setattr(LLM.time, "sleep", _REAL_SLEEP)
    ctx, elapsed = _run("slow", timeout=0.3)  # 假 LLM 睡 0.3 + 5 秒
    _assert_undetermined(ctx, "slow")
    assert ctx.rca_undetermined_reason.startswith("llm_timeout")
    assert elapsed < 5.0, f"等到假 LLM 睡完了（{elapsed:.1f} 秒）：逾時沒生效"


def test_retries_stay_zero_in_the_shipped_workflow():
    wf = load_workflow(workflow_path("rca_report_llm"))
    by_op = {n.operator: n.params for n in wf.nodes}
    assert by_op["enrichment.extract_evidence"]["retries"] == 0 and by_op["rca.root_cause.llm"]["retries"] == 0


def test_faithful_control_passes_validation_and_names_a_root_cause():
    ctx, _ = _run("faithful")
    assert ctx.rca_status == "ok", ctx.rca_undetermined_reason
    cb = [e for e in ctx.rca_evidence if e.kind == "circuit_breaker_open"]
    assert [e.entity for e in cb] == ["ord-svc"] and all(e.quote in CB for e in cb)
    assert ctx.rca_root_candidates and ctx.rca_root_candidates[0].entity == "ord-svc"
