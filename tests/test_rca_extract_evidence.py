"""認知型 RCA 的 LLM 兩段：enrichment.extract_evidence（日誌 → 驗證過的證據；失敗 → undetermined）
與 enrichment.rca_narrator（戰情摘要；失敗 → 模板，不動根因結論與 rca_status）。

假供應商只登記在這個測試檔（CLAUDE.md §100.2：aiops/ 裡不放測試專用的供應商）。
"""
import json
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from aiops.engine.context import LogRecord, PipelineContext, RootCandidate
from aiops.engine.errors import WorkflowError, WorkflowParamError
from aiops.engine.rca_types import EvidenceItem, Fact
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.enrichment import evidence_extractor as ex
from aiops.stages.enrichment.base import BaseLLM, LLMError
from aiops.stages.enrichment.registry import register

NOW = datetime(2026, 11, 9, 12, 30, tzinfo=timezone.utc)
CB = "CircuitBreaker 'payment' is OPEN and does not permit further calls (request 17)"
GC = "GC(142) Pause Full (Allocation Failure) 3840ms"
DL = "org.postgresql.util.PSQLException: ERROR: deadlock detected"

_GOOD = [
    {"kind": "circuit_breaker_open", "entity": "checkout", "state": "present", "confidence": 0.9, "line": "L1",
     "quote": "CircuitBreaker 'payment' is OPEN"},
    {"kind": "gc_pause", "entity": "order-svc", "confidence": 0.8, "quote": "Pause Full (Allocation Failure) 3840ms"},
]


class _Fake(BaseLLM):
    """model 名稱決定行為；calls 記在 class 上給測試數（每次 build 都是新物件）。"""

    name = "test-fake"
    calls: list[str] = []

    def __init__(self, mode: str):
        self.mode = mode

    def complete(self, system, prompt):
        _Fake.calls.append(self.mode)
        if self.mode == "error":
            raise LLMError("HTTP 503")
        if self.mode == "slow":
            threading.Event().wait(1.0)  # conftest 把 time.sleep 換成記錄器了，這裡要真的卡住
            return json.dumps({"evidence": _GOOD})
        if self.mode == "badjson":
            return "我覺得是 payment 壞了"
        if self.mode == "empty":
            return '{"evidence": []}'
        if self.mode == "noevidence":
            return json.dumps({"items": []})
        if self.mode == "hallucinate":
            return json.dumps({"evidence": [{"kind": "deadlock", "entity": "inventory", "confidence": 0.9,
                                             "quote": "deadlock detected on table orders"}]})
        if self.mode == "badkind":
            return json.dumps({"evidence": [{"kind": "cosmic_ray", "entity": "checkout", "confidence": 0.5, "quote": "OPEN"}]})
        if self.mode == "mixed":
            return json.dumps({"evidence": _GOOD + [
                {"kind": "deadlock", "entity": "inventory", "confidence": 0.7, "quote": "ERROR: deadlock detected"},
                {"kind": "deadlock", "entity": "inventory", "confidence": 0.7, "quote": "ERROR: deadlock detected"},  # 重複
                {"kind": "oom", "entity": "checkout", "confidence": 0.9, "quote": "OutOfMemoryError"},           # 幻覺
                {"kind": "gc_pause", "entity": "made-up-svc", "confidence": 0.9, "quote": "GC(142)"},           # 未知實體
                {"kind": "gc_pause", "entity": "order-svc", "confidence": 1.7, "quote": "GC(142)"},             # 信心超界
                {"kind": "gc_pause", "entity": "order-svc", "state": "maybe", "quote": "GC(142)"},              # state 不合
                {"kind": ["gc_pause"], "entity": "order-svc", "quote": "GC(142)"},                              # kind 型別錯
                "不是物件",
            ]})
        if self.mode == "narrate_ok":
            return json.dumps({"summary": "checkout 熔斷 payment，最可能的根因是 payment（機率 0.71）。",
                               "claims": [{"text": "payment 機率 0.71", "evidence": ["root_candidates"]},
                                          {"text": "checkout 日誌出現熔斷", "evidence": ["evidence"]}]})
        if self.mode == "narrate_number":
            return json.dumps({"summary": "payment 錯誤率 87%", "claims": [{"text": "x", "evidence": ["root_candidates"]}]})
        if self.mode == "narrate_outsider":
            return json.dumps({"summary": "search-svc 也受影響", "claims": [{"text": "x", "evidence": ["root_candidates"]}]})
        if self.mode == "narrate_undetermined_ok":
            return json.dumps({"summary": "無法判定根因：LLM 逾時，請人工看日誌。", "claims": [{"text": "無法判定", "evidence": ["rca_status"]}]})
        if self.mode == "narrate_claims_cause":
            return json.dumps({"summary": "根因是 checkout。", "claims": [{"text": "checkout", "evidence": ["rca_status"]}]})
        return json.dumps({"evidence": _GOOD})


@register("test-fake")
def _fake_factory(model, base_url, api_key):
    return _Fake(model)


@pytest.fixture(autouse=True)
def _reset_calls(monkeypatch):
    _Fake.calls = []
    monkeypatch.delenv(ex.PROVIDER_ENV, raising=False)


def _ctx(logs=True) -> PipelineContext:
    ctx = PipelineContext(pipeline="rca_report_llm", now=NOW)
    ev = [AnomalyEvent(entity=e, metric="latency", ts=NOW - timedelta(minutes=5), score=5.0)
          for e in ("checkout", "order-svc", "inventory")]
    ctx.incidents = [Incident(events=ev)]
    ctx.topology = [("checkout", "payment"), ("checkout", "order-svc"), ("order-svc", "inventory")]
    if logs:
        t = NOW - timedelta(minutes=10)
        ctx.rca_logs = (
            [LogRecord(t + timedelta(seconds=i), "checkout", CB.replace("17", str(17 + i)), "CallNotPermittedException")
             for i in range(5)]
            + [LogRecord(t, "order-svc", GC, "GcPause"), LogRecord(t + timedelta(seconds=30), "inventory", DL, "PSQLException")])
        ctx.rca_logs_baseline = [LogRecord(t - timedelta(minutes=40), "checkout", "connection reset by peer", "IOException")]
        ctx.rca_log_window_minutes = (30.0, 60.0)
    return ctx


def _extract(ctx, **params):
    params.setdefault("provider", "test-fake")
    return OperatorRegistry.create("enrichment.extract_evidence", params).execute(ctx)


# ── 抽取：成功與驗證 ──


def test_valid_evidence_is_accepted_and_log_ts_comes_from_the_quoted_line():
    ctx = _extract(_ctx(), model="ok")
    assert ctx.rca_status == "ok" and ctx.rca_undetermined_reason == ""
    assert [(e.kind, e.entity, e.state) for e in ctx.rca_evidence] == [
        ("circuit_breaker_open", "checkout", "present"), ("gc_pause", "order-svc", "present")]
    cb = ctx.rca_evidence[0]
    assert isinstance(cb, EvidenceItem) and cb.confidence == 0.9
    assert cb.log_ts == (NOW - timedelta(minutes=10)).isoformat()  # L1 是 checkout 第一筆
    assert ctx.rca_evidence[1].log_ts == (NOW - timedelta(minutes=10)).isoformat()


def test_each_invalid_item_is_dropped_with_its_reason_and_counted_in_the_log():
    ctx = _extract(_ctx(), model="mixed")
    assert ctx.rca_status == "ok"
    assert [(e.kind, e.entity) for e in ctx.rca_evidence] == [
        ("circuit_breaker_open", "checkout"), ("gc_pause", "order-svc"), ("deadlock", "inventory")]  # 重複的只留一筆
    rejected = [a for a in ctx.audit if a.startswith("extract_evidence 拒絕")]
    reasons = "\n".join(rejected)
    assert len(rejected) == 6
    for needle in ("逐字", "已知實體", "confidence", "state", "kind 不在詞表", "不是 JSON 物件"):
        assert needle in reasons, needle
    assert any("採用 3、拒絕 6" in a for a in ctx.audit)


def test_hallucinated_quote_alone_makes_the_report_undetermined():
    ctx = _extract(_ctx(), model="hallucinate")
    assert ctx.rca_status == "undetermined" and ctx.rca_evidence == []
    assert ctx.rca_undetermined_reason.startswith("llm_all_rejected：") and "逐字" in ctx.rca_undetermined_reason


def test_kind_outside_the_vocabulary_is_rejected():
    ctx = _extract(_ctx(), model="badkind")
    assert ctx.rca_status == "undetermined" and "cosmic_ray" in ctx.rca_undetermined_reason


def test_an_empty_evidence_list_is_not_a_failure():
    """LLM 看完日誌說「沒有符合的現象」是合法答案，不是被拒。"""
    ctx = _extract(_ctx(), model="empty")
    assert ctx.rca_status == "ok" and ctx.rca_evidence == [] and _Fake.calls == ["empty"]


# ── 抽取：失敗 → undetermined ──


@pytest.mark.parametrize("model, code", [
    ("error", "llm_error"),
    ("badjson", "llm_bad_json"),
    ("noevidence", "llm_bad_schema"),
])
def test_llm_failures_make_the_report_undetermined_not_an_exception(model, code):
    ctx = _extract(_ctx(), model=model)
    assert ctx.rca_status == "undetermined" and ctx.rca_evidence == []
    assert ctx.rca_undetermined_reason.split("：", 1)[0] == code
    assert any("無法判定" in a for a in ctx.audit)


def test_timeout_gives_up_after_the_configured_seconds_without_retrying_by_default():
    started = time.monotonic()
    ctx = _extract(_ctx(), model="slow", timeout_seconds=0.2)
    assert time.monotonic() - started < 0.9
    assert ctx.rca_status == "undetermined" and ctx.rca_undetermined_reason.startswith("llm_timeout：")
    assert "0.2 秒" in ctx.rca_undetermined_reason and _Fake.calls == ["slow"]


def test_retries_are_bounded_by_the_node_param():
    ctx = _extract(_ctx(), model="error", retries=2)
    assert _Fake.calls == ["error"] * 3 and "共試 3 次" in ctx.rca_undetermined_reason


def test_no_provider_configured_is_undetermined_llm_not_configured():
    ctx = OperatorRegistry.create("enrichment.extract_evidence", {}).execute(_ctx())
    assert ctx.rca_status == "undetermined" and ctx.rca_undetermined_reason.startswith("llm_not_configured：")
    assert _Fake.calls == []


def test_provider_can_come_from_the_environment(monkeypatch):
    monkeypatch.setenv(ex.PROVIDER_ENV, "test-fake")
    monkeypatch.setenv("AIOPS_LLM_MODEL", "ok")
    ctx = OperatorRegistry.create("enrichment.extract_evidence", {}).execute(_ctx())
    assert ctx.rca_status == "ok" and len(ctx.rca_evidence) == 2


def test_incomplete_provider_config_is_llm_not_configured(monkeypatch):
    monkeypatch.delenv("AIOPS_LLM_API_KEY", raising=False)
    ctx = _extract(_ctx(), provider="anthropic", model="claude-x")
    assert ctx.rca_undetermined_reason.startswith("llm_not_configured：") and "AIOPS_LLM_API_KEY" in ctx.rca_undetermined_reason


def test_without_logs_the_llm_is_not_called_and_status_stays_ok():
    ctx = OperatorRegistry.create("enrichment.extract_evidence", {}).execute(_ctx(logs=False))  # 連供應商都沒設定
    assert ctx.rca_status == "ok" and ctx.rca_evidence == [] and _Fake.calls == []


# ── 抽取：送入的內容 ──


def test_input_is_templates_with_capped_representative_lines():
    ctx = _ctx()
    patterns, lines = ex.build_input(ctx.rca_logs, ctx.rca_logs_baseline, ctx.rca_log_window_minutes,
                                     max_patterns=20, examples_per_pattern=2, max_line_chars=400)
    assert {p["service"] for p in patterns} == {"checkout", "order-svc", "inventory"}
    cb = next(p for p in patterns if p["service"] == "checkout")
    assert cb["count_during"] == 5 and cb["new"] and len(cb["lines"]) == 2  # 5 筆同樣板只送 2 筆原文
    assert len(lines) == 4 and all(l.id.startswith("L") for l in lines)
    _, short = ex.build_input(ctx.rca_logs, [], (30.0, 60.0), 1, 1, 40)
    assert len(short) == 1 and len(short[0].text) == 40
    # 被截掉的部分不算送入：quote 不能引用截掉的字
    accepted, rejected = ex.validate_items([{"kind": "circuit_breaker_open", "entity": "checkout", "quote": "further calls"}],
                                           short, {"checkout"})
    assert not accepted and "逐字" in rejected[0]["reason"]


# ── 載入期檢查 ──


@pytest.mark.parametrize("params, needle", [
    ({"timeout_seconds": -1}, "timeout_seconds"),
    ({"timeout_seconds": 0}, "timeout_seconds"),
    ({"retries": -1}, "retries"),
    ({"retries": 1.5}, "retries"),
    ({"retries": 99}, "retries"),
    ({"max_patterns": 0}, "max_patterns"),
    ({"provider": "noop"}, "noop"),
    ({"provider": "made-up"}, "provider"),
])
def test_bad_params_fail_at_load_time(params, needle):
    with pytest.raises(WorkflowParamError, match=needle):
        OperatorRegistry.create("enrichment.extract_evidence", params)


def test_misspelled_param_fails_at_load_time():
    with pytest.raises(WorkflowError, match="timeout"):
        OperatorRegistry.create("enrichment.extract_evidence", {"timeout": 5})


# ── 摘要作者 ──


def _ranked(ctx):
    ctx.rca_root_candidates = [RootCandidate("payment", 0.71, "checkout 的熔斷器對 payment 打開", attribution={"posterior": {"self": 0.71}}),
                               RootCandidate("checkout", 0.12, "呼叫端行為")]
    ctx.rca_facts = [Fact("entry_failure_rate", "checkout", 0.42, "ratio", "incident")]
    ctx.rca_evidence = [EvidenceItem("circuit_breaker_open", "checkout", quote="CircuitBreaker 'payment' is OPEN")]
    ctx.rca_runbook = {"id": "RB-04", "name": "下游熔斷", "actions": ["確認 payment 健康狀態"]}
    return ctx


def _narrate(ctx, **params):
    params.setdefault("provider", "test-fake")
    return OperatorRegistry.create("enrichment.rca_narrator", params).execute(ctx)


def test_grounded_narrative_is_accepted():
    ctx = _narrate(_ranked(_ctx()), model="narrate_ok")
    assert ctx.llm_generated_by == "llm" and ctx.llm_rejected is None and "0.71" in ctx.llm_summary
    assert ctx.rca_root_candidates[0].entity == "payment" and ctx.rca_status == "ok"


@pytest.mark.parametrize("model, needle", [
    ("narrate_number", "數字 87"),
    ("narrate_outsider", "search-svc"),
    ("error", "llm_error"),
    ("badjson", "llm_bad_json"),
])
def test_ungrounded_or_failed_narrative_falls_back_to_the_template(model, needle):
    ctx = _ctx()
    ctx.topology.append(("search-svc", "payment"))  # 拓撲裡有、但沒出事也不在候選裡
    ctx = _narrate(_ranked(ctx), model=model)
    assert ctx.llm_generated_by == "template" and needle in ctx.llm_rejected
    assert ctx.llm_summary.startswith("最可能的根因：payment（機率 0.71）")
    assert {k for c in ctx.llm_claims for k in c["evidence"]} == {"root_candidates", "facts", "evidence", "runbook"}
    assert ctx.rca_status == "ok" and [c.entity for c in ctx.rca_root_candidates] == ["payment", "checkout"]


def test_undetermined_template_says_so_and_the_narrator_does_not_change_the_status():
    ctx = _ctx()
    ctx.rca_status, ctx.rca_undetermined_reason = "undetermined", "llm_timeout：LLM 超過 30 秒沒有回應（共試 1 次）"
    ctx.rca_facts = [Fact("entry_failure_rate", "checkout", 0.42, "ratio")]
    ctx = _narrate(ctx)  # 沒設定 model：test-fake 預設回抽取格式，驗證不過
    assert ctx.llm_generated_by == "template"
    assert ctx.llm_summary.startswith("無法判定根因：llm_timeout：") and ctx.rca_status == "undetermined"
    assert ctx.llm_claims[0]["evidence"] == ["rca_status"]


def test_undetermined_narrative_must_say_undetermined():
    ctx = _ctx()
    ctx.rca_status, ctx.rca_undetermined_reason = "undetermined", "llm_timeout：LLM 逾時"
    assert _narrate(ctx, model="narrate_claims_cause").llm_rejected.startswith("判定狀態是 undetermined")
    assert _narrate(ctx, model="narrate_undetermined_ok").llm_generated_by == "llm"


def test_narrator_without_provider_uses_the_template():
    ctx = OperatorRegistry.create("enrichment.rca_narrator", {}).execute(_ranked(_ctx()))
    assert ctx.llm_generated_by == "template" and ctx.llm_rejected.startswith("llm_not_configured：")


def test_no_evidence_no_candidates_template_says_insufficient():
    ctx = _ctx()
    ctx.rca_root_note = "證據不足：沒有 trace"
    ctx = OperatorRegistry.create("enrichment.rca_narrator", {}).execute(ctx)
    assert ctx.llm_summary.startswith("證據不足，不指認單一服務：證據不足：沒有 trace")


def test_narrator_bad_timeout_fails_at_load_time():
    with pytest.raises(WorkflowParamError, match="timeout_seconds"):
        OperatorRegistry.create("enrichment.rca_narrator", {"timeout_seconds": -1})
