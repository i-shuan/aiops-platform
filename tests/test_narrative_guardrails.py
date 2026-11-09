"""三行式止血摘要（enrichment.llm_briefing）的 context 壓縮、JSON 解析、白名單／數字／黑名單守門與模板退路
（docs/RCA_STEP2_DESIGN.md 第 4 節、第 6.4 節）。不連外：LLM 用假供應商或 httpx.MockTransport。
"""
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from aiops.engine.context import PipelineContext, RootCandidate, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.enrichment import narrative, summarizer
from aiops.stages.enrichment.base import BaseLLM, LLMError
from aiops.stages.enrichment.providers import AnthropicLLM, OpenAICompatibleLLM, build_llm

T0 = datetime(2026, 11, 3, 15, 40, tzinfo=timezone.utc)
ATTR = {"resource": "tax-gw:443", "kind": "external", "subtype": "http", "share": 1.0, "by": "latency",
        "self_ms": {"before": 13.2, "during": 12.6}, "wait_ms": {"before": 13.3, "during": 101.4},
        "failures": {"before": 1, "during": 13}}
GOOD = {"summary_line": "【一句話根因】bill-svc 等待 tax-gw:443 由 13.3 ms 升到 101.4 ms，程式自身 13.2→12.6 ms 沒變",
        "action_line": "【立即止血動作】依 RB-01 先查 tax-gw:443 的健康狀態與網路延遲，確認 bill-svc 的 timeout 與連線池",
        "verification_line": "【驗證與排除項】範例 trace dd487b01 入口 500 失敗在 tax-gw:443，排除 bill-svc 程式本身"}


class FakeLLM(BaseLLM):
    name = "fake"

    def __init__(self, reply=None, error=None):
        self.reply, self.error, self.calls = reply, error, 0

    def complete(self, system, prompt):
        self.calls += 1
        self.prompt = prompt
        if self.error:
            raise LLMError(self.error)
        return self.reply if isinstance(self.reply, str) else json.dumps(self.reply, ensure_ascii=False)


def _ctx(with_attr=True, with_sample=True, with_logs=False, topology=()):
    ctx = PipelineContext(pipeline="rca_report", now=T0 + timedelta(hours=1))
    ctx.incidents = [Incident([AnomalyEvent("bill-svc", "latency", T0 + m * timedelta(minutes=1), 6.0, {"signal": "latency"})
                               for m in range(0, 30, 5)])]
    ctx.incidents[0].severity = "P0"
    ctx.incidents_built = True
    ctx.topology = list(topology)
    if with_attr:
        ctx.rca_root_candidates = [RootCandidate("bill-svc", 1.0, "自身耗時 26.5→114.0", "tax-gw:443（external/http）", dict(ATTR))]
    if with_sample:
        ctx.rca_spans = [SpanRecord("dd487b01", "x", None, "bill-svc", T0 + timedelta(minutes=3), 237.0, True, status_code=500, name="POST /invoice"),
                         SpanRecord("dd487b01", "w", "x", "bill-svc", T0 + timedelta(minutes=3), 223.0, True,
                                    kind="external", subtype="http", resource="tax-gw:443")]
    if with_logs:
        from aiops.engine.context import LogRecord
        ctx.rca_logs = [LogRecord(T0 + timedelta(minutes=i), "bill-svc", "Downstream tax-gw:443 returned 503", "HttpServerErrorException", "dd487b01")
                        for i in range(13)]
        ctx.rca_log_window_minutes = (30.0, 60.0)
    OperatorRegistry.create("rca.section.suggested_actions", {}).execute(ctx)  # 先算 Runbook（正式 YAML 的順序）
    return ctx


def _run(ctx, provider="openai", llm=None, monkeypatch=None, **params):
    if llm is not None:
        monkeypatch.setattr(summarizer, "build_llm", lambda *a, **k: llm)
    return OperatorRegistry.create("enrichment.llm_briefing", {"provider": provider, **params}).execute(ctx)


# 1. context 鍵固定、缺的是 None
def test_context_has_fixed_keys_and_nulls_for_missing_evidence():
    full = narrative.build_context(_ctx(with_logs=True), _ctx().incidents[0])
    assert set(full) == {"incident_meta", "root_cause", "root_note", "sample_trace", "top_error_logs", "matched_runbook"}
    assert full["incident_meta"] == {"entities": ["bill-svc"], "duration_minutes": 25, "severity": "P0"}
    assert full["root_cause"]["attribution"]["resource"] == "tax-gw:443" and full["matched_runbook"]["id"] == "RB-01"
    assert full["sample_trace"] == {"trace_id": "dd487b01", "endpoint": "POST /invoice", "duration_ms": 237.0, "status_code": 500,
                                    "failed": True, "failed_resource": "tax-gw:443"}
    assert full["top_error_logs"][0]["service"] == "bill-svc" and full["top_error_logs"][0]["count"] == 13
    empty = narrative.build_context(_ctx(with_attr=False, with_sample=False), _ctx().incidents[0])
    assert empty["root_cause"] is None and empty["sample_trace"] is None and empty["top_error_logs"] == []
    assert empty["matched_runbook"]["id"] == "RB-00"


# 2. 合法三行 → llm
def test_grounded_briefing_is_accepted(monkeypatch):
    llm = FakeLLM(GOOD)
    ctx = _run(_ctx(), llm=llm, monkeypatch=monkeypatch)
    assert ctx.llm_briefing_generated_by == "llm" and ctx.llm_briefing == GOOD and ctx.llm_briefing_rejected is None
    assert llm.calls == 1 and '"matched_runbook"' in llm.prompt and "RB-01" in llm.prompt
    out = ctx.to_result()["narrative"]
    assert out["briefing"] == GOOD and out["briefing_generated_by"] == "llm"


# 3. 白名單：拓撲裡有、但不在事件與證據裡的服務不准提
def test_mentioning_a_known_but_unrelated_service_is_rejected(monkeypatch):
    bad = {**GOOD, "summary_line": "【一句話根因】bill-svc 與 pay-api 一起變慢"}
    ctx = _run(_ctx(topology=[("pay-api", "bill-svc")]), llm=FakeLLM(bad), monkeypatch=monkeypatch)
    assert ctx.llm_briefing_generated_by == "template" and "白名單以外的實體：pay-api" in ctx.llm_briefing_rejected
    assert ctx.llm_briefing["summary_line"].startswith("【一句話根因】bill-svc 等待 tax-gw:443（external/http）13.3→101.4 ms")


# 4. 數字 ±10%
@pytest.mark.parametrize("number, ok", [("101", True), ("100", True), ("110", True), ("150", False), ("12", True), ("50", False)])
def test_numbers_must_be_within_ten_percent_of_the_evidence(monkeypatch, number, ok):
    reply = {**GOOD, "verification_line": f"【驗證與排除項】等待 tax-gw:443 約 {number} ms"}
    ctx = _run(_ctx(), llm=FakeLLM(reply), monkeypatch=monkeypatch)
    assert (ctx.llm_briefing_generated_by == "llm") is ok
    if not ok:
        assert f"數字 {number}" in ctx.llm_briefing_rejected


# 5. 黑名單：證據裡沒有 Kafka 就不准提；證據裡有 oracle 就放行
def test_hallucinated_components_are_rejected_unless_they_are_in_the_evidence(monkeypatch):
    reply = {**GOOD, "action_line": "【立即止血動作】檢查 Kafka 消費者是否堆積"}
    ctx = _run(_ctx(), llm=FakeLLM(reply), monkeypatch=monkeypatch)
    assert ctx.llm_briefing_generated_by == "template" and "kafka" in ctx.llm_briefing_rejected
    context = narrative.build_context(_ctx(), _ctx().incidents[0])
    context["root_cause"]["attribution"]["resource"] = "oracle"
    assert narrative.validate_briefing({**GOOD, "action_line": "【立即止血動作】排查 oracle 慢查詢"}, context, set()) is None


# 6. 格式錯、不是 JSON、供應商例外 → 模板，errors 為空
@pytest.mark.parametrize("reply, reason", [
    ({k: v for k, v in GOOD.items() if k != "action_line"}, "缺少 action_line"),
    ({**GOOD, "summary_line": "bill-svc 變慢"}, "必須以 【一句話根因】 開頭"),
    ({**GOOD, "summary_line": "【一句話根因】" + "長" * 200}, "超過 200 字"),
    ("這不是 JSON", "不是合法 JSON"),
    ([1, 2], "不是 JSON 物件"),
])
def test_malformed_replies_fall_back_to_the_template(monkeypatch, reply, reason):
    ctx = _run(_ctx(), llm=FakeLLM(reply if isinstance(reply, str) else json.dumps(reply, ensure_ascii=False)), monkeypatch=monkeypatch)
    assert ctx.llm_briefing_generated_by == "template" and reason in ctx.llm_briefing_rejected and ctx.errors == []
    assert set(ctx.llm_briefing) == set(narrative.BRIEFING_KEYS)


def test_provider_outage_and_noop_both_use_the_template(monkeypatch):
    ctx = _run(_ctx(), llm=FakeLLM(error="timeout"), monkeypatch=monkeypatch)
    assert ctx.llm_briefing_generated_by == "template" and "timeout" in ctx.llm_briefing_rejected and ctx.errors == []
    monkeypatch.setattr(summarizer, "build_llm", lambda *a, **k: pytest.fail("noop 不該建立 LLM"))
    ctx = _run(_ctx(), provider="noop")
    assert ctx.llm_briefing_generated_by == "template" and ctx.llm_briefing_rejected is None
    assert ctx.llm_briefing["action_line"] == "【立即止血動作】[RB-01] 檢查下游依賴目標 tax-gw:443 的健康狀態與網路延遲（bill-svc 的增量幾乎全在等它）"
    assert "排除它本身的程式與機器" in ctx.llm_briefing["verification_line"] and "dd487b01" in ctx.llm_briefing["verification_line"]


def test_template_without_candidate_states_insufficient_evidence():
    ctx = _ctx(with_attr=False, with_sample=False)
    ctx.rca_root_note = "證據不足，不指認根因：沒有任何服務的自身耗時明顯上升"
    ctx.rca_runbook = None
    OperatorRegistry.create("rca.section.suggested_actions", {}).execute(ctx)
    ctx = _run(ctx, provider="noop")
    assert ctx.llm_briefing["summary_line"].startswith("【一句話根因】證據不足，不指認單一服務（bill-svc）")
    assert "[RB-06]" in ctx.llm_briefing["action_line"]


# 7b. 推理型模型不接受 temperature 0：拿掉再送一次（只針對這個 400，其他錯誤照丟）
def test_openai_drops_temperature_when_the_model_rejects_it():
    bodies = []

    def handler(req):
        body = json.loads(req.content)
        bodies.append(body)
        if "temperature" in body:
            return httpx.Response(400, json={"error": {"message": "Unsupported value: 'temperature' does not support 0 with this model."}})
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    llm = OpenAICompatibleLLM("k", "gpt-x", "http://gw", client=httpx.Client(transport=httpx.MockTransport(handler)), json_mode=True)
    assert llm.complete("s", "p") == "ok"
    assert [("temperature" in b, b.get("response_format")) for b in bodies] == [(True, {"type": "json_object"}), (False, {"type": "json_object"})]
    other = OpenAICompatibleLLM("k", "gpt-x", "http://gw", client=httpx.Client(transport=httpx.MockTransport(
        lambda r: httpx.Response(400, json={"error": {"message": "context length exceeded"}}))))
    with pytest.raises(LLMError, match="context length"):
        other.complete("s", "p")


# 7. json_mode 只對 OpenAI 相容供應商
def test_json_mode_sets_response_format_only_for_openai_compatible(monkeypatch):
    seen = {}

    def handler(req):
        seen.update(json.loads(req.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": "{}"}}]})

    OpenAICompatibleLLM("k", "m", "http://gw", client=httpx.Client(transport=httpx.MockTransport(handler)), json_mode=True).complete("s", "p")
    assert seen["response_format"] == {"type": "json_object"}
    seen.clear()
    OpenAICompatibleLLM("k", "m", "http://gw", client=httpx.Client(transport=httpx.MockTransport(handler))).complete("s", "p")
    assert "response_format" not in seen

    def anthropic(req):
        seen.update(json.loads(req.content))
        return httpx.Response(200, json={"content": [{"type": "text", "text": "{}"}]})

    seen.clear()
    AnthropicLLM("k", "m", client=httpx.Client(transport=httpx.MockTransport(anthropic))).complete("s", "p")
    assert "response_format" not in seen
    monkeypatch.setenv("AIOPS_LLM_API_KEY", "k")
    assert build_llm("openai", "m", json_mode=True).json_mode is True
    assert build_llm("openai", "m").json_mode is False
    assert not hasattr(build_llm("anthropic", "m", json_mode=True), "json_mode")
