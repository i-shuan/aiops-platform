"""RCA 報告、LLM 敘述（依據驗證與退回模板）、LLM 供應商與通知出口的傳輸層。"""
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from aiops.engine.errors import InfrastructureError, NotFound, WorkflowError
from aiops.engine.pipeline import Pipeline, load_workflow
from aiops.engine.requests import ExternalAnomalyIn, IngestAnomalies
from aiops.engine.types import Notification
from aiops.stages.enrichment.base import BaseLLM, LLMError, parse_json_reply
from aiops.stages.enrichment.providers import AnthropicLLM, OpenAICompatibleLLM, build_llm
from aiops.stages.notification.transport import AlertmanagerTransport
from aiops.interfaces.runner import run, run_context, workflow_path
from aiops.storage.hooks import StorageHooks


def _incident(now, topology=None, tag="x"):
    """建立一個 a(上游被拖累) + b(源頭) 的事件群並回傳 id。拓撲只寫在告警 workflow 裡，不給 RCA 任何資訊。"""
    items = [ExternalAnomalyIn(external_id=f"{tag}1", entity="svc-a", metric="latency", ts=now - timedelta(minutes=1), score=3),
             ExternalAnomalyIn(external_id=f"{tag}2", entity="svc-b", metric="latency", ts=now - timedelta(minutes=2), score=6)]
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline="it_otel", source="x", anomalies=items))
    from aiops.engine.pipeline import Pipeline as P

    wf = load_workflow(workflow_path("it_otel"))
    wf.topology = topology if topology is not None else [("svc-a", "svc-b")]
    result = P(wf, StorageHooks()).run(now=now, targets=[]).to_result()
    return result["incidents"][0]["id"]


def _rca(incident_id, provider="noop", now=None):
    wf = load_workflow(workflow_path("rca_report"))
    next(n for n in wf.nodes if n.operator == "enrichment.llm_summary").params["provider"] = provider
    return Pipeline(wf, StorageHooks()).run(incident_id=incident_id, now=now).to_result()


# ── 報告段落 ──


def test_all_sections_present_when_data_is_there_and_missing_ones_are_skipped_not_empty(now):
    rca = _rca(_incident(now), now=now)
    keys = [s["key"] for s in rca["sections"]]
    assert keys == ["summary", "timeline", "affected_entities", "golden_signals", "root_cause_candidates",
                    "propagation_path", "graph_basis", "suggested_actions"]  # graph_basis：關係圖依據（邊的出處）
    assert rca["skipped"] == ["trace_evidence", "log_patterns", "similar_cases"] and all(s["body"].strip() for s in rca["sections"])
    # 預設根因算子改成 evidence；這裡沒有 trace 來源，所以退回拓撲推測，並且要在報告裡寫明不是用 trace 判斷的
    assert rca["root_cause"]["algorithm"] == "rca.root_cause.evidence"
    assert "沒有 trace 證據" in rca["root_cause"]["note"] and "改用 topological" in rca["root_cause"]["note"]


def test_without_topology_the_graph_sections_are_skipped(now):
    rca = _rca(_incident(now, topology=[]), now=now)
    assert "propagation_path" in rca["skipped"]
    # 「可能的源頭」不再整段消失：寫明證據不足（沒有 trace、也沒有拓撲），讓值班的人知道是算了但不能指認
    candidates = {s["key"]: s["body"] for s in rca["sections"]}["root_cause_candidates"]
    level, candidates = candidates.split("\n", 1)  # 第一行是結論等級：沒有候選 = 無法判定
    assert level.startswith("【結論等級：無法判定（undetermined）】") and rca["root_cause"]["conclusion_type"] == "undetermined"
    assert candidates.startswith("證據不足") and rca["root_cause"]["candidates"] == []


def test_root_cause_is_the_dependency_not_the_caller(now):
    sections = {s["key"]: s["body"] for s in _rca(_incident(now), now=now)["sections"]}
    level, candidates = sections["root_cause_candidates"].split("\n", 1)  # 第一行是結論等級：沒有 span，是拓撲推測
    assert level.startswith("【結論等級：拓撲推測（topology_inference）】")
    assert candidates.startswith("1. svc-b")  # topology 是 (caller, callee)，被呼叫的依賴是源頭
    assert sections["propagation_path"].startswith("svc-b（")


def test_unknown_incident_is_not_found_and_unknown_section_is_a_workflow_error(now):
    with pytest.raises(NotFound):
        run("rca_report", incident_id=999)
    wf = load_workflow(workflow_path("rca_report"))
    next(n for n in wf.nodes if n.operator == "rca.report").params["sections"] = ["summary", "made_up"]
    with pytest.raises(WorkflowError, match="made_up"):
        Pipeline(wf, StorageHooks())


# ── LLM 敘述 ──


class FakeLLM(BaseLLM):
    name = "fake"

    def __init__(self, reply=None, error=None):
        self.reply, self.error, self.calls = reply, error, 0

    def complete(self, system, prompt):
        self.calls += 1
        if self.error:
            raise LLMError(self.error)
        return self.reply if isinstance(self.reply, str) else json.dumps(self.reply, ensure_ascii=False)


def _use(monkeypatch, llm):
    monkeypatch.setattr("aiops.stages.enrichment.summarizer.build_llm", lambda *a, **k: llm)


def test_noop_never_touches_the_network_and_uses_the_template(now, monkeypatch):
    monkeypatch.setattr("aiops.stages.enrichment.summarizer.build_llm",
                        lambda *a, **k: pytest.fail("noop 不該建立 LLM"))
    monkeypatch.setattr(httpx, "post", lambda *a, **k: pytest.fail("noop 不該發出請求"))
    n = _rca(_incident(now), now=now)["narrative"]
    assert n["generated_by"] == "template" and n["rejected"] is None
    assert n["claims"][0]["evidence"] == ["root_cause_candidates"]


def test_grounded_llm_narrative_is_accepted(now, monkeypatch):
    llm = FakeLLM({"summary": "svc-b 最可能是源頭，svc-a 受到牽連。",
                   "claims": [{"text": "svc-b 上游沒有其他異常", "evidence": ["root_cause_candidates"]}]})
    _use(monkeypatch, llm)
    n = _rca(_incident(now), provider="openai", now=now)["narrative"]
    assert n["generated_by"] == "llm" and n["summary"].startswith("svc-b") and llm.calls == 1


@pytest.mark.parametrize("reply, reason", [
    ({"summary": "svc-zzz 也有問題", "claims": [{"text": "x", "evidence": ["summary"]}]}, None),  # 未知實體不一定在拓撲裡
    ({"summary": "錯誤率高達 87%", "claims": [{"text": "x", "evidence": ["summary"]}]}, "數字 87"),
    ({"summary": "ok", "claims": [{"text": "x", "evidence": ["made_up"]}]}, "不存在的段落"),
    ({"summary": "ok", "claims": []}, "1～8"),
    ("這不是 JSON", "不是合法 JSON"),
])
def test_ungrounded_llm_output_falls_back_to_the_template(now, monkeypatch, reply, reason):
    _use(monkeypatch, FakeLLM(reply))
    n = _rca(_incident(now), provider="openai", now=now)["narrative"]
    if reason:
        assert n["generated_by"] == "template" and reason in n["rejected"]


def test_mentioning_an_entity_outside_the_incident_is_rejected(now, monkeypatch):
    """svc-c 在拓撲裡、但這次沒出事：LLM 提到它就是憑空加的實體，要退回模板。"""
    _use(monkeypatch, FakeLLM({"summary": "svc-c 也受影響", "claims": [{"text": "x", "evidence": ["summary"]}]}))
    incident_id = _incident(now, topology=[("svc-a", "svc-b"), ("svc-c", "svc-b")])
    n = _rca(incident_id, provider="openai", now=now)["narrative"]
    assert n["generated_by"] == "template" and "事件群以外的實體" in n["rejected"]


def test_rca_uses_the_topology_snapshot_stored_with_the_incident(now):
    """拓撲只寫在告警 workflow 裡；事件群存檔時一起存下來，RCA 才跟當時的收斂結果一致。"""
    from aiops.storage.repository import Incidents

    incident_id = _incident(now)
    assert Incidents.get(incident_id)["context"]["topology"] == [["svc-a", "svc-b"]]


def test_llm_outage_or_missing_config_degrades_instead_of_failing(now, monkeypatch):
    _use(monkeypatch, FakeLLM(error="timeout"))
    n = _rca(_incident(now), provider="openai", now=now)["narrative"]
    assert n["generated_by"] == "template" and "timeout" in n["rejected"]

    monkeypatch.undo()  # 用真的 build_llm：anthropic 沒金鑰
    monkeypatch.setenv("AIOPS_LLM_MODEL", "claude-sonnet-5-5")
    n2 = _rca(_incident(now.replace(day=9), tag="y"), provider="anthropic", now=now.replace(day=9))["narrative"]
    assert n2["generated_by"] == "template" and "AIOPS_LLM_API_KEY" in n2["rejected"]


def test_unknown_provider_is_rejected_when_the_workflow_loads(now):
    wf = load_workflow(workflow_path("rca_report"))
    next(n for n in wf.nodes if n.operator == "enrichment.llm_summary").params["provider"] = "skynet"
    with pytest.raises(WorkflowError):
        Pipeline(wf, StorageHooks())


# ── LLM 供應商（模擬 HTTP）──


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_openai_request_and_reply():
    seen = {}

    def handler(req):
        seen.update(url=str(req.url), auth=req.headers.get("authorization"), body=json.loads(req.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": "答案"}}]})

    llm = OpenAICompatibleLLM("k-1", "gpt-x", "http://gw.internal/", client=_client(handler))
    assert llm.complete("系統", "問題") == "答案"
    assert seen["url"] == "http://gw.internal/v1/chat/completions" and seen["auth"] == "Bearer k-1"
    assert seen["body"]["temperature"] == 0 and seen["body"]["messages"][0] == {"role": "system", "content": "系統"}


def test_openai_gateway_without_key_sends_no_auth_header():
    seen = {}

    def handler(req):
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    OpenAICompatibleLLM(None, "m", "http://gw", client=_client(handler)).complete("s", "p")
    assert seen["auth"] is None


def test_anthropic_request_joins_text_blocks_and_ignores_others():
    seen = {}

    def handler(req):
        seen.update(url=str(req.url), headers=dict(req.headers), body=json.loads(req.content))
        return httpx.Response(200, json={"content": [{"type": "text", "text": "第一段"}, {"type": "tool_use", "id": "x"},
                                                     {"type": "text", "text": "第二段"}]})

    llm = AnthropicLLM("ak", "claude-sonnet-5-5", client=_client(handler))
    assert llm.complete("系統", "問題") == "第一段第二段"
    assert seen["url"] == "https://api.anthropic.com/v1/messages"
    assert seen["headers"]["x-api-key"] == "ak" and seen["headers"]["anthropic-version"] == "2023-06-01"
    assert seen["body"]["system"] == "系統" and seen["body"]["messages"] == [{"role": "user", "content": "問題"}]


def _boom(req):
    raise httpx.ConnectError("refused", request=req)


@pytest.mark.parametrize("handler", [lambda r: httpx.Response(500, text="oops"),
                                     lambda r: httpx.Response(429, json={"error": "rate limit"}),
                                     lambda r: httpx.Response(200, text="not json"),
                                     lambda r: httpx.Response(200, json={"unexpected": True}), _boom])
@pytest.mark.parametrize("make", [lambda c: OpenAICompatibleLLM("k", "m", "http://gw", client=c),
                                  lambda c: AnthropicLLM("k", "m", client=c)])
def test_provider_failures_become_llm_error(handler, make):
    with pytest.raises(LLMError):
        make(_client(handler)).complete("s", "p")


def test_build_llm_configuration(monkeypatch):
    assert build_llm("noop") is None
    with pytest.raises(LLMError, match="模型"):
        build_llm("openai")
    with pytest.raises(LLMError, match="未知"):
        build_llm("mystery", "m")
    monkeypatch.setenv("AIOPS_LLM_API_KEY", "k")
    monkeypatch.delenv("AIOPS_LLM_BASE_URL", raising=False)
    for provider in ("openai", "anthropic"):  # 沒寫端點：不預設送到官方公網端點（M4），要明確寫出來
        with pytest.raises(LLMError, match="base_url"):
            build_llm(provider, "m")
    assert isinstance(build_llm("openai", "m", "https://api.openai.com"), OpenAICompatibleLLM)
    assert isinstance(build_llm("anthropic", "m", "https://api.anthropic.com"), AnthropicLLM)
    monkeypatch.setenv("AIOPS_LLM_BASE_URL", "http://vllm.llm.svc:8000")  # 環境變數給的內網端點也算明寫
    assert isinstance(build_llm("openai", "m"), OpenAICompatibleLLM)


def test_parse_json_reply_strips_fences_and_rejects_non_objects():
    assert parse_json_reply('```json\n{"a": 1}\n```') == {"a": 1}
    with pytest.raises(LLMError):
        parse_json_reply("[1, 2]")


# ── 通知出口傳輸 ──


def _note(**kw):
    return Notification({"severity": "P1", "dept": "pay"}, {"summary": "s"}, datetime(2026, 11, 8), **kw)


def test_alertmanager_body_auth_and_end_time(monkeypatch):
    calls = []

    def fake_post(url, json, headers, timeout):
        calls.append((url, json, headers))
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", fake_post)
    AlertmanagerTransport("http://am:9093/", token="t0k").send(
        [_note(), _note(ends_at=datetime(2026, 11, 8, 1, tzinfo=timezone.utc))])
    url, body, headers = calls[0]
    assert url == "http://am:9093/api/v2/alerts" and headers == {"Authorization": "Bearer t0k"}
    assert body[0]["startsAt"] == "2026-11-08T00:00:00Z" and "endsAt" not in body[0]
    assert body[1]["endsAt"] == "2026-11-08T01:00:00Z"


def test_alertmanager_failure_is_infrastructure_error_and_empty_send_is_a_noop(monkeypatch):
    monkeypatch.setattr(httpx, "post", lambda url, **_: (_ for _ in ()).throw(httpx.ConnectError("x", request=httpx.Request("POST", url))))
    with pytest.raises(InfrastructureError):
        AlertmanagerTransport("http://am:9093").send([_note()])
    AlertmanagerTransport("http://am:9093").send([])  # 沒有通知就不發請求
