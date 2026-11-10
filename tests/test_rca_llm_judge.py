"""rca.root_cause.llm：呼叫圖＋事實＋日誌證據交給 LLM 判斷根因；候選逐一驗證，LLM 失效或全數被拒 → undetermined。

假供應商只登記在這個測試檔（CLAUDE.md §100.2：aiops/ 裡不放測試專用的供應商）。
"""
import json
import threading
from datetime import datetime, timedelta, timezone

import pytest

from aiops.engine.context import PipelineContext
from aiops.engine.errors import WorkflowParamError
from aiops.engine.pipeline import load_workflow
from aiops.engine.rca_types import EvidenceItem, Fact
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.interfaces.runner import load_all, workflow_path
from aiops.stages.enrichment import evidence_extractor as ex
from aiops.stages.enrichment import rca_judge as judge
from aiops.stages.enrichment import rca_narrator as narrator
from aiops.stages.enrichment.base import BaseLLM, LLMError
from aiops.stages.enrichment.registry import register

NOW = datetime(2026, 11, 10, 12, 30, tzinfo=timezone.utc)

# 事實編號：F1 checkout 等 payment、F2 payment 自身耗時、F3 checkout 入口失敗率、F4 order-svc 等 inventory
_PAYMENT = {"entity": "payment", "confidence": 0.8, "subtype": "cpu", "resource": "",
            "reason": "payment 自身耗時增加 850 ms，checkout 只是在等它", "cites": ["F2", "F1", "E1"]}
_CHECKOUT = {"entity": "checkout", "confidence": 0.15, "subtype": "dependency", "resource": "payment",
             "reason": "checkout 等 payment 的時間多了 900 ms", "cites": ["F1"]}


class _Judge(BaseLLM):
    """model 名稱決定行為；calls 與 prompts 記在 class 上給測試看（每次 build 都是新物件）。"""

    name = "test-judge"
    calls: list[str] = []
    prompts: list[str] = []

    def __init__(self, mode: str):
        self.mode = mode

    def complete(self, system, prompt):
        _Judge.calls.append(self.mode)
        _Judge.prompts.append(prompt)
        reply = {
            "ok": {"candidates": [_CHECKOUT, _PAYMENT], "note": ""},  # 故意把信心低的排前面：算子要照信心排
            "unknown_entity": {"candidates": [{**_PAYMENT, "entity": "billing-svc"}, _CHECKOUT]},
            "bad_cite": {"candidates": [{**_PAYMENT, "cites": ["F2", "F99"]}, _CHECKOUT]},
            "no_cite": {"candidates": [{**_PAYMENT, "cites": []}, _CHECKOUT]},
            "made_up_number": {"candidates": [{**_PAYMENT, "reason": "payment 錯誤率 63%，CPU 用到 4200 ms"}, _CHECKOUT]},
            "bad_confidence": {"candidates": [{**_PAYMENT, "confidence": 1.4}, _CHECKOUT]},
            "bad_subtype": {"candidates": [{**_PAYMENT, "subtype": "cosmic_ray"}, _CHECKOUT]},
            "bad_resource": {"candidates": [{**_CHECKOUT, "resource": "redis-cache"}]},
            "duplicate": {"candidates": [_PAYMENT, {**_PAYMENT, "confidence": 0.3}]},
            "all_rejected": {"candidates": [{**_PAYMENT, "entity": "billing-svc"}, {**_CHECKOUT, "cites": ["F42"]}]},
            "insufficient": {"candidates": [], "note": "只有症狀，沒有任何服務的自身耗時或錯誤來源事實"},
            "no_candidates_key": {"answer": "payment"},
            "many": {"candidates": [{**_PAYMENT, "confidence": 0.5}, {**_CHECKOUT, "confidence": 0.3},
                                    {"entity": "order-svc", "confidence": 0.2, "subtype": "dependency",
                                     "reason": "order-svc 在等 inventory", "cites": ["F4"]}]},
        }
        if self.mode == "error":
            raise LLMError("HTTP 503")
        if self.mode == "slow":
            threading.Event().wait(1.0)  # conftest 把 time.sleep 換成記錄器了，這裡要真的卡住
        if self.mode == "badjson":
            return "我覺得是 payment 壞了"
        return json.dumps(reply.get(self.mode, reply["ok"]), ensure_ascii=False)


@register("test-judge")
def _judge_factory(model, base_url, api_key):
    return _Judge(model)


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    _Judge.calls, _Judge.prompts = [], []
    monkeypatch.delenv(ex.PROVIDER_ENV, raising=False)


def _ctx(facts=True, evidence=True) -> PipelineContext:
    ctx = PipelineContext(pipeline="rca_report_llm", now=NOW)
    ctx.incidents = [Incident(events=[
        AnomalyEvent(entity="checkout", metric="latency", ts=NOW - timedelta(minutes=5), score=6.0),
        AnomalyEvent(entity="payment", metric="latency", ts=NOW - timedelta(minutes=4), score=9.0)])]
    ctx.topology = [("checkout", "payment"), ("checkout", "order-svc"), ("order-svc", "inventory")]
    if facts:
        ctx.rca_facts = [
            Fact(kind="wait_delta", entity="checkout", value=900.0, unit="ms", window="incident",
                 detail={"target": "payment", "target_kind": "service", "before": 40.0, "during": 940.0}),
            Fact(kind="self_time_delta", entity="payment", value=850.0, unit="ms", window="incident",
                 detail={"before": 30.0, "during": 880.0}),
            Fact(kind="entry_failure_rate", entity="checkout", value=0.2, window="incident"),
            Fact(kind="wait_delta", entity="order-svc", value=3.0, unit="ms", window="incident",
                 detail={"target": "inventory", "target_kind": "service"}),
        ]
    if evidence:
        ctx.rca_evidence = [EvidenceItem(kind="timeout", entity="checkout", quote="Read timed out calling payment")]
    return ctx


def _rank(ctx, **params):
    params.setdefault("provider", "test-judge")
    return OperatorRegistry.create("rca.root_cause.llm", params).execute(ctx)


def _rejections(ctx) -> str:
    return "\n".join(a for a in ctx.audit if a.startswith("root_cause.llm 拒絕"))


# ── 送給 LLM 的東西 ──


def test_prompt_carries_the_call_graph_the_facts_and_the_evidence_together():
    _rank(_ctx(), model="ok")
    [prompt] = _Judge.prompts
    for needle in ("checkout -> payment", "order-svc -> inventory",          # 呼叫圖
                   '"id": "F1"', '"kind": "wait_delta"', '"target": "payment"',  # 事實（含 detail）
                   '"id": "E1"', "Read timed out calling payment",             # 日誌證據
                   '"first_alarm"', "wait_delta", "self_time_delta"):          # 告警先後、事實種類說明
        assert needle in prompt, needle
    assert judge.FACT_GLOSSARY["wait_delta"] in prompt
    assert "entry_latency" not in prompt  # 沒出現的事實種類不解釋，省 prompt


def test_facts_over_the_limit_are_not_sent_and_the_llm_is_told_how_many():
    ctx = _rank(_ctx(), model="ok", max_facts=2)
    [prompt] = _Judge.prompts
    assert '"id": "F2"' in prompt and '"id": "F3"' not in prompt and "另有 2 筆" in prompt
    assert any("事實 2／4 筆" in a for a in ctx.audit)


# ── 驗證 ──


def test_valid_judgement_is_ranked_by_confidence_and_keeps_what_it_cited():
    ctx = _rank(_ctx(), model="ok")
    assert ctx.rca_status == "ok" and ctx.rca_root_algorithm == "rca.root_cause.llm"
    top, second = ctx.rca_root_candidates
    assert (top.entity, top.score) == ("payment", 0.8) and (second.entity, second.score) == ("checkout", 0.15)
    assert "LLM 判斷" in top.reason and "80%" in top.reason and "F2、F1、E1" in top.reason
    assert top.attribution == {"judge": "llm", "cause": "cpu", "cites": ["F2", "F1", "E1"]}
    assert second.dependency == "payment" and second.attribution["resource"] == "payment"
    assert any("採用 2、拒絕 0" in a for a in ctx.audit)


@pytest.mark.parametrize("mode, why", [
    ("unknown_entity", "已知實體"), ("bad_cite", "不存在的編號"), ("no_cite", "至少一筆"),
    ("made_up_number", "找不到"), ("bad_confidence", "confidence"), ("bad_subtype", "subtype"),
])
def test_a_candidate_that_fails_validation_is_dropped_with_its_reason(mode, why):
    ctx = _rank(_ctx(), model=mode)
    assert ctx.rca_status == "ok"
    assert [c.entity for c in ctx.rca_root_candidates] == ["checkout"]  # payment 那筆被丟、其餘照用
    assert why in _rejections(ctx)
    assert any("採用 1、拒絕 1" in a for a in ctx.audit)


def test_made_up_number_names_the_number():
    assert "數字 63 " in _rejections(_rank(_ctx(), model="made_up_number"))


def test_numbers_inside_names_and_cite_ids_are_not_checked_and_ratios_may_be_percent():
    cited = [{"id": "F3", "kind": "entry_failure_rate", "entity": "ord-svc-2", "value": 0.2}]
    assert judge.unsupported_number("ord-svc-2 失敗率 20%（F3），連到 tax-gw:443", cited) is None
    assert judge.unsupported_number("ord-svc-2 失敗率 0.2", cited) is None
    assert judge.unsupported_number("ord-svc-2 失敗率 55%", cited) == "55"
    assert judge.unsupported_number("ord-svc-2 多花了 20 ms", cited) == "20"  # 沒有 % 就不能拿比例乘 100 來對


def test_resource_not_in_the_cited_facts_is_cleared_but_the_candidate_stays():
    [c] = _rank(_ctx(), model="bad_resource").rca_root_candidates
    assert c.entity == "checkout" and c.dependency == ""
    assert c.attribution["resource_dropped"] == "redis-cache" and "resource" not in c.attribution


def test_one_candidate_per_entity_and_top_k_caps_the_list():
    assert [c.score for c in _rank(_ctx(), model="duplicate").rca_root_candidates] == [0.8]
    assert [c.entity for c in _rank(_ctx(), model="many", top_k=2).rca_root_candidates] == ["payment", "checkout"]


# ── 無法判定：LLM 失效或全數被拒 ──


@pytest.mark.parametrize("mode, params, code", [
    ("error", {}, "llm_error"), ("slow", {"timeout_seconds": 0.05}, "llm_timeout"),
    ("badjson", {}, "llm_bad_json"), ("no_candidates_key", {}, "llm_bad_schema"),
    ("all_rejected", {}, "llm_all_rejected"),
])
def test_llm_failure_gives_undetermined_with_code_and_no_candidates(mode, params, code):
    ctx = _rank(_ctx(), model=mode, **params)
    assert ctx.rca_status == "undetermined" and ctx.rca_root_candidates == []
    assert ctx.rca_undetermined_reason.startswith(f"{code}：")
    assert ctx.rca_root_note.startswith("無法判定：") and code in ctx.rca_root_note
    assert len(_Judge.calls) == 1  # 預設不重試


def test_provider_not_configured_is_undetermined_without_any_call():
    ctx = OperatorRegistry.create("rca.root_cause.llm", None).execute(_ctx())
    assert ctx.rca_status == "undetermined" and ctx.rca_undetermined_reason.startswith("llm_not_configured：")
    assert _Judge.calls == []


def test_retries_are_bounded():
    ctx = _rank(_ctx(), model="error", retries=2)
    assert len(_Judge.calls) == 3 and "共試 3 次" in ctx.rca_undetermined_reason


# ── 不是失敗的空結果 ──


def test_llm_saying_evidence_is_insufficient_is_an_empty_ranking_not_a_failure():
    ctx = _rank(_ctx(), model="insufficient")
    assert ctx.rca_status == "ok" and ctx.rca_root_candidates == []
    assert ctx.rca_root_note.startswith("LLM 判斷證據不足") and "只有症狀" in ctx.rca_root_note


def test_upstream_undetermined_or_no_material_means_the_llm_is_not_called():
    ctx = _ctx()
    ctx.rca_status, ctx.rca_undetermined_reason = "undetermined", "llm_timeout：30 秒沒有回應"
    ctx = _rank(ctx, model="ok")
    assert ctx.rca_root_candidates == [] and "llm_timeout" in ctx.rca_root_note
    assert ctx.rca_undetermined_reason == "llm_timeout：30 秒沒有回應"  # 上游的原因不被蓋掉
    empty = _rank(_ctx(facts=False, evidence=False), model="ok")
    assert empty.rca_status == "ok" and empty.rca_root_candidates == [] and "證據不足" in empty.rca_root_note
    assert _Judge.calls == []


# ── 載入期檢查、摘要標示、管線 ──


@pytest.mark.parametrize("params", [
    {"timeout_seconds": 0}, {"retries": -1}, {"retries": ex.MAX_RETRIES + 1}, {"max_facts": 0}, {"top_k": 0},
    {"provider": "noop"}, {"provider": "no-such-provider"},
])
def test_bad_params_fail_at_load_time(params):
    with pytest.raises(WorkflowParamError):
        OperatorRegistry.create("rca.root_cause.llm", params)


def test_narrator_labels_llm_candidates_as_self_rated_confidence_not_probability():
    by_key = {s["key"]: s for s in narrator.build_sections(_rank(_ctx(), model="ok"))}
    assert by_key["root_candidates"]["title"] == "根因候選（LLM 判斷）"
    assert "payment（信心 0.8）" in by_key["root_candidates"]["body"] and "機率" not in by_key["root_candidates"]["title"]


def test_llm_workflow_loads_and_differs_from_cognitive_only_in_the_judge_node():
    load_all()
    llm, cog = (load_workflow(workflow_path(n)) for n in ("rca_report_llm", "rca_report_cognitive"))
    assert [n.name for n in llm.nodes] == [n.name for n in cog.nodes]
    diff = [(a.operator, b.operator) for a, b in zip(llm.nodes, cog.nodes) if a.operator != b.operator]
    assert diff == [("rca.root_cause.llm", "rca.root_cause.bayes")]
    same = [(a.name, a.params, b.params) for a, b in zip(llm.nodes, cog.nodes) if a.operator == b.operator and a.params != b.params]
    assert same == []  # 撈取、證據層、報告、摘要的參數完全相同：比的是判斷層


# ── 資源（外部 API、資料庫）：不是實體，但要出現在圖上；LLM 指名後數字由事實抄進 attribution ──


def _external_ctx() -> PipelineContext:
    ctx = _ctx(evidence=False)
    ctx.topology = []
    ctx.rca_facts = [
        Fact(kind="self_time_delta", entity="checkout", value=3000.0, unit="ms", window="incident",
             detail={"before": 70.0, "during": 3070.0, "app_before": 40.2, "app_during": 40.1}),
        Fact(kind="wait_delta", entity="checkout", value=2970.0, unit="ms", window="incident",
             detail={"target": "pay-gw:443", "target_kind": "external/http", "before": 20.0, "during": 2990.0,
                     "failures_before": 0, "failures_during": 1191}),
        Fact(kind="wait_delta", entity="checkout", value=0.1, unit="ms", window="incident",
             detail={"target": "payment", "target_kind": "service", "before": 5.0, "during": 5.1}),
    ]
    return ctx


def test_graph_lists_resources_that_are_not_entities():
    ctx = _external_ctx()
    graph = judge.graph_rows(ctx, {"checkout", "payment"})
    assert graph == {"calls": [], "async": [], "resources": ["checkout -> pay-gw:443（external/http）"]}
    _rank(ctx, model="ok")
    assert "checkout -> pay-gw:443（external/http）" in _Judge.prompts[0]
    assert any("呼叫圖 1 條邊" in a for a in ctx.audit)


def test_named_resource_gets_its_numbers_copied_from_the_facts():
    facts = judge.fact_rows(_external_ctx().rca_facts, 10)
    assert judge.resource_numbers("checkout", "pay-gw:443", facts) == {
        "kind": "external", "subtype": "http", "wait_ms": {"before": 20.0, "during": 2990.0},
        "failures": {"before": 0, "during": 1191}, "self_ms": {"before": 40.2, "during": 40.1}, "share": 0.99}
    assert judge.resource_numbers("checkout", "no-such-db", facts) == {}       # 沒有對應的事實：不編數字
    assert judge.resource_numbers("payment", "pay-gw:443", facts) == {}        # 別的服務的等待不算它的
