"""混沌場景 24 改對 rca_report_llm 之後，腳本化假 LLM（scripts/chaos/scripted_llm.py）多了判斷節點的兩個模式。
場景 24 要資料庫才能跑；這裡不用資料庫，直接拿平台真的 rca_judge.build_prompt 產生判斷節點的 prompt，
確認假 LLM 的回覆經過平台真的 validate_candidates：judge_faithful 要被接受、judge_hallucinate 要全部被拒。
（只驗回覆形狀與驗證的互動，不代表真模型判得對：見 scripted_llm.py 檔頭的誠實限制。）"""
import json
from datetime import datetime, timedelta, timezone

from aiops.engine.rca_types import EvidenceItem
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.enrichment.rca_judge import SYSTEM, build_prompt, evidence_rows, validate_candidates
from scripts.chaos import scripted_llm as LLM

T0 = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
CB = "CircuitBreaker 'payment-gw' is OPEN and does not permit further calls"


def _prompt():
    events = [AnomalyEvent("ord-svc", "errors", T0 + timedelta(minutes=m), 6.0) for m in range(3)]
    incident = Incident(events)
    facts = [{"id": "F1", "kind": "call_failure_rate", "entity": "ord-svc", "value": 1.0,
              "detail": {"callee": "payment-gw:443"}}]
    evidence = evidence_rows([EvidenceItem("circuit_breaker_open", "ord-svc", "present", 0.95, CB,
                                           "2026-11-09T12:03:00.000Z")])
    entities = ["ord-svc", "pay-api"]
    graph = {"calls": [["ord-svc", "payment-gw:443"]], "async": [], "resources": {"ord-svc": ["payment-gw:443"]}}
    return build_prompt(incident, graph, facts, evidence, entities, 0, 3), facts, evidence, set(entities)


def _reply(mode, prompt):
    return json.loads(LLM.ScriptedLLM(mode).complete(SYSTEM, prompt))


def test_faithful_judge_reply_passes_the_real_validation():
    prompt, facts, evidence, entities = _prompt()
    reply = _reply("judge_faithful", prompt)
    accepted, rejected = validate_candidates(reply["candidates"], facts, evidence, entities)
    assert [a["entity"] for a in accepted] == ["ord-svc"] and accepted[0]["cites"] == ["E1"] and rejected == []


def test_hallucinating_judge_reply_is_rejected_by_the_real_validation():
    prompt, facts, evidence, entities = _prompt()
    reply = _reply("judge_hallucinate", prompt)
    accepted, rejected = validate_candidates(reply["candidates"], facts, evidence, entities)
    assert accepted == [] and len(rejected) == 1 and "不是已知實體" in rejected[0]["reason"]


def test_faithful_judge_without_verified_evidence_answers_nothing():
    prompt = build_prompt(Incident([AnomalyEvent("ord-svc", "errors", T0, 6.0)]), {"calls": [], "async": [], "resources": {}},
                          [], [], ["ord-svc"], 0, 3)
    assert _reply("judge_faithful", prompt)["candidates"] == []


def test_wire_maps_each_scenario_mode_onto_the_judge_node():
    class Node:
        def __init__(self, name, operator):
            self.name, self.operator, self.params = name, operator, {}

    class Wf:
        name = "rca_report_llm"
        nodes = [Node("extract_evidence", "enrichment.extract_evidence"), Node("root_cause", "rca.root_cause.llm"),
                 Node("narrator", "enrichment.rca_narrator")]

    expect = {"down": "down", "garbage": "garbage", "faithful": "judge_faithful", "hallucinate": "judge_hallucinate"}
    for mode, judge in expect.items():
        wf = Wf()
        assert LLM.wire(wf, mode) == ["extract_evidence", "root_cause", "narrator"]
        models = {n.name: n.params["model"] for n in wf.nodes}
        assert models == {"extract_evidence": mode, "root_cause": judge, "narrator": mode}
    wf = Wf()
    LLM.wire(wf, "slow")
    assert all(n.params["model"].startswith("slow:") for n in wf.nodes)
