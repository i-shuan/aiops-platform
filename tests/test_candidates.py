"""根因分析列出幾個可能的原因，各帶信心（高／中／低）。信心是模型標的；程式只做一件事：沒有直接證據指向那個系統就壓成低。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import db as dbmod
from src import rca, report
from src.grafana import Grafana
from src.grouping import Alert, converge
from tests.test_main import rows, world  # noqa: F401（fixture）

T0 = datetime(2026, 11, 21, 2, 41, 10, tzinfo=timezone.utc)
NOW = T0 + timedelta(seconds=64)
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs"), ("erp-core", "mq-broker")]


def incident(*services):
    alerts = [Alert(f"f{i}", "firing", f"r{i}", f"{s} 告警", s, "P2", T0 + timedelta(seconds=i), None, {"A": 1})
              for i, s in enumerate(services or ("mq-broker", "cim-eap", "amhs-mcs"))]
    [inc] = converge([], alerts, [], EDGES, now=NOW, close_after_seconds=300)
    return inc


def evidence(fake, es="es"):
    grafana = Grafana("http://grafana.test", "t", transport=fake.transport, retries=0)
    return rca.collect_evidence(incident(), EDGES, grafana, es_datasource_uid=es, log_index="factory-logs-*",
                                trace_index="factory-traces", id_fields=["eqp_id", "lot_id", "carrier_id"], now=NOW)


def answer(*candidates, summary="mq-broker 最可能是源頭"):
    return json.dumps({"summary": summary, "systems": ["mq-broker"], "ids": [],
                       "candidates": [{"source": s, "confidence": c, "reason": r} for s, c, r in candidates]}, ensure_ascii=False)


def test_candidates_are_kept_in_order_with_their_confidence(fake):
    ev = evidence(fake)
    summary, got = rca.read_answer(answer(("mq-broker", "高", "它的錯誤日誌寫 consumer lag"), ("cim-eap", "中", "T3 逾時"),
                                          ("amhs-mcs", "低", "只是下游")), ev)
    assert summary == "mq-broker 最可能是源頭"
    assert [(c["source"], c["confidence"]) for c in got] == [("mq-broker", "高"), ("cim-eap", "中"), ("amhs-mcs", "低")]
    assert rca.candidates_text(got).splitlines()[0] == "可能原因 1｜mq-broker（信心高）：它的錯誤日誌寫 consumer lag"


def test_a_system_with_no_direct_evidence_is_capped_at_low(fake):
    """erp-core 只出現在關係邊裡（沒有它的日誌、Trace、指標）：模型標高也沒用，程式壓成低。"""
    ev = evidence(fake)
    _summary, got = rca.read_answer(answer(("erp-core", "高", "它在上游"), ("mq-broker", "高", "有錯誤日誌")), ev)
    assert [(c["source"], c["confidence"]) for c in got] == [("erp-core", "低"), ("mq-broker", "高")]


def test_with_only_alerts_every_candidate_is_low(fake):
    ev = evidence(fake, es="")
    _summary, got = rca.read_answer(answer(("mq-broker", "高", "它的告警最先響"), ("cim-eap", "中", "告警數值偏高")), ev)
    assert [c["confidence"] for c in got] == ["低", "低"]


def test_a_system_named_only_in_another_systems_log_message_counts_as_direct_evidence(fake):
    ev = evidence(fake)
    ev["error_logs"].append({"time": "2026-11-21T02:41:07Z", "system": "mq-broker", "message": "connection refused erp-core:5432"})
    _summary, got = rca.read_answer(answer(("erp-core", "高", "mq-broker 的錯誤日誌寫連不上它")), ev)
    assert got[0]["confidence"] == "高"


@pytest.mark.parametrize("confidence", ["", "很高", "90%", None])
def test_missing_or_odd_confidence_becomes_low(fake, confidence):
    _summary, got = rca.read_answer(answer(("mq-broker", confidence, "x")), evidence(fake))
    assert got[0]["confidence"] == "低"


def test_at_most_three_candidates_are_kept(fake):
    _summary, got = rca.read_answer(answer(*[("mq-broker", "低", f"理由{i}") for i in range(6)]), evidence(fake))
    assert len(got) == rca.MAX_CANDIDATES == 3


@pytest.mark.parametrize("bad, message", [
    (("nonexistent-svc", "高", "x"), "nonexistent-svc"),           # 編出來的系統：整份退回
    (("mq-broker", "高", "集中在機台 EQP-LITHO-11"), "EQP-LITHO-11"),  # 理由裡編了一台機台
])
def test_invented_names_in_candidates_reject_the_whole_answer(fake, bad, message):
    with pytest.raises(ValueError, match=message):
        rca.read_answer(answer(bad), evidence(fake))


def test_answers_without_candidates_still_work(fake):
    old = json.dumps({"summary": "mq-broker 可能是源頭", "systems": ["mq-broker"], "ids": []}, ensure_ascii=False)
    assert rca.read_answer(old, evidence(fake)) == ("mq-broker 可能是源頭", [])
    assert rca.check_answer(old, evidence(fake)) == "mq-broker 可能是源頭"


def test_annotation_lists_causes_under_the_analysis_line_and_escapes_them():
    inc = incident()
    inc.summary = "[LLM 推論] 最可能是 mq-broker"
    inc.candidates = "可能原因 1｜mq-broker（信心高）：<b>磁碟滿</b>\n可能原因 2｜cim-eap（信心低）：只是下游"
    inc.evidence_note = "已有的證據｜告警 3 筆"
    l1, l2, c1, c2, have, last = report.render(inc, "http://g", "Asia/Taipei")["text"].split("<br>")
    assert l2 == "根因分析｜[LLM 推論] 最可能是 mq-broker"
    assert c1 == "可能原因 1｜mq-broker（信心高）：&lt;b&gt;磁碟滿&lt;/b&gt;" and c2.startswith("可能原因 2｜cim-eap（信心低）")
    assert have == "已有的證據｜告警 3 筆" and "台灣時間" in last


def llm_answering(payload):
    calls = []

    def handle(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": payload}}]})

    return httpx.MockTransport(handle), calls


def test_a_round_stores_the_causes_and_writes_them_to_grafana(world):  # noqa: F811
    w = world
    transport, calls = llm_answering(answer(("mq-broker", "高", "它的錯誤日誌寫 consumer lag exceeded"), ("cim-eap", "低", "下游")))
    w.receive("firing_mq", "firing_cim")
    assert w.run(60, llm=transport)["status"] == "ok"
    [inc] = rows(w, dbmod.incidents)
    assert inc["candidates"].splitlines() == ["可能原因 1｜mq-broker（信心高）：它的錯誤日誌寫 consumer lag exceeded",
                                              "可能原因 2｜cim-eap（信心低）：下游"]
    note = w.fake.annotations[inc["annotation_id"]]["text"]
    assert "<br>可能原因 1｜mq-broker（信心高）" in note and note.index("根因分析｜") < note.index("可能原因 1｜") < note.index("已有的證據｜")
    assert "就算只有告警，也要" in calls[0]["messages"][0]["content"]


def test_a_failed_analysis_has_no_causes_and_clears_the_old_ones(world):  # noqa: F811
    w = world
    good, _ = llm_answering(answer(("mq-broker", "高", "它的錯誤日誌寫 consumer lag exceeded")))
    w.receive("firing_mq")
    w.run(60, llm=good)
    assert rows(w, dbmod.incidents)[0]["candidates"]
    w.receive("firing_cim")   # 成員變了：重新分析，這次模型掛了
    w.run(120, llm=httpx.MockTransport(lambda r: httpx.Response(502, text="bad gateway")))
    [inc] = rows(w, dbmod.incidents)
    assert inc["summary"].startswith("[未經分析]") and not inc["candidates"]
    assert "可能原因" not in w.fake.annotations[inc["annotation_id"]]["text"]
