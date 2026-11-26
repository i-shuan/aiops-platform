"""LLM 不通（或回答沒通過檢查）時的註記：流程照走，第 2 行說明原因與什麼時候再試，底下直接列程式收集到的證據。
那幾行是原文與筆數，沒有推論，所以標的是 [未經分析]，不是 [LLM 推論]。"""
from __future__ import annotations

import json
from dataclasses import replace

import httpx

from src import db as dbmod
from src import rca
from tests.conftest import SHIFT, log_row, recorded_trace, shifted_trace
from tests.test_main import T0, rows, world  # noqa: F401（fixture）


def down(request):
    raise httpx.ConnectError("llm down")


def answering(content, status=200):
    return httpx.MockTransport(lambda r: httpx.Response(status, json={"choices": [{"message": {"content": content}}]}))


def note_lines(w):
    [inc] = rows(w, dbmod.incidents)
    return w.fake.annotations[inc["annotation_id"]]["text"].split("<br>"), inc


def test_when_the_llm_is_down_the_evidence_is_listed_under_an_honest_label(world):  # noqa: F811
    w = world
    w.receive("firing_mq", "firing_cim", "firing_amhs")
    out = w.run(64, llm=httpx.MockTransport(down))
    assert out["status"] == "degraded" and out["needs_followup"]
    lines, inc = note_lines(w)
    assert lines[0].startswith("【P1・進行中】")
    assert lines[1] == "根因分析｜[未經分析] LLM 目前連不上（連續失敗 1 次，約 1 分鐘後重試）。以下是程式收集到的證據，沒有經過推論："
    assert "[LLM 推論]" not in "".join(lines)                       # 沒有經過模型的東西不准標成推論
    facts = [l for l in lines if l.startswith("・")]
    assert facts[0] == "・告警：mq-broker：MQ queue depth；cim-eap：CIM T3 timeout；amhs-mcs：AMHS transfer duration"
    assert len(facts) == rca.MAX_FACT_LINES and all("新出現：" in f and "事故前 0 筆）" in f for f in facts[1:])
    assert "・cim-eap 新出現：T3 timeout waiting S6F12 reply（事故期間 3 筆，事故前 0 筆）" in facts   # 原文與筆數
    assert lines[len(facts) + 2].startswith("證據｜已有告警 3 筆")     # 原本那一行還在，排在後面
    assert not inc["analyzed"] and not inc["candidates"] and inc["llm_failures"] == 1


def test_errors_that_were_already_there_come_after_the_new_ones_and_the_chain_gets_the_last_line(world):  # noqa: F811
    w = world
    start = T0 - rca.timedelta(minutes=10)
    w.fake.logs += [log_row(start - rca.timedelta(minutes=5, seconds=n), "mq-broker", "ERROR", "consumer lag exceeded on queue lot.dispatch")
                    for n in range(4)]                               # 這一種事故前就有 4 筆
    trace = shifted_trace(recorded_trace(), SHIFT)
    trace["processes"]["p1"]["serviceName"] = "cim-eap"
    w.fake.traces = {trace["traceID"]: trace}
    w.receive("firing_mq", "firing_cim")
    w.run(64, llm=httpx.MockTransport(down), settings=replace(w.settings, traces_datasource_uid="vt-uid"))
    lines, _inc = note_lines(w)
    facts = [l for l in lines if l.startswith("・")]
    assert len(facts) == rca.MAX_FACT_LINES
    assert facts[-1] == "・呼叫鏈停在：cim-eap → mes-db，connection refused: mes-db:5432"
    old = next(f for f in facts if "consumer lag" in f)
    assert old == "・mq-broker：consumer lag exceeded on queue lot.dispatch（事故期間 1 筆，事故前 4 筆）"   # 不是新出現的：不標，照實寫筆數
    assert facts.index(old) > max(i for i, f in enumerate(facts) if "新出現" in f)


def test_a_rejected_answer_says_so_and_the_wait_follows_the_backoff(world):  # noqa: F811
    w = world
    invented = json.dumps({"summary": "源頭是 billing-db", "systems": ["billing-db"], "ids": []}, ensure_ascii=False)
    w.receive("firing_mq")
    w.run(64, llm=answering(invented))                               # 點名了證據裡沒有的系統：幻覺檢查擋下
    lines, _ = note_lines(w)
    assert lines[1].startswith("根因分析｜[未經分析] LLM 的回答沒有通過檢查（連續失敗 1 次，約 1 分鐘後重試）")
    w.run(64 + 61, llm=answering("not json"))
    lines, _ = note_lines(w)
    assert "（連續失敗 2 次，約 2 分鐘後重試）" in lines[1]
    w.run(64 + 61 + 121, llm=answering("", status=503))              # 閘道回 503：算連不上
    lines, inc = note_lines(w)
    assert lines[1].startswith("根因分析｜[未經分析] LLM 目前連不上（連續失敗 3 次，約 4 分鐘後重試）") and inc["llm_failures"] == 3


def test_waiting_rounds_do_not_rewrite_the_annotation_and_recovery_replaces_everything(world):  # noqa: F811
    w = world
    w.receive("firing_mq", "firing_cim")
    w.run(64, llm=httpx.MockTransport(down))
    calls = len(w.fake.calls)
    w.run(90, llm=httpx.MockTransport(down))                         # 退避中：不查、不問、也不重寫註記（剩幾分鐘變了不算）
    assert len(w.fake.calls) == calls
    w.run(64 + 61)                                                   # LLM 回來了
    lines, inc = note_lines(w)
    assert lines[1].startswith("根因分析｜[LLM 推論]") and not any(l.startswith("・") for l in lines)
    assert inc["analyzed"] and inc["llm_failures"] == 0 and inc["evidence_note"].startswith("證據｜已有")


def test_without_logs_the_alerts_line_is_still_there_and_long_messages_are_cut(world):  # noqa: F811
    w = world
    w.fake.logs = [log_row(T0 - rca.timedelta(seconds=30), "mq-broker", "ERROR", "<b>disk</b> " + "x" * 500)]
    w.receive("firing_mq")
    w.run(64, llm=httpx.MockTransport(down))
    lines, _ = note_lines(w)
    facts = [l for l in lines if l.startswith("・")]
    assert facts[0] == "・告警：mq-broker：MQ queue depth" and len(facts) == 2
    assert "&lt;b&gt;disk&lt;/b&gt;" in facts[1] and "<b>" not in facts[1] and facts[1].count("x") <= rca.FACT_CHARS   # 跳脫、截斷
    w.fake.logs = []
    w.receive("firing_cim")
    w.run(64 + 61, llm=httpx.MockTransport(down))
    lines, _ = note_lines(w)
    assert [l for l in lines if l.startswith("・")][0] == "・告警：mq-broker：MQ queue depth；cim-eap：CIM T3 timeout"


def test_not_configuring_an_llm_is_not_a_failure_and_keeps_the_plain_summary(world):  # noqa: F811
    w = world
    w.receive("firing_mq")
    assert w.run(64, settings=replace(w.settings, llm_model=""))["status"] == "ok"
    lines, inc = note_lines(w)
    assert lines[1] == "根因分析｜[未經分析] mq-broker：MQ queue depth" and not any(l.startswith("・") for l in lines)
    assert inc["llm_failures"] == 0
