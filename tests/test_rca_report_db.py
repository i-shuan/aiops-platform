"""RCA（證據收集、LLM 呼叫、幻覺檢查、退回）、註記三行、資料庫讀寫。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import rca, report
from src.db import Database
from src.grafana import Grafana, parse_webhook
from src.grouping import converge
from tests.conftest import collect_evidence, webhook

T0 = datetime(2026, 11, 21, 2, 41, 10, tzinfo=timezone.utc)   # 樣本裡 MQ 告警的 startsAt
NOW = T0 + timedelta(seconds=64)
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")]
LLM = {"api_url": "http://llm.test/v1", "api_key": "k", "model": "m"}


def alerts(*names):
    return [a for n in names for a in parse_webhook(webhook(n))[0]]


def incident():
    [inc] = converge([], alerts("firing_mq", "firing_cim"), [], EDGES, now=NOW, close_after_seconds=300)
    return inc


def evidence(fake):
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    return collect_evidence(incident(), EDGES, g, logs_datasource_uid="vl", id_fields=["eqp_id", "lot_id", "carrier_id"], now=NOW)


def llm(answer, status=200):
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if isinstance(answer, Exception):
            raise answer
        content = answer if isinstance(answer, str) else json.dumps(answer, ensure_ascii=False)
        return httpx.Response(status, json={"choices": [{"message": {"role": "assistant", "content": content}}]})

    return httpx.MockTransport(handler), seen


GOOD = {"summary": "mq-broker 佇列積壓為可能源頭，下游 cim-eap 出現 T3 逾時，錯誤集中在機台 EQP-CVD-07 與批號 LOT24A0031",
        "systems": ["mq-broker", "cim-eap"], "ids": ["EQP-CVD-07", "LOT24A0031"]}


def test_evidence_has_the_four_packs(fake):
    ev = evidence(fake)
    assert [(a["system"], a["status"], a["values"]) for a in ev["anomalies"]] == [
        ("mq-broker", "firing", {"A": 350, "C": 1}), ("cim-eap", "firing", {"A": 12, "C": 1})]
    assert ev["edges"][0] == {"upstream": "mq-broker", "downstream": "cim-eap"}
    # 只撈這個事故的系統（mq-broker、cim-eap）：樣本裡 amhs-mcs 的那一筆錯誤（STK01）不在裡面
    assert (len(ev["error_logs"]), sum(e.get("similar_in_sample", 1) for e in ev["error_logs"])) == (3, 5)
    assert ev["id_counts"]["eqp_id"] == {"EQP-CVD-07": 3, "EQP-ETCH-02": 1}
    assert ev["id_counts"]["lot_id"]["LOT24A0031"] == 2 and ev["id_counts"]["carrier_id"]["FOUP0912"] == 2
    [query, neighbors] = fake.queries("sort by (_time) desc")
    assert query.startswith('"service.name":in("cim-eap","mq-broker") ')
    # 關係邊上直接相連、但沒有告警的 amhs-mcs：另外補撈它的錯誤與警告，放在自己的一包，不混進事故系統的樣本
    assert neighbors.startswith('"service.name":in("amhs-mcs") ')
    [stk] = ev["neighbor_logs"]
    assert (stk["system"], stk["example"], stk["level"]) == ("amhs-mcs", "transfer job aborted: destination port occupied", "ERROR")
    prompt = rca.build_prompt(ev)
    for part in ("【1.", "【2.", "【3.", "【4.", "EQP-CVD-07", "T3 timeout"):
        assert part in prompt


def test_evidence_without_log_datasource_or_when_the_query_fails_is_still_usable(fake):
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    ev = collect_evidence(incident(), EDGES, g, logs_datasource_uid="", id_fields=["eqp_id"], now=NOW)
    assert ev["error_logs"] == [] and "沒有設定日誌資料源" in ev["notes"][0] and fake.log_queries == []
    fake.fail.add("/api/datasources/proxy")
    ev = collect_evidence(incident(), EDGES, g, logs_datasource_uid="vl", id_fields=["eqp_id"], now=NOW)
    assert ev["error_logs"] == [] and "撈取失敗" in ev["notes"][0] and len(ev["anomalies"]) == 2


def test_llm_answer_that_only_names_things_in_evidence_is_used(fake):
    transport, seen = llm(GOOD)
    line, analyzed, reason = rca.analyze(incident(), evidence(fake), transport=transport, **LLM)
    assert analyzed and reason == "" and line == f"[LLM 推論] {GOOD['summary']}"
    sent = json.loads(seen[0].content)
    assert str(seen[0].url) == "http://llm.test/v1/chat/completions" and sent["model"] == "m"
    assert seen[0].headers["authorization"] == "Bearer k" and "EQP-CVD-07" in sent["messages"][1]["content"]


@pytest.mark.parametrize("answer, why", [
    ({**GOOD, "summary": "源頭是 erp-core，拖累 mq-broker", "systems": ["mq-broker", "erp-core"], "ids": []}, "erp-core"),   # 編了一個系統
    ({**GOOD, "ids": ["EQP-CVD-99"]}, "EQP-CVD-99"),                               # 編了一台機台
    ({**GOOD, "summary": "源頭是 mq-broker，集中在機台 EQP-LITHO-11", "ids": []}, "EQP-LITHO-11"),  # 只寫在句子裡
    ("根因是 mq-broker", "JSONDecodeError"),                                        # 不是 JSON
    ({"systems": []}, "KeyError"),                                                 # 少了 summary
    ({**GOOD, "summary": "  "}, "summary 是空的"),
    (httpx.ConnectError("refused"), "ConnectError"),
    (httpx.ReadTimeout("slow"), "ReadTimeout"),
])
def test_bad_llm_answers_fall_back_to_the_raw_summary(fake, answer, why):
    transport, _ = llm(answer)
    line, analyzed, reason = rca.analyze(incident(), evidence(fake), transport=transport, **LLM)
    assert not analyzed and why in reason
    assert line == "[未經分析] mq-broker：MQ queue depth；cim-eap：CIM T3 timeout"


def test_llm_http_error_falls_back(fake):
    transport, _ = llm(GOOD, status=500)
    line, analyzed, reason = rca.analyze(incident(), evidence(fake), transport=transport, **LLM)
    assert not analyzed and "500" in reason and line.startswith("[未經分析]")


def test_without_llm_settings_nothing_is_sent(fake):
    transport, seen = llm(GOOD)
    line, analyzed, reason = rca.analyze(incident(), evidence(fake), api_url="", api_key="", model="", transport=transport)
    assert not analyzed and seen == [] and line.startswith("[未經分析]") and "沒有設定" in reason


def test_fenced_json_and_full_endpoint_url_are_accepted(fake):
    transport, seen = llm("```json\n" + json.dumps(GOOD, ensure_ascii=False) + "\n```")
    line, analyzed, _ = rca.analyze(incident(), evidence(fake), api_url="http://llm.test/v1/chat/completions",
                                    api_key="", model="m", transport=transport)
    assert analyzed and str(seen[0].url) == "http://llm.test/v1/chat/completions"
    assert "authorization" not in seen[0].headers


# ── 註記三行 ──

def test_annotation_is_three_lines_with_taiwan_time_and_rule_links():
    inc = incident()
    inc.summary = "[LLM 推論] 源頭可能是 mq-broker"
    body = report.render(inc, "https://grafana.corp/", "Asia/Taipei")
    l1, l2, l3 = body["text"].split("<br>")
    assert l1 == "【P1・進行中】受影響系統：cim-eap、mq-broker｜收斂告警 2 筆"
    assert l2 == "根因分析｜[LLM 推論] 源頭可能是 mq-broker"
    assert l3.startswith("2026-11-21 10:41 ～ 進行中（台灣時間）｜調整優先權：")
    assert '<a href="https://grafana.corp/alerting/cg4xgh0mv7oqod/edit" target="_blank">MQ queue depth（P1）</a>' in l3
    assert l3.index("MQ queue depth") < l3.index("CIM T3 timeout")  # 嚴重的排前面
    assert body["tags"] == ["aiops", f"aiops-incident:{inc.uid}", "P1", "open", "cim-eap", "mq-broker"]
    assert body["time"] == int(T0.timestamp() * 1000) and body["timeEnd"] == int(NOW.timestamp() * 1000)


def test_closed_incident_shows_its_end_time():
    inc = incident()
    inc.status, inc.ended_at, inc.summary = "closed", T0 + timedelta(minutes=9), "x"
    body = report.render(inc, "http://g", "Asia/Taipei")
    assert "【P1・已結案】" in body["text"] and "2026-11-21 10:41 ～ 2026-11-21 10:50（台灣時間）" in body["text"]
    assert "closed" in body["tags"] and body["timeEnd"] == int(inc.ended_at.timestamp() * 1000)


def test_html_in_names_and_llm_text_is_escaped():
    inc = incident()
    inc.summary = "[LLM 推論] <script>alert(1)</script>\n第二行 <img src=x onerror=1>"
    inc.signals["369c5f4adcb630d1"]["title"] = 'MQ <b>depth</b> "x"'
    inc.services = ["a<b", "mq-broker"]
    text = report.render(inc, "http://g", "Asia/Taipei")["text"]
    assert text.count("<br>") == 2 and "<script>" not in text and "<img" not in text and "<b>" not in text
    assert "&lt;script&gt;" in text and "a&lt;b" in text
    assert text.count("<a href=") == 2  # 只有程式自己產生的連結是標籤


def test_two_instances_of_one_rule_share_a_link_and_missing_rule_uid_gives_no_link():
    inc = incident()
    twin = dict(inc.signals["369c5f4adcb630d1"], severity="P0")
    inc.signals["other-instance"] = twin
    inc.signals["866d3aec816817da"]["rule_uid"] = ""
    text = report.render(inc, "http://g", "Asia/Taipei")["text"]
    assert text.count("<a href=") == 1 and "MQ queue depth（P0）</a>" in text and "CIM T3 timeout（P2）" in text


# ── 資料庫 ──

@pytest.fixture
def db(db_url):
    database = Database(db_url)
    yield database
    database.close()


def test_incident_round_trips_through_the_database(db):
    inc = incident()
    inc.summary, inc.analyzed, inc.annotation_id, inc.annotated, inc.resolved_at = "s", True, 7, True, NOW
    db.save_incidents([inc], NOW)
    [back] = db.open_incidents()
    assert back == inc and back.started_at.tzinfo is not None and db.get_incident(inc.uid) == inc
    assert back.signals["369c5f4adcb630d1"]["starts_at"] == T0 and back.signals["369c5f4adcb630d1"]["ends_at"] is None
    inc.status, inc.ended_at, inc.annotated = "closed", NOW, False
    db.save_incidents([inc], NOW)
    assert db.open_incidents() == [] and [i.uid for i in db.unannotated_incidents()] == [inc.uid]


def test_inbox_keeps_alerts_in_arrival_order_until_they_are_processed_with_the_incidents(db, db_url):
    assert db.pending() == ([], [])
    db.enqueue([], NOW)
    db.enqueue(alerts("firing_mq"), NOW)
    db.enqueue(alerts("firing_cim", "resolved_mq"), NOW)
    ids, got = db.pending()
    assert got == alerts("firing_mq", "firing_cim", "resolved_mq") and len(ids) == 3
    assert got[2].ends_at == datetime(2026, 11, 21, 2, 42, 20, tzinfo=timezone.utc) and got[0].values == {"A": 350, "C": 1}
    again = Database(db_url)  # 服務重啟：還沒處理的告警還在
    assert again.pending()[1] == got
    again.close()
    db.save_incidents([incident()], NOW, processed=ids[:2])
    assert db.pending() == ([ids[2]], alerts("resolved_mq")) and len(db.open_incidents()) == 1


def test_temperature_is_not_sent_unless_configured(fake):
    """有些模型只接受預設的 temperature，硬送 0 會被 400 拒絕（用真的 OpenAI 模型 gpt-6-luna 實測遇到的）。"""
    transport, seen = llm(GOOD)
    rca.analyze(incident(), evidence(fake), transport=transport, **LLM)
    assert "temperature" not in json.loads(seen[0].content)
    transport, seen = llm(GOOD)
    rca.analyze(incident(), evidence(fake), transport=transport, options=rca.LlmOptions(temperature=0.2), **LLM)
    assert json.loads(seen[0].content)["temperature"] == 0.2
    transport, seen = llm(GOOD)
    rca.analyze(incident(), evidence(fake), transport=transport, options=rca.LlmOptions(temperature=0), **LLM)  # 0 是合法值，不能被當成「沒設」
    assert json.loads(seen[0].content)["temperature"] == 0


def test_a_model_rejecting_the_request_falls_back_and_the_reason_names_the_problem(fake):
    rejecting = httpx.MockTransport(lambda r: httpx.Response(400, json={"error": {"message": "Unsupported value: 'temperature'"}}))
    line, analyzed, reason = rca.analyze(incident(), evidence(fake), transport=rejecting, options=rca.LlmOptions(temperature=0), **LLM)
    assert not analyzed and "400" in reason and line.startswith("[未經分析]")
