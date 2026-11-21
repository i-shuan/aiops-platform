"""RCA（證據收集、LLM 呼叫、幻覺檢查、退回）、註記三行、資料庫讀寫。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import rca, report
from src.db import Database
from src.grafana import Grafana
from src.grouping import Signal, converge

T0 = datetime(2026, 11, 21, 0, 26, tzinfo=timezone.utc)
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")]
LLM = {"api_url": "http://llm.test/v1", "api_key": "k", "model": "m"}


def incident():
    sigs = [Signal("r1", "MQ queue depth", "mq-broker", "P1", T0, T0 + timedelta(minutes=9), 10, 8.0, ("hourly_baseline",)),
            Signal("r2", "CIM T3 timeout", "cim-eap", "P2", T0 + timedelta(minutes=2), T0 + timedelta(minutes=9), 5, 6.0,
                   ("sparse_count",))]
    [inc] = converge([], sigs, EDGES, blind_services=set(), close_after_rounds=3)
    return inc


def evidence(fake):
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    return rca.collect_evidence(incident(), EDGES, g, es_datasource_uid="es", log_index="factory-logs",
                                trace_index="factory-traces", id_fields=["eqp_id", "lot_id", "carrier_id"])


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
    assert [a["system"] for a in ev["anomalies"]] == ["mq-broker", "cim-eap"]
    assert ev["edges"][0] == {"upstream": "mq-broker", "downstream": "cim-eap"}
    assert len(ev["error_logs"]) == 6 and len(ev["error_traces"]) == 2
    assert ev["id_counts"]["eqp_id"] == {"EQP-CVD-07": 3, "STK01": 1, "EQP-ETCH-02": 1}
    assert ev["id_counts"]["lot_id"]["LOT24A0031"] == 3 and ev["id_counts"]["carrier_id"]["FOUP0912"] == 3
    body = fake.es_requests[0]
    assert "cim-eap" in body and "mq-broker" in body  # 只撈這個事故的系統
    prompt = rca.build_prompt(ev)
    for part in ("【1.", "【2.", "【3.", "【4.", "EQP-CVD-07", "T3 timeout"):
        assert part in prompt


def test_evidence_without_log_datasource_or_when_es_fails_is_still_usable(fake):
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    ev = rca.collect_evidence(incident(), EDGES, g, es_datasource_uid="", log_index="*", trace_index="",
                              id_fields=["eqp_id"])
    assert ev["error_logs"] == [] and "沒有設定日誌資料源" in ev["notes"][0] and fake.es_requests == []
    fake.fail.add("/api/datasources/proxy")
    ev = rca.collect_evidence(incident(), EDGES, g, es_datasource_uid="es", log_index="*", trace_index="",
                              id_fields=["eqp_id"])
    assert ev["error_logs"] == [] and "撈取失敗" in ev["notes"][0] and len(ev["anomalies"]) == 2


def test_llm_answer_that_only_names_things_in_evidence_is_used(fake):
    transport, seen = llm(GOOD)
    line, analyzed, reason = rca.analyze(incident(), evidence(fake), transport=transport, **LLM)
    assert analyzed and reason == "" and line == f"[LLM 推論] {GOOD['summary']}"
    sent = json.loads(seen[0].content)
    assert str(seen[0].url) == "http://llm.test/v1/chat/completions" and sent["model"] == "m"
    assert seen[0].headers["authorization"] == "Bearer k" and "EQP-CVD-07" in sent["messages"][1]["content"]


@pytest.mark.parametrize("answer, why", [
    ({**GOOD, "systems": ["mq-broker", "erp-core"]}, "erp-core"),                  # 編了一個系統
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
    assert l1 == "【P1・進行中】受影響系統：cim-eap、mq-broker｜收斂告警 15 筆"
    assert l2 == "[LLM 推論] 源頭可能是 mq-broker"
    assert l3.startswith("2026-11-21 08:26 ～ 進行中（台灣時間）｜調整優先權：")
    assert '<a href="https://grafana.corp/alerting/r1/edit" target="_blank">MQ queue depth（P1）</a>' in l3
    assert l3.index("MQ queue depth") < l3.index("CIM T3 timeout")  # 嚴重的排前面
    assert body["tags"] == ["aiops", f"aiops-incident:{inc.uid}", "P1", "open", "cim-eap", "mq-broker"]
    assert body["time"] == int(T0.timestamp() * 1000) and body["timeEnd"] == body["time"] + 9 * 60000


def test_closed_incident_shows_its_end_time():
    inc = incident()
    inc.status, inc.ended_at, inc.summary = "closed", T0 + timedelta(minutes=9), "x"
    body = report.render(inc, "http://g", "Asia/Taipei")
    assert "【P1・已結案】" in body["text"] and "2026-11-21 08:26 ～ 2026-11-21 08:35（台灣時間）" in body["text"]
    assert "closed" in body["tags"]


def test_html_in_names_and_llm_text_is_escaped():
    inc = incident()
    inc.summary = "[LLM 推論] <script>alert(1)</script>\n第二行 <img src=x onerror=1>"
    inc.signals["r1"]["title"] = 'MQ <b>depth</b> "x"'
    inc.services = ["a<b", "mq-broker"]
    text = report.render(inc, "http://g", "Asia/Taipei")["text"]
    assert text.count("<br>") == 2 and "<script>" not in text and "<img" not in text and "<b>" not in text
    assert "&lt;script&gt;" in text and "a&lt;b" in text
    assert text.count("<a href=") == 2  # 只有程式自己產生的連結是標籤


# ── 資料庫 ──

@pytest.fixture
def db(tmp_path):
    database = Database(f"sqlite:///{tmp_path}/t.db")
    yield database
    database.close()


def test_incident_round_trips_through_the_database(db):
    inc = incident()
    inc.summary, inc.analyzed, inc.annotation_id, inc.annotated = "s", True, 7, True
    db.save_incidents([inc], {"r1": T0 + timedelta(minutes=9)}, T0)
    [back] = db.open_incidents()
    assert back == inc and back.started_at.tzinfo is not None
    assert back.signals["r1"]["first_ts"] == T0 and db.get_incident(inc.uid) == inc
    assert db.load_rule("r1")["last_anomaly_ts"] == T0 + timedelta(minutes=9)
    inc.status, inc.ended_at, inc.annotated = "closed", T0, False
    db.save_incidents([inc], {}, T0)
    assert db.open_incidents() == [] and [i.uid for i in db.unannotated_incidents()] == [inc.uid]


def test_baseline_round_trip_and_mark_does_not_erase_it(db):
    assert db.load_rule("r1") is None
    db.save_baseline("r1", title="t", service="s", query_hash="h", state={"version": 1, "x": [1.5]}, train_points=99, now=T0)
    db.mark_anomaly("r1", T0, T0)
    row = db.load_rule("r1")
    assert row["state"] == {"version": 1, "x": [1.5]} and row["trained_at"] == T0 and row["last_anomaly_ts"] == T0
    db.save_baseline("r1", title="t", service="s", query_hash="h2", state=None, train_points=3, now=T0)
    assert db.load_rule("r1")["state"] is None and db.load_rule("r1")["last_anomaly_ts"] == T0


def test_incident_windows_are_per_service_and_tables_are_created_idempotently(db, tmp_path):
    inc = incident()
    db.save_incidents([inc], {}, T0)
    assert db.incident_windows("mq-broker", T0 - timedelta(days=1)) == [(T0, T0 + timedelta(minutes=9))]
    assert db.incident_windows("amhs-mcs", T0 - timedelta(days=1)) == []
    assert db.incident_windows("mq-broker", T0 + timedelta(days=1)) == []
    again = Database(f"sqlite:///{tmp_path}/t.db")  # 第二次啟動：表已經在，資料還在
    assert len(again.open_incidents()) == 1
    again.close()
