"""註記的「證據」那一行：這次分析手上有哪些證據、缺哪些、為什麼缺。全部由程式從實際撈到的結果產生，不經過 LLM。
「錯誤日誌 0 筆」三種原因的計數回應是真的 Grafana 13.0.7 + Elasticsearch 8.19.23 錄下來的（log_counts_*.json，
錄製腳本 record_log_counts.py）。"""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import db as dbmod
from src import rca, report
from src.grafana import Grafana, LogFields
from src.grouping import Alert, converge
from tests.conftest import recorded
from tests.test_main import LLM_ANSWER, rows, world  # noqa: F401（fixture）

T0 = datetime(2026, 11, 21, 2, 41, 10, tzinfo=timezone.utc)
NOW = T0 + timedelta(seconds=64)
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")]
METRIC = rca.MetricQuery("佇列待處理訊息數", 'sum by (queue) (rabbitmq_queue_messages_ready{service="$service"})')


def incident(*services):
    alerts = [Alert(f"f{i}", "firing", f"r{i}", f"{s} 告警", s, "P2", T0 + timedelta(seconds=i), None, {"A": 1})
              for i, s in enumerate(services or ("mq-broker", "cim-eap", "amhs-mcs"))]
    [inc] = converge([], alerts, [], EDGES, now=NOW, close_after_seconds=300)
    return inc


def no_error_logs(fake):
    """ES 查得到、但錯誤日誌是 0 筆（回應格式是錄下來的真實樣本，只把命中清空）。"""
    fake.es = copy.deepcopy(recorded("es_proxy_logs_traces")["response"])
    for resp in fake.es["responses"]:
        resp["hits"] = {"total": {"value": 0, "relation": "eq"}, "max_score": None, "hits": []}


def collect(fake, inc=None, *, es="es", tempo="", metrics=rca.MetricOptions(), edges=EDGES, **kw):
    grafana = Grafana("http://grafana.test", "t", transport=fake.transport, retries=0)
    return rca.collect_evidence(inc or incident(), edges, grafana, es_datasource_uid=es, log_index="factory-logs-*",
                                trace_index="factory-traces", id_fields=["eqp_id", "lot_id", "carrier_id"], now=NOW,
                                tempo_datasource_uid=tempo, metrics=metrics, **kw)


def line(ev, **kw):
    return rca.overview_text(rca.overview(ev, **kw))


# ── 有什麼 ──

def test_everything_present_is_listed_with_counts(fake):
    ev = collect(fake, tempo="tempo-uid", metrics=rca.MetricOptions(datasource_uid="prom-uid", queries=(METRIC,)))
    got = rca.overview(ev)
    assert got["missing"] == [] and got["off"] == [] and got["failed"] == []
    text = "、".join(got["have"])
    assert text.startswith("告警 3 筆、這些系統之間的關係邊 2 條、錯誤日誌 ")
    for part in ("識別碼", "跨系統時間線", "失敗請求的呼叫鏈 1 條", "指標走勢 1 條"):
        assert part in text
    assert rca.overview_text(got) == "有的證據｜" + text   # 什麼都不缺：只有一行


# ── 錯誤日誌 0 筆：三種原因分得出來，各有下一步 ──

@pytest.mark.parametrize("counts, says, next_step", [
    ("log_counts_index_empty", "索引在這段時間一筆資料都沒有", "es.log_index"),
    ("log_counts_service_mismatch", "索引有 4 筆，但沒有一筆的 service.name 是這些系統", "值是否跟告警的系統名稱完全一樣"),
    ("log_counts_no_errors", "這些系統有 5 筆日誌，但 log.level 沒有一筆是 ERROR／FATAL／CRITICAL", "等級的寫法對不上"),
])
def test_zero_error_logs_says_which_step_failed(fake, counts, says, next_step):
    no_error_logs(fake)
    fake.log_counts = counts
    ev = collect(fake)
    text = line(ev)
    [missing] = [l for l in text.splitlines() if l.startswith("缺的證據｜錯誤日誌：")]   # 自己一行
    assert says in missing and next_step in missing
    assert len(fake.count_requests) == 1
    # 送出去的計數查詢跟錄製時同一個形狀（兩個子查詢：整個時間窗、時間窗 + 系統）
    was = [json.loads(l) for l in recorded(counts)["request"]["body"].splitlines()][1::2]
    shape = lambda q: (q["size"], q["track_total_hits"], len(q["query"]["bool"]["filter"]))
    assert [shape(q) for q in fake.count_requests[0]] == [shape(q) for q in was]


def test_the_extra_count_query_only_happens_when_there_are_no_error_logs(fake):
    collect(fake)
    assert fake.count_requests == []


def test_no_level_field_and_custom_levels_are_named_in_the_reason(fake):
    no_error_logs(fake)
    ev = collect(fake, log_fields=LogFields(service="kubernetes.container_name", level=""))
    assert "訊息文字裡沒有錯誤字樣" in line(ev, log_fields=LogFields(service="kubernetes.container_name", level=""))
    assert "severity 沒有一筆是 E／SEVERE" in line(ev, log_fields=LogFields(level="severity"), error_levels=("E", "SEVERE"))


def test_if_the_count_itself_fails_the_line_still_says_logs_are_missing(fake):
    no_error_logs(fake)
    real = fake.msearch
    fake.msearch = lambda body: real(body) if '"size": 100' in body else httpx.Response(500, json={"message": "boom"})
    ev = collect(fake)
    assert "缺的證據｜錯誤日誌：沒有查到這些系統的錯誤日誌" in line(ev) and "log_counts" not in ev


# ── 沒接、查詢失敗 ──

def test_sources_that_are_not_configured_are_listed_as_not_connected(fake):
    text = line(collect(fake, es=""))
    assert "還沒接的｜日誌（es.datasource_uid）、呼叫鏈（tempo.datasource_uid）、指標走勢（metrics.queries）" in text
    assert "查詢失敗" not in text


def test_failed_queries_are_reported_as_failed_not_as_missing(fake):
    fake.fail.add("/api/datasources/proxy/uid/")
    ev = collect(fake, tempo="tempo-uid", metrics=rca.MetricOptions(datasource_uid="prom-uid", queries=(METRIC,)))
    got = rca.overview(ev)
    assert [s.split("：")[0] for s in got["failed"]] == ["日誌", "呼叫鏈", "指標走勢"] and got["off"] == []


def test_queried_but_empty_traces_and_metrics_say_why(fake):
    fake.tempo_search = {"traces": []}
    ev = collect(fake, incident("amhs-mcs"), tempo="tempo-uid",
                 metrics=rca.MetricOptions(datasource_uid="prom-uid", queries=(METRIC,)))
    text = line(ev)
    assert "缺的證據｜呼叫鏈：Tempo 裡沒有這些系統的失敗請求" in text and "缺的證據｜指標走勢：設定的查詢在這段時間都沒有資料" in text


# ── 關係邊 ──

def test_edges_are_judged_against_the_systems_in_this_incident(fake):
    assert "這些系統之間的關係邊 2 條" in line(collect(fake))
    assert "缺的證據｜關係邊：這幾個系統之間沒有宣告上下游" in line(collect(fake, edges=[("x", "y")]))
    assert "連到這個系統的關係邊 1 條（另一端沒有告警）" in line(collect(fake, incident("mq-broker")))
    alone = line(collect(fake, incident("mq-broker"), edges=[]))
    assert "關係邊" not in alone   # 單一系統、沒宣告關係邊：不算缺（系統自己出問題時不需要它）


# ── 註記與整輪 ──

def test_annotation_shows_the_evidence_line_between_summary_and_time_and_escapes_it():
    inc = incident()
    inc.summary = "[LLM 推論] 還無法判定源頭"
    inc.evidence_note = "有的證據｜告警 3 筆\n缺的證據｜錯誤日誌：<b>x</b>  多個空白\n\n還沒接的｜呼叫鏈（tempo.datasource_uid）"
    l1, l2, have, missing, off, last = report.render(inc, "http://g", "Asia/Taipei")["text"].split("<br>")
    assert (have, off) == ("有的證據｜告警 3 筆", "還沒接的｜呼叫鏈（tempo.datasource_uid）")
    assert missing == "缺的證據｜錯誤日誌：&lt;b&gt;x&lt;/b&gt; 多個空白"   # 跳脫；每項自己一行
    assert l2 == "[LLM 推論] 還無法判定源頭" and "台灣時間" in last
    inc.evidence_note = ""
    assert report.render(inc, "http://g", "Asia/Taipei")["text"].count("<br>") == 2   # 還沒收集過證據：沒有這一行


def test_a_round_writes_the_reason_into_the_annotation_and_tells_the_llm(world):  # noqa: F811
    """使用者實際遇到的情況：告警進來了、日誌的欄位對不上。註記要直接說出是哪一關沒過，而不是只有「證據不足」。"""
    w = world
    no_error_logs(w.fake)
    w.fake.log_counts = "log_counts_service_mismatch"
    calls = []

    def llm(request):   # 沒有日誌的情境，模型不會（也不能）點名機台：world 預設的回答有 EQP-CVD-07，會被幻覺檢查擋掉
        calls.append(json.loads(request.content))
        answer = {"summary": "還無法判定源頭。只有告警與關係邊，日誌的系統名稱欄位對不上。", "systems": ["mq-broker"], "ids": []}
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(answer, ensure_ascii=False)}}]})

    w.receive("firing_mq", "firing_cim")
    assert w.run(60, llm=httpx.MockTransport(llm))["status"] == "ok"
    [inc] = rows(w, dbmod.incidents)
    assert "缺的證據｜錯誤日誌：索引有 4 筆，但沒有一筆的 service.name 是這些系統" in inc["evidence_note"]
    note = w.fake.annotations[inc["annotation_id"]]["text"]
    assert "[LLM 推論] 還無法判定源頭" in note and "<br>有的證據｜告警 2 筆" in note and "<br>缺的證據｜錯誤日誌：" in note and "service.name 是這些系統" in note
    prompt = calls[0]["messages"][1]["content"]
    assert prompt.startswith("【0. 證據概況") and "service.name 是這些系統" in prompt.split("【1.")[0]
    system = calls[0]["messages"][0]["content"]
    assert "不要只寫「證據不足」" in system and "不能當成它就是源頭的依據" in system


def test_the_evidence_line_survives_a_restart_and_is_kept_when_only_the_status_changes(world):  # noqa: F811
    w = world
    w.receive("firing_mq")
    w.run(60)
    [before] = rows(w, dbmod.incidents)
    w.receive("resolved_mq")
    w.run(120)   # 只是恢復：不重新分析，證據那一行照舊
    [after] = rows(w, dbmod.incidents)
    assert after["evidence_note"] == before["evidence_note"] != "" and len(w.llm_calls) == 1
    assert "有的證據｜" in w.fake.annotations[after["annotation_id"]]["text"]


def test_without_llm_the_evidence_line_is_still_written(world):  # noqa: F811
    w = world
    w.receive("firing_mq")
    w.run(60, settings=replace(w.settings, llm_model=""))
    [inc] = rows(w, dbmod.incidents)
    assert inc["summary"].startswith("[未經分析]") and inc["evidence_note"].startswith("有的證據｜告警 1 筆")


def test_source_field_is_optional_but_must_be_a_system_in_the_evidence(fake):
    ev = collect(fake)
    good = {"summary": "mq-broker 最可能是源頭", "source": "mq-broker", "systems": ["mq-broker"], "ids": []}
    assert rca.check_answer(json.dumps(good), ev).startswith("mq-broker")
    assert rca.check_answer(json.dumps({**good, "source": ""}), ev)                      # 還無法判定
    assert rca.check_answer(json.dumps({k: v for k, v in good.items() if k != "source"}), ev)   # 自訂提示詞沒要求這一欄
    with pytest.raises(ValueError, match="erp-core"):
        rca.check_answer(json.dumps({**good, "source": "erp-core"}), ev)
