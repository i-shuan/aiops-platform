"""註記的「證據」那一行：這次分析手上有哪些證據、缺哪些、為什麼缺。全部由程式從實際撈到的結果產生，不經過 LLM。
「錯誤日誌 0 筆」三種原因各有一批從真的 VictoriaLogs v1.17.0 錄下來的資料（victoria_logs.json 的 count-* 三個 dataset，
錄製腳本 record_victoria.py）；假的 Grafana 照查詢條件去數，數出來的跟真的一樣（tests/test_grafana.py 有逐條比對）。"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import db as dbmod
from src import rca, report
from src.grafana import Grafana
from src.logs import LogFields
from src.grouping import Alert, converge
from tests.conftest import collect_evidence, SHIFT, recorded_rows, recorded_trace, shifted, shifted_trace, victoria
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
    """日誌查得到、但沒有錯誤等級的（樣本裡只剩那一筆 INFO）。"""
    fake.logs = [row for row in fake.logs if row["log.level"] != "ERROR"]


def collect(fake, inc=None, *, logs="vl", traces="", metrics=rca.MetricOptions(), edges=EDGES, **kw):
    grafana = Grafana("http://grafana.test", "t", transport=fake.transport, retries=0)
    return collect_evidence(inc or incident(), edges, grafana, logs_datasource_uid=logs, id_fields=["eqp_id", "lot_id", "carrier_id"],
                                now=NOW, traces_datasource_uid=traces, metrics=metrics, **kw)


COUNT = "count() as total, count() if"   # 錯誤日誌 0 筆之後多問的那一次


def line(ev, **kw):
    return rca.overview_text(rca.overview(ev, **kw))


# ── 有什麼 ──

def test_everything_present_is_listed_with_counts(fake):
    trace = shifted_trace(recorded_trace(), SHIFT)
    trace["processes"]["p1"]["serviceName"] = "cim-eap"      # 錄下來的 Trace 是 web-portal → mes-api；讓其中一段屬於這張事故的系統
    fake.traces = {trace["traceID"]: trace}
    ev = collect(fake, traces="vt-uid", metrics=rca.MetricOptions(datasource_uid="prom-uid", queries=(METRIC,)))
    got = rca.overview(ev)
    assert got["missing"] == [] and got["off"] == [] and got["failed"] == []
    text = "、".join(got["have"])
    assert text.startswith("告警 3 筆、這些系統之間的關係邊 2 條、錯誤日誌 ")
    for part in ("識別碼", "跨系統時間線", "失敗請求的呼叫鏈 1 條", "指標走勢 1 條"):
        assert part in text
    assert rca.overview_text(got) == "已有的證據｜" + text   # 什麼都不缺：只有一行


# ── 錯誤日誌 0 筆：三種原因分得出來，各有下一步 ──

@pytest.mark.parametrize("dataset, says, next_step", [
    ("count-empty", "這段時間一筆日誌都沒有", "logs.extra_filter"),
    ("count-mismatch", "這段時間有 4 筆日誌，但沒有一筆的 service.name 是這些系統", "值是否跟告警的系統名稱完全一樣"),
    ("count-no-errors", "這些系統有 5 筆日誌，但 log.level 沒有一筆是 ERROR／FATAL／CRITICAL", "等級的寫法對不上"),
])
def test_zero_error_logs_says_which_step_failed(fake, dataset, says, next_step):
    fake.logs = shifted(recorded_rows(dataset), SHIFT)
    ev = collect(fake)
    text = line(ev)
    [missing] = [l for l in text.splitlines() if l.startswith("缺錯誤日誌｜還看不出哪個系統出了什麼錯、哪些錯誤是這次才出現的。")]   # 自己一行，先講少了它哪種分析做不了
    assert says in missing and next_step in missing
    [sent] = fake.queries(COUNT)
    # 送出去的計數查詢跟錄製時同一個寫法（錄製時前面多一段只看某個 dataset 的條件，系統名稱的順序不同）
    was = victoria("logs")["exchanges"][f"count_logs_{dataset}"][0]["request"]["form"]["query"]
    assert was.replace('in("mq-broker","cim-eap","amhs-mcs")', 'in("amhs-mcs","cim-eap","mq-broker")') == f'{{dataset="{dataset}"}} {sent}'


def test_the_extra_count_query_only_happens_when_there_are_no_error_logs(fake):
    collect(fake)
    assert fake.queries(COUNT) == []


def test_no_level_field_and_custom_levels_are_named_in_the_reason(fake):
    no_error_logs(fake)
    assert "severity 沒有一筆是 E／SEVERE" in line(collect(fake), log_fields=LogFields(level="severity"), error_levels=("E", "SEVERE"))
    k8s = LogFields(service="kubernetes.container_name", level="")
    fake.logs = [row for row in shifted(recorded_rows("unparsed"), SHIFT) if "track-in" in row["_msg"]]   # 沒拆欄位、訊息裡也沒有錯誤字樣
    assert "這些系統有 1 筆日誌，但訊息文字裡沒有錯誤字樣" in line(collect(fake, log_fields=k8s), log_fields=k8s)


def test_if_the_count_itself_fails_the_line_still_says_logs_are_missing(fake):
    no_error_logs(fake)
    fake.fail_queries = {COUNT}
    ev = collect(fake)
    assert "缺錯誤日誌｜還看不出哪個系統出了什麼錯、哪些錯誤是這次才出現的。沒有查到這些系統的錯誤日誌" in line(ev) and "log_counts" not in ev


# ── 沒接、查詢失敗 ──

def test_sources_that_are_not_configured_are_listed_as_not_connected(fake):
    text = line(collect(fake, logs=""))
    assert [l.split("｜")[0] for l in text.splitlines()[1:]] == ["沒接日誌", "沒接呼叫鏈", "沒接指標走勢"]
    assert "沒接日誌｜還看不出哪個系統出了什麼錯、哪些錯誤是這次才出現的。要接就設定 logs.datasource_uid" in text
    assert "查詢失敗" not in text


def test_failed_queries_are_reported_as_failed_not_as_missing(fake):
    fake.fail.add("/api/datasources/proxy/uid/")
    ev = collect(fake, traces="vt-uid", metrics=rca.MetricOptions(datasource_uid="prom-uid", queries=(METRIC,)))
    got = rca.overview(ev)
    assert [name for name, _why in got["failed"]] == ["日誌", "呼叫鏈", "指標走勢"] and got["off"] == []


def test_queried_but_empty_traces_and_metrics_say_why(fake):
    fake.traces = {}
    ev = collect(fake, incident("amhs-mcs"), traces="vt-uid",
                 metrics=rca.MetricOptions(datasource_uid="prom-uid", queries=(METRIC,)))
    text = line(ev)
    assert "缺呼叫鏈｜還看不出失敗的請求實際停在哪個系統（包含沒有告警的相依服務）。VictoriaTraces 裡沒有這些系統的失敗請求" in text
    assert "缺指標走勢｜還看不出數值是怎麼變壞的（流量、佇列、資源）。設定的查詢在這段時間都沒有資料" in text


# ── 關係邊 ──

def test_edges_are_judged_against_the_systems_in_this_incident(fake):
    assert "這些系統之間的關係邊 2 條" in line(collect(fake))
    assert "關係邊" not in line(collect(fake, edges=[("x", "y")]))   # 這幾個系統之間沒宣告：不算缺，不提（關係邊是加分的）
    assert "連到這個系統的關係邊 1 條（另一端沒有告警）" in line(collect(fake, incident("mq-broker")))
    alone = line(collect(fake, incident("mq-broker"), edges=[]))
    assert "關係邊" not in alone   # 單一系統、沒宣告關係邊：不算缺（系統自己出問題時不需要它）


# ── 註記與整輪 ──

def test_annotation_shows_the_evidence_line_between_summary_and_time_and_escapes_it():
    inc = incident()
    inc.summary = "[LLM 推論] 還無法判定源頭"
    inc.evidence_note = "已有的證據｜告警 3 筆\n缺錯誤日誌｜<b>x</b>  多個空白\n\n沒接呼叫鏈｜要接就設定 traces.datasource_uid"
    l1, l2, have, missing, off, last = report.render(inc, "http://g", "Asia/Taipei")["text"].split("<br>")
    assert (have, off) == ("已有的證據｜告警 3 筆", "沒接呼叫鏈｜要接就設定 traces.datasource_uid")
    assert missing == "缺錯誤日誌｜&lt;b&gt;x&lt;/b&gt; 多個空白"   # 跳脫；每項自己一行
    assert l2 == "根因分析｜[LLM 推論] 還無法判定源頭" and "台灣時間" in last
    inc.evidence_note = ""
    assert report.render(inc, "http://g", "Asia/Taipei")["text"].count("<br>") == 2   # 還沒收集過證據：沒有這一行


def test_a_round_writes_the_reason_into_the_annotation_and_tells_the_llm(world):  # noqa: F811
    """使用者實際遇到的情況：告警進來了、日誌的欄位對不上。註記要直接說出是哪一關沒過，而不是只有「證據不足」。"""
    w = world
    w.fake.logs = shifted(recorded_rows("count-mismatch"), SHIFT)   # 有日誌，但 service.name 寫的是別的名字
    calls = []

    def llm(request):   # 沒有日誌的情境，模型不會（也不能）點名機台：world 預設的回答有 EQP-CVD-07，會被幻覺檢查擋掉
        calls.append(json.loads(request.content))
        answer = {"summary": "還無法判定源頭。只有告警與關係邊，日誌的系統名稱欄位對不上。", "systems": ["mq-broker"], "ids": []}
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(answer, ensure_ascii=False)}}]})

    w.receive("firing_mq", "firing_cim")
    assert w.run(60, llm=httpx.MockTransport(llm))["status"] == "ok"
    [inc] = rows(w, dbmod.incidents)
    assert inc["evidence_note"].startswith("證據｜已有告警 2 筆") and "\n" not in inc["evidence_note"]   # 註記上只有一行
    assert "缺錯誤日誌（這段時間有 4 筆日誌，但沒有一筆的 service.name 是這些系統" in inc["evidence_note"]
    note = w.fake.annotations[inc["annotation_id"]]["text"]
    assert "<br>根因分析｜[LLM 推論] 還無法判定源頭" in note and "<br>證據｜已有告警 2 筆" in note and "service.name 是這些系統" in note
    prompt = calls[0]["messages"][1]["content"]
    assert prompt.startswith("【0. 證據概況") and "service.name 是這些系統" in prompt.split("【1.")[0]
    assert "缺錯誤日誌｜還看不出哪個系統出了什麼錯、哪些錯誤是這次才出現的" in prompt.split("【1.")[0]   # 完整的說明（少了它哪種分析做不了）只給模型
    system = calls[0]["messages"][0]["content"]
    assert "不要只回「無法判定」" in system and "不能當成它就是源頭的依據" in system and "candidates" in system and "0 到 100 的整數" in system


def test_the_evidence_line_survives_a_restart_and_is_kept_when_only_the_status_changes(world):  # noqa: F811
    w = world
    w.receive("firing_mq")
    w.run(60)
    [before] = rows(w, dbmod.incidents)
    w.receive("resolved_mq")
    w.run(120)   # 只是恢復：不重新分析，證據那一行照舊
    [after] = rows(w, dbmod.incidents)
    assert after["evidence_note"] == before["evidence_note"] != "" and len(w.llm_calls) == 1
    assert "證據｜已有" in w.fake.annotations[after["annotation_id"]]["text"]


def test_without_llm_the_evidence_line_is_still_written(world):  # noqa: F811
    w = world
    w.receive("firing_mq")
    w.run(60, settings=replace(w.settings, llm_model=""))
    [inc] = rows(w, dbmod.incidents)
    assert inc["summary"].startswith("[未經分析]") and inc["evidence_note"].startswith("證據｜已有告警 1 筆")


def test_source_field_is_optional_but_must_be_a_system_in_the_evidence(fake):
    ev = collect(fake)
    good = {"summary": "mq-broker 最可能是源頭", "source": "mq-broker", "systems": ["mq-broker"], "ids": []}
    assert rca.check_answer(json.dumps(good), ev).startswith("mq-broker")
    assert rca.check_answer(json.dumps({**good, "source": ""}), ev)                      # 還無法判定
    assert rca.check_answer(json.dumps({k: v for k, v in good.items() if k != "source"}), ev)   # 自訂提示詞沒要求這一欄
    with pytest.raises(ValueError, match="erp-core"):
        rca.check_answer(json.dumps({**good, "source": "erp-core"}), ev)


def test_the_annotation_gets_one_brief_line_while_the_llm_gets_the_full_explanation(fake):
    fake.logs = shifted(recorded_rows("count-mismatch"), SHIFT)
    groups = rca.overview(collect(fake, traces="vt-uid"))
    brief = rca.overview_brief(groups)
    assert "\n" not in brief and brief.startswith("證據｜已有告警 3 筆")
    assert "缺錯誤日誌（這段時間有 4 筆日誌" in brief and "還看不出" not in brief          # 留原因，不重複講能分析什麼
    assert brief.endswith("沒接指標走勢") and "metrics.queries" not in brief          # 沒接的只列名稱
    assert "還看不出" in rca.overview_text(groups) and "metrics.queries" in rca.overview_text(groups)


def test_logs_without_equipment_or_lot_fields_are_not_reported_as_missing(fake):
    """IT 服務的日誌本來就沒有機台、批號欄位：有多少證據就分析多少，不每次都唸一遍。"""
    fake.logs = [{k: v for k, v in row.items() if k not in ("eqp_id", "lot_id", "carrier_id")} for row in fake.logs]
    text = line(collect(fake))
    assert "機台" not in text and "批號" not in text and "eqp_id" not in text and text.startswith("已有的證據｜告警 3 筆")
    assert "錯誤日誌 " in text
