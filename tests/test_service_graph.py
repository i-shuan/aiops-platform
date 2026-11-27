"""系統之間的關係全部由程式從資料學，沒有手寫的 edges（src/graph.py）。三個來源：Tempo 的 service graph、事故的呼叫鏈、錯誤日誌。
用途：合併事故（兩個系統都在告警、而且這段時間它們之間有失敗的呼叫或日誌指名對方才併）與給 LLM 當線索。

service graph 的樣本是 Tempo 2.9.0 的 metrics-generator 寫進 VictoriaMetrics、經 Grafana 13.0.7 查回來的（service_graph.json）；
Trace 的樣本是 VictoriaTraces v0.5.0 的（web-portal → mes-api → mes-db）。"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import db as dbmod
from src import graph, rca
from src.grafana import Grafana, GrafanaError, parse_webhook
from src.main import ConfigError, Settings
from tests.conftest import SHIFT, log_row, recorded_service_graph, recorded_trace, shifted_trace, webhook
from tests.test_main import T0, rows, world  # noqa: F401（fixture）

MINIMAL = {"grafana": {"url": "http://g", "token": "t"}, "webhook": {"token": "w"}}
NOW = datetime(2026, 11, 27, 9, 10, tzinfo=timezone.utc)


def alert_for(w, service, fingerprint, name="firing_mq"):
    """樣本裡的告警換成別的系統（規則的 service 標籤不同）。"""
    payload = webhook(name)
    payload["alerts"][0]["labels"].update(service=service, alertname=f"{service} errors")
    payload["alerts"][0]["fingerprint"] = fingerprint
    database = dbmod.Database(w.settings.database_url)
    database.enqueue(parse_webhook(payload)[0], T0)
    database.close()


def learned(w):
    return sorted((r["caller"], r["callee"], r["source"]) for r in rows(w, dbmod.calls))


def incidents_of(w):
    return sorted(json.loads(i["services"]) for i in rows(w, dbmod.incidents))


def replaying(name, seen=None):
    queue = list(recorded_service_graph(name))

    def handler(request):
        if seen is not None:
            seen.append(request)
        reply = queue.pop(0)
        body = reply["response"]
        return httpx.Response(reply["status"], json=body) if isinstance(body, dict) else httpx.Response(reply["status"], text=body)
    return Grafana("http://grafana.test", "t", transport=httpx.MockTransport(handler), retries=0)


# ── 讀 Tempo 的 service graph ──

def test_service_graph_is_read_from_the_recorded_tempo_metrics():
    seen = []
    edges = graph.service_graph(replaying("window_with_traffic", seen), "vm-uid", NOW - timedelta(minutes=15), NOW)
    assert edges == [{"caller": "mes-api", "callee": "mes-db", "requests": 9, "failed": 6, "kind": "database"},        # 沒埋點的資料庫也在
                     {"caller": "mes-api", "callee": "report-worker", "requests": 9, "failed": 0, "kind": "messaging"},   # 經過訊息佇列的（Tempo 標 messaging_system）
                     {"caller": "web-portal", "callee": "mes-api", "requests": 9, "failed": 6, "kind": ""}]
    assert not any(e["caller"] == "user" for e in edges)                 # Tempo 把入口請求畫成 client="user"：不是系統，不記
    assert [r.url.path for r in seen] == ["/api/datasources/proxy/uid/vm-uid/api/v1/query"] * 2
    assert [r.url.params["query"] for r in seen] == [x["request"]["params"]["query"] for x in recorded_service_graph("window_with_traffic")]
    assert "increase(traces_service_graph_request_failed_total[15m])" in seen[1].url.params["query"]


def test_no_traffic_is_an_empty_graph_and_a_failing_query_does_not_stop_the_round():
    assert graph.service_graph(replaying("window_before_any_traffic"), "vm-uid", NOW - timedelta(hours=5), NOW - timedelta(hours=4)) == []
    bad = recorded_service_graph("bad_query")[0]
    assert bad["status"] == 422
    broken = Grafana("http://grafana.test", "t", transport=httpx.MockTransport(lambda r: httpx.Response(422, json=bad["response"])), retries=0)
    with pytest.raises(GrafanaError, match="HTTP 422"):
        graph.service_graph(broken, "vm-uid", NOW - timedelta(minutes=5), NOW)
    window = {"start": NOW - timedelta(minutes=5), "end": NOW}
    read = lambda **kw: graph.read_service_graph(broken, **{"traces_type": "tempo", "traces_uid": "t", "metrics_uid": "", **kw}, **window)
    assert read(metrics_uid="vm-uid") == []                         # 查不動：這一輪當成沒有這個來源
    assert read() == []                                             # Tempo 沒設存指標的資料源：不查
    assert read(traces_type="victoriatraces") == []                 # VictoriaTraces 查不動：一樣當成沒有
    assert read(traces_type="victoriatraces", traces_uid="") == []  # 沒接 Trace：不查


def test_victoriatraces_dependencies_give_who_calls_whom_without_numbers():
    seen = []
    edges = graph.dependencies(replaying("victoriatraces_dependencies", seen), "vt-uid", NOW - timedelta(minutes=15), NOW)
    assert edges == [{"caller": "mes-api", "callee": "report-worker", "requests": None, "failed": None, "kind": ""},
                     {"caller": "web-portal", "callee": "mes-api", "requests": None, "failed": None, "kind": ""}]
    real = recorded_service_graph("victoriatraces_dependencies")[0]["response"]["data"]
    assert all(d["callCount"] > 9 for d in real)       # 錄製時只送了十幾條 Trace，它回報上百次：次數會重複計算，所以不用
    assert not any("mes-db" in (d["parent"], d["child"]) for d in real)   # 沒埋點的資料庫不在上面（Tempo 的有）
    assert seen[0].url.path == "/api/datasources/proxy/uid/vt-uid/api/dependencies" and seen[0].url.params["lookback"] == "900000"
    assert graph.dependencies(replaying("victoriatraces_dependencies_before_any_traffic"), "vt-uid", NOW - timedelta(days=3), NOW - timedelta(days=2)) == []
    # 沒有失敗數：光靠它不能合併，只能當線索
    assert graph.failing_links({"mes-api", "web-portal"}, graph_edges=edges, trace_calls=[], log_mentions=[]) == []


def test_with_victoriatraces_the_graph_is_learned_from_its_dependencies_and_merging_still_needs_a_failing_trace(world):  # noqa: F811
    w = world
    settings = replace(w.settings, service_graph_datasource_uid="", traces_datasource_uid="vt-uid")   # traces.type 預設是 victoriatraces
    w.fake.dependencies = [("web-portal", "mes-api"), ("mes-api", "report-worker")]
    w.fake.traces = {}                                                 # 這段時間沒有失敗的 Trace
    alert_for(w, "web-portal", "fp-portal")
    alert_for(w, "mes-api", "fp-api")
    w.run(64, settings=settings)
    assert incidents_of(w) == [["mes-api"], ["web-portal"]]           # 有呼叫關係、但沒看到失敗：不併
    assert learned(w) == [("mes-api", "report-worker", "service_graph"), ("web-portal", "mes-api", "service_graph")]
    assert len(w.fake.dependency_requests) == 1 and w.fake.service_graph_requests == []
    prompt = w.llm_calls[0]["messages"][1]["content"]
    section = prompt.split("【2. 系統上下游關係")[1].split("【3.")[0]
    relations = json.loads(section[section.index("["):])
    assert all(r["source"] == "service_graph" and "requests" not in r and "failed" not in r for r in relations)   # 沒有數字就不給


# ── 從錯誤日誌學 ──

def test_log_mentions_match_whole_known_names_only():
    logs = [{"system": "instana-agent", "message": "Cannot connect to MariaDb at db-host:3306"},
            {"system": "cim-eap", "message": "publish to MQ-BROKER timed out; mes-api-v2 unaffected; mq-broker.internal ok"},
            {"system": "mq-broker", "message": "mq-broker disk full"}]
    names = {"mariadb", "mq-broker", "mes-api", "mes", "db", "instana-agent"}
    assert graph.mentions(logs, names) == [("instana-agent", "mariadb"), ("cim-eap", "mq-broker")]
    # 沒中的：mes-api（後面接 -v2 是別的名稱）、mes（只是別的名稱的一部分）、db（太短）、mq-broker 提到自己
    assert graph.mentions(logs, set()) == []


def test_only_calls_that_are_failing_now_between_two_alerting_systems_can_merge():
    edges = [{"caller": "a", "callee": "hub", "requests": 9, "failed": 3, "kind": ""}, {"caller": "b", "callee": "hub", "requests": 9, "failed": 0, "kind": ""},
             {"caller": "c", "callee": "quiet", "requests": 9, "failed": 9, "kind": ""}]
    links = graph.failing_links({"a", "b", "hub", "c"}, graph_edges=edges, trace_calls=[("b", "a"), ("x", "y")], log_mentions=[("hub", "a")])
    assert links == [("a", "hub"), ("a", "b")]   # b → hub 沒有失敗：不併；c → quiet 有失敗但 quiet 沒告警：不併；方向不分，重複的只留一條


# ── 合併事故 ──

def test_systems_merge_when_the_service_graph_shows_failures_between_them(world):  # noqa: F811
    w = world                                        # world 的 service graph：cim-eap → mq-broker、amhs-mcs → cim-eap，都有失敗
    w.receive("firing_mq", "firing_cim", "firing_amhs")
    w.run(64)
    assert incidents_of(w) == [["amhs-mcs", "cim-eap", "mq-broker"]]
    assert learned(w) == [("amhs-mcs", "cim-eap", "service_graph"), ("cim-eap", "mq-broker", "service_graph")]
    # 合併只查 2 次（請求數、失敗數）；分析時再讀一次帶延遲與基線的版本：同樣 2 次，加延遲與基線 10 次
    assert len(w.fake.service_graph_requests) == 2 + 12 and w.fake.trace_requests == []   # 已經連成一塊：不用再查 Trace
    assert w.fake.error_searches == 1                                               # 日誌只有收集證據的那一次
    w.run(124)
    assert len(w.fake.service_graph_requests) == 2 + 12                             # 沒有新告警的續跑：什麼都不查


def test_a_call_path_without_failures_does_not_merge_and_a_shared_hub_only_pulls_in_its_failing_callers(world):  # noqa: F811
    w = world
    w.fake.service_graph = [("cim-eap", "mq-broker", 50, 0), ("amhs-mcs", "mq-broker", 50, 7)]   # 兩個都連 mq-broker，只有 amhs-mcs 的呼叫在失敗
    w.receive("firing_mq", "firing_cim", "firing_amhs")
    w.run(64)
    assert incidents_of(w) == [["amhs-mcs", "mq-broker"], ["cim-eap"]]
    [alone] = [i for i in rows(w, dbmod.incidents) if json.loads(i["services"]) == ["cim-eap"]]
    assert "關係邊" in alone["evidence_note"]            # 有呼叫路徑還是會告訴 LLM，只是不拿來合併


def test_without_a_service_graph_failing_traces_between_alerting_systems_merge_them(world):  # noqa: F811
    w = world
    settings = replace(w.settings, service_graph_datasource_uid="", traces_datasource_uid="vt-uid")
    alert_for(w, "web-portal", "fp-portal")
    alert_for(w, "mes-api", "fp-api")
    w.run(64, settings=settings)
    assert incidents_of(w) == [["mes-api", "web-portal"]]             # 失敗的 Trace 裡 web-portal 呼叫 mes-api
    assert ("web-portal", "mes-api", "trace") in learned(w) and ("mes-api", "mes-db", "trace") in learned(w)
    assert w.fake.service_graph_requests == []


def test_a_successful_call_inside_a_failed_trace_does_not_merge(world):  # noqa: F811
    """真的 Tempo 實測時遇到的：請求失敗了，但途中送給 report-worker 的訊息有送到。report-worker 剛好也在告警（別的原因），
    不能只因為它出現在那條失敗的 Trace 裡就被併進來。"""
    w = world
    trace = shifted_trace(recorded_trace(), SHIFT)
    ok = dict(next(sp for sp in recorded_trace("get_trace_ok")["spans"] if sp["operationName"] == "POST /api/track-in"))   # 一段成功的 span
    api_server = next(sp for sp in trace["spans"] if sp["operationName"] == "POST /api/track-in")
    consume = {**ok, "traceID": trace["traceID"], "spanID": "e1" * 8, "operationName": "consume lot.events", "processID": "p9",
               "startTime": api_server["startTime"] + 1000,
               "references": [{"refType": "CHILD_OF", "spanID": api_server["spanID"], "traceID": trace["traceID"]}]}
    trace = {**trace, "spans": [*trace["spans"], consume], "processes": {**trace["processes"], "p9": {"serviceName": "report-worker", "tags": []}}}
    w.fake.traces = {trace["traceID"]: trace}
    settings = replace(w.settings, service_graph_datasource_uid="", traces_datasource_uid="vt-uid")
    for n, service in enumerate(("web-portal", "mes-api", "report-worker")):
        alert_for(w, service, f"fp-{n}")
    w.run(64, settings=settings)
    assert incidents_of(w) == [["mes-api", "web-portal"], ["report-worker"]]
    assert ("mes-api", "report-worker", "trace") in learned(w)        # 這條呼叫還是學起來了，只是不拿來合併


def test_an_error_log_that_names_the_other_alerting_system_merges_them(world):  # noqa: F811
    w = world
    settings = replace(w.settings, service_graph_datasource_uid="")
    w.fake.logs.append(log_row(T0 - timedelta(seconds=40), "cim-eap", "ERROR", "publish failed: mq-broker refused connection"))
    w.receive("firing_mq", "firing_cim", "firing_amhs")
    w.run(64, settings=settings)
    assert incidents_of(w) == [["amhs-mcs"], ["cim-eap", "mq-broker"]]   # amhs-mcs 的日誌沒有提到別人：自己一張
    assert ("cim-eap", "mq-broker", "log") in learned(w)


def test_nothing_observed_means_every_system_gets_its_own_incident(world):  # noqa: F811
    w = world
    w.receive("firing_mq", "firing_cim")
    w.run(64, settings=replace(w.settings, service_graph_datasource_uid=""))
    assert incidents_of(w) == [["cim-eap"], ["mq-broker"]] and learned(w) == []


def test_turning_the_graph_off_stops_learning_and_merging(world):  # noqa: F811
    w = world
    w.receive("firing_mq", "firing_cim")
    w.run(64, settings=replace(w.settings, graph_retention_days=0))
    assert incidents_of(w) == [["cim-eap"], ["mq-broker"]] and learned(w) == [] and w.fake.service_graph_requests == []


# ── 給 LLM 當線索 ──

def test_calls_are_read_from_a_call_chain_at_system_level():
    chain = rca.summarize_trace(recorded_trace(), "t", rca.EvidenceLimits())
    assert rca.calls_in({"call_chains": [chain]}) == [("web-portal", "mes-api"), ("mes-api", "mes-db")]   # 同系統內部的呼叫不算；peer.service 也算
    assert rca.calls_in({"call_chains": [chain]}, failed_only=True) == [("web-portal", "mes-api"), ("mes-api", "mes-db")]   # 這條 Trace 每一段都失敗
    ok = rca.summarize_trace(recorded_trace("get_trace_ok"), "t", rca.EvidenceLimits())
    assert rca.calls_in({"call_chains": [ok]}) and rca.calls_in({"call_chains": [ok]}, failed_only=True) == []
    assert rca.calls_in({"call_chains": []}) == [] and rca.calls_in({}) == []


def test_relations_reach_the_llm_with_where_they_came_from(world):  # noqa: F811
    w = world
    w.receive("firing_mq", "firing_cim")
    w.run(64)
    prompt = w.llm_calls[0]["messages"][1]["content"]
    section = prompt.split("【2. 系統上下游關係")[1].split("【3.")[0]
    relations = json.loads(section[section.index("["):])
    [mq] = [r for r in relations if r["upstream"] == "mq-broker"]
    assert {"downstream": "cim-eap", "source": "service_graph", "requests": 6, "failed": 4}.items() <= mq.items()
    assert "source 是" not in section and "service_graph、trace＝downstream 實際呼叫過 upstream" in section
    assert any(r["downstream"] == "amhs-mcs" for r in relations)       # 相鄰但沒告警的系統也在（它呼叫 cim-eap）
    [inc] = rows(w, dbmod.incidents)
    assert "這些系統之間的關係邊 1 條" in inc["evidence_note"]


def test_a_later_incident_gets_what_an_earlier_one_taught_even_without_any_trace(world):  # noqa: F811
    w = world
    settings = replace(w.settings, service_graph_datasource_uid="", traces_datasource_uid="vt-uid")
    alert_for(w, "web-portal", "fp-portal")
    w.run(64, settings=settings)
    w.fake.traces = {}                                                 # 之後另一張事故：這次撈不到任何 Trace
    alert_for(w, "mes-api", "fp-api")
    w.run(64 + 600, settings=settings)
    prompt = w.llm_calls[-1]["messages"][1]["content"]
    section = prompt.split("【2. 系統上下游關係")[1].split("【3.")[0]
    assert sorted((r["downstream"], r["upstream"], r["source"]) for r in json.loads(section[section.index("["):])) == [
        ("mes-api", "mes-db", "trace"), ("web-portal", "mes-api", "trace")]


def test_a_system_known_only_from_a_relation_may_be_named_but_only_as_a_guess():
    ev = {"anomalies": [{"system": "mes-api", "rule": "errors", "severity": "P2", "started": "x", "status": "firing", "values": {}}],
          "edges": [rca.relation({"caller": "mes-api", "callee": "mes-db", "source": "service_graph", "last_seen": "x", "times": 3})],
          "error_logs": [], "error_patterns": [], "warnings": [], "id_counts": {}, "timelines": [], "call_chains": [], "metrics": [], "notes": []}
    assert ev["edges"] == [{"upstream": "mes-db", "downstream": "mes-api", "source": "service_graph", "last_seen": "x", "times": 3}]
    answer = json.dumps({"summary": "目前只能推測 mes-db 有狀況", "systems": [], "ids": [], "candidates": [
        {"source": "mes-db", "kind": "evidence", "confidence": 90, "reason": "mes-api 會呼叫 mes-db"}]}, ensure_ascii=False)
    _s, [candidate] = rca.read_answer(answer, ev)
    assert (candidate["source"], candidate["confidence"]) == ("mes-db", rca.NO_DIRECT_EVIDENCE_CAP)   # 有呼叫路徑不是這次出事的證據


def test_relations_not_seen_for_long_expire(world):  # noqa: F811
    w = world
    w.receive("firing_mq", "firing_cim")
    w.run(64)
    database = dbmod.Database(w.settings.database_url)
    later = T0 + timedelta(days=31)
    assert database.calls_touching(["cim-eap"], later, 30, 20) == [] and len(database.calls_touching(["cim-eap"], later, 60, 20)) == 2
    assert {"mq-broker", "cim-eap", "amhs-mcs"} <= database.known_systems()
    database.record_calls([("a", "b")], "trace", later, 30)           # 下次有東西要記的時候順便清掉過期的
    database.close()
    assert learned(w) == [("a", "b", "trace")]


# ── 設定 ──

def test_edges_can_no_longer_be_written_by_hand_and_the_graph_settings_are_checked():
    with pytest.raises(ConfigError, match="edges 已經移除"):
        Settings.from_dict({**MINIMAL, "edges": [["mq-broker", "cim-eap"]]})
    d = Settings.from_dict(MINIMAL)
    assert (d.graph_retention_days, d.service_graph_datasource_uid) == (30, "") and not hasattr(d, "edges")
    s = Settings.from_dict({**MINIMAL, "traces": {"type": "tempo", "service_graph_datasource_uid": "vm-1", "graph_retention_days": 0}})
    assert (s.service_graph_datasource_uid, s.graph_retention_days) == ("vm-1", 0)
    with pytest.raises(ConfigError, match="只有 traces.type 是 tempo"):     # VictoriaTraces 的從同一個資料源讀，不用另外設
        Settings.from_dict({**MINIMAL, "traces": {"service_graph_datasource_uid": "vm-1"}})
    for bad in (-1, 400, "30"):
        with pytest.raises(ConfigError, match="traces.graph_retention_days"):
            Settings.from_dict({**MINIMAL, "traces": {"graph_retention_days": bad}})
