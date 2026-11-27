"""動態的系統關係：從事故的呼叫鏈一次一次累積「誰呼叫誰」，之後的事故拿來給 LLM 當線索。
來源只有 Trace（真的發生過的呼叫）。它不拿來合併事故：合併只認 config.yaml 手寫的 edges。
Trace 的樣本是 VictoriaTraces v0.5.0 經 Grafana 實際回的（web-portal → mes-api → mes-db）。"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta

import pytest

from src import db as dbmod
from src import rca
from src.grafana import parse_webhook
from src.main import ConfigError, Settings
from tests.conftest import recorded_trace, webhook
from tests.test_main import T0, rows, world  # noqa: F401（fixture）

MINIMAL = {"grafana": {"url": "http://g", "token": "t"}, "webhook": {"token": "w"}}


def alert_for(w, service, fingerprint, name="firing_mq"):
    """樣本裡的告警換成別的系統（規則的 service 標籤不同）。"""
    payload = webhook(name)
    payload["alerts"][0]["labels"].update(service=service, alertname=f"{service} errors")
    payload["alerts"][0]["fingerprint"] = fingerprint
    database = dbmod.Database(w.settings.database_url)
    database.enqueue(parse_webhook(payload)[0], T0)
    database.close()


def graph(w):
    return sorted((r["caller"], r["callee"], r["times"]) for r in rows(w, dbmod.calls))


def with_traces(w, **kw):
    return replace(w.settings, traces_datasource_uid="vt-uid", **kw)


def test_calls_are_read_from_a_call_chain_at_system_level():
    chain = rca.summarize_trace(recorded_trace(), "t", rca.EvidenceLimits())
    assert rca.calls_in({"call_chains": [chain]}) == [("web-portal", "mes-api"), ("mes-api", "mes-db")]   # 同系統內部的呼叫不算；peer.service 也算
    assert rca.calls_in({"call_chains": []}) == [] and rca.calls_in({}) == []


def test_an_incident_with_traces_teaches_the_graph_and_a_later_one_gets_it_as_a_clue(world):  # noqa: F811
    w = world
    alert_for(w, "web-portal", "fp-portal")
    w.run(64, settings=with_traces(w))
    assert graph(w) == [("mes-api", "mes-db", 1), ("web-portal", "mes-api", 1)]
    first_prompt = w.llm_calls[0]["messages"][1]["content"]
    assert "【6. 呼叫鏈" in first_prompt and "【8. 以前的事故" not in first_prompt     # 這次的呼叫鏈已經在證據裡：不重複列

    w.fake.traces = {}                                                # 之後另一張事故：這次沒有撈到任何 Trace
    alert_for(w, "mes-api", "fp-api")
    w.run(64 + 600, settings=with_traces(w))
    prompt = w.llm_calls[-1]["messages"][1]["content"]
    section = prompt.split("【8. 以前的事故裡實際看到的呼叫關係")[1].split("【")[0]
    learned = json.loads(section[section.index("["):])
    assert [(c["caller"], c["callee"], c["times"]) for c in learned] == [("mes-api", "mes-db", 1), ("web-portal", "mes-api", 1)]
    assert "不代表這次的問題經過它" in prompt
    [api] = [i for i in rows(w, dbmod.incidents) if json.loads(i["services"]) == ["mes-api"]]
    assert "以前事故看過的呼叫關係 2 條" in api["evidence_note"]
    assert graph(w) == [("mes-api", "mes-db", 1), ("web-portal", "mes-api", 1)]   # 這次沒看到新的呼叫：次數不變


def test_learned_calls_never_merge_incidents(world):  # noqa: F811
    """學到 web-portal → mes-api 之後，這兩個系統同時告警還是各開一張：合併只認手寫的 edges。"""
    w = world
    alert_for(w, "web-portal", "fp-portal")
    w.run(64, settings=with_traces(w))
    alert_for(w, "mes-api", "fp-api")
    w.run(124, settings=with_traces(w))
    assert sorted(json.loads(i["services"]) for i in rows(w, dbmod.incidents)) == [["mes-api"], ["web-portal"]]


def test_seeing_a_call_again_counts_it_and_calls_not_seen_for_long_expire(world):  # noqa: F811
    w = world
    alert_for(w, "web-portal", "fp-portal")
    w.run(64, settings=with_traces(w))
    alert_for(w, "mes-api", "fp-api")                                 # 另一張事故，又看到同樣的呼叫
    w.run(124, settings=with_traces(w))
    assert graph(w) == [("mes-api", "mes-db", 2), ("web-portal", "mes-api", 2)]
    database = dbmod.Database(w.settings.database_url)
    later = T0 + timedelta(days=31)
    assert database.calls_touching(["mes-api"], later, 30, 20) == []                 # 超過 30 天沒再看到：不拿出來
    assert len(database.calls_touching(["mes-api"], later, 60, 20)) == 2             # 保留期設長一點就還在
    assert [c["callee"] for c in database.calls_touching(["mes-db"], T0, 30, 20)] == ["mes-db"]   # 被呼叫的那一端也查得到
    database.record_calls([("a", "b")], later, 30)                                   # 下次有東西要記的時候順便清掉過期的
    database.close()
    assert graph(w) == [("a", "b", 1)]


def test_the_graph_can_be_turned_off(world):  # noqa: F811
    w = world
    alert_for(w, "web-portal", "fp-portal")
    w.run(64, settings=with_traces(w, graph_retention_days=0))
    assert graph(w) == [] and "【8." not in w.llm_calls[0]["messages"][1]["content"]


def test_a_system_known_only_from_the_graph_may_be_named_but_only_as_a_guess():
    ev = {"anomalies": [{"system": "mes-api", "rule": "errors", "severity": "P2", "started": "x", "status": "firing", "values": {}}],
          "edges": [], "error_logs": [], "error_patterns": [], "warnings": [], "id_counts": {}, "timelines": [], "call_chains": [],
          "metrics": [], "notes": [], "observed_calls": [{"caller": "mes-api", "callee": "mes-db", "last_seen": "x", "times": 3}]}
    answer = json.dumps({"summary": "目前只能推測 mes-db 有狀況", "systems": [], "ids": [], "candidates": [
        {"source": "mes-db", "kind": "evidence", "confidence": 90, "reason": "以前看過 mes-api 會呼叫 mes-db"}]}, ensure_ascii=False)
    _s, [candidate] = rca.read_answer(answer, ev)
    assert (candidate["source"], candidate["confidence"]) == ("mes-db", rca.NO_DIRECT_EVIDENCE_CAP)   # 有呼叫路徑不是這次出事的證據


def test_graph_retention_setting():
    assert Settings.from_dict(MINIMAL).graph_retention_days == 30
    assert Settings.from_dict({**MINIMAL, "traces": {"graph_retention_days": 0}}).graph_retention_days == 0
    for bad in (-1, 400, "30"):
        with pytest.raises(ConfigError, match="traces.graph_retention_days"):
            Settings.from_dict({**MINIMAL, "traces": {"graph_retention_days": bad}})
