"""訊息佇列的「連結」接法：發送端一條 Trace，每個消費端各自開一條新的 Trace，只用連結指回發送的那一段（常說的「四個 trace id」）。
連結寫明了對方的 trace id 與 span id，所以照著接就好，不靠時間去猜。

樣本（trace_links.json）：eap 發一筆訊息，service-a／b／c 各開一條 Trace 處理，service-b 失敗。同樣四條分別送進
VictoriaTraces v0.5.0 與 Tempo 2.9.0，經 Grafana 13.0.7 取回來的。"""
from __future__ import annotations

import json
from dataclasses import replace

import httpx
import pytest

from src import evidence as gather
from src import db as dbmod
from src import rca
from src.grafana import Grafana, parse_webhook
from src.traces import Tempo, VictoriaTraces, _hex
from tests.conftest import FIXTURES, webhook
from tests.test_main import T0, rows, world  # noqa: F401（fixture）

LINKS = json.loads((FIXTURES / "trace_links.json").read_text(encoding="utf-8"))


def store(backend):
    """照編號回錄下來的真實回應的 Trace 來源（VictoriaTraces 或 Tempo）。"""
    by_id = {LINKS["trace_ids"][backend][which]: LINKS["exchanges"][f"{backend}_{which}"][0] for which in ("service-b", "eap")}

    def handler(request: httpx.Request) -> httpx.Response:
        reply = by_id.get(request.url.path.rsplit("/", 1)[1])
        return httpx.Response(reply["status"], json=reply["response"]) if reply else httpx.Response(404)
    grafana = Grafana("http://grafana.test", "t", transport=httpx.MockTransport(handler), retries=0)
    return (VictoriaTraces if backend == "vt" else Tempo)(grafana, "uid")


@pytest.mark.parametrize("backend", ["vt", "tempo"])
def test_a_consumer_trace_is_joined_to_the_service_that_sent_the_message(backend):
    s, ids = store(backend), LINKS["trace_ids"][backend]
    consumer = s.get_trace(ids["service-b"])
    assert gather.linked_spans(consumer) == [(ids["eap"], "a2" * 8)]          # 連結指到發送端那條 Trace 的「publish」那一段
    senders = gather.senders_of(consumer, s)
    assert senders == {(ids["eap"], "a2" * 8): "eap"}                      # 把那一條取回來看是誰送的
    [row] = gather.summarize_trace(consumer, ids["service-b"], gather.EvidenceLimits(), senders)["spans"]
    assert (row["service"], row["called_by"], row["via"], row["status"], row["error"]) == ("service-b", "eap", "messaging", "error", "handler crashed")
    chain = {"call_chains": [gather.summarize_trace(consumer, ids["service-b"], gather.EvidenceLimits(), senders)]}
    assert gather.calls_in(chain) == gather.calls_in(chain, messaging_only=True) == gather.calls_in(chain, failed_only=True) == [("eap", "service-b")]
    # 沒有把對方那一條取回來（過期、被取樣掉）：不知道是誰送的，就不接，不猜
    [alone] = gather.summarize_trace(consumer, ids["service-b"], gather.EvidenceLimits())["spans"]
    assert alone["called_by"] is None and gather.calls_in({"call_chains": [{"spans": [alone]}]}) == []


def test_both_sources_give_the_same_chain_for_the_same_messages():
    chains = []
    for backend in ("vt", "tempo"):
        s, ids = store(backend), LINKS["trace_ids"][backend]
        consumer = s.get_trace(ids["service-b"])
        chains.append(gather.summarize_trace(consumer, "t", gather.EvidenceLimits(), gather.senders_of(consumer, s))["spans"])
    assert chains[0] == chains[1]
    raw = LINKS["exchanges"]["tempo_service-b"][0]["response"]["batches"][0]["scopeSpans"][0]["spans"][0]
    assert raw["links"][0]["traceId"].endswith("==") and _hex(raw["links"][0]["traceId"]) == LINKS["trace_ids"]["tempo"]["eap"]   # Tempo 回的編號是 base64
    assert _hex("a2" * 8) == "a2" * 8 and _hex("") == "" and _hex("not base64!") == "not base64!"


def test_a_messaging_relation_points_the_other_way_for_the_llm():
    sync = gather.relation({"caller": "mes-api", "callee": "mes-db", "source": "trace", "kind": "", "last_seen": "x", "times": 1})
    queue = gather.relation({"caller": "eap", "callee": "service-b", "source": "trace", "kind": "messaging", "last_seen": "x", "times": 1})
    assert (sync["upstream"], sync["downstream"]) == ("mes-db", "mes-api") and "kind" not in sync   # 被呼叫的出問題會影響呼叫它的
    assert (queue["upstream"], queue["downstream"], queue["kind"]) == ("eap", "service-b", "messaging")   # 送訊息的出問題，收的才受影響
    assert "downstream 出問題通常不會回頭影響 upstream" in rca.build_prompt({
        "anomalies": [], "edges": [queue], "error_logs": [], "id_counts": {}, "notes": []})


def alert_for(w, service, fingerprint):
    payload = webhook("firing_mq")
    payload["alerts"][0]["labels"].update(service=service, alertname=f"{service} errors")
    payload["alerts"][0]["fingerprint"] = fingerprint
    database = dbmod.Database(w.settings.database_url)
    database.enqueue(parse_webhook(payload)[0], T0)
    database.close()


def test_a_round_merges_the_sender_and_the_failing_consumer_and_remembers_the_queue_relation(world):  # noqa: F811
    w = world
    ids = LINKS["trace_ids"]["vt"]
    at = int((T0.timestamp() - 30) * 1_000_000)                            # 樣本的時間移到這張事故的時間窗裡（只移時間）
    for which in ("service-b", "eap"):
        trace = LINKS["exchanges"][f"vt_{which}"][0]["response"]["data"][0]
        w.fake.traces[ids[which]] = {**trace, "spans": [{**sp, "startTime": at} for sp in trace["spans"]]}
    settings = replace(w.settings, service_graph_datasource_uid="", traces_datasource_uid="vt-uid")
    alert_for(w, "eap", "fp-eap")
    alert_for(w, "service-b", "fp-b")
    w.run(64, settings=settings)
    assert [json.loads(i["services"]) for i in rows(w, dbmod.incidents)] == [["eap", "service-b"]]   # 四個 trace id 也接得起來
    [learned] = [r for r in rows(w, dbmod.calls) if r["source"] == "trace"]
    assert (learned["caller"], learned["callee"], learned["kind"]) == ("eap", "service-b", "messaging")
    prompt = w.llm_calls[0]["messages"][1]["content"]
    assert '"called_by": "eap"' in prompt and '"via": "messaging"' in prompt
    w.fake.traces = {}                                                     # 之後的事故沒有 Trace：學到的關係照樣給 LLM，方向是 eap → service-b
    alert_for(w, "service-b", "fp-b2")
    w.run(64 + 900, settings=settings)
    section = w.llm_calls[-1]["messages"][1]["content"].split("【2. 系統上下游關係")[1].split("【3.")[0]
    [relation] = json.loads(section[section.index("["):])
    assert (relation["upstream"], relation["downstream"], relation["kind"]) == ("eap", "service-b", "messaging")
