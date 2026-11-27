"""日誌與 Trace 各有兩種來源（VictoriaLogs／Elasticsearch、VictoriaTraces／Tempo）：同一批資料，分析的程式要拿到同樣的東西。

Elasticsearch 的樣本（elasticsearch.json）是 record_elasticsearch.py 把跟 VictoriaLogs **同一批資料**寫進 Elasticsearch 8.19.23，
經 Grafana 13.0.7 的資料源代理（Editor Token）錄下來的；Tempo 的樣本（tempo_*.json）是 Tempo 2.9.0 錄的，送進去的三條 Trace
跟 VictoriaTraces 的是同一組。所以這裡可以直接拿兩邊的結果互相比。"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import db as dbmod
from src import rca
from src.grafana import Grafana, GrafanaError
from src.grouping import Alert, converge
from src.logs import Elasticsearch, LogFields, VictoriaLogs
from src.main import ConfigError, Settings, log_store, trace_store
from src.traces import Tempo, VictoriaTraces, _from_otlp, _traceql_regex
from tests.conftest import FIXTURES, FakeGrafana, recorded, recorded_rows, recorded_trace
from tests.test_main import rows, world  # noqa: F401（fixture）

BASE = datetime(2026, 11, 21, 0, 36, 38, tzinfo=timezone.utc)   # 樣本錄製的基準時間
HOUR = (BASE - timedelta(minutes=60), BASE + timedelta(minutes=1))
SERVICES = ["mq-broker", "cim-eap", "amhs-mcs"]
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")]
K8S = LogFields(service="kubernetes.container_name", level="")
ES = json.loads((FIXTURES / "elasticsearch.json").read_text(encoding="utf-8"))["exchanges"]
MINIMAL = {"grafana": {"url": "http://g", "token": "t"}, "webhook": {"token": "w"}}


def replaying(name: str, seen: list[str] | None = None) -> Grafana:
    """照順序回錄下來的真實回應；送出去的請求內容記在 seen。"""
    queue = list(ES[name])

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request.content.decode())
        reply = queue.pop(0)
        return httpx.Response(reply["status"], text=reply["response"], headers={"Content-Type": "application/json"})
    return Grafana("http://grafana.test", "t", transport=httpx.MockTransport(handler), retries=0)


def es(name: str, index: str = "rec-factory", fields: LogFields = LogFields(), seen: list[str] | None = None) -> Elasticsearch:
    return Elasticsearch(replaying(name, seen), "es-uid", fields, index)


def victoria(dataset: str = "factory", fields: LogFields = LogFields()) -> VictoriaLogs:
    fake = FakeGrafana(shift=timedelta(0))
    fake.logs = recorded_rows(dataset)
    return VictoriaLogs(Grafana("http://grafana.test", "t", transport=fake.transport), "vl", fields)


def essence(row: dict, fields: LogFields = LogFields()) -> tuple:
    """一筆日誌裡分析會用到的東西：時間、系統、等級、訊息、識別碼。兩種來源的小數秒寫法不同，比的時候轉成時間。"""
    return (rca.stamp(row["_time"]), row.get(fields.service), row.get(fields.level) if fields.level else None, row["_msg"],
            row.get("eqp_id"), row.get("lot_id"), row.get("carrier_id"))


# ── 日誌：Elasticsearch 回的跟 VictoriaLogs 是同一種形狀、同樣的內容 ──

def test_elasticsearch_rows_are_flattened_into_the_same_shape_as_victorialogs():
    seen: list[str] = []
    found = es("search_errors", seen=seen).search_errors(SERVICES, *HOUR, 50)
    assert seen == [ES["search_errors"][0]["request"]["body"]]            # 送出去的跟錄製時一模一樣
    header, query = (json.loads(line) for line in seen[0].splitlines())
    assert header == {"index": "rec-factory", "ignore_unavailable": True} and query["sort"] == [{"@timestamp": "desc"}]
    first = found[0]
    assert first["_time"] == "2026-11-21T00:30:18.000Z" and first["_msg"] == "consumer lag exceeded on queue lot.dispatch"
    assert first["service.name"] == "mq-broker" and first["log.level"] == "ERROR"   # 巢狀的 service: {name} 攤平成帶點的鍵
    assert not any(isinstance(value, dict) for row in found for value in row.values())
    assert [essence(r) for r in found] == [essence(r) for r in victoria().search_errors(SERVICES, *HOUR, 50)]


def test_every_log_query_gives_the_same_answer_from_both_sources():
    v = victoria()
    assert [essence(r) for r in es("search_errors_size_2").search_errors(SERVICES, *HOUR, 2)] == \
        [essence(r) for r in v.search_errors(SERVICES, *HOUR, 2)]
    window = (BASE - timedelta(seconds=450), BASE - timedelta(seconds=390))
    assert [essence(r) for r in es("search_errors_window").search_errors(SERVICES, *window, 50)] == \
        [essence(r) for r in v.search_errors(SERVICES, *window, 50)]
    kw = {"warn_levels": ("WARN", "WARNING"), "first_n": 2}
    a, b = es("error_profile").error_profile(SERVICES, *HOUR, **kw), v.error_profile(SERVICES, *HOUR, **kw)
    assert {s: [essence(r) for r in docs] for s, docs in a["services"].items()} == \
        {s: [essence(r) for r in docs] for s, docs in b["services"].items()} and a["warnings"] == b["warnings"] == []
    warned = es("error_profile_warnings", "rec-count-no-errors").error_profile(SERVICES, *HOUR, warn_levels=("WARN", "WARNING"))
    assert [essence(r) for r in warned["warnings"]] == [essence(r) for r in victoria("count-no-errors").error_profile(
        SERVICES, *HOUR, warn_levels=("WARN", "WARNING"))["warnings"]]
    items = [("cim-eap", ["timeout", "waiting", "reply"], ("ERROR",)), ("cim-eap", ["equipment", "disconnected"], ("ERROR",)),
             ("mq-broker", ["consumer", "lag", "exceeded", "on", "queue", "lot.dispatch"], ("ERROR",)),
             ("amhs-mcs", ["no", "such", "words"], ("ERROR",))]
    args = (BASE - timedelta(seconds=450), HOUR[1], HOUR[0])
    assert es("count_phrases").count_phrases(items, *args) == v.count_phrases(items, *args) == [(1, 2), (0, 1), (1, 0), (0, 0)]
    for dataset, counts in (("factory", (7, 7)), ("count-mismatch", (4, 0)), ("count-no-errors", (5, 5)), ("count-empty", (0, 0))):
        want = {"all": counts[0], "services": counts[1]}
        assert es(f"count_logs_{dataset}", f"rec-{dataset}").count_logs(SERVICES, *HOUR) == want
        if dataset != "count-empty":
            assert victoria(dataset).count_logs(SERVICES, *HOUR) == want
    lots = ["LOT24A0031", "LOT24A0060", "LOT-NOPE"]
    a, b = es("search_timeline").search_timeline("lot_id", lots, *HOUR, 2), v.search_timeline("lot_id", lots, *HOUR, 2)
    assert {k: ([essence(r) for r in x["hits"]], x["total"]) for k, x in a.items()} == \
        {k: ([essence(r) for r in x["hits"]], x["total"]) for k, x in b.items()}


def test_without_a_level_field_both_sources_pick_the_same_rows_by_words_in_the_message():
    seen: list[str] = []
    picked = es("search_errors_unparsed", "rec-unparsed", K8S, seen).search_errors(["cim-eap", "mq-broker"], *HOUR, 50)
    assert '"match": {"message": {"query": "error fatal critical", "operator": "or"}}' in seen[0]
    assert [essence(r, K8S) for r in picked] == [essence(r, K8S) for r in victoria("unparsed", K8S).search_errors(["cim-eap", "mq-broker"], *HOUR, 50)]
    assert any("0 error(s)" in r["_msg"] for r in picked)       # INFO 的訊息寫到 error 這個詞：兩種來源都會誤撈
    items = [("cim-eap", ["publish", "to", "eap.events", "timed", "out"], ("ERROR", "error")), ("cim-eap", ["health", "probe"], ("ERROR",))]
    args = (HOUR[0], HOUR[1], HOUR[0] - timedelta(minutes=30))
    assert es("count_phrases_unparsed", "rec-unparsed", K8S).count_phrases(items, *args) == \
        victoria("unparsed", K8S).count_phrases(items, *args) == [(1, 0), (1, 0)]


def test_elasticsearch_errors_are_reported_and_a_missing_index_is_just_empty():
    assert es("missing_index", "rec-nope").search_errors(SERVICES, *HOUR, 5) == []   # ignore_unavailable：索引不存在不是錯誤
    with pytest.raises(GrafanaError, match="ES 錯誤日誌查詢失敗.*Fielddata is disabled"):
        es("bad_field_type", fields=LogFields(time="message")).search_errors(SERVICES, *HOUR, 5)


def incident():
    alerts = [Alert(f"f{i}", "firing", f"r{i}", f"{s} 告警", s, "P2", BASE - timedelta(seconds=330 - i), None, {"A": 1})
              for i, s in enumerate(SERVICES)]
    [inc] = converge([], alerts, [], EDGES, now=BASE, close_after_seconds=300)
    return inc


def test_the_whole_evidence_is_the_same_whichever_log_source_it_came_from():
    """同一張事故、同一批資料：經 Elasticsearch 收集到的整包證據，跟經 VictoriaLogs 的一樣（只差時間的小數寫法與來源名稱）。"""
    seen: list[str] = []
    store = es("collect_evidence", seen=seen)
    from_es = rca.collect_evidence(incident(), EDGES, store.grafana, logs=store, id_fields=["eqp_id", "lot_id", "carrier_id"], now=BASE)
    assert seen == [x["request"]["body"] for x in ES["collect_evidence"]]   # 送出的五個請求跟錄製時一模一樣
    v = victoria()
    from_victoria = rca.collect_evidence(incident(), EDGES, v.grafana, logs=v, id_fields=["eqp_id", "lot_id", "carrier_id"], now=BASE)
    same_time = lambda text: json.loads(json.dumps(text, ensure_ascii=False).replace(".000Z", "Z"))
    assert from_es.pop("log_source")["name"] == "Elasticsearch" and from_victoria.pop("log_source")["name"] == "VictoriaLogs"
    assert same_time(from_es) == same_time(from_victoria)
    assert len(from_es["error_logs"]) == 4 and len(from_es["timelines"]) == 2 and all("during" in r for r in from_es["error_patterns"])
    text = rca.overview_text(rca.overview({**from_es, "log_source": {"name": "Elasticsearch", "scope_hint": Elasticsearch.scope_hint}}))
    assert text.startswith("已有的證據｜告警 3 筆")


def test_zero_logs_hint_names_the_source_that_is_configured():
    store = es("count_logs_count-empty", "rec-count-empty")
    store.search_errors = lambda *a, **kw: []
    ev = rca.collect_evidence(incident(), EDGES, store.grafana, logs=store, id_fields=["lot_id"], now=BASE)
    assert "這段時間一筆日誌都沒有。檢查 logs.index，以及日誌有沒有在送進 Elasticsearch" in rca.overview_text(rca.overview(ev))


# ── Trace：Tempo 回的 OTLP 轉成跟 VictoriaTraces 同一種形狀 ──

def tempo(handler) -> Tempo:
    return Tempo(Grafana("http://grafana.test", "t", transport=httpx.MockTransport(handler), retries=0), "tempo-uid")


def test_tempo_search_and_fetch_use_the_recorded_requests():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        name = "tempo_search" if request.url.path.endswith("/api/search") else "tempo_trace"
        return httpx.Response(200, json=recorded(name)["response"])

    found = tempo(handler).search_error_traces(["web-portal", "mes-api", "mes-db"], *HOUR, 20)
    assert seen[0].url.path == "/api/datasources/proxy/uid/tempo-uid/api/search"
    assert seen[0].url.params["q"] == recorded("tempo_search")["request"]["params"]["q"]      # TraceQL 跟錄製時一樣
    assert sorted(t["id"] for t in found) == ["11" * 16, "22" * 16] and all(t["trace"] is None for t in found)   # 只有編號，沒有整條
    assert all(t["start"] > 1_700_000_000_000_000 for t in found)                              # 開始時間換成微秒
    trace = tempo(handler).get_trace("11" * 16)
    assert seen[-1].url.path == "/api/datasources/proxy/uid/tempo-uid/api/traces/" + "11" * 16 and trace["traceID"] == "11" * 16
    assert tempo(lambda r: httpx.Response(404)).get_trace("ff" * 16) is None                   # Tempo 的 404 沒有內容
    bad = recorded("tempo_search_bad_query")
    with pytest.raises(GrafanaError, match="HTTP 400"):
        tempo(lambda r: httpx.Response(bad["status"], text=bad["response"])).search_error_traces(["a"], *HOUR, 5)


def test_a_tempo_trace_becomes_the_same_call_chain_as_the_victoriatraces_one():
    """兩邊收到的是同一條 Trace（同樣四個 span）。轉換之後，給 LLM 看的呼叫鏈要一模一樣。"""
    converted = _from_otlp(recorded("tempo_trace")["response"], "11" * 16)
    assert {p["serviceName"] for p in converted["processes"].values()} == {"web-portal", "mes-api"}
    deepest = next(sp for sp in converted["spans"] if sp["operationName"] == "SELECT lot_state")
    tags = {t["key"]: t["value"] for t in deepest["tags"]}
    assert tags["error"] == "true" and tags["otel.status_description"] == "connection refused: mes-db:5432"
    parent = next(sp for sp in converted["spans"] if sp["operationName"] == "POST /api/track-in")
    # 奈秒換成微秒；父子關係還在（Tempo 經 Grafana 回的 span 編號是 base64，跟 Jaeger 的十六進位寫法不同，但自己對得起來）
    assert deepest["duration"] == 4_980_000 and deepest["references"][0]["spanID"] == parent["spanID"]
    limits = rca.EvidenceLimits()
    assert rca.summarize_trace(converted, "11" * 16, limits) == rca.summarize_trace(recorded_trace(), "11" * 16, limits)
    numeric = {"resourceSpans": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "svc"}}]},
                                  "scopeSpans": [{"spans": [{"spanId": "s1", "name": "op", "startTimeUnixNano": "1000000",
                                                             "endTimeUnixNano": "3000000", "status": {"code": 2}}]}]}]}
    [only] = rca.summarize_trace(_from_otlp(numeric, "t"), "t", limits)["spans"]     # resourceSpans 與數字狀態碼（OTLP 的另一種寫法）
    assert (only["status"], only["duration_ms"], only["service"]) == ("error", 2, "svc")
    assert _from_otlp({"batches": []}, "t")["spans"] == []


def test_service_names_are_escaped_for_traceql():
    assert _traceql_regex("mes-api") == "mes-api"
    assert _traceql_regex("svc.v2") == r"svc\\.v2" and _traceql_regex("a|b") == r"a\\|b" and _traceql_regex('we"ird') == r'we\"ird'


def test_call_chains_from_tempo_fetch_only_the_traces_that_are_used(fake):
    """Tempo 的搜尋只回編號：要用到的才逐條取（VictoriaTraces 的搜尋直接回整條，不用再取）。"""
    alerts = [Alert(f"f{i}", "firing", f"r{i}", f"{s} errors", s, "P2", BASE, None, {}) for i, s in enumerate(("web-portal", "mes-api"))]
    [inc] = converge([], alerts, [], [("mes-api", "web-portal")], now=BASE, close_after_seconds=300)
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    ev = rca.collect_evidence(inc, [], g, traces=Tempo(g, "tempo-uid"), id_fields=[], now=BASE)
    [chain] = ev["call_chains"]            # 搜到 2 條；另一條取回時 Tempo 回 404（假設已過保留期限）→ 略過
    assert chain["trace_id"] == "11" * 16 and chain["spans"][-1]["error"] == "connection refused: mes-db:5432"
    assert [r.url.path.rsplit("/", 1)[1] for r in fake.trace_requests] == ["search", "22" * 16, "11" * 16]   # 離現在近的先取
    fake.tempo_search = {"traces": []}
    ev = rca.collect_evidence(inc, [], g, traces=Tempo(g, "tempo-uid"), id_fields=[], now=BASE)
    assert "缺呼叫鏈｜還看不出失敗的請求實際停在哪個系統（包含沒有告警的相依服務）。Tempo 裡沒有這些系統的失敗請求" in rca.overview_text(rca.overview(ev))


# ── 設定 ──

def test_the_source_types_are_chosen_in_config_and_default_to_victoria():
    d = Settings.from_dict({**MINIMAL, "logs": {"datasource_uid": "a"}, "traces": {"datasource_uid": "b"}})
    g = Grafana("http://g", "t")
    assert (d.logs_type, d.traces_type) == ("victorialogs", "victoriatraces")
    assert isinstance(log_store(d, g), VictoriaLogs) and isinstance(trace_store(d, g), VictoriaTraces)
    s = Settings.from_dict({**MINIMAL, "logs": {"type": "elasticsearch", "datasource_uid": "a", "index": "fluentd-*",
                                                "fields": {"service": "kubernetes.container_name", "level": "", "time": "ts", "message": "log"}},
                            "traces": {"type": "tempo", "datasource_uid": "b"}})
    store = log_store(s, g)
    assert isinstance(store, Elasticsearch) and (store.index, store.uid) == ("fluentd-*", "a")
    assert store.fields == LogFields(service="kubernetes.container_name", level="", time="ts", message="log")
    assert isinstance(trace_store(s, g), Tempo)
    none = Settings.from_dict(MINIMAL)
    assert log_store(none, g) is None and trace_store(none, g) is None       # 沒設 datasource_uid = 沒接


@pytest.mark.parametrize("extra, message", [
    ({"logs": {"type": "loki"}}, "logs.type 要是 victorialogs／elasticsearch"),
    ({"traces": {"type": "jaeger"}}, "traces.type 要是 victoriatraces／tempo"),
    ({"logs": {"index": "x-*"}}, "logs.index 只有 logs.type 是 elasticsearch"),
    ({"logs": {"type": "elasticsearch", "extra_filter": "{a=\"b\"}"}}, "logs.extra_filter 只有 logs.type 是 victorialogs"),
    ({"logs": {"fields": {"time": "ts"}}}, "logs.fields 有不認得的設定"),       # VictoriaLogs 的時間固定是 _time
    ({"logs": {"type": "elasticsearch", "fields": {"message": ""}}}, "只有 level 可以留空"),
])
def test_settings_that_do_not_fit_the_chosen_source_are_rejected(extra, message):
    with pytest.raises(ConfigError, match=message):
        Settings.from_dict({**MINIMAL, **extra})


# ── 整輪 ──

def test_a_round_with_elasticsearch_and_tempo_writes_the_same_kind_of_annotation(world):  # noqa: F811
    w = world
    slow = json.loads((FIXTURES / "slow_requests.json").read_text(encoding="utf-8"))["exchanges"]
    # 五個是日誌證據；第六個是「用呼叫鏈的 trace id 查日誌」，真的 Elasticsearch 對一條日誌裡沒有的 Trace 回的內容
    w.fake.msearch_replies = [{"status": x["status"], "response": x["response"]}
                              for x in (*ES["collect_evidence"], *slow["es_logs_of_unknown_trace"])]
    settings = replace(w.settings, logs_type="elasticsearch", logs_datasource_uid="es-uid", logs_index="rec-factory",
                       traces_type="tempo", traces_datasource_uid="tempo-uid")
    w.receive("firing_mq", "firing_cim", "firing_amhs")
    assert w.run(64, settings=settings)["status"] == "ok"
    [inc] = rows(w, dbmod.incidents)
    assert len(w.fake.msearch_requests) == 6 and w.fake.log_queries == [] and w.fake.msearch_replies == []   # 只打了 Elasticsearch
    assert all(json.loads(body.splitlines()[0])["index"] == "rec-factory" for body in w.fake.msearch_requests)
    note = w.fake.annotations[inc["annotation_id"]]["text"]
    assert "根因分析｜[LLM 推論]" in note and "證據｜已有告警 3 筆" in note and "錯誤日誌 4 種" in note
    prompt = w.llm_calls[0]["messages"][1]["content"]
    assert "EQP-CVD-07" in prompt and "T3 timeout waiting S6F12 reply" in prompt
    assert "缺呼叫鏈（Tempo 裡沒有這些系統的失敗請求" in inc["evidence_note"] or "失敗請求的呼叫鏈" in inc["evidence_note"]
