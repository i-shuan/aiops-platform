"""關聯證據：工廠用批號／載具把跨系統的日誌串成時間線（ES），IT 服務用 Tempo 的呼叫鏈。
Tempo 的回應是 Grafana 13.0.7 + Tempo 2.9.0 實際回的內容（tests/fixtures/grafana13/tempo_*.json，Editor Token 經資料源代理）。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import rca
from src.grafana import Grafana, LogFields, _traceql_regex, parse_webhook
from src.grouping import Alert, converge
from src.main import ConfigError, Settings
from tests.conftest import recorded, webhook

T0 = datetime(2026, 11, 21, 2, 41, 10, tzinfo=timezone.utc)
NOW = T0 + timedelta(seconds=64)
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs"), ("web-portal", "mes-api"), ("mes-api", "cim-eap")]
LLM = {"api_url": "http://llm.test/v1", "api_key": "k", "model": "m"}
MINIMAL = "grafana: {url: 'http://g', token: 't'}\nwebhook: {token: 'w'}\n"
TRACE_1 = "11111111111111111111111111111111"


def incident(*services):
    if services:
        alerts = [Alert(f"f{i}", "firing", f"r{i}", f"{s} errors", s, "P2", T0, None, {}) for i, s in enumerate(services)]
    else:
        alerts = [a for n in ("firing_mq", "firing_cim") for a in parse_webhook(webhook(n))[0]]
    [inc] = converge([], alerts, [], EDGES, now=NOW, close_after_seconds=300)
    return inc


def collect(fake, inc=None, *, es="es", tempo="", limits=rca.EvidenceLimits(), **kw):
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    return rca.collect_evidence(inc or incident(), EDGES, g, es_datasource_uid=es, log_index="factory-logs-*",
                                trace_index="factory-traces", id_fields=["eqp_id", "lot_id", "carrier_id"], now=NOW, limits=limits,
                                tempo_datasource_uid=tempo, **kw)


def llm_returning(answer):
    seen = []

    def handler(request):
        seen.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(answer, ensure_ascii=False)}}]})

    return httpx.MockTransport(handler), seen


# ── 關聯時間線（ES）──

def test_most_frequent_lot_and_carrier_become_cross_system_timelines(fake):
    ev = collect(fake)
    assert [(t["key"], t["id"]) for t in ev["timelines"]] == [("lot_id", "LOT24A0031"), ("carrier_id", "FOUP0912")]
    lot = ev["timelines"][0]
    times = [e["time"] for e in lot["events"]]
    assert times == sorted(times) and len(times) == 3                 # 由舊到新
    assert lot["systems"] == ["amhs-mcs", "cim-eap"]                  # 同一批貨先後出現在兩個系統
    assert [e["system"] for e in lot["events"]] == ["cim-eap", "cim-eap", "amhs-mcs"]
    assert lot["events"][0]["level"] == "ERROR" and lot["events"][0]["eqp_id"] == "EQP-CVD-07" and not lot["truncated"]


def test_timeline_query_is_not_limited_to_errors_or_to_the_incident_systems(fake):
    collect(fake, limits=rca.EvidenceLimits(max_timeline_events=7, window_padding_minutes=30))
    [lots, carriers] = fake.timeline_requests           # 每種關聯碼一次 _msearch
    q = lots[0]
    text = json.dumps(q)
    assert q["size"] == 7 and q["sort"] == [{"@timestamp": "asc"}] and q["track_total_hits"] is True
    assert "log.level" not in text and "service.name" not in text      # 不限等級、不限系統：要看到這批貨的完整經過
    assert '"lot_id": ["LOT24A0031"]' in text and '"lot_id.keyword": ["LOT24A0031"]' in text
    assert q["query"]["bool"]["filter"][0]["range"]["@timestamp"]["gte"] == int((T0 - timedelta(minutes=30)).timestamp() * 1000)
    assert '"carrier_id": ["FOUP0912"]' in json.dumps(carriers[0])


def test_timeline_shows_normal_events_around_the_errors_and_says_when_it_is_cut(fake):
    docs = [{"@timestamp": f"2026-11-21T02:{m:02d}:00Z", "service": {"name": svc}, "log": {"level": lvl}, "message": msg, "lot_id": "LOT24A0031"}
            for m, svc, lvl, msg in [(30, "cim-eap", "INFO", "lot track-in complete"), (38, "amhs-mcs", "INFO", "transfer complete"),
                                     (40, "cim-eap", "ERROR", "HSMS T3 Timeout"), (41, "amhs-mcs", "ERROR", "transfer aborted")]]
    fake.timeline_docs = {"LOT24A0031": docs, "FOUP0912": []}
    ev = collect(fake, limits=rca.EvidenceLimits(max_timeline_events=3))
    [line] = ev["timelines"]                                           # 沒有紀錄的那個關聯碼不出現
    assert [e["level"] for e in line["events"]] == ["INFO", "INFO", "ERROR"] and line["truncated"] is True


def test_timelines_can_be_limited_or_turned_off(fake):
    assert [t["id"] for t in collect(fake, limits=rca.EvidenceLimits(max_timeline_ids=1))["timelines"]] in (["LOT24A0031"], ["FOUP0912"])
    before = len(fake.timeline_requests)
    assert collect(fake, limits=rca.EvidenceLimits(timeline_fields=()))["timelines"] == []
    assert collect(fake, limits=rca.EvidenceLimits(max_timeline_ids=0))["timelines"] == []
    assert len(fake.timeline_requests) == before                        # 關掉就不多查


def test_timeline_uses_the_configured_time_and_service_fields(fake):
    fake.es = {"responses": [{"hits": {"hits": [{"_source": {"ts": "2026-11-21T02:42:00Z", "app": "cim-eap", "severity": "ERROR",
                                                             "msg": "T3", "lot_id": "L1"}}]}},
                             {"hits": {"hits": []}}]}
    ev = collect(fake, log_fields=LogFields(time="ts", service="app", level="severity", message="msg"))
    assert fake.timeline_requests[0][0]["sort"] == [{"ts": "asc"}]
    assert ev["timelines"][0]["events"] == [{"time": "2026-11-21T02:42:00Z", "system": "cim-eap", "level": "ERROR", "message": "T3", "lot_id": "L1"}]


def test_timeline_failure_keeps_the_rest_of_the_evidence(fake):
    original = fake.msearch
    fake.msearch = lambda body: original(body) if '"log.level"' in body else httpx.Response(200, json={"responses": []})
    ev = collect(fake)
    assert ev["timelines"] == [] and len(ev["error_logs"]) == 6 and any("關聯時間線撈取失敗" in n for n in ev["notes"])


# ── 呼叫鏈（Tempo）──

def test_error_traces_become_call_chains_with_the_deepest_failure_marked(fake):
    ev = collect(fake, incident("web-portal", "mes-api"), tempo="tempo-uid")
    [chain] = ev["call_chains"]            # 搜到 2 條；第 2 條取回時 Tempo 回 404（假設已過保留期限）→ 略過
    assert chain["trace_id"] == TRACE_1 and chain["systems"] == ["mes-api", "mes-db", "web-portal"] and not chain["truncated"]
    rows = [(r["service"], r["span"], r["called_by"], r["status"]) for r in chain["spans"]]
    assert rows == [("web-portal", "POST /lots/track-in", None, "error"),
                    ("web-portal", "HTTP POST mes-api /api/track-in", "web-portal", "error"),
                    ("mes-api", "POST /api/track-in", "web-portal", "error"),
                    ("mes-api", "SELECT lot_state", "mes-api", "error")]
    assert [r["origin"] for r in chain["spans"]] == [False, False, False, True]   # 最下游、底下沒有其他失敗的那一段
    origin = chain["spans"][-1]
    assert origin["error"] == "connection refused: mes-db:5432" and origin["attributes"] == {"db.system": "postgresql", "peer.service": "mes-db"}
    assert origin["duration_ms"] == 4980 and origin["offset_ms"] == 8


def test_tempo_is_searched_with_traceql_for_the_incident_systems_and_window(fake):
    collect(fake, incident("web-portal", "mes-api"), tempo="tempo-uid", limits=rca.EvidenceLimits(max_traces=2))
    search, first, second = fake.tempo_requests
    assert search.url.path == "/api/datasources/proxy/uid/tempo-uid/api/search"
    assert search.url.params["q"] == '{ resource.service.name =~ "mes-api|web-portal" && status = error }'
    assert search.url.params["limit"] == "20"     # 多搜一些再自己挑最近的（Tempo 不保證先回最新的）
    assert int(search.url.params["start"]) == int((T0 - timedelta(minutes=10)).timestamp())
    assert first.url.path.endswith(f"/api/traces/{TRACE_1}") or second.url.path.endswith(f"/api/traces/{TRACE_1}")


def test_the_most_recent_traces_are_chosen_because_tempo_does_not_return_newest_first(fake):
    """用真的 Tempo 2.9.0 實跑時遇到的：時間窗裡同時有舊的與剛發生的錯誤 Trace，搜尋先回了舊的。"""
    trace = fake.tempo_traces[TRACE_1]
    fake.tempo_traces = {name: trace for name in ("old", "newer", "newest")}
    fake.tempo_search = {"traces": [{"traceID": "old", "startTimeUnixNano": "100"}, {"traceID": "newest", "startTimeUnixNano": "300"},
                                    {"traceID": "newer", "startTimeUnixNano": "200"}]}
    ev = collect(fake, incident("web-portal", "mes-api"), tempo="tempo-uid", limits=rca.EvidenceLimits(max_traces=2))
    assert [c["trace_id"] for c in ev["call_chains"]] == ["newest", "newer"]
    fake.tempo_requests.clear()
    assert collect(fake, incident("web-portal", "mes-api"), tempo="tempo-uid", limits=rca.EvidenceLimits(max_traces=0))["call_chains"] == []
    assert fake.tempo_requests == []            # max_traces = 0：連搜尋都不做


def test_trace_ids_found_in_error_logs_are_fetched_directly_before_searching(fake):
    """日誌帶 trace id 時直接用編號取：不必等 Tempo 搜尋得到（實測要 8～40 秒以上），而且一定是這次錯誤的那幾條。"""
    trace = fake.tempo_traces[TRACE_1]
    fake.tempo_traces = {"from-log-1": trace, "from-log-2": trace, "from-search": trace}
    fake.tempo_search = {"traces": [{"traceID": "from-search", "startTimeUnixNano": "9"}]}
    logs = fake.es["responses"][0]["hits"]["hits"]
    logs[0]["_source"]["trace"] = {"id": "from-log-1"}
    logs[1]["_source"]["trace"] = {"id": "sampled-out"}          # Tempo 沒有這條（被取樣掉）：略過
    logs[2]["_source"]["trace"] = {"id": "from-log-2"}
    logs[3]["_source"]["trace"] = {"id": "from-log-1"}           # 重複的 id 只取一次
    ev = collect(fake, incident("mes-api", "cim-eap"), tempo="tempo-uid")
    assert [c["trace_id"] for c in ev["call_chains"]] == ["from-log-1", "from-log-2", "from-search"]   # 不夠 3 條才用搜尋補
    paths = [r.url.path.rsplit("/", 1)[1] for r in fake.tempo_requests]
    assert paths == ["from-log-1", "sampled-out", "from-log-2", "search", "from-search"]
    fake.tempo_requests.clear()
    two = collect(fake, incident("mes-api", "cim-eap"), tempo="tempo-uid", limits=rca.EvidenceLimits(max_traces=2))
    assert [c["trace_id"] for c in two["call_chains"]] == ["from-log-1", "from-log-2"]
    assert all("search" not in r.url.path for r in fake.tempo_requests)                       # 日誌裡的就夠了：不搜尋
    off = collect(fake, incident("mes-api", "cim-eap"), tempo="tempo-uid", limits=rca.EvidenceLimits(trace_id_field=""))
    assert [c["trace_id"] for c in off["call_chains"]] == ["from-search"]


def test_recorded_tempo_responses_have_the_shape_we_parse():
    found = recorded("tempo_search")
    assert found["status"] == 200 and {t["traceID"] for t in found["response"]["traces"]} == {TRACE_1, "2" * 32}
    trace = recorded("tempo_trace")["response"]
    assert "batches" in trace and trace["batches"][0]["scopeSpans"][0]["spans"][0]["status"]["code"] == "STATUS_CODE_ERROR"
    assert recorded("tempo_trace_missing")["status"] == 404 and recorded("tempo_trace_missing")["response"] == ""
    assert recorded("tempo_search_bad_query")["status"] == 400


def test_call_chains_do_not_need_es_and_are_skipped_without_a_tempo_datasource(fake):
    ev = collect(fake, incident("web-portal", "mes-api"), es="", tempo="tempo-uid")
    assert len(ev["call_chains"]) == 1 and ev["error_logs"] == [] and fake.es_requests == []
    fake.tempo_requests.clear()
    assert collect(fake, incident("web-portal", "mes-api"))["call_chains"] == [] and fake.tempo_requests == []


def test_tempo_failure_or_no_traces_is_noted_and_never_blocks(fake):
    fake.fail.add("/api/datasources/proxy/uid/tempo-uid")
    ev = collect(fake, tempo="tempo-uid")
    assert ev["call_chains"] == [] and any("呼叫鏈（Tempo）撈取失敗" in n for n in ev["notes"]) and len(ev["error_logs"]) == 6
    fake.fail.clear()
    fake.tempo_search = {"traces": []}
    assert any("沒有這些系統的錯誤 Trace" in n for n in collect(fake, tempo="tempo-uid")["notes"])


def test_long_traces_keep_failed_spans_first_and_other_trace_formats_are_read():
    def span(i, parent, failed):
        return {"spanId": f"s{i}", **({"parentSpanId": parent} if parent else {}), "name": f"op{i}",
                "startTimeUnixNano": str(1000 + i), "endTimeUnixNano": str(2000 + i), "status": {"code": 2} if failed else {}}

    spans = [span(0, None, False)] + [span(i, "s0", False) for i in range(1, 40)] + [span(40, "s0", True)]
    trace = {"resourceSpans": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "svc"}}]},
                                "scopeSpans": [{"spans": spans}]}]}   # resourceSpans 與數字狀態碼（OTLP 的另一種寫法）
    chain = rca.summarize_trace(trace, "t1", rca.EvidenceLimits(max_trace_spans=5))
    assert len(chain["spans"]) == 5 and chain["truncated"] is True
    assert chain["spans"][0]["span"] == "op0" and chain["spans"][-1]["span"] == "op40" and chain["spans"][-1]["origin"] is True
    assert rca.summarize_trace({"batches": []}, "t2", rca.EvidenceLimits()) is None


def test_service_names_are_escaped_for_traceql():
    assert _traceql_regex("mes-api") == "mes-api"
    assert _traceql_regex("svc.v2") == r"svc\\.v2" and _traceql_regex("a|b") == r"a\\|b" and _traceql_regex('we"ird') == r'we\"ird'


# ── 進 LLM 與幻覺檢查 ──

def test_prompt_gains_the_two_packs_only_when_there_is_something_to_show(fake):
    plain = rca.build_prompt(collect(fake, incident("erp"), limits=rca.EvidenceLimits(timeline_fields=())))
    assert "【5." not in plain and "【6." not in plain
    full = rca.build_prompt(collect(fake, incident("mes-api", "cim-eap"), tempo="tempo-uid"))
    assert "【5. 關聯時間線" in full and "只代表先後，不代表因果" in full and "LOT24A0031" in full
    assert "【6. 呼叫鏈" in full and "connection refused: mes-db:5432" in full and '"origin": true' in full


def test_a_system_seen_only_in_the_call_chain_may_be_named_but_an_invented_one_may_not(fake):
    inc = incident("web-portal", "mes-api")
    ev = collect(fake, inc, tempo="tempo-uid")
    good = {"summary": "源頭可能是 mes-db 連線被拒，mes-api 與 web-portal 連帶失敗", "systems": ["mes-db", "mes-api"], "ids": []}
    line, analyzed, _ = rca.analyze(inc, ev, transport=llm_returning(good)[0], **LLM)
    assert analyzed and "mes-db" in line                                # mes-db 沒有告警、也不在關係邊，但呼叫鏈裡真的有
    without = collect(fake, inc)                                        # 沒有 Tempo：同一個答案就沒有憑據
    assert not rca.analyze(inc, without, transport=llm_returning(good)[0], **LLM)[1]
    invented = {**good, "systems": ["billing-db"]}
    assert not rca.analyze(inc, ev, transport=llm_returning(invented)[0], **LLM)[1]


def test_system_prompt_explains_how_to_read_timelines_and_call_chains():
    assert "不代表因果" in rca.SYSTEM_PROMPT and "呼叫鏈" in rca.SYSTEM_PROMPT and "時鐘" in rca.SYSTEM_PROMPT
    for key in ("summary", "systems", "ids"):
        assert key in rca.SYSTEM_PROMPT


# ── 設定 ──

def load(tmp_path, text):
    f = tmp_path / "config.yaml"
    f.write_text(text, encoding="utf-8")
    return Settings.load(str(f))


def test_tempo_and_timeline_settings(tmp_path):
    d = load(tmp_path, MINIMAL)
    assert d.tempo_datasource_uid == "" and d.evidence.timeline_fields == ("lot_id", "carrier_id")
    assert (d.evidence.max_timeline_ids, d.evidence.max_timeline_events, d.evidence.max_traces, d.evidence.max_trace_spans) == (2, 30, 3, 30)
    assert d.evidence.trace_id_field == "trace.id"
    assert load(tmp_path, MINIMAL + "tempo: {trace_id_field: traceID}\n").evidence.trace_id_field == "traceID"
    s = load(tmp_path, MINIMAL + "tempo: {datasource_uid: abc, max_traces: 5, max_spans: 60}\n"
                                 "es: {id_fields: [pod, lot_id], timeline_fields: [lot_id]}\n"
                                 "llm: {max_timeline_ids: 4, max_timeline_events: 50}\n")
    assert s.tempo_datasource_uid == "abc" and s.evidence.timeline_fields == ("lot_id",)
    assert (s.evidence.max_timeline_ids, s.evidence.max_timeline_events, s.evidence.max_traces, s.evidence.max_trace_spans) == (4, 50, 5, 60)
    it_only = load(tmp_path, MINIMAL + "es: {id_fields: [pod, node]}\n")   # 沒有批號的環境：預設就不做時間線
    assert it_only.evidence.timeline_fields == ()
    assert load(tmp_path, MINIMAL + "es: {timeline_fields: []}\n").evidence.timeline_fields == ()


@pytest.mark.parametrize("text, message", [
    ("es: {timeline_fields: [trace_id]}", "不在 es.id_fields 裡"),
    ("es: {timeline_fields: lot_id}", "es.timeline_fields"),
    ("tempo: {uid: x}", "tempo 有不認得的設定"),
    ("tempo: {max_traces: -1}", "tempo.max_traces"),
    ("tempo: {max_spans: 0}", "tempo.max_spans"),
    ("llm: {max_timeline_events: 0}", "llm.max_timeline_events"),
])
def test_tempo_and_timeline_settings_are_validated(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load(tmp_path, MINIMAL + text + "\n")
