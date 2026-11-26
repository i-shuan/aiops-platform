"""關聯證據：工廠用批號／載具把跨系統的日誌串成時間線（VictoriaLogs），IT 服務用 VictoriaTraces 的呼叫鏈。
日誌與 Trace 的樣本是 Grafana 13.0.7 + VictoriaLogs v1.17.0／VictoriaTraces v0.5.0 實際回的內容
（tests/fixtures/grafana13/victoria_*.json，Editor Token 經資料源代理）。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import rca
from src.grafana import Grafana, LogFields, parse_webhook
from src.grouping import Alert, converge
from src.main import ConfigError, Settings
from tests.conftest import SHIFT, log_row, recorded_rows, recorded_trace, shifted, victoria, webhook

T0 = datetime(2026, 11, 21, 2, 41, 10, tzinfo=timezone.utc)
NOW = T0 + timedelta(seconds=64)
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs"), ("web-portal", "mes-api"), ("mes-api", "cim-eap")]
LLM = {"api_url": "http://llm.test/v1", "api_key": "k", "model": "m"}
MINIMAL = "grafana: {url: 'http://g', token: 't'}\nwebhook: {token: 'w'}\n"
TRACE_1, TRACE_2 = "11" * 16, "22" * 16


def incident(*services):
    if services:
        alerts = [Alert(f"f{i}", "firing", f"r{i}", f"{s} errors", s, "P2", T0, None, {}) for i, s in enumerate(services)]
    else:
        alerts = [a for n in ("firing_mq", "firing_cim") for a in parse_webhook(webhook(n))[0]]
    [inc] = converge([], alerts, [], EDGES, now=NOW, close_after_seconds=300)
    return inc


def collect(fake, inc=None, *, logs="vl", traces="", limits=rca.EvidenceLimits(), **kw):
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    return rca.collect_evidence(inc or incident(), EDGES, g, logs_datasource_uid=logs, id_fields=["eqp_id", "lot_id", "carrier_id"],
                                now=NOW, limits=limits, traces_datasource_uid=traces, **kw)


def trace_copy(trace_id, seconds_later=0):
    """錄下來那條失敗的 Trace 換一個編號、時間往後移幾秒（落在事故時間窗裡）。"""
    base = recorded_trace()
    micros = int((SHIFT.total_seconds() + seconds_later) * 1_000_000)
    return {**base, "traceID": trace_id, "spans": [{**sp, "traceID": trace_id, "startTime": sp["startTime"] + micros}
                                                   for sp in base["spans"]]}


def llm_returning(answer):
    seen = []

    def handler(request):
        seen.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(answer, ensure_ascii=False)}}]})

    return httpx.MockTransport(handler), seen


# ── 關聯時間線（VictoriaLogs）──

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
    [lots, lot_totals] = [q for q in fake.log_queries if '"lot_id":in(' in q["query"]]        # 每種關聯碼兩次：內容、筆數
    assert lots["query"] == '"lot_id":in("LOT24A0031") | sort by (_time) partition by ("lot_id") limit 7'   # 由舊到新
    assert lot_totals["query"] == '"lot_id":in("LOT24A0031") | stats by ("lot_id") count() as total'
    assert "log.level" not in lots["query"] and "service.name" not in lots["query"]   # 不限等級、不限系統：要看到這批貨的完整經過
    assert lots["start"] == "2026-11-21T02:11:10.000000Z"                             # 事故開始前 30 分鐘
    assert fake.queries('"carrier_id":in("FOUP0912")')


def test_timeline_shows_normal_events_around_the_errors_and_says_when_it_is_cut(fake):
    fake.logs = [log_row(f"2026-11-21T02:{m:02d}:00Z", svc, lvl, msg, lot_id="LOT24A0031")
                 for m, svc, lvl, msg in [(32, "cim-eap", "INFO", "lot track-in complete"), (38, "amhs-mcs", "INFO", "transfer complete"),
                                          (40, "cim-eap", "ERROR", "HSMS T3 Timeout"), (41, "amhs-mcs", "ERROR", "transfer aborted")]]
    ev = collect(fake, limits=rca.EvidenceLimits(max_timeline_events=3))
    [line] = ev["timelines"]                                           # 只有批號有紀錄；沒有載具編號就沒有那一條
    assert [e["level"] for e in line["events"]] == ["INFO", "INFO", "ERROR"] and line["truncated"] is True


def test_timelines_can_be_limited_or_turned_off(fake):
    assert [t["id"] for t in collect(fake, limits=rca.EvidenceLimits(max_timeline_ids=1))["timelines"]] in (["LOT24A0031"], ["FOUP0912"])
    before = len(fake.queries("partition by (\"lot_id\")") + fake.queries("partition by (\"carrier_id\")"))
    assert collect(fake, limits=rca.EvidenceLimits(timeline_fields=()))["timelines"] == []
    assert collect(fake, limits=rca.EvidenceLimits(max_timeline_ids=0))["timelines"] == []
    assert len(fake.queries("partition by (\"lot_id\")") + fake.queries("partition by (\"carrier_id\")")) == before   # 關掉就不多查


def test_timeline_uses_the_configured_service_and_level_fields(fake):
    fake.logs = [{"_time": "2026-11-21T02:40:00Z", "_msg": "T3", "app": "cim-eap", "severity": "ERROR", "lot_id": "L1"}]
    ev = collect(fake, log_fields=LogFields(service="app", level="severity"))
    assert fake.queries('"app":in(') and fake.queries('"severity":in(')
    assert ev["timelines"][0]["events"] == [{"time": "2026-11-21T02:40:00Z", "system": "cim-eap", "level": "ERROR", "message": "T3", "lot_id": "L1"}]


def test_timeline_failure_keeps_the_rest_of_the_evidence(fake):
    fake.fail_queries = {'"lot_id":in(', '"carrier_id":in('}
    ev = collect(fake)
    assert ev["timelines"] == [] and (len(ev["error_logs"]), sum(e.get("similar_in_sample", 1) for e in ev["error_logs"])) == (3, 5) and any("關聯時間線撈取失敗" in n for n in ev["notes"])


# ── 呼叫鏈（VictoriaTraces，Jaeger 相容的查詢介面）──

def test_error_traces_become_call_chains_with_the_deepest_failure_marked(fake):
    ev = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid")
    later, chain = ev["call_chains"]       # 搜到兩條失敗的（成功的那條不會被搜到）；離現在近的排前面
    assert (later["trace_id"], chain["trace_id"]) == (TRACE_2, TRACE_1)
    assert chain["systems"] == ["mes-api", "mes-db", "web-portal"] and not chain["truncated"]
    rows = [(r["service"], r["span"], r["called_by"], r["status"]) for r in chain["spans"]]
    assert rows == [("web-portal", "POST /lots/track-in", None, "error"),
                    ("web-portal", "HTTP POST mes-api /api/track-in", "web-portal", "error"),
                    ("mes-api", "POST /api/track-in", "web-portal", "error"),
                    ("mes-api", "SELECT lot_state", "mes-api", "error")]
    assert [r["origin"] for r in chain["spans"]] == [False, False, False, True]   # 最下游、底下沒有其他失敗的那一段
    origin = chain["spans"][-1]
    assert origin["error"] == "connection refused: mes-db:5432" and origin["attributes"] == {"db.system": "postgresql", "peer.service": "mes-db"}
    assert origin["duration_ms"] == 4980 and origin["offset_ms"] == 8


def test_traces_are_searched_per_system_for_the_incident_window(fake):
    collect(fake, incident("web-portal", "mes-api"), traces="vt-uid", limits=rca.EvidenceLimits(max_traces=2))
    first, second = fake.trace_requests                       # Jaeger 的搜尋一次一個服務；回來的已經是完整的 Trace，不用再逐條取
    assert first.url.path == "/api/datasources/proxy/uid/vt-uid/api/traces"
    assert sorted(r.url.params["service"] for r in (first, second)) == ["mes-api", "web-portal"]
    assert json.loads(first.url.params["tags"]) == {"error": "true"}
    assert first.url.params["limit"] == "20"      # 多搜一些再自己挑最近的
    assert int(first.url.params["start"]) == int((T0 - timedelta(minutes=10)).timestamp() * 1_000_000)   # 微秒


def test_the_most_recent_traces_are_chosen(fake):
    fake.traces = {name: trace_copy(name, seconds) for name, seconds in (("old", 0), ("newest", 60), ("newer", 30))}
    ev = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid", limits=rca.EvidenceLimits(max_traces=2))
    assert [c["trace_id"] for c in ev["call_chains"]] == ["newest", "newer"]
    fake.trace_requests.clear()
    assert collect(fake, incident("web-portal", "mes-api"), traces="vt-uid", limits=rca.EvidenceLimits(max_traces=0))["call_chains"] == []
    assert fake.trace_requests == []            # max_traces = 0：連搜尋都不做


def test_trace_ids_found_in_error_logs_are_fetched_directly_before_searching(fake):
    """日誌帶 trace id 時直接用編號取：一定是這次錯誤的那幾條。不夠才用搜尋補。"""
    fake.traces = {name: trace_copy(name) for name in ("from-log-1", "from-log-2", "from-search")}
    del fake.traces["from-search"]["spans"][0]["tags"][:]         # 先讓它搜不到（沒有 error 標籤的 span 不影響用編號取）
    fake.traces["from-search"] = trace_copy("from-search")
    cim = sorted((row for row in fake.logs if row["service.name"] == "cim-eap" and row["log.level"] == "ERROR"),
                 key=lambda row: row["_time"], reverse=True)       # 錯誤日誌是新的在前
    cim[0]["trace_id"] = "from-log-1"
    cim[1]["trace_id"] = "sampled-out"                            # VictoriaTraces 沒有這條（被取樣掉）：略過
    cim[2]["trace_id"] = "from-log-2"
    cim[3]["trace_id"] = "from-log-1"                             # 重複的 id 只取一次
    ev = collect(fake, incident("mes-api", "cim-eap"), traces="vt-uid")
    assert [c["trace_id"] for c in ev["call_chains"]] == ["from-log-1", "from-log-2", "from-search"]   # 不夠 3 條才用搜尋補
    paths = [r.url.path.rsplit("/", 1)[1] for r in fake.trace_requests]
    assert paths == ["from-log-1", "sampled-out", "from-log-2", "traces", "traces"]              # 最後兩次是搜尋（每個系統一次）
    fake.trace_requests.clear()
    two = collect(fake, incident("mes-api", "cim-eap"), traces="vt-uid", limits=rca.EvidenceLimits(max_traces=2))
    assert [c["trace_id"] for c in two["call_chains"]] == ["from-log-1", "from-log-2"]
    assert all(not r.url.path.endswith("/api/traces") for r in fake.trace_requests)             # 日誌裡的就夠了：不搜尋
    off = collect(fake, incident("mes-api", "cim-eap"), traces="vt-uid", limits=rca.EvidenceLimits(trace_id_field="", max_traces=1))
    assert len(off["call_chains"]) == 1 and all(r.url.path.endswith("/api/traces") for r in fake.trace_requests[-2:])


def test_recorded_traces_have_the_shape_we_parse():
    trace = recorded_trace()
    assert trace["traceID"] == TRACE_1 and {p["serviceName"] for p in trace["processes"].values()} == {"web-portal", "mes-api"}
    deepest = next(sp for sp in trace["spans"] if sp["operationName"] == "SELECT lot_state")
    tags = {t["key"]: t["value"] for t in deepest["tags"]}
    assert tags["error"] == "true" and tags["otel.status_description"] == "connection refused: mes-db:5432"
    assert tags["peer.service"] == "mes-db" and deepest["duration"] == 4_980_000                 # 微秒
    assert deepest["references"] == [{"refType": "CHILD_OF", "spanID": "b1" * 8, "traceID": TRACE_1}]
    missing = victoria("traces")["exchanges"]["get_trace_missing"][0]
    assert missing["status"] == 404 and json.loads(missing["response"])["data"] == []
    ok = recorded_trace("get_trace_ok")
    assert {t["value"] for sp in ok["spans"] for t in sp["tags"] if t["key"] == "error"} == {"false"}   # 成功的 span 是 error=false


def test_call_chains_do_not_need_logs_and_are_skipped_without_a_traces_datasource(fake):
    ev = collect(fake, incident("web-portal", "mes-api"), logs="", traces="vt-uid")
    assert len(ev["call_chains"]) == 2 and ev["error_logs"] == [] and fake.log_queries == []
    fake.trace_requests.clear()
    assert collect(fake, incident("web-portal", "mes-api"))["call_chains"] == [] and fake.trace_requests == []


def test_trace_failure_or_no_traces_is_noted_and_never_blocks(fake):
    fake.fail.add("/api/datasources/proxy/uid/vt-uid")
    ev = collect(fake, traces="vt-uid")
    assert ev["call_chains"] == [] and any("呼叫鏈撈取失敗" in n for n in ev["notes"]) and (len(ev["error_logs"]), sum(e.get("similar_in_sample", 1) for e in ev["error_logs"])) == (3, 5)
    fake.fail.clear()
    fake.traces = {}
    assert any("沒有這些系統的錯誤 Trace" in n for n in collect(fake, traces="vt-uid")["notes"])


def test_long_traces_keep_failed_spans_first_and_other_ways_to_mark_failure_are_read():
    def span(i, parent, tags):
        return {"spanID": f"s{i}", "operationName": f"op{i}", "processID": "p1", "startTime": 1000 + i, "duration": 1000,
                "references": [{"refType": "CHILD_OF", "spanID": parent, "traceID": "t1"}] if parent else [],
                "tags": [{"key": k, "type": "string", "value": v} for k, v in tags.items()]}

    spans = [span(0, None, {})] + [span(i, "s0", {}) for i in range(1, 40)] + [span(40, "s0", {"otel.status_code": "ERROR"})]
    trace = {"traceID": "t1", "spans": spans, "processes": {"p1": {"serviceName": "svc"}}}   # 沒有 error 標籤、只有 otel.status_code
    chain = rca.summarize_trace(trace, "t1", rca.EvidenceLimits(max_trace_spans=5))
    assert len(chain["spans"]) == 5 and chain["truncated"] is True
    assert chain["spans"][0]["span"] == "op0" and chain["spans"][-1]["span"] == "op40" and chain["spans"][-1]["origin"] is True
    assert rca.summarize_trace({"traceID": "t2", "spans": [], "processes": {}}, "t2", rca.EvidenceLimits()) is None


# ── 進 LLM 與幻覺檢查 ──

def test_prompt_gains_the_two_packs_only_when_there_is_something_to_show(fake):
    plain = rca.build_prompt(collect(fake, incident("erp"), limits=rca.EvidenceLimits(timeline_fields=())))
    assert "【5." not in plain and "【6." not in plain
    full = rca.build_prompt(collect(fake, incident("mes-api", "cim-eap"), traces="vt-uid"))
    assert "【5. 關聯時間線" in full and "只代表先後，不代表因果" in full and "LOT24A0031" in full
    assert "【6. 呼叫鏈" in full and "connection refused: mes-db:5432" in full and '"origin": true' in full


def test_a_system_seen_only_in_the_call_chain_may_be_named_but_an_invented_one_may_not(fake):
    inc = incident("web-portal", "mes-api")
    ev = collect(fake, inc, traces="vt-uid")
    good = {"summary": "源頭可能是 mes-db 連線被拒，mes-api 與 web-portal 連帶失敗", "systems": ["mes-db", "mes-api"], "ids": []}
    line, analyzed, _ = rca.analyze(inc, ev, transport=llm_returning(good)[0], **LLM)
    assert analyzed and "mes-db" in line                                # mes-db 沒有告警、也不在關係邊，但呼叫鏈裡真的有
    without = collect(fake, inc)                                        # 沒有 Trace：同一個答案就沒有憑據
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


def test_traces_and_timeline_settings(tmp_path):
    d = load(tmp_path, MINIMAL)
    assert d.traces_datasource_uid == "" and d.evidence.timeline_fields == ("lot_id", "carrier_id")
    assert (d.evidence.max_timeline_ids, d.evidence.max_timeline_events, d.evidence.max_traces, d.evidence.max_trace_spans) == (2, 30, 3, 30)
    assert d.evidence.trace_id_field == "trace_id"
    assert load(tmp_path, MINIMAL + "traces: {trace_id_field: traceID}\n").evidence.trace_id_field == "traceID"
    s = load(tmp_path, MINIMAL + "traces: {datasource_uid: abc, max_traces: 5, max_spans: 60}\n"
                                 "logs: {id_fields: [pod, lot_id], timeline_fields: [lot_id]}\n"
                                 "llm: {max_timeline_ids: 4, max_timeline_events: 50}\n")
    assert s.traces_datasource_uid == "abc" and s.evidence.timeline_fields == ("lot_id",)
    assert (s.evidence.max_timeline_ids, s.evidence.max_timeline_events, s.evidence.max_traces, s.evidence.max_trace_spans) == (4, 50, 5, 60)
    it_only = load(tmp_path, MINIMAL + "logs: {id_fields: [pod, node]}\n")   # 沒有批號的環境：預設就不做時間線
    assert it_only.evidence.timeline_fields == ()
    assert load(tmp_path, MINIMAL + "logs: {timeline_fields: []}\n").evidence.timeline_fields == ()


@pytest.mark.parametrize("text, message", [
    ("logs: {timeline_fields: [trace_id]}", "不在 logs.id_fields 裡"),
    ("logs: {timeline_fields: lot_id}", "logs.timeline_fields"),
    ("traces: {uid: x}", "traces 有不認得的設定"),
    ("traces: {max_traces: -1}", "traces.max_traces"),
    ("traces: {max_spans: 0}", "traces.max_spans"),
    ("llm: {max_timeline_events: 0}", "llm.max_timeline_events"),
    ("es: {datasource_uid: x}", "es（改成 logs）"),               # 換成 Victoria 之前的區段名稱：直接說改成什麼
    ("tempo: {datasource_uid: x}", "tempo（改成 traces）"),
    ("logs: {log_index: 'x-*'}", "logs 有不認得的設定"),
])
def test_traces_and_timeline_settings_are_validated(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load(tmp_path, MINIMAL + text + "\n")


# ── 日誌沒有等級欄位（沒拆欄位的容器標準輸出）：logs.fields.level 留空，改用訊息文字挑錯誤日誌 ──

NO_LEVEL = LogFields(service="kubernetes.container_name", level="")


def test_without_a_level_field_errors_are_picked_by_words_in_the_message(fake):
    fake.logs = shifted(recorded_rows("unparsed"), SHIFT)   # 真的 VictoriaLogs 裡沒拆欄位的容器日誌：等級只在訊息文字裡
    ev = collect(fake, log_fields=NO_LEVEL)
    [query] = fake.queries("sort by (_time) desc")
    # 不分大小寫比對完整的詞：預設的 ERROR／error／Error… 收成三個詞，任何一個出現就算
    assert query == ('"kubernetes.container_name":in("cim-eap","mq-broker") (_msg:i("error") or _msg:i("fatal") or _msg:i("critical"))'
                     ' | sort by (_time) desc | limit 100')
    assert not fake.queries("log.level") and not fake.queries('"":')
    assert [e["system"] for e in ev["error_logs"]] == ["cim-eap", "cim-eap", "mq-broker"]   # 照第一次出現的時間排
    assert any("0 error(s)" in e["message"] for e in ev["error_logs"])   # INFO 的訊息寫到 error 這個詞也被挑進來（已知限制）
    assert any("依訊息文字裡的等級字樣挑的" in note for note in ev["notes"])
    assert "可能混入不是錯誤的紀錄" in rca.build_prompt(ev)   # LLM 看得到這批樣本的來歷
    for line in ev["timelines"]:
        assert all("level" not in event for event in line["events"])   # 沒有等級欄位就不硬塞一個空的 level


def test_with_a_level_field_nothing_changes(fake):
    ev = collect(fake)
    [query] = fake.queries("sort by (_time) desc")
    assert '"log.level":in("ERROR","error","Error","FATAL","fatal","CRITICAL","critical")' in query and "_msg:i(" not in query
    assert not any("等級字樣" in note for note in ev["notes"])


def test_only_the_log_level_field_may_be_left_empty(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(MINIMAL + "logs: {fields: {service: kubernetes.container_name, level: ''}}\n")
    assert Settings.load(str(path)).log_fields == NO_LEVEL
    path.write_text(MINIMAL + "logs: {fields: {service: ''}}\n")
    with pytest.raises(ConfigError, match="只有 level 可以留空"):
        Settings.load(str(path))


def test_ids_that_only_appear_in_log_messages_count_as_evidence(fake):
    """日誌沒拆欄位時，機台與批號只在訊息文字裡、不在識別碼統計裡：模型引用它們不是幻覺；證據裡完全沒有的照樣擋。
    （真的 LLM + 沒拆欄位的容器日誌實測時遇到的：五個識別碼都在訊息裡，卻整份被退回。）"""
    ev = collect(fake, log_fields=NO_LEVEL)
    ev["id_counts"] = {"eqp_id": {}, "lot_id": {}, "carrier_id": {}}
    ev["timelines"] = []
    ev["error_logs"] = [{"time": "2026-11-21T02:41:07Z", "system": "cim-eap",
                         "message": "publish timed out lot=LOT24A0031 eqp=EQP-CVD-07"}]
    answer = {"summary": "cim-eap 發佈逾時，集中在 EQP-CVD-07", "systems": ["cim-eap"], "ids": ["EQP-CVD-07", "LOT24A0031"]}
    assert rca.check_answer(json.dumps(answer), ev).startswith("cim-eap 發佈逾時")
    for invented in ("EQP-LITHO-11", "", 7):
        with pytest.raises(ValueError, match="點名了證據裡沒有的名稱"):
            rca.check_answer(json.dumps({**answer, "ids": [invented]}), ev)
