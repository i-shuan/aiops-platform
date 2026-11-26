"""Grafana 對接層：全部用 Grafana 13.0.7（後面是 VictoriaLogs／VictoriaTraces）的真實樣本（tests/fixtures/grafana13/）驗解析。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src.grafana import Grafana, GrafanaError, WebhookError, parse_webhook
from src.logs import LogFields, VictoriaLogs
from src.traces import VictoriaTraces
from tests.conftest import FakeGrafana, recorded, recorded_rows, victoria, webhook


def client(handler) -> Grafana:
    return Grafana("http://grafana.test", "token", transport=httpx.MockTransport(handler))


def vlogs(handler, uid="u", **kw) -> VictoriaLogs:
    return VictoriaLogs(client(handler), uid, **kw)


def vtraces(handler, uid="vt-uid") -> VictoriaTraces:
    return VictoriaTraces(client(handler), uid)


def test_firing_webhook_becomes_an_alert():
    [alert], problems = parse_webhook(webhook("firing_mq"))
    assert problems == []
    assert (alert.status, alert.title, alert.service, alert.severity) == ("firing", "MQ queue depth", "mq-broker", "P1")
    assert alert.fingerprint == "369c5f4adcb630d1" and alert.rule_uid == "cg4xgh0mv7oqod"
    assert alert.starts_at == datetime(2026, 11, 21, 2, 41, 10, tzinfo=timezone.utc) and alert.ends_at is None
    assert alert.values == {"A": 350, "C": 1}


def test_resolved_webhook_keeps_the_fingerprint_and_has_an_end_time():
    [alert], _ = parse_webhook(webhook("resolved_mq"))
    assert alert.status == "resolved" and alert.fingerprint == "369c5f4adcb630d1"
    assert alert.ends_at == datetime(2026, 11, 21, 2, 42, 20, tzinfo=timezone.utc)


def test_alerts_without_aiops_detect_are_ignored_and_bad_labels_are_reported():
    payload = webhook("firing_mq")
    base = payload["alerts"][0]
    unmanaged = json.loads(json.dumps(base)); del unmanaged["labels"]["aiops_detect"]; unmanaged["fingerprint"] = "f1"
    no_service = json.loads(json.dumps(base)); del no_service["labels"]["service"]; no_service["fingerprint"] = "f2"
    bad_sev = json.loads(json.dumps(base)); bad_sev["labels"]["aiops_severity"] = "critical"; bad_sev["fingerprint"] = "f3"
    payload["alerts"] = [base, unmanaged, no_service, bad_sev]
    alerts, problems = parse_webhook(payload)
    assert [(a.fingerprint, a.severity) for a in alerts] == [("369c5f4adcb630d1", "P1"), ("f3", "P3")]
    assert len(problems) == 2 and "沒有 service 標籤" in problems[0] and "不是 P0～P3" in problems[1]


def test_nanosecond_timestamps_are_parsed():
    payload = webhook("resolved_mq")
    payload["alerts"][0]["endsAt"] = "2026-11-15T13:46:50.010208167Z"  # 舊平台樣本裡 Grafana 實際給過的格式
    [alert], _ = parse_webhook(payload)
    assert alert.ends_at == datetime(2026, 11, 15, 13, 46, 50, 10208, tzinfo=timezone.utc)


@pytest.mark.parametrize("payload", [
    [], {}, {"alerts": "x"}, {"alerts": [1]}, {"alerts": [{"labels": {}, "status": "firing"}]},
    {"alerts": [{"labels": {}, "status": "pending", "fingerprint": "a", "startsAt": "2026-11-21T00:00:00Z"}]},
    {"alerts": [{"labels": {}, "status": "firing", "fingerprint": "a", "startsAt": "0001-01-01T00:00:00Z"}]},
])
def test_things_that_are_not_grafana_webhooks_are_rejected(payload):
    with pytest.raises(WebhookError):
        parse_webhook(payload)


NOW = datetime(2026, 11, 21, 0, 36, 38, tzinfo=timezone.utc)   # 樣本錄製的時間
HOUR = (NOW - timedelta(hours=1), NOW + timedelta(minutes=1))
SERVICES = ["mq-broker", "cim-eap", "amhs-mcs"]


def answering(exchanges: list[dict], seen: list[httpx.Request]):
    """照順序回錄下來的真實回應（原樣的文字）。"""
    queue = list(exchanges)

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        rec = queue.pop(0)
        return httpx.Response(rec["status"], text=rec["response"])
    return handler


def form(request: httpx.Request) -> dict:
    return dict(httpx.QueryParams(request.content.decode()))


def test_error_logs_come_from_the_victorialogs_proxy_newest_first():
    seen: list[httpx.Request] = []
    found = vlogs(answering(victoria("logs")["exchanges"]["search_errors"], seen), "vl-uid", extra_filter='{dataset="factory"}').search_errors(
        SERVICES, *HOUR, 50)
    [request] = seen
    assert (request.method, request.url.path) == ("POST", "/api/datasources/proxy/uid/vl-uid/select/logsql/query")
    assert form(request) == victoria("logs")["exchanges"]["search_errors"][0]["request"]["form"]   # 送的跟錄製時一模一樣
    assert len(found) == 6 and [d["_time"] for d in found] == sorted((d["_time"] for d in found), reverse=True)
    assert found[1]["eqp_id"] == "EQP-ETCH-02" and found[1]["service.name"] == "cim-eap" and found[1]["log.level"] == "ERROR"
    assert found[0]["_msg"] == "consumer lag exceeded on queue lot.dispatch"


def test_every_log_query_reads_the_recorded_victorialogs_answer():
    """每一個查詢方法：送出的請求跟錄製時一樣，而且讀得懂真的 VictoriaLogs 回的內容。"""
    recorded_logs = victoria("logs")["exchanges"]
    k8s = LogFields(service="kubernetes.container_name", level="")

    def call(name, method, *args, extra_filter="", log_fields=LogFields(), **kw):
        seen: list[httpx.Request] = []
        store = vlogs(answering(recorded_logs[name], seen), "<uid>", fields=log_fields, extra_filter=extra_filter)
        result = getattr(store, method)(*args, **kw)
        assert [form(r) for r in seen] == [x["request"]["form"] for x in recorded_logs[name]], name
        return result

    profile = call("error_profile", "error_profile", SERVICES, *HOUR, warn_levels=("WARN", "WARNING"), first_n=2,
                   extra_filter='{dataset="factory"}')
    assert {s: [d["_msg"] for d in docs] for s, docs in profile["services"].items()} == {
        "mq-broker": ["consumer lag exceeded on queue lot.dispatch"], "amhs-mcs": ["transfer job aborted: destination port occupied"],
        "cim-eap": ["T3 timeout waiting S6F12 reply", "T3 timeout waiting S2F42 reply"]}   # 每個系統各自最早的兩筆
    assert profile["warnings"] == []
    warned = call("error_profile_warnings", "error_profile", SERVICES, *HOUR, warn_levels=("WARN", "WARNING"),
                  extra_filter='{dataset="count-no-errors"}')
    assert [d["_msg"] for d in warned["warnings"]] == ["stocker STK01 port P3 slow to respond", "connection pool usage 92% (184/200)"]
    counts = call("count_phrases", "count_phrases", [
        ("cim-eap", ["timeout", "waiting", "reply"], ("ERROR",)), ("cim-eap", ["equipment", "disconnected"], ("ERROR",)),
        ("mq-broker", ["consumer", "lag", "exceeded", "on", "queue", "lot.dispatch"], ("ERROR",)),
        ("amhs-mcs", ["no", "such", "words"], ("ERROR",))],
        NOW - timedelta(seconds=450), HOUR[1], HOUR[0], extra_filter='{dataset="factory"}')
    assert counts == [(1, 2), (0, 1), (1, 0), (0, 0)]   # （事故期間, 事故前）
    assert call("count_logs_factory", "count_logs", SERVICES, *HOUR, extra_filter='{dataset="factory"}') == {"all": 7, "services": 7}
    assert call("count_logs_count-mismatch", "count_logs", SERVICES, *HOUR, extra_filter='{dataset="count-mismatch"}') == {
        "all": 4, "services": 0}
    assert call("count_logs_count-empty", "count_logs", SERVICES, *HOUR, extra_filter='{dataset="count-empty"}') == {"all": 0, "services": 0}
    lines = call("search_timeline", "search_timeline", "lot_id", ["LOT24A0031", "LOT24A0060", "LOT-NOPE"], *HOUR, 2,
                 extra_filter='{dataset="factory"}')
    assert {v: (len(got["hits"]), got["total"]) for v, got in lines.items()} == {
        "LOT24A0031": (2, 3), "LOT24A0060": (1, 1), "LOT-NOPE": (0, 0)}
    assert [d["service.name"] for d in lines["LOT24A0031"]["hits"]] == ["cim-eap", "cim-eap"]   # 由舊到新的前兩筆
    unparsed = call("search_errors_unparsed", "search_errors", ["cim-eap", "mq-broker"], *HOUR, 50, log_fields=k8s,
                    extra_filter='{dataset="unparsed"}')
    # 沒有等級欄位：照訊息文字裡的字樣挑。INFO 的那筆寫了 "0 error(s)" 也會被挑進來（已知限制）
    assert [d["_msg"].split(" | ")[1].strip() for d in unparsed] == ["FATAL", "INFO", "ERROR"]
    assert call("quoted_service_name", "search_errors", ['a"b\\c'], *HOUR, 5, extra_filter='{dataset="factory"}') == []


def test_a_rejected_log_query_is_reported_without_the_url_noise():
    """VictoriaLogs 的 400 前面有一大段 remoteAddr 與整條網址；錯誤訊息只留真正的原因。"""
    [rec] = victoria("logs")["exchanges"]["bad_filter"]
    assert rec["status"] == 400 and rec["response"].startswith("remoteAddr:")
    with pytest.raises(GrafanaError) as e:
        vlogs(answering([rec], []), extra_filter="{dataset=").search_errors(SERVICES, *HOUR, 5)
    assert "cannot parse query" in str(e.value) and "remoteAddr" not in str(e.value) and "requestURI" not in str(e.value)


def test_a_log_answer_that_is_not_json_lines_is_reported():
    with pytest.raises(GrafanaError, match="不是一行一個 JSON"):
        vlogs(lambda r: httpx.Response(200, text="<html>login</html>")).search_errors(["a"], *HOUR)


def test_fake_logs_answer_like_the_real_one():
    """假的 Grafana 用 tests/logsql.py 篩資料。這裡拿同一批資料、錄製時送過的每一條查詢去問它，答案要跟真的 VictoriaLogs 一樣
    （stats by 的列順序 VictoriaLogs 不保證，所以那一種不比順序）。"""
    recorded_logs = victoria("logs")["exchanges"]
    fake = FakeGrafana(shift=timedelta(0))
    fake.logs = [row for name in recorded_logs if name.startswith("dump_") for row in recorded_rows(name[5:])]
    http = httpx.Client(transport=fake.transport, base_url="http://grafana.test")
    checked = 0
    for name, exchanges in recorded_logs.items():
        for rec in exchanges:
            got = http.post("/api/datasources/proxy/uid/x/select/logsql/query", data=rec["request"]["form"])
            assert got.status_code == rec["status"], name
            if rec["status"] != 200:
                continue
            rows = lambda text: [json.loads(line) for line in text.splitlines() if line.strip()]
            order = (lambda items: sorted(items, key=json.dumps)) if "stats by" in rec["request"]["form"]["query"] else list
            assert order(rows(got.text)) == order(rows(rec["response"])), name
            checked += 1
    assert checked >= 20


def test_traces_come_from_the_jaeger_proxy():
    recorded_traces = victoria("traces")["exchanges"]
    seen: list[httpx.Request] = []
    found = vtraces(answering(recorded_traces["search_error_traces"], seen)).search_error_traces(
        ["web-portal", "mes-api", "mes-db"], *HOUR, 20)
    assert [r.url.path for r in seen] == ["/api/datasources/proxy/uid/vt-uid/api/traces"] * 3   # Jaeger 的搜尋一次一個服務
    assert [dict(r.url.params) for r in seen] == [x["request"]["params"] for x in recorded_traces["search_error_traces"]]
    assert sorted(t["id"] for t in found) == ["11" * 16, "22" * 16]   # 兩個服務都搜到同樣兩條：各留一份；成功的那條沒有
    assert all(t["trace"]["spans"] and t["trace"]["processes"] for t in found)
    assert {t["id"]: t["start"] for t in found} == {"11" * 16: 1795220838000000, "22" * 16: 1795220840000000}   # 最早的 span 的開始時間（微秒）
    one = vtraces(answering(recorded_traces["get_trace"], seen)).get_trace("11" * 16)
    assert one["traceID"] == "11" * 16 and len(one["spans"]) == 4
    assert seen[-1].url.path == "/api/datasources/proxy/uid/vt-uid/api/traces/" + "11" * 16
    assert vtraces(answering(recorded_traces["get_trace_missing"], seen)).get_trace("ff" * 16) is None   # 404
    assert vtraces(answering(recorded_traces["search_no_such_service"], seen)).search_error_traces(["nope"], *HOUR, 20) == []


def test_fake_traces_answer_like_the_real_one():
    fake = FakeGrafana(shift=timedelta(0))
    g = VictoriaTraces(Grafana("http://grafana.test", "t", transport=fake.transport), "u")
    recorded_traces = victoria("traces")["exchanges"]
    real = lambda name: [t for x in recorded_traces[name] for t in json.loads(x["response"])["data"]]
    found = g.search_error_traces(["web-portal", "mes-api", "mes-db"], *HOUR, 20)
    assert sorted((t["trace"] for t in found), key=lambda t: t["traceID"]) == sorted(
        {t["traceID"]: t for t in real("search_error_traces")}.values(), key=lambda t: t["traceID"])
    assert g.search_error_traces(["nope"], *HOUR, 20) == []
    assert g.search_error_traces(["mes-api"], NOW - timedelta(minutes=5), NOW, 20) == []   # Trace 在時間窗之外
    assert g.get_trace("11" * 16) == real("get_trace")[0] and g.get_trace("33" * 16) == real("get_trace_ok")[0]
    assert g.get_trace("ff" * 16) is None


def test_annotation_is_created_once_then_patched(fake):
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    body = {"time": 1, "timeEnd": 2, "tags": ["aiops", "aiops-incident:abc"], "text": "x"}
    first = g.upsert_annotation(None, "aiops-incident:abc", body)
    again = g.upsert_annotation(None, "aiops-incident:abc", {**body, "text": "y"})  # id 沒存到：用標籤找回來
    third = g.upsert_annotation(first, "aiops-incident:abc", {**body, "text": "z"})
    assert first == again == third and len(fake.annotations) == 1 and fake.annotations[first]["text"] == "z"


def test_recorded_annotation_responses_have_the_fields_we_read():
    assert isinstance(recorded("annotation_create")["response"]["id"], int)
    assert recorded("annotation_list")["response"][0]["id"] == recorded("annotation_create")["response"]["id"]
    assert recorded("annotation_patch")["status"] == 200


def test_auth_failure_is_reported_and_transient_errors_are_retried(monkeypatch):
    monkeypatch.setattr("src.grafana.time.sleep", lambda s: None)
    now = datetime(2026, 11, 21, tzinfo=timezone.utc)
    search = lambda g: VictoriaLogs(g, "u").search_errors(["a"], now, now)
    with pytest.raises(GrafanaError, match="HTTP 401"):
        search(client(lambda r: httpx.Response(401, json={"message": "Unauthorized"})))
    attempts = []

    def flaky(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(503) if len(attempts) < 3 else httpx.Response(
            200, text=victoria("logs")["exchanges"]["search_errors"][0]["response"])

    assert len(search(client(flaky))) == 6 and len(attempts) == 3
    with pytest.raises(GrafanaError, match="重試後仍失敗"):
        search(client(lambda r: httpx.Response(503)))


def annotation_ids_by_tag(fake, tag):
    return [x["id"] for x in fake.handle(httpx.Request("GET", "http://x/api/annotations", params={"tags": tag})).json()]


def test_changed_tags_replace_the_annotation_because_grafana_keeps_finding_it_by_the_old_tags(fake):
    """Grafana 13.0.7 實測：PATCH／PUT 改了註記的標籤之後，用舊標籤查還是查得到（標籤索引沒更新）。
    直接 PATCH 的話，儀表板的「進行中」清單會一直列著已結案的事故。"""
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    opened = {"time": 1, "timeEnd": 2, "tags": ["aiops", "aiops-incident:abc", "P1", "open"], "text": "x"}
    first = g.upsert_annotation(None, "aiops-incident:abc", opened)
    closed = g.upsert_annotation(first, "aiops-incident:abc", {**opened, "tags": ["aiops", "aiops-incident:abc", "P1", "closed"]})
    assert closed != first and list(fake.annotations) == [closed]
    assert annotation_ids_by_tag(fake, "open") == [] and annotation_ids_by_tag(fake, "closed") == [closed]
    escalated = g.upsert_annotation(closed, "aiops-incident:abc", {**opened, "tags": ["aiops", "aiops-incident:abc", "P0", "closed"]})
    assert annotation_ids_by_tag(fake, "P1") == [] and annotation_ids_by_tag(fake, "P0") == [escalated]


def test_same_tags_are_patched_in_place_and_a_deleted_annotation_is_recreated(fake):
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    body = {"time": 1, "timeEnd": 2, "tags": ["aiops", "aiops-incident:abc", "open"], "text": "x"}
    first = g.upsert_annotation(None, "aiops-incident:abc", body)
    assert g.upsert_annotation(first, "aiops-incident:abc", {**body, "text": "更新了摘要", "timeEnd": 9}) == first
    assert fake.annotations[first]["text"] == "更新了摘要" and ("DELETE", f"/api/annotations/{first}") not in fake.calls
    del fake.annotations[first], fake.tag_index[first]              # 被人在 Grafana 上刪掉了（Grafana 對不存在的 id 回 403）
    again = g.upsert_annotation(first, "aiops-incident:abc", body)
    assert again != first and list(fake.annotations) == [again]
