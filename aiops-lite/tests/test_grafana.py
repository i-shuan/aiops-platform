"""Grafana 對接層：全部用 Grafana 13.0.7 的真實回應樣本（tests/fixtures/grafana13/）驗解析與防呆。"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src.grafana import MAX_POINTS, Grafana, GrafanaError, QueryError, Rule, parse_frames
from tests.conftest import recorded, replay

RULE = Rule("uid1", "AMHS transfer duration", "amhs-mcs", "P2", "ds1", {"rawSql": "select 1"})


def window(name: str) -> tuple[int, int]:
    req = recorded(name)["request"]["json"]
    return int(req["from"]), int(req["to"])


def client(handler) -> Grafana:
    return Grafana("http://grafana.test", "token", transport=httpx.MockTransport(handler))


def test_only_rules_labelled_aiops_detect_are_managed():
    rules, problems = client(lambda r: replay("alert_rules")).list_rules()
    assert [(r.title, r.service, r.severity) for r in rules] == [
        ("MQ queue depth", "mq-broker", "P1"), ("CIM T3 timeout", "cim-eap", "P2"),
        ("AMHS transfer duration", "amhs-mcs", "P2")]
    assert problems == []
    assert "expr" in rules[0].model and "rawSql" in rules[2].model  # 重放的是資料源查詢，不是 reduce／threshold


def test_rule_without_service_is_skipped_and_bad_severity_is_reported():
    raw = copy.deepcopy(recorded("alert_rules")["response"])
    del raw[0]["labels"]["service"]
    raw[1]["labels"]["aiops_severity"] = "critical"
    raw[2]["isPaused"] = True
    rules, problems = client(lambda r: httpx.Response(200, json=raw)).list_rules()
    assert [(r.title, r.severity) for r in rules] == [("CIM T3 timeout", "P3")]
    assert len(problems) == 2 and "沒有 service 標籤" in problems[0] and "不是 P0～P3" in problems[1]


def test_query_hash_changes_when_the_query_changes():
    other = Rule("uid1", "AMHS transfer duration", "amhs-mcs", "P2", "ds1", {"rawSql": "select 2"})
    assert RULE.query_hash != other.query_hash
    assert RULE.query_hash == Rule("uid1", "renamed", "amhs-mcs", "P0", "ds1", {"rawSql": "select 1"}).query_hash


def test_promql_response_becomes_a_series():
    start, end = window("query_promql")
    df = parse_frames(recorded("query_promql")["response"], RULE, start, end, 60000)
    assert len(df) == 61 and df["ts"].is_monotonic_increasing
    assert df["value"].iloc[0] == 46 and df["value"].max() > 300  # 樣本最後 15 分鐘是灌進去的積壓


def test_sql_time_plus_single_value_is_accepted():
    start, end = window("query_sql_single_value")
    df = parse_frames(recorded("query_sql_single_value")["response"], RULE, start, end, 60000)
    assert len(df) == 58 and df["value"].between(20, 120).all()


def test_sql_with_two_value_columns_is_rejected_not_silently_truncated():
    start, end = window("query_sql_multi_value")
    with pytest.raises(QueryError, match="2 個數值欄.*transfer_duration.*pending_jobs"):
        parse_frames(recorded("query_sql_multi_value")["response"], RULE, start, end, 60000)


def test_sql_long_format_is_rejected():
    start, end = window("query_sql_long_format")
    with pytest.raises(QueryError, match="長格式"):
        parse_frames(recorded("query_sql_long_format")["response"], RULE, start, end, 60000)


def test_sql_without_timefilter_is_rejected():
    start, end = window("query_sql_no_timefilter")  # 請求 15 分鐘，資料源回了整張表（3 小時）
    with pytest.raises(QueryError, match=r"\$__timeFilter"):
        parse_frames(recorded("query_sql_no_timefilter")["response"], RULE, start, end, 60000)


def test_datasource_rejecting_the_query_is_a_rule_problem_not_an_outage():
    g = client(lambda r: replay("query_promql_bad"))  # Grafana 回 HTTP 400，錯誤在 results.A.error
    now = datetime(2026, 11, 21, tzinfo=timezone.utc)
    with pytest.raises(QueryError, match="parse error"):
        g.query_series(RULE, now - timedelta(hours=1), now, 60)


def test_multiple_series_are_rejected():
    raw = copy.deepcopy(recorded("query_promql")["response"])
    raw["results"]["A"]["frames"].append(copy.deepcopy(raw["results"]["A"]["frames"][0]))
    start, end = window("query_promql")
    with pytest.raises(QueryError, match="2 條序列"):
        parse_frames(raw, RULE, start, end, 60000)


def test_no_data_is_an_empty_series_not_an_error():
    raw = {"results": {"A": {"status": 200, "frames": [{"schema": {"fields": []}, "data": {"values": []}}]}}}
    assert parse_frames(raw, RULE, 0, 60000, 60000).empty


def test_training_window_is_fetched_in_chunks_and_instant_queries_become_range(fake):
    fake.series[("ds1", "up")] = lambda ts: ts * 0 + 1.0
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    rule = Rule("u", "t", "s", "P1", "ds1", {"expr": "up", "instant": True, "range": False, "intervalMs": 1000,
                                             "maxDataPoints": 43200})
    end = datetime(2026, 11, 21, tzinfo=timezone.utc)
    df = g.query_series(rule, end - timedelta(days=14), end, 60)
    assert len(fake.queries) == 5  # 20,160 點 / 5,000
    assert len(df) == 14 * 1440 + 1 and not df["ts"].duplicated().any()
    for body in fake.queries:
        q = body["queries"][0]
        assert (int(body["to"]) - int(body["from"])) / 60000 <= MAX_POINTS
        assert q["instant"] is False and q["range"] is True and q["intervalMs"] == 60000
        assert q["maxDataPoints"] == MAX_POINTS


def test_es_proxy_returns_error_logs_and_error_spans():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"], seen["body"] = request.url.path, request.content.decode()
        return replay("es_proxy_logs_traces")

    now = datetime(2026, 11, 21, 0, 36, tzinfo=timezone.utc)
    found = client(handler).search_errors("es-uid", "factory-logs", "factory-traces", ["cim-eap", "amhs-mcs"],
                                          now - timedelta(hours=1), now)
    assert seen["path"] == "/api/datasources/proxy/uid/es-uid/_msearch"
    lines = [json.loads(line) for line in seen["body"].splitlines()]
    assert [lines[0]["index"], lines[2]["index"]] == ["factory-logs", "factory-traces"]
    assert len(found["logs"]) == 6 and len(found["traces"]) == 2
    assert found["logs"][1]["eqp_id"] == "EQP-ETCH-02" and found["traces"][0]["error"]["message"]


def test_es_error_inside_msearch_is_reported():
    body = {"responses": [{"error": {"type": "index_not_found_exception"}, "status": 404}]}
    now = datetime(2026, 11, 21, tzinfo=timezone.utc)
    with pytest.raises(GrafanaError, match="index_not_found"):
        client(lambda r: httpx.Response(200, json=body)).search_errors("u", "logs", None, ["a"], now, now)


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
    with pytest.raises(GrafanaError, match="HTTP 401"):
        client(lambda r: httpx.Response(401, json={"message": "Unauthorized"})).list_rules()
    attempts = []

    def flaky(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(503) if len(attempts) < 3 else replay("alert_rules")

    assert len(client(flaky).list_rules()[0]) == 3 and len(attempts) == 3
    with pytest.raises(GrafanaError, match="重試後仍失敗"):
        client(lambda r: httpx.Response(503)).list_rules()
