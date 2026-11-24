"""Grafana 對接層：全部用 Grafana 13.0.7 的真實樣本（tests/fixtures/grafana13/）驗解析。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src.grafana import Grafana, GrafanaError, WebhookError, parse_webhook
from tests.conftest import recorded, replay, webhook


def client(handler) -> Grafana:
    return Grafana("http://grafana.test", "token", transport=httpx.MockTransport(handler))


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
    now = datetime(2026, 11, 21, tzinfo=timezone.utc)
    search = lambda g: g.search_errors("u", "logs", "traces", ["a"], now, now)
    with pytest.raises(GrafanaError, match="HTTP 401"):
        search(client(lambda r: httpx.Response(401, json={"message": "Unauthorized"})))
    attempts = []

    def flaky(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(503) if len(attempts) < 3 else replay("es_proxy_logs_traces")

    assert len(search(client(flaky))["logs"]) == 6 and len(attempts) == 3
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
