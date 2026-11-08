import httpx
import pytest

from aiops.infrastructure.errors import InfrastructureError
from aiops.notifiers.alertmanager import AlertmanagerNotifier
from aiops.notifiers.base import Notification

EVENTS = [
    {"entity": "svc-a", "metric": "latency", "ts": "2026-11-08T00:00:00", "score": 3, "attrs": {"dept": "payments"}},
    {"entity": "svc-b", "metric": "cpu", "ts": "2026-11-08T00:01:00", "score": 4, "attrs": {"dept": "platform"}},
    {"entity": "svc-c", "metric": "disk_io", "ts": "2026-11-08T05:00:00", "score": 1, "attrs": {"dept": "storage"}},
]
CONTEXT = {"topology": [["svc-a", "svc-b"]], "p0_entities": ["svc-c"]}


def _process(client, events=EVENTS, preset="it_otel", context=CONTEXT):
    r = client.post("/api/v1/alerts/process", json={"preset": preset, "events": events, "context": context})
    assert r.status_code == 200, r.text
    return {tuple(i["entities"]): i for i in r.json()["result"]}


def _silence(client, **kw):
    body = {"kind": "maintenance", "start": "2026-11-08T04:00:00", "end": "2026-11-08T06:00:00",
            "reason": "換硬碟", "created_by": "amy", **kw}
    r = client.post("/api/v1/alerts/silences", json=body)
    assert r.status_code == 200, r.text
    return r.json()["result"]


# ── 通知 payload ──


def test_payload_has_severity_and_one_notification_per_dept(client, sent):
    out = _process(client)
    assert out[("svc-c",)]["severity"] == "P0"  # 1 + p0_boost 10
    assert out[("svc-a", "svc-b")]["severity"] == "P1"  # 4 × 1.5
    assert out[("svc-a", "svc-b")]["notifications"] == 2  # payments、platform 各一則

    by_dept = {n.labels["dept"]: n for n in sent}
    assert set(by_dept) == {"payments", "platform", "storage"}
    assert by_dept["storage"].labels["severity"] == "P0"
    assert by_dept["payments"].labels["incident_uid"] == out[("svc-a", "svc-b")]["uid"]
    assert "incident_id" not in by_dept["payments"].labels  # 整數 id 不能拿去外面當識別
    assert by_dept["payments"].annotations["summary"].startswith("[P1] svc-a, svc-b")


def test_incident_uid_is_unique_across_database_resets(client, sent):
    """資料庫重建後整數 id 會從 1 重來，但送出去的 incident_uid 不能重複，否則 Alertmanager 會把不相干的事件群併成一組。"""
    from aiops.models import core

    first = _process(client)[("svc-c",)]
    core.reset_all()
    second = _process(client)[("svc-c",)]
    assert first["id"] == second["id"]  # 內部 id 撞號（預期）
    assert first["uid"] != second["uid"] and first["uid"].startswith("inc-")
    storage_uids = [n.labels["incident_uid"] for n in sent if n.labels["dept"] == "storage"]
    assert storage_uids == [first["uid"], second["uid"]]  # 兩次送出去的識別不同


def test_event_without_dept_goes_to_unassigned(client, sent):
    _process(client, events=[{"entity": "x", "metric": "cpu", "ts": "2026-11-08T00:00:00", "score": 2}],
             preset="metrics_only", context={})
    assert [n.labels["dept"] for n in sent] == ["unassigned"]


# ── 抑制 ──


def test_silence_window_suppresses_fully_covered_incident(client, sent):
    s = _silence(client, entity="svc-c")
    out = _process(client)
    assert out[("svc-c",)]["suppressed_by"] == f"silence_window：maintenance#{s['id']}"
    assert out[("svc-c",)]["notifications"] == 0
    assert "storage" not in {n.labels["dept"] for n in sent}


def test_partially_covered_incident_still_notifies(client, sent):
    _silence(client, entity="svc-a", start="2026-11-07T23:00:00", end="2026-11-08T01:00:00")
    out = _process(client)
    assert out[("svc-a", "svc-b")]["suppressed_by"] is None  # svc-b 不在靜音範圍


def test_dept_scoped_silence(client, sent):
    _silence(client, dept="storage")
    assert _process(client)[("svc-c",)]["suppressed_by"].startswith("silence_window")


def test_dedup_suppresses_repeat_run(client, sent):
    _process(client)
    first = len(sent)
    again = _process(client)
    assert all(i["suppressed_by"].startswith("dedup") for i in again.values())
    assert len(sent) == first


def test_same_root_cause_suppresses_upstream_symptom(client, sent):
    events = [  # 相隔 20 分鐘，超過 topology_chain 的 10 分鐘，不會被合併
        {"entity": "frontend", "metric": "cpu", "ts": "2026-11-08T00:00:00", "score": 2},
        {"entity": "db", "metric": "cpu", "ts": "2026-11-08T00:20:00", "score": 5},
    ]
    out = _process(client, events=events, context={"topology": [["frontend", "db"]]})
    assert out[("frontend",)]["suppressed_by"].startswith("same_root_cause")
    assert out[("db",)]["suppressed_by"] is None


def test_silence_requires_ordered_window(client):
    body = {"kind": "mute", "start": "2026-11-08T06:00:00", "end": "2026-11-08T04:00:00",
            "reason": "x", "created_by": "amy"}
    assert client.post("/api/v1/alerts/silences", json=body).status_code == 422


def test_list_silences(client):
    _silence(client)
    assert len(client.get("/api/v1/alerts/silences").json()["result"]) == 1


# ── 通知出口 ──


def test_alertmanager_body_and_auth(monkeypatch):
    calls = []

    def fake_post(url, json, headers, timeout):
        calls.append((url, json, headers))
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr("aiops.notifiers.alertmanager.httpx.post", fake_post)
    from datetime import datetime

    n = Notification({"severity": "P1", "dept": "payments"}, {"summary": "s"}, datetime(2026, 11, 8))
    AlertmanagerNotifier("http://am:9093/", token="t0k").send([n])

    url, body, headers = calls[0]
    assert url == "http://am:9093/api/v2/alerts"
    assert body == [{"labels": {"severity": "P1", "dept": "payments"}, "annotations": {"summary": "s"},
                     "startsAt": "2026-11-08T00:00:00Z"}]
    assert headers == {"Authorization": "Bearer t0k"}


def test_alertmanager_failure_is_infrastructure_error(monkeypatch):
    def boom(url, **_):
        raise httpx.ConnectError("refused", request=httpx.Request("POST", url))

    monkeypatch.setattr("aiops.notifiers.alertmanager.httpx.post", boom)
    from datetime import datetime

    with pytest.raises(InfrastructureError):
        AlertmanagerNotifier("http://am:9093").send([Notification({}, {}, datetime(2026, 11, 8))])


def test_missing_alertmanager_url_returns_503(client, monkeypatch):
    monkeypatch.setenv("AIOPS_NOTIFIER", "alertmanager")
    r = client.post("/api/v1/alerts/process", json={"preset": "metrics_only", "events": EVENTS[:1]})
    assert r.status_code == 503
