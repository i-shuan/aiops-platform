"""API 只是薄薄一層：把請求交給 runner，再把結果與錯誤翻成 HTTP。這裡驗證端到端的 HTTP 行為。"""
from datetime import timedelta

NOW = "2026-11-08T12:00:00Z"


def test_operators_listing_includes_core_kinds(client):
    names = client.get("/api/v1/operators").json()["result"]
    for expected in ("detector.zscore", "detector.mad", "alert.suppress", "enrichment.llm_summary", "notifier.log"):
        assert expected in names or any(n.startswith(expected.split(".")[0] + ".") for n in names)


def test_run_workflow_creates_one_incident_and_rerun_is_idempotent(client, sent):
    first = client.post("/api/v1/runs", json={"workflow": "metrics_only", "now": NOW}).json()["result"]
    assert len(first["incidents"]) == 1 and not first.get("errors")
    inc = first["incidents"][0]
    assert inc["uid"].startswith("inc-") and not inc["suppressed_by"]
    n_sent = len(sent)
    assert n_sent >= 1

    again = client.post("/api/v1/runs", json={"workflow": "metrics_only", "now": NOW}).json()["result"]
    assert all(i["uid"] == inc["uid"] for i in again.get("incidents", []))
    # 同一批異常不會重新開單；第二輪只多一則「恢復」通知（同一個 incident_uid、帶 ends_at）
    assert len(sent) == n_sent + 1
    assert sent[-1].labels["incident_uid"] == inc["uid"] and sent[-1].ends_at is not None
    third = client.post("/api/v1/runs", json={"workflow": "metrics_only", "now": NOW}).json()["result"]
    assert not third.get("incidents") and len(sent) == n_sent + 1  # 已恢復，不再重複通知


def test_unknown_workflow_is_a_client_error(client):
    r = client.post("/api/v1/runs", json={"workflow": "no_such_workflow"})
    assert 400 <= r.status_code < 500


def test_silence_then_run_is_suppressed(client, sent):
    body = {"kind": "maintenance", "start": "2026-11-08T00:00:00Z", "end": "2026-11-09T00:00:00Z",
            "reason": "維護", "created_by": "amy"}
    assert client.post("/api/v1/alerts/silences", json=body).status_code == 200
    out = client.post("/api/v1/runs", json={"workflow": "metrics_only", "now": NOW}).json()["result"]
    assert out["incidents"] and all(i["suppressed_by"] for i in out["incidents"])
    assert sent == []


def test_incident_get_and_resolve_and_rca(client):
    out = client.post("/api/v1/runs", json={"workflow": "metrics_only", "now": NOW}).json()["result"]
    inc = out["incidents"][0]
    got = client.get(f"/api/v1/alerts/incidents/{inc['id']}")
    assert got.status_code == 200
    assert client.get("/api/v1/alerts/incidents/99999").status_code == 404

    rca = client.post("/api/v1/rca", json={"incident_id": inc["id"]})
    assert rca.status_code == 200 and rca.json()["result"]["sections"]

    res = client.post(f"/api/v1/alerts/incidents/{inc['uid']}/resolve")
    assert res.status_code == 200
    assert client.post("/api/v1/alerts/incidents/inc-nope/resolve").status_code == 404


def test_bad_request_bodies_are_rejected(client):
    assert client.post("/api/v1/runs", json={}).status_code == 422
    bad = {"kind": "maintenance", "start": "2026-11-09T00:00:00Z", "end": "2026-11-08T00:00:00Z",
           "reason": "x", "created_by": "a"}
    assert client.post("/api/v1/alerts/silences", json=bad).status_code == 422
