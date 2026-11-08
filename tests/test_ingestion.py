def _submit(client, fields):
    r = client.post(
        "/api/v1/ingestion/mappings", json={"source_type": "factory_es", "fields": fields, "submitter": "amy"}
    )
    assert r.status_code == 200
    return r.json()["result"]["id"]


def test_mapping_approval_supersedes_previous(client):
    first = _submit(client, {"@timestamp": "ts", "cpu": "value"})
    assert client.post(f"/api/v1/ingestion/mappings/{first}/approve", json={"approver": "bob"}).status_code == 200
    second = _submit(client, {"@timestamp": "ts", "cpu.pct": "value"})
    client.post(f"/api/v1/ingestion/mappings/{second}/approve", json={"approver": "bob"})

    current = client.get("/api/v1/ingestion/mappings/approved/factory_es").json()["result"]
    assert current["id"] == second

    again = client.post(f"/api/v1/ingestion/mappings/{first}/approve", json={"approver": "bob"})
    assert again.status_code == 409  # 已 superseded


def test_no_approved_mapping_is_404(client):
    _submit(client, {"a": "ts"})
    assert client.get("/api/v1/ingestion/mappings/approved/factory_es").status_code == 404


def test_trace_merge_outputs_standard_series(client):
    spans = [
        {"trace_id": "t1", "service": "cart", "start": "2026-11-08T00:00:10", "duration_ms": 100},
        {"trace_id": "t2", "service": "cart", "start": "2026-11-08T00:00:40", "duration_ms": 300, "error": True},
        {"trace_id": "t3", "service": "cart", "start": "2026-11-08T00:01:05", "duration_ms": 50},
    ]
    rows = client.post("/api/v1/ingestion/traces/merge", json={"spans": spans}).json()["result"]
    assert [(r["ts"], r["count"], r["error_rate"]) for r in rows] == [
        ("2026-11-08T00:00:00", 2, 0.5),
        ("2026-11-08T00:01:00", 1, 0.0),
    ]
