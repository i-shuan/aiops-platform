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


LOT_STEPS = [
    {"lot_id": "L1", "station": "etch", "equipment": "ETCH-01", "start": "2026-11-08T01:00:00", "end": "2026-11-08T01:30:00"},
    {"lot_id": "L1", "station": "litho", "equipment": "LITHO-02", "start": "2026-11-08T00:00:00", "end": "2026-11-08T00:45:00"},
    {"lot_id": "L2", "station": "litho", "equipment": "LITHO-02", "start": "2026-11-08T00:10:00", "end": "2026-11-08T00:50:00"},
    {"lot_id": "L2", "station": "etch", "equipment": "ETCH-01", "start": "2026-11-08T00:40:00", "end": "2026-11-08T01:20:00"},
]


def test_lot_paths_are_ordered_and_edges_counted(client):
    r = client.post("/api/v1/ingestion/lot-paths/merge", json={"steps": LOT_STEPS}).json()["result"]
    l1, l2 = r["lots"]
    assert l1["path"] == ["LITHO-02", "ETCH-01"]  # 依時間排序，不是依輸入順序
    assert l1["total_minutes"] == 90 and l1["overlapping_steps"] == 0
    assert l2["overlapping_steps"] == 1  # 前一站沒結束下一站就開始
    assert r["edges"] == [{"from": "LITHO-02", "to": "ETCH-01", "lots": 2}]
    assert r["topology"] == [["LITHO-02", "ETCH-01"]]


def test_lot_step_with_end_before_start_is_rejected(client):
    bad = {**LOT_STEPS[0], "end": "2026-11-08T00:00:00"}
    assert client.post("/api/v1/ingestion/lot-paths/merge", json={"steps": [bad]}).status_code == 422


def _ingest(client, ids, pipeline="fab"):
    anomalies = [{"external_id": i, "entity": "ETCH-01", "metric": "rf_power", "ts": "2026-11-08T01:00:00",
                  "score": 5, "attrs": {"dept": "fab-etch"}} for i in ids]
    r = client.post("/api/v1/ingestion/anomalies", json={"pipeline": pipeline, "source": "fdc", "anomalies": anomalies})
    assert r.status_code == 200, r.text
    return r.json()["result"]


def test_ingest_is_idempotent_per_external_id(client):
    assert _ingest(client, ["a1", "a2"]) | {} == {"pipeline": "fab", "source": "fdc", "accepted": 2, "duplicates": 0}
    assert _ingest(client, ["a2", "a3"])["accepted"] == 1


def test_duplicate_ids_in_one_batch_are_rejected(client):
    r = client.post("/api/v1/ingestion/anomalies", json={"pipeline": "fab", "source": "fdc", "anomalies": [
        {"external_id": "x", "entity": "e", "metric": "m", "ts": "2026-11-08T00:00:00", "score": 1}] * 2})
    assert r.status_code == 400


def test_external_anomalies_flow_into_alerts(client, sent):
    _ingest(client, ["a1"])
    body = {"pipeline": "fab", "preset": "factory"}
    first = client.post("/api/v1/alerts/process-pending", json=body).json()["result"]
    assert (first["external"], first["detected"]) == (1, 0)
    assert sent[0].labels["dept"] == "fab-etch"
    assert client.post("/api/v1/alerts/process-pending", json=body).json()["result"]["processed_anomalies"] == 0


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
