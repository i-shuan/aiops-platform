EVENTS = [
    {"entity": "svc-a", "metric": "latency", "ts": "2026-11-08T00:00:00", "score": 3},
    {"entity": "svc-a", "metric": "cpu", "ts": "2026-11-08T00:02:00", "score": 2},
    {"entity": "svc-b", "metric": "cpu", "ts": "2026-11-08T00:01:00", "score": 4},
    {"entity": "svc-c", "metric": "disk_io", "ts": "2026-11-08T05:00:00", "score": 1},
]
CONTEXT = {"topology": [["svc-a", "svc-b"]], "p0_entities": ["svc-c"]}


def _process(client, preset):
    r = client.post("/api/v1/alerts/process", json={"preset": preset, "events": EVENTS, "context": CONTEXT})
    assert r.status_code == 200, r.text
    return r.json()["result"]


def test_it_otel_pipeline_groups_by_entity_and_topology_then_scores(client):
    incidents = _process(client, "it_otel")
    assert [i["entities"] for i in incidents] == [["svc-c"], ["svc-a", "svc-b"]]
    assert incidents[0]["score"] == 11  # 1 + p0_boost 10
    assert incidents[1]["score"] == 6  # max 4 × golden_signals 1.5
    assert incidents[1]["tags"] == ["same_entity", "topology_chain"]


def test_metrics_only_pipeline_uses_fewer_parts(client):
    incidents = _process(client, "metrics_only")
    assert [i["entities"] for i in incidents] == [["svc-b"], ["svc-a"], ["svc-c"]]
    assert [i["score"] for i in incidents] == [4, 3, 1]


def test_rca_sections_follow_preset(client):
    chain = _process(client, "it_otel")[1]
    it = client.post("/api/v1/rca/generate", json={"incident_id": chain["id"], "preset": "it_otel"}).json()["result"]
    assert [s["key"] for s in it["sections"]] == ["summary", "timeline", "affected_entities", "golden_signals"]

    basic = client.post("/api/v1/rca/generate", json={"incident_id": chain["id"], "preset": "metrics_only"}).json()
    assert [s["key"] for s in basic["result"]["sections"]] == ["summary", "timeline", "affected_entities"]


def test_rca_skips_sections_without_data(client):
    lone = _process(client, "it_otel")[0]  # svc-c 只有 disk_io，沒有黃金訊號
    r = client.post("/api/v1/rca/generate", json={"incident_id": lone["id"], "preset": "it_otel"}).json()["result"]
    assert r["skipped"] == ["golden_signals"]


def test_rca_unknown_incident_is_404(client):
    assert client.post("/api/v1/rca/generate", json={"incident_id": 999, "preset": "it_otel"}).status_code == 404
