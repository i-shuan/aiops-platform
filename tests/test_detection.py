from fastapi.testclient import TestClient

from aiops.app import create_app

client = TestClient(create_app())

BODY = {
    "metric": {"source_type": "mock", "query": "cpu_usage"},
    "time_range": {"start": "2026-11-08T00:00:00", "end": "2026-11-08T02:00:00", "step_seconds": 60},
    "detector_name": "zscore",
}


def test_run_detection_finds_spike():
    r = client.post("/api/v1/detection/run", json=BODY)
    assert r.status_code == 200
    assert r.json()["result"]["anomaly_count"] >= 1


def test_unknown_detector_is_400():
    r = client.post("/api/v1/detection/run", json={**BODY, "detector_name": "nope"})
    assert r.status_code == 400
