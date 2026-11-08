def _body(window, **kw):
    return {"metric": {"source_type": "mock", "query": "cpu_usage"}, "time_range": window, **kw}


def test_run_detection_finds_spike(client, window):
    r = client.post("/api/v1/detection/run", json=_body(window, detector_name="zscore"))
    assert r.status_code == 200
    assert r.json()["result"]["anomaly_count"] >= 1


def test_shift_detector_is_pluggable(client, window):
    r = client.post("/api/v1/detection/run", json=_body(window, detector_name="shift"))
    assert r.status_code == 200
    assert r.json()["result"]["detector_name"] == "shift"


def test_train_window_is_separate(client, window):
    train = {"start": "2026-11-07T00:00:00", "end": "2026-11-07T02:00:00", "step_seconds": 60}
    r = client.post("/api/v1/detection/run", json=_body(window, train_time_range=train))
    assert r.status_code == 200


def test_unknown_detector_is_400(client, window):
    r = client.post("/api/v1/detection/run", json=_body(window, detector_name="nope"))
    assert r.status_code == 400


def test_unknown_preset_is_400(client, window):
    r = client.post("/api/v1/detection/run", json=_body(window, preset="nope"))
    assert r.status_code == 400


def test_time_range_must_be_ordered(client, window):
    r = client.post("/api/v1/detection/run", json=_body({**window, "start": window["end"]}))
    assert r.status_code == 422


def test_list_detectors(client):
    assert {"zscore", "shift"} <= set(client.get("/api/v1/detection/detectors").json()["result"])
