def _propose(client, source="auto", threshold=2.5):
    r = client.post(
        "/api/v1/tuning/proposals",
        json={"source": source, "detector_name": "zscore", "params": {"threshold": threshold}, "proposer": "amy"},
    )
    assert r.status_code == 200, r.text
    return r.json()["result"]["id"]


def _backtest(client, pid, window):
    body = {"metric": {"source_type": "mock", "query": "cpu_usage"}, "time_range": window}
    return client.post(f"/api/v1/tuning/proposals/{pid}/backtest", json=body)


def _approve(client, pid):
    return client.post(f"/api/v1/tuning/proposals/{pid}/approve", json={"approver": "bob"})


def test_proposal_backtest_approve_then_detection_uses_it(client, window):
    pid = _propose(client)
    bt = _backtest(client, pid, window).json()["result"]
    assert bt["status"] == "backtested" and bt["backtest"]["points"] > 0

    approved = _approve(client, pid).json()["result"]
    assert approved["active_config"]["version"] == 1

    run = client.post(
        "/api/v1/detection/run", json={"metric": {"query": "cpu_usage"}, "time_range": window}
    ).json()["result"]
    assert run["params"] == {"threshold": 2.5}


def test_cannot_approve_without_backtest(client):
    assert _approve(client, _propose(client)).status_code == 409


def test_stale_proposal_cannot_overwrite_newer_config(client, window):
    auto = _propose(client, source="auto", threshold=2.0)
    manual = _propose(client, source="manual", threshold=4.0)
    _backtest(client, manual, window)
    assert _approve(client, manual).status_code == 200  # 人工先調好

    _backtest(client, auto, window)
    r = _approve(client, auto)  # 舊的自動提案不能蓋掉
    assert r.status_code == 409
    assert client.get(f"/api/v1/tuning/proposals/{auto}").json()["result"]["status"] == "stale"


def test_unknown_proposal_is_404(client):
    assert client.get("/api/v1/tuning/proposals/999").status_code == 404
