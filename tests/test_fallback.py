from aiops.domains.alerts.dao import IncidentDAO
from aiops.infrastructure import fallback
from aiops.infrastructure.errors import InfrastructureError


def test_db_down_returns_503_and_hits_webhook(client, monkeypatch):
    def _db_down(*_args, **_kwargs):
        raise InfrastructureError("postgres 連不上")

    sent = []
    monkeypatch.setattr(IncidentDAO, "create", staticmethod(_db_down))
    monkeypatch.setenv("AIOPS_FALLBACK_WEBHOOK", "http://hooks.test/aiops")
    monkeypatch.setattr(fallback.httpx, "post", lambda url, json, timeout: sent.append((url, json)))

    event = {"entity": "svc-a", "metric": "latency", "ts": "2026-11-08T00:00:00", "score": 3}
    r = client.post("/api/v1/alerts/process", json={"preset": "metrics_only", "events": [event]})

    assert r.status_code == 503
    assert sent[0][0] == "http://hooks.test/aiops"
    assert sent[0][1]["path"] == "/api/v1/alerts/process"
    assert sent[0][1]["message"] == "postgres 連不上"
