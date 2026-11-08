from aiops.engine.errors import InfrastructureError
from aiops.interfaces.bootstrap import fallback
from aiops.storage.repository import Silences


def test_db_down_returns_503_and_hits_webhook(client, monkeypatch):
    def _db_down(*_args, **_kwargs):
        raise InfrastructureError("postgres 連不上")

    sent = []
    monkeypatch.setattr(Silences, "list_all", staticmethod(_db_down))
    monkeypatch.setenv("AIOPS_FALLBACK_WEBHOOK", "http://hooks.test/aiops")
    monkeypatch.setattr(fallback.httpx, "post", lambda url, json, timeout: sent.append((url, json)))

    r = client.get("/api/v1/alerts/silences")

    assert r.status_code == 503
    assert sent[0][0] == "http://hooks.test/aiops"
    assert sent[0][1]["path"] == "/api/v1/alerts/silences"
    assert sent[0][1]["message"] == "postgres 連不上"


def test_webhook_failure_never_hides_the_original_503(client, monkeypatch):
    import httpx

    monkeypatch.setattr(Silences, "list_all", staticmethod(lambda: (_ for _ in ()).throw(InfrastructureError("down"))))
    monkeypatch.setenv("AIOPS_FALLBACK_WEBHOOK", "http://hooks.test/aiops")
    monkeypatch.setattr(fallback.httpx, "post", lambda *a, **k: (_ for _ in ()).throw(httpx.ConnectError("x")))
    assert client.get("/api/v1/alerts/silences").status_code == 503
