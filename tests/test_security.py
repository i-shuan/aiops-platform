"""API 認證：每一個 /api 端點都必須驗 token；驗證寫在 security/，用 Depends 掛在 router 上，api.py 不碰。"""
import re

import pytest
from fastapi.testclient import TestClient

from aiops.app import create_app


def _api_routes(app):
    for path, ops in app.openapi()["paths"].items():
        if path.startswith("/api/"):
            for method in ops:
                yield method.upper(), re.sub(r"\{[^}]+\}", "1", path)


@pytest.fixture
def anon(client):
    """沒帶 token 的 client（共用 client fixture 的 DB 與 AIOPS_API_TOKEN 設定）。"""
    return TestClient(client.app)


def test_every_api_route_rejects_missing_token(anon):
    routes = list(_api_routes(anon.app))
    assert len(routes) >= 15
    leaked = [(m, p) for m, p in routes if anon.request(m, p, json={}).status_code != 401]
    assert not leaked, f"這些端點沒帶 token 也進得去：{leaked}"


def test_wrong_token_is_rejected(anon):
    r = anon.get("/api/v1/alerts/silences", headers={"Authorization": "Bearer nope"})
    assert r.status_code == 401 and r.headers["www-authenticate"] == "Bearer"


def test_correct_token_passes(client):
    assert client.get("/api/v1/alerts/silences").status_code == 200


def test_rotation_accepts_any_listed_token(anon, monkeypatch):
    monkeypatch.setenv("AIOPS_API_TOKEN", "old-token, new-token")
    for t in ("old-token", "new-token"):
        assert anon.get("/api/v1/alerts/silences", headers={"Authorization": f"Bearer {t}"}).status_code == 200


def test_fails_closed_when_no_token_configured(anon, monkeypatch):
    monkeypatch.delenv("AIOPS_API_TOKEN")
    r = anon.get("/api/v1/alerts/silences", headers={"Authorization": "Bearer anything"})
    assert r.status_code == 401 and "沒有設定" in r.json()["detail"]


def test_auth_can_be_disabled_for_local_dev(anon, monkeypatch):
    monkeypatch.setenv("AIOPS_AUTH_DISABLED", "true")
    assert anon.get("/api/v1/alerts/silences").status_code == 200


def test_health_is_public(anon):
    assert anon.get("/health").status_code == 200


def test_new_domains_are_protected_automatically():
    """驗證掛在 init_routers 上：每個領域 router 都有 require_token，不靠各領域自己記得加。"""
    from aiops.security.auth import require_token

    app = create_app()
    unprotected = [
        r.path for r in app.routes
        if getattr(r, "path", "").startswith("/api/")
        and require_token not in [d.call for d in getattr(r, "dependant").dependencies]
    ]
    assert not unprotected, unprotected
