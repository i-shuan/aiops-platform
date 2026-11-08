"""資料真的存進資料庫：換一個連線（模擬服務重啟）後還在；資料庫連不上時回 503。"""
import pytest

from aiops.app import create_app
from aiops.models import core, db

SILENCE = {"kind": "maintenance", "start": "2026-11-08T04:00:00+08:00", "end": "2026-11-08T06:00:00+08:00",
           "entity": "svc-a", "reason": "換硬碟", "created_by": "amy"}


def TestClient(app):  # noqa: N802  帶上測試 token
    from fastapi.testclient import TestClient as _Client

    from tests.conftest import AUTH

    return _Client(app, headers=AUTH)


@pytest.fixture
def file_db(tmp_path, monkeypatch):
    monkeypatch.setenv("AIOPS_API_TOKEN", "test-token")
    url = f"sqlite:///{(tmp_path / 'aiops.db').as_posix()}"
    db.configure(url)
    core.reset_all()
    yield url
    db.configure("sqlite://")


def test_data_survives_restart(file_db):
    TestClient(create_app()).post("/api/v1/alerts/silences", json=SILENCE)

    db.configure(file_db)  # 丟掉舊連線，重新連同一個檔案 = 服務重啟
    rows = TestClient(create_app()).get("/api/v1/alerts/silences").json()["result"]
    assert [r["entity"] for r in rows] == ["svc-a"]


def test_times_are_stored_as_utc(file_db):
    TestClient(create_app()).post("/api/v1/alerts/silences", json=SILENCE)
    row = TestClient(create_app()).get("/api/v1/alerts/silences").json()["result"][0]
    assert row["start"] == "2026-11-07T20:00:00"  # +08:00 -> UTC


def test_startup_waits_for_database_then_gives_up(monkeypatch):
    """資料庫比平台晚就緒時，啟動要等它；等超過時限才放棄。"""
    from aiops import initialization
    from aiops.infrastructure.errors import InfrastructureError
    from aiops.models import db as db_module

    calls, now = [], [0.0]

    def flaky_create_all():
        calls.append(1)
        if len(calls) < 3:
            raise InfrastructureError("connection refused")

    monkeypatch.setattr(db_module, "create_all", flaky_create_all)
    monkeypatch.setenv("AIOPS_DB_STARTUP_TIMEOUT", "60")
    initialization.init_db(sleep=lambda s: now.__setitem__(0, now[0] + s), clock=lambda: now[0])
    assert len(calls) == 3  # 前兩次失敗、第三次成功

    monkeypatch.setenv("AIOPS_DB_STARTUP_TIMEOUT", "5")
    monkeypatch.setattr(db_module, "create_all", lambda: (_ for _ in ()).throw(InfrastructureError("down")))
    now[0] = 0.0
    with pytest.raises(InfrastructureError):
        initialization.init_db(sleep=lambda s: now.__setitem__(0, now[0] + s), clock=lambda: now[0])


def test_database_unavailable_returns_503(client, tmp_path):
    db.configure(f"sqlite:///{(tmp_path / 'no-such-dir' / 'x.db').as_posix()}")
    try:
        r = client.get("/api/v1/alerts/silences")
        assert r.status_code == 503
    finally:
        db.configure("sqlite://")
