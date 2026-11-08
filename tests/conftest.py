import os

os.environ["AIOPS_DATABASE_URL"] = "sqlite://"  # 測試一律用記憶體 SQLite，必須在載入 app 之前設定

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from aiops.app import create_app  # noqa: E402
from aiops.models import core, db  # noqa: E402
from aiops.notifiers.log import LogNotifier  # noqa: E402


@pytest.fixture
def client(monkeypatch):
    db.configure("sqlite://")
    core.reset_all()
    LogNotifier.sent.clear()
    for var in ("AIOPS_NOTIFIER", "AIOPS_ALERTMANAGER_URL", "AIOPS_ALERTMANAGER_TOKEN", "AIOPS_FALLBACK_WEBHOOK"):
        monkeypatch.delenv(var, raising=False)
    return TestClient(create_app())


@pytest.fixture
def sent():
    return LogNotifier.sent


@pytest.fixture
def window():
    return {"start": "2026-11-08T00:00:00", "end": "2026-11-08T02:00:00", "step_seconds": 60}
