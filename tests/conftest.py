import pytest
from fastapi.testclient import TestClient

from aiops.app import create_app
from aiops.models import core
from aiops.notifiers.log import LogNotifier


@pytest.fixture
def client(monkeypatch):
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
