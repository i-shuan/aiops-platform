import pytest
from fastapi.testclient import TestClient

from aiops.app import create_app
from aiops.models import core


@pytest.fixture
def client():
    core.reset_all()
    return TestClient(create_app())


@pytest.fixture
def window():
    return {"start": "2026-11-08T00:00:00", "end": "2026-11-08T02:00:00", "step_seconds": 60}
