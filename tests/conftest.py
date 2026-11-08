import os

os.environ["AIOPS_DATABASE_URL"] = "sqlite://"  # 測試一律用記憶體 SQLite，必須在載入 app 之前設定
TEST_TOKEN = "test-token"
AUTH = {"Authorization": f"Bearer {TEST_TOKEN}"}

from datetime import datetime, timezone  # noqa: E402

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from aiops.app import create_app  # noqa: E402
from aiops.operators import load_all  # noqa: E402
from aiops.operators.notifiers.transport import LogTransport  # noqa: E402
from aiops.storage import db, models  # noqa: E402

load_all()
NOW = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def fresh_db(monkeypatch):
    """每個測試都是全新的資料庫與乾淨的通知紀錄。"""
    db.configure("sqlite://")
    models.reset_all()
    LogTransport.sent.clear()
    for var in ("AIOPS_ALERTMANAGER_URL", "AIOPS_ALERTMANAGER_TOKEN", "AIOPS_FALLBACK_WEBHOOK", "AIOPS_AUTH_DISABLED",
                "AIOPS_LLM_PROVIDER", "AIOPS_LLM_MODEL", "AIOPS_LLM_API_KEY", "AIOPS_LLM_BASE_URL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AIOPS_NOTIFIER", "log")
    monkeypatch.setenv("AIOPS_API_TOKEN", TEST_TOKEN)


@pytest.fixture
def client():
    return TestClient(create_app(), headers=AUTH)


@pytest.fixture
def sent():
    return LogTransport.sent


@pytest.fixture
def now():
    return NOW
