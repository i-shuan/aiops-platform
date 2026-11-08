import os

os.environ["AIOPS_DATABASE_URL"] = "sqlite://"  # 測試一律用記憶體 SQLite，必須在載入 app 之前設定
TEST_TOKEN = "test-token"
AUTH = {"Authorization": f"Bearer {TEST_TOKEN}"}

from datetime import datetime, timezone  # noqa: E402

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from aiops.interfaces.app import create_app  # noqa: E402
from aiops.interfaces.runner import load_all  # noqa: E402
from aiops.stages.notification.transport import LogTransport  # noqa: E402
from aiops.storage import db, models  # noqa: E402

load_all()
NOW = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def fresh_db(monkeypatch):
    """每個測試都是全新的資料庫與乾淨的通知紀錄。

    通知紀錄由測試這邊錄：把 LogTransport.send 包一層，照常寫日誌、另外收進這個測試自己的 list。
    生產代碼的 LogTransport 不保留任何東西（類別層級的 list 在常駐進程會無限長大，CLAUDE.md 第 100 節第 3 條）。"""
    db.configure("sqlite://")
    models.reset_all()
    recorded: list = []
    original_send = LogTransport.send

    def record(self, notifications):
        original_send(self, notifications)
        recorded.extend(notifications)

    monkeypatch.setattr(LogTransport, "send", record)
    for var in ("AIOPS_ALERTMANAGER_URL", "AIOPS_ALERTMANAGER_TOKEN", "AIOPS_FALLBACK_WEBHOOK", "AIOPS_AUTH_DISABLED",
                "AIOPS_LLM_PROVIDER", "AIOPS_LLM_MODEL", "AIOPS_LLM_API_KEY", "AIOPS_LLM_BASE_URL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AIOPS_NOTIFIER", "log")
    monkeypatch.setenv("AIOPS_API_TOKEN", TEST_TOKEN)
    return recorded


@pytest.fixture(autouse=True)
def retry_waits(monkeypatch):
    """資料源重試（sources/_http）的等待不要真的睡：改成記下每次要等幾秒，測試可以檢查重試了幾次、間隔對不對。"""
    from aiops.sources import _http

    waits: list[float] = []
    monkeypatch.setattr(_http.time, "sleep", waits.append)
    return waits


@pytest.fixture
def client():
    return TestClient(create_app(), headers=AUTH)


@pytest.fixture
def sent(fresh_db):
    """這個測試裡 notifier.log／notifier.auto(log) 送出的通知。"""
    return fresh_db


@pytest.fixture
def now():
    return NOW
