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


@pytest.fixture(scope="session")
def _workflow_dir_with_samples(tmp_path_factory):
    """出貨的 aiops/pipelines/ 加上 scripts/pipelines/ 的偵測管線樣本（sample_*），放在一個暫存目錄。
    出貨的使用者管線只吃外部告警、不跑偵測器（偵測演算法只當平台自己的看門狗）；偵測機制的端到端測試用 sample_* 這幾條，
    生產程式不變，靠這裡把 runner.WORKFLOW_DIR 指到暫存目錄（跟回測、混沌腳本同一個 scripts/sample_pipelines.py）。"""
    from scripts.sample_pipelines import merged_workflow_dir

    return merged_workflow_dir(tmp_path_factory.mktemp("workflows"))


PRODUCTION_DETECTION_SCOPES = None  # 出貨的偵測入口政策（載入時記下來，給政策測試對照與還原）


@pytest.fixture(autouse=True)
def sample_pipelines(monkeypatch, _workflow_dir_with_samples):
    """樣本放在 pipelines/samples/，偵測機制的測試要能拿它跑任意資料源（mock、elasticsearch…）：偵測入口政策多放一個
    samples 資料夾、不限資料源。出貨的政策（只有 watchdog、只准 platform）由 production_detection_scopes 還原後測。"""
    from types import MappingProxyType

    from aiops.interfaces import runner

    global PRODUCTION_DETECTION_SCOPES
    if PRODUCTION_DETECTION_SCOPES is None:
        PRODUCTION_DETECTION_SCOPES = runner.DETECTION_SCOPES
    monkeypatch.setattr(runner, "WORKFLOW_DIR", _workflow_dir_with_samples)
    monkeypatch.setattr(runner, "DETECTION_SCOPES", MappingProxyType({**PRODUCTION_DETECTION_SCOPES, "samples": None}))
    return _workflow_dir_with_samples


@pytest.fixture
def production_detection_scopes(monkeypatch):
    """偵測入口政策恢復成出貨的樣子（樣本不能當入口）。"""
    from aiops.interfaces import runner

    monkeypatch.setattr(runner, "DETECTION_SCOPES", PRODUCTION_DETECTION_SCOPES)
    return PRODUCTION_DETECTION_SCOPES


@pytest.fixture(autouse=True)
def retry_waits(monkeypatch):
    """資料源重試（sources/_http）的等待不要真的睡：改成記下每次要等幾秒，測試可以檢查重試了幾次、間隔對不對。"""
    from aiops.sources import _http

    waits: list[float] = []
    monkeypatch.setattr(_http.time, "sleep", waits.append)
    return waits


@pytest.fixture
def db_retry_waits(monkeypatch):
    """資料庫寫入交易的重試（storage/db.run_in_transaction）不要真的睡：記下每次要等幾秒。"""
    from aiops.storage import db

    waits: list[float] = []
    monkeypatch.setattr(db.time, "sleep", waits.append)
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
