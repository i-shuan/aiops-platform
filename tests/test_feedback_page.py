"""回饋入口：告警卡片的 feedback_url（連到平台的回饋頁）與平台的 /feedback 靜態頁。

守則（CLAUDE.md 周邊元件守則）：表單在自研平台，Grafana 只放連結；頁面本身不含資料與機密，
送出走既有的 /api/v1/tuning/feedback，照樣要 token。
"""
import re
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from aiops.engine.context import PipelineContext
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent
from aiops.stages.notification.links import PUBLIC_URL_ENV, feedback_url

T0 = datetime(2026, 11, 3, 14, 50, tzinfo=timezone.utc)


def _cards(**params):
    ctx = PipelineContext(pipeline="it_otel", now=T0 + timedelta(hours=1))
    ctx.anomalies = [AnomalyEvent("cart-api", "latency", T0 + timedelta(minutes=m), 11.0, {}) for m in (0, 30)]
    ctx = OperatorRegistry.create("alert.same_entity", {"window_minutes": 60}).execute(ctx)
    return OperatorRegistry.create("alert.notify_payload", params).execute(ctx)


def test_without_public_url_the_card_has_no_feedback_link(monkeypatch):
    monkeypatch.delenv(PUBLIC_URL_ENV, raising=False)
    assert feedback_url("inc-1") is None
    [card] = _cards().notifications
    assert "feedback_url" not in card.annotations


def test_card_links_to_the_platform_feedback_page_for_this_incident(monkeypatch):
    monkeypatch.setenv(PUBLIC_URL_ENV, "https://aiops.example.internal/")
    ctx = _cards()
    [card] = ctx.notifications
    uid = ctx.incidents[0].uid
    assert card.annotations["feedback_url"] == f"https://aiops.example.internal/feedback?incident_uid={uid}"
    assert "feedback_url" not in card.labels  # 連結放註解：標籤是告警的身分，不能因為網址變了就變成另一張告警


def test_incident_uid_is_url_encoded():
    import os
    os.environ[PUBLIC_URL_ENV] = "http://p"
    try:
        assert feedback_url("inc a&b=1") == "http://p/feedback?incident_uid=inc+a%26b%3D1"
    finally:
        del os.environ[PUBLIC_URL_ENV]


def test_feedback_page_is_public_static_html_without_secrets(client):
    anon = TestClient(client.app)
    r = anon.get("/feedback?incident_uid=inc-123")
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/html")
    page = r.text
    assert "/api/v1/tuning/feedback" in page and 'name="verdict"' in page
    for value in ("true_positive", "false_positive", "missed"):
        assert f'value="{value}"' in page
    assert "inc-123" not in page                 # 伺服器不把查詢參數寫進 HTML（由頁面自己讀，不會有注入）
    assert "test-token" not in page and "Bearer \" + token" in page  # token 由使用者自己填，頁面裡沒有任何金鑰
    assert not re.search(r"https?://", page)     # 不載入任何外部資源
    assert "innerHTML" not in page               # 回應一律用 textContent 顯示


def test_submitting_feedback_still_needs_a_token(client):
    anon = TestClient(client.app)
    body = {"incident_uid": "inc-x", "verdict": "false_positive", "text": "當時沒有問題", "author": "amy"}
    assert anon.post("/api/v1/tuning/feedback", json=body).status_code == 401


def test_feedback_page_is_not_listed_as_an_api(client):
    assert "/feedback" not in client.app.openapi()["paths"]
