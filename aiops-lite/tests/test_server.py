"""常駐服務：debounce 的排程邏輯（純邏輯），以及真的起 HTTP 服務收 Grafana 13.0.7 的 webhook 樣本。"""
from __future__ import annotations

import json
import threading
import time

import httpx
import pytest
import sqlalchemy as sa

from src import db as dbmod
from src.main import Settings, process
from src import server as servermod
from src.server import Debounce, serve
from tests.conftest import webhook

AUTH = {"Authorization": "Bearer hook-secret"}


# ── 排程邏輯 ──

def test_window_opens_on_the_first_webhook_and_is_not_extended_by_later_ones():
    d = Debounce(60)
    assert d.wait_seconds(0) is None          # 沒事：等 Webhook
    d.webhook(100)
    d.webhook(130)                            # 下游晚 30 秒才響
    d.webhook(159)                            # 一直有告警進來也不會把視窗往後推
    assert d.wait_seconds(130) == 30 and d.wait_seconds(160) == 0 and d.wait_seconds(999) == 0


def test_alerts_arriving_during_a_round_open_the_next_window():
    d = Debounce(60)
    d.webhook(100)
    d.start_round()                           # 時間到，開始跑
    assert d.wait_seconds(161) is None
    d.webhook(165)                            # 跑的途中又進來一筆
    assert d.wait_seconds(165) == 60


def test_followup_never_shortens_or_replaces_a_debounce_window():
    d = Debounce(60)
    d.followup(0, 20)
    assert d.wait_seconds(5) == 15            # 有進行中事故：20 秒後續跑
    d.webhook(10)                             # 這時候進來新告警：照樣等滿 debounce，不是 10 秒後就跑
    assert d.wait_seconds(10) == 60
    d.followup(11, 1)                         # 視窗開著的時候排續跑：不影響
    assert d.wait_seconds(11) == 59


def test_zero_debounce_runs_immediately():
    d = Debounce(0)
    d.webhook(5)
    assert d.wait_seconds(5) == 0


# ── HTTP 服務 ──

@pytest.fixture
def live(fake, tmp_path):
    settings = Settings(grafana_url="http://grafana.test", grafana_token="t", webhook_token="hook-secret",
                        database_url=f"sqlite:///{tmp_path}/lite.db", port=0, debounce_seconds=1, followup_seconds=1,
                        close_after_seconds=0, edges=(("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")))
    summaries = []

    def run_round():
        if live.hold is not None:
            live.hold.wait(10)  # 測試用來模擬「這一輪卡住」
        summaries.append(process(settings, grafana_transport=fake.transport))
        return summaries[-1]

    started = []

    def start():
        server, worker, db = serve(settings, host="127.0.0.1", run_round=run_round)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        started.append((server, worker, db))
        return f"http://127.0.0.1:{server.server_address[1]}"

    class Live:
        pass

    live = Live()
    live.settings, live.summaries, live.start, live.fake = settings, summaries, start, fake
    live.started, live.hold = started, None
    yield live
    for server, worker, db in started:
        worker.stop()
        server.shutdown()
        server.server_close()
        db.close()


def wait_for(condition, seconds=8.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


def table(settings, t):
    database = dbmod.Database(settings.database_url)
    with database.engine.connect() as conn:
        out = [dict(r) for r in conn.execute(sa.select(t)).mappings()]
    database.close()
    return out


def test_burst_of_webhooks_is_stored_at_once_and_processed_in_one_round_after_the_debounce(live):
    url = live.start()
    t0 = time.monotonic()
    for name in ("firing_mq", "firing_cim", "firing_amhs"):
        r = httpx.post(f"{url}/webhook", json=webhook(name), headers=AUTH)
        assert r.status_code == 200 and r.json() == {"accepted": 1, "problems": []}
    assert len(table(live.settings, dbmod.inbox)) == 3 and live.summaries == []  # 先落地，還沒開始分析
    assert wait_for(lambda: live.summaries)
    assert time.monotonic() - t0 >= 1.0                                          # 等滿 debounce 才跑
    assert live.summaries[0]["alerts"] == 3 and live.summaries[0]["open_incidents"] == 1
    [inc] = table(live.settings, dbmod.incidents)
    assert json.loads(inc["services"]) == ["amhs-mcs", "cim-eap", "mq-broker"] and inc["alert_count"] == 3
    assert len(live.fake.annotations) == 1

    # 有進行中事故：沒有新 Webhook 也會續跑
    assert wait_for(lambda: len(live.summaries) >= 3)
    # 全部恢復（close_after_seconds = 0）→ 結案 → 不再續跑
    for name in ("resolved_mq", "resolved_cim", "resolved_amhs"):
        assert httpx.post(f"{url}/webhook", json=webhook(name), headers=AUTH).status_code == 200
    assert wait_for(lambda: live.summaries[-1]["open_incidents"] == 0 and not live.summaries[-1]["needs_followup"])
    done = len(live.summaries)
    time.sleep(2.5)
    assert len(live.summaries) == done and table(live.settings, dbmod.incidents)[0]["status"] == "closed"
    assert len(live.fake.annotations) == 1 and "closed" in live.fake.annotations[1]["tags"]


def test_requests_without_the_right_token_or_format_are_rejected_and_never_trigger_a_round(live):
    url = live.start()
    assert httpx.post(f"{url}/webhook", json=webhook("firing_mq")).status_code == 401
    assert httpx.post(f"{url}/webhook", json=webhook("firing_mq"), headers={"Authorization": "Bearer nope"}).status_code == 401
    assert httpx.post(f"{url}/webhook", content=b"{not json", headers=AUTH).status_code == 400
    assert httpx.post(f"{url}/webhook", json={"hello": "world"}, headers=AUTH).status_code == 400
    assert httpx.post(f"{url}/other", json=webhook("firing_mq"), headers=AUTH).status_code == 404
    assert httpx.post(f"{url}/webhook", content=b"x" * 1_000_001, headers=AUTH).status_code == 413
    assert httpx.get(f"{url}/healthz").json() == {"status": "ok"}
    unmanaged = webhook("firing_mq")
    del unmanaged["alerts"][0]["labels"]["aiops_detect"]
    assert httpx.post(f"{url}/webhook", json=unmanaged, headers=AUTH).json() == {"accepted": 0, "problems": []}
    time.sleep(1.6)
    assert live.summaries == [] and table(live.settings, dbmod.inbox) == []


def test_alert_missing_service_is_reported_back_to_the_sender(live):
    url = live.start()
    payload = webhook("firing_mq")
    del payload["alerts"][0]["labels"]["service"]
    body = httpx.post(f"{url}/webhook", json=payload, headers=AUTH).json()
    assert body["accepted"] == 0 and "沒有 service 標籤" in body["problems"][0]


def test_alerts_left_in_the_inbox_are_processed_after_a_restart(live):
    database = dbmod.Database(live.settings.database_url)
    from src.grafana import parse_webhook
    from datetime import datetime, timezone
    database.enqueue(parse_webhook(webhook("firing_mq"))[0], datetime.now(timezone.utc))  # 上一個行程收到、還沒處理就掛了
    database.close()
    live.start()
    assert wait_for(lambda: live.summaries) and live.summaries[0]["alerts"] == 1


def test_database_failure_on_receive_returns_503_so_grafana_retries(live, monkeypatch):
    url = live.start()

    def boom(self, alerts, now):
        raise sa.exc.OperationalError("insert", {}, Exception("db down"))

    monkeypatch.setattr(dbmod.Database, "enqueue", boom)
    assert httpx.post(f"{url}/webhook", json=webhook("firing_mq"), headers=AUTH).status_code == 503
    time.sleep(1.5)
    assert live.summaries == []


def test_healthz_needs_no_token_and_reports_a_dead_worker(live):
    url = live.start()
    r = httpx.get(f"{url}/healthz")  # 沒帶 Authorization：K8s 探針不會被擋
    assert r.status_code == 200 and r.json() == {"status": "ok"}
    _server, worker, _db = live.started[0]
    worker.stop()
    worker.join(5)
    r = httpx.get(f"{url}/healthz")
    assert r.status_code == 503 and r.json() == {"status": "unhealthy", "reason": "worker dead"}


def test_healthz_reports_a_round_that_is_stuck_and_recovers_when_it_finishes(live, monkeypatch):
    live.hold = threading.Event()
    url = live.start()
    assert httpx.post(f"{url}/webhook", json=webhook("firing_mq"), headers=AUTH).status_code == 200
    _server, worker, _db = live.started[0]
    assert wait_for(lambda: worker.round_started is not None)       # debounce 到了，這一輪開始跑、然後卡住
    assert httpx.get(f"{url}/healthz").status_code == 200           # 還在正常的執行時間內：健康
    monkeypatch.setattr(servermod, "STUCK_SECONDS", 0.2)
    time.sleep(0.4)
    r = httpx.get(f"{url}/healthz")                                 # 工作執行緒還活著（is_alive），但卡太久
    assert worker.is_alive() and r.status_code == 503 and r.json()["reason"].startswith("round stuck")
    live.hold.set()                                                 # 那一輪跑完了
    assert wait_for(lambda: live.summaries and worker.round_started is None)
    assert httpx.get(f"{url}/healthz").status_code == 200


def test_a_failing_round_does_not_kill_the_worker(live, monkeypatch):
    calls = []

    def boom(*a, **kw):
        calls.append(1)
        raise RuntimeError("db down")

    monkeypatch.setattr(servermod, "RETRY_SECONDS", 0.2)
    monkeypatch.setattr("tests.test_server.process", boom)
    url = live.start()
    httpx.post(f"{url}/webhook", json=webhook("firing_mq"), headers=AUTH)
    assert wait_for(lambda: len(calls) >= 2)                        # 失敗後自己重試
    assert httpx.get(f"{url}/healthz").status_code == 200
