"""管理介面的 API：登入、事故清單與回饋、設定存進資料庫並在下一輪生效、機密不外洩、靜態檔。
真的起 HTTP 服務（src.server.serve），Grafana 用 tests/conftest.py 的 FakeGrafana（回應格式是錄下來的真實樣本）。"""
from __future__ import annotations

import json
import socket
import threading
import time
from datetime import datetime, timezone

import httpx
import pytest

from src import admin as adminmod
from src.db import Database
from src.main import ConfigError, Settings, apply_overrides, diff_overrides, editable_view, effective_settings
from src.server import serve
from tests.conftest import webhook

ADMIN_TOKEN = "admin-s3cret-0123456789"
SECRETS = {"grafana": "gf-s3cret-token", "webhook": "wh-s3cret-token-0123", "llm": "sk-s3cret-llm-key", "admin": ADMIN_TOKEN}
ADMIN = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
HOOK = {"Authorization": f"Bearer {SECRETS['webhook']}"}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def config(db_url: str, *, admin: bool = True, extra: str = "") -> str:
    return (f"grafana: {{url: 'http://grafana.test', token: '{SECRETS['grafana']}'}}\n"
            f"webhook: {{token: '{SECRETS['webhook']}', port: {free_port()}}}\n"
            f"database: {{url: '{db_url}'}}\n"
            f"llm: {{url: 'http://llm.test/v1', api_key: '{SECRETS['llm']}'}}\n"
            + (f"admin: {{token: '{ADMIN_TOKEN}'}}\n" if admin else "")
            + "debounce_seconds: 0\nfollowup_seconds: 1\nclose_after_seconds: 0\n"
              "edges: [[mq-broker, cim-eap], [cim-eap, amhs-mcs]]\n" + extra)


@pytest.fixture
def site(fake, db_url, tmp_path):
    """起一個真的服務。start(admin=…, extra=…) 回傳 (網址, worker)；設定從 config.yaml 讀（跟正式環境同一條路）。"""
    started = []

    def start(**kw):
        path = tmp_path / f"config-{len(started)}.yaml"
        path.write_text(config(db_url, **kw), encoding="utf-8")
        server, worker, db = serve(Settings.load(str(path)), host="127.0.0.1", grafana_transport=fake.transport)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        started.append((server, worker, db))
        return f"http://127.0.0.1:{server.server_address[1]}", worker

    yield start
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


def one_incident(url: str) -> dict:
    assert httpx.post(f"{url}/webhook", json=webhook("firing_mq"), headers=HOOK).status_code == 200
    assert wait_for(lambda: httpx.get(f"{url}/api/incidents", headers=ADMIN).json()["total"] == 1)
    return httpx.get(f"{url}/api/incidents", headers=ADMIN).json()["incidents"][0]


# ── 登入 ──

def test_api_needs_the_admin_token_and_the_webhook_token_does_not_work(site):
    url, _worker = site()
    assert httpx.get(f"{url}/api/overview").status_code == 401
    assert httpx.get(f"{url}/api/overview", headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert httpx.get(f"{url}/api/overview", headers=HOOK).status_code == 401   # Webhook 密鑰是填在 Grafana 上的，不能拿來改設定
    assert httpx.put(f"{url}/api/settings", json={"values": {}}, headers=HOOK).status_code == 401
    ok = httpx.get(f"{url}/api/overview", headers=ADMIN)
    assert ok.status_code == 200 and ok.json()["worker"] == {"healthy": True, "reason": None}
    assert ok.headers["cache-control"] == "no-store"


def test_without_an_admin_token_the_whole_ui_is_off_but_the_webhook_still_works(site):
    url, _worker = site(admin=False)
    for path in ("/api/overview", "/api/settings", "/ui/", "/ui/index.html"):
        assert httpx.get(f"{url}{path}", headers=ADMIN).status_code == 404, path
    assert httpx.get(f"{url}/", follow_redirects=False).status_code == 404
    assert httpx.post(f"{url}/webhook", json=webhook("firing_mq"), headers=HOOK).status_code == 200
    assert httpx.get(f"{url}/healthz").status_code == 200


@pytest.mark.parametrize("extra, message", [
    ("admin: {token: short}\n", "至少要 16 個字元"),
    (f"admin: {{token: '{SECRETS['webhook']}'}}\n", "不能跟 webhook.token 一樣"),
    ("admin: {token: 'a-long-enough-admin-token', user: x}\n", "admin 有不認得的設定"),
])
def test_bad_admin_token_settings_are_rejected_at_startup(tmp_path, extra, message):
    path = tmp_path / "config.yaml"
    path.write_text(config("sqlite://", admin=False, extra=extra))
    with pytest.raises(ConfigError, match=message):
        Settings.load(str(path))


# ── 機密 ──

def test_no_endpoint_ever_returns_a_secret(site):
    url, _worker = site()
    inc = one_incident(url)
    httpx.put(f"{url}/api/settings", headers=ADMIN, json={"values": {"debounce_seconds": 5}})
    bodies = [httpx.get(f"{url}/api/{p}", headers=ADMIN).text for p in
              ("overview", "incidents", f"incidents/{inc['uid']}", "rounds", "settings")]
    bodies.append(httpx.post(f"{url}/api/checks", headers=ADMIN, json={"llm": False}).text)
    for body in bodies:
        for name, secret in SECRETS.items():
            assert secret not in body, f"{name} 的密鑰出現在回應裡"
    assert "sqlite" not in bodies[4] and "postgresql" not in bodies[4]   # 連線字串也不在設定頁裡


@pytest.mark.parametrize("values", [
    {"grafana": {"token": "x"}}, {"grafana": {"url": "http://evil.test"}}, {"webhook": {"token": "x"}},
    {"database": {"url": "sqlite://"}}, {"llm": {"api_key": "x"}}, {"llm": {"url": "http://evil.test/v1"}},
    {"admin": {"token": "another-long-admin-token"}}, {"llm": {"system_prompt_file": "/etc/passwd"}},
    {"log_level": "DEBUG"}, {"nonsense": 1},
])
def test_secrets_and_endpoints_cannot_be_changed_from_the_ui(site, values):
    url, _worker = site()
    r = httpx.put(f"{url}/api/settings", headers=ADMIN, json={"values": values})
    assert r.status_code == 400 and "不能在管理介面修改" in r.json()["error"]
    assert httpx.get(f"{url}/api/settings", headers=ADMIN).json()["overrides"] == {}


# ── 設定 ──

def test_settings_page_shows_effective_values_and_what_config_yaml_says(site):
    url, _worker = site()
    view = httpx.get(f"{url}/api/settings", headers=ADMIN).json()
    assert view["values"] == view["base"] and view["overrides"] == {} and view["history"] == [] and view["problem"] is None
    assert view["values"]["labels"] == {"detect": "aiops_detect", "severity": "aiops_severity", "service": "service", "default_severity": "P3"}
    assert view["values"]["edges"] == [["mq-broker", "cim-eap"], ["cim-eap", "amhs-mcs"]]
    assert view["values"]["es"]["log_fields"]["level"] == "log.level" and view["values"]["llm"]["temperature"] is None


def test_only_values_that_differ_from_config_yaml_are_stored(site, db_url):
    url, _worker = site()
    values = httpx.get(f"{url}/api/settings", headers=ADMIN).json()["values"]
    values["labels"]["service"] = "system"
    values["es"]["log_fields"]["level"] = ""
    values["es"]["log_fields"]["service"] = "kubernetes.container_name"
    values["edges"].append(["mes-db", "mes-api"])
    saved = httpx.put(f"{url}/api/settings", headers=ADMIN, json={"values": values})
    assert saved.status_code == 200, saved.text
    body = saved.json()
    assert body["overrides"] == {"labels": {"service": "system"}, "edges": values["edges"],
                                 "es": {"log_fields": {"time": "@timestamp", "service": "kubernetes.container_name",
                                                       "level": "", "message": "message"}}}
    assert body["values"] == values and body["base"]["labels"]["service"] == "service"
    assert len(body["history"]) == 1 and body["history"][0]["before"] == {} and body["history"][0]["after"] == body["overrides"]
    # 存一樣的東西不會多一筆歷史；全部改回 config.yaml 的值 = 清掉覆寫
    assert len(httpx.put(f"{url}/api/settings", headers=ADMIN, json={"values": values}).json()["history"]) == 1
    back = httpx.put(f"{url}/api/settings", headers=ADMIN, json={"values": body["base"]}).json()
    assert back["overrides"] == {} and back["values"] == back["base"] and len(back["history"]) == 2


@pytest.mark.parametrize("change, message", [
    (lambda v: v.update(debounce_seconds=-1), "debounce_seconds"),
    (lambda v: v.update(debounce_seconds="abc"), "debounce_seconds"),
    (lambda v: v.update(timezone="Mars/Phobos"), "不是合法時區"),
    (lambda v: v["labels"].update(service=""), "三個不同、非空"),
    (lambda v: v["labels"].update(service="aiops_detect"), "三個不同、非空"),
    (lambda v: v["labels"].update(default_severity="P9"), "P0～P3"),
    (lambda v: v.update(edges=[["a", "a"]]), "關係邊"),
    (lambda v: v["es"]["log_fields"].update(service=""), "只有 level 可以留空"),
    (lambda v: v["es"].update(timeline_fields=["nope"]), "不在 es.id_fields 裡"),
    (lambda v: v["llm"].update(id_pattern="("), "正規表示式"),
    (lambda v: v["llm"].update(retry_max_seconds=1), "不能比"),
])
def test_invalid_settings_are_rejected_with_the_same_checks_as_startup_and_nothing_is_stored(site, change, message):
    url, _worker = site()
    values = httpx.get(f"{url}/api/settings", headers=ADMIN).json()["values"]
    change(values)
    r = httpx.put(f"{url}/api/settings", headers=ADMIN, json={"values": values})
    assert r.status_code == 400 and message in r.json()["error"], r.text
    after = httpx.get(f"{url}/api/settings", headers=ADMIN).json()
    assert after["overrides"] == {} and after["history"] == []


def test_a_saved_setting_is_used_from_the_next_round_and_survives_a_restart(site):
    """標籤名稱改成 system：改之前，只帶 system 標籤的告警被略過；改之後不用重啟就收得進來；重啟後還是新的值。"""
    url, worker = site()
    alert = webhook("firing_mq")
    for item in alert["alerts"]:
        item["labels"]["system"] = item["labels"].pop("service")
    before = httpx.post(f"{url}/webhook", json=alert, headers=HOOK).json()
    assert before["accepted"] == 0 and "沒有 service 標籤" in before["problems"][0]

    values = httpx.get(f"{url}/api/settings", headers=ADMIN).json()["values"]
    values["labels"]["service"] = "system"
    values["followup_seconds"] = 7
    assert httpx.put(f"{url}/api/settings", headers=ADMIN, json={"values": values}).status_code == 200
    assert worker.settings.labels.service == "system" and worker.settings.followup_seconds == 7   # 存的當下就套用
    assert httpx.post(f"{url}/webhook", json=alert, headers=HOOK).json()["accepted"] == 1
    assert wait_for(lambda: httpx.get(f"{url}/api/incidents", headers=ADMIN).json()["total"] == 1)

    url2, worker2 = site()   # 同一個資料庫、同一份 config.yaml，再起一個服務 = 重啟
    assert worker2.settings.labels.service == "system" and worker2.settings.followup_seconds == 7
    assert httpx.get(f"{url2}/api/settings", headers=ADMIN).json()["values"]["labels"]["service"] == "system"


def test_settings_are_reread_before_every_round_not_kept_in_memory(site, db_url):
    """常駐服務不能靠記憶體記住設定：別的地方（另一個行程、直接改資料庫）改了，下一輪開始前就會讀到。"""
    url, worker = site()
    database = Database(db_url)
    database.save_overrides({"close_after_seconds": 123}, now=datetime.now(timezone.utc))
    database.close()
    assert worker.settings.close_after_seconds == 0
    assert httpx.post(f"{url}/webhook", json=webhook("firing_mq"), headers=HOOK).status_code == 200
    assert wait_for(lambda: worker.rounds >= 1)
    assert worker.settings.close_after_seconds == 123


def test_overrides_that_no_longer_fit_config_yaml_fall_back_to_config_yaml(tmp_path, db_url):
    """存的時候合法，之後 config.yaml 把 id_fields 改掉，資料庫裡的 timeline_fields 就對不上了：服務只用 config.yaml，不停掉。"""
    path = tmp_path / "config.yaml"
    path.write_text(config(db_url))
    base = Settings.load(str(path))
    database = Database(db_url)
    wanted = editable_view(base)
    wanted["es"]["timeline_fields"] = ["lot_id"]
    overrides = diff_overrides(wanted, editable_view(base))
    assert apply_overrides(base, overrides).evidence.timeline_fields == ("lot_id",)
    database.save_overrides(overrides, now=datetime.now(timezone.utc))
    path.write_text(config(db_url, extra="es: {id_fields: [eqp_id]}\n"))
    changed = Settings.load(str(path))
    assert effective_settings(changed, database) == changed
    view = adminmod.Admin(changed, database, get_settings=lambda: changed).settings_view()
    assert "timeline_fields" in view["problem"] and view["values"] == view["base"]
    database.close()


def test_without_overrides_the_settings_object_is_the_one_from_config_yaml(tmp_path, db_url):
    path = tmp_path / "config.yaml"
    path.write_text(config(db_url))
    base = Settings.load(str(path))
    database = Database(db_url)
    assert effective_settings(base, database) is base
    database.close()


# ── 事故與回饋 ──

def test_incident_list_detail_and_feedback(site):
    url, _worker = site()
    listed = one_incident(url)
    assert listed["status"] == "open" and listed["services"] == ["mq-broker"] and listed["feedback"] is None
    assert listed["summary"].startswith("[未經分析]")   # 沒有設定模型：照實標示
    detail = httpx.get(f"{url}/api/incidents/{listed['uid']}", headers=ADMIN).json()
    assert len(detail["alerts"]) == 1 and detail["alerts"][0]["rule_url"].startswith("http://grafana.test/alerting/")
    assert detail["feedback_history"] == []

    bad = httpx.post(f"{url}/api/incidents/{listed['uid']}/feedback", headers=ADMIN, json={"verdict": "maybe"})
    assert bad.status_code == 400
    assert httpx.post(f"{url}/api/incidents/nope/feedback", headers=ADMIN, json={"verdict": "ok"}).status_code == 404
    first = httpx.post(f"{url}/api/incidents/{listed['uid']}/feedback", headers=ADMIN,
                       json={"verdict": "wrong", "note": " 其實是 broker 磁碟滿了 "}).json()
    assert first["feedback"]["verdict"] == "wrong" and first["feedback"]["note"] == "其實是 broker 磁碟滿了"
    assert first["feedback"]["summary"] == listed["summary"]   # 記下評的是哪一版摘要
    second = httpx.post(f"{url}/api/incidents/{listed['uid']}/feedback", headers=ADMIN, json={"verdict": "ok"}).json()
    assert second["feedback"]["verdict"] == "ok" and [f["verdict"] for f in second["feedback_history"]] == ["ok", "wrong"]
    overview = httpx.get(f"{url}/api/overview", headers=ADMIN).json()
    assert overview["feedback"] == {"ok": 1, "wrong": 0} and overview["open_incidents"] == 1   # 以最新一筆為準
    assert httpx.get(f"{url}/api/incidents?status=closed", headers=ADMIN).json()["total"] == 0
    assert httpx.get(f"{url}/api/incidents?status=bogus", headers=ADMIN).status_code == 400
    assert httpx.get(f"{url}/api/incidents?limit=0", headers=ADMIN).status_code == 400
    rounds = httpx.get(f"{url}/api/rounds", headers=ADMIN).json()["rounds"]
    assert rounds and rounds[-1]["alerts"] == 1


def test_feedback_does_not_change_the_incident_or_what_is_written_to_grafana(site, fake):
    url, _worker = site()
    inc = one_incident(url)
    assert wait_for(lambda: bool(fake.annotations))
    before = (httpx.get(f"{url}/api/incidents/{inc['uid']}", headers=ADMIN).json()["summary"], json.dumps(fake.annotations, sort_keys=True))
    httpx.post(f"{url}/api/incidents/{inc['uid']}/feedback", headers=ADMIN, json={"verdict": "wrong", "note": "x"})
    time.sleep(1.5)   # 讓續跑的那一輪跑過
    after = (httpx.get(f"{url}/api/incidents/{inc['uid']}", headers=ADMIN).json()["summary"], json.dumps(fake.annotations, sort_keys=True))
    assert after == before


# ── 其他 ──

def test_unknown_api_paths_and_bad_bodies(site):
    url, _worker = site()
    assert httpx.get(f"{url}/api/nope", headers=ADMIN).status_code == 404
    assert httpx.put(f"{url}/api/settings", headers=ADMIN, content=b"{not json").status_code == 400
    assert httpx.put(f"{url}/api/settings", headers=ADMIN, json=[1]).status_code == 400
    assert httpx.put(f"{url}/api/settings", headers=ADMIN, json={}).status_code == 400
    big = httpx.put(f"{url}/api/settings", headers=ADMIN, content=b"x" * 1_000_001)
    assert big.status_code == 413


def test_static_files_are_served_without_a_token_and_cannot_escape_the_folder(site, tmp_path, monkeypatch):
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<!doctype html><title>ui</title>")
    (dist / "assets" / "app-abc.js").write_text("console.log(1)")
    (tmp_path / "secret.txt").write_text("outside")
    monkeypatch.setattr(adminmod, "UI_DIR", dist)
    monkeypatch.setattr(adminmod.Admin.__init__, "__kwdefaults__", {**adminmod.Admin.__init__.__kwdefaults__, "ui_dir": dist})
    url, _worker = site()
    home = httpx.get(f"{url}/", follow_redirects=False)
    assert home.status_code == 302 and home.headers["location"] == "/ui/"
    page = httpx.get(f"{url}/ui/")
    assert page.status_code == 200 and "<title>ui</title>" in page.text and page.headers["cache-control"] == "no-store"
    asset = httpx.get(f"{url}/ui/assets/app-abc.js")
    assert asset.status_code == 200 and "javascript" in asset.headers["content-type"] and "immutable" in asset.headers["cache-control"]
    assert "<title>ui</title>" in httpx.get(f"{url}/ui/incidents/abc").text   # 前端的路徑：回 index.html
    for sneaky in ("/ui/../secret.txt", "/ui/%2e%2e/secret.txt", "/ui/..%2fsecret.txt", "/ui//etc/passwd"):
        r = httpx.get(f"{url}{sneaky}")
        assert "outside" not in r.text and "root:" not in r.text, sneaky
