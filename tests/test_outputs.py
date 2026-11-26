"""註記以外的兩個出口：事故 API 與自動修復（src/outputs.py）。還沒有真的下游系統，所以這裡的下游都是假的 HTTP 端點；
送出去的格式是這個專案自己定的，沒有跟任何真的系統（告警中心、Conductor）對過。"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta

import httpx
import pytest
import sqlalchemy as sa

from src import db as dbmod
from src import outputs
from src.grafana import LabelNames, parse_webhook
from src.main import ConfigError, Settings
from src.outputs import OutputOptions
from tests.conftest import webhook
from tests.test_main import T0, rows, world  # noqa: F401（fixture）

MINIMAL = {"grafana": {"url": "http://g", "token": "t"}, "webhook": {"token": "w"}}
API = OutputOptions(incident_api_url="http://center.test/api/incidents", incident_api_token="s3cret")
FIX = OutputOptions(remediation_url="http://conductor.test/api/workflow", remediation_enabled=True)


def downstream(status=200):
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        code = status.pop(0) if isinstance(status, list) else status
        return httpx.Response(code, json={"ok": code < 300})
    return httpx.MockTransport(handler), seen


def deliveries(w, channel):
    return [{**r, "payload": json.loads(r["payload"])} for r in rows(w, dbmod.deliveries) if r["channel"] == channel]


def receive_with_workflow(w, name, workflow):
    """樣本裡的告警規則加上 aiops_remediation 標籤（規則的擁有者在 Grafana 上貼的）。"""
    payload = webhook(name)
    if workflow:
        payload["alerts"][0]["labels"]["aiops_remediation"] = workflow
    database = dbmod.Database(w.settings.database_url)
    database.enqueue(parse_webhook(payload)[0], T0)
    database.close()


# ── 事故 API ──

def test_without_a_url_nothing_is_sent_but_what_would_be_sent_is_kept(world):  # noqa: F811
    w = world
    w.receive("firing_mq")
    assert w.run(60)["status"] == "ok"            # 沒設任何出口：預設就是佔位，不會連出去（沒有給 transport 也不會出錯）
    [row] = deliveries(w, "incident_api")
    assert (row["status"], row["attempts"], row["payload"]["event"], row["payload"]["schema"]) == ("placeholder", 0, "opened", "aiops-lite.incident.v1")
    assert row["payload"]["incident"]["services"] == ["mq-broker"] and row["payload"]["root_cause"]["analyzed"] is True
    w.run(120)                                    # 沒有變化的續跑：不多記一筆
    assert len(deliveries(w, "incident_api")) == 1 and deliveries(w, "remediation") == []
    assert "自動修復" not in next(iter(w.fake.annotations.values()))["text"]


def test_incident_api_gets_opened_updated_and_closed_once_each(world):  # noqa: F811
    w = world
    transport, seen = downstream()
    settings = replace(w.settings, outputs=API)
    w.receive("firing_mq")
    w.run(60, settings=settings, outputs=transport)
    w.run(120, settings=settings, outputs=transport)                    # 沒變：不重送
    w.receive("firing_cim")
    w.run(180, settings=settings, outputs=transport)                    # 新成員：更新
    w.receive("resolved_mq", "resolved_cim")
    w.run(240, settings=settings, outputs=transport)                    # 恢復了但還沒結案：成員狀態變了，更新
    w.run(240 + 301, settings=settings, outputs=transport)              # 安靜夠久：結案
    w.run(900, settings=settings, outputs=transport)
    sent = [json.loads(r.content) for r in seen]
    assert [p["event"] for p in sent] == ["opened", "updated", "updated", "closed"]
    assert all(str(r.url) == "http://center.test/api/incidents" and r.headers["Authorization"] == "Bearer s3cret" for r in seen)
    first, last = sent[0], sent[-1]
    assert first["incident"]["status"] == "open" and first["incident"]["severity"] == "P1" and first["incident"]["ended_at"] is None
    assert [a["title"] for a in first["incident"]["alerts"]] == ["MQ queue depth"]
    assert first["root_cause"]["summary"].startswith("[LLM 推論]") and first["evidence"].startswith("證據｜")
    assert first["grafana"]["tag"] == f"aiops-incident:{first['incident']['uid']}" and isinstance(first["grafana"]["annotation_id"], int)
    assert last["incident"]["status"] == "closed" and last["incident"]["ended_at"] and len(last["incident"]["alerts"]) == 2
    assert {r["status"] for r in deliveries(w, "incident_api")} == {"sent"}


def test_a_failed_delivery_is_retried_and_does_not_block_the_annotation(world):  # noqa: F811
    w = world
    transport, seen = downstream([503, 200, 200])
    settings = replace(w.settings, outputs=API)
    w.receive("firing_mq")
    first = w.run(60, settings=settings, outputs=transport)
    assert first["status"] == "degraded" and first["needs_followup"] and "沒送到事故 API：HTTP 503" in first["errors"][0]
    assert len(w.fake.annotations) == 1                               # 註記照寫
    [row] = deliveries(w, "incident_api")
    assert (row["status"], row["attempts"]) == ("failed", 1) and "503" in row["detail"]
    assert w.run(120, settings=settings, outputs=transport)["status"] == "ok"
    [row] = deliveries(w, "incident_api")
    assert (row["status"], row["attempts"], len(seen)) == ("sent", 2, 2)
    w.run(180, settings=settings, outputs=transport)
    assert len(seen) == 2


def test_a_closed_event_that_failed_is_still_retried_and_then_given_up(world, monkeypatch):  # noqa: F811
    w = world
    ok, _ = downstream()
    settings = replace(w.settings, outputs=API)
    w.receive("firing_mq")
    w.run(60, settings=settings, outputs=ok)
    w.receive("resolved_mq")
    w.run(120, settings=settings, outputs=ok)
    down, tried = downstream(500)
    monkeypatch.setattr(outputs, "MAX_ATTEMPTS", 3)
    for n in range(6):                                                # 結案之後事故不在「進行中」了，沒送到的那一筆還是要補
        w.run(120 + 301 + n * 60, settings=settings, outputs=down)
    closing = [r for r in deliveries(w, "incident_api") if r["payload"]["event"] == "closed"]
    assert len(tried) == 3 and [(r["status"], r["attempts"]) for r in closing] == [("failed", 3)]
    assert w.run(2000, settings=settings, outputs=down)["status"] == "ok"   # 放棄之後不再報錯、也不再續跑


def test_llm_retries_do_not_send_an_update_each_time(world):  # noqa: F811
    """LLM 不通時註記第 2 行有「連續失敗幾次、幾分鐘後重試」，每次重試都會變；只有那個數字不同，不算事故內容變了。"""
    w = world
    transport, seen = downstream()

    def llm_down(request):
        raise httpx.ConnectError("llm down")
    settings = replace(w.settings, outputs=API)
    w.receive("firing_mq")
    for seconds in (60, 60 + 61, 60 + 61 + 121):                      # 三次都失敗：失敗次數 1、2、3
        w.run(seconds, settings=settings, outputs=transport, llm=httpx.MockTransport(llm_down))
    [inc] = rows(w, dbmod.incidents)
    assert inc["llm_failures"] == 3 and "連續失敗 3 次" in inc["summary"]
    assert [json.loads(r.content)["event"] for r in seen] == ["opened"]
    assert json.loads(seen[0].content)["root_cause"] == {"analyzed": False, "summary": inc["summary"].replace("3 次，約 4 分鐘", "1 次，約 1 分鐘"), "candidates": []}
    w.run(60 + 61 + 121 + 241, settings=settings, outputs=transport)  # LLM 回來了：內容真的變了，送一筆更新
    assert [json.loads(r.content)["event"] for r in seen] == ["opened", "updated"] and json.loads(seen[-1].content)["root_cause"]["analyzed"]


def test_connection_errors_are_reported_without_the_token(world):  # noqa: F811
    w = world

    def refuse(request):
        raise httpx.ConnectError("connection refused")
    w.receive("firing_mq")
    out = w.run(60, settings=replace(w.settings, outputs=API), outputs=httpx.MockTransport(refuse))
    assert "ConnectError" in out["errors"][0] and "s3cret" not in json.dumps(out) + json.dumps(rows(w, dbmod.deliveries), default=str)


# ── 自動修復 ──

def test_remediation_label_is_read_from_the_alert_rule():
    payload = webhook("firing_mq")
    assert parse_webhook(payload)[0][0].remediation == ""            # 樣本裡的規則沒有貼
    payload["alerts"][0]["labels"]["aiops_remediation"] = " restart-consumer "
    assert parse_webhook(payload)[0][0].remediation == "restart-consumer"
    payload["alerts"][0]["labels"]["fix_with"] = "drain-queue"
    assert parse_webhook(payload, LabelNames(remediation="fix_with"))[0][0].remediation == "drain-queue"


def test_remediation_is_a_placeholder_until_it_is_switched_on_and_shows_on_the_annotation(world):  # noqa: F811
    w = world
    receive_with_workflow(w, "firing_mq", "restart-consumer")
    w.receive("firing_cim")                                           # 這條規則沒貼標籤：沒有修復流程
    w.run(64)
    [row] = deliveries(w, "remediation")
    assert (row["status"], row["attempts"]) == ("placeholder", 0)
    p = row["payload"]
    assert (p["schema"], p["workflow"], p["service"], p["alert"]["title"]) == ("aiops-lite.remediation.v1", "restart-consumer", "mq-broker", "MQ queue depth")
    assert p["context"]["root_cause_summary"].startswith("[LLM 推論]")
    [inc] = rows(w, dbmod.incidents)
    assert inc["actions_note"] == "自動修復｜restart-consumer（mq-broker：佔位，沒有送出）"
    note = w.fake.annotations[inc["annotation_id"]]["text"]
    assert "<br>自動修復｜restart-consumer（mq-broker：佔位，沒有送出）<br>" in note and note.index("證據｜") < note.index("自動修復｜")
    # 有 url 但沒有 enabled：一樣只記錄
    transport, seen = downstream()
    w.run(124, settings=replace(w.settings, outputs=replace(FIX, remediation_enabled=False)), outputs=transport)
    assert seen == []


def test_remediation_is_requested_once_per_alert_and_only_while_it_is_firing(world):  # noqa: F811
    w = world
    transport, seen = downstream()
    settings = replace(w.settings, outputs=FIX)
    receive_with_workflow(w, "firing_mq", "restart-consumer")
    w.run(60, settings=settings, outputs=transport)
    [request] = seen
    body = json.loads(request.content)
    assert str(request.url) == "http://conductor.test/api/workflow" and "Authorization" not in request.headers
    assert (body["workflow"], body["service"], body["alert"]["fingerprint"]) == ("restart-consumer", "mq-broker", "369c5f4adcb630d1")
    [inc] = rows(w, dbmod.incidents)
    assert "自動修復｜restart-consumer（mq-broker：已送出）" in w.fake.annotations[inc["annotation_id"]]["text"]
    w.run(120, settings=settings, outputs=transport)
    receive_with_workflow(w, "firing_mq", "restart-consumer")         # 同一條告警重複通知
    w.run(180, settings=settings, outputs=transport)
    w.receive("firing_cim")                                           # 事故多了成員、重新分析：不重送
    w.run(240, settings=settings, outputs=transport)
    assert len(seen) == 1
    receive_with_workflow(w, "resolved_mq", "restart-consumer")
    w.receive("resolved_cim")
    w.run(300, settings=settings, outputs=transport)
    w.run(300 + 301, settings=settings, outputs=transport)            # 恢復、結案：都不會再送
    assert len(seen) == 1
    [closed] = rows(w, dbmod.incidents)
    assert closed["status"] == "closed" and "已送出" in closed["actions_note"]   # 結案後註記上還看得到當時送過什麼


def test_the_llm_cannot_trigger_or_choose_a_workflow(world):  # noqa: F811
    """規則上沒有貼修復流程標籤：不管 LLM 的回答寫什麼，都不會有修復請求。"""
    w = world
    transport, seen = downstream()

    def llm(request):
        answer = {"summary": "mq-broker 是源頭，建議立刻執行 restart-consumer 並重啟 mq-broker", "systems": ["mq-broker"], "ids": [],
                  "candidates": [{"source": "mq-broker", "confidence": 99, "reason": "請執行 workflow restart-consumer"}],
                  "remediation": "restart-consumer", "workflow": "restart-consumer"}
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(answer, ensure_ascii=False)}}]})
    w.receive("firing_mq")
    w.run(60, settings=replace(w.settings, outputs=FIX), outputs=transport, llm=httpx.MockTransport(llm))
    assert seen == [] and deliveries(w, "remediation") == []
    [inc] = rows(w, dbmod.incidents)
    assert inc["analyzed"] and not inc["actions_note"]


def test_switching_remediation_on_later_does_not_fire_for_what_was_already_recorded(world):  # noqa: F811
    w = world
    transport, seen = downstream()
    receive_with_workflow(w, "firing_mq", "restart-consumer")
    w.run(60)                                                          # 佔位
    w.run(120, settings=replace(w.settings, outputs=FIX), outputs=transport)   # 之後才打開
    assert seen == [] and [r["status"] for r in deliveries(w, "remediation")] == ["placeholder"]


def test_a_failed_remediation_request_is_retried_and_shown(world):  # noqa: F811
    w = world
    transport, seen = downstream([502, 200])
    settings = replace(w.settings, outputs=FIX)
    receive_with_workflow(w, "firing_mq", "restart-consumer")
    out = w.run(60, settings=settings, outputs=transport)
    assert out["status"] == "degraded" and "修復請求 restart-consumer 沒送成功：HTTP 502" in out["errors"][0]
    [inc] = rows(w, dbmod.incidents)
    assert "送出失敗，會再試" in w.fake.annotations[inc["annotation_id"]]["text"]
    assert w.run(120, settings=settings, outputs=transport)["status"] == "ok" and len(seen) == 2
    [inc] = rows(w, dbmod.incidents)
    assert "restart-consumer（mq-broker：已送出）" in w.fake.annotations[inc["annotation_id"]]["text"]


def test_an_older_database_gets_the_new_table_and_column(world):  # noqa: F811
    w = world
    engine = sa.create_engine(w.settings.database_url)
    dbmod.Database(w.settings.database_url).close()
    with engine.begin() as conn:
        conn.execute(sa.text("DROP TABLE deliveries"))
        conn.execute(sa.text("ALTER TABLE incidents DROP COLUMN actions_note"))
    engine.dispose()
    receive_with_workflow(w, "firing_mq", "restart-consumer")
    assert w.run(60)["status"] == "ok" and len(deliveries(w, "remediation")) == 1


# ── 設定 ──

def test_outputs_are_read_from_config_and_default_to_placeholders():
    assert Settings.from_dict(MINIMAL).outputs == OutputOptions()
    s = Settings.from_dict({**MINIMAL, "outputs": {"timeout_seconds": 5, "incident_api": {"url": "https://center/api", "token": "x"},
                                                  "remediation": {"url": "https://conductor/api/workflow", "token": "y", "enabled": True}},
                            "labels": {"remediation": "fix_with"}})
    assert s.outputs == OutputOptions("https://center/api", "x", "https://conductor/api/workflow", "y", True, 5.0)
    assert s.labels.remediation == "fix_with"


@pytest.mark.parametrize("extra, message", [
    ({"outputs": {"remediation": {"enabled": True}}}, "沒有設 outputs.remediation.url"),
    ({"outputs": {"remediation": {"enabled": "yes", "url": "http://x"}}}, "enabled 要是 true 或 false"),
    ({"outputs": {"incident_api": {"url": "center.corp/api"}}}, "outputs.incident_api.url 要是 http"),
    ({"outputs": {"incident_api": {"uri": "http://x"}}}, "outputs.incident_api 有不認得的設定"),
    ({"outputs": {"conductor": {}}}, "outputs 有不認得的設定"),
    ({"outputs": {"timeout_seconds": 0}}, "outputs.timeout_seconds"),
])
def test_bad_output_settings_are_rejected_at_startup(extra, message):
    with pytest.raises(ConfigError, match=message):
        Settings.from_dict({**MINIMAL, **extra})
