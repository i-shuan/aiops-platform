"""一輪一輪跑 main.process：真實的 Grafana webhook 樣本 + 假的 Grafana（ES、註記）+ SQLite + 假的 LLM。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import sqlalchemy as sa

from src import db as dbmod
from src.grafana import parse_webhook
from src.main import ConfigError, Settings, process, split_batch
from tests.conftest import webhook

T0 = datetime(2026, 11, 21, 2, 41, 10, tzinfo=timezone.utc)  # 樣本裡第一筆告警（MQ）的 startsAt
LLM_ANSWER = {"summary": "mq-broker 佇列積壓可能是源頭，下游 cim-eap 的 T3 逾時集中在機台 EQP-CVD-07",
              "systems": ["mq-broker", "cim-eap"], "ids": ["EQP-CVD-07"]}
BASE_ENV = {"GRAFANA_URL": "http://g", "GRAFANA_TOKEN": "t", "AIOPS_WEBHOOK_TOKEN": "w"}


@pytest.fixture
def world(fake, db_url):
    settings = Settings(grafana_url="http://grafana.test", grafana_token="t", webhook_token="w",
                        database_url=db_url, llm_api_url="http://llm.test/v1", llm_model="m",
                        es_datasource_uid="es-uid", log_index="factory-logs", trace_index="factory-traces",
                        close_after_seconds=300, edges=(("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")))
    llm_calls = []

    def llm(request: httpx.Request) -> httpx.Response:
        llm_calls.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(LLM_ANSWER, ensure_ascii=False)}}]})

    class World:
        pass

    w = World()
    w.fake, w.settings, w.llm_calls, w.llm = fake, settings, llm_calls, httpx.MockTransport(llm)

    def receive(*names):  # Webhook 收到時 server 做的事：解析、存進 inbox
        database = dbmod.Database(settings.database_url)
        for name in names:
            database.enqueue(parse_webhook(webhook(name))[0], T0)
        database.close()

    def run(seconds, **kw):
        return process(kw.pop("settings", settings), now=T0 + timedelta(seconds=seconds),
                       grafana_transport=fake.transport, llm_transport=kw.pop("llm", w.llm))

    w.receive, w.run = receive, run
    return w


def rows(w, table):
    database = dbmod.Database(w.settings.database_url)
    with database.engine.connect() as conn:
        out = [dict(r) for r in conn.execute(sa.select(table)).mappings()]
    database.close()
    return out


def test_cascade_becomes_one_incident_one_llm_call_one_annotation_then_closes(world):
    w = world
    # debounce 視窗內陸續收到三個系統的告警（上游先、下游後）→ 時間到跑一輪
    w.receive("firing_mq", "firing_cim", "firing_amhs")
    first = w.run(64)
    assert first["status"] == "ok" and first["alerts"] == 3 and first["open_incidents"] == 1 and first["errors"] == []
    assert first["needs_followup"] is True
    [inc] = rows(w, dbmod.incidents)
    assert json.loads(inc["services"]) == ["amhs-mcs", "cim-eap", "mq-broker"] and inc["severity"] == "P1"
    assert inc["alert_count"] == 3 and inc["analyzed"] and rows(w, dbmod.inbox) == []
    assert len(w.fake.annotations) == 1 and len(w.llm_calls) == 1  # 三個告警只問一次 LLM、只寫一筆註記
    note = w.fake.annotations[inc["annotation_id"]]
    l1, l2, l3 = note["text"].split("<br>")
    assert l1 == "【P1・進行中】受影響系統：amhs-mcs、cim-eap、mq-broker｜收斂告警 3 筆"
    assert l2.startswith("[LLM 推論] ") and "EQP-CVD-07" in l2 and "10:41 ～ 進行中（台灣時間）" in l3
    assert note["time"] == int(T0.timestamp() * 1000) and "open" in note["tags"]
    prompt = w.llm_calls[0]["messages"][1]["content"]
    assert "MQ queue depth" in prompt and '"upstream": "mq-broker"' in prompt and "EQP-CVD-07" in prompt
    assert '"A": 350' in prompt  # 觸發當下的數值也是證據
    assert prompt.index("mq-broker") < prompt.index("AMHS transfer duration")  # 依開始時間排序：先響的在前

    # 續跑（沒有新 Webhook）：什麼都沒變，不重問 LLM、不重寫註記
    calls = len(w.fake.calls)
    again = w.run(124)
    assert again["alerts"] == 0 and again["open_incidents"] == 1 and len(w.llm_calls) == 1
    assert len(w.fake.calls) == calls

    # 兩個先恢復：事故還在進行中
    w.receive("resolved_amhs", "resolved_cim")
    w.run(190)
    [inc2] = rows(w, dbmod.incidents)
    assert inc2["status"] == "open" and inc2["resolved_at"] is None and len(w.llm_calls) == 1
    # 最後一個也恢復：開始計時，還不結案
    w.receive("resolved_mq")
    w.run(250)
    assert rows(w, dbmod.incidents)[0]["status"] == "open" and rows(w, dbmod.incidents)[0]["resolved_at"] is not None
    assert w.run(540)["open_incidents"] == 1          # 安靜 290 秒：還沒到 300
    last = w.run(550)                                 # 滿 300 秒：結案
    [closed] = rows(w, dbmod.incidents)
    assert closed["status"] == "closed" and last["open_incidents"] == 0 and last["needs_followup"] is False
    note = w.fake.annotations[inc["annotation_id"]]
    assert len(w.fake.annotations) == 1 and "closed" in note["tags"] and "【P1・已結案】" in note["text"]
    assert "10:41 ～ 2026-11-21 10:42（台灣時間）" in note["text"]  # 結束時間 = 最後一筆告警的 endsAt
    assert "[LLM 推論]" in note["text"] and len(w.llm_calls) == 1  # 結案不再問 LLM
    assert {r["status"] for r in rows(w, dbmod.rounds)} == {"ok"} and len(rows(w, dbmod.rounds)) == 6


def test_late_alert_joins_the_open_incident_and_is_reanalyzed(world):
    w = world
    w.receive("firing_mq")
    w.run(64)
    w.receive("firing_cim")  # 下游在下一個視窗才響
    w.run(130)
    [inc] = rows(w, dbmod.incidents)
    assert inc["alert_count"] == 2 and json.loads(inc["services"]) == ["cim-eap", "mq-broker"]
    assert len(w.llm_calls) == 2 and len(w.fake.annotations) == 1  # 成員變了：重新分析，註記原地更新


def test_nothing_pending_does_nothing(world):
    out = world.run(0)
    assert out == {"status": "ok", "alerts": 0, "open_incidents": 0, "errors": [], "incidents": [], "needs_followup": False}
    assert world.fake.calls == [] and world.llm_calls == []


def test_llm_outage_falls_back_and_is_retried_on_the_next_round(world):
    w = world

    def down(request):
        raise httpx.ConnectError("llm down")

    w.receive("firing_mq", "firing_cim")
    out = w.run(64, llm=httpx.MockTransport(down))
    [inc] = rows(w, dbmod.incidents)
    assert out["status"] == "degraded" and "未經 LLM 分析" in out["errors"][0] and not inc["analyzed"]
    assert "[未經分析] mq-broker：MQ queue depth；cim-eap：CIM T3 timeout" in w.fake.annotations[inc["annotation_id"]]["text"]
    w.run(124)  # 續跑時 LLM 回來了
    [inc] = rows(w, dbmod.incidents)
    assert inc["analyzed"] and "[LLM 推論]" in w.fake.annotations[inc["annotation_id"]]["text"]
    assert len(w.fake.annotations) == 1


def test_without_llm_settings_the_incident_is_still_written(world):
    w = world
    w.receive("firing_mq")
    out = w.run(64, settings=Settings(**{**w.settings.__dict__, "llm_api_url": "", "llm_model": ""}))
    assert out["status"] == "ok" and w.llm_calls == []
    assert "[未經分析] mq-broker：MQ queue depth" in next(iter(w.fake.annotations.values()))["text"]


def test_annotation_failure_is_retried_next_round_without_duplicates(world, monkeypatch):
    w = world
    monkeypatch.setattr("src.grafana.time.sleep", lambda s: None)
    w.fake.fail.add("/api/annotations")
    w.receive("firing_mq")
    out = w.run(64)
    [inc] = rows(w, dbmod.incidents)
    assert out["status"] == "degraded" and "註記寫入失敗" in out["errors"][-1] and out["needs_followup"]
    assert not inc["annotated"] and inc["annotation_id"] is None and w.fake.annotations == {}
    assert rows(w, dbmod.inbox) == []  # 告警已經收成事故，不會因為註記失敗再處理一次
    w.fake.fail.clear()
    w.run(124)
    [inc] = rows(w, dbmod.incidents)
    assert inc["annotated"] and len(w.fake.annotations) == 1 and inc["alert_count"] == 1


def test_database_failure_fails_the_round_and_keeps_the_alerts(world, monkeypatch):
    w = world
    w.receive("firing_mq")

    def boom(self, *a, **kw):
        raise sa.exc.OperationalError("insert", {}, Exception("db down"))

    monkeypatch.setattr(dbmod.Database, "save_incidents", boom)
    with pytest.raises(sa.exc.OperationalError):
        w.run(64)
    monkeypatch.undo()
    assert len(rows(w, dbmod.inbox)) == 1 and rows(w, dbmod.incidents) == []
    assert rows(w, dbmod.rounds)[0]["status"] == "failed"
    assert w.run(124)["open_incidents"] == 1  # 下一輪照常處理


def test_split_batch_keeps_alerts_that_fired_and_resolved_within_the_window():
    [mq], [mq_done], [cim] = (parse_webhook(webhook(n))[0] for n in ("firing_mq", "resolved_mq", "firing_cim"))
    assert split_batch([mq, cim, mq_done]) == ([mq, cim], [mq_done])
    assert split_batch([mq_done, mq]) == ([mq], [])      # 先恢復又響：現在是響的
    assert split_batch([mq, mq]) == ([mq], [])           # 重複通知


FULL_CONFIG = """
grafana: {url: "https://grafana.corp", token: "glsa_x"}
webhook: {token: "hook", port: 9090}
database: {url: "postgresql+psycopg://u:p@db:5432/aiops"}
llm: {url: "https://llm.corp/v1", api_key: "sk", model: "m"}
es:
  datasource_uid: "abc"
  log_index: "factory-logs"
  trace_index: "factory-traces"
  id_fields: [eqp_id, lot_id]
  log_fields: {time: ts, service: app, level: severity, message: msg}
  trace_fields: {service: app, span: operation}
timezone: Asia/Tokyo
log_level: debug
debounce_seconds: 90
followup_seconds: 30
close_after_seconds: 600
edges:
  - [mq-broker, cim-eap]
  - cim-eap -> amhs-mcs
"""
MINIMAL = "grafana: {url: 'http://g', token: 't'}\nwebhook: {token: 'w'}\n"


def config(tmp_path, text):
    f = tmp_path / "config.yaml"
    f.write_text(text, encoding="utf-8")
    return str(f)


def test_every_setting_comes_from_the_one_yaml_file(tmp_path):
    s = Settings.load(config(tmp_path, FULL_CONFIG))
    assert (s.grafana_url, s.grafana_token, s.webhook_token, s.port) == ("https://grafana.corp", "glsa_x", "hook", 9090)
    assert s.database_url == "postgresql+psycopg://u:p@db:5432/aiops"
    assert (s.llm_api_url, s.llm_api_key, s.llm_model) == ("https://llm.corp/v1", "sk", "m")
    assert (s.es_datasource_uid, s.log_index, s.trace_index, s.id_fields) == ("abc", "factory-logs", "factory-traces", ("eqp_id", "lot_id"))
    assert (s.log_fields.time, s.log_fields.service, s.log_fields.level, s.log_fields.message) == ("ts", "app", "severity", "msg")
    assert (s.trace_fields.service, s.trace_fields.span, s.trace_fields.status) == ("app", "operation", "status.code")  # 沒寫的用預設
    assert (s.tz, s.log_level) == ("Asia/Tokyo", "DEBUG")
    assert (s.debounce_seconds, s.followup_seconds, s.close_after_seconds) == (90, 30, 600)
    assert s.edges == (("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs"))


def test_only_the_required_settings_need_to_be_written(tmp_path):
    s = Settings.load(config(tmp_path, MINIMAL))
    assert (s.debounce_seconds, s.followup_seconds, s.close_after_seconds, s.edges) == (60, 60, 300, ())
    assert (s.port, s.tz, s.database_url, s.llm_api_url, s.es_datasource_uid) == (8080, "Asia/Taipei", "sqlite:///aiops-lite.db", "", "")
    assert s.log_fields.level == "log.level" and s.id_fields == ("eqp_id", "lot_id", "carrier_id")


def test_the_example_config_is_valid_and_matches_the_defaults(tmp_path):
    from pathlib import Path
    text = (Path(__file__).resolve().parents[1] / "config.example.yaml").read_text(encoding="utf-8")
    s = Settings.load(config(tmp_path, text.replace('token: ""', 'token: "x"')))
    d = Settings(grafana_url="", grafana_token="", webhook_token="")
    assert (s.log_fields, s.trace_fields, s.id_fields, s.tz) == (d.log_fields, d.trace_fields, d.id_fields, d.tz)
    assert (s.debounce_seconds, s.followup_seconds, s.close_after_seconds) == (60, 60, 300) and len(s.edges) == 2


def test_config_path_defaults_and_the_single_environment_variable(tmp_path, monkeypatch):
    path = config(tmp_path, MINIMAL)
    monkeypatch.delenv("AIOPS_CONFIG_FILE", raising=False)
    with pytest.raises(ConfigError, match="/etc/aiops/config.yaml"):
        Settings.load()
    monkeypatch.setenv("AIOPS_CONFIG_FILE", path)
    assert Settings.load().grafana_url == "http://g"
    monkeypatch.setenv("GRAFANA_URL", "http://ignored")  # 其他環境變數一律不看
    assert Settings.load().grafana_url == "http://g"


def test_secrets_do_not_appear_when_settings_are_printed(tmp_path):
    shown = repr(Settings.load(config(tmp_path, FULL_CONFIG)))
    for secret in ("glsa_x", "hook", "sk", "u:p@db"):
        assert secret not in shown


@pytest.mark.parametrize("text, message", [
    ("", "grafana.url、grafana.token、webhook.token"),
    ("grafana: {url: 'http://g'}\nwebhook: {token: 'w'}\n", "grafana.token"),
    (MINIMAL + "grafan: {}\n", "不認得的設定 \\['grafan'\\]"),
    (MINIMAL + "es: {datasource: x}\n", "es 有不認得的設定"),
    (MINIMAL + "es: {log_fields: {levl: x}}\n", "es.log_fields 有不認得的設定"),
    (MINIMAL + "es: {log_fields: {level: ''}}\n", "不能是空的"),
    (MINIMAL + "es: {id_fields: []}\n", "id_fields"),
    (MINIMAL + "es: {id_fields: eqp_id}\n", "id_fields"),
    (MINIMAL + "grafana: [1]\n", "要是一組設定"),
    (MINIMAL + "llm: {api_key: 12345}\n", "要是文字"),
    (MINIMAL + "webhook: {token: w, port: http}\n", "webhook.port"),
    (MINIMAL + "webhook: {token: w, port: 70000}\n", "webhook.port"),
    (MINIMAL + "timezone: Taiwan/Hsinchu\n", "timezone"),
    (MINIMAL + "log_level: LOUD\n", "log_level"),
    (MINIMAL + "debounce_seconds: -1\n", "debounce_seconds"),
    (MINIMAL + "debounce_seconds: 60s\n", "debounce_seconds"),
    (MINIMAL + "debounce_seconds: 1.5\n", "debounce_seconds"),
    (MINIMAL + "debounce_seconds: true\n", "debounce_seconds"),
    (MINIMAL + "debounce_second: 60\n", "不認得的設定"),
    (MINIMAL + "followup_seconds: 0\n", "followup_seconds"),
    (MINIMAL + "edges: [[a]]\n", "關係邊"),
    (MINIMAL + "edges: [[a, a]]\n", "關係邊"),
    (MINIMAL + "edges: [a - b]\n", "關係邊"),
    (MINIMAL + "edges: {a: b}\n", "edges 要是清單"),
    ("- a\n", "最外層要是物件"),
    ("grafana: {url: [x", "不是合法的 YAML"),
])
def test_config_mistakes_are_rejected_at_startup(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        Settings.load(config(tmp_path, text))


def test_zero_debounce_is_allowed_but_zero_followup_is_not(tmp_path):
    assert Settings.load(config(tmp_path, MINIMAL + "debounce_seconds: 0\n")).debounce_seconds == 0
    assert Settings.load(config(tmp_path, MINIMAL + "close_after_seconds: 0\n")).close_after_seconds == 0


def test_configured_field_names_are_used_for_the_es_query_and_for_reading_the_documents(world):
    """公司的日誌欄位跟預設不同：查詢條件、排序、讀回來的內容都要照 config.yaml 的欄位名稱。"""
    from dataclasses import replace
    from src.grafana import LogFields, TraceFields
    w = world
    es_hits = {"responses": [
        {"hits": {"hits": [{"_source": {"ts": "2026-11-21T02:42:00Z", "app": "cim-eap", "severity": "ERROR",
                                        "msg": "T3 timeout", "eqp_id": "EQP-CVD-07", "lot_id": "LOT24A0031"}}]}},
        {"hits": {"hits": [{"_source": {"app": "cim-eap", "operation": "SECS S6F11"}}]}}]}
    w.fake.es = es_hits
    settings = replace(w.settings, log_fields=LogFields(time="ts", service="app", level="severity", message="msg"),
                       trace_fields=TraceFields(service="app", span="operation"))
    w.receive("firing_mq", "firing_cim")
    w.run(64, settings=settings)
    sent = [json.loads(line) for line in w.fake.es_requests[0].splitlines()]
    log_query, trace_query = sent[1], sent[3]
    assert log_query["sort"] == [{"ts": "desc"}] and trace_query["sort"] == [{"@timestamp": "desc"}]
    text = json.dumps(sent)
    assert '"app"' in text and '"severity"' in text and '"service.name"' not in text and '"log.level"' not in text
    assert '"status.code"' in text  # 沒改的欄位照預設
    prompt = w.llm_calls[0]["messages"][1]["content"]
    assert "T3 timeout" in prompt and "EQP-CVD-07" in prompt and "SECS S6F11" in prompt
