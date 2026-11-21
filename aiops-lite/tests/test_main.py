"""一輪一輪跑 main.process：真實的 Grafana webhook 樣本 + 假的 Grafana（ES、註記）+ SQLite + 假的 LLM。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import sqlalchemy as sa

from src import db as dbmod
from src.grafana import parse_webhook
from src.main import ConfigError, Settings, load_config, process, split_batch
from tests.conftest import webhook

T0 = datetime(2026, 11, 21, 2, 41, 10, tzinfo=timezone.utc)  # 樣本裡第一筆告警（MQ）的 startsAt
LLM_ANSWER = {"summary": "mq-broker 佇列積壓可能是源頭，下游 cim-eap 的 T3 逾時集中在機台 EQP-CVD-07",
              "systems": ["mq-broker", "cim-eap"], "ids": ["EQP-CVD-07"]}
BASE_ENV = {"GRAFANA_URL": "http://g", "GRAFANA_TOKEN": "t", "AIOPS_WEBHOOK_TOKEN": "w"}


@pytest.fixture
def world(fake, tmp_path):
    settings = Settings(grafana_url="http://grafana.test", grafana_token="t", webhook_token="w",
                        database_url=f"sqlite:///{tmp_path}/lite.db", llm_api_url="http://llm.test/v1", llm_model="m",
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


def test_timing_and_edges_come_from_the_yaml_file(tmp_path):
    f = tmp_path / "config.yaml"
    f.write_text("debounce_seconds: 90\nfollowup_seconds: 30\nclose_after_seconds: 600\n"
                 "edges:\n  - [mq-broker, cim-eap]\n  - cim-eap -> amhs-mcs\n", encoding="utf-8")
    s = Settings.load({**BASE_ENV, "AIOPS_CONFIG_FILE": str(f), "AIOPS_ID_FIELDS": "eqp_id, lot_id"})
    assert (s.debounce_seconds, s.followup_seconds, s.close_after_seconds) == (90, 30, 600)
    assert s.edges == (("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")) and s.id_fields == ("eqp_id", "lot_id")
    defaults = load_config(str(tmp_path / "missing.yaml"))  # 沒有設定檔：預設值、沒有關係邊
    assert defaults == {"debounce_seconds": 60, "followup_seconds": 60, "close_after_seconds": 300, "edges": ()}
    f.write_text("debounce_seconds: 0\n", encoding="utf-8")  # 0 = 不等（不建議，但合法）
    assert load_config(str(f))["debounce_seconds"] == 0


@pytest.mark.parametrize("text", [
    "debounce_seconds: -1", "debounce_seconds: 60s", "debounce_seconds: 1.5", "debounce_seconds: true",
    "followup_seconds: 0", "debounce_second: 60", "edges: [[a]]", "edges: [[a, a]]", "edges: [a - b]",
    "edges: {a: b}", "- a", "edges: [[a, b",
])
def test_config_mistakes_are_rejected_at_startup(tmp_path, text):
    f = tmp_path / "config.yaml"
    f.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(str(f))


def test_required_environment_is_checked(tmp_path):
    with pytest.raises(ConfigError, match="AIOPS_WEBHOOK_TOKEN"):
        Settings.load({"GRAFANA_URL": "http://g", "GRAFANA_TOKEN": "t"})
    with pytest.raises(ConfigError, match="AIOPS_TZ"):
        Settings.load({**BASE_ENV, "AIOPS_TZ": "Taiwan/Hsinchu", "AIOPS_CONFIG_FILE": str(tmp_path / "x")})
    with pytest.raises(ConfigError, match="AIOPS_PORT"):
        Settings.load({**BASE_ENV, "AIOPS_PORT": "http", "AIOPS_CONFIG_FILE": str(tmp_path / "x")})
