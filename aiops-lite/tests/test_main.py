"""端到端：假的 Grafana（回應格式照 13.0.7 的樣本）+ SQLite + 假的 LLM，一輪一輪跑 main.run。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import numpy as np
import pytest
import sqlalchemy as sa

from src import db as dbmod
from src.grafana import GrafanaError
from src.main import ConfigError, Settings, load_edges, run
from tests.conftest import ms, noise

NOW = datetime(2026, 11, 21, 0, 36, tzinfo=timezone.utc)
INCIDENT_START = NOW - timedelta(minutes=10)
INCIDENT_END = NOW + timedelta(minutes=2)
LLM_ANSWER = {"summary": "mq-broker 佇列積壓可能是源頭，下游 cim-eap 的 T3 逾時集中在機台 EQP-CVD-07",
              "systems": ["mq-broker", "cim-eap"], "ids": ["EQP-CVD-07"]}


def in_incident(ts):
    return (ts >= ms(INCIDENT_START)) & (ts < ms(INCIDENT_END))


def mq_depth(ts):
    return 60 + 20 * np.sin(2 * np.pi * ts / 86400000) + 3 * noise(ts, 1.0) + np.where(in_incident(ts), 300, 0)


def t3_timeouts(ts):
    return np.where(in_incident(ts), 2.0, (noise(ts, 2.0) > 0.998).astype(float))


def transfer(ts):
    return 30 + 5 * np.sin(2 * np.pi * ts / 86400000) + noise(ts, 3.0)


@pytest.fixture
def world(fake, tmp_path):
    for title, fn in (("MQ queue depth", mq_depth), ("CIM T3 timeout", t3_timeouts), ("AMHS transfer duration", transfer)):
        rule = fake.rule(title)["data"][0]
        fake.series[(rule["datasourceUid"], rule["model"].get("expr") or rule["model"].get("rawSql"))] = fn
    edges = tmp_path / "edges.yaml"
    edges.write_text("edges:\n  - [mq-broker, cim-eap]\n  - cim-eap -> amhs-mcs\n", encoding="utf-8")
    settings = Settings(grafana_url="http://grafana.test", grafana_token="t",
                        database_url=f"sqlite:///{tmp_path}/lite.db", edges_file=str(edges),
                        llm_api_url="http://llm.test/v1", llm_model="m", es_datasource_uid="es-uid",
                        log_index="factory-logs", trace_index="factory-traces")
    llm_calls = []

    def llm(request: httpx.Request) -> httpx.Response:
        llm_calls.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(LLM_ANSWER, ensure_ascii=False)}}]})

    class World:
        pass

    w = World()
    w.fake, w.settings, w.llm_calls, w.llm = fake, settings, llm_calls, httpx.MockTransport(llm)
    w.run = lambda minutes=0, **kw: run(kw.pop("settings", settings), now=NOW + timedelta(minutes=minutes),
                                        grafana_transport=fake.transport, llm_transport=kw.pop("llm", w.llm))
    w.db = lambda: dbmod.Database(settings.database_url)
    return w


def rows(w, table):
    database = w.db()
    with database.engine.connect() as conn:
        out = [dict(r) for r in conn.execute(sa.select(table)).mappings()]
    database.close()
    return out


def test_incident_opens_is_absorbed_and_closes_with_one_annotation(world):
    w = world
    # 第 1 輪：上游 mq-broker 積壓、下游 cim-eap 逾時暴增 → 一張事故、一筆註記
    first = w.run(0)
    assert first["status"] == "ok" and first["rules"] == 3 and first["open_incidents"] == 1 and first["errors"] == []
    [inc] = rows(w, dbmod.incidents)
    assert json.loads(inc["services"]) == ["cim-eap", "mq-broker"] and inc["severity"] == "P1" and inc["analyzed"]
    assert len(w.fake.annotations) == 1 and len(w.llm_calls) == 1
    note = w.fake.annotations[inc["annotation_id"]]
    l1, l2, l3 = note["text"].split("<br>")
    assert l1.startswith("【P1・進行中】受影響系統：cim-eap、mq-broker｜收斂告警 ") and "EQP-CVD-07" in l2
    assert l2.startswith("[LLM 推論] ") and "08:26 ～ 進行中（台灣時間）" in l3
    assert note["time"] == ms(INCIDENT_START) and "open" in note["tags"]
    count1 = inc["alert_count"]
    assert count1 >= 10
    # LLM 收到的是四包證據
    prompt = w.llm_calls[0]["messages"][1]["content"]
    assert "MQ queue depth" in prompt and '"upstream": "mq-broker"' in prompt and "EQP-CVD-07" in prompt

    # 第 2 輪（5 分鐘後）：異常還持續了 2 分鐘 → 同一張事故、同一筆註記；已經報過的點不重複計
    second = w.run(5)
    [inc2] = rows(w, dbmod.incidents)
    assert inc2["uid"] == inc["uid"] and count1 < inc2["alert_count"] <= count1 + 12 and inc2["quiet_rounds"] == 0
    assert len(w.fake.annotations) == 1 and second["open_incidents"] == 1
    assert len(w.llm_calls) == 1  # 成員規則沒變，只是多了幾個點：不重新問 LLM
    assert f"收斂告警 {inc2['alert_count']} 筆" in w.fake.annotations[inc["annotation_id"]]["text"]

    # 同一個時間點重跑一次：不重複計數（new_only）
    w.run(5)
    assert rows(w, dbmod.incidents)[0]["alert_count"] == inc2["alert_count"]

    # 逾時暴增在 00:38 結束，但 sparse_count 看的是「最近 30 分鐘累積幾筆」：暴增離開窗口之前（到 01:06）它每一輪都還有新的越線點，
    # 事故維持進行中。這是滾動窗的特性（README「已知限制」），不是結案邏輯的問題。
    for minutes in (10, 15, 20, 25, 30):
        w.run(minutes)
    [still] = rows(w, dbmod.incidents)
    assert still["status"] == "open" and still["quiet_rounds"] == 0
    assert json.loads(still["signals"])[w.fake.rule("MQ queue depth")["uid"]]["count"] == 13  # 12 分鐘 + 回落那一點

    # 之後連續 3 輪沒有新異常才結案
    w.run(35)
    w.run(40)
    assert [(r["status"], r["quiet_rounds"]) for r in rows(w, dbmod.incidents)] == [("open", 2)]
    last = w.run(45)
    [closed] = rows(w, dbmod.incidents)
    assert closed["status"] == "closed" and closed["quiet_rounds"] == 3 and last["open_incidents"] == 0
    note = w.fake.annotations[inc["annotation_id"]]
    assert len(w.fake.annotations) == 1 and "closed" in note["tags"] and "【P1・已結案】" in note["text"]
    assert "[LLM 推論]" in note["text"] and len(w.llm_calls) == 1  # 結案不再問 LLM，沿用最後的推論
    # 每一輪都有執行紀錄
    assert [r["status"] for r in rows(w, dbmod.rounds)] == ["ok"] * 11
    # 基準線只訓練一次（每條規則 14 天 = 5 段），之後每輪每條規則只查 1 次最近一小時
    assert len(w.fake.queries) == 3 * 5 + 11 * 3
    w.run(50)  # 結案之後再跑：不會重開、不會再寫註記
    assert [r["status"] for r in rows(w, dbmod.incidents)] == ["closed"] and len(w.fake.annotations) == 1


def test_quiet_system_produces_nothing(world):
    w = world
    out = w.run(-120)  # 事故發生前兩小時
    assert out == {"status": "ok", "rules": 3, "anomalies": 0, "open_incidents": 0, "errors": [], "incidents": []}
    assert w.fake.annotations == {} and w.llm_calls == [] and rows(w, dbmod.incidents) == []


def test_one_broken_rule_does_not_stop_the_others_and_is_not_counted_as_quiet(world):
    w = world
    broken = w.fake.rule("AMHS transfer duration")["data"][0]
    del w.fake.series[(broken["datasourceUid"], broken["model"]["rawSql"])]
    out = w.run(0)
    assert out["status"] == "degraded" and out["open_incidents"] == 1
    assert len(out["errors"]) == 1 and "amhs-mcs／AMHS transfer duration：QueryError" in out["errors"][0]
    assert rows(w, dbmod.rounds)[0]["status"] == "degraded"


def test_llm_outage_falls_back_and_is_retried_next_round(world):
    w = world

    def down(request):
        raise httpx.ConnectError("llm down")

    out = w.run(0, llm=httpx.MockTransport(down))
    [inc] = rows(w, dbmod.incidents)
    assert out["status"] == "degraded" and "未經 LLM 分析" in out["errors"][0] and not inc["analyzed"]
    note = w.fake.annotations[inc["annotation_id"]]
    assert "[未經分析] mq-broker：MQ queue depth；cim-eap：CIM T3 timeout" in note["text"]
    w.run(5)  # LLM 回來了
    [inc] = rows(w, dbmod.incidents)
    assert inc["analyzed"] and "[LLM 推論]" in w.fake.annotations[inc["annotation_id"]]["text"]
    assert len(w.fake.annotations) == 1


def test_without_llm_settings_the_incident_is_still_written(world):
    w = world
    settings = Settings(**{**w.settings.__dict__, "llm_api_url": "", "llm_model": ""})
    out = w.run(0, settings=settings)
    assert out["status"] == "ok" and w.llm_calls == []
    assert "[未經分析]" in next(iter(w.fake.annotations.values()))["text"]


def test_annotation_failure_is_retried_next_round_without_duplicates(world):
    w = world
    w.fake.fail.add("/api/annotations")
    out = w.run(0)
    [inc] = rows(w, dbmod.incidents)
    assert out["status"] == "degraded" and "註記寫入失敗" in out["errors"][-1]
    assert not inc["annotated"] and inc["annotation_id"] is None and w.fake.annotations == {}
    w.fake.fail.clear()
    w.run(20)  # 異常早就結束了：這一輪沒有新異常，但註記要補上
    [inc] = rows(w, dbmod.incidents)
    assert inc["annotated"] and len(w.fake.annotations) == 1


def test_grafana_outage_fails_the_round_and_leaves_a_record(world, monkeypatch):
    w = world
    monkeypatch.setattr("src.grafana.time.sleep", lambda s: None)
    w.fake.fail.add("/api/v1/provisioning")
    with pytest.raises(GrafanaError):
        w.run(0)
    [r] = rows(w, dbmod.rounds)
    assert r["status"] == "failed" and "GrafanaError" in r["errors"]


def test_changed_query_retrains_and_baseline_is_reused_otherwise(world):
    w = world
    w.run(-120)
    trained = len(w.fake.queries)
    w.run(-115)
    assert len(w.fake.queries) == trained + 3  # 沒有重訓
    rule = w.fake.rule("MQ queue depth")["data"][0]
    key = (rule["datasourceUid"], rule["model"]["expr"])
    rule["model"]["expr"] = "max(mq_queue_depth)"
    w.fake.series[(key[0], "max(mq_queue_depth)")] = w.fake.series[key]
    w.run(-110)
    assert len(w.fake.queries) == trained + 3 + 5 + 3  # 那條規則重抓 14 天


def test_new_rule_with_too_little_history_is_reported_not_judged(world):
    w = world
    rule = w.fake.rule("MQ queue depth")["data"][0]
    start = ms(NOW - timedelta(minutes=90))
    w.fake.series[(rule["datasourceUid"], rule["model"]["expr"])] = lambda ts: np.where(ts >= start, mq_depth(ts), np.nan)
    out = w.run(0)
    assert any("訓練資料不足" in e for e in out["errors"])
    assert all("mq-broker" not in i["services"] for i in rows(w, dbmod.incidents))


def test_settings_and_edges_are_validated_at_startup(tmp_path):
    with pytest.raises(ConfigError, match="GRAFANA_TOKEN"):
        Settings.from_env({"GRAFANA_URL": "http://g"})
    base = {"GRAFANA_URL": "http://g", "GRAFANA_TOKEN": "t"}
    with pytest.raises(ConfigError, match="AIOPS_CLOSE_AFTER_ROUNDS"):
        Settings.from_env({**base, "AIOPS_CLOSE_AFTER_ROUNDS": "0"})
    with pytest.raises(ConfigError, match="AIOPS_STEP_SECONDS"):
        Settings.from_env({**base, "AIOPS_STEP_SECONDS": "1m"})
    with pytest.raises(ConfigError, match="AIOPS_TZ"):
        Settings.from_env({**base, "AIOPS_TZ": "Taiwan/Hsinchu"})
    s = Settings.from_env({**base, "AIOPS_ID_FIELDS": "eqp_id, lot_id", "AIOPS_CLOSE_AFTER_ROUNDS": "5"})
    assert s.id_fields == ("eqp_id", "lot_id") and s.close_after_rounds == 5 and s.tz == "Asia/Taipei"

    assert load_edges(str(tmp_path / "missing.yaml")) == []
    f = tmp_path / "e.yaml"
    for bad in ("edges: [[a]]", "edges: [[a, a]]", "edges: [a - b]", "nodes: []", "edges: [[a, b"):
        f.write_text(bad, encoding="utf-8")
        with pytest.raises(ConfigError):
            load_edges(str(f))
    f.write_text("edges:\n  - [a, b]\n  - b -> c\n", encoding="utf-8")
    assert load_edges(str(f)) == [("a", "b"), ("b", "c")]
