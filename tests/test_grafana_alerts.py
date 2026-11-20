"""各團隊自己的 Grafana 告警 -> webhook -> 平台的外部異常（admin.ingest_grafana_alerts、POST /api/v1/ingestion/grafana-alerts）。

樣本是 Grafana 13.0.7 實際送出的內容（tests/fixtures/grafana_webhook/，怎麼抓的寫在那裡的 README）。
端到端那幾個測試走完整條路：webhook -> 外部異常 -> 那條管線下一輪收斂成事件群並通知 -> RCA 出報告（帶結論等級）。
"""
import copy
import json
from datetime import timedelta
from pathlib import Path

import pytest

from aiops.engine.errors import WorkflowParamError
from aiops.engine.pipeline import Pipeline, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import GrafanaAlertIngest
from aiops.interfaces.runner import run, workflow_path
from aiops.storage import crud
from aiops.storage.hooks import StorageHooks
from aiops.storage.models import ExternalAnomalyRow

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "grafana_webhook"
URL = "/api/v1/ingestion/grafana-alerts"


def _payload(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


FIRING = "firing_CheckoutCpuHigh"
NO_SERVICE = "firing_DiskAlmostFull"
RESOLVED = "resolved_CheckoutCpuHigh"


def _with_labels(labels: dict, fingerprint: str) -> dict:
    """從真實的 firing 樣本複製一份，換掉標籤與指紋（模擬同一個 Grafana 的另一條告警）。"""
    payload = copy.deepcopy(_payload(FIRING))
    alert = payload["alerts"][0]
    alert["labels"] = {"alertname": alert["labels"]["alertname"], "grafana_folder": "team-a", **labels}
    alert["fingerprint"] = fingerprint
    return payload


def _post(client, payload, pipeline="it_otel", source="team-a", **kw):
    return client.post(URL, params={"pipeline": pipeline, "source": source}, json=payload, **kw)


def _stored() -> list[dict]:
    return crud.find(ExternalAnomalyRow)


# ── 轉換 ──


def test_a_real_grafana_firing_notification_becomes_one_external_anomaly(client):
    r = _post(client, _payload(FIRING))
    assert r.status_code == 200, r.text
    result = r.json()["result"]
    assert (result["received"], result["accepted"], result["duplicates"], result["resolved_ignored"], result["rejected"]) == (1, 1, 0, 0, [])
    [row] = _stored()
    assert (row["pipeline"], row["source"], row["entity"], row["metric"], row["score"], row["status"]) == (
        "it_otel", "team-a", "checkout", "CheckoutCpuHigh", 10.0, "pending")
    assert row["external_id"].startswith("da8e608b969fe98c@")  # Grafana 的指紋 @ 收到的那一分鐘
    attrs = row["attrs"]
    assert attrs["labels"]["severity"] == "critical" and attrs["score_source"] == "severity=critical"
    assert attrs["starts_at"] == "2026-11-15T13:45:20+00:00" and attrs["values"] == {"A": 95, "B": 1}
    assert attrs["generator_url"].endswith("/alerting/grafana/cpu-high/view?orgId=1") and attrs["grafana_receiver"] == "to-aiops"
    # annotations（自由文字）不收：平台只存結構化欄位
    assert "annotations" not in attrs and "自由文字" not in json.dumps(row, ensure_ascii=False, default=str)


def test_an_alert_without_an_entity_label_is_rejected_with_the_reason_not_guessed(client):
    """真實樣本：DiskAlmostFull 只有 host 標籤、沒有 service。不拿 host 當實體猜，拒收並說明要哪個標籤；仍回 200，不讓 Grafana 重送。"""
    r = _post(client, _payload(NO_SERVICE))
    assert r.status_code == 200
    result = r.json()["result"]
    assert result["accepted"] == 0 and _stored() == []
    [rejected] = result["rejected"]
    assert rejected["alertname"] == "DiskAlmostFull"
    assert "service、service_name" in rejected["reason"] and "host" in rejected["reason"]


def test_a_resolved_notification_is_not_turned_into_an_anomaly(client):
    """結案通知不轉：平台的事件群照它自己的規則結案（安靜滿 resolve_after_minutes）。"""
    result = _post(client, _payload(RESOLVED)).json()["result"]
    assert (result["accepted"], result["resolved_ignored"], result["rejected"]) == (0, 1, [])
    assert _stored() == []


def test_repeats_within_a_minute_are_one_point_and_later_repeats_are_new_points(client, monkeypatch, now):
    """Grafana 對還沒結束的告警會定期重送：同一分鐘只收一次；下一次重送記成新的點，事件群才不會在告警還沒結束時就被結案。"""
    first = run("admin_ingest_grafana_alerts", now=now, request=GrafanaAlertIngest(pipeline="it_otel", source="team-a", payload=_payload(FIRING)))
    again = run("admin_ingest_grafana_alerts", now=now + timedelta(seconds=30),
                request=GrafanaAlertIngest(pipeline="it_otel", source="team-a", payload=_payload(FIRING)))
    later = run("admin_ingest_grafana_alerts", now=now + timedelta(minutes=5),
                request=GrafanaAlertIngest(pipeline="it_otel", source="team-a", payload=_payload(FIRING)))
    assert (first["accepted"], again["accepted"], again["duplicates"], later["accepted"]) == (1, 0, 1, 1)
    assert sorted(r["ts"] for r in _stored()) == [now.replace(tzinfo=None), (now + timedelta(minutes=5)).replace(tzinfo=None)]
    other = run("admin_ingest_grafana_alerts", now=now,  # 另一個團隊（source）推一樣的指紋：各自算，不互相去重
                request=GrafanaAlertIngest(pipeline="it_otel", source="team-b", payload=_payload(FIRING)))
    assert other["accepted"] == 1


def test_severity_maps_to_a_score_and_unknown_severity_uses_the_default(client):
    payloads = {"warning": "fp-warn", "P1": "fp-p1", "whatever": "fp-unknown"}
    for severity, fp in payloads.items():
        assert _post(client, _with_labels({"service": "cart", "severity": severity}, fp)).json()["result"]["accepted"] == 1
    _post(client, _with_labels({"service": "cart"}, "fp-none"))  # 沒有 severity 標籤
    got = {r["external_id"].split("@")[0]: (r["score"], r["attrs"]["score_source"]) for r in _stored()}
    assert got == {"fp-warn": (3.0, "severity=warning"), "fp-p1": (6.0, "severity=p1"),
                   "fp-unknown": (3.0, "default"), "fp-none": (3.0, "default")}


def test_default_score_null_rejects_unknown_severity_and_params_are_checked_at_load(now):
    op = OperatorRegistry.create("admin.ingest_grafana_alerts", {"severity_scores": {"critical": 10}, "default_score": None})
    from aiops.engine.context import PipelineContext

    ctx = PipelineContext(pipeline="admin_ingest_grafana_alerts", now=now,
                          request=GrafanaAlertIngest(pipeline="p", source="s", payload=_with_labels({"service": "a", "severity": "low"}, "x")))
    result = op.execute(ctx).result
    assert result["accepted"] == 0 and "不在 severity_scores 裡" in result["rejected"][0]["reason"]
    for bad in ({"severity_scores": {"critical": -1}}, {"default_score": -2}, {"entity_labels": []}, {"metric_label": ""}):
        with pytest.raises(WorkflowParamError):
            OperatorRegistry.create("admin.ingest_grafana_alerts", bad)


def test_the_entity_label_order_comes_from_the_yaml(now):
    from aiops.engine.context import PipelineContext

    op = OperatorRegistry.create("admin.ingest_grafana_alerts", {"entity_labels": ["app", "service"]})
    ctx = PipelineContext(pipeline="x", now=now, request=GrafanaAlertIngest(
        pipeline="p", source="s", payload=_with_labels({"service": "checkout", "app": "checkout-v2"}, "y")))
    op.execute(ctx)
    assert [r["entity"] for r in _stored()] == ["checkout-v2"]
    shipped = load_workflow(workflow_path("admin_ingest_grafana_alerts")).nodes[0].params
    assert shipped["entity_labels"] == ["service", "service_name"] and shipped["default_score"] == 3


# ── API 入口 ──


def test_the_webhook_needs_the_platform_token_and_valid_query_parameters(client):
    from fastapi.testclient import TestClient

    from aiops.interfaces.app import create_app

    assert TestClient(create_app()).post(URL, params={"pipeline": "it_otel"}, json=_payload(FIRING)).status_code == 401
    assert client.post(URL, json=_payload(FIRING)).status_code == 422                       # 沒帶 pipeline
    assert _post(client, _payload(FIRING), pipeline="../x").status_code == 422             # 管線名稱白名單
    assert _post(client, _payload(FIRING), source="team a").status_code == 422             # 來源代號白名單
    assert _post(client, _payload(FIRING), pipeline="it_otle").status_code == 404           # 管線名稱打錯：對方 Grafana 看得到送達失敗
    assert _post(client, _payload(FIRING), pipeline="rca_report").status_code == 400        # 不是偵測管線
    r = client.post(URL, params={"pipeline": "it_otel"}, json={"receiver": "x"})          # 不是 Grafana 的告警格式
    assert r.status_code == 400 and "alerts" in r.json()["message"]
    assert _stored() == []


def test_unknown_top_level_fields_from_newer_grafana_versions_are_fine(client):
    """第三方格式不用嚴格模型：真實樣本本來就帶 appVersion、truncatedAlerts 這些欄位，之後的版本再多欄位也不該進不來。"""
    payload = _payload(FIRING)
    assert "appVersion" in payload
    payload["someFieldFromGrafana14"] = {"x": 1}
    payload["alerts"][0]["anotherNewField"] = True
    assert _post(client, payload).json()["result"]["accepted"] == 1


# ── 端到端：webhook -> 下一輪收斂成事件群 -> RCA ──


def _detection_round(now, topology=None):
    """那條管線的批次層：這一輪沒有逐序列節點要跑（targets=[]），只處理推進來的外部異常。"""
    wf = load_workflow(workflow_path("it_otel"))
    if topology is not None:
        wf.topology = topology
    return Pipeline(wf, StorageHooks()).run(now=now, targets=[])


def test_end_to_end_alerts_from_related_services_become_one_incident_with_an_rca_report(client, sent, now, monkeypatch):
    monkeypatch.delenv("AIOPS_GRAFANA_URL", raising=False)  # 沒設定 Grafana 資料源：RCA 不撈 trace／日誌
    for service, fp in (("checkout", "fp-1"), ("payment", "fp-2"), ("cart", "fp-3")):
        assert _post(client, _with_labels({"service": service, "severity": "critical"}, fp)).json()["result"]["accepted"] == 1
    ctx = _detection_round(now, topology=[("cart", "checkout"), ("checkout", "payment")])
    assert [sorted(i.entities) for i in ctx.incidents] == [["cart", "checkout", "payment"]]  # 三張告警收斂成一件事
    [incident] = ctx.incidents
    assert incident.severity == "P0" and len(sent) >= 1
    assert all(e.detector == "external:team-a" for e in incident.events)
    assert all(r["status"] == "processed" for r in _stored())
    # RCA：沒有 trace（ES 沒設定）-> 依拓撲推測，報告標成 topology_inference，不是定論
    inc_id = ctx.incident_ids[incident.fingerprint]
    rca = client.post("/api/v1/rca", json={"incident_id": inc_id, "workflow": "rca_report"}).json()["result"]
    assert rca["root_cause"]["conclusion_type"] == "topology_inference"
    assert rca["root_cause"]["candidates"][0]["entity"] == "payment"
    assert {s["key"]: s["body"] for s in rca["sections"]}["root_cause_candidates"].startswith("【結論等級：拓撲推測")


def test_end_to_end_a_still_firing_alert_keeps_one_incident_open_instead_of_new_cards(client, sent, now):
    """Grafana 重送還沒結束的告警：同一張事故卡吸收新的點，不會每次重送都開一張新卡。"""
    run("admin_ingest_grafana_alerts", now=now, request=GrafanaAlertIngest(pipeline="it_otel", source="team-a", payload=_payload(FIRING)))
    first = _detection_round(now)
    assert len(first.incidents) == 1
    run("admin_ingest_grafana_alerts", now=now + timedelta(minutes=5),
        request=GrafanaAlertIngest(pipeline="it_otel", source="team-a", payload=_payload(FIRING)))
    second = _detection_round(now + timedelta(minutes=5))
    assert second.incidents == []  # 被進行中的事件群吸收
    [open_incident] = second.open_incidents
    assert open_incident.uid == first.incidents[0].uid and len(open_incident.incident.events) == 2


def test_end_to_end_no_alerts_means_the_platform_stays_quiet(client, sent, now):
    """對方沒設規則、沒有推告警：平台不產生任何事件群、不送任何告警（心跳除外）。"""
    ctx = _detection_round(now)
    assert ctx.incidents == [] and ctx.anomalies == []
    assert [n for n in sent if n.labels.get("alertname") != "AIOpsWatchdog"] == []


def test_an_unreadable_starts_at_is_kept_as_is_with_a_warning(client, caplog):
    payload = _with_labels({"service": "cart"}, "fp-time")
    payload["alerts"][0]["startsAt"] = "yesterday-ish"
    assert _post(client, payload).json()["result"]["accepted"] == 1
    assert _stored()[0]["attrs"]["starts_at"] == "yesterday-ish" and "startsAt 看不懂" in caplog.text
