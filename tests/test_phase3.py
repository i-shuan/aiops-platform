"""第三階段：新偵測器、工廠收斂、拓撲根因推論、LLM 敘述與規則抽取（依據驗證與退回模板）。"""
import json
from datetime import datetime, timedelta

import pandas as pd
import pytest

from aiops.llm.base import BaseLLM, LLMError
from aiops.plugins.base import AnomalyEvent, Incident
from aiops.plugins.registry import DetectorRegistry, GrouperRegistry, SectionRegistry
from tests.test_alerts_rca import CONTEXT, EVENTS

T0 = datetime(2026, 11, 8)


def _df(values, **peers):
    return pd.DataFrame({"ts": pd.date_range(T0, periods=len(values), freq="60s"), "value": values, **peers})


# ── 3.1 偵測器 ──


def test_peer_ignores_shared_spike_but_catches_lone_deviation():
    base = [50.0] * 60
    everyone_up = _df([50.0] * 30 + [90.0] + [50.0] * 29, peer_0=[50.0] * 30 + [90.0] + [50.0] * 29)
    det = DetectorRegistry.get("peer")({}).fit(_df(base, peer_0=[50.5] * 60))
    assert not any(det.detect(everyone_up).is_anomaly)  # 大家一起高，不是我的問題

    lone = _df([50.0] * 30 + [90.0] + [50.0] * 29, peer_0=base)
    assert det.detect(lone).is_anomaly[30]


def test_rules_need_consecutive_breaches():
    det = DetectorRegistry.get("rules")({"upper": 80, "consecutive": 3}).fit(_df([0.0]))
    flags = det.detect(_df([50, 85, 50, 85, 85, 85, 50])).is_anomaly
    assert flags == [False, False, False, False, False, True, False]


def test_rules_without_bounds_is_rejected_by_api(client):
    body = {"metric": {"query": "cpu"}, "time_range": {"start": "2026-11-08T00:00:00", "end": "2026-11-08T01:00:00"},
            "detector_name": "rules", "params": {}}
    assert client.post("/api/v1/detection/run", json=body).status_code == 400


def test_chronic_catches_small_sustained_drift_that_zscore_misses():
    rng = pd.Series(range(120)).map(lambda i: 50.0 + (i % 5 - 2) * 0.5)  # 平穩，std≈0.7
    train = _df(rng.tolist())
    drift = _df(rng[:60].tolist() + (rng[60:] + 1.0).tolist())  # 後半段整體高 1（不到 2 個標準差）
    assert not any(DetectorRegistry.get("zscore")({}).fit(train).detect(drift).is_anomaly)
    assert any(DetectorRegistry.get("chronic")({"window": 20}).fit(train).detect(drift).is_anomaly)


def test_peer_detection_via_api_requires_peers(client):
    body = {"metric": {"query": "cpu-a"}, "time_range": {"start": "2026-11-08T00:00:00", "end": "2026-11-08T01:00:00"},
            "detector_name": "peer"}
    assert client.post("/api/v1/detection/run", json=body).status_code == 400
    ok = client.post("/api/v1/detection/run", json={**body, "peers": [{"query": "cpu-b"}, {"query": "cpu-c"}]})
    assert ok.status_code == 200, ok.text


# ── 3.1 工廠收斂 ──


def _inc(entity, minutes):
    e = AnomalyEvent(entity, "rf_power", T0 + timedelta(minutes=minutes), 5.0)
    return Incident([e], score=5.0)


def test_lot_path_chain_merges_upstream_downstream_tools():
    edges = [{"from": "LITHO-02", "to": "ETCH-01", "lots": 3}, {"from": "ETCH-01", "to": "CMP-01", "lots": 1}]
    grouper = GrouperRegistry.get("lot_path_chain")({"min_lots": 2})
    out = grouper.group([_inc("LITHO-02", 0), _inc("ETCH-01", 30), _inc("CMP-01", 40)], {"lot_edges": edges})
    assert sorted(i.entities for i in out) == [["CMP-01"], ["ETCH-01", "LITHO-02"]]  # 流向太少的不併


# ── 3.2 RCA 根因推論 ──


def _incident(*pairs):
    events = [AnomalyEvent(e, m, T0 + timedelta(minutes=t), s) for e, m, t, s in pairs]
    return Incident.combine([Incident([e], score=e.score) for e in events])


def test_root_cause_is_deepest_anomalous_dependency_in_it():
    inc = _incident(("frontend", "latency", 2, 3.0), ("cart", "latency", 1, 4.0), ("redis", "cpu", 0, 6.0))
    ctx = {"topology": [["frontend", "cart"], ["cart", "redis"]]}  # [caller, callee]
    body = SectionRegistry.get("root_cause_candidates")().build(inc, ctx).body
    assert body.startswith("1. redis")
    path = SectionRegistry.get("propagation_path")().build(inc, ctx).body
    assert path.startswith("redis（00:00:00） → cart（00:01:00） → frontend（00:02:00）")


def test_root_cause_is_upstream_tool_in_factory():
    inc = _incident(("ETCH-01", "rf_power", 30, 5.0), ("LITHO-02", "focus", 0, 4.0))
    ctx = {"lot_edges": [{"from": "LITHO-02", "to": "ETCH-01", "lots": 2}]}
    assert SectionRegistry.get("root_cause_candidates")().build(inc, ctx).body.startswith("1. LITHO-02")
    actions = SectionRegistry.get("suggested_actions")().build(inc, ctx).body
    assert "優先處理可能的源頭：LITHO-02" in actions and "保養" in actions


def test_trace_evidence_uses_trace_series_near_incident():
    inc = _incident(("cart", "latency", 0, 4.0))
    ctx = {"trace_series": [
        {"service": "cart", "ts": "2026-11-08T00:00:00", "count": 10, "error_rate": 0.5, "p95_ms": 300.0},
        {"service": "cart", "ts": "2026-11-09T00:00:00", "count": 99, "error_rate": 0.9, "p95_ms": 999.0},  # 太遠
        {"service": "other", "ts": "2026-11-08T00:00:00", "count": 5, "error_rate": 0.0, "p95_ms": 10.0},
    ]}
    body = SectionRegistry.get("trace_evidence")().build(inc, ctx).body
    assert body == "cart：請求 10 次；錯誤率最高 50%（2026-11-08T00:00:00）；p95 最高 300 ms（2026-11-08T00:00:00）"


# ── 3.2 LLM：依據驗證與退回模板 ──


class FakeLLM(BaseLLM):
    name = "fake"

    def __init__(self, reply=None, error=None):
        self.reply, self.error, self.calls = reply, error, 0

    def complete(self, system, prompt):
        self.calls += 1
        if self.error:
            raise LLMError(self.error)
        return self.reply if isinstance(self.reply, str) else json.dumps(self.reply, ensure_ascii=False)


@pytest.fixture
def chain_id(client):
    r = client.post("/api/v1/alerts/process", json={"preset": "it_otel", "events": EVENTS, "context": CONTEXT})
    return next(i["id"] for i in r.json()["result"] if i["entities"] == ["svc-a", "svc-b"])


def _use_llm(monkeypatch, llm):
    monkeypatch.setattr("aiops.domains.rca.commands.generate.get_llm", lambda: llm)
    monkeypatch.setattr("aiops.domains.tuning.commands.feedback.get_llm", lambda: llm)


def _narrative(client, chain_id):
    return client.post("/api/v1/rca/generate", json={"incident_id": chain_id, "preset": "it_otel",
                                                      "context": CONTEXT}).json()["result"]["narrative"]


def test_narrative_without_llm_uses_template(client, chain_id):
    n = _narrative(client, chain_id)
    assert n["generated_by"] == "template" and n["rejected"] is None
    assert n["claims"][0]["evidence"] == ["root_cause_candidates"]


def test_grounded_llm_narrative_is_accepted(client, chain_id, monkeypatch):
    reply = {"summary": "svc-b 最可能是源頭，svc-a 受到牽連。",
             "claims": [{"text": "svc-b 上游沒有其他異常", "evidence": ["root_cause_candidates"]}]}
    _use_llm(monkeypatch, FakeLLM(reply))
    n = _narrative(client, chain_id)
    assert n["generated_by"] == "llm" and n["summary"].startswith("svc-b")


@pytest.mark.parametrize("reply, reason", [
    ({"summary": "svc-c 也有問題", "claims": [{"text": "x", "evidence": ["summary"]}]}, "事件群以外的實體"),
    ({"summary": "錯誤率高達 87%", "claims": [{"text": "x", "evidence": ["summary"]}]}, "數字 87"),
    ({"summary": "ok", "claims": [{"text": "x", "evidence": ["made_up"]}]}, "不存在的段落"),
    ("這不是 JSON", "不是合法 JSON"),
])
def test_ungrounded_llm_narrative_falls_back(client, chain_id, monkeypatch, reply, reason):
    _use_llm(monkeypatch, FakeLLM(reply))
    n = _narrative(client, chain_id)
    assert n["generated_by"] == "template" and reason in n["rejected"]


def test_llm_outage_falls_back(client, chain_id, monkeypatch):
    _use_llm(monkeypatch, FakeLLM(error="timeout"))
    n = _narrative(client, chain_id)
    assert n["generated_by"] == "template" and "timeout" in n["rejected"]


# ── 3.2 回饋 -> 調參提案 ──


def _feedback(client, uid, verdict="false_positive", detector="zscore"):
    r = client.post("/api/v1/tuning/feedback", json={"incident_uid": uid, "verdict": verdict, "detector_name": detector,
                                                     "text": "半夜流量低時一直誤報，門檻太敏感", "author": "amy"})
    assert r.status_code == 200, r.text
    return r.json()["result"]["id"]


@pytest.fixture
def uid(client):
    r = client.post("/api/v1/alerts/process", json={"preset": "metrics_only", "events": EVENTS[:1]})
    return r.json()["result"][0]["uid"]


def test_feedback_requires_real_incident(client):
    r = client.post("/api/v1/tuning/feedback", json={"incident_uid": "inc-nope", "verdict": "missed",
                                                     "text": "x", "author": "amy"})
    assert r.status_code == 404


def test_template_rule_raises_threshold_on_false_positive(client, uid):
    fid = _feedback(client, uid)
    r = client.post(f"/api/v1/tuning/feedback/{fid}/propose", json={"proposer": "bot"}).json()["result"]
    assert r["generated_by"] == "template"
    assert r["proposal"]["params"] == {"threshold": 3.6} and r["proposal"]["status"] == "pending"
    again = client.post(f"/api/v1/tuning/feedback/{fid}/propose", json={"proposer": "bot"})
    assert again.status_code == 409  # 一筆回饋只轉一次


def test_valid_llm_rule_becomes_llm_proposal(client, uid, monkeypatch):
    _use_llm(monkeypatch, FakeLLM({"params": {"threshold": 4.0}, "reason": "夜間誤報多，調高門檻"}))
    r = client.post(f"/api/v1/tuning/feedback/{_feedback(client, uid)}/propose", json={"proposer": "bot"}).json()
    assert r["result"]["generated_by"] == "llm" and r["result"]["proposal"]["source"] == "llm"
    assert r["result"]["proposal"]["params"] == {"threshold": 4.0}


@pytest.mark.parametrize("params, reason", [
    ({"threshold": 2.0}, "方向與回饋相反"),
    ({"threshold": 9.0}, "變動超過"),
    ({"window": 5}, "不是可調參數"),
    ({"threshold": 50}, "超出範圍"),
])
def test_invalid_llm_rule_falls_back_to_template(client, uid, monkeypatch, params, reason):
    _use_llm(monkeypatch, FakeLLM({"params": params, "reason": "r"}))
    r = client.post(f"/api/v1/tuning/feedback/{_feedback(client, uid)}/propose", json={"proposer": "bot"}).json()
    assert r["result"]["generated_by"] == "template" and reason in r["result"]["rejected"]
    assert r["result"]["proposal"]["params"] == {"threshold": 3.6}


def test_true_positive_feedback_does_not_propose(client, uid):
    fid = _feedback(client, uid, verdict="true_positive")
    assert client.post(f"/api/v1/tuning/feedback/{fid}/propose", json={"proposer": "bot"}).status_code == 409


def test_rules_detector_cannot_be_auto_tuned(client, uid):
    fid = _feedback(client, uid, detector="rules")
    assert client.post(f"/api/v1/tuning/feedback/{fid}/propose", json={"proposer": "bot"}).status_code == 400
