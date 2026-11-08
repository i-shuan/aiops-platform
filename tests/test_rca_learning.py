"""路線 B：RCA 根因演算法可抽換（拓撲、PageRank、時序互相關）、段落註冊、漏報補錄、歷史事故經驗庫、
調參成效報表、規則升級器。"""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from aiops.engine.context import KnowledgeCase, PipelineContext, Target
from aiops.engine.errors import WorkflowError
from aiops.engine.pipeline import Pipeline, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import ExternalAnomalyIn, IngestAnomalies
from aiops.engine.signature import metric_names
from aiops.engine.types import AnomalyEvent, Incident
from aiops.interfaces.runner import run, run_context, workflow_path
from aiops.storage.hooks import StorageHooks

T = datetime(2026, 11, 8, 11, 0, tzinfo=timezone.utc)


def _ev(entity, minute, score, metric="latency", source_type=""):
    return AnomalyEvent(entity, metric, T + timedelta(minutes=minute), score, {}, 1.0, "detector.zscore", "detect",
                        source_type)


def _ctx(events, topology=()):
    ctx = PipelineContext(pipeline="rca_report", now=T)
    ctx.incidents = [Incident(events)]
    ctx.topology = list(topology)
    return ctx


def _rank(name, ctx, **params):
    return [c.entity for c in OperatorRegistry.create(name, params).rank(ctx.incidents[0], ctx)]


# ── B.1 根因演算法可抽換 ──


CHAIN = [("svc-a", "svc-b"), ("svc-b", "svc-c")]  # a 呼叫 b、b 呼叫 c：c 出事會一路拖垮 b、a


def test_topological_and_pagerank_agree_on_a_simple_chain():
    ctx = _ctx([_ev("svc-a", 0, 2), _ev("svc-b", 1, 8), _ev("svc-c", 2, 3)], CHAIN)
    assert _rank("rca.root_cause.topological", ctx)[0] == "svc-c"
    assert _rank("rca.root_cause.pagerank", ctx)[0] == "svc-c"


def test_pagerank_prefers_the_root_that_drags_down_more_services():
    """兩個都沒有異常上游的源頭：x 拖垮三個呼叫端，y 誰都沒影響、只是比較早出現。
    拓撲啟發式只看先後會把 y 排第一；PageRank 看影響範圍會把 x 排第一。"""
    topology = [("d1", "x"), ("d2", "x"), ("d3", "x"), ("y", "z")]  # z 沒異常
    events = [_ev("y", 0, 4), _ev("x", 3, 4), _ev("d1", 4, 2), _ev("d2", 4, 2), _ev("d3", 5, 2)]
    ctx = _ctx(events, topology)
    assert _rank("rca.root_cause.topological", ctx)[0] == "y"
    assert _rank("rca.root_cause.pagerank", ctx)[0] == "x"


def test_graph_algorithms_give_nothing_without_a_graph():
    ctx = _ctx([_ev("a", 0, 2), _ev("b", 1, 3)])
    assert _rank("rca.root_cause.topological", ctx) == [] and _rank("rca.root_cause.pagerank", ctx) == []


def _series(values, start=T - timedelta(minutes=30)):
    return pd.DataFrame({"ts": pd.date_range(start, periods=len(values), freq="60s"), "value": values})


def test_cross_correlation_finds_who_moves_first():
    rng = np.random.default_rng(7)
    leader = np.cumsum(rng.normal(0, 1, 120))
    follower = np.roll(leader, 3) + rng.normal(0, 0.1, 120)  # 晚 3 個點、形狀一樣
    ctx = _ctx([_ev("db", 0, 3), _ev("api", 2, 6)])
    ctx.rca_series = {("db", "latency"): _series(leader), ("api", "latency"): _series(follower)}
    op = OperatorRegistry.create("rca.root_cause.cross_correlation", {})
    candidates = op.rank(ctx.incidents[0], ctx)
    assert [c.entity for c in candidates] == ["db"] and "api（3 點" in candidates[0].reason


def test_cross_correlation_stays_quiet_when_series_are_unrelated_or_missing():
    rng = np.random.default_rng(3)
    ctx = _ctx([_ev("a", 0, 3), _ev("b", 1, 3)])
    ctx.rca_series = {("a", "latency"): _series(rng.normal(0, 1, 120)), ("b", "latency"): _series(rng.normal(0, 1, 120))}
    assert _rank("rca.root_cause.cross_correlation", ctx, min_corr=0.6) == []
    ctx.rca_series = {("a", "latency"): _series(rng.normal(0, 1, 120))}
    assert _rank("rca.root_cause.cross_correlation", ctx) == []  # 只有一個實體的時序


def test_incident_series_refetches_only_events_that_know_their_source():
    ctx = _ctx([_ev("cart", 0, 3, metric="cpu", source_type="mock"), _ev("fdc", 1, 3, metric="rf")])  # fdc 是外部匯入
    ctx = OperatorRegistry.create("ingestion.incident_series", {"before_minutes": 10, "after_minutes": 5}).execute(ctx)
    assert list(ctx.rca_series) == [("cart", "cpu")] and len(ctx.rca_series[("cart", "cpu")]) > 10


def _chain_incident(now, tag):
    items = [ExternalAnomalyIn(external_id=f"{tag}{i}", entity=e, metric="latency", ts=now - timedelta(minutes=m), score=s)
             for i, (e, m, s) in enumerate([("svc-a", 1, 2), ("svc-b", 2, 8), ("svc-c", 3, 3)])]
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline="it_otel", source="x", anomalies=items))
    wf = load_workflow(workflow_path("it_otel"))
    wf.topology = CHAIN
    return Pipeline(wf, StorageHooks()).run(now=now, targets=[]).to_result()["incidents"][0]


def _rca(incident_id, root="rca.root_cause.topological", workflow="rca_report", now=None):
    wf = load_workflow(workflow_path(workflow))
    next(n for n in wf.nodes if n.name == "root_cause").operator = root
    return Pipeline(wf, StorageHooks()).run(incident_id=incident_id, now=now).to_result()


def test_switching_the_root_cause_algorithm_is_only_a_yaml_change(now):
    inc = _chain_incident(now, "pr")
    for algo in ("rca.root_cause.topological", "rca.root_cause.pagerank"):
        out = _rca(inc["id"], algo, now=now)
        sections = {s["key"]: s["body"] for s in out["sections"]}
        assert out["root_cause"]["algorithm"] == algo and out["root_cause"]["candidates"][0]["entity"] == "svc-c"
        assert sections["root_cause_candidates"].startswith("1. svc-c") and algo.split(".")[-1] in sections["root_cause_candidates"]
        assert sections["propagation_path"].startswith("svc-c（")


def test_correlation_report_runs_end_to_end_and_degrades_without_enough_series(now):
    ctx = run_context("metrics_only", now=now, strict=False)
    out = run("rca_report_correlation", incident_id=_first_incident_id(ctx))
    assert out["root_cause"]["algorithm"] == "rca.root_cause.cross_correlation"
    assert "root_cause_candidates" in out["skipped"] and not out.get("errors")  # 只有一個實體，比不出先後


def _first_incident_id(ctx):
    return next(iter(ctx.incident_ids.values()))


def test_sections_are_registered_operators_and_unknown_ones_fail_at_load():
    names = OperatorRegistry.names("rca.section.")
    assert {"rca.section.summary", "rca.section.similar_cases", "rca.section.root_cause_candidates"} <= set(names)
    wf = load_workflow(workflow_path("rca_report"))
    next(n for n in wf.nodes if n.operator == "rca.report").params["sections"] = ["summary", "made_up"]
    with pytest.raises(WorkflowError, match="可用：.*similar_cases"):
        Pipeline(wf, StorageHooks())


def test_a_section_can_also_be_its_own_yaml_node(now, tmp_path):
    inc = _chain_incident(now, "own")
    path = tmp_path / "mini.yaml"
    path.write_text("name: mini\nnodes:\n  - {name: summary, operator: rca.section.summary}\n", encoding="utf-8")
    out = Pipeline(load_workflow(path), StorageHooks()).run(incident_id=inc["id"]).to_result()
    assert [s["key"] for s in out["sections"]] == ["summary"]


# ── B.3 漏報補錄 ──


def _missed(client, **over):
    body = {"verdict": "missed", "text": "凌晨 DB 卡住但沒告警", "author": "amy", "pipeline": "metrics_only",
            "entity": "metric_a", "detector": "detector.zscore",
            "time_range": {"start": "2026-11-08T05:58:00Z", "end": "2026-11-08T06:02:00Z"}, **over}
    return client.post("/api/v1/tuning/feedback", json=body)


def test_a_real_miss_can_be_reported_without_an_incident(client):
    r = _missed(client)
    assert r.status_code == 200
    fb = r.json()["result"]["feedback"]
    assert fb["incident_uid"] is None and fb["pipeline"] == "metrics_only" and fb["time_start"].startswith("2026-11-08T05:58")
    assert _missed(client, time_range=None).status_code == 422
    assert _missed(client, verdict="false_positive").status_code == 422  # 沒有事件群只能是漏報


def test_a_reported_miss_becomes_a_scoped_proposal_and_the_backtest_checks_that_window(client):
    fid = _missed(client).json()["result"]["feedback"]["id"]
    proposal = client.post("/api/v1/tuning/feedback/propose",
                           json={"feedback_id": fid, "proposer": "bot", "scope": "entity"}).json()["result"]["proposal"]
    assert (proposal["pipeline"], proposal["entity"]) == ("metrics_only", "metric_a")
    assert proposal["params"]["threshold"] < 3.0  # 漏報 = 太遲鈍，門檻調低
    body = {"proposal_id": proposal["id"], "metric": {"source_type": "mock", "query": "cpu"},
            "time_range": {"start": "2026-11-08T00:00:00Z", "end": "2026-11-08T12:00:00Z"}}
    windows = client.post("/api/v1/tuning/proposals/backtest", json=body).json()["result"]["proposal"]["backtest"]["missed_windows"]
    assert windows[0]["feedback_id"] == fid and windows[0]["proposed_caught"] is True  # mock 的尖峰剛好在 06:00
    body["time_range"] = {"start": "2026-11-08T07:00:00Z", "end": "2026-11-08T12:00:00Z"}
    windows = client.post("/api/v1/tuning/proposals/backtest", json=body).json()["result"]["proposal"]["backtest"]["missed_windows"]
    assert "不在回測範圍" in windows[0]["note"]


# ── B.4 歷史事故經驗庫 ──


def test_metric_names_ignore_labels_and_promql_functions():
    assert metric_names('sum(rate(http_requests_total{code=~"5.."}[5m]))') == {"http_requests_total"}
    assert metric_names('cpu{service="cart"}') == {"cpu"}


def test_confirmed_incidents_with_a_fix_show_up_as_similar_cases_later(client, now):
    first = run_context("metrics_only", now=now, strict=False)
    first_uid, first_id = first.incidents[0].uid, _first_incident_id(first)
    r = client.post("/api/v1/tuning/feedback", json={
        "incident_uid": first_uid, "verdict": "true_positive", "text": "真的有事", "author": "amy",
        "resolution": "重啟 metric_a 的收集器", "root_cause": "收集器記憶體洩漏"})
    assert r.status_code == 200 and r.json()["result"]["knowledge"]["entities"] == ["metric_a"]

    second = run_context("metrics_only", now=now + timedelta(hours=3), strict=False)
    report = run("rca_report", incident_id=_first_incident_id(second))
    similar = next(s["body"] for s in report["sections"] if s["key"] == "similar_cases")
    assert first_uid in similar and "重啟 metric_a 的收集器" in similar and "收集器記憶體洩漏" in similar
    assert "similar_cases" in run("rca_report", incident_id=first_id)["skipped"]  # 不會把自己當成相似案例


def test_knowledge_needs_a_confirmed_verdict_and_a_resolution(client):
    base = {"incident_uid": "inc-x", "text": "t", "author": "a"}
    assert client.post("/api/v1/tuning/feedback", json={**base, "verdict": "false_positive", "resolution": "x"}).status_code == 422
    assert client.post("/api/v1/tuning/feedback", json={**base, "verdict": "true_positive", "root_cause": "x"}).status_code == 422


def test_similarity_weights_entities_metrics_and_signals():
    section = OperatorRegistry.create("rca.section.similar_cases", {"min_similarity": 0.1})
    ctx = _ctx([_ev("db", 0, 3, metric="pg_lock_waits{db=\"main\"}")])
    case = lambda uid, entities, metrics: KnowledgeCase(uid, "", entities, metrics, [], "fix", "", T)  # noqa: E731
    ctx.knowledge_cases = [case("same-entity", ["db"], ["cpu"]), case("same-both", ["db"], ["pg_lock_waits"]),
                           case("unrelated", ["web"], ["http_requests_total"])]
    body = section.build(ctx.incidents[0], ctx)
    assert body.index("same-both") < body.index("same-entity") and "unrelated" not in body


# ── B.5 調參成效報表、規則升級器 ──


def _approve(client, params, pipeline="metrics_only", entity="metric_a", detector="detector.zscore"):
    pid = client.post("/api/v1/tuning/proposals", json={"source": "manual", "detector": detector, "params": params,
                                                         "proposer": "amy", "pipeline": pipeline,
                                                         "entity": entity}).json()["result"]["proposal"]["id"]
    client.post("/api/v1/tuning/proposals/backtest", json={
        "proposal_id": pid, "metric": {"source_type": "mock", "query": "cpu"},
        "time_range": {"start": "2026-11-08T00:00:00Z", "end": "2026-11-08T12:00:00Z"}})
    r = client.post("/api/v1/tuning/proposals/approve", json={"proposal_id": pid, "approver": "bob"})
    assert r.status_code == 200, r.text
    return pid


def test_tuning_metrics_compares_before_and_after_each_approval(client, now):
    ctx = run_context("metrics_only", now=now, strict=False)
    client.post("/api/v1/tuning/feedback", json={"incident_uid": ctx.incidents[0].uid, "verdict": "false_positive",
                                                 "detector": "detector.zscore", "text": "誤報", "author": "amy"})
    pid = _approve(client, {"threshold": 3.5})
    report = client.get("/api/v1/tuning/metrics?window_days=7").json()["result"]
    change = next(c for c in report["changes"] if c["proposal_id"] == pid)
    assert change["before"]["incidents"] == 1 and change["before"]["false_positive"] == 1
    assert change["after"]["incidents"] == 0 and "不代表因果" in report["caveat"]
    other_scope = _approve(client, {"threshold": 3.2}, pipeline="it_otel", entity="cart")
    change = next(c for c in client.get("/api/v1/tuning/metrics").json()["result"]["changes"] if c["proposal_id"] == other_scope)
    assert change["before"]["incidents"] == 0  # 別條管線的事件群不算在這個調參頭上


def test_rule_promoter_turns_repeated_tuning_into_a_rule_proposal(client, now):
    run_context("metrics_only", now=now, strict=False)  # 先有基準線
    for threshold in (3.2, 3.4, 3.6):
        _approve(client, {"threshold": threshold})
    out = client.post("/api/v1/tuning/rules/promote", json={"min_approvals": 3}).json()["result"]
    assert len(out["created"]) == 1
    rule = out["created"][0]["proposal"]
    assert rule["detector_name"] == "detector.rules" and (rule["pipeline"], rule["entity"]) == ("metrics_only", "metric_a")
    assert rule["params"]["lower"] < rule["params"]["upper"] and rule["status"] == "pending"
    again = client.post("/api/v1/tuning/rules/promote", json={"min_approvals": 3}).json()["result"]
    assert not again["created"] and "待處理" in again["skipped"][0]["reason"]
    assert not client.post("/api/v1/tuning/rules/promote", json={"min_approvals": 4}).json()["result"]["created"]


def test_rule_promoter_explains_why_it_skips(client, now):
    for threshold in (3.2, 3.4):
        _approve(client, {"threshold": threshold}, pipeline="*", entity="*")
    skipped = client.post("/api/v1/tuning/rules/promote", json={"min_approvals": 2}).json()["result"]["skipped"]
    assert "管線 × 實體" in skipped[0]["reason"]


def test_rules_detector_without_a_rule_stays_silent_when_rules_are_optional(now):
    df = pd.DataFrame({"ts": pd.date_range(T, periods=10, freq="60s"), "value": [50.0] * 9 + [95.0]})
    ctx = PipelineContext(pipeline="p", now=now, target=Target("mock", "q", "e"))
    ctx.raw_df = df
    op = OperatorRegistry.create("detector.rules", {"require_rule": False})
    op.node_name = "rules"
    assert op.execute(ctx).anomalies == []
    ctx.detector_params["rules"] = {"upper": 90}  # 規則升級器的提案核准後，就從這裡進來
    assert len(op.execute(ctx).anomalies) == 1
    with pytest.raises(Exception, match="至少要給"):
        OperatorRegistry.get("detector.rules")().validate_params()
