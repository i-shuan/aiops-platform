"""平台看門狗（pipelines/watchdog/platform_watchdog.yaml）、平台自身資料源（source_type = platform）、心跳告警、偵測入口政策。

偵測演算法只當平台自己的看門狗（CLAUDE.md §1.1 第 9 點）：看門狗只讀平台自己的紀錄（pipeline_runs、外部告警的接收紀錄），
呼叫端給的序列（/runs 帶 targets、調參回測、序列畫像）只准平台看門狗管線、平台自己的紀錄，其他 403。
這裡的紀錄是測試直接寫進資料庫的（合成），門檻在模擬紀錄上的回放數字見 docs/WATCHDOG.md。
"""
import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
import yaml

from aiops.engine.errors import BadRequest
from aiops.engine.pipeline import Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.platform_series import HEARTBEAT_STATUS, PLATFORM_SOURCE, bucket_counts, parse_platform_query, selects
from aiops.engine.requests import GrafanaAlertIngest
from aiops.interfaces.runner import run, workflow_path
from aiops.storage import crud
from aiops.storage.hooks import StorageHooks
from aiops.storage.models import ExternalAnomalyRow, PipelineRunRow
from aiops.storage.repository import Anomalies, ExternalAnomalies, StreamHealth

ROOT = Path(__file__).resolve().parents[1]
T = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
STEP = timedelta(minutes=5)
WATCHDOG = ROOT / "aiops" / "pipelines" / "watchdog" / "platform_watchdog.yaml"
GRAFANA_FIRING = ROOT / "tests" / "fixtures" / "grafana_webhook" / "firing_CheckoutCpuHigh.json"


def _naive(t: datetime) -> datetime:
    return t.replace(tzinfo=None)


def _seed_runs(pipeline: str, start: datetime, end: datetime, failed_at: set[datetime] = frozenset()) -> None:
    """[start, end) 每 5 分鐘一輪；failed_at 裡的那幾輪整輪失敗。"""
    rows, t = [], start
    while t < end:
        failed = t in failed_at
        rows.append({"pipeline": pipeline, "run_at": _naive(t), "status": "failed" if failed else "ok",
                     "failure_type": "infrastructure_error" if failed else "", "message": "", "targets": 1,
                     "failed_targets": 0})
        t += STEP
    crud.create_many(PipelineRunRow, rows)


def _seed_heartbeats(source: str, start: datetime, end: datetime, every: timedelta = STEP) -> None:
    rows, t, i = [], start, 0
    while t < end:
        rows.append({"pipeline": "it_otel", "source": source, "external_id": f"hb-{i}", "entity": "heartbeat",
                     "metric": "AIOpsHeartbeat", "ts": _naive(t), "score": 0.0, "value": None, "attrs": {},
                     "received_at": _naive(t), "status": HEARTBEAT_STATUS})
        t, i = t + every, i + 1
    crud.create_many(ExternalAnomalyRow, rows)


def _watchdog(now: datetime, extra_targets: list[TargetSpec] = ()):
    wf = load_workflow(WATCHDOG)
    if extra_targets:
        wf = Workflow.model_validate(wf.model_dump() | {"targets": [*wf.model_dump()["targets"], *[t.model_dump() for t in extra_targets]]})
    return Pipeline(wf, StorageHooks()).run(now=now)


def _fired(ctx) -> set[tuple[str, str]]:
    return {(e.entity, e.detector_node) for e in ctx.detected_anomalies}


# ── 平台自身資料源：查詢語法與每格筆數（純函式）──


def test_platform_queries_are_parsed_strictly():
    q = parse_platform_query("runs/metrics_only/failed")
    assert (q.table, q.key, q.field) == ("runs", "metrics_only", "failed")
    assert parse_platform_query("alerts/*/heartbeats").key == "*"
    for bad in ("runs/metrics_only", "runs//failed", "logs/x/count", "runs/x/heartbeats", "runs/*/rounds", "a b/c/d"):
        with pytest.raises(ValueError):
            parse_platform_query(bad)
    rounds, failed, not_ok = (parse_platform_query(f"runs/p/{f}") for f in ("rounds", "failed", "not_ok"))
    assert [selects(q, s) for q in (rounds, failed, not_ok) for s in ("ok", "degraded", "failed")] == [
        True, True, True, False, False, True, False, True, True]
    alerts, beats, pushes = (parse_platform_query(f"alerts/s/{f}") for f in ("alerts", "heartbeats", "pushes"))
    assert [selects(q, s) for q in (alerts, beats, pushes) for s in ("pending", HEARTBEAT_STATUS)] == [
        True, False, False, True, True, True]


def test_buckets_count_events_keep_zeros_and_drop_the_unfinished_bucket():
    start, end = T - timedelta(minutes=20), T
    times = [T - timedelta(minutes=19), T - timedelta(minutes=14), T - timedelta(minutes=2)]  # 11:41、11:46、11:58
    df = bucket_counts(times, start, end, 300, now=T - timedelta(minutes=1))  # 最後一格 11:55～12:00 還沒過完
    assert list(df["ts"].dt.strftime("%H:%M")) == ["11:40", "11:45", "11:50"]
    assert list(df["value"]) == [1.0, 1.0, 0.0]  # 沒有紀錄的格子是 0，不是缺值
    full = bucket_counts(times, start, end, 300, now=T)
    assert list(full["value"]) == [1.0, 1.0, 0.0, 1.0]


# ── 看門狗端到端 ──


def test_watchdog_stays_quiet_when_the_platform_is_healthy():
    _seed_runs("metrics_only", T - timedelta(days=14), T)
    ctx = _watchdog(T)
    assert ctx.errors == [] and ctx.detected_anomalies == [] and ctx.incidents == []


def test_watchdog_catches_a_stopped_schedule():
    _seed_runs("metrics_only", T - timedelta(days=14), T - timedelta(minutes=30))  # 最後半小時一輪都沒跑
    ctx = _watchdog(T)
    assert ("metrics_only", "stopped_rounds") in _fired(ctx)
    [incident] = ctx.incidents
    assert incident.entities == ["metrics_only"] and {e.metric for e in incident.events} == {"runs/metrics_only/rounds"}


def test_watchdog_catches_a_burst_of_failed_rounds_with_the_sparse_count_detector():
    rare = {T - timedelta(days=d, hours=3) for d in (12, 8, 3)}  # 兩週裡零星失敗 3 輪：平常的發生率
    burst = {T - timedelta(minutes=m) for m in (40, 35, 30, 25, 20)}  # 最近 40 分鐘連續失敗 5 輪
    _seed_runs("metrics_only", T - timedelta(days=14), T, failed_at=rare | burst)
    ctx = _watchdog(T)
    assert ("metrics_only", "failures") in _fired(ctx)
    scores = [e.score for e in ctx.detected_anomalies if e.detector_node == "failures"]
    assert min(scores) >= 4  # sparse_count 的分數是 −log10 機率，門檻 4


def test_the_usual_rate_of_rare_failures_does_not_fire():
    rare = {T - timedelta(days=d, hours=3) for d in (12, 8, 3)} | {T - timedelta(hours=2)}
    _seed_runs("metrics_only", T - timedelta(days=14), T, failed_at=rare)
    assert ("metrics_only", "failures") not in _fired(_watchdog(T))


def test_watchdog_catches_a_team_whose_heartbeat_stopped_and_not_one_that_keeps_beating():
    targets = [TargetSpec(source_type=PLATFORM_SOURCE, query=f"alerts/{s}/heartbeats", entity=s, detectors=["stopped_heartbeats"])
               for s in ("team-a", "team-b")]
    _seed_runs("metrics_only", T - timedelta(days=2), T)
    _seed_heartbeats("team-a", T - timedelta(days=2), T - timedelta(minutes=40))  # 40 分鐘前起一筆心跳都沒有
    _seed_heartbeats("team-b", T - timedelta(days=2), T)
    fired = _fired(_watchdog(T, targets))
    assert ("team-a", "stopped_heartbeats") in fired and ("team-b", "stopped_heartbeats") not in fired


def test_a_bad_platform_query_is_a_configuration_error_not_silence():
    targets = [TargetSpec(source_type=PLATFORM_SOURCE, query="runs/metrics_only/failures", entity="x", detectors=["failures"])]
    _seed_runs("metrics_only", T - timedelta(days=1), T)
    ctx = _watchdog(T, targets)
    assert any(e.entity == "x" and "欄位只能是" in e.message for e in ctx.errors)
    assert [r["status"] for r in StreamHealth.list(entity="x")] == ["config_error"]


def test_the_fetch_window_covers_a_full_sparse_count_window():
    """稀疏計數要湊滿一個窗口才判：fetch 的 lookback 不夠，它每一輪都會棄權、什麼都抓不到（第一版就是這樣，測試抓到的）。"""
    from aiops.engine.registry import OperatorRegistry

    wf = load_workflow(WATCHDOG)
    fetch = next(n.params for n in wf.nodes if n.operator == "ingestion.fetch")
    for node in (n for n in wf.nodes if n.operator == "detector.sparse_count"):
        window = OperatorRegistry.create(node.operator, node.params).param("window")
        assert fetch["lookback_minutes"] * 60 // fetch["step_seconds"] >= window, node.name


def test_the_watchdog_records_its_own_rounds_too():
    _seed_runs("metrics_only", T - timedelta(days=1), T)
    _watchdog(T)
    from aiops.storage.repository import PipelineRuns

    assert [r["status"] for r in PipelineRuns.list(pipeline="platform_watchdog")] == ["ok"]


# ── 心跳告警：只給看門狗數，不進任何管線 ──


def _heartbeat_payload() -> dict:
    payload = json.loads(GRAFANA_FIRING.read_text(encoding="utf-8"))
    alert = payload["alerts"][0]
    alert["labels"] = {"alertname": "AIOpsHeartbeat", "aiops_heartbeat": "true", "grafana_folder": "team-a"}
    alert["fingerprint"] = "heartbeat-fp"
    return payload


def test_a_heartbeat_alert_is_stored_for_the_watchdog_and_never_becomes_an_incident(client, now):
    r = client.post("/api/v1/ingestion/grafana-alerts", params={"pipeline": "it_otel", "source": "team-a"}, json=_heartbeat_payload())
    result = r.json()["result"]
    assert r.status_code == 200 and (result["accepted"], result["heartbeats"], result["rejected"]) == (0, 1, [])
    [row] = crud.find(ExternalAnomalyRow)
    assert row["status"] == HEARTBEAT_STATUS and row["source"] == "team-a"
    assert ExternalAnomalies.pending("it_otel") == []  # 使用者管線不會載入它
    ctx = Pipeline(load_workflow(workflow_path("it_otel")), StorageHooks()).run(now=now)
    assert ctx.anomalies == [] and ctx.incidents == []
    # API 這條路的時間是牆鐘（不是測試的 now）：查一段夠寬的範圍
    wide = (datetime(2000, 1, 1), datetime(2100, 1, 1))
    assert [status for _, status in ExternalAnomalies.received("team-a", *wide)] == [HEARTBEAT_STATUS]


def test_heartbeat_pushes_are_counted_by_the_receipt_time_of_the_round(now):
    run("admin_ingest_grafana_alerts", now=now, request=GrafanaAlertIngest(pipeline="it_otel", source="team-a", payload=_heartbeat_payload()))
    [row] = crud.find(ExternalAnomalyRow)
    assert row["received_at"] == _naive(now)


# ── 偵測入口政策：呼叫端給的序列只准平台看門狗管線、平台自己的紀錄 ──


def test_the_shipped_policy_is_watchdog_only_and_platform_records_only(production_detection_scopes):
    assert dict(production_detection_scopes) == {"watchdog": frozenset({PLATFORM_SOURCE})}


def test_the_watchdog_pipeline_reads_nothing_but_platform_records():
    raw = yaml.safe_load(WATCHDOG.read_text(encoding="utf-8"))
    assert raw["targets"] and {t["source_type"] for t in raw["targets"]} == {PLATFORM_SOURCE}
    for t in raw["targets"]:
        assert not t.get("peers") and not t.get("peer_group")


def _target(source_type: str, query: str) -> dict:
    return {"source_type": source_type, "query": query, "entity": "x"}


@pytest.mark.usefixtures("production_detection_scopes")
def test_runs_with_caller_series_only_work_on_the_watchdog_with_platform_records(client):
    _seed_runs("metrics_only", T - timedelta(days=1), T)
    cases = {
        ("it_otel", "mock"): 403,                     # 使用者管線
        ("sample_metrics_only", "mock"): 403,         # 樣本不是出貨的看門狗資料夾
        ("platform_watchdog", "mock"): 403,           # 看門狗也不准跑外部序列
        ("platform_watchdog", "elasticsearch"): 403,
        ("nope_pipeline", "platform"): 404,
    }
    for (workflow, source), status in cases.items():
        query = "runs/metrics_only/rounds" if source == "platform" else "cpu"
        r = client.post("/api/v1/runs", json={"workflow": workflow, "targets": [_target(source, query)]})
        assert r.status_code == status, (workflow, source, r.text)
    ok = client.post("/api/v1/runs", json={"workflow": "platform_watchdog", "now": T.isoformat(),
                                           "targets": [{**_target("platform", "runs/metrics_only/rounds"), "detectors": ["stopped_rounds"]}]})
    assert ok.status_code == 200, ok.text
    assert "偵測演算法只用來監控 AIOps 平台自己" in client.post("/api/v1/runs", json={
        "workflow": "it_otel", "targets": [_target("mock", "cpu")]}).json()["message"]


def _proposal(client, pipeline: str) -> int:
    r = client.post("/api/v1/tuning/proposals", json={"source": "manual", "detector": "detector.sparse_count",
                                                      "params": {"threshold": 5.0}, "proposer": "ops", "pipeline": pipeline})
    assert r.status_code == 200, r.text
    return r.json()["result"]["proposal"]["id"]


@pytest.mark.usefixtures("production_detection_scopes")
def test_backtest_only_runs_on_watchdog_proposals_and_platform_records(client):
    _seed_runs("metrics_only", T - timedelta(days=3), T)
    window = {"start": (T - timedelta(days=2)).isoformat(), "end": T.isoformat(), "step_seconds": 300}
    blocked = [(_proposal(client, "it_otel"), "platform", "runs/metrics_only/failed"),   # 使用者管線的提案
               (_proposal(client, "platform_watchdog"), "mock", "cpu"),                 # 看門狗提案、外部序列
               (_proposal(client, "*"), "elasticsearch", "{}")]                          # 不限管線的提案、外部序列
    for pid, source, query in blocked:
        r = client.post("/api/v1/tuning/proposals/backtest",
                        json={"proposal_id": pid, "metric": {"source_type": source, "query": query}, "time_range": window})
        assert r.status_code == 403, (pid, source, r.text)
    assert client.post("/api/v1/tuning/proposals/backtest", json={
        "proposal_id": 9999, "metric": {"source_type": "platform", "query": "runs/metrics_only/failed"}, "time_range": window}).status_code == 404
    for pid in (_proposal(client, "platform_watchdog"), _proposal(client, "*")):
        r = client.post("/api/v1/tuning/proposals/backtest", json={
            "proposal_id": pid, "metric": {"source_type": "platform", "query": "runs/metrics_only/failed"}, "time_range": window})
        assert r.status_code == 200, r.text


@pytest.mark.usefixtures("production_detection_scopes")
def test_profile_only_runs_on_the_watchdog_with_platform_records(client):
    _seed_runs("metrics_only", T - timedelta(days=15), T)
    body = {"entity": "metrics_only", "days": 8, "end": T.isoformat()}
    assert client.post("/api/v1/tuning/profile", json={**body, "pipeline": "it_otel",
                                                       "metric": {"source_type": "platform", "query": "runs/metrics_only/rounds"}}).status_code == 403
    assert client.post("/api/v1/tuning/profile", json={**body, "pipeline": "platform_watchdog",
                                                       "metric": {"source_type": "mock", "query": "cpu"}}).status_code == 403
    r = client.post("/api/v1/tuning/profile", json={**body, "pipeline": "platform_watchdog",
                                                    "metric": {"source_type": "platform", "query": "runs/metrics_only/rounds"}})
    assert r.status_code == 200, r.text
