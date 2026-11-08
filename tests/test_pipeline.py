"""偵測 -> anomalies -> 告警 的整條管線，以及批次訓練與排程入口。"""
from datetime import datetime, timedelta

import yaml

from aiops.tasks.__main__ import main
from aiops.tasks.config import Job
from aiops.tasks.pipeline import run_job

NOW = datetime(2026, 11, 8, 12, 0)
WINDOW = {"start": "2026-11-08T00:00:00", "end": "2026-11-08T02:00:00", "step_seconds": 60}


def _job(name="cpu", **kw):
    targets = kw.pop("targets", [{"query": "cpu_usage", "entity": "svc-a", "attrs": {"dept": "payments"}}])
    return Job(name=name, preset="metrics_only", targets=targets, **kw)


# ── 偵測端：保存模型、寫入異常 ──


def test_detection_with_pipeline_persists_only_new_anomalies(client):
    body = {"metric": {"query": "cpu_usage"}, "time_range": WINDOW, "pipeline": "p1", "entity": "svc-a"}
    first = client.post("/api/v1/detection/run", json=body).json()["result"]
    assert first["model"] == "fitted_and_saved" and first["persisted"] >= 1

    again = client.post("/api/v1/detection/run", json=body).json()["result"]
    assert again["model"] == "saved" and again["persisted"] == 0  # 同一段時間不重複寫


def test_detection_without_pipeline_persists_nothing(client):
    r = client.post("/api/v1/detection/run", json={"metric": {"query": "cpu_usage"}, "time_range": WINDOW}).json()
    assert "persisted" not in r["result"] and r["result"]["model"] == "fitted"


def test_train_endpoint_and_freshness(client):
    body = {"metric": {"query": "cpu_usage"}, "train_time_range": WINDOW, "pipeline": "p1"}
    assert client.post("/api/v1/detection/train", json=body).json()["result"]["trained"] is True
    fresh = client.post("/api/v1/detection/train", json={**body, "if_older_than_minutes": 60}).json()["result"]
    assert fresh["trained"] is False


# ── 告警端：讀未處理異常 ──


def test_process_pending_reads_each_anomaly_once(client, sent):
    client.post("/api/v1/detection/run", json={"metric": {"query": "cpu_usage"}, "time_range": WINDOW,
                                                "pipeline": "p1", "attrs": {"dept": "payments"}})
    body = {"pipeline": "p1", "preset": "metrics_only"}
    first = client.post("/api/v1/alerts/process-pending", json=body).json()["result"]
    assert first["processed_anomalies"] >= 1 and first["incidents"]
    assert sent and sent[0].labels["dept"] == "payments"

    again = client.post("/api/v1/alerts/process-pending", json=body).json()["result"]
    assert again["processed_anomalies"] == 0


def test_pipelines_do_not_see_each_others_anomalies(client):
    for p in ("p1", "p2"):
        client.post("/api/v1/detection/run",
                    json={"metric": {"query": f"q-{p}"}, "time_range": WINDOW, "pipeline": p, "entity": p})
    r = client.post("/api/v1/alerts/process-pending", json={"pipeline": "p1", "preset": "metrics_only"}).json()
    assert {e for i in r["result"]["incidents"] for e in i["entities"]} == {"p1"}


# ── 排程：train -> detect -> alerts ──


def test_run_job_trains_detects_and_notifies(client, sent):
    r = run_job(_job(), now=NOW)
    assert r["trained"] == 1
    assert r["persisted_anomalies"] >= 1 and r["processed_anomalies"] == r["persisted_anomalies"]
    assert r["notified_incidents"] >= 1 and sent[0].labels["dept"] == "payments"


def test_rerunning_same_window_is_idempotent(client, sent):
    # mock 尖峰在偵測窗中間（窗結束前 30 分鐘），結案門檻要大於 30 分鐘才會是「進行中」
    run_job(_job(resolve_after_minutes=60), now=NOW)
    first = [(n.labels, n.ends_at) for n in sent]
    r = run_job(_job(resolve_after_minutes=60), now=NOW)
    assert (r["trained"], r["persisted_anomalies"], r["processed_anomalies"], r["incidents"]) == (0, 0, 0, 0)
    # 第二輪只有心跳：重送的通知跟第一輪 labels 完全相同、沒有結案
    assert r["refreshed_incidents"] == 1
    assert [(n.labels, n.ends_at) for n in sent[len(first):]] == first


# ── 恢復通知 ──


def test_incident_resolves_after_quiet_period_with_same_labels(client, sent):
    from aiops.domains.alerts.commands.sync import SyncOpenIncidentsCommand
    from aiops.domains.alerts.schemas import SyncIncidentsRequest

    run_job(_job(resolve_after_minutes=60), now=NOW)
    opened = sent[0]
    # 最後一筆異常（11:35）距離 12:05 只有 30 分鐘 -> 只心跳
    assert run_job(_job(resolve_after_minutes=60), now=NOW + timedelta(minutes=5))["resolved_incidents"] == 0
    # 之後不再偵測，2 小時後同步 -> 安靜夠久，結案
    r = SyncOpenIncidentsCommand(SyncIncidentsRequest(pipeline="cpu", resolve_after_minutes=60),
                                 now=NOW + timedelta(hours=2)).run()
    assert r["resolved"] == 1
    closing = sent[-1]
    assert closing.labels == opened.labels and closing.ends_at is not None
    assert SyncOpenIncidentsCommand(SyncIncidentsRequest(pipeline="cpu"), now=NOW + timedelta(hours=3)).run() == {
        "pipeline": "cpu", "refreshed": 0, "resolved": 0, "resolved_uids": []}


def test_new_anomalies_keep_incident_open(client, sent):
    """實體後來又有異常（即使被 dedup 抑制、沒開新事件群），舊事件群也不能被誤判成結束。"""
    # 第一輪尖峰在 11:30；第二輪（12:50）的新尖峰在 12:20。
    # 只看事件群本身：11:30 到 12:50 已安靜 80 分鐘 > 45 -> 會被誤判結案
    # 看到新異常：12:20 到 12:50 只安靜 30 分鐘 < 45 -> 正確地保持進行中
    run_job(_job(resolve_after_minutes=45), now=NOW)
    later = run_job(_job(resolve_after_minutes=45), now=NOW + timedelta(minutes=50))
    assert later["persisted_anomalies"] >= 1
    assert later["resolved_incidents"] == 0


def test_manual_resolve_sends_ends_at(client, sent):
    r = client.post("/api/v1/alerts/process", json={"preset": "metrics_only", "events": [
        {"entity": "x", "metric": "cpu", "ts": "2026-11-08T00:00:00", "score": 2}]}).json()["result"][0]
    resolved = client.post(f"/api/v1/alerts/incidents/{r['uid']}/resolve").json()["result"]
    assert resolved["status"] == "resolved" and sent[-1].ends_at is not None
    assert client.post(f"/api/v1/alerts/incidents/{r['uid']}/resolve").status_code == 409


def test_model_retrains_only_after_retrain_minutes(client):
    run_job(_job(retrain_minutes=60), now=NOW)
    assert run_job(_job(retrain_minutes=60), now=NOW + timedelta(minutes=30))["trained"] == 0
    assert run_job(_job(retrain_minutes=60), now=NOW + timedelta(minutes=90))["trained"] == 1


def test_cli_run_once(client, tmp_path, capsys):
    cfg = tmp_path / "jobs.yaml"
    cfg.write_text(yaml.safe_dump({"jobs": [_job().model_dump()]}, allow_unicode=True), encoding="utf-8")
    assert main(["run-once", "-c", str(cfg)]) == 0
    assert '"job": "cpu"' in capsys.readouterr().out


def test_cli_reports_failed_job_with_nonzero_exit(client, tmp_path):
    bad = _job(name="bad", detector_name="no-such-detector").model_dump()
    cfg = tmp_path / "jobs.yaml"
    cfg.write_text(yaml.safe_dump({"jobs": [bad]}), encoding="utf-8")
    assert main(["run-once", "-c", str(cfg)]) == 1


def test_example_jobs_file_is_valid():
    from pathlib import Path

    from aiops.tasks.config import load_jobs

    jobs = load_jobs(Path(__file__).resolve().parents[1] / "configs" / "jobs.example.yaml")
    assert [j.name for j in jobs] == ["it-cpu"]
