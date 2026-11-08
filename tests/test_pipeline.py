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
    run_job(_job(), now=NOW)
    n = len(sent)
    r = run_job(_job(), now=NOW)
    assert (r["trained"], r["persisted_anomalies"], r["processed_anomalies"]) == (0, 0, 0)
    assert len(sent) == n


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
