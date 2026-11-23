"""使用者決定（2026-11-15）：平台的自動偵測演算法只用來監控 AIOps 平台自己（看門狗），不給使用者用。

使用者的告警規則設在他們自己的監控系統／Grafana，經 webhook 推進來（docs/EXTERNAL_ALERTS.md）；平台只收斂、出 RCA 建議。
這個檔把這件事擋住：
  - 出貨的使用者管線（aiops/pipelines/detection/）不抓資料、不跑偵測器、沒有 targets
  - 偵測器節點只准出現在 aiops/pipelines/watchdog/（監控平台自己；目前還沒有）
  - 帶偵測器的管線樣本只放在 scripts/pipelines/（給測試、回測、混沌腳本），不在 aiops/、不進映像檔
  - API 不能拿使用者管線對自己的序列跑偵測（POST /api/v1/runs 帶 targets 會被拒）
"""
from pathlib import Path

import pytest
import yaml

from aiops.engine.errors import WorkflowError
from aiops.engine.pipeline import Pipeline, TargetSpec, load_workflow
from aiops.engine.registry import OperatorRegistry

ROOT = Path(__file__).resolve().parents[1]
SHIPPED = ROOT / "aiops" / "pipelines"
USER_PIPELINES = sorted((SHIPPED / "detection").glob("*.yaml"))
WATCHDOG_DIR = SHIPPED / "watchdog"
GRAFANA_DIR = SHIPPED / "grafana"


def _operators(path: Path) -> list[str]:
    return [n["operator"] for n in (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("nodes") or []]


def test_there_are_user_pipelines_to_check():
    assert {p.stem for p in USER_PIPELINES} == {"it_otel", "it_apm", "factory", "metrics_only"}


@pytest.mark.parametrize("path", USER_PIPELINES, ids=lambda p: p.stem)
def test_user_pipelines_only_take_external_alerts(path):
    """沒有逐序列的節點（抓資料、偵測器）、沒有 targets：每一輪只處理推進來的外部告警。"""
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert not raw.get("targets"), f"{path.name} 不准有 targets"
    banned = [op for op in _operators(path) if op.startswith(("detector.", "ingestion."))]
    assert not banned, f"{path.name} 不准有抓資料或偵測的節點：{banned}"
    pipeline = Pipeline(load_workflow(path))
    assert pipeline.target_nodes == [] and pipeline.detector_nodes == []
    assert all(getattr(op, "scope", "target") != "target" for _, op in pipeline.nodes)


def test_detector_nodes_only_appear_in_watchdog_and_grafana_rule_pipelines():
    """出貨的 YAML 裡，偵測器節點只准在 pipelines/watchdog/（監控 AIOps 平台自己）與 pipelines/grafana/
    （2026-11-20 起：偵測對象由使用者在 Grafana 告警規則上貼 aiops_detect 標籤決定）。
    pipelines/detection/ 的使用者管線仍然只吃外部推進來的告警。"""
    detectors = set(OperatorRegistry.names("detector."))
    assert detectors
    offenders = [f"{p.relative_to(SHIPPED).as_posix()}: {op}" for p in SHIPPED.rglob("*.yaml")
                 if WATCHDOG_DIR not in p.parents and GRAFANA_DIR not in p.parents for op in _operators(p) if op in detectors]
    assert not offenders, offenders


def test_grafana_rule_pipelines_take_their_targets_from_grafana_and_nowhere_else():
    """pipelines/grafana/ 的管線不准自己寫 targets：對象與嚴重度都在使用者的 Grafana 上，這個檔只描述流程。"""
    files = sorted(GRAFANA_DIR.glob("*.yaml"))
    assert files
    for path in files:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert raw.get("targets_from") == "grafana" and not raw.get("targets"), path.name


def test_detection_samples_live_outside_the_platform_package_and_image():
    samples = sorted((ROOT / "scripts" / "pipelines").glob("sample_*.yaml"))
    assert {p.stem for p in samples} == {"sample_it_otel", "sample_it_apm", "sample_factory", "sample_metrics_only"}
    assert not list(SHIPPED.rglob("sample_*.yaml"))
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    copies = [line.split()[1] for line in dockerfile.splitlines() if line.startswith(("COPY", "ADD"))]
    assert copies == ["requirements.txt", "aiops"], copies  # scripts/ 不進映像檔


@pytest.mark.parametrize("path", USER_PIPELINES, ids=lambda p: p.stem)
def test_targets_on_a_user_pipeline_are_rejected_at_load_and_from_the_api(path, client):
    wf = load_workflow(path)
    target = TargetSpec(source_type="mock", query="cpu", entity="cart")
    with pytest.raises(WorkflowError, match="targets 不會被用到"):
        Pipeline(wf.model_copy(update={"targets": [target]}))
    with pytest.raises(WorkflowError, match="targets 不會被用到"):
        Pipeline(wf).run(targets=[target])
    # API 在進引擎之前先過偵測入口政策（runner.check_detection_entry）：使用者管線不准拿來跑呼叫端的序列，403
    r = client.post("/api/v1/runs", json={"workflow": path.stem, "targets": [target.model_dump()]})
    assert r.status_code == 403 and "grafana-alerts" in r.json()["message"]
    assert client.post("/api/v1/runs", json={"workflow": path.stem}).status_code == 200  # 不帶 targets：照常跑一輪


def test_a_round_with_no_pushed_alerts_still_leaves_a_run_row(now):
    """只吃外部告警的管線也記資料健康度：每一輪一列，證明平台那時候有在跑（告警沒來時分得出是對方沒推還是平台沒跑）。"""
    from aiops.interfaces.runner import run_scheduled
    from aiops.storage.repository import PipelineRuns, StreamHealth

    ctx = run_scheduled("it_otel", now)
    assert ctx.incidents == [] and ctx.anomalies == []
    [row] = PipelineRuns.list(pipeline="it_otel")
    assert (row["status"], row["targets"], row["failed_targets"]) == ("ok", 0, 0)
    assert StreamHealth.list() == []  # 沒有抓資料，就沒有資料流
