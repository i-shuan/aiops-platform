"""source_type 必填：漏寫或空白一律拒絕，絕不默默退回 mock（假資料）。

以前 TargetSpec／PeerSpec／MetricIn 的 source_type 預設是 "mock"：API 呼叫端或 YAML 漏寫時不會報錯，
而是拿假資料跑偵測與回測。現在：
- API：請求驗證失敗回 422（FastAPI 對 Pydantic 驗證錯誤的標準回應，跟平台其他欄位錯誤一致），錯誤指出 source_type
- workflow YAML：載入時就丟 WorkflowError，不會等到排程跑起來
"""
import pytest
from pydantic import ValidationError

from aiops.engine.errors import WorkflowError
from aiops.engine.pipeline import PeerSpec, TargetSpec, load_workflow
from aiops.engine.requests import MetricIn


@pytest.mark.parametrize("model, extra", [(TargetSpec, {}), (PeerSpec, {}), (MetricIn, {})])
@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_or_blank_source_type_is_rejected(model, extra, value):
    data = {"query": "cpu", **extra}
    if value is not None:
        data["source_type"] = value
    with pytest.raises(ValidationError) as e:
        model(**data)
    assert [err["loc"] for err in e.value.errors()] == [("source_type",)]
    assert e.value.errors()[0]["type"] == ("missing" if value is None else "string_too_short")


def test_explicit_source_type_is_kept_and_trimmed():
    assert TargetSpec(source_type=" prometheus ", query="up").source_type == "prometheus"
    assert MetricIn(source_type="mock", query="cpu").source_type == "mock"


def _locs(response):
    return [tuple(err["loc"]) for err in response.json()["detail"]]


def test_api_run_without_source_type_is_rejected(client):
    r = client.post("/api/v1/runs", json={"workflow": "metrics_only", "targets": [{"query": "cpu", "entity": "x"}]})
    assert r.status_code == 422 and ("body", "targets", 0, "source_type") in _locs(r)
    r = client.post("/api/v1/runs", json={"workflow": "metrics_only",
                                          "targets": [{"source_type": "mock", "query": "cpu",
                                                       "peers": [{"query": "cpu2"}]}]})
    assert r.status_code == 422 and ("body", "targets", 0, "peers", 0, "source_type") in _locs(r)


def test_api_backtest_without_source_type_is_rejected(client):
    body = {"proposal_id": 1, "metric": {"query": "cpu"},
            "time_range": {"start": "2026-11-01T00:00:00Z", "end": "2026-11-02T00:00:00Z"}}
    r = client.post("/api/v1/tuning/proposals/backtest", json=body)
    assert r.status_code == 422 and _locs(r) == [("body", "metric", "source_type")]


def test_api_run_with_explicit_source_type_still_works(client):
    # 帶偵測器的樣本：出貨的 metrics_only 只吃外部告警，給 targets 會 400（不會被用到）
    r = client.post("/api/v1/runs", json={"workflow": "sample_metrics_only",
                                          "targets": [{"source_type": "mock", "query": "metric_a", "entity": "metric_a"}]})
    assert r.status_code == 200


def test_workflow_yaml_without_source_type_fails_at_load(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text("name: t\nnodes:\n  - {name: z, operator: detector.zscore}\ntargets:\n  - {query: cpu, entity: x}\n",
                    encoding="utf-8")
    with pytest.raises(WorkflowError, match="source_type"):
        load_workflow(path)
