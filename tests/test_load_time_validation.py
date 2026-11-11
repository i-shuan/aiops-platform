"""載入期參數防線（D5）：寫錯的設定在「建 Pipeline／載入 YAML／收到 API 請求」當下就報錯，不准默默吞掉、也不准拖到執行期。

兩種錯：
1. 值不合理——`threshold: -3`、`window_minutes: 0`、`cooldown_minutes: -5`：算子的 __init__ 檢查，引擎轉成 WorkflowError。
2. 欄位打錯——`param:`（少了 s）、`windw_days:`、`fliter:`：Pydantic 模型一律 extra="forbid"。

本檔沒有涵蓋、照實說的界線見最下面的 test_known_limits_*。
"""
import inspect
import textwrap

import pytest
from pydantic import BaseModel

from aiops.engine import requests as request_models
from aiops.engine.errors import BadRequest, WorkflowError
from aiops.engine.pipeline import NodeSpec, PeerGroupSpec, PeerSpec, Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.interfaces.runner import WORKFLOW_DIR, build_pipeline
from aiops.sources.query.elasticsearch import EsMetricQuery, parse_es_query
from aiops.stages.detectors.base import DetectorOperator


def _wf(*nodes, targets=None):
    specs = [NodeSpec(name="fetch", operator="ingestion.fetch")]
    specs += [NodeSpec(name=n, operator=o, params=p) for n, o, p in nodes]
    return Workflow(name="t", nodes=specs,
                    targets=targets or [TargetSpec(source_type="mock", query="a", entity="a")])


def _yaml(tmp_path, body):
    path = tmp_path / "wf.yaml"
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return path


# ── 驗收條文：threshold: -3 在載入時直接拋例外 ──────────────────────────────────────────────

def test_negative_threshold_in_yaml_fails_when_the_workflow_is_loaded(tmp_path):
    path = _yaml(tmp_path, """
        name: bad
        nodes:
          - {name: fetch, operator: ingestion.fetch}
          - {name: detect, operator: detector.mad, params: {threshold: -3}}
        targets:
          - {source_type: mock, query: a, entity: a}
    """)
    with pytest.raises(WorkflowError, match=r"detector\.mad.*threshold=-3 超出範圍"):
        build_pipeline(str(path), allow_file=True)


def test_negative_threshold_in_target_overrides_fails_when_the_pipeline_is_built():
    wf = _wf(("detect", "detector.mad", {}),
             targets=[TargetSpec(source_type="mock", query="a", entity="a", overrides={"detect": {"threshold": -3}})])
    with pytest.raises(WorkflowError, match="threshold=-3 超出範圍"):
        Pipeline(wf)


def _tunable_cases():
    cases = []
    for name in OperatorRegistry.names("detector."):
        cls = OperatorRegistry.get(name)
        if not (isinstance(cls, type) and issubclass(cls, DetectorOperator)):
            continue
        for key, (_default, low, high) in cls.tunable.items():
            cases.append(pytest.param(name, key, low - 1, id=f"{name}.{key}<{low:g}"))
            cases.append(pytest.param(name, key, high + 1, id=f"{name}.{key}>{high:g}"))
            cases.append(pytest.param(name, key, "3", id=f"{name}.{key}=str"))
            cases.append(pytest.param(name, key, float("nan"), id=f"{name}.{key}=nan"))
            cases.append(pytest.param(name, key, True, id=f"{name}.{key}=true"))
    return cases


TUNABLE_CASES = _tunable_cases()


def test_the_sweep_really_covers_every_tunable_detector():
    covered = {c.values[0] for c in TUNABLE_CASES}
    assert {"detector.mad", "detector.zscore", "detector.hourly_baseline", "detector.quantile_drift"} <= covered
    assert len(TUNABLE_CASES) >= 100


@pytest.mark.parametrize("operator,key,value", TUNABLE_CASES)
def test_every_tunable_of_every_detector_is_range_checked_at_load(operator, key, value):
    """每個偵測器的每個可調參數：低於下限、高於上限、字串、NaN、布林，全部在建 Pipeline 時擋下。"""
    with pytest.raises(WorkflowError, match=key):
        Pipeline(_wf(("detect", operator, {key: value})))


# ── 窗口、冷卻、門檻、權重：0 與負數 ───────────────────────────────────────────────────────

@pytest.mark.parametrize("operator,params,message", [
    ("ingestion.fetch", {"lookback_minutes": 0}, "lookback_minutes 必須大於 0"),
    ("ingestion.fetch", {"lookback_minutes": -60}, "lookback_minutes 必須大於 0"),
    ("ingestion.fetch", {"step_seconds": 0}, "step_seconds 必須大於 0"),
    ("ingestion.fetch", {"train_lookback_minutes": 0}, "train_lookback_minutes 必須大於 0"),
    ("ingestion.fetch", {"train_lookback_minutes": -1}, "train_lookback_minutes 必須大於 0"),
    ("ingestion.fetch", {"retrain_minutes": 0}, "retrain_minutes 必須大於 0"),
    ("ingestion.fetch", {"lookback_minutes": "60"}, "lookback_minutes 必須是有限的數值"),
    ("alert.same_entity", {"window_minutes": 0}, "window_minutes 必須大於 0"),
    ("alert.same_entity", {"window_minutes": -15}, "window_minutes 必須大於 0"),
    ("alert.topology_chain", {"window_minutes": 0}, "window_minutes 必須大於 0"),
    ("alert.lot_path_chain", {"window_minutes": -1}, "window_minutes 必須大於 0"),
    ("alert.lot_path_chain", {"min_lots": 0}, "min_lots 必須是 >= 1 的整數"),
    ("alert.lot_path_chain", {"min_lots": 1.5}, "min_lots 必須是 >= 1 的整數"),
    ("alert.absorb_open", {"resolve_after_minutes": 0}, "resolve_after_minutes 必須大於 0"),
    ("alert.heartbeat", {"resolve_after_minutes": -30}, "resolve_after_minutes 必須大於 0"),
    ("alert.suppress_same_root_cause", {"window_minutes": 0}, "window_minutes 必須大於 0"),
    ("alert.suppress_dedup", {"cooldown_minutes": 0}, "cooldown_minutes 必須大於 0"),
    ("alert.suppress_dedup", {"cooldown_minutes": -30}, "cooldown_minutes 必須大於 0"),
    ("alert.suppress_dedup", {"cooldown_minutes": True}, "cooldown_minutes 必須是有限的數值"),
    ("alert.golden_signals", {"boost": 0}, "boost 必須大於 0"),
    ("alert.p0_boost", {"boost": -2}, "boost 必須大於 0"),
    ("alert.notify_payload", {"severity_thresholds": {"P0": -3, "P3": -5}}, "不能小於 0"),
    ("alert.notify_payload", {"severity_thresholds": {"P0": 3, "P1": 6, "P3": 0}}, "必須 P0 > P1 > P2 > P3"),
    ("alert.notify_payload", {"severity_thresholds": {"P0": 10, "SEV1": 5}}, "SEV1"),
    ("alert.notify_payload", {"severity_thresholds": {}}, "severity_thresholds"),
    ("alert.notify_payload", {"severity_thresholds": {"P0": "high"}}, "必須是有限的數值"),
    ("detector.ensemble", {"strategy": "at_least", "min_votes": 0}, "min_votes 必須是 >= 1 的整數"),
    ("detector.ensemble", {"strategy": "majority", "min_votes": 2}, "min_votes"),
    ("detector.rules", {"upper": 5, "lower": 9}, "lower"),
    ("detector.rules", {"upper": "high"}, "upper 必須是有限的數值"),
    ("detector.rules", {"upper": 5, "consecutive": 0}, "consecutive 必須 >= 1 的整數"),
    ("detector.hard_limit", {"fatal_ceiling": 5, "consecutive": 0}, "consecutive 必須是 >= 1 的整數"),
    ("detector.hard_limit", {"max_rise": 0}, "max_rise 必須 > 0"),
    ("rca.section.similar_cases", {"w_entities": -1}, "w_entities 不能小於 0"),
    ("rca.section.similar_cases", {"w_entities": 0, "w_metrics": 0, "w_signals": 0}, "不能全是 0"),
])
def test_nonsense_values_fail_when_the_pipeline_is_built(operator, params, message):
    with pytest.raises(WorkflowError, match=message):
        Pipeline(_wf(("node", operator, params)))


def test_unknown_operator_param_fails_when_the_pipeline_is_built():
    with pytest.raises(WorkflowError, match="windw_minutes"):
        Pipeline(_wf(("group", "alert.same_entity", {"windw_minutes": 15})))
    with pytest.raises(WorkflowError, match="treshold"):
        Pipeline(_wf(("detect", "detector.mad", {"treshold": 3.5})))


def test_values_that_look_odd_but_are_legitimate_still_load():
    """防線不能誤傷：致命界線可以是負數（溫度、餘額）、權重可以關成 0、非正整數的窗口可以是小數。"""
    pipe = Pipeline(_wf(("fatal", "detector.hard_limit", {"fatal_lower": -3, "fatal_ceiling": 0}),
                        ("vote", "detector.ensemble", {"bypass": ["fatal"]}),
                        ("rules", "detector.rules", {"lower": -3, "upper": -1}),
                        ("group", "alert.same_entity", {"window_minutes": 0.5}),
                        ("payload", "alert.notify_payload", {"severity_thresholds": {"P0": 10, "P3": 0}}),
                        ("similar", "rca.section.similar_cases", {"w_signals": 0, "same_pipeline_bonus": 0})))
    assert [n.name for n, _ in pipe.nodes] == ["fetch", "fatal", "vote", "rules", "group", "payload", "similar"]


# ── 未知欄位：workflow YAML ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("body,field", [
    ("""
        name: bad
        node_list: []
        nodes: [{name: fetch, operator: ingestion.fetch}]
     """, "node_list"),
    ("""
        name: bad
        nodes: [{name: detect, operator: detector.mad, param: {threshold: 9}}]
     """, "param"),
    ("""
        name: bad
        nodes: [{name: ai, operator: enrichment.llm_summary, optionl: true}]
     """, "optionl"),
    ("""
        name: bad
        nodes: [{name: fetch, operator: ingestion.fetch}]
        targets: [{source_type: mock, query: a, entity: a, override: {detect: {threshold: 9}}}]
     """, "override"),
    ("""
        name: bad
        nodes: [{name: fetch, operator: ingestion.fetch}]
        targets: [{source_type: mock, query: a, entity: a, peers: [{source_type: mock, query: b, entitty: b}]}]
     """, "entitty"),
    ("""
        name: bad
        nodes: [{name: fetch, operator: ingestion.fetch}]
        targets: [{source_type: mock, query: a, entity: a, peer_group: {query: q, by: pod, min_peer: 3}}]
     """, "min_peer"),
])
def test_unknown_fields_in_workflow_yaml_fail_at_load(tmp_path, body, field):
    """以前：`param:`（少了 s）整包被丟掉，偵測器用預設門檻跑、沒有任何錯誤。"""
    with pytest.raises(WorkflowError, match=field):
        load_workflow(_yaml(tmp_path, body))


@pytest.mark.parametrize("model", [Workflow, NodeSpec, TargetSpec, PeerSpec, PeerGroupSpec, EsMetricQuery])
def test_workflow_and_query_models_forbid_unknown_fields(model):
    assert model.model_config.get("extra") == "forbid"


def test_every_api_request_model_forbids_unknown_fields():
    models = [m for _, m in inspect.getmembers(request_models, inspect.isclass)
              if issubclass(m, BaseModel) and m.__module__ == request_models.__name__]
    assert len(models) >= 25
    loose = [m.__name__ for m in models if m.model_config.get("extra") != "forbid"]
    assert loose == []


def test_every_pydantic_model_under_engine_and_sources_is_strict():
    """新加的模型忘了 extra="forbid" 就紅：掃 aiops/engine 與 aiops/sources 所有模組裡定義的 Pydantic 模型。"""
    import importlib
    import pkgutil

    import aiops.engine
    import aiops.sources

    loose = []
    for pkg in (aiops.engine, aiops.sources):
        for info in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
            mod = importlib.import_module(info.name)
            for _, m in inspect.getmembers(mod, inspect.isclass):
                if issubclass(m, BaseModel) and m.__module__ == mod.__name__ and m.model_config.get("extra") != "forbid":
                    loose.append(f"{mod.__name__}.{m.__name__}")
    assert loose == []


# ── 未知欄位：API 請求、ES 查詢 ───────────────────────────────────────────────────────────

def test_api_rejects_unknown_request_fields_instead_of_running_with_defaults(client):
    ok = {"kind": "maintenance", "start": "2026-11-08T00:00:00Z", "end": "2026-11-09T00:00:00Z",
          "reason": "x", "created_by": "a"}
    res = client.post("/api/v1/alerts/silences", json={**ok, "entitys": ["cart-api"]})
    assert res.status_code == 422
    assert "entitys" in res.text
    res = client.post("/api/v1/runs", json={"workflow": "it_otel", "target": []})  # targets 少了 s
    assert res.status_code == 422
    assert "target" in res.text


def test_api_rejects_a_bad_override_value_with_400_not_500(client):
    body = {"workflow": "it_otel", "targets": [
        {"source_type": "mock", "query": "a", "entity": "a", "overrides": {"detect_rshift": {"threshold": -3}}}]}
    res = client.post("/api/v1/runs", json=body)
    assert res.status_code == 400
    assert "threshold=-3" in res.text


def test_typo_in_elasticsearch_query_is_an_error_not_a_silently_wider_query():
    """`fliter` 被丟掉 = 沒有過濾條件 = 整個 index 的平均：數字照出、沒人發現。"""
    with pytest.raises(ValueError, match="fliter"):
        parse_es_query('{"index": "metrics-*", "value_field": "v", "fliter": {"term": {"service.name": "cart-api"}}}')
    assert parse_es_query('{"index": "metrics-*", "value_field": "v", "filter": {"term": {"a": "b"}}}').filter


# ── 啟動期：出貨的 workflow 全部建得起來；直接 create 的錯誤型別 ───────────────────────────

def test_every_shipped_workflow_still_builds_under_the_strict_rules():
    names = sorted(p.stem for p in WORKFLOW_DIR.rglob("*.yaml"))
    assert len(names) >= 20
    for name in names:
        build_pipeline(name)


def test_param_errors_are_workflow_errors_and_still_value_errors():
    """算子 __init__ 丟的 ValueError 以前原樣漏出 OperatorRegistry.create（API 變 500）。
    現在是 WorkflowError（400、載入期），同時仍是 ValueError：直接呼叫 create 的程式照舊接得到。"""
    with pytest.raises(WorkflowError) as e:
        OperatorRegistry.create("alert.suppress_dedup", {"cooldown_minutes": -30})
    assert isinstance(e.value, ValueError) and isinstance(e.value, BadRequest)
    assert e.value.status_code == 400


# ── 照實說的界線 ─────────────────────────────────────────────────────────────────────────

def test_known_limit_rules_without_any_rule_loads_and_abstains_instead_of_failing():
    """detector.rules 節點不寫 upper／lower 是合法的（規則可以只寫在某幾條 target 的 overrides），
    所以「完全沒規則」不是載入錯誤：那條序列的節點棄權（can_evaluate False），不進多數決分母。"""
    pipe = Pipeline(_wf(("rules", "detector.rules", {})))
    assert pipe.nodes[1][1].has_rule is False
