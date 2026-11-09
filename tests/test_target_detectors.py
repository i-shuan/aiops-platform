"""每個 target 自己宣告要跑哪些偵測器（detectors 白名單）、節點層 include／exclude_entities、
target overrides、裁決的真實分母（棄權不算）、載入期防呆。"""
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from aiops.control_plane.learning.tuning import _check_params
from aiops.engine.context import PipelineContext, Target
from aiops.engine.errors import BadRequest, WorkflowError
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, NullHooks, Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent
from aiops.stages.detectors.base import DetectorOperator
from aiops.stages.ensemble.ensemble import EnsembleOperator

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)
START = NOW - timedelta(minutes=100)
# 測試段：0 為主，第 50 點 2.5、第 70 點 8。訓練段 ±1 交錯 -> 平均 0、標準差 1，z 分數就是值本身
SMALL, BIG = 50, 70


@OperatorRegistry.register("test.frame")
class _Frame(BaseOperator):
    """固定資料：empty_for 裡的實體拿到空的 DataFrame（模擬這一輪沒資料）。"""

    def __init__(self, empty_for: list[str] | None = None):
        self.empty_for = set(empty_for or [])

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        ts = pd.date_range(START, periods=100, freq="60s")
        values = [0.0] * 100
        values[SMALL], values[BIG] = 2.5, 8.0
        ctx.train_df = pd.DataFrame({"ts": ts - timedelta(days=1), "value": [(-1.0) ** i for i in range(100)]})
        ctx.raw_df = pd.DataFrame({"ts": ts, "value": values})
        if ctx.target.entity in self.empty_for:
            ctx.raw_df, ctx.train_df = ctx.raw_df.iloc[0:0], None
        return ctx


def _wf(*nodes, targets=None, frame=None):
    specs = [NodeSpec(name="fetch", operator="test.frame", params=frame or {})]
    specs += [NodeSpec(name=n, operator=o, params=p) for n, o, p in nodes]
    targets = targets if targets is not None else [{"source_type": "mock", "query": "a", "entity": "a"}]
    return Workflow(name="t", nodes=specs, targets=[TargetSpec(**t) for t in targets])


Z_LOW = ("z_low", "detector.zscore", {"threshold": 2.0})   # 抓 SMALL、BIG
Z_HIGH = ("z_high", "detector.zscore", {"threshold": 5.0})  # 只抓 BIG
MAD = ("mad", "detector.mad", {})


def _minutes(events):
    return sorted({int((e.ts - START).total_seconds() // 60) for e in events})


def _run(*nodes, **kw):
    return _per_target(Pipeline(_wf(*nodes, **kw)))


class _Recorder(NullHooks):
    def __init__(self):
        self.contexts, self.loaded = {}, {}

    def load_target(self, ctx, detector_nodes):
        self.loaded[ctx.target.entity] = [n for n, _ in detector_nodes]

    def persist_target(self, ctx):
        self.contexts[ctx.target.entity] = ctx


def _per_target(pipe: Pipeline, **run_kw):
    rec = _Recorder()
    pipe.hooks = rec
    pipe.run(now=NOW, **run_kw)
    return rec


# ── 白名單 ──


def test_without_detectors_every_detector_node_runs_like_before():
    rec = _run(Z_LOW, Z_HIGH, MAD)
    ctx = rec.contexts["a"]
    assert ctx.detector_runs == ["z_low", "z_high", "mad"]
    assert rec.loaded["a"] == ["z_low", "z_high", "mad"]
    assert not [line for line in ctx.audit if ":skipped:" in line]
    assert {(e.detector_node, m) for e in ctx.anomalies for m in _minutes([e])} >= {("z_low", SMALL), ("z_high", BIG)}


def test_whitelist_by_node_name_runs_only_that_node_of_the_same_operator():
    rec = _run(Z_LOW, Z_HIGH, targets=[{"source_type": "mock", "query": "a", "entity": "a", "detectors": ["z_high"]},
                                       {"source_type": "mock", "query": "b", "entity": "b"}])
    a, b = rec.contexts["a"], rec.contexts["b"]
    assert a.detector_runs == ["z_high"] and rec.loaded["a"] == ["z_high"]  # 沒選的節點也不載基準線、調參
    assert {e.detector_node for e in a.anomalies} == {"z_high"} and _minutes(a.anomalies) == [BIG]
    assert "z_low:skipped:not-selected" in a.audit
    assert b.detector_runs == ["z_low", "z_high"]  # 另一條沒寫 detectors：全部照跑


def test_whitelist_by_operator_name_selects_all_nodes_of_that_operator():
    rec = _run(Z_LOW, Z_HIGH, MAD, targets=[{"source_type": "mock", "query": "a", "entity": "a", "detectors": ["detector.zscore"]}])
    assert rec.contexts["a"].detector_runs == ["z_low", "z_high"]


def test_exclude_beats_include_and_whitelist_and_include_limits_entities():
    excluded = ("z_low", "detector.zscore", {"threshold": 2.0, "include_entities": ["a", "b"], "exclude_entities": ["a"]})
    only_b = ("z_high", "detector.zscore", {"threshold": 5.0, "include_entities": ["b"]})
    rec = _run(excluded, only_b, MAD,
               targets=[{"source_type": "mock", "query": "a", "entity": "a", "detectors": ["z_low", "z_high", "mad"]},
                        {"source_type": "mock", "query": "b", "entity": "b"}])
    a, b = rec.contexts["a"], rec.contexts["b"]
    assert a.detector_runs == ["mad"]
    assert "z_low:skipped:excluded" in a.audit and "z_high:skipped:not-included" in a.audit
    assert b.detector_runs == ["z_low", "z_high", "mad"]


def test_routing_params_never_reach_the_algorithm_or_tuning():
    op = OperatorRegistry.create("detector.zscore", {"threshold": 2.0, "exclude_entities": ["x"]})
    assert op.params == {"threshold": 2.0} and op.exclude_entities == {"x"} and op.include_entities is None
    with pytest.raises(BadRequest, match="路由參數"):
        _check_params("detector.zscore", {"exclude_entities": ["x"]})
    with pytest.raises(WorkflowError, match="清單"):
        OperatorRegistry.create("detector.zscore", {"include_entities": "a"})  # 字串不是清單（會被拆成字元）


# ── 裁決的真實分母 ──


def _vote(events, runs, **params):
    ctx = PipelineContext(pipeline="p", now=NOW, target=Target("mock", "m", "e"))
    ctx.anomalies, ctx.detector_runs = list(events), list(runs)
    op = EnsembleOperator(**params)
    op.node_name = "vote"
    return op.execute(ctx)


def _ev(node, minute):
    return AnomalyEvent("e", "m", START + timedelta(minutes=minute), 3.0, {}, 1.0, "detector.x", node)


def test_ensemble_denominator_counts_only_nodes_that_evaluated():
    events = [_ev("a", 1), _ev("b", 2)]
    # 3 個參與者、各 1 票：多數決要 2 票，全部擋掉（沒抓到的節點 = 反對票）
    assert _vote(events, ["a", "b", "c"], strategy="majority").anomalies == []
    # c 沒評估（沒資料／被跳過）：分母 2，strict 仍要 2 票
    assert _vote(events, ["a", "b"], strategy="majority").anomalies == []
    # 指定了 nodes，但 b、c 沒跑：分母只剩 a，1 個參與者 = 它抓到就通過
    ctx = _vote([_ev("a", 1)], ["a"], strategy="majority", nodes=["a", "b", "c"])
    assert _minutes(ctx.anomalies) == [1] and "ensemble:vote:abstained:b,c" in ctx.audit
    assert any("voters=1:need=1" in line for line in ctx.audit)


def test_ensemble_tie_breaker():
    events = [_ev("a", 1), _ev("a", 3), _ev("b", 3)]
    assert _minutes(_vote(events, ["a", "b"], strategy="majority").anomalies) == [3]
    assert _minutes(_vote(events, ["a", "b"], strategy="majority", tie_breaker="loose").anomalies) == [1, 3]
    # 奇數參與者沒有平手，loose 不影響
    assert _minutes(_vote(events, ["a", "b", "c"], strategy="majority", tie_breaker="loose").anomalies) == [3]
    with pytest.raises(WorkflowError, match="tie_breaker"):
        OperatorRegistry.create("detector.ensemble", {"tie_breaker": "maybe"})


def test_at_least_is_capped_by_participants():
    assert _minutes(_vote([_ev("a", 1)], ["a"], strategy="at_least", min_votes=2).anomalies) == [1]


VOTE = ("vote", "detector.ensemble", {"strategy": "majority"})


def test_ensemble_in_pipeline_skipped_and_empty_nodes_abstain():
    # 三個節點：z_low 抓 SMALL+BIG、z_high 只抓 BIG、mad（訓練段 MAD=1，修正 z 1.69 < 3.5）只抓 BIG
    rec = _run(Z_LOW, Z_HIGH, MAD, VOTE,
               targets=[{"source_type": "mock", "query": "a", "entity": "a"},
                        {"source_type": "mock", "query": "b", "entity": "b", "detectors": ["z_high"]},
                        {"source_type": "mock", "query": "c", "entity": "c"}],
               frame={"empty_for": ["c"]})
    a, b, c = rec.contexts["a"], rec.contexts["b"], rec.contexts["c"]
    assert any("voters=3:need=2" in line for line in a.audit)
    assert _minutes(a.anomalies) == [BIG]  # SMALL 只有 1/3 票
    # b 只選 z_high：另外兩個沒跑、不算反對票，分母 1
    assert any("voters=1:need=1" in line for line in b.audit) and _minutes(b.anomalies) == [BIG]
    assert [e.voted_by for e in b.anomalies] == [("z_high",)]
    # c 沒資料：沒有任何節點評估，也就沒有參與者
    assert c.detector_runs == [] and c.anomalies == []


def test_ran_but_found_nothing_is_a_no_vote():
    rec = _run(Z_LOW, Z_HIGH, ("vote", "detector.ensemble", {"strategy": "intersection"}))
    assert _minutes(rec.contexts["a"].anomalies) == [BIG]  # SMALL 只有 z_low 抓到，z_high 有評估、投反對


def test_two_participants_tie_breaker_in_pipeline():
    strict = _run(Z_LOW, Z_HIGH, VOTE)
    loose = _run(Z_LOW, Z_HIGH, ("vote", "detector.ensemble", {"strategy": "majority", "tie_breaker": "loose"}))
    assert _minutes(strict.contexts["a"].anomalies) == [BIG]
    assert _minutes(loose.contexts["a"].anomalies) == [SMALL, BIG]


# ── overrides ──


def test_override_precedence_default_tuned_yaml_target():
    op = OperatorRegistry.create("detector.quantile_drift", {"window": 40})
    op.node_name = "q"
    ctx = PipelineContext(pipeline="p", now=NOW, target=Target("mock", "m", "e"))
    ctx.detector_params = {"q": {"window": 20, "threshold": 4.0, "quantile": 0.9}}
    assert op.effective_params(ctx) == {"window": 40, "threshold": 4.0, "quantile": 0.9}  # YAML 鎖 window
    ctx.target.overrides = {"q": {"window": 60, "quantile": 0.8}}
    assert op.effective_params(ctx) == {"window": 60, "threshold": 4.0, "quantile": 0.8}  # target 最優先
    assert "params:q:target-override:quantile,window" in ctx.audit


def test_override_changes_what_the_node_flags():
    rec = _run(Z_LOW, targets=[{"source_type": "mock", "query": "a", "entity": "a", "overrides": {"z_low": {"threshold": 5.0}}},
                               {"source_type": "mock", "query": "b", "entity": "b"}])
    assert _minutes(rec.contexts["a"].anomalies) == [BIG]
    assert _minutes(rec.contexts["b"].anomalies) == [SMALL, BIG]


@pytest.mark.parametrize("override, message", [
    ({"z_low": {"threshold": 99}}, "超出範圍"),                 # 跟調參同一套範圍檢查
    ({"z_low": {"treshold": 3}}, "沒有參數"),                   # 拼錯參數名
    ({"z_lw": {"threshold": 3}}, "不是偵測節點名稱"),            # 拼錯節點名
    ({"z_low": {"exclude_entities": ["a"]}}, "路由參數"),       # 路由不准寫在 target 上
])
def test_bad_overrides_fail_at_load_time(override, message):
    with pytest.raises(WorkflowError, match=message):
        Pipeline(_wf(Z_LOW, targets=[{"source_type": "mock", "query": "a", "entity": "a", "overrides": override}]))


def test_override_for_a_node_the_target_did_not_select_is_rejected():
    with pytest.raises(WorkflowError, match="沒有選它"):
        Pipeline(_wf(Z_LOW, Z_HIGH, targets=[{"source_type": "mock", "query": "a", "entity": "a", "detectors": ["z_high"],
                                              "overrides": {"z_low": {"threshold": 3}}}]))


# ── 載入期防呆 ──


def test_unknown_detector_name_fails_at_load_and_lists_available_nodes():
    with pytest.raises(WorkflowError, match=r"z_lw.*z_low（detector.zscore）"):
        Pipeline(_wf(Z_LOW, VOTE, targets=[{"source_type": "mock", "query": "a", "entity": "a", "detectors": ["z_lw"]}]))
    with pytest.raises(WorkflowError):  # 裁決節點不是偵測節點
        Pipeline(_wf(Z_LOW, VOTE, targets=[{"source_type": "mock", "query": "a", "entity": "a", "detectors": ["vote"]}]))


def test_api_targets_get_the_same_checks():
    pipe = Pipeline(_wf(Z_LOW, targets=[]))
    with pytest.raises(WorkflowError, match="z_lw"):
        pipe.run(now=NOW, targets=[TargetSpec(source_type="mock", query="a", detectors=["z_lw"])])
    rec = _per_target(pipe, targets=[TargetSpec(source_type="mock", query="a", detectors=["detector.zscore"])])
    assert rec.contexts["a"].detector_runs == ["z_low"]


def test_routing_entities_must_exist_in_the_workflow_targets():
    with pytest.raises(WorkflowError, match=r"exclude_entities.*\['paymnt'\]"):
        Pipeline(_wf(("z", "detector.zscore", {"exclude_entities": ["paymnt"]})))
    # workflow 沒宣告 targets（全部由 API 傳入）時無從比對，不擋
    Pipeline(_wf(("z", "detector.zscore", {"exclude_entities": ["paymnt"]}), targets=[]))


def test_misspelled_target_field_and_empty_whitelist_fail(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text("""name: t
nodes:
  - {name: z, operator: detector.zscore}
targets:
  - {source_type: mock, query: a, detector: [z]}
""", encoding="utf-8")
    with pytest.raises(WorkflowError, match="detector") as e:
        load_workflow(path)
    # 要是因為拼錯欄位而失敗，不是別的欄位（例如少了 source_type）剛好讓它報錯
    assert "Extra inputs are not permitted" in str(e.value) and "source_type" not in str(e.value)
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as e:
        TargetSpec(source_type="mock", query="a", detectors=[])
    assert [(err["loc"], err["type"]) for err in e.value.errors()] == [(("detectors",), "too_short")]


# ── 稽核 Q5：冷啟動棄權、白名單跳過，只剩一個參與者時任何策略都是 pass-through ──


@pytest.mark.parametrize("strategy", [{"strategy": "intersection"}, {"strategy": "majority"},
                                      {"strategy": "at_least", "min_votes": 3}])
def test_cold_start_and_skipped_nodes_leave_one_voter_who_decides_alone(strategy):
    n_skip = ("n_skip", "detector.zscore", {"threshold": 2.0})            # 會抓 SMALL＋BIG，但 target 沒選它
    n_cold = ("n_cold", "detector.mad", {"min_train_points": 10_000})     # 訓練段 100 點 < 10000：冷啟動棄權
    n_ok = ("n_ok", "detector.zscore", {"threshold": 5.0})                # 只抓 BIG
    rec = _run(n_skip, n_cold, n_ok, ("vote", "detector.ensemble", strategy),
               targets=[{"source_type": "mock", "query": "a", "entity": "a", "detectors": ["n_cold", "n_ok"]}])
    a = rec.contexts["a"]
    assert a.detector_runs == ["n_ok"]
    assert "n_skip:skipped:not-selected" in a.audit and any(l.startswith("train:n_cold:too-few-points") for l in a.audit)
    assert any("voters=1:need=1" in line for line in a.audit)
    assert _minutes(a.anomalies) == [BIG] and [(e.detector_node, e.voted_by) for e in a.anomalies] == [("vote", ("n_ok",))]


# ── 稽核 Q13：路由順序（exclude → include → detectors）與被跳過的節點零開銷 ──


def test_routing_order_is_visible_in_the_skip_reason_and_skipped_nodes_load_nothing(monkeypatch):
    """both：include b、exclude a。a 被 exclude 先擋（原因 excluded；若先看 include 會變成 not-included）。
    inc_b：include b，但 b 的白名單沒選它 -> not-selected（include 過了才看白名單）。
    走真的 StorageHooks：被跳過的節點不查調參、不查基準線、也不存基準線。"""
    from aiops.storage import crud, repository
    from aiops.storage.hooks import StorageHooks
    from aiops.storage.models import DetectorModelRow

    models_get, candidates = [], []
    real_get, real_cand = repository.DetectorModels.get, repository.ActiveConfigs.candidates
    monkeypatch.setattr(repository.DetectorModels, "get",
                        staticmethod(lambda p, e, q, op, node: (models_get.append((e, node)), real_get(p, e, q, op, node))[1]))
    monkeypatch.setattr(repository.ActiveConfigs, "candidates",
                        staticmethod(lambda op, p, e: (candidates.append((e, op)), real_cand(op, p, e))[1]))
    wf = _wf(("both", "detector.zscore", {"include_entities": ["b"], "exclude_entities": ["a"]}),
             ("inc_b", "detector.mad", {"include_entities": ["b"]}),
             ("sel", "detector.quantile_drift", {"window": 20}),
             targets=[{"source_type": "mock", "query": "a", "entity": "a", "detectors": ["both", "inc_b", "sel"]},
                      {"source_type": "mock", "query": "b", "entity": "b", "detectors": ["both"]}])
    ctx = Pipeline(wf, StorageHooks()).run(now=NOW)
    skips = sorted(line for line in ctx.audit if ":skipped:" in line)
    assert skips == ["both:skipped:excluded", "inc_b:skipped:not-included", "inc_b:skipped:not-selected",
                     "sel:skipped:not-selected"]
    assert sorted(models_get) == [("a", "sel"), ("b", "both")]
    assert sorted(candidates) == [("a", "detector.quantile_drift"), ("b", "detector.zscore")]
    assert sorted((r["entity"], r["node"]) for r in crud.find(DetectorModelRow)) == [("a", "sel"), ("b", "both")]


# ── 稽核 Q14：巢狀字典參數逐鍵 deep_merge（調參 ← 節點 YAML ← target overrides） ──


@OperatorRegistry.register("test.nested_params")
class _NestedParams(DetectorOperator):
    """參數 bands 是巢狀字典。detect 用 bands.lo × bands.nested.y 當門檻：兩個鍵分別只出現在調參層與 overrides 層，
    淺層合併會整個換掉 bands，就拿不到其中一個。"""

    algorithm = "nested"
    tunable = {"threshold": (3.0, 1.0, 10.0)}
    other_params = frozenset({"bands"})

    def fit(self, df):
        self.fitted = True
        return self

    def get_state(self):
        return {}

    def load_state(self, state):
        self.fitted = True
        return self

    def detect(self, df):
        bands = self.params["bands"]
        limit = bands["lo"] * bands["nested"]["y"]
        return self._result(df, df["value"], df["value"] > limit)


TUNED = {"bands": {"hi": 1, "lo": 2, "nested": {"z": 9, "x": 0}}, "threshold": 4.0}
YAML = {"bands": {"hi": 5, "nested": {"x": 1}}}
OVERRIDE = {"bands": {"mid": 3, "nested": {"y": 2, "x": 8}}}
MERGED = {"bands": {"hi": 5, "lo": 2, "mid": 3, "nested": {"z": 9, "x": 8, "y": 2}}, "threshold": 4.0}


def test_nested_params_merge_key_by_key_across_all_three_layers():
    op = OperatorRegistry.create("test.nested_params", YAML)
    op.node_name = "n"
    ctx = PipelineContext(pipeline="p", now=NOW, target=Target("mock", "q", "e", overrides={"n": OVERRIDE}))
    ctx.detector_params["n"] = TUNED
    assert op.effective_params(ctx) == MERGED  # 兄弟鍵 lo、mid、z 都在；hi：YAML 蓋調參；x：override 蓋 YAML 蓋調參
    assert TUNED["bands"]["nested"] == {"z": 9, "x": 0} and op.params == YAML  # 合併不改任何一層的輸入


class _Tuned(NullHooks):
    """不碰資料庫，直接給核准調參（StorageHooks 的 load_target 會從 active_detector_configs 填同一個欄位）。"""

    def load_target(self, ctx, detector_nodes):
        ctx.detector_params = {name: TUNED for name, _ in detector_nodes}


def test_nested_params_reach_the_algorithm_merged_in_a_pipeline():
    wf = _wf(("n", "test.nested_params", YAML),
             targets=[{"source_type": "mock", "query": "a", "entity": "a", "overrides": {"n": OVERRIDE}}])
    ctx = Pipeline(wf, _Tuned()).run(now=NOW)
    assert _minutes(ctx.anomalies) == [BIG]  # 門檻 lo(2，調參層) × nested.y(2，override 層) = 4：只有 BIG(8) 越線
