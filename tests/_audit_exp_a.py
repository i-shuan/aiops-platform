"""架構稽核的實驗（不是正式測試、不會被預設收集）：.venv/Scripts/python -m pytest tests/_audit_exp_a.py -s -q
每個 exp_* 印出觀察到的結果，給 synth-out/reports/arch_audit.md 當證據。"""
import dataclasses
import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from aiops.engine.context import PipelineContext, SpanRecord, Target
from aiops.engine.errors import BadRequest, InfrastructureError, NotFound, PlatformError, WorkflowError
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.topology import service_edges
from aiops.engine.types import AnomalyEvent
from aiops.sources.base import InvalidQueryError, NoDataError
from aiops.sources.mock import MockSpec
from aiops.stages.detectors.base import DetectorOperator
from aiops.stages.rca.spans import inbound_callers, window_stats
from aiops.storage import db, models, repository
from aiops.storage.hooks import StorageHooks

T0 = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)


def show(title, obj):
    print(f"\n### {title}\n{json.dumps(obj, ensure_ascii=False, default=str, indent=1)}")


def frame(n, spike=None, start=T0 - timedelta(hours=2)):
    values = [50.0 + (i % 3) for i in range(n)]
    if spike is not None and n:
        values[-1] = spike
    return pd.DataFrame({"ts": pd.date_range(start, periods=n, freq="60s"), "value": values})


def anomaly_rows():
    with db.read_session() as s:
        return [{"entity": r.entity, "detector_name": r.detector_name, "detector_node": r.detector_node,
                 "level": r.level} for r in s.query(models.AnomalyRow).all()]


def incident_rows():
    with db.read_session() as s:
        return [r.incident for r in s.query(models.IncidentRow).all()]


# ── Q4 ──

def exp_q4_frozen_and_lineage():
    e = AnomalyEvent("svc", "q", T0, 9.0, detector="detector.zscore", detector_node="z")
    try:
        e.detector = "x"  # type: ignore[misc]
        frozen = "沒擋住"
    except dataclasses.FrozenInstanceError as err:
        frozen = f"FrozenInstanceError: {err}"
    e.attrs["dept"] = "後來塞的"  # 淺層 frozen：attrs dict 本身仍可改
    show("Q4a frozen", {"set detector": frozen, "attrs 仍可原地修改": e.attrs})

    # 兩個偵測節點 + 裁決（keep_members=False） -> anomalies 表寫的是什麼
    wf = Workflow(name="q4", targets=[TargetSpec(source_type="mock", query="metric_q4", entity="svc")], nodes=[
        NodeSpec(name="fetch", operator="ingestion.fetch", params={"lookback_minutes": 120}),
        NodeSpec(name="z", operator="detector.zscore"),
        NodeSpec(name="m", operator="detector.mad"),
        NodeSpec(name="vote", operator="detector.ensemble", params={"strategy": "union"}),
        NodeSpec(name="new_only", operator="detector.new_only"),
        NodeSpec(name="group", operator="alert.same_entity"),
        NodeSpec(name="absorb", operator="alert.absorb_open"),
        NodeSpec(name="payload", operator="alert.notify_payload"),
        NodeSpec(name="heartbeat", operator="alert.heartbeat"),
        NodeSpec(name="notify", operator="notifier.log")])
    repository.ExternalAnomalies.create_many([{"pipeline": "q4", "source": "fdc", "external_id": "x1", "entity": "tool-9",
                                               "metric": "temp", "ts": (T0 - timedelta(minutes=3)).replace(tzinfo=None),
                                               "value": 99.0, "score": 7.0, "attrs": {}}])
    ctx = Pipeline(wf, StorageHooks()).run(now=T0)
    snap = [{"entities": i["entities"], "events": [(ev["detector"], ev["detector_node"], ev["voted_by"]) for ev in i["events"]]}
            for i in incident_rows()]
    show("Q4b anomalies 表", anomaly_rows())
    show("Q4b 事件群快照（detector, detector_node, voted_by）", snap)
    show("Q4b ctx.anomalies detectors", sorted({(a.detector, a.detector_node) for a in ctx.anomalies}))
    cols = [c.name for c in models.AnomalyRow.__table__.columns]
    show("Q4b anomalies 表欄位（有沒有 voted_by）", cols)


# ── Q5 ──

def _run_detectors(nodes, target_detectors=None, raw_n=120, spike=500.0):
    """直接用 Pipeline（NullHooks）跑：自訂一個 fetch 算子塞固定資料，避免依賴 mock 的尖峰位置。"""
    wf = Workflow(name="q5", targets=[TargetSpec(source_type="mock", query="q", entity="svc",
                                                 detectors=target_detectors)],
                  nodes=[NodeSpec(name="fetch", operator="audit.fixed_frame", params={"n": raw_n, "spike": spike}),
                         *nodes])
    return Pipeline(wf).run(now=T0)


@OperatorRegistry.register("audit.fixed_frame")
class FixedFrame(BaseOperator):
    scope = "target"

    def __init__(self, n: int = 120, spike: float | None = 500.0):
        self.n, self.spike = n, spike

    def execute(self, ctx):
        ctx.raw_df = frame(self.n, self.spike)
        return ctx


def exp_q5_ensemble_denominator():
    nodes = [NodeSpec(name="n_skip", operator="detector.zscore"),
             NodeSpec(name="n_cold", operator="detector.mad", params={"min_train_points": 10000}),
             NodeSpec(name="n_ok", operator="detector.zscore"),
             NodeSpec(name="vote", operator="detector.ensemble", params={"strategy": "intersection"})]
    ctx = _run_detectors(nodes, target_detectors=["n_cold", "n_ok", "vote"] and ["n_cold", "n_ok"])
    show("Q5a 白名單跳過 n_skip、冷啟動 n_cold、正常 n_ok + intersection",
         {"audit": [a for a in ctx.audit if a.startswith(("n_", "train:", "ensemble", "vote"))],
          "anomalies": [(a.detector, a.detector_node, a.voted_by, a.level) for a in ctx.anomalies]})

    # 沒資料：fetch 給空 df
    nodes2 = [NodeSpec(name="a", operator="detector.zscore"), NodeSpec(name="b", operator="detector.mad"),
              NodeSpec(name="vote", operator="detector.ensemble", params={"strategy": "majority"})]
    ctx2 = PipelineContext(pipeline="p", now=T0, target=Target("mock", "q", "svc"))
    ctx2.raw_df = frame(0)
    for n in nodes2:
        o = OperatorRegistry.create(n.operator, n.params)
        o.node_name = n.name
        ctx2 = o.execute(ctx2)
    show("Q5b 這條序列沒資料（直接跑算子；經過引擎的話整輪會被斷流攔截）",
         {"detector_runs": ctx2.detector_runs, "audit": ctx2.audit, "anomalies": len(ctx2.anomalies)})

    # 缺口候選：rules 沒有規則（require_rule=false）-> 永遠不會判，但算進參與者
    nodes3 = [NodeSpec(name="z", operator="detector.zscore"),
              NodeSpec(name="r_norule", operator="detector.rules", params={"require_rule": False}),
              NodeSpec(name="vote", operator="detector.ensemble", params={"strategy": "majority"})]
    ctx3 = _run_detectors(nodes3)
    show("Q5c zscore + rules(沒規則) + majority", {
        "audit": [a for a in ctx3.audit if "ensemble" in a],
        "anomalies": [(a.detector_node, a.voted_by) for a in ctx3.anomalies]})

    # 缺口候選：quantile_drift 窗口比這一輪資料長 -> 一個點都算不出分數，卻算參與者
    nodes4 = [NodeSpec(name="z", operator="detector.zscore"),
              NodeSpec(name="qd", operator="detector.quantile_drift", params={"window": 600}),
              NodeSpec(name="vote", operator="detector.ensemble", params={"strategy": "majority"})]
    ctx4 = _run_detectors(nodes4)
    show("Q5d zscore + quantile_drift(window=600 > 120 點) + majority", {
        "audit": [a for a in ctx4.audit if "ensemble" in a],
        "anomalies": [(a.detector_node, a.voted_by) for a in ctx4.anomalies]})


# ── Q6 ──

def exp_q6_entry_definition():
    t = T0
    spans = [
        SpanRecord("t1", "a1", None, "api", t, 100),     # 真入口
        SpanRecord("t1", "b1", "a1", "cart", t, 80),     # api -> cart
        SpanRecord("t2", "b2", "zz-missing", "cart", t, 80),  # 父 span 沒撈到（抽樣、時間窗邊界）
        SpanRecord("t3", "c3", "", "cart", t, 80),        # parent_id 是空字串
    ]
    show("Q6 service_edges", {f"{a}->{b}": n for (a, b), n in service_edges(spans).items()})
    show("Q6 inbound_callers（callee -> {caller: 次數}，'' = 外部入口）",
         inbound_callers(spans, t - timedelta(minutes=1), t + timedelta(minutes=1)))
    show("Q6 window_stats requests", {k: v.requests for k, v in window_stats(spans, t - timedelta(minutes=1),
                                                                               t + timedelta(minutes=1)).items()})


# ── Q8 ──

@OperatorRegistry.register("audit.raise")
class Raise(BaseOperator):
    scope = "target"

    def __init__(self, kind: str = "platform"):
        self.kind = kind

    def execute(self, ctx):
        q = ctx.target.query
        if q.startswith("det_platform"):
            raise PlatformError("偵測器自己的 PlatformError")
        if q.startswith("det_badreq"):
            raise BadRequest("偵測器自己的 BadRequest（例如 validate_params）")
        if q.startswith("det_notfound"):
            raise NotFound("偵測器內部查表 NotFound")
        if q.startswith("det_value"):
            raise ValueError("偵測器程式錯誤")
        return ctx


@pytest.fixture
def flaky(monkeypatch):
    real, real_group = MockSpec.fetch_data, MockSpec.fetch_group

    def fetch(self, query, start, end, step_seconds, field_map=None):
        if query.startswith("empty:"):
            raise NoDataError("沒資料")
        if query.startswith("blank:"):
            return pd.DataFrame({"ts": pd.Series([], dtype="datetime64[ns, UTC]"), "value": pd.Series([], dtype=float)})
        if query.startswith("bad:"):
            raise InvalidQueryError("查詢寫錯")
        if query.startswith("nan:"):
            df = real(self, query, start, end, step_seconds, field_map)
            df["value"] = float("nan")
            return df
        return real(self, query, start, end, step_seconds, field_map)

    def group(self, query, start, end, step_seconds, by, field_map=None):
        out = real_group(self, query, start, end, step_seconds, by, field_map)
        return {k: v for k, v in out.items() if k == f"{by}-0"} if query.startswith("solo:") else out

    monkeypatch.setattr(MockSpec, "fetch_data", fetch)
    monkeypatch.setattr(MockSpec, "fetch_group", group)


def _starve_wf(targets, ratio=1.0):
    specs = []
    for q in targets:
        if q.startswith("solo:"):
            specs.append(TargetSpec(source_type="mock", query="metric_x", entity=q, attrs={"pod": "pod-0"},
                                    peer_group={"query": q, "by": "pod"}))
        else:
            specs.append(TargetSpec(source_type="mock", query=q, entity=q))
    return Pipeline(Workflow(name="q8", targets=specs, max_failed_target_ratio=ratio, nodes=[
        NodeSpec(name="fetch", operator="ingestion.fetch", params={"lookback_minutes": 60}),
        NodeSpec(name="det", operator="audit.raise"),
        NodeSpec(name="z", operator="detector.zscore", params={"min_train_points": 10})]))


def exp_q8_starvation(flaky):
    cases = {
        "全部 NoDataError": ["empty:a", "empty:b"],
        "全部抓回空 df": ["blank:a", "blank:b"],
        "全部 BadRequest（查詢寫錯）": ["bad:a", "bad:b"],
        "全部值是 NaN": ["nan:a", "nan:b"],
        "全部偵測器 PlatformError": ["det_platform_a", "det_platform_b"],
        "全部偵測器 BadRequest": ["det_badreq_a", "det_badreq_b"],
        "全部偵測器 NotFound": ["det_notfound_a", "det_notfound_b"],
        "全部 peer_group 只撈到自己（fetch.py:82 NotFound）": ["solo:a", "solo:b"],
        "1 條 NoData + 1 條 BadRequest": ["empty:a", "bad:b"],
        "1 條偵測器 ValueError + 1 條正常": ["det_value_a", "metric_ok"],
    }
    out = {}
    for label, targets in cases.items():
        try:
            ctx = _starve_wf(targets).run(now=T0)
            out[label] = f"成功；errors={[(e.entity, e.message[:30]) for e in ctx.errors]}"
        except InfrastructureError as e:
            out[label] = f"InfrastructureError：{str(e)[:60]}"
        except Exception as e:  # noqa: BLE001
            out[label] = f"{type(e).__name__}：{str(e)[:60]}"
    show("Q8 strict=False", out)
    strict = {}
    for label, targets in {"NoData": ["empty:a"], "空 df": ["blank:a"], "BadRequest": ["bad:a"]}.items():
        try:
            _starve_wf(targets).run(now=T0, strict=True)
            strict[label] = "成功（沒丟錯）"
        except Exception as e:  # noqa: BLE001
            strict[label] = f"{type(e).__name__}({getattr(e, 'status_code', '')})：{str(e)[:50]}"
    show("Q8 strict=True", strict)


# ── Q9 ──

def exp_q9_requires_training():
    rows = {}
    for name in OperatorRegistry.names("detector."):
        cls = OperatorRegistry.get(name)
        rows[name] = {"DetectorOperator": issubclass(cls, DetectorOperator),
                      "has_baseline": getattr(cls, "has_baseline", False),
                      "requires_training": getattr(cls, "requires_training", None),
                      "requires_peers": getattr(cls, "requires_peers", None)}
    show("Q9 12 個偵測器", rows)

    def ctx_with(raw, train=None, state=None, node="n"):
        c = PipelineContext(pipeline="p", now=T0, target=Target("mock", "q", "svc"))
        c.raw_df, c.train_df = raw, train
        if state:
            c.detector_states[node] = state
        return c

    r = OperatorRegistry.create("detector.rules", {"upper": 80})
    c = r.execute(ctx_with(frame(1, spike=500.0)))
    show("Q9a rules 一個點", {"detector_runs": c.detector_runs, "anomalies": len(c.anomalies)})

    # wow：訓練窗 100 列（過門檻），但只有 2 列有上週同一時刻的值
    train = frame(100)
    train["lag_10080"] = float("nan")
    train.loc[98:, "lag_10080"] = [50.0, 51.0]
    raw = frame(30, spike=500.0)
    raw["lag_10080"] = 50.0
    w = OperatorRegistry.create("detector.wow")
    w.node_name = "n"
    c = w.execute(ctx_with(raw, train))
    show("Q9b wow：100 列訓練、只有 2 列有 lag", {"detector_runs": c.detector_runs,
                                                "learned_state": c.new_detector_states["n"].state if c.new_detector_states else None,
                                                "points_recorded": c.new_detector_states["n"].points if c.new_detector_states else None,
                                                "anomalies": len(c.anomalies)})

    # 已有可用的舊基準線、到期重訓但這次訓練窗只抓到 20 點 -> 連舊基準線也不用
    from aiops.engine.context import DetectorState
    old = DetectorState({"mean": 50.0, "std": 1.0}, T0 - timedelta(days=2), points=1440, version=3,
                        detector="detector.zscore")
    z = OperatorRegistry.create("detector.zscore")
    z.node_name = "n"
    c = z.execute(ctx_with(frame(30, spike=500.0), train=frame(20), state=old))
    show("Q9c 有舊基準線、重訓資料只有 20 點、這一輪有 500 的尖峰", {
        "detector_runs": c.detector_runs, "anomalies": len(c.anomalies),
        "audit": [a for a in c.audit if a.startswith("train:")]})


# ── Q13 ──

def exp_q13_routing_and_zero_overhead(monkeypatch):
    calls = {"DetectorModels.get": [], "ActiveConfigs.candidates": [], "fetch_data": 0}
    real_get, real_cand, real_fetch = (repository.DetectorModels.get, repository.ActiveConfigs.candidates,
                                       MockSpec.fetch_data)
    monkeypatch.setattr(repository.DetectorModels, "get",
                        staticmethod(lambda *a, **k: (calls["DetectorModels.get"].append(k.get("node", a[-1])), real_get(*a, **k))[1]))
    monkeypatch.setattr(repository.ActiveConfigs, "candidates",
                        staticmethod(lambda *a, **k: (calls["ActiveConfigs.candidates"].append(a[0]), real_cand(*a, **k))[1]))

    def fetch(self, *a, **k):
        calls["fetch_data"] += 1
        return real_fetch(self, *a, **k)

    monkeypatch.setattr(MockSpec, "fetch_data", fetch)
    import aiops.storage.hooks as hooks_mod
    monkeypatch.setattr(hooks_mod, "DetectorModels", repository.DetectorModels)
    monkeypatch.setattr(hooks_mod, "ActiveConfigs", repository.ActiveConfigs)

    def wf(targets):
        return Pipeline(Workflow(name="q13", targets=targets, nodes=[
            NodeSpec(name="fetch", operator="ingestion.fetch",
                     params={"lookback_minutes": 60, "train_lookback_minutes": 1440, "retrain_minutes": 1440}),
            NodeSpec(name="both", operator="detector.zscore", params={"include_entities": ["a"], "exclude_entities": ["a"]}),
            NodeSpec(name="inc_b", operator="detector.mad", params={"include_entities": ["b"]}),
            NodeSpec(name="sel", operator="detector.shift")]), StorageHooks())

    targets = [TargetSpec(source_type="mock", query="qa", entity="a", detectors=["both", "sel"]),
               TargetSpec(source_type="mock", query="qb", entity="b", detectors=["both"])]
    out = []
    for rnd in (1, 2):
        for k in calls:
            calls[k] = [] if isinstance(calls[k], list) else 0
        ctx = wf(targets).run(now=T0 + timedelta(minutes=rnd))
        out.append({"round": rnd, **{k: v for k, v in calls.items()},
                    "skip_audit": [a for a in ctx.audit if ":skipped:" in a]})
    show("Q13 路由順序與零開銷（a: both 被 include+exclude 同時點名、sel 有選；b: 只選 both、inc_b include b 但沒被選）", out)
    with db.read_session() as s:
        models_saved = sorted((r.entity, r.node) for r in s.query(models.DetectorModelRow).all())
    show("Q13 存下的基準線 (entity, node)", models_saved)

    # 全部偵測節點都被跳過的序列：每輪仍抓訓練窗嗎？
    calls["fetch_data"] = 0
    only_b = [TargetSpec(source_type="mock", query="qc", entity="c", detectors=["inc_b"])]  # inc_b 只 include b -> c 被跳過
    p = Pipeline(Workflow(name="q13b", nodes=wf(targets).workflow.nodes, targets=[*targets, *only_b]), StorageHooks())
    per_round = []
    for rnd in (1, 2, 3):
        calls["fetch_data"] = 0
        p.run(now=T0 + timedelta(minutes=10 + rnd), targets=only_b)
        per_round.append(calls["fetch_data"])
    show("Q13b 偵測節點全被跳過的序列 c，每輪 fetch_data 次數", per_round)


# ── Q14 ──

@OperatorRegistry.register("audit.nested")
class Nested(DetectorOperator):
    algorithm = "nested"
    tunable = {"threshold": (3.0, 1.0, 10.0)}
    other_params = frozenset({"bands"})
    seen: list = []  # noqa: RUF012  只在實驗裡用

    def fit(self, df):
        self.fitted = True
        return self

    def get_state(self):
        return {}

    def load_state(self, state):
        self.fitted = True
        return self

    def detect(self, df):
        return self._result(df, df["value"] * 0, df["value"] < 0)


def exp_q14_deep_merge():
    op = OperatorRegistry.create("audit.nested", {"bands": {"hi": 5, "nested": {"x": 1}}})
    op.node_name = "n"
    ctx = PipelineContext(pipeline="p", now=T0, target=Target("mock", "q", "svc",
                                                              overrides={"n": {"bands": {"mid": 3, "nested": {"y": 2}}}}))
    ctx.detector_params["n"] = {"bands": {"hi": 1, "lo": 2, "nested": {"z": 9}}, "threshold": 4.0}
    show("Q14 effective_params（調參 ← YAML ← override）", {"effective": op.effective_params(ctx),
                                                         "audit": ctx.audit})


# ── Q15 ──

def exp_q15_load_time_errors(client, monkeypatch):
    out = {}
    for label, spec in {"detectors 不存在": {"detectors": ["nope"]}, "overrides 不存在": {"overrides": {"nope": {}}},
                        "overrides 參數名錯": {"overrides": {"detect": {"thresh": 1}}},
                        "overrides 範圍錯": {"overrides": {"detect": {"threshold": 999}}}}.items():
        try:
            Pipeline(Workflow(name="q15", targets=[TargetSpec(source_type="mock", query="q", entity="e", **spec)], nodes=[
                NodeSpec(name="fetch", operator="ingestion.fetch"), NodeSpec(name="detect", operator="detector.zscore")]))
            out[label] = "沒擋"
        except WorkflowError as e:
            out[label] = f"WorkflowError（__init__）：{str(e)[:70]}"
    show("Q15a YAML targets -> Pipeline.__init__", out)

    touched = {"load_batch": 0, "fetch": 0}
    real_lb = StorageHooks.load_batch
    monkeypatch.setattr(StorageHooks, "load_batch", lambda self, ctx: (touched.__setitem__("load_batch", touched["load_batch"] + 1), real_lb(self, ctx))[1])
    real_fetch = MockSpec.fetch_data
    monkeypatch.setattr(MockSpec, "fetch_data", lambda self, *a, **k: (touched.__setitem__("fetch", touched["fetch"] + 1), real_fetch(self, *a, **k))[1])
    api = {}
    for label, spec in {"detectors 不存在": {"detectors": ["nope"]}, "overrides 不存在": {"overrides": {"nope": {}}}}.items():
        r = client.post("/api/v1/runs", json={"workflow": "metrics_only", "targets": [
            {"source_type": "mock", "query": "ok_metric", "entity": "ok"},
            {"source_type": "mock", "query": "q", "entity": "e", **spec}]})
        api[label] = {"status": r.status_code, "body": r.text[:160]}
    api["load_batch 被呼叫次數"], api["fetch 次數"] = touched["load_batch"], touched["fetch"]
    show("Q15b API targets -> run() 開頭", api)


EXPERIMENTS = [n for n in dir() if n.startswith("exp_")]


@pytest.mark.parametrize("name", EXPERIMENTS)
def test_experiment(name, request):
    fn = globals()[name]
    import inspect
    fn(**{p: request.getfixturevalue(p) for p in inspect.signature(fn).parameters})
