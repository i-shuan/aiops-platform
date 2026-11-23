"""架構稽核（2026-11-09，main@bd16b7a）找到的缺口。報告：docs/ARCH_AUDIT.md（每個缺口的重現方式、影響、建議修法）。

每個測試寫的是「應該要有的行為」。還沒修的標 xfail(strict=True)：哪天修好了會變成 XPASS -> 紅燈，提醒把 xfail 拿掉。
已修的（docstring 標「已修」）就是一般的回歸測試：G1、G6～G15（G5 除外）、G17、G18、G19。
裁決不修的：G5（attrs 唯讀化會讓 JSON 序列化失敗，WontFix）、G16（調參不分節點是設計決定）；G3、G4 留待下一輪。
"""
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import peer_probe  # 測試專用的 requires_peers 偵測器
import test_architecture as arch  # tests/ 在 sys.path 上（pytest 預設 prepend 匯入模式）
from aiops.engine.context import DetectorState, PeerRef, PipelineContext, SpanRecord, Target
from aiops.engine.errors import InfrastructureError, NotFound
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent
from aiops.sources.mock import MockSpec
from aiops.stages.rca.spans import inbound_callers
from aiops.storage import db, models
from aiops.storage.hooks import StorageHooks

T0 = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)


def _frame(n, spike=None):
    values = [50.0 + (i % 3) for i in range(n)]
    if spike is not None and n:
        values[-1] = spike
    return pd.DataFrame({"ts": pd.date_range(T0 - timedelta(hours=2), periods=n, freq="60s"), "value": values})


@OperatorRegistry.register("audit_gap.frame")
class _Frame(BaseOperator):
    """固定資料的 fetch：120 點、最後一點是 500 的尖峰。"""

    scope = "target"

    def execute(self, ctx):
        ctx.raw_df = _frame(120, 500.0)
        return ctx


_SNR = {"snr": {"p_fa": 0.001}}


def _detect(nodes):
    wf = Workflow(name="gap", targets=[TargetSpec(source_type="mock", query="q", entity="svc")], nodes=[
        NodeSpec(name="fetch", operator="audit_gap.frame"), *nodes])
    return Pipeline(wf).run(now=T0)


# ── Q1／Q3：架構測試的盲點（用暫存的假 aiops/ 樹餵給既有的架構測試，它應該要紅） ──


def _guard_on(tmp_path, monkeypatch, files: dict[str, str], guard) -> bool:
    for rel, body in files.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body, encoding="utf-8")
    monkeypatch.setattr(arch, "ROOT", tmp_path)
    try:
        guard()
    except AssertionError:
        return True  # 架構測試紅了 = 抓到
    return False


_CLASS_LEVEL = "class Det:\n    tunable = {'threshold': (3.0, 1.0, 10.0)}\n"  # 讓「至少掃到一個類別層級物件」的前提成立


@pytest.mark.xfail(strict=True, reason="Q3 缺口：test_no_module_or_class_level_state_is_mutated_at_runtime 只認"
                   "「直接對模組層級名稱呼叫 append/update…或下標賦值」；別名、類別層級計數器、lru_cache、屬性賦值、"
                   "跨模組修改都抓不到")
@pytest.mark.parametrize("body", [
    "_SEEN = []\ndef f(k):\n    s = _SEEN\n    s.append(k)\n",
    "class Op:\n    runs = 0\n    def execute(self, ctx):\n        type(self).runs += 1\n        return ctx\n",
    "import functools\n@functools.lru_cache(maxsize=None)\ndef lookup(k):\n    return k\n",
    "import types\n_STATE = types.SimpleNamespace(n=0)\ndef f():\n    _STATE.n = _STATE.n + 1\n",
    "from aiops.engine import topology\ndef f():\n    topology.CACHE = {}\n",
], ids=["alias", "class-counter", "lru_cache", "attr-assign", "cross-module"])
def test_mutable_state_guard_catches_indirect_mutation(tmp_path, monkeypatch, body):
    assert _guard_on(tmp_path, monkeypatch, {"stages/x/base.py": _CLASS_LEVEL, "stages/x/probe.py": body},
                     arch.test_no_module_or_class_level_state_is_mutated_at_runtime)


@pytest.mark.parametrize("rel,body,guard", [
    ("stages/grouping/probe.py", "import importlib\nrepo = importlib.import_module('aiops.storage.repository')\n",
     "test_operators_do_not_touch_orm_or_storage_except_admin"),
    ("stages/grouping/probe.py", "import http.client\n", "test_operators_do_not_use_the_network_except_llm_notifiers_admin"),
    ("stages/enrichment/probe.py", "from aiops.sources import base\n",
     "test_operators_do_not_use_the_network_except_llm_notifiers_admin"),
], ids=["importlib-storage", "http.client", "enrichment-imports-sources"])
def test_import_guards_catch_indirect_imports(tmp_path, monkeypatch, rel, body, guard):
    """G1（已修）：import 防線也看 importlib.import_module 的字串引數、http.client，且 enrichment/ 不准 import sources。"""
    assert _guard_on(tmp_path, monkeypatch, {rel: body}, getattr(arch, guard))


# ── Q4：資料血緣 ──


@pytest.mark.xfail(strict=True, reason="Q4 缺口：AnomalyEvent 是 frozen，但 attrs 是普通 dict，下游可以原地改（淺層凍結）")
def test_anomaly_event_attrs_cannot_be_mutated_in_place():
    e = AnomalyEvent("svc", "q", T0, 9.0, {"dept": "it"}, detector="detector.hourly_baseline", detector_node="z")
    with pytest.raises(TypeError):
        e.attrs["dept"] = "changed"


# ── Q5：棄權 ──


@pytest.mark.parametrize("op", ["detector.residual_chronic"])
def test_window_longer_than_the_data_abstains(op):
    """G6（已修）：滾動窗比這一輪資料長 -> can_evaluate=False -> 棄權，z 的事件不受影響。
    原本後面接表決節點（detector.ensemble、後來 soft_vote），驗的是棄權者不算參與者；表決節點已移除，
    改成直接看基底留下的棄權紀錄，與事件只來自 z（斷言 voted_by == ("z",) 改成 detector_node == "z"）。"""
    ctx = _detect([NodeSpec(name="z", operator="detector.hourly_baseline", params=_SNR),
                   NodeSpec(name="w", operator=op, params={"window": 600, **_SNR})])
    assert [a.detector_node for a in ctx.anomalies] == ["z"]
    assert "detect:w:cannot-evaluate:abstain" in ctx.audit and "detect:z:cannot-evaluate:abstain" not in ctx.audit


# ── Q6：入口 span 的定義 ──


def test_orphan_spans_are_not_counted_as_external_entry_requests():
    """G7（已修）：父 span 不在樣本裡的孤兒記成 UNKNOWN_CALLER（"?"），仍算一次請求，但不是外部入口（""）。"""
    spans = [SpanRecord("t1", "a1", None, "api", T0, 100), SpanRecord("t1", "b1", "a1", "cart", T0, 80),
             SpanRecord("t2", "b2", "missing-parent", "cart", T0, 80)]
    callers = inbound_callers(spans, T0 - timedelta(minutes=1), T0 + timedelta(minutes=1))
    assert callers["cart"].get("", 0) == 0
    assert callers["cart"] == {"api": 1, "?": 1} and callers["api"] == {"": 1}


# ── Q8：斷流攔截 ──


@pytest.fixture
def solo_group(monkeypatch):
    real = MockSpec.fetch_group
    monkeypatch.setattr(MockSpec, "fetch_group", lambda self, query, start, end, step, by, field_map=None:
                        {k: v for k, v in real(self, query, start, end, step, by, field_map).items() if k == f"{by}-0"})


def test_peer_group_with_only_itself_is_not_data_starvation(solo_group):
    """G8（已修）：單一副本的服務 peer_group 只撈到自己 -> 記成那條序列的錯誤、需要同儕的偵測器棄權，不算斷流。"""
    targets = [TargetSpec(source_type="mock", query="m", entity=e, attrs={"pod": "pod-0"},
                          peer_group={"query": f"grp_{e}", "by": "pod"}) for e in ("a", "b")]
    ctx = Pipeline(Workflow(name="gap_q8", targets=targets, nodes=[
        NodeSpec(name="fetch", operator="ingestion.fetch"), NodeSpec(name="p", operator=peer_probe.NAME)])).run(now=T0)
    assert sorted(e.entity for e in ctx.errors) == ["a", "b"]  # 記成這兩條序列的錯誤，不是整輪斷流


@OperatorRegistry.register("audit_gap.notfound")
class _NotFoundInDetector(BaseOperator):
    scope = "target"

    def execute(self, ctx):
        raise NotFound("偵測器內部查表找不到（不是資料源沒資料）")


def test_not_found_from_a_non_fetch_node_is_not_starvation():
    """G8（已修）：只有資料源丟的 NoDataError 算斷流；其他節點丟的 NotFound 只是那條序列的錯誤。"""
    targets = [TargetSpec(source_type="mock", query=q, entity=q) for q in ("a", "b")]
    ctx = Pipeline(Workflow(name="gap_q8b", targets=targets, nodes=[
        NodeSpec(name="fetch", operator="ingestion.fetch"), NodeSpec(name="x", operator="audit_gap.notfound")])).run(now=T0)
    assert sorted(e.entity for e in ctx.errors) == ["a", "b"]


@OperatorRegistry.register("audit_gap.bug")
class _BugOnOneTarget(BaseOperator):
    scope = "target"

    def execute(self, ctx):
        if ctx.target.entity == "broken":
            raise ValueError("演算法 bug")
        return ctx


def test_a_bug_on_one_target_does_not_take_down_the_whole_round():
    """G9（已修）：strict=False 時一條序列的任何例外都只記在 errors（含型別），健康的序列照跑；strict=True 照丟。"""
    targets = [TargetSpec(source_type="mock", query=q, entity=q) for q in ("broken", "healthy")]
    wf = Workflow(name="gap_q8c", targets=targets, nodes=[
        NodeSpec(name="fetch", operator="ingestion.fetch"), NodeSpec(name="x", operator="audit_gap.bug")])
    ctx = Pipeline(wf).run(now=T0)
    assert [e.entity for e in ctx.errors] == ["broken"] and ctx.errors[0].message.startswith("ValueError: ")
    with pytest.raises(ValueError):
        Pipeline(wf).run(now=T0, strict=True)


def test_strict_mode_reports_an_empty_frame_as_no_data_404(monkeypatch):
    """G10（已修）：strict=True（手動動作）時資料源回 200 但零筆，跟資料源丟 NoDataError 一樣是 404；
    strict=False（排程）照舊走斷流判斷（503）。"""
    from aiops.engine.errors import NoDataError
    monkeypatch.setattr(MockSpec, "fetch_data", lambda self, q, s, e, step, fm=None: pd.DataFrame(
        {"ts": pd.Series([], dtype="datetime64[ns, UTC]"), "value": pd.Series([], dtype=float)}))
    wf = Workflow(name="gap_q8d", targets=[TargetSpec(source_type="mock", query="q", entity="svc")],
                  nodes=[NodeSpec(name="fetch", operator="ingestion.fetch")])
    with pytest.raises(NoDataError) as e:
        Pipeline(wf).run(now=T0, strict=True)
    assert e.value.status_code == 404 and "svc" in str(e.value)
    with pytest.raises(InfrastructureError, match="資料斷流"):
        Pipeline(wf).run(now=T0)


# ── Q9：冷啟動門檻 ──


def _ctx(raw, train=None, state=None):
    c = PipelineContext(pipeline="p", now=T0, target=Target("mock", "q", "svc"))
    c.raw_df, c.train_df = raw, train
    if state is not None:
        c.detector_states["n"] = state
    return c


def test_baseline_records_usable_training_points_for_peer_detectors():
    """G11：DetectorState.points 記的是 training_points（lag／同儕有值的列），不是列數：100 列只有 2 列有同儕的值 -> 2。
    原本這裡驗的是冷啟動門檻（2 < min_train_points 60 -> 不學、不判，detector_runs == []）；min_train_points 已於
    2026-11-20 移除，現在 2 個點也照學照判，所以斷言改成「有跑、記下來的點數是 2」。"""
    train = _frame(100)
    train["peer_0"] = float("nan")
    train.loc[98:, "peer_0"] = [50.0, 51.0]
    raw = _frame(30, 500.0)
    raw["peer_0"] = 50.0
    op = OperatorRegistry.create(peer_probe.NAME)
    op.node_name = "n"
    ctx = _ctx(raw, train)
    ctx.target = Target("mock", "q", "svc", {}, [PeerRef("mock", "p0")])
    out = op.execute(ctx)
    assert out.detector_runs == ["n"] and out.new_detector_states["n"].points == 2


def test_retrain_with_few_points_replaces_the_old_baseline():
    """原本是 G12「到期重訓但訓練窗資料不足（20 點 < min_train_points）時沿用舊基準線」；min_train_points 與那條
    沿用舊基準線的路已移除：20 個點也照樣重訓，新基準線取代舊的（照判，沒有偵測盲區這點不變）。"""
    old = DetectorState({"buckets": {}, "global": [50.0, 1.0]}, T0 - timedelta(days=2), points=1440, version=3,
                        detector="detector.hourly_baseline")  # 沒有時段資料：每個點都用全段的 (平均 50, 標準差 1)
    op = OperatorRegistry.create("detector.hourly_baseline")
    op.node_name = "n"
    ctx = op.execute(_ctx(_frame(30, 500.0), train=_frame(20), state=old))
    assert ctx.detector_runs == ["n"] and ctx.anomalies
    assert ctx.new_detector_states["n"].points == 20 and not any("keep-old-baseline" in line for line in ctx.audit)


# ── Q12：結案寫入失敗 ──


@OperatorRegistry.register("audit_gap.spike")
class _Spike(BaseOperator):
    scope = "target"

    def execute(self, ctx):
        ctx.anomalies = [AnomalyEvent(ctx.target.entity, "q", T0 - timedelta(minutes=1), 8.0)]
        return ctx


def test_watchdog_does_not_renew_when_finalize_fails(monkeypatch):
    """G13（已修）：notifier.watchdog 改到 after_finalize，「已通知」與結案寫失敗時心跳不續命。"""
    from sqlalchemy.exc import OperationalError

    from aiops.storage import repository
    beats = []
    cls = OperatorRegistry.get("notifier.watchdog")
    real = cls.heartbeat
    monkeypatch.setattr(cls, "heartbeat", lambda self, ctx, now: (beats.append(now), real(self, ctx, now))[1])
    monkeypatch.setattr("aiops.storage.db.time.sleep", lambda s: None)

    def boom(*a, **k):
        raise OperationalError("INSERT", {}, Exception("db gone after push"))

    monkeypatch.setattr(repository.Notifications, "record", staticmethod(boom))

    wf = Workflow(name="gap_q12", targets=[TargetSpec(source_type="mock", query="q", entity="svc")], nodes=[
        NodeSpec(name="d", operator="audit_gap.spike"), NodeSpec(name="g", operator="alert.same_entity"),
        NodeSpec(name="p", operator="alert.notify_payload"), NodeSpec(name="n", operator="notifier.log"),
        NodeSpec(name="w", operator="notifier.watchdog")])
    with pytest.raises(InfrastructureError):
        Pipeline(wf, StorageHooks()).run(now=T0)
    assert beats == []


# ── Q13：被跳過的節點零開銷 ──


def test_target_whose_detectors_are_all_skipped_does_not_fetch_training_data(monkeypatch):
    """G14（已修）：引擎把這條序列真的會跑的基準線節點放進 ctx.baseline_nodes；全被跳過的序列 fetch 不抓訓練窗。"""
    calls = []
    real = MockSpec.fetch_data
    monkeypatch.setattr(MockSpec, "fetch_data", lambda self, q, s, e, step, fm=None: (calls.append((s, e)), real(self, q, s, e, step, fm))[1])
    targets = [TargetSpec(source_type="mock", query="qa", entity="a"), TargetSpec(source_type="mock", query="qc", entity="c")]
    p = Pipeline(Workflow(name="gap_q13", targets=targets, nodes=[
        NodeSpec(name="fetch", operator="ingestion.fetch",
                 params={"lookback_minutes": 60, "train_lookback_minutes": 1440, "retrain_minutes": 1440}),
        NodeSpec(name="z", operator="detector.hourly_baseline", params={"include_entities": ["a"]})]), StorageHooks())
    p.run(now=T0, targets=[targets[1]])
    assert len(calls) == 1  # 只抓偵測窗


# ── Q14：回測跟正式偵測用的參數不一樣 ──


def test_backtest_uses_the_same_effective_params_as_the_pipeline(tmp_path, monkeypatch, now):
    """G15（已修）：回測跟偵測走同一個 layer_params：疊上提案所屬管線的節點 YAML params（鎖定鍵）；
    YAML 鎖了 threshold 時現行與提案都是 9.0，結果一樣，並把被鎖的鍵列在 workflow.locked。"""
    from aiops.engine.requests import ProposalBacktest, ProposalCreate
    from aiops.interfaces import runner
    import shutil
    shutil.copytree(runner.WORKFLOW_DIR, tmp_path, dirs_exist_ok=True)  # 內建的 admin_* 也要找得到
    (tmp_path / "detection" / "locked.yaml").write_text(
        "name: locked\nnodes:\n  - {name: fetch, operator: ingestion.fetch}\n"
        "  - {name: detect, operator: detector.hourly_baseline, params: {threshold: 9.0}}\n", encoding="utf-8")
    monkeypatch.setattr(runner, "WORKFLOW_DIR", tmp_path)
    pid = runner.run("admin_proposal_create", request=ProposalCreate(
        source="manual", detector="detector.hourly_baseline", params={"threshold": 1.5}, proposer="amy",
        pipeline="locked"))["proposal"]["id"]
    req = ProposalBacktest(proposal_id=pid, metric={"source_type": "mock", "query": "cpu"}, train_ratio=0.3,
                           time_range={"start": now.replace(hour=0), "end": now, "step_seconds": 60})
    bt = runner.run("admin_proposal_backtest", request=req, targets=req.to_targets(), now=now)["proposal"]["backtest"]
    assert bt["current_params"].get("threshold") == 9.0 and bt["proposed_params"].get("threshold") == 9.0
    assert bt["workflow"]["node"] == "detect" and bt["workflow"]["locked"] == ["threshold"]
    assert bt["current_anomalies"] == bt["proposed_anomalies"]  # 鎖住了，提案在正式偵測裡不會有任何差別


# ── Q8 追加：資料源回 HTTP 200 但內容是 NaN／Inf 或格式不對 ──


def test_malformed_source_response_is_a_platform_error():
    """G18（已修）：資料源回 200 但格式不對 -> InvalidQueryError（PlatformError），記成那條序列的錯誤。"""
    import json as _json

    import httpx

    from aiops.engine.errors import PlatformError
    from aiops.sources.prometheus import PrometheusSpec
    start = T0 - timedelta(minutes=60)
    # Elasticsearch 指標查詢的兩個案例（桶沒有 key、value 是字串）隨 ElasticsearchSpec.fetch_data 一起刪除；
    # Grafana 資料源回應格式不對的案例在 tests/test_grafana_source.py
    body = _json.dumps({"status": "success", "data": {"result": [{"metric": {}, "values": [[T0.timestamp(), "abc"]]}]}}).encode()
    spec, q = PrometheusSpec("http://p", client=httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=body)))), "up"
    with pytest.raises(PlatformError):
        spec.fetch_data(q, start, T0, 60)


# ── Q15 追加：RCA 帶不存在的事件群 ──


def test_rca_for_a_missing_incident_says_which_incident(client):
    """G19（已修）：load_batch 查不到事件群就丟 NotFound 帶 id，API 回 404。"""
    r = client.post("/api/v1/rca", json={"incident_id": 999999})
    assert r.status_code == 404 and "999999" in r.text
