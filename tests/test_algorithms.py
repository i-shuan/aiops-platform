"""路線 A：168 時段基準線、動態同儕、lag 欄位、多偵測器裁決的血緣、模型檔（artifact）存儲。"""
from datetime import datetime, timedelta, timezone

import httpx
import numpy as np
import pandas as pd
import pytest

import peer_probe  # 測試專用的 requires_peers 偵測器（tests/ 在 sys.path 上）
from aiops.engine.context import PeerGroup, PipelineContext, Target
from aiops.engine.errors import BadRequest
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import Pipeline, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent
from aiops.sources.base import InvalidQueryError
from aiops.sources.elasticsearch import ElasticsearchSpec
from aiops.sources.mock import _seasonal
from aiops.sources.prometheus import PrometheusSpec
from aiops.stages.detectors.base import DetectorOperator, pack_arrays, unpack_arrays
from aiops.stages.ingestion.fetch import FetchOperator, fetch_target_frame
from aiops.interfaces.runner import run_context
from aiops.storage import crud
from aiops.storage.models import AnomalyRow, DetectorModelRow

SUNDAY = datetime(2026, 11, 1, tzinfo=timezone.utc)  # 2026-11-01 是星期日


def _seasonal_frame(start, minutes, query="seasonal:x"):
    ts = pd.date_range(start, periods=minutes, freq="60s")
    return pd.DataFrame({"ts": ts, "value": _seasonal(query, ts)})


def _global_zscore_flags(train, test, threshold=3.0):
    """對照組：整段訓練資料只有一組平均與標準差（以前的 detector.zscore），偏離超過 threshold 個標準差就算。"""
    scores = ((test["value"] - train["value"].mean()) / train["value"].std(ddof=0)).abs()
    return (scores > threshold).tolist()


def _run_yaml(tmp_path, text, now, name="wf"):
    path = tmp_path / f"{name}.yaml"
    path.write_text(text, encoding="utf-8")
    return run_context(str(path), now=now, strict=False, allow_file=True)


# ── A.1 168 時段基準線 ──


def test_hourly_baseline_falls_back_to_the_global_baseline_with_less_than_two_weeks_of_data():
    """只有 7 天訓練資料：每個「週幾幾點」只看過一次，變異被低估，所以整條退回全段統計（等同 zscore），不會更吵。"""
    train = _seasonal_frame(SUNDAY, 7 * 1440)
    test = _seasonal_frame(SUNDAY + timedelta(days=7), 1440)
    test.loc[3 * 60, "value"] = 52.0
    hourly = OperatorRegistry.get("detector.hourly_baseline")().fit(train)
    assert hourly.buckets_ready == 0 and hourly.get_state()["buckets_ready"] == 0
    assert hourly.detect(test).is_anomaly == _global_zscore_flags(train, test)  # 逐點跟全段 zscore 一模一樣
    # 想強制用 7 天資料就明說：min_weeks=1（變異會被低估，真實資料回放時流量指標因此很吵）
    forced = OperatorRegistry.get("detector.hourly_baseline")(min_weeks=1).fit(train)
    assert forced.buckets_ready == 168 and forced.detect(test).is_anomaly[3 * 60]


def test_hourly_baseline_turns_on_per_slot_once_it_has_seen_enough_weeks():
    two_weeks = OperatorRegistry.get("detector.hourly_baseline")().fit(_seasonal_frame(SUNDAY, 14 * 1440))
    assert two_weeks.buckets_ready == 168
    # 訓練窗口從某小時中間開始：被切成兩半的那一小時合起來只算一次，不會被當成兩週
    odd = OperatorRegistry.get("detector.hourly_baseline")().fit(_seasonal_frame(SUNDAY + timedelta(minutes=20), 7 * 1440))
    assert odd.buckets_ready == 0 and max(v[3] for v in odd.buckets_.values()) <= 1.01
    with pytest.raises(TypeError, match="min_weeks"):
        OperatorRegistry.get("detector.hourly_baseline")(min_weeks=0.5)


def test_hourly_baseline_loads_states_saved_before_min_weeks_existed():
    algo = OperatorRegistry.get("detector.hourly_baseline")().fit(_seasonal_frame(SUNDAY, 14 * 1440))
    old = algo.get_state()
    old["buckets"] = {b: v[:3] for b, v in old["buckets"].items()}  # 舊版只存 (平均, 標準差, 點數)
    restored = OperatorRegistry.get("detector.hourly_baseline")().load_state(old)
    assert restored.buckets_ready == 0  # 不知道看過幾週 -> 保守退回全段統計，等下次重訓
    restored.detect(_seasonal_frame(SUNDAY + timedelta(days=14), 60))  # 照樣能跑


def test_hourly_baseline_catches_what_a_single_global_baseline_misses():
    train = _seasonal_frame(SUNDAY, 14 * 1440)
    test = _seasonal_frame(SUNDAY + timedelta(days=14), 1440)  # 第三週的星期日
    i = 3 * 60  # 星期日凌晨三點：平常是一天裡最低的時候
    test.loc[i, "value"] = 52.0  # 這個值放在下午很正常，放在凌晨三點就是異常
    hourly = OperatorRegistry.get("detector.hourly_baseline")().fit(train).detect(test)
    assert hourly.is_anomaly[i] and not _global_zscore_flags(train, test)[i]
    normal_flags = sum(hourly.is_anomaly) - 1
    assert normal_flags / len(test) < 0.02  # 平常的點幾乎不誤報


def test_hourly_baseline_buckets_follow_the_timezone():
    utc = OperatorRegistry.get("detector.hourly_baseline")()
    taipei = OperatorRegistry.get("detector.hourly_baseline")(tz="Asia/Taipei")
    ts = pd.Series(pd.to_datetime(["2026-11-08T00:00:00Z"]))  # UTC 星期日 00:00 = 台北星期日 08:00
    assert utc._bucket(ts)[0] == 6 * 24 and taipei._bucket(ts)[0] == 6 * 24 + 8
    with pytest.raises(TypeError, match="時區"):
        OperatorRegistry.get("detector.hourly_baseline")(tz="Mars/Olympus")


def test_hourly_baseline_in_a_pipeline_stores_168_buckets(tmp_path):
    now = SUNDAY + timedelta(days=7, hours=12)
    _run_yaml(tmp_path, """name: hourly
nodes:
  - {name: fetch, operator: ingestion.fetch, params: {lookback_minutes: 60, train_lookback_minutes: 10080}}
  - {name: detect, operator: detector.hourly_baseline}
targets:
  - {source_type: mock, query: 'seasonal:traffic', entity: web}
""", now)
    state = next(m["state"] for m in crud.find(DetectorModelRow) if m["pipeline"] == "hourly")
    assert len(state["buckets"]) == 168 and state["tz"] == "UTC"


def test_it_otel_uses_the_hourly_baseline_with_a_week_of_training():
    from aiops.interfaces.runner import workflow_path

    wf = load_workflow(workflow_path("sample_it_otel"))
    nodes = {n.name: n for n in wf.nodes}
    assert nodes["detect"].operator == "detector.hourly_baseline"
    assert nodes["fetch"].params["train_lookback_minutes"] >= 10080


def test_factory_sample_detection_is_three_independent_detectors():
    """factory 樣本的偵測組合：三個偵測節點（週期、突變、漂移）都開 snr，各自成立；沒有表決節點（soft_vote 已移除）。"""
    from aiops.interfaces.runner import workflow_path

    wf = load_workflow(workflow_path("sample_factory"))
    ops = [n.operator for n in wf.nodes]
    assert ops[ops.index("detector.hourly_baseline"):ops.index("detector.new_only") + 1] == [
        "detector.hourly_baseline", "detector.residual_shift", "detector.residual_chronic", "detector.new_only"]
    # factory 保留預設的安全退回（不寫 min_weeks）
    hourly = next(n for n in wf.nodes if n.operator == "detector.hourly_baseline")
    assert hourly.params.get("min_weeks") is None
    pipeline = Pipeline(wf)
    assert [n for n, _ in pipeline.detector_nodes] == ["detect", "detect_rshift", "detect_rchronic"]  # new_only 不載基準線
    assert all(op.snr is not None for _, op in pipeline.nodes if getattr(op, "has_baseline", False))  # 門檻都由 p_fa 換算


# ── A.2 動態同儕 ──


def _peer_ctx(attrs, entity="pod-0", query='cpu{service="${service}"}'):
    target = Target("mock", 'cpu{pod="pod-0"}', entity, attrs, [], PeerGroup(query, "pod"))
    return PipelineContext(pipeline="p", now=SUNDAY, target=target)


def test_peer_group_pulls_the_whole_group_and_excludes_self():
    ctx = FetchOperator().execute(_peer_ctx({"service": "cart", "pod": "pod-2"}))
    assert ctx.peer_members == ["pod-0", "pod-1", "pod-3", "pod-4"]
    assert [c for c in ctx.raw_df.columns if c.startswith("peer_")] == ["peer_0", "peer_1", "peer_2", "peer_3"]


def test_peer_group_errors_are_clear():
    with pytest.raises(BadRequest, match="service"):
        FetchOperator().execute(_peer_ctx({"pod": "pod-0"}))  # query 引用的 ${service} 沒有
    with pytest.raises(BadRequest, match="找不到自己"):
        FetchOperator().execute(_peer_ctx({"service": "cart", "pod": "nope"}))


def test_peer_detector_works_with_a_dynamic_group(tmp_path, now):
    """引擎的同儕路徑（peer_group -> peer_* 欄位 -> requires_peers 偵測器）；偵測器用測試專用的 peer_probe。"""
    ctx = _run_yaml(tmp_path, f"""name: dyn_peer
nodes:
  - {{name: fetch, operator: ingestion.fetch, params: {{lookback_minutes: 60, train_lookback_minutes: 1440}}}}
  - {{name: peer, operator: {peer_probe.NAME}}}
targets:
  - {{source_type: mock, query: 'cpu{{pod="pod-0"}}', entity: pod-0, attrs: {{service: cart}},
     peer_group: {{query: 'cpu{{service="${{service}}"}}', by: pod}}}}
""", now)
    assert ctx.anomalies and all(a.detector == peer_probe.NAME for a in ctx.anomalies) and not ctx.errors


def _prom(series):
    body = {"status": "success", "data": {"resultType": "matrix", "result": series}}
    return PrometheusSpec("http://prom", client=httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, json=body))))


def test_prometheus_fetch_group_splits_by_label():
    s = [{"metric": {"pod": "a"}, "values": [[1762560000, "1"]]}, {"metric": {"pod": "b"}, "values": [[1762560000, "2"]]}]
    members = _prom(s).fetch_group("cpu", SUNDAY, SUNDAY + timedelta(hours=1), 60, "pod")
    assert sorted(members) == ["a", "b"] and members["b"]["value"].tolist() == [2.0]
    with pytest.raises(InvalidQueryError, match="沒有 node 標籤"):
        _prom(s).fetch_group("cpu", SUNDAY, SUNDAY + timedelta(hours=1), 60, "node")
    dup = [s[0], {"metric": {"pod": "a", "x": "1"}, "values": [[1762560000, "3"]]}]
    with pytest.raises(InvalidQueryError, match="多條序列"):
        _prom(dup).fetch_group("cpu", SUNDAY, SUNDAY + timedelta(hours=1), 60, "pod")


def test_sources_without_group_support_say_so():
    with pytest.raises(InvalidQueryError, match="不支援動態同儕"):
        ElasticsearchSpec("http://es").fetch_group('{"index": "m", "agg": "count"}', SUNDAY, SUNDAY, 60, "host")


# ── A.3 同比／環比 ──


def test_fetch_lags_line_up_with_the_past():
    ctx = PipelineContext(pipeline="p", now=SUNDAY, target=Target("mock", "seasonal:x", "e"))
    start = SUNDAY + timedelta(days=2)
    df = fetch_target_frame(ctx, start, start + timedelta(hours=1), 60, [1440])
    past = _seasonal_frame(start - timedelta(days=1), 61)
    assert np.allclose(df["lag_1440"].to_numpy(), past["value"].to_numpy())
    with pytest.raises(ValueError, match="整數倍"):
        FetchOperator(step_seconds=120, lags_minutes=[1])


@OperatorRegistry.register("test.lag_probe")
class _LagProbe(DetectorOperator):
    """測試專用：需要上週同一時刻（lag_10080）的偵測器。同比／環比偵測器已移除，基底的 lag 檢查仍要有人測。"""
    algorithm = "lag_probe"
    tunable = {"threshold": (3.0, 1.0, 20.0)}

    def required_lags(self):
        return [10080]

    def fit(self, df):
        self.fitted = True
        return self

    def get_state(self):
        return {}

    def load_state(self, state):
        self.fitted = True
        return self

    def detect(self, df):
        scores = (df["value"] - df["lag_10080"]).abs()
        return self._result(df, scores, scores > self.param("threshold"))


def test_lag_detectors_need_the_lag_column(now):
    ctx = PipelineContext(pipeline="p", now=now, target=Target("mock", "q", "e"))
    ctx.raw_df = _seasonal_frame(SUNDAY, 60)
    with pytest.raises(BadRequest, match="lags_minutes 包含 \[10080\]"):
        OperatorRegistry.get("test.lag_probe")().execute(ctx)


# ── A.4 多個偵測節點：各自成立、各自留血緣 ──


def _event(node, minute, score=1.0):
    return AnomalyEvent("e", "m", SUNDAY + timedelta(minutes=minute), score, {"dept": "d"}, 1.0, "detector.hourly_baseline", node)


def test_two_detector_nodes_in_a_pipeline_each_record_their_own_lineage(tmp_path, now):
    """原本測的是軟投票裁決事件的血緣（anomalies 表只有 detector.soft_vote／vote）；soft_vote 已移除，
    同一條管線改成驗「兩個節點各自的事件都落地、各帶自己的節點名」。"""
    _run_yaml(tmp_path, """name: voted
nodes:
  - {name: fetch, operator: ingestion.fetch, params: {lookback_minutes: 60, train_lookback_minutes: 1440}}
  - {name: z_sensitive, operator: detector.hourly_baseline, params: {snr: {p_fa: 0.01}}}
  - {name: z_strict, operator: detector.hourly_baseline, params: {snr: {p_fa: 0.0001}}}
  - {name: new_only, operator: detector.new_only}
targets:
  - {source_type: mock, query: 'cpu{service="cart"}', entity: cart}
""", now, name="voted")
    rows = [r for r in crud.find(AnomalyRow) if r["pipeline"] == "voted"]
    assert {(r["detector_name"], r["detector_node"]) for r in rows} == {("detector.hourly_baseline", "z_sensitive"),
                                                                         ("detector.hourly_baseline", "z_strict")}
    by_node = {node: {r["ts"] for r in rows if r["detector_node"] == node} for node in ("z_sensitive", "z_strict")}
    assert by_node["z_strict"] <= by_node["z_sensitive"]  # 同一個演算法：門檻嚴的節點抓到的點，門檻鬆的一定也抓到
    wf = load_workflow(tmp_path / "voted.yaml")
    assert [n for n, _ in Pipeline(wf).detector_nodes] == ["z_sensitive", "z_strict"]


@OperatorRegistry.register("test.lineage_frame")
class _LineageFrame(BaseOperator):
    """固定資料：訓練段 ±1 交錯（mean 0、std 1），偵測段最後一點 50 -> 兩個偵測節點都會抓到。"""

    scope = "target"

    def execute(self, ctx):
        ts = pd.date_range(ctx.now - timedelta(minutes=30), periods=30, freq="60s")
        ctx.train_df = pd.DataFrame({"ts": pd.date_range(ctx.now - timedelta(days=1), periods=100, freq="60s"),
                                     "value": [(-1.0) ** i for i in range(100)]})
        ctx.raw_df = pd.DataFrame({"ts": ts, "value": [0.0] * 29 + [50.0]})
        return ctx


def test_lineage_is_fixed_at_detection_and_lands_unchanged_in_the_table_and_the_snapshot(now):
    """稽核 Q4：兩個偵測節點 + 一筆外部匯入異常，走 StorageHooks 落地（原本中間有軟投票裁決，已移除）。
    anomalies 表每個節點各記自己的事件；事件群快照帶著每個事件自己的 detector、detector_node（voted_by 現在一律是空的）；
    外部匯入的不寫 anomalies 表，快照裡是 external:<source>、節點空字串。"""
    from aiops.engine.pipeline import NodeSpec, TargetSpec, Workflow
    from aiops.storage.hooks import StorageHooks
    from aiops.storage.models import IncidentRow
    from aiops.storage.repository import ExternalAnomalies

    ExternalAnomalies.create_many([{"pipeline": "lineage", "source": "fdc", "external_id": "x1", "entity": "tool-9",
                                    "metric": "temp", "ts": (now - timedelta(minutes=3)).replace(tzinfo=None),
                                    "value": 99.0, "score": 7.0, "attrs": {}}])
    wf = Workflow(name="lineage", targets=[TargetSpec(source_type="mock", query="q", entity="svc")], nodes=[
        NodeSpec(name="fetch", operator="test.lineage_frame"),
        NodeSpec(name="z", operator="detector.hourly_baseline", params={"snr": {"p_fa": 0.001}}),
        NodeSpec(name="m", operator="detector.hourly_baseline", params={"snr": {"p_fa": 0.001}}),
        NodeSpec(name="new_only", operator="detector.new_only"),
        NodeSpec(name="group", operator="alert.same_entity"),
        NodeSpec(name="absorb", operator="alert.absorb_open"),
        NodeSpec(name="payload", operator="alert.notify_payload"),
        NodeSpec(name="heartbeat", operator="alert.heartbeat"),
        NodeSpec(name="notify", operator="notifier.log")])
    ctx = Pipeline(wf, StorageHooks()).run(now=now)
    assert {(e.detector, e.detector_node, e.voted_by) for e in ctx.detected_anomalies} == \
        {("detector.hourly_baseline", "z", ()), ("detector.hourly_baseline", "m", ())}
    rows = [r for r in crud.find(AnomalyRow) if r["pipeline"] == "lineage"]
    assert {(r["entity"], r["detector_name"], r["detector_node"]) for r in rows} == {
        ("svc", "detector.hourly_baseline", "z"), ("svc", "detector.hourly_baseline", "m")}
    snapshot = {tuple(i["incident"]["entities"]): sorted((ev["detector"], ev["detector_node"], list(ev["voted_by"]))
                                                         for ev in i["incident"]["events"]) for i in crud.find(IncidentRow)}
    assert snapshot == {("svc",): [("detector.hourly_baseline", "m", []), ("detector.hourly_baseline", "z", [])],
                        ("tool-9",): [("external:fdc", "", [])]}


def test_events_are_immutable():
    from dataclasses import FrozenInstanceError

    with pytest.raises(FrozenInstanceError):  # 只接受「frozen 擋下修改」，別的例外不算
        _event("a", 1).score = 99  # type: ignore[misc]


# ── A.5 模型檔（artifact） ──


@pytest.fixture
def artifact_detector():
    name = "detector.test_artifact"
    if not OperatorRegistry.has(name):
        @OperatorRegistry.register(name)
        class ArtifactDetector(DetectorOperator):
            algorithm = "test_artifact"
            tunable = {"threshold": (3.0, 1.0, 10.0)}
            loaded: list = []

            def fit(self, df):
                self.mean_, self.weights_ = float(df["value"].mean()), np.arange(5.0)
                self.fitted = True
                return self

            def get_state(self):
                return {"mean": self.mean_}

            def load_state(self, state):
                self.mean_, self.fitted = state["mean"], True
                return self

            def get_artifact(self):
                return pack_arrays({"w": self.weights_})

            def load_artifact(self, fmt, data):
                self.weights_ = unpack_arrays(fmt, data)["w"]
                type(self).loaded.append(self.weights_.tolist())

            def detect(self, df):
                scores = (df["value"] - self.mean_).abs()
                return self._result(df, scores, scores > 1000)
    return OperatorRegistry.get(name)


def test_artifacts_are_saved_and_loaded_back(tmp_path, now, artifact_detector):
    text = """name: art
nodes:
  - {name: fetch, operator: ingestion.fetch, params: {lookback_minutes: 60, train_lookback_minutes: 1440}}
  - {name: model, operator: detector.test_artifact}
targets:
  - {source_type: mock, query: 'cpu', entity: cpu}
"""
    _run_yaml(tmp_path, text, now)
    row = next(m for m in crud.find(DetectorModelRow) if m["pipeline"] == "art")
    assert row["artifact_format"] == "npz" and row["artifact"]
    _run_yaml(tmp_path, text, now + timedelta(minutes=5))  # 基準線沒過期：載入而不是重訓
    assert artifact_detector.loaded and artifact_detector.loaded[-1] == [0.0, 1.0, 2.0, 3.0, 4.0]


def test_artifacts_never_use_pickle(tmp_path):
    import io

    with pytest.raises(ValueError, match="物件型別"):
        pack_arrays({"x": np.array([{"a": 1}], dtype=object)})
    buf = io.BytesIO()
    np.savez(buf, x=np.array([{"evil": 1}], dtype=object))  # 有人竄改資料庫裡的模型檔，塞進需要 pickle 的內容
    with pytest.raises(ValueError):
        unpack_arrays("npz", buf.getvalue())
    with pytest.raises(ValueError, match="不支援"):
        unpack_arrays("pickle", b"x")
