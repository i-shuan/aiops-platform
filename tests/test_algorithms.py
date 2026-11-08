"""路線 A：168 時段基準線、動態同儕、同比／環比、分位數漂移、多偵測器裁決、模型檔（artifact）存儲。"""
from datetime import datetime, timedelta, timezone

import httpx
import numpy as np
import pandas as pd
import pytest

from aiops.core.context import PeerGroup, PipelineContext, Target
from aiops.core.errors import BadRequest, WorkflowError
from aiops.core.pipeline import Pipeline, load_workflow
from aiops.core.registry import OperatorRegistry
from aiops.core.types import AnomalyEvent
from aiops.datasource_specs.base import InvalidQueryError
from aiops.datasource_specs.elasticsearch import ElasticsearchSpec
from aiops.datasource_specs.mock import _seasonal
from aiops.datasource_specs.prometheus import PrometheusSpec
from aiops.operators.detectors.base import DetectorOperator, pack_arrays, unpack_arrays
from aiops.operators.detectors.ensemble import EnsembleOperator
from aiops.operators.fetch import FetchOperator, fetch_target_frame
from aiops.runner import run_context
from aiops.storage import crud
from aiops.storage.models import AnomalyRow, DetectorModelRow

SUNDAY = datetime(2026, 11, 1, tzinfo=timezone.utc)  # 2026-11-01 是星期日


def _seasonal_frame(start, minutes, query="seasonal:x"):
    ts = pd.date_range(start, periods=minutes, freq="60s")
    return pd.DataFrame({"ts": ts, "value": _seasonal(query, ts)})


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
    zscore = OperatorRegistry.get("detector.zscore")().fit(train)
    assert hourly.buckets_ready == 0 and hourly.get_state()["buckets_ready"] == 0
    assert hourly.detect(test).is_anomaly == zscore.detect(test).is_anomaly  # 逐點跟 zscore 一模一樣
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
    zscore = OperatorRegistry.get("detector.zscore")().fit(train).detect(test)
    assert hourly.is_anomaly[i] and not zscore.is_anomaly[i]
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
    from aiops.runner import workflow_path

    wf = load_workflow(workflow_path("it_otel"))
    nodes = {n.name: n for n in wf.nodes}
    assert nodes["detect"].operator == "detector.hourly_baseline"
    assert nodes["fetch"].params["train_lookback_minutes"] >= 10080


@pytest.mark.parametrize("name", ["it_otel", "factory"])
def test_default_detection_is_a_majority_vote_of_three_detectors(name):
    """真實資料回放：多數決比聯集少 88% 的單點誤報。it_otel 與 factory 預設都是這個組合。"""
    from aiops.runner import workflow_path

    wf = load_workflow(workflow_path(name))
    ops = [n.operator for n in wf.nodes]
    assert ops[ops.index("detector.hourly_baseline"):ops.index("detector.new_only") + 1] == [
        "detector.hourly_baseline", "detector.mad", "detector.quantile_drift", "detector.ensemble", "detector.new_only"]
    vote = next(n for n in wf.nodes if n.operator == "detector.ensemble")
    assert vote.params == {"strategy": "majority"}
    pipeline = Pipeline(wf)
    assert [n for n, _ in pipeline.detector_nodes] == ["detect", "detect_mad", "detect_qdrift"]  # 投票節點不載基準線


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
    ctx = _run_yaml(tmp_path, """name: dyn_peer
nodes:
  - {name: fetch, operator: ingestion.fetch, params: {lookback_minutes: 60, train_lookback_minutes: 1440}}
  - {name: peer, operator: detector.peer}
targets:
  - {source_type: mock, query: 'cpu{pod="pod-0"}', entity: pod-0, attrs: {service: cart},
     peer_group: {query: 'cpu{service="${service}"}', by: pod}}
""", now)
    assert ctx.anomalies and all(a.detector == "detector.peer" for a in ctx.anomalies) and not ctx.errors


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


# ── A.3 同比／環比、分位數漂移 ──


def test_fetch_lags_line_up_with_the_past():
    ctx = PipelineContext(pipeline="p", now=SUNDAY, target=Target("mock", "seasonal:x", "e"))
    start = SUNDAY + timedelta(days=2)
    df = fetch_target_frame(ctx, start, start + timedelta(hours=1), 60, [1440])
    past = _seasonal_frame(start - timedelta(days=1), 61)
    assert np.allclose(df["lag_1440"].to_numpy(), past["value"].to_numpy())
    with pytest.raises(ValueError, match="整數倍"):
        FetchOperator(step_seconds=120, lags_minutes=[1])


def test_day_over_day_flags_a_jump_that_yesterday_did_not_have():
    ctx = PipelineContext(pipeline="p", now=SUNDAY, target=Target("mock", "seasonal:x", "e"))
    # 只用平日（週二～週四）訓練：週一跟週日比本來就差很多（週末效應），那種落差要用 detector.wow
    train = fetch_target_frame(ctx, SUNDAY + timedelta(days=2), SUNDAY + timedelta(days=5), 60, [1440])
    test = fetch_target_frame(ctx, SUNDAY + timedelta(days=5), SUNDAY + timedelta(days=5, hours=6), 60, [1440])
    test.loc[100, "value"] += 25
    result = OperatorRegistry.get("detector.dod")().fit(train).detect(test)
    assert result.is_anomaly[100] and sum(result.is_anomaly) <= 3


def test_compare_detectors_need_the_lag_column(now):
    ctx = PipelineContext(pipeline="p", now=now, target=Target("mock", "q", "e"))
    ctx.raw_df = _seasonal_frame(SUNDAY, 60)
    with pytest.raises(BadRequest, match="lags_minutes"):
        OperatorRegistry.get("detector.wow")().execute(ctx)
    assert OperatorRegistry.get("detector.wow")().required_lags() == [10080]
    assert OperatorRegistry.get("detector.dod")(offset_minutes=60).required_lags() == [60]


def test_quantile_drift_sees_a_fatter_tail_with_the_same_average():
    rng = np.random.default_rng(1)
    base = rng.normal(100, 5, 2000)
    train = pd.DataFrame({"ts": pd.date_range(SUNDAY, periods=2000, freq="60s"), "value": base})
    tail = rng.normal(100, 5, 300)
    tail[::5] += 30  # 每 5 個點有一個很慢的請求
    tail -= tail.mean() - 100  # 平均拉回 100
    test = pd.DataFrame({"ts": pd.date_range(SUNDAY + timedelta(days=3), periods=300, freq="60s"), "value": tail})
    drift = OperatorRegistry.get("detector.quantile_drift")().fit(train).detect(test)
    chronic = OperatorRegistry.get("detector.chronic")().fit(train).detect(test)
    assert sum(drift.is_anomaly) > 100 and sum(chronic.is_anomaly) == 0


# ── A.4 多偵測器裁決 ──


def _event(node, minute, score=1.0):
    return AnomalyEvent("e", "m", SUNDAY + timedelta(minutes=minute), score, {"dept": "d"}, 1.0, "detector.zscore", node)


def _vote(events, runs=("a", "b", "c"), **params):
    ctx = PipelineContext(pipeline="p", now=SUNDAY, target=Target("mock", "m", "e"))
    ctx.anomalies, ctx.detector_runs = list(events), list(runs)
    op = EnsembleOperator(**params)
    op.node_name = "vote"
    return op.execute(ctx).anomalies


EVENTS = [_event("a", 1, 2), _event("b", 1, 5), _event("a", 2), _event("a", 3), _event("b", 3), _event("c", 3)]


def test_ensemble_strategies():
    def minutes(evts):
        return sorted(int((e.ts - SUNDAY).total_seconds() // 60) for e in evts)

    assert minutes(_vote(EVENTS, strategy="intersection")) == [3]
    assert minutes(_vote(EVENTS, strategy="majority")) == [1, 3]
    assert minutes(_vote(EVENTS, strategy="union")) == [1, 2, 3]
    assert minutes(_vote(EVENTS, strategy="at_least", min_votes=3)) == [3]
    first = next(e for e in _vote(EVENTS, strategy="majority") if e.ts == SUNDAY + timedelta(minutes=1))
    assert first.voted_by == ("a", "b") and first.score == 5 and first.detector == "detector.ensemble"
    assert first.detector_node == "vote"


def test_ensemble_keeps_non_participants_and_can_keep_members():
    external = AnomalyEvent("e", "m", SUNDAY, 9.0)  # 外部匯入：不屬於任何偵測節點
    out = _vote([*EVENTS, external], strategy="intersection", nodes=["a", "b"])
    assert external in out and len([e for e in out if e.detector == "detector.ensemble"]) == 2
    kept = _vote(EVENTS, strategy="intersection", keep_members=True)
    assert len(kept) == len(EVENTS) + 1


def test_ensemble_rejects_bad_params_at_load_time():
    with pytest.raises(WorkflowError):
        OperatorRegistry.create("detector.ensemble", {"strategy": "vibes"})
    with pytest.raises(WorkflowError, match="min_votes"):
        OperatorRegistry.create("detector.ensemble", {"strategy": "at_least"})


def test_ensemble_in_a_pipeline_records_the_decision_with_lineage(tmp_path, now):
    _run_yaml(tmp_path, """name: voted
nodes:
  - {name: fetch, operator: ingestion.fetch, params: {lookback_minutes: 60, train_lookback_minutes: 1440}}
  - {name: z_sensitive, operator: detector.zscore, params: {threshold: 1.5}}
  - {name: z_strict, operator: detector.zscore, params: {threshold: 6.0}}
  - {name: vote, operator: detector.ensemble, params: {strategy: intersection}}
  - {name: new_only, operator: detector.new_only}
targets:
  - {source_type: mock, query: 'cpu{service="cart"}', entity: cart}
""", now, name="voted")
    rows = [r for r in crud.find(AnomalyRow) if r["pipeline"] == "voted"]
    assert rows and {(r["detector_name"], r["detector_node"]) for r in rows} == {("detector.ensemble", "vote")}
    wf = load_workflow(tmp_path / "voted.yaml")
    assert [n for n, _ in Pipeline(wf).detector_nodes] == ["z_sensitive", "z_strict"]  # 裁決節點不載基準線


def test_events_are_immutable():
    with pytest.raises(Exception):
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
