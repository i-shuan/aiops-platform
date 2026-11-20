"""scripts/synth_generator：拓撲邊、確定性、雙速密度、文件欄位符合平台的讀取程式（aiops/sources/grafana.py 的 _span）。

用 2 天的小劇本跑（完整 2 週的劇本由 validate_offline 驗）。
"""
import hashlib
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from aiops.engine.context import SpanRecord
from aiops.engine.topology import propagation_edges, service_edges
from aiops.sources.grafana import _span
from scripts.synth_generator.faults import Envelope, scenario_from_dict, validate
from scripts.synth_generator.generate import generate
from scripts.synth_generator.topology import DEFAULT_TOPOLOGY
from scripts.synth_generator.waveforms import build_state


def small(**over):
    raw = {
        "name": "small", "namespace": "gentest", "seed": 42, "tz": "Asia/Taipei",
        "window": {"start": "2026-11-02T00:00:00Z", "end": "2026-11-04T00:00:00Z", "step_seconds": 60},
        "base_rate_per_minute": 40,
        "traces": {"dense_per_minute": 6, "sparse_per_hour": 4, "margin_minutes": 120},
        "incidents": [{
            "id": "inc", "mechanism": "load_amplification", "start": "2026-11-03T14:05:00Z", "end": "2026-11-03T16:20:00Z",
            "root_cause": "cart-api",
            "propagation": [
                {"entity": "cart-api", "lag_minutes": 0, "latency_shift_ms": 280, "trace_error_rate": 0.35, "call_multiplier": 2.5},
                {"entity": "ord-svc", "lag_minutes": 5, "latency_shift_ms": 210, "trace_error_rate": 0.25},
                {"entity": "bill-svc", "lag_minutes": 15, "latency_shift_ms": 160, "trace_error_rate": 0.15},
            ],
        }],
    }
    raw.update(over)
    return raw


def docs(path: Path):
    lines = path.read_text(encoding="utf-8").splitlines()
    return [(json.loads(a), json.loads(b)) for a, b in zip(lines[::2], lines[1::2])]


@pytest.fixture(scope="module")
def out(tmp_path_factory):
    d = tmp_path_factory.mktemp("synth")
    generate(scenario_from_dict(small()), d)
    return d


def sha(d: Path) -> dict[str, str]:
    return {f: hashlib.sha256((d / f).read_bytes()).hexdigest()
            for f in ("metrics.ndjson", "traces.ndjson", "logs.ndjson", "ground_truth.json")}


def test_same_seed_same_bytes_other_seed_differs(out, tmp_path):
    generate(scenario_from_dict(small()), tmp_path / "again")
    assert sha(tmp_path / "again") == sha(out)
    generate(scenario_from_dict(small(seed=7)), tmp_path / "seed7")
    assert sha(tmp_path / "seed7")["traces.ndjson"] != sha(out)["traces.ndjson"]


def test_trace_tree_gives_the_dag_edges(out):
    spans = [SpanRecord(**_span(d)) for _, d in docs(out / "traces.ndjson")]
    edges = service_edges(spans)
    assert set(edges) == DEFAULT_TOPOLOGY.edges() == {("api-gw", "auth-svc"), ("api-gw", "cart-api"),
                                                      ("cart-api", "ord-svc"), ("ord-svc", "bill-svc")}
    # 平台的傳播方向（callee -> caller）
    assert ("bill-svc", "ord-svc") in propagation_edges(list(edges), [])


def test_parent_ids_link_strictly_inside_one_trace(out):
    by_trace = defaultdict(list)
    for _, d in docs(out / "traces.ndjson"):
        by_trace[d["trace"]["id"]].append(d)
    for tid, ds in by_trace.items():
        ids = {d["span"]["id"] if "span" in d else d["transaction"]["id"] for d in ds}
        roots = [d for d in ds if "parent" not in d]
        assert len(roots) == 1 and roots[0]["service"]["name"] == "api-gw" and roots[0]["processor"]["event"] == "transaction"
        for d in ds:
            if "parent" in d:
                assert d["parent"]["id"] in ids, tid
            if d["processor"]["event"] == "span":
                assert d["parent"]["id"] == d["transaction"]["id"]
                assert d["span"]["destination"]["service"]["resource"]
        # 跨服務：下游 transaction 的 parent.id 是上游 client span 的 span.id
        span_svc = {d["span"]["id"]: d["service"]["name"] for d in ds if d["processor"]["event"] == "span"}
        for d in ds:
            if d["processor"]["event"] == "transaction" and "parent" in d:
                assert span_svc[d["parent"]["id"]] != d["service"]["name"]


def test_every_trace_doc_reads_back_through_platform_span_reader(out):
    for action, d in docs(out / "traces.ndjson"):
        s = _span(d)
        assert s is not None and s["duration_ms"] > 0
        kind = d["processor"]["event"]
        assert kind in ("transaction", "span")
        assert ("span" in d) == (kind == "span")  # transaction 文件不能帶 span（_span 會讀成 duration 0）
        assert d["event"]["outcome"] in ("success", "failure")
        assert action == {"create": {"_index": "traces-apm-gentest"}}


def test_transaction_duration_is_self_plus_calls(out):
    by_tx = defaultdict(int)
    tx = {}
    for _, d in docs(out / "traces.ndjson"):
        if d["processor"]["event"] == "span":
            by_tx[d["transaction"]["id"]] += d["span"]["duration"]["us"]
        else:
            tx[d["transaction"]["id"]] = d["transaction"]["duration"]["us"]
    assert all(tx[t] >= by_tx[t] for t in tx)


def test_dual_speed_trace_density(out):
    roots = [pd.Timestamp(d["@timestamp"]) for _, d in docs(out / "traces.ndjson")
             if d["processor"]["event"] == "transaction" and "parent" not in d]
    roots = pd.Series(roots)
    dense = roots[(roots >= pd.Timestamp("2026-11-03T12:05Z")) & (roots < pd.Timestamp("2026-11-03T18:20Z"))]
    sparse = roots[roots < pd.Timestamp("2026-11-03T12:05Z")]
    dense_per_min = len(dense) / (375)
    sparse_per_hour = len(sparse) / (36 + 5 / 60)
    assert 4.5 < dense_per_min < 7.5
    assert 2 < sparse_per_hour < 6.5


def test_metrics_follow_the_same_state_as_traces(out):
    state = build_state(scenario_from_dict(small()), DEFAULT_TOPOLOGY)
    acc = defaultdict(lambda: [0.0, 0])
    for action, d in docs(out / "metrics.ndjson"):
        assert action == {"create": {"_index": "metrics-apm.transaction.1m-gentest"}}
        h = d["transaction"]["duration"]["histogram"]
        assert h["values"] == sorted(h["values"]) and len(set(h["values"])) == len(h["values"])
        assert sum(h["counts"]) == d["transaction"]["duration"]["summary"]["value_count"] \
            == d["event"]["success_count"]["value_count"]
        a = acc[(d["service"]["name"], d["@timestamp"])]
        a[0] += d["transaction"]["duration"]["summary"]["sum"]
        a[1] += d["transaction"]["duration"]["summary"]["value_count"]
    t0 = state.scenario.window.start
    for svc in ("cart-api", "ord-svc", "bill-svc"):
        minutes = range(2 * 1440 - 1440 + 14 * 60 + 35, 1440 + 15 * 60 + 40)   # 高原期
        got = sum(acc[(svc, (t0 + pd.Timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S.000Z"))][0] for i in minutes) / \
            sum(acc[(svc, (t0 + pd.Timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S.000Z"))][1] for i in minutes) / 1000
        want = float(np.mean([state.e2e_ms[svc][i] for i in minutes]))
        assert abs(got - want) / want < 0.05, (svc, got, want)


def test_error_logs_share_trace_and_grafana_can_walk_the_message(out):
    trace_ids = {d["trace"]["id"] for _, d in docs(out / "traces.ndjson")}
    logs = docs(out / "logs.ndjson")
    assert logs
    for action, d in logs:
        assert action == {"create": {"_index": "logs-apm.error-gentest"}}
        assert d["processor"]["event"] == "error" and d["trace"]["id"] in trace_ids
        ex = d["error"]["exception"]
        assert isinstance(ex, dict) and isinstance(ex["message"], str) and ex["message"] == d["message"]
        assert set(ex) == {"type", "message", "handled"}


def test_envelope_ramp_plateau_decay():
    env = Envelope(10, 90, 20)
    v = env(np.array([-1, 0, 5, 10, 60, 99.9, 110, 120, 130]))
    assert v[0] == 0 and v[1] == 0 and v[2] == pytest.approx(0.5) and v[3] == v[4] == v[5] == 1
    assert 0 < v[6] < 0.3 and v[7] == 0 and v[8] == 0


@pytest.mark.parametrize("change, message", [
    ({"mechanism": None}, "mechanism"),
    ({"mechanism": "callee_slowdown"}, "最下游"),
    ({"propagation": [{"entity": "cart-api", "latency_shift_ms": 100}]}, "call_multiplier"),
    ({"root_cause": "redis-cache"}, "propagation"),
])
def test_scenario_mistakes_are_rejected(change, message):
    raw = small()
    raw["incidents"][0].update(change)
    with pytest.raises(ValueError, match=message):
        validate(scenario_from_dict(raw), DEFAULT_TOPOLOGY)


def test_namespace_must_be_gen_so_existing_streams_are_never_written():
    for bad in ("scn", "synth", "default", "gen-x"):
        with pytest.raises(ValueError, match="namespace"):
            scenario_from_dict(small(namespace=bad))


def test_callee_slowdown_only_moves_the_root_itself():
    raw = small()
    raw["incidents"][0] = {"id": "slow", "mechanism": "callee_slowdown", "start": "2026-11-03T14:05:00Z",
                           "end": "2026-11-03T16:05:00Z", "root_cause": "bill-svc",
                           "propagation": [{"entity": "bill-svc", "latency_shift_ms": 400, "trace_error_rate": 0.2}]}
    state = build_state(scenario_from_dict(raw), DEFAULT_TOPOLOGY)
    i = 1440 + 15 * 60   # 11/03 15:00，高原期
    assert state.self_ms["ord-svc"][i] == pytest.approx(45.0, rel=0.15)        # ord-svc 自己沒變
    assert state.e2e_ms["ord-svc"][i] - state.e2e_ms["ord-svc"][100] > 350     # 但整段耗時包含 bill-svc 的 400ms
    assert state.mult["cart-api"][i] == 1.0
