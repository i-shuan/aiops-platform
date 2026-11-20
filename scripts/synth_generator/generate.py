"""第 4 層的前半：把三種訊號寫成本機 bulk NDJSON（_bulk 的 create 格式），外加 ground_truth.json 與 manifest.json。

不連任何網路。載進 ES 的 es_writer.py 已刪除（repo 不再有直連 ES 的程式）。
ground_truth.json 只給離線驗證用，**不寫進 ES 文件**。
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .documents import KINDS, data_stream
from .faults import Scenario, iso, validate
from .metric_emitter import metric_docs
from .topology import DEFAULT_TOPOLOGY, Topology
from .trace_emitter import SLOW_FACTOR, trace_and_log_docs
from .waveforms import SystemState, build_state

FILES = {"metrics": "metrics.ndjson", "traces": "traces.ndjson", "logs": "logs.ndjson"}


def _line(index: str, doc: dict) -> str:
    action = json.dumps({"create": {"_index": index}}, separators=(",", ":"))
    return action + "\n" + json.dumps(doc, separators=(",", ":"), ensure_ascii=False) + "\n"


def ground_truth(state: SystemState) -> dict[str, Any]:
    topo, sc = state.topo, state.scenario
    incidents = []
    for inc in sc.incidents:
        faults = {f.entity: f for f in inc.faults}
        if inc.mechanism == "callee_slowdown":
            # 上游的耗時包含下游，下游一變慢上游同一分鐘就跟著變慢（沒有 lag）
            affected = {inc.root_cause: iso(inc.start), **{u: iso(inc.start) for u in sorted(topo.upstream(inc.root_cause))}}
            direction = "callee -> caller（跟平台 propagation_edges 同向）"
        else:
            affected = {e: iso(inc.onset(e)) for e in faults}
            for u in sorted(topo.upstream(inc.root_cause)):
                affected.setdefault(u, iso(inc.start))  # 上游的總耗時包含根因自己的變慢
            direction = "caller -> callee（負載放大，跟平台 propagation_edges 反向）"
        incidents.append({
            "id": inc.id, "mechanism": inc.mechanism, "root_cause": inc.root_cause, "direction": direction,
            "start": iso(inc.start), "end": iso(inc.end),
            "envelope_minutes": {"ramp": inc.envelope.ramp_minutes, "plateau": inc.envelope.plateau_minutes,
                                 "decay": inc.envelope.decay_minutes},
            "self_faults": [{"entity": f.entity, "onset": iso(inc.onset(f.entity)), "lag_minutes": f.lag_minutes,
                             "latency_shift_ms": f.latency_shift_ms, "trace_error_rate": f.trace_error_rate,
                             "call_multiplier": f.call_multiplier} for f in inc.faults],
            "latency_affected_onset": affected,
            "qps_affected": sorted(topo.downstream(inc.root_cause)) if inc.mechanism == "load_amplification" else [],
        })
    return {
        "scenario": sc.name, "seed": sc.seed, "window": {"start": iso(sc.window.start), "end": iso(sc.window.end)},
        "tz": sc.tz, "entry": topo.entry,
        "service_edges": sorted([list(e) for e in topo.edges()]),
        "external_dependencies": sorted({c.callee for s in topo.services.values() for c in s.calls
                                         if not topo.is_service(c.callee)}),
        "incidents": incidents,
        "error_rate_spikes": {s: [iso(state.ts[i].to_pydatetime()) for i in idx] for s, idx in sorted(state.spikes.items())},
        "slow_log_factor": SLOW_FACTOR,
    }


def generate(scenario: Scenario, out_dir: str | Path, topo: Topology = DEFAULT_TOPOLOGY) -> dict[str, Any]:
    warnings = validate(scenario, topo)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    state = build_state(scenario, topo)
    ds = {k: data_stream(k, scenario.namespace) for k in KINDS}
    counts = dict.fromkeys(KINDS, 0)
    with open(out / FILES["metrics"], "w", encoding="utf-8", newline="\n") as f:
        for doc in metric_docs(state):
            f.write(_line(ds["metrics"], doc))
            counts["metrics"] += 1
    with open(out / FILES["traces"], "w", encoding="utf-8", newline="\n") as ft, \
            open(out / FILES["logs"], "w", encoding="utf-8", newline="\n") as fl:
        for batch in trace_and_log_docs(state):
            for doc in batch.traces:
                ft.write(_line(ds["traces"], doc))
            for doc in batch.logs:
                fl.write(_line(ds["logs"], doc))
            counts["traces"] += len(batch.traces)
            counts["logs"] += len(batch.logs)
    (out / "ground_truth.json").write_text(json.dumps(ground_truth(state), ensure_ascii=False, indent=2) + "\n",
                                           encoding="utf-8", newline="\n")
    manifest = {
        "scenario": scenario.name, "seed": scenario.seed, "namespace": scenario.namespace,
        "files": {FILES[k]: {"kind": k, "data_stream": ds[k], "docs": counts[k], "sha256": _sha256(out / FILES[k])}
                  for k in KINDS},
        "ground_truth_sha256": _sha256(out / "ground_truth.json"),
        "warnings": warnings,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                                       encoding="utf-8", newline="\n")
    return manifest


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
