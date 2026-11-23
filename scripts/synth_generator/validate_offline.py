"""離線驗證：直接 import 平台的程式（不連 ES），吃產生器輸出的 NDJSON，印出實際量到的數字。

python -m scripts.synth_generator.validate_offline synth-out/nov03_cascade [--json 結果.json]

量什麼：
1. trace 文件交給平台的 aiops.sources.grafana._span 解析 -> SpanRecord -> engine.topology.service_edges，印出邊與次數
2. metrics 依 ES 會做的聚合（Σsummary.sum／Σvalue_count、Σsuccess_count）還原每分鐘延遲、請求數、錯誤率；
   跟同一段時間 trace 的平均耗時對照（同一個狀態函數，應該對得起來）
3. 偵測器回放（真的 Pipeline + 註冊的偵測算子）：latency 跑 mad、quantile_drift；qps 跑 hourly_baseline（訓練 14 天、min_weeks 2）；
   error_rate 三個一起跑看 ensemble 多數決能不能濾掉單點毛刺。印出事件區間內／外的警報點數
4. RCA：事件期間的延遲異常組成 Incident，用 rca.root_cause.topological 排名（pagerank、cross_correlation 已移除）
5. log：模擬 Grafana 沿 _source 逐層走 error.exception.message 的取值
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from aiops.engine.context import PipelineContext, SpanRecord
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.topology import service_edges
from aiops.engine.types import Incident
from aiops.interfaces.runner import load_all
from aiops.sources.grafana import _span
from aiops.stages.rca.algorithms.topological import TopologicalRootCause

warnings.filterwarnings("ignore")
KINDS = ("latency_ms", "qps", "error_rate")
_SERIES: dict[tuple[str, str], pd.DataFrame] = {}
_SPLIT: dict[str, pd.Timestamp] = {}


def read_docs(path: Path):
    with open(path, encoding="utf-8") as f:
        for k, line in enumerate(f):
            if k % 2 == 1:
                yield json.loads(line)


def metric_series(out: Path) -> dict[tuple[str, str], pd.DataFrame]:
    """跟 ES 的 date_histogram + sum 聚合一樣：同一分鐘、同服務的 success／failure 兩份文件加起來。"""
    acc: dict[tuple[str, str], list[float]] = defaultdict(lambda: [0.0, 0, 0, 0])
    for d in read_docs(out / "metrics.ndjson"):
        a = acc[(d["service"]["name"], d["@timestamp"])]
        s = d["transaction"]["duration"]["summary"]
        a[0] += s["sum"]
        a[1] += s["value_count"]
        a[2] += d["event"]["success_count"]["sum"]
        a[3] += d["event"]["success_count"]["value_count"]
    rows = pd.DataFrame([(svc, ts, *v) for (svc, ts), v in acc.items()],
                        columns=["service", "ts", "sum_us", "n", "ok", "ok_n"])
    rows["ts"] = pd.to_datetime(rows["ts"], utc=True)
    out_series = {}
    for svc, g in rows.sort_values("ts").groupby("service"):
        g = g.reset_index(drop=True)
        out_series[(svc, "latency_ms")] = pd.DataFrame({"ts": g.ts, "value": g.sum_us / g.n / 1000.0})
        out_series[(svc, "qps")] = pd.DataFrame({"ts": g.ts, "value": g.n.astype(float)})
        out_series[(svc, "error_rate")] = pd.DataFrame({"ts": g.ts, "value": 1 - g.ok / g.ok_n})
    return out_series


@OperatorRegistry.register("synth.replay_fetch")
class ReplayFetch(BaseOperator):
    """回放用：從記憶體裡的序列切訓練段與測試段（測試段前面多帶 60 點讓滾動窗湊滿，不計分）。"""

    def execute(self, ctx):
        kind = ctx.target.query.split("{", 1)[0]
        df = _SERIES[(ctx.target.entity, kind)]
        lo, split, hi = _SPLIT["train_start"], _SPLIT["train_end"], _SPLIT["test_end"]
        train = df[(df.ts >= lo) & (df.ts < split)]
        test = df[(df.ts >= split) & (df.ts < hi)]
        ctx.train_df = train.reset_index(drop=True)
        ctx.raw_df = pd.concat([train.tail(60), test]).reset_index(drop=True)
        return ctx


def run_detectors(services: list[str], kind: str, nodes: list[NodeSpec], train_start, train_end, test_end):
    _SPLIT.update(train_start=train_start, train_end=train_end, test_end=test_end)
    wf = Workflow(name=f"synth_{kind}", nodes=[NodeSpec(name="fetch", operator="synth.replay_fetch"), *nodes],
                  targets=[TargetSpec(source_type="mock", query=f'{kind}{{service="{s}"}}', entity=s) for s in services])
    ctx = Pipeline(wf).run(now=test_end.to_pydatetime(), strict=True)
    return [e for e in ctx.anomalies if pd.Timestamp(e.ts) >= train_end]


def count_in_out(events, w0, w1, by="detector_node"):
    table: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for e in events:
        t = pd.Timestamp(e.ts)
        table[getattr(e, by)][e.entity][0 if w0 <= t <= w1 else 1] += 1
    return {node: {ent: tuple(v) for ent, v in sorted(ents.items())} for node, ents in sorted(table.items())}


def grafana_path(src: dict[str, Any], path: str):
    """Grafana 的 ES log 面板取 logMessageField 的方式：沿 _source 用點號逐層走物件；遇到陣列就走不下去（回 None）。"""
    cur: Any = src
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur if isinstance(cur, str) else None


def main(argv=None) -> int:
    getattr(sys.stdout, "reconfigure", lambda **_: None)(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--json", help="把量到的數字另存成 JSON")
    args = ap.parse_args(argv)
    out = Path(args.out)
    gt = json.loads((out / "ground_truth.json").read_text(encoding="utf-8"))
    inc = gt["incidents"][0]
    w0, w1 = pd.Timestamp(inc["start"]), pd.Timestamp(inc["end"])
    load_all()
    result: dict[str, Any] = {"scenario": gt["scenario"], "mechanism": inc["mechanism"], "root_cause": inc["root_cause"]}
    print(f"=== {gt['scenario']}：{inc['mechanism']}，真實根因 {inc['root_cause']}，事件 {w0:%m-%d %H:%M}–{w1:%H:%M}Z ===")

    # 1. trace -> 平台 _span -> SpanRecord -> service_edges
    spans, zero_dur, n_docs = [], 0, 0
    tx_dur: dict[str, list[tuple[pd.Timestamp, float]]] = defaultdict(list)
    for d in read_docs(out / "traces.ndjson"):
        n_docs += 1
        s = _span(d)
        if s is None:
            continue
        zero_dur += s["duration_ms"] <= 0
        spans.append(SpanRecord(**s))
        if d["processor"]["event"] == "transaction":
            tx_dur[s["service"]].append((pd.Timestamp(s["ts"]), s["duration_ms"]))
    edges = service_edges(spans)
    traces_total = len({s.trace_id for s in spans})
    services_per_trace = Counter(len(v) for v in _group_services(spans).values())
    print(f"\n[1] trace：{n_docs} 份文件，_span 解析成功 {len(spans)}，duration=0 的 {zero_dur}；{traces_total} 條 trace，"
          f"每條 trace 的服務數分佈 {dict(sorted(services_per_trace.items()))}")
    print("    service_edges（caller -> callee: 次數）：")
    for (a, b), c in sorted(edges.items()):
        print(f"      {a} -> {b}: {c}")
    expected = {tuple(e) for e in gt["service_edges"]}
    print(f"    跟劇本 DAG 一致：{set(edges) == expected}（劇本 {sorted(expected)}）")
    result["service_edges"] = {f"{a}->{b}": c for (a, b), c in sorted(edges.items())}
    result["edges_match_dag"] = set(edges) == expected

    # 2. metrics 還原 + 跟 trace 對照
    _SERIES.clear()
    _SERIES.update(metric_series(out))
    services = sorted({s for s, _ in _SERIES})
    plateau = (w0 + pd.Timedelta(minutes=25), w0 + pd.Timedelta(minutes=100))
    print(f"\n[2] 同一個狀態函數的對照：高原期 {plateau[0]:%H:%M}–{plateau[1]:%H:%M}Z 的平均延遲（ms）")
    print(f"    {'服務':10} {'metrics':>9} {'trace':>9} {'trace 筆數':>9} {'平常(metrics)':>13}")
    consistency = {}
    for svc in services:
        lat = _SERIES[(svc, "latency_ms")]
        m = lat[(lat.ts >= plateau[0]) & (lat.ts < plateau[1])].value.mean()
        normal = lat[lat.ts < w0 - pd.Timedelta(hours=3)].value.median()
        tr = [d for t, d in tx_dur[svc] if plateau[0] <= t < plateau[1]]
        print(f"    {svc:10} {m:9.1f} {np.mean(tr) if tr else float('nan'):9.1f} {len(tr):9d} {normal:13.1f}")
        consistency[svc] = {"metrics_ms": round(float(m), 1), "trace_ms": round(float(np.mean(tr)), 1) if tr else None,
                            "trace_n": len(tr), "normal_ms": round(float(normal), 1)}
    result["plateau_latency"] = consistency

    # 3. 偵測器回放：訓練 10/20–11/03 00:00（14 天），測試 11/03 一整天
    t_start, t_split, t_end = (pd.Timestamp(gt["window"]["start"]), pd.Timestamp("2026-11-03T00:00:00Z"),
                               pd.Timestamp(gt["window"]["end"]))
    print(f"\n[3] 偵測器回放：訓練 {t_start:%m-%d}–{t_split:%m-%d %H:%M}（14 天），測試 {t_split:%m-%d} 一整天；"
          f"數字是「事件區間內／區間外」的異常點數")
    det: dict[str, Any] = {}
    lat_events = run_detectors(services, "latency_ms", [
        NodeSpec(name="lat_mad", operator="detector.mad"),
        NodeSpec(name="lat_qdrift", operator="detector.quantile_drift"),
    ], t_start, t_split, t_end)
    qps_events = run_detectors(services, "qps", [
        NodeSpec(name="qps_hourly", operator="detector.hourly_baseline", params={"tz": gt["tz"]}),
    ], t_start, t_split, t_end)
    for title, events in (("latency", lat_events), ("qps", qps_events)):
        table = count_in_out(events, w0, w1)
        for node, ents in table.items():
            print(f"    {title:8} {node:11} " + "  ".join(f"{e}={i}/{o}" for e, (i, o) in ents.items()))
            det[node] = {e: {"in": i, "out": o} for e, (i, o) in ents.items()}
    result["detectors"] = det

    # 3b. error_rate 單點毛刺：訓練 7 天（min_weeks 1），測試其餘 8 天（毛刺夠多才看得出濾掉幾個）
    e_split = t_start + pd.Timedelta(days=7)
    spikes = {s: [pd.Timestamp(t) for t in ts if pd.Timestamp(t) >= e_split] for s, ts in gt["error_rate_spikes"].items()}
    err_events = run_detectors(services, "error_rate", [
        NodeSpec(name="err_mad", operator="detector.mad"),
        NodeSpec(name="err_qdrift", operator="detector.quantile_drift"),
        NodeSpec(name="err_hourly", operator="detector.hourly_baseline", params={"tz": gt["tz"], "min_weeks": 1}),
        NodeSpec(name="vote", operator="detector.ensemble", params={"strategy": "majority", "keep_members": True}),
    ], t_start, e_split, t_end)
    strict_events = run_detectors(services, "error_rate", [
        NodeSpec(name="err_mad", operator="detector.mad"),
        NodeSpec(name="err_qdrift", operator="detector.quantile_drift"),
        NodeSpec(name="vote2", operator="detector.ensemble", params={"strategy": "majority", "keep_members": True}),
    ], t_start, e_split, t_end)
    n_spikes = sum(len(v) for v in spikes.values())
    print(f"\n[3b] error_rate 單點毛刺：測試段 {e_split:%m-%d}–{t_end:%m-%d} 有 {n_spikes} 個毛刺（ground truth），"
          f"事件區間外")
    spike_report = {}
    for label, events in (("mad+qdrift+hourly", err_events), ("mad+qdrift", strict_events)):
        by_node: dict[str, set] = defaultdict(set)
        for e in events:
            by_node[e.detector_node].add((e.entity, pd.Timestamp(e.ts)))
        for node in sorted(by_node):
            hit = sum((s, t) in by_node[node] for s, ts in spikes.items() for t in ts)
            outside = sum(1 for (_s, t) in by_node[node] if not (w0 <= t <= w1))
            inside = len(by_node[node]) - outside
            print(f"    {label:18} {node:11} 抓到毛刺 {hit}/{n_spikes}；區間外總點數 {outside}，區間內 {inside}")
            spike_report[f"{label}:{node}"] = {"spikes_hit": hit, "spikes": n_spikes, "outside": outside, "inside": inside}
    result["error_spikes"] = spike_report

    # 4. RCA：事件期間（前後 30 分鐘）的延遲異常（mad 或 qdrift 任一）組成一個 Incident
    pad = pd.Timedelta(minutes=30)
    events = [e for e in lat_events if w0 - pad <= pd.Timestamp(e.ts) <= w1 + pad]
    print(f"\n[4] RCA：事件 ±30 分鐘內的延遲異常 {len(events)} 筆，實體 "
          f"{sorted(Counter(e.entity for e in events).items())}")
    rca: dict[str, Any] = {}
    if events:
        incident = Incident(events=events)
        first = {e: min(pd.Timestamp(x.ts) for x in events if x.entity == e) for e in incident.entities}
        print("    各實體最早異常：" + "  ".join(f"{e}={t:%H:%M}" for e, t in sorted(first.items(), key=lambda kv: kv[1])))
        ctx = PipelineContext(pipeline="synth_rca", now=datetime.now(timezone.utc))
        ctx.topology = sorted(edges)
        algos = [("topological", TopologicalRootCause())]
        for label, algo in algos:
            cands = algo.rank(incident, ctx)
            top = cands[0] if cands else None
            ok = "✓" if top and top.entity == inc["root_cause"] else "✗"
            print(f"    {label:42} 第一候選 {top.entity if top else '（無）'} {ok if top else ''}")
            for c in cands[:3]:
                print(f"        {c.entity:9} score={c.score:.3f}  {c.reason}")
            rca[label] = [{"entity": c.entity, "score": round(c.score, 4), "reason": c.reason} for c in cands]
    result["rca"] = rca

    # 5. log 欄位
    n_log, ok_path, ok_msg, array_fail = 0, 0, 0, 0
    sample = None
    for d in read_docs(out / "logs.ndjson"):
        n_log += 1
        v = grafana_path(d, "error.exception.message")
        ok_path += v is not None
        ok_msg += isinstance(d.get("message"), str)
        legacy = {**d, "error": {**d["error"], "exception": [d["error"]["exception"]]}}  # 舊格式（陣列）
        array_fail += grafana_path(legacy, "error.exception.message") is None
        sample = sample or v
    print(f"\n[5] log：{n_log} 筆；沿 _source 走 error.exception.message 取到字串 {ok_path}/{n_log}，頂層 message {ok_msg}/{n_log}；"
          f"同樣文件改成舊的陣列格式時取不到 {array_fail}/{n_log}")
    print(f"    範例：{sample}")
    result["logs"] = {"docs": n_log, "grafana_path_ok": ok_path, "message_ok": ok_msg, "array_format_fails": array_fail}
    if args.json:
        Path(args.json).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


def _group_services(spans):
    by_trace: dict[str, set] = defaultdict(set)
    for s in spans:
        by_trace[s.trace_id].add(s.service)
    return by_trace


if __name__ == "__main__":
    raise SystemExit(main())
