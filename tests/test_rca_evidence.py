"""rca.root_cause.evidence 的強斷言驗收（CLAUDE.md 第 99、100 節）。

資料：scripts/synth_generator/scenarios/ 底下的正式劇本，只把時間窗縮短到事故當天（11/3 11:00–17:00Z）讓測試跑得動，
並換 3 個 seed 重產，確認不是單一隨機樣本的運氣。span 一律用平台讀 ES 的同一個函式（aiops.sources.elasticsearch._span）
從產生器的原始 APM 文件解析，不手工捏造。完整 2 週劇本走真的 rca_report workflow 的結果記在 README「RCA」一節。

斷言：
- callee_slowdown 第 1 名必須是 bill-svc；load_amplification 第 1 名必須是 cart-api。第 2 名不算過，不准改資料只准改算子。
- 共同外部因素（每個服務一起、同幅度變慢）必須回「證據不足」，不准指認無辜的服務。
- 沒有故障的對照劇本（同一套產生器，事故效果全設 0）不准指認任何服務。
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from aiops.engine.context import PipelineContext, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.sources.elasticsearch import _span
from scripts.synth_generator.faults import scenario_from_dict
from scripts.synth_generator.generate import generate

SCENARIOS = Path(__file__).resolve().parents[1] / "scripts" / "synth_generator" / "scenarios"
WINDOW = {"start": "2026-11-03T11:00:00Z", "end": "2026-11-03T17:00:00Z", "step_seconds": 60}
BASELINE = timedelta(minutes=15)  # 跟 rca_report.yaml 的 incident_traces before_minutes 一樣


def _generate(tmp_path_factory, name: str, seed: int, mutate=None) -> Path:
    raw = yaml.safe_load((SCENARIOS / f"{name}.yaml").read_text(encoding="utf-8"))
    raw.update(window=WINDOW, seed=seed)
    if mutate:
        mutate(raw)
    out = tmp_path_factory.mktemp(f"{name}-{seed}")
    generate(scenario_from_dict(raw), out)
    return out


def _spans(out: Path) -> list[SpanRecord]:
    lines = (out / "traces.ndjson").read_text(encoding="utf-8").splitlines()
    return [SpanRecord(**s) for s in (_span(json.loads(doc)) for doc in lines[1::2]) if s]


def _incident(gt_incident: dict) -> Incident:
    """事件群 = 標準答案裡延遲受影響的服務，從各自的起點到事故結束每分鐘一個異常點（等於偵測器理想的輸出）。"""
    end = datetime.fromisoformat(gt_incident["end"].replace("Z", "+00:00"))
    events = []
    for entity, onset in gt_incident["latency_affected_onset"].items():
        t = datetime.fromisoformat(onset.replace("Z", "+00:00"))
        while t <= end:
            events.append(AnomalyEvent(entity, "latency", t, 5.0))
            t += timedelta(minutes=1)
    return Incident(events)


def _rank(spans, incident, **params):
    ctx = PipelineContext(pipeline="rca", now=incident.end)
    lo, hi = incident.start - BASELINE, incident.end + timedelta(minutes=1)
    ctx.rca_spans = [s for s in spans if lo <= s.ts <= hi]  # 跟 incident_traces 撈的範圍一樣（事件前 15 分鐘到結束）
    ranked = OperatorRegistry.create("rca.root_cause.evidence", params).rank(incident, ctx)
    return ranked, ctx


@pytest.mark.parametrize("seed", [42, 7, 2026])
@pytest.mark.parametrize("name", ["nov03_callee_slowdown", "nov03_cascade"])
def test_root_cause_is_ranked_first(tmp_path_factory, name, seed):
    out = _generate(tmp_path_factory, name, seed)
    gt = json.loads((out / "ground_truth.json").read_text(encoding="utf-8"))["incidents"][0]
    ranked, ctx = _rank(_spans(out), _incident(gt))
    assert ctx.rca_root_note == "", ctx.rca_root_note
    assert ranked[0].entity == gt["root_cause"], [(c.entity, round(c.score, 2)) for c in ranked]
    if gt["mechanism"] == "load_amplification":  # 理由要講出是流量放大，不是只給一個分數
        assert "放大" in ranked[0].reason


def test_graph_algorithms_still_miss_load_amplification(tmp_path_factory):
    """對照組：同一份資料，拓撲排序與 PageRank 仍然排錯（方向假設相反）。這是新算子存在的理由，留著當回歸基準。"""
    out = _generate(tmp_path_factory, "nov03_cascade", 42)
    truth = json.loads((out / "ground_truth.json").read_text(encoding="utf-8"))
    gt = truth["incidents"][0]
    incident = _incident(gt)
    ctx = PipelineContext(pipeline="rca", now=incident.end)
    ctx.topology = [tuple(e) for e in truth["service_edges"]]  # 劇本真正的呼叫邊（trace 還原出來的也是這 4 條）
    for algo in ("rca.root_cause.topological", "rca.root_cause.pagerank"):
        ranked = OperatorRegistry.create(algo, {}).rank(incident, ctx)
        assert ranked and ranked[0].entity != gt["root_cause"], algo


def test_quiet_control_scenario_never_names_a_suspect(tmp_path_factory):
    """同一套產生器，事故效果全部設 0（延遲偏移 0、錯誤率 0）：有密集 trace、但沒有任何故障。假裝 4 個服務都報了異常，也不准指認。"""
    def null_fault(raw):
        raw["incidents"][0]["propagation"] = [{**p, "latency_shift_ms": 0.0, "trace_error_rate": 0.0}
                                              for p in raw["incidents"][0]["propagation"]]
    out = _generate(tmp_path_factory, "nov03_callee_slowdown", 42, null_fault)
    spans = _spans(out)
    entities = ["api-gw", "cart-api", "ord-svc", "bill-svc"]
    for start_min in range(0, 180, 30):  # 6 個時段，各 60 分鐘
        t0 = datetime(2026, 11, 3, 13, 0, tzinfo=timezone.utc) + timedelta(minutes=start_min)
        incident = Incident([AnomalyEvent(e, "latency", t0 + timedelta(minutes=m), 5.0) for e in entities for m in range(60)])
        ranked, ctx = _rank(spans, incident)
        assert ranked == [] and ctx.rca_root_note.startswith("證據不足"), (t0, [(c.entity, c.score) for c in ranked])


# ── 合成 span：證據不足的各種情況 ──

T = datetime(2026, 11, 3, 14, 0, tzinfo=timezone.utc)


def _traffic(extra_self_ms: dict[str, float], minutes=range(-15, 30), per_minute=4, fanout=None):
    """api-gw -> cart-api -> ord-svc 的呼叫鏈。extra_self_ms：事件期間（t >= 0）各服務自身多花的時間。
    fanout：事件期間 cart-api 每個請求呼叫 ord-svc 幾次（流量放大）。"""
    spans, n = [], 0
    for m in minutes:
        for k in range(per_minute):
            n += 1
            ts = T + timedelta(minutes=m, seconds=k * 10)
            during = m >= 0
            tid = f"t{n}"
            add = (lambda svc: extra_self_ms.get(svc, 0.0) if during else 0.0)
            calls = (fanout or 1) if during else 1
            ord_ms = [20.0 + add("ord-svc")] * calls
            cart_ms = 10.0 + add("cart-api") + sum(ord_ms)
            gw_ms = 5.0 + add("api-gw") + cart_ms
            spans.append(SpanRecord(tid, f"{tid}g", None, "api-gw", ts, gw_ms))
            spans.append(SpanRecord(tid, f"{tid}c", f"{tid}g", "cart-api", ts, cart_ms))
            for i, d in enumerate(ord_ms):
                spans.append(SpanRecord(tid, f"{tid}o{i}", f"{tid}c", "ord-svc", ts, d))
    return spans


def _synthetic_rank(spans, entities=("api-gw", "cart-api", "ord-svc")):
    incident = Incident([AnomalyEvent(e, "latency", T + timedelta(minutes=m), 5.0) for e in entities for m in range(30)])
    ctx = PipelineContext(pipeline="rca", now=T + timedelta(minutes=30))
    ctx.rca_spans = spans
    return OperatorRegistry.create("rca.root_cause.evidence", {}).rank(incident, ctx), ctx


def test_common_external_cause_is_reported_as_insufficient_evidence():
    """每個服務一起多花 50 ms（例如共用的資料源抖動）：拉不開，不指認任何一個。"""
    ranked, ctx = _synthetic_rank(_traffic({"api-gw": 50.0, "cart-api": 50.0, "ord-svc": 50.0}))
    assert ranked == [] and "拉不開" in ctx.rca_root_note


def test_a_single_slow_service_is_named_and_waiting_callers_are_not():
    ranked, ctx = _synthetic_rank(_traffic({"ord-svc": 200.0}))
    assert ranked[0].entity == "ord-svc" and ctx.rca_root_note == ""
    assert {c.entity: round(c.score, 2) for c in ranked}["cart-api"] == 0.0  # 呼叫端在等，自身耗時沒漲


def test_fan_out_amplifier_gets_the_load_induced_slowdown():
    """cart-api 對 ord-svc 的呼叫變 3 倍，ord-svc 因排隊自身多 60 ms、cart-api 自己多 50 ms：根因是放大者 cart-api。"""
    ranked, ctx = _synthetic_rank(_traffic({"cart-api": 50.0, "ord-svc": 60.0}, fanout=3))
    assert ranked[0].entity == "cart-api" and "放大" in ranked[0].reason
    ord_reason = next(c.reason for c in ranked if c.entity == "ord-svc")
    assert "流量被 cart-api 放大" in ord_reason


@pytest.mark.parametrize("spans, expect", [
    ([], "沒有 trace"),  # 沒有 trace、也沒有拓撲可以退回
    (_traffic({"ord-svc": 200.0}, minutes=range(0, 30)), "沒有涵蓋事件開始之前"),
    (_traffic({"ord-svc": 200.0}, per_minute=1, minutes=range(-5, 5)), "少於"),
    (_traffic({}), "沒有任何服務的自身耗時或自身錯誤率明顯上升"),
])
def test_insufficient_evidence_reasons(spans, expect):
    ranked, ctx = _synthetic_rank(spans)
    assert ranked == [] and expect in ctx.rca_root_note


def test_insufficient_evidence_shows_up_in_the_report_section():
    ranked, ctx = _synthetic_rank(_traffic({}))
    ctx.incidents = [Incident([AnomalyEvent("cart-api", "latency", T, 5.0)])]
    ctx.rca_root_algorithm = "rca.root_cause.evidence"
    section = OperatorRegistry.create("rca.section.root_cause_candidates", {})
    incident = ctx.incidents[0]
    assert section.applies(incident, ctx) and section.build(incident, ctx).startswith("證據不足")
