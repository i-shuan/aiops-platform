"""訊息佇列的非同步邊 -> 事實（mq_lag）。延遲、發布時窗由 ingestion.incident_traces 還原好放在 ctx.rca_mq（engine/mq.py）。

剝掉的解讀：stages/rca/mq.is_backlog（延遲過門檻且翻倍才算積壓）是判斷，不搬；只輸出延遲 P95 本身與事件前的 P95。

生產者在「發布時窗」的數字（producer_self_time_delta／producer_wait_delta）：消費者 12:20 才出事，訊息可能是 12:00 發的；
其他事實都是拿事件時窗比事件前，生產者當時的異常剛好落在「事件前」那一邊，看起來反而像它變好了。
這裡另外拿 (發布時窗) 比 (發布時窗之前等長的一段)，跟舊判斷層用同一個 producer_windows。只陳述數字，不判斷有沒有異常。
"""
from __future__ import annotations

from aiops.engine.context import MqEdge, SpanRecord
from aiops.engine.rca_types import Fact
from aiops.stages.rca.mq import producer_windows
from aiops.stages.rca.spans import resource_kinds

SOURCE = "stages.rca.facts.mq"


def mq_facts(edges: list[MqEdge], min_messages: int, skips: list[str]) -> list[Fact]:
    out: list[Fact] = []
    for e in sorted(edges, key=lambda e: (e.consumer, e.producer, e.queue, e.pub_start)):
        if e.messages < min_messages:
            skips.append(f"mq_lag:{e.consumer}:佇列 {e.queue or '?'} 只有 {e.messages} 則訊息 < {min_messages}")
            continue
        out.append(Fact("mq_lag", e.consumer, e.lag_ms_p95, "ms", "incident",
                        {"producer": e.producer, "queue": e.queue, "p50_ms": e.lag_ms_p50, "before": e.lag_ms_p95_before,
                         "n": e.messages, "failed": e.failed, "pub_start": e.pub_start.isoformat(),
                         "pub_end": e.pub_end.isoformat(), "linked_by": e.linked_by}, SOURCE))
    return out


def producer_facts(spans: list[SpanRecord], edges: list[MqEdge], min_requests: int, skips: list[str]) -> list[Fact]:
    """每條知道生產者的非同步邊：生產者在發布時窗 vs 之前等長一段的自身耗時、自身錯誤率、各資源等待。
    同一個生產者、同一個發布時窗只算一次（一個生產者發給多個消費者時時窗常常一樣）。"""
    out: list[Fact] = []
    kinds: dict[str, str] | None = None
    seen: set[tuple] = set()
    for e in sorted(edges, key=lambda e: (e.producer, e.pub_start, e.pub_end, e.consumer, e.queue)):
        if not e.producer or e.producer == e.consumer or (e.producer, e.pub_start, e.pub_end) in seen:
            continue
        seen.add((e.producer, e.pub_start, e.pub_end))
        b, d = producer_windows(spans, e)
        nb, nd = (b.requests if b else 0), (d.requests if d else 0)
        if b is None or d is None or min(nb, nd) < min_requests:
            skips.append(f"producer_window:{e.producer}:發布時窗或它之前等長一段的入口 {nb}→{nd} 筆，有一邊 < {min_requests}")
            continue
        kinds = resource_kinds(spans) if kinds is None else kinds
        common = {"consumer": e.consumer, "queue": e.queue, "pub_start": e.pub_start.isoformat(),
                  "pub_end": e.pub_end.isoformat(), "n": nd, "n_before": nb}
        out.append(Fact("producer_self_time_delta", e.producer, d.self_ms_per_request - b.self_ms_per_request, "ms", "publish",
                        {**common, "before": b.self_ms_per_request, "during": d.self_ms_per_request,
                         "app_before": b.app_ms_per_request, "app_during": d.app_ms_per_request,
                         "own_failure_rate_before": b.own_error_rate, "own_failure_rate_during": d.own_error_rate}, SOURCE))
        for res in sorted(set(b.wait_ms_per_request) | set(d.wait_ms_per_request)):
            bw, dw = b.wait_ms_per_request.get(res, 0.0), d.wait_ms_per_request.get(res, 0.0)
            out.append(Fact("producer_wait_delta", e.producer, dw - bw, "ms", "publish",
                            {**common, "target": res, "target_kind": kinds.get(res, ""), "before": bw, "during": dw,
                             "failures_before": int(b.wait_failures.get(res, 0)),
                             "failures_during": int(d.wait_failures.get(res, 0))}, SOURCE))
    return out
