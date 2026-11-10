"""訊息佇列的非同步邊 -> 事實（mq_lag）。延遲、發布時窗由 ingestion.incident_traces 還原好放在 ctx.rca_mq（engine/mq.py）。

剝掉的解讀：stages/rca/mq.is_backlog（延遲過門檻且翻倍才算積壓）是判斷，不搬；只輸出延遲 P95 本身與事件前的 P95。
"""
from __future__ import annotations

from aiops.engine.context import MqEdge
from aiops.engine.rca_types import Fact

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
