"""現場還原呼叫關係：用 ingestion.incident_traces 撈到的 span，算出事件當下服務之間的呼叫邊，注入 ctx.topology。

接在根因演算法（rca.root_cause.topological）前面，它們就能用「案發當下真的有發生的呼叫」排根因，
而不是只靠事先存好的拓撲（可能過時，或根本沒有）。邊的算法跟批次匯入 trace 時一樣（engine/topology.service_edges）：
子 span 的 parent_id 指到別的服務的 span，就是一次跨服務呼叫。

params:
  mode       merge（預設）：跟既有拓撲（事件快照、trace 匯入推導的、workflow 寫死的）取聯集
             replace：只用這次現撈的（現撈沒有任何邊時不會清空既有拓撲，避免把有用的資訊丟掉）
  min_calls  一條邊至少要被呼叫幾次才採用（預設 1；資料雜時調高可以濾掉零星的呼叫）
"""
from __future__ import annotations

from aiops.engine.context import PipelineContext
from aiops.engine.graph import ORIGIN_INCIDENT_TRACE, EdgeKind, Provenance
from aiops.engine.registry import OperatorRegistry
from aiops.engine.topology import service_edge_last_seen, service_edges
from aiops.engine.types import as_utc
from aiops.engine.operator import BaseOperator
from aiops.stages.rca.base import need_incident

MODES = ("merge", "replace")


@OperatorRegistry.register("rca.build_dynamic_topology")
class BuildDynamicTopology(BaseOperator):
    scope = "batch"

    def __init__(self, mode: str = "merge", min_calls: int = 1):
        if mode not in MODES:
            raise TypeError(f"mode 只能是 {', '.join(MODES)}，不是 {mode!r}")
        if int(min_calls) < 1:
            raise TypeError("min_calls 至少 1")
        self.mode, self.min_calls = mode, int(min_calls)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        need_incident(ctx)
        counts = service_edges(ctx.rca_spans)
        edges = sorted((pair, n) for pair, n in counts.items() if n >= self.min_calls)
        before = ctx.graph.declared(EdgeKind.CALL)
        found = [pair for pair, _ in edges]
        if self.mode == "replace" and found:
            ctx.topology = found
        else:
            ctx.topology = before + [pair for pair in found if pair not in set(before)]
        # 出處：這次事件的 span 裡直接看到的邊，是比「以前沉澱過」更強的依據，蓋掉資料表載入的那一筆
        last = service_edge_last_seen(ctx.rca_spans)
        for (a, b), n in edges:
            ctx.edge_provenance[(EdgeKind.CALL.value, a, b)] = Provenance(
                ORIGIN_INCIDENT_TRACE, f"{n} 次呼叫", as_utc(last[(a, b)]) if (a, b) in last else None)
        added = [pair for pair in found if pair not in set(before)]
        ctx.result["dynamic_topology"] = {
            "mode": self.mode, "spans": len(ctx.rca_spans), "traces": len({s.trace_id for s in ctx.rca_spans}),
            "edges": [{"caller": a, "callee": b, "calls": n} for (a, b), n in edges],
            "added_to_topology": [list(p) for p in added], "topology_size": len(ctx.graph.declared(EdgeKind.CALL)),
        }
        if ctx.rca_spans and not found:
            ctx.log("dynamic_topology:no-cross-service-calls")  # 有撈到 span，但每條 trace 都只在一個服務裡
        ctx.log(f"dynamic_topology:{len(found)}edges:+{len(added)}")
        return ctx
