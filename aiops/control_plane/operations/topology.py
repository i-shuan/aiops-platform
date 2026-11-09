"""服務拓撲排程沉澱：定期從 trace 來源撈 span、算出服務呼叫邊，累加進 service_edges，並移除太久沒出現的邊。

偵測時 ctx.topology 預設就讀 service_edges（storage/hooks.py），alert.topology_chain 沿邊收斂。以前邊只靠批次匯入
（admin.merge_traces），沒人呼叫就永遠是空的或過時的；這個動作讓排程（run-once -w admin_topology_refresh）自己補。
邊的算法跟批次匯入、RCA 現場還原同一份（engine/topology.service_edges）。
"""
from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

from aiops.engine.context import PipelineContext
from aiops.engine.errors import BadRequest
from aiops.engine.operator import BaseOperator
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import TopologyRefresh
from aiops.engine.topology import service_edges
from aiops.engine.types import as_utc
from aiops.sources import get_datasource_spec, is_configured, source_types
from aiops.storage.repository import ServiceEdges


@OperatorRegistry.register("admin.topology_refresh")
class TopologyRefreshOp(BaseOperator):
    """撈 [end − lookback_minutes, end] 的 span -> (caller, callee, 次數) -> ServiceEdges.add_calls（累加、last_seen = 現在）
    -> ServiceEdges.prune（last_seen 早於 現在 − prune_days 的邊移除；NULL 不刪）。

    - 沒給請求（排程 run-once）就用 TopologyRefresh 的預設值。
    - 資料源沒設定：略過並在結果說明，不丟錯（還沒接 trace 的環境不該讓排程一直紅）；也不 prune（沒看到東西不代表邊消失了）。
    - 連不上：資料源丟 InfrastructureError，整輪失敗（排程動作，要讓人知道拓撲停止更新了）。
    - 超過 max_spans：資料源以 trace 為單位抽樣，truncated 照實回報（次數是樣本內的次數，不是全量）。
    - last_seen 用牆鐘（ctx.now），不是 trace 的時間：回放歷史窗（給 end）看到的邊也算「剛確認過還在」。
    """

    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = ctx.request if ctx.request is not None else TopologyRefresh()
        if not isinstance(req, TopologyRefresh):
            raise BadRequest("admin.topology_refresh 需要 TopologyRefresh 請求")
        if req.source_type not in source_types():
            raise BadRequest(f"未知的資料源: {req.source_type}（可用：{', '.join(source_types())}）")
        end = as_utc(req.end) if req.end else ctx.now
        start = end - timedelta(minutes=req.lookback_minutes)
        window = {"start": start.isoformat(), "end": end.isoformat()}
        if not is_configured(req.source_type):
            ctx.result = {"source_type": req.source_type, "skipped": f"資料源 {req.source_type} 沒有設定，略過（不更新、不移除邊）",
                          "spans": 0, "truncated": False, "edges_seen": 0, "edges_added": 0, "edges_updated": 0,
                          "edges_pruned": 0, "edges": [], "window": window}
            return ctx
        spans, truncated = get_datasource_spec(req.source_type).fetch_spans(start, end, None, req.max_spans)
        edges = service_edges(SimpleNamespace(span_id=s.get("span_id"), parent_id=s.get("parent_id"), service=s["service"])
                              for s in spans)
        written = ServiceEdges.add_calls(dict(edges), seen_at=ctx.now)
        pruned = ServiceEdges.prune(ctx.now - timedelta(days=req.prune_days))
        ctx.result = {
            "source_type": req.source_type, "spans": len(spans), "truncated": truncated,
            "edges_seen": len(edges), "edges_added": written["added"], "edges_updated": written["updated"],
            "edges_pruned": pruned,
            "edges": [{"caller": a, "callee": b, "calls": n} for (a, b), n in sorted(edges.items())],
            "window": window,
        }
        return ctx
