"""告警抑制。被抑制的事件群直接寫上 incident.suppressed_by（原因），第一個給出原因的算子生效，
所以 YAML 裡的順序就是優先順序（靜音／排除時段通常排第一）。輸入全部來自 ctx（靜音、冷卻紀錄、拓撲）。
"""
from __future__ import annotations

from datetime import timedelta

from aiops.engine.context import PipelineContext, Silence
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident, as_utc, gap_seconds
from aiops.stages._common import ensure_incidents
from aiops.engine.operator import BaseOperator


def _covers(silence: Silence, event: AnomalyEvent) -> bool:
    if silence.entity and silence.entity != event.entity:
        return False
    if silence.dept and silence.dept != event.attrs.get("dept"):
        return False
    return as_utc(silence.start) <= as_utc(event.ts) <= as_utc(silence.end)


@OperatorRegistry.register("alert.suppress_window")
class SilenceWindowSuppressor(BaseOperator):
    """靜音、維護窗口、人工排除時段。entity／dept 為空代表不限。**每一筆事件**都落在某個符合範圍的時段內才抑制。"""

    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        for inc in ensure_incidents(ctx):
            if inc.suppressed_by:
                continue
            hits: list[Silence] = []
            for event in inc.events:
                match = next((s for s in ctx.silences if _covers(s, event)), None)
                if match is None:
                    hits = []
                    break
                hits.append(match)
            if hits:
                inc.suppressed_by = "silence_window：" + ", ".join(sorted({f"{s.kind}#{s.id}" for s in hits}))
        return ctx


@OperatorRegistry.register("alert.suppress_same_root_cause")
class SameRootCauseSuppressor(BaseOperator):
    """同根因抑制：上游只是被下游拖累的症狀，下游已在同批告警就不另發。

    本群每個實體都直接呼叫了另一群的某個實體，且另一群分數較高（同分取批次中較前者）、時間相近，就抑制本群。
    params: window_minutes。
    """

    scope = "batch"

    def __init__(self, window_minutes: float = 30):
        self.window = float(window_minutes) * 60

    def _reason(self, inc: Incident, index: int, batch: list[Incident], edges: set[tuple[str, str]]) -> str | None:
        for i, other in enumerate(batch):
            if other is inc or gap_seconds(inc, other) > self.window:
                continue
            if other.score < inc.score or (other.score == inc.score and i > index):
                continue
            downstream = set(other.entities)
            if all(any((x, y) in edges for y in downstream) for x in inc.entities):
                return f"same_root_cause：下游 {', '.join(other.entities)} 已在同批告警"
        return None

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        edges = {(caller, callee) for caller, callee in ctx.topology if caller != callee}
        if not edges:
            return ctx
        batch = ensure_incidents(ctx)
        reasons = [self._reason(inc, i, batch, edges) for i, inc in enumerate(batch)]
        for inc, reason in zip(batch, reasons):  # 先全部判斷、再一起寫，結果不受迴圈順序影響
            if reason and not inc.suppressed_by:
                inc.suppressed_by = reason
        return ctx


@OperatorRegistry.register("alert.suppress_dedup")
class DedupSuppressor(BaseOperator):
    """重複通知抑制：同一組實體在冷卻時間內已通知過就不重發。以事件時間比較（不看系統時鐘），重跑同一批資料也不會重發。
    params: cooldown_minutes。
    """

    scope = "batch"

    def __init__(self, cooldown_minutes: float = 30):
        self.cooldown = float(cooldown_minutes) * 60

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        for inc in ensure_incidents(ctx):
            if inc.suppressed_by:
                continue
            for sent in ctx.recent_notifications:
                if sent.fingerprint != inc.fingerprint:
                    continue
                if (as_utc(inc.start) - as_utc(sent.last_event_ts)).total_seconds() <= self.cooldown:
                    inc.suppressed_by = f"dedup：{self.cooldown / 60:g} 分鐘內已通知過"
                    break
        return ctx
