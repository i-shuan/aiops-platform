from aiops.plugins.base import BaseSuppressor, Incident, gap_seconds
from aiops.plugins.registry import SuppressorRegistry


@SuppressorRegistry.register
class SameRootCauseSuppressor(BaseSuppressor):
    """同根因抑制：上游只是被下游拖累的症狀，下游已在同批告警就不另發。

    context["topology"]：有方向的邊 [caller, callee]。
    context["batch"]：同一批的所有 Incident（由 Command 提供）。
    本群每個實體都直接呼叫了另一群的某個實體，且另一群分數較高（同分取批次中較前者）、
    時間相近，就抑制本群。params: window_minutes（預設 30）。
    """

    name = "same_root_cause"

    def suppress(self, incident: Incident, context: dict) -> str | None:
        edges = {(e[0], e[1]) for e in context.get("topology", []) if len(e) == 2 and e[0] != e[1]}
        batch = context.get("batch", [])
        me = next((i for i, other in enumerate(batch) if other is incident), None)
        if not edges or me is None:
            return None
        window = float(self.params.get("window_minutes", 30)) * 60
        for i, other in enumerate(batch):
            if other is incident or gap_seconds(incident, other) > window:
                continue
            if other.score < incident.score or (other.score == incident.score and i > me):
                continue
            downstream = set(other.entities)
            if all(any((x, y) in edges for y in downstream) for x in incident.entities):
                return f"same_root_cause：下游 {', '.join(other.entities)} 已在同批告警"
        return None
