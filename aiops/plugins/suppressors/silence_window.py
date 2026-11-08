from aiops.plugins.base import AnomalyEvent, BaseSuppressor, Incident, as_utc
from aiops.plugins.registry import SuppressorRegistry


def _covers(silence: dict, event: AnomalyEvent) -> bool:
    if silence.get("entity") and silence["entity"] != event.entity:
        return False
    if silence.get("dept") and silence["dept"] != event.attrs.get("dept"):
        return False
    return as_utc(silence["start"]) <= as_utc(event.ts) <= as_utc(silence["end"])


@SuppressorRegistry.register
class SilenceWindowSuppressor(BaseSuppressor):
    """靜音、維護窗口、人工排除時段。

    context["silences"]：[{"id", "kind", "start", "end", "entity", "dept"}]，時間已是 datetime。
    entity／dept 為空代表不限。**每一筆事件**都落在某個符合範圍的時段內才抑制；只蓋到一部分仍照發。
    """

    name = "silence_window"

    def suppress(self, incident: Incident, context: dict) -> str | None:
        silences = context.get("silences", [])
        hits = []
        for event in incident.events:
            match = next((s for s in silences if _covers(s, event)), None)
            if match is None:
                return None
            hits.append(match)
        return "silence_window：" + ", ".join(sorted({f"{s['kind']}#{s['id']}" for s in hits}))
