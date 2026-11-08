from aiops.plugins.base import BaseSuppressor, Incident, as_utc
from aiops.plugins.registry import SuppressorRegistry


@SuppressorRegistry.register
class DedupSuppressor(BaseSuppressor):
    """重複通知抑制：同一組實體在冷卻時間內已通知過就不重發。

    context["recent_notifications"]：[{"incident_id", "fingerprint", "last_event_ts"}]，時間已是 datetime。
    以事件時間比較（不看系統時鐘），重跑同一批資料也不會重發。params: cooldown_minutes（預設 30）。
    """

    name = "dedup"

    def suppress(self, incident: Incident, context: dict) -> str | None:
        cooldown = float(self.params.get("cooldown_minutes", 30)) * 60
        for sent in context.get("recent_notifications", []):
            if sent["fingerprint"] != incident.fingerprint:
                continue
            if (as_utc(incident.start) - as_utc(sent["last_event_ts"])).total_seconds() <= cooldown:
                return f"dedup：{cooldown / 60:g} 分鐘內已通知過（incident {sent['incident_id']}）"
        return None
