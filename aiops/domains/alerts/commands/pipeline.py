"""告警管線本體：給 ProcessAlertsCommand（直接收事件）與 ProcessPendingAnomaliesCommand（讀 anomalies）共用。

收斂 -> 計分 -> 抑制 -> 產生通知 -> 推送 -> 記錄。流程固定，零件由 preset 決定。
要不要發、發什麼都在這裡決定好；接收端（Alertmanager）只負責路由。
"""
from aiops.commands.base import CommandError
from aiops.domains.alerts.commands.payload import build_notifications, severity_for
from aiops.domains.alerts.dao import IncidentDAO, NotificationDAO, SilenceDAO
from aiops.notifiers import get_notifier
from aiops.plugins.base import AnomalyEvent, Incident
from aiops.plugins.registry import GrouperRegistry, ScorerRegistry, SuppressorRegistry
from aiops.presets.base import Preset


def check_preset(preset: Preset) -> None:
    missing = [g for g in preset.groupers if not GrouperRegistry.has(g)]
    missing += [s for s in preset.scorers if not ScorerRegistry.has(s)]
    missing += [s for s in preset.suppressors if not SuppressorRegistry.has(s)]
    if missing:
        raise CommandError(f"preset {preset.name} 引用了未註冊的外掛: {missing}")


class AlertPipeline:
    def __init__(self, preset: Preset, context: dict, plugin_params: dict[str, dict]):
        self.preset = preset
        self.context = context
        self.plugin_params = plugin_params

    def _params(self, name: str) -> dict:
        return self.plugin_params.get(name, {})

    def _converge(self, events: list[AnomalyEvent]) -> list[Incident]:
        incidents = [Incident([e], score=e.score) for e in events]
        for name in self.preset.groupers:
            incidents = GrouperRegistry.get(name)(self._params(name)).group(incidents, self.context)
        scorers = [ScorerRegistry.get(name)(self._params(name)) for name in self.preset.scorers]
        for inc in incidents:
            for scorer in scorers:
                inc.score = scorer.score(inc, self.context)
        return sorted(incidents, key=lambda i: i.score, reverse=True)

    def run(self, events: list[AnomalyEvent]) -> list[dict]:
        notifier = get_notifier()  # 設定錯誤先爆，不要算完才發現送不出去
        incidents = self._converge(events)
        recent = NotificationDAO.list_for([inc.fingerprint for inc in incidents])
        ctx = {**self.context, "batch": incidents, "silences": SilenceDAO.list_all(), "recent_notifications": recent}
        suppressors = [SuppressorRegistry.get(name)(self._params(name)) for name in self.preset.suppressors]

        results, outgoing, to_record = [], [], []
        for inc in incidents:
            reason = next((r for s in suppressors if (r := s.suppress(inc, ctx))), None)
            severity = severity_for(inc.score, self.preset.severity_thresholds)
            row = IncidentDAO.create(self.preset.name, inc.to_dict(), severity, reason)
            notes = [] if reason else build_notifications(row["id"], inc, self.preset.name, severity)
            if notes:
                outgoing += notes
                to_record.append((row["id"], inc, severity, len(notes)))
                # 同一批後面的事件群也要看得到「剛剛決定要發」的這筆，避免同批重複
                recent.append({"incident_id": row["id"], "fingerprint": inc.fingerprint, "last_event_ts": inc.end})
            results.append(
                {"id": row["id"], **row["incident"], "severity": severity,
                 "suppressed_by": reason, "notifications": len(notes)}
            )

        notifier.send(outgoing)
        for incident_id, inc, severity, count in to_record:
            NotificationDAO.record(incident_id, inc.fingerprint, severity, inc.end, count)
        return results
