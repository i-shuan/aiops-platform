"""學習閉環的下游：調參成效報表。手動動作（admin），只讀自己的狀態。

- admin.tuning_metrics：每個核准過的調參，核准前後各 N 天「同一層」的事件群數、誤報／確認／漏報回饋數。
  事件群歸給哪個調參，靠事件群裡異常的偵測器血緣（detector）＋管線＋實體判斷；沒有血緣的舊事件群不計。
  這是前後對照，不是因果證明（同時間的流量變化、其他調參也會影響），報表會一併附上這個提醒。
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from aiops.engine.context import PipelineContext
from aiops.engine.params import ANY
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import TuningMetricsQuery
from aiops.engine.types import as_utc
from aiops.engine.operator import BaseOperator
from aiops.storage.repository import Feedbacks, Incidents, Proposals



def _in_scope(incident_row: dict, proposal: dict) -> bool:
    if proposal["pipeline"] != ANY and incident_row["pipeline"] != proposal["pipeline"]:
        return False
    events = incident_row["incident"].get("events", [])
    if proposal["entity"] != ANY and proposal["entity"] not in incident_row["incident"].get("entities", []):
        return False
    return any(e.get("detector") == proposal["detector_name"] for e in events)


def _missed_in_scope(fb: dict, proposal: dict) -> bool:
    """沒有事件群的漏報補錄：只看管線與實體（它本來就沒有偵測器血緣）。"""
    if fb["incident_uid"] is not None or fb["verdict"] != "missed":
        return False
    if proposal["pipeline"] != ANY and fb["pipeline"] != proposal["pipeline"]:
        return False
    return proposal["entity"] == ANY or fb["entity"] == proposal["entity"]


@OperatorRegistry.register("admin.tuning_metrics")
class TuningMetricsOp(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = ctx.request if isinstance(ctx.request, TuningMetricsQuery) else TuningMetricsQuery()
        now = as_utc(ctx.now)
        window = timedelta(days=req.window_days)
        approved = sorted(Proposals.approved_between(now - timedelta(days=req.lookback_days), now),
                          key=lambda p: p["approved_at"])
        changes: list[dict[str, Any]] = []
        if approved:
            span_start = as_utc(approved[0]["approved_at"]) - window
            incidents = Incidents.created_between(span_start, now)
            feedback = Feedbacks.created_between(span_start, now)
            fb_by_uid: dict[str, list[dict]] = {}
            for fb in feedback:
                if fb["incident_uid"]:
                    fb_by_uid.setdefault(fb["incident_uid"], []).append(fb)
            for p in approved:
                t = as_utc(p["approved_at"])
                before = self._window(p, incidents, feedback, fb_by_uid, t - window, t)
                after = self._window(p, incidents, feedback, fb_by_uid, t, min(t + window, now))
                changes.append({"proposal_id": p["id"], "detector": p["detector_name"], "pipeline": p["pipeline"],
                                "entity": p["entity"], "params": p["params"], "source": p["source"],
                                "approved_at": t.isoformat(), "before": before, "after": after})
        totals = {side: {k: sum(c[side][k] for c in changes) for k in ("incidents", "false_positive", "true_positive", "missed")}
                  for side in ("before", "after")}
        ctx.result = {"window_days": req.window_days, "lookback_days": req.lookback_days, "changes": changes,
                      "totals": totals,
                      "caveat": "前後對照不代表因果：同一段時間的流量變化、其他調參與部署也會影響這些數字。"
                                "事件群依偵測器血緣歸屬，沒有血緣的舊事件群不計入。"}
        return ctx

    @staticmethod
    def _window(p, incidents, feedback, fb_by_uid, start, end) -> dict[str, Any]:
        days = max((end - start).total_seconds() / 86400, 0.0)
        rows = [i for i in incidents if start <= as_utc(i["created_at"]) < end and _in_scope(i, p)]
        verdicts = [fb["verdict"] for i in rows for fb in fb_by_uid.get(i["uid"], [])]
        missed = sum(1 for fb in feedback if _missed_in_scope(fb, p) and fb["time_start"] is not None
                     and start <= as_utc(fb["time_start"]) < end)
        return {"days": round(days, 2), "incidents": len(rows),
                "incidents_per_day": round(len(rows) / days, 3) if days else None,
                "false_positive": verdicts.count("false_positive"), "true_positive": verdicts.count("true_positive"),
                "missed": verdicts.count("missed") + missed}
