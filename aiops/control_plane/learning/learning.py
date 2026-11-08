"""學習閉環的下游：調參成效報表、規則升級器。都是手動動作（admin），只讀寫自己的狀態。

- admin.tuning_metrics：每個核准過的調參，核准前後各 N 天「同一層」的事件群數、誤報／確認／漏報回饋數。
  事件群歸給哪個調參，靠事件群裡異常的偵測器血緣（detector）＋管線＋實體判斷；沒有血緣的舊事件群不計。
  這是前後對照，不是因果證明（同時間的流量變化、其他調參也會影響），報表會一併附上這個提醒。
- admin.rule_promote：同一層被調參很多次，代表自動門檻一直調不穩；改用人訂的靜態上下限可能比較實在。
  用那一層目前的基準線與門檻換算出上下限，產生一個 detector.rules 提案（仍要回測、核准才生效）。
  要讓核准後的規則真的被用到，workflow 要有 detector.rules 節點（params: {require_rule: false}，沒規則的序列就不判）。
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from aiops.engine.context import PipelineContext
from aiops.engine.params import ANY
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import RulePromote, TuningMetricsQuery
from aiops.engine.types import as_utc
from aiops.control_plane.learning.tuning import _create_proposal, _effective_row
from aiops.engine.operator import BaseOperator
from aiops.storage.repository import DetectorModels, Feedbacks, Incidents, Proposals

RULES_DETECTOR = "detector.rules"
_MAD_TO_STD = 1 / 0.6745


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


def _rule_from_state(detector: str, state: dict, threshold: float) -> tuple[float, float] | None:
    """依偵測器的基準線與門檻換算出等價的 (下限, 上限)。不支援的偵測器回傳 None。"""
    if detector == "detector.zscore":
        center, scale = float(state["mean"]), float(state["std"])
    elif detector == "detector.mad":
        center, scale = float(state["median"]), float(state["mad"]) * _MAD_TO_STD
    else:
        return None
    return round(center - threshold * scale, 6), round(center + threshold * scale, 6)


@OperatorRegistry.register("admin.rule_promote")
class RulePromoteOp(BaseOperator):
    """params（給排程直接跑時用；API 請求帶的值優先）：min_approvals、window_days、consecutive。"""

    scope = "batch"
    SUPPORTED = ("detector.zscore", "detector.mad")

    def __init__(self, min_approvals: int = 3, window_days: int = 30, consecutive: int = 3):
        self.defaults = RulePromote(min_approvals=min_approvals, window_days=window_days, consecutive=consecutive)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = ctx.request if isinstance(ctx.request, RulePromote) else self.defaults
        now = as_utc(ctx.now)
        approved = Proposals.approved_between(now - timedelta(days=req.window_days), now)
        counts: dict[tuple[str, str, str], int] = {}
        for p in approved:
            if p["detector_name"] != RULES_DETECTOR:
                key = (p["detector_name"], p["pipeline"], p["entity"])
                counts[key] = counts.get(key, 0) + 1
        created, skipped = [], []
        for (detector, pipeline, entity), n in sorted(counts.items()):
            if n < req.min_approvals:
                continue
            scope = {"detector": detector, "pipeline": pipeline, "entity": entity, "approvals": n}
            reason = self._blocker(detector, pipeline, entity)
            if reason:
                skipped.append({**scope, "reason": reason})
                continue
            models = DetectorModels.for_scope(pipeline, entity, detector)
            latest = max(models, key=lambda m: m["trained_at"])
            threshold = float((_effective_row(detector, pipeline, entity) or {}).get("params", {}).get("threshold", 3.0))
            bounds = _rule_from_state(detector, latest["state"], threshold)
            lower, upper = bounds  # _blocker 已確認支援
            proposal = _create_proposal(
                "auto", RULES_DETECTOR, {"lower": lower, "upper": upper, "consecutive": req.consecutive},
                f"{window_text(req.window_days)}內核准調參 {n} 次（{detector}），改用靜態上下限："
                f"依基準線（{latest['metric']}）與門檻 {threshold:g} 換算", req.proposer, pipeline, entity)
            created.append({**scope, "proposal": proposal})
        ctx.result = {"created": created, "skipped": skipped,
                      "note": "規則提案仍要回測、核准；workflow 要有 detector.rules 節點（require_rule: false）才會用到"}
        return ctx

    def _blocker(self, detector: str, pipeline: str, entity: str) -> str | None:
        if pipeline == ANY or entity == ANY:
            return "只有指定到「管線 × 實體」的調參能轉成規則（範圍太大的上下限沒有意義）"
        if detector not in self.SUPPORTED:
            return f"{detector} 不支援換算成上下限（目前支援：{', '.join(self.SUPPORTED)}）"
        if Proposals.open_for(RULES_DETECTOR, pipeline, entity):
            return "這一層已經有待處理的規則提案"
        models = DetectorModels.for_scope(pipeline, entity, detector)
        if not models:
            return "這一層還沒有基準線，無法換算上下限"
        metrics = {m["metric"] for m in models}
        if len(metrics) > 1:
            return f"這個實體有 {len(metrics)} 條指標的基準線，規則的範圍沒有指標維度、無法決定用哪一條：{sorted(metrics)}"
        return None


def window_text(days: int) -> str:
    return f"{days} 天"
