"""事故卡的分項覆核：收斂、RCA、結案、嚴重度四件事各自對不對（engine/requests.ReviewIn）。

為什麼要有：既有的回饋只有「真異常／誤報／漏報」，那是給偵測器調門檻用的。平台現在的主要工作是把外部告警
收斂成事故卡、做 RCA，這兩件事錯在哪（併錯、拆錯、根因指錯、太早結案、嚴重度給錯）以前沒有地方記。
這些紀錄事後補不回來（事故過了就沒人記得），所以先把結構定下來、從第一張卡開始收。

這個模組只做兩件事，都不會改任何設定：
- stored()：送出回饋時檢查細節欄位對不對得上這張卡，並把「平台當時的說法」一起存下來
  （嚴重度、狀態、實體、RCA 結論等級與第一候選）。事後重跑 RCA、事件群升級都會改掉這些，不存就對不回去。
- admin.review_metrics：統計每一項看過幾張、錯幾張、錯在哪，並指出每一種錯對應到哪個設定。
  要不要調、調多少，仍然是人看了報表之後改 YAML（或之後另做提案 -> 回測 -> 核准），這裡不產生提案。

同一張卡被覆核多次：每一項取最後一次有填的（後來的人改判就以後來的為準）。
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from aiops.engine.context import PipelineContext
from aiops.engine.errors import BadRequest, NotFound
from aiops.engine.operator import BaseOperator
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import ReviewIn, ReviewMetricsQuery
from aiops.engine.types import as_utc
from aiops.storage.repository import Feedbacks, Incidents

# 每一項可能的判定（第一個是「對」）
ASPECTS: dict[str, tuple[str, ...]] = {
    "grouping": ("ok", "merged_wrong", "split_wrong"),
    "rca": ("ok", "wrong_root", "missed_root"),
    "resolve": ("ok", "early"),
    "severity": ("ok", "too_high", "too_low"),
}

# 每一種錯指向哪個設定。只是指路，不是自動調整的規則。
POINTS_AT: dict[str, str] = {
    "grouping.merged_wrong": "收斂範圍太寬：收斂節點的時間窗、關係圖裡把這些實體連在一起的邊（RCA 報告的「關係圖依據」段落看得到邊的來源）",
    "grouping.split_wrong": "收斂範圍太窄：時間窗太短，或關係圖缺了連接這些實體的邊（補宣告邊：admin.graph_import）",
    "rca.wrong_root": "根因判斷：依平台當時的結論等級分開看（rca_by_conclusion）——confirmed 錯代表證據判讀有問題，"
                      "topology_inference 錯代表關係圖方向或邊不對",
    "rca.missed_root": "證據門檻或證據來源：平台說無法判定但人找得到，看當時缺的是 trace、日誌，還是門檻擋掉了",
    "resolve.early": "結案太早：alert.absorb_open 與 alert.heartbeat 的 resolve_after_minutes（要蓋得住外部告警的重送間隔）",
    "severity.too_high": "嚴重度太高：alert.notify_payload 的 severity_thresholds 與計分節點",
    "severity.too_low": "嚴重度太低：同上",
}

CAVEAT = ("這是人工覆核的統計，不是自動調整的依據：每一項只算有人看過的卡（reviewed），沒看的不算對也不算錯；"
          "覆核的卡通常不是隨機抽的（出問題的比較會被回報），比例會比實際悲觀。樣本少的時候只看個案，不要拿比例去調設定。")


def stored(review: ReviewIn, incident: dict) -> dict[str, Any]:
    """要寫進 feedback.review 的內容：有填的項目＋平台當時對這張卡的說法。細節對不上這張卡就拒絕。"""
    entities = list(incident["incident"].get("entities", []))
    outside = [e for e in review.unrelated_entities if e not in entities]
    if outside:
        raise BadRequest(f"unrelated_entities 要是這張卡裡的實體，{outside} 不在其中：{entities}")
    if review.unrelated_entities and set(review.unrelated_entities) >= set(entities):
        raise BadRequest("unrelated_entities 不能是這張卡的全部實體：整張卡都不對請用 verdict=false_positive")
    if incident["uid"] in review.same_as:
        raise BadRequest("same_as 要填「其他」事件群，不能是這張卡自己")
    missing = [uid for uid in review.same_as if Incidents.get_by_uid(uid) is None]
    if missing:
        raise NotFound(f"same_as 裡找不到事件群：{missing}")
    return {**review.to_dict(), "platform": {
        "severity": incident["severity"] or "", "status": incident["status"] or "", "entities": entities,
        "rca_conclusion_type": incident["rca_conclusion_type"] or "", "rca_root_entity": incident["rca_root_entity"] or ""}}


def _tally(values: list[str], choices: tuple[str, ...]) -> dict[str, int]:
    return {"reviewed": len(values), **{c: values.count(c) for c in choices}}


@OperatorRegistry.register("admin.review_metrics")
class ReviewMetricsOp(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = ctx.request if isinstance(ctx.request, ReviewMetricsQuery) else ReviewMetricsQuery()
        now = as_utc(ctx.now)
        rows = [fb for fb in Feedbacks.created_between(now - timedelta(days=req.lookback_days), now)
                if fb["incident_uid"] and (req.pipeline is None or fb["pipeline"] == req.pipeline)]
        rows.sort(key=lambda fb: (fb["created_at"], fb["id"]))
        # 每張卡每一項：最後一次有填的判定，連同那一次存下的平台說法
        latest: dict[str, dict[str, tuple[str, dict]]] = {aspect: {} for aspect in ASPECTS}
        verdicts: dict[str, str] = {}
        for fb in rows:
            verdicts[fb["incident_uid"]] = fb["verdict"]
            review = fb["review"] or {}
            for aspect in ASPECTS:
                if review.get(aspect):
                    latest[aspect][fb["incident_uid"]] = (review[aspect], review.get("platform", {}))
        aspects = {aspect: _tally([v for v, _ in latest[aspect].values()], choices) for aspect, choices in ASPECTS.items()}

        by_conclusion: dict[str, list[str]] = {}
        for value, platform in latest["rca"].values():
            by_conclusion.setdefault(platform.get("rca_conclusion_type") or "not_run", []).append(value)
        too_high: dict[str, int] = {}
        for value, platform in latest["severity"].values():
            if value == "too_high":
                level = platform.get("severity") or "unknown"
                too_high[level] = too_high.get(level, 0) + 1
        false_alarms: dict[str, int] = {}
        for uid, verdict in verdicts.items():
            if verdict == "false_positive":
                incident = Incidents.get_by_uid(uid)
                level = (incident["severity"] if incident else "") or "unknown"
                false_alarms[level] = false_alarms.get(level, 0) + 1

        reviewed = {uid for per_aspect in latest.values() for uid in per_aspect}
        ctx.result = {
            "lookback_days": req.lookback_days, "pipeline": req.pipeline,
            "incidents_with_feedback": len(verdicts), "incidents_reviewed": len(reviewed),
            "aspects": aspects,
            # RCA 的對錯依「平台當時給的結論等級」分開：confirmed 該比 topology_inference 準，不準就是等級給得太有把握
            "rca_by_conclusion": {c: _tally(v, ASPECTS["rca"]) for c, v in sorted(by_conclusion.items())},
            # 誤報落在哪個嚴重度（P3 的誤報可以接受，P0／P1 的誤報不行）
            "false_positive_by_severity": dict(sorted(false_alarms.items())),
            "severity_too_high_by_level": dict(sorted(too_high.items())),
            "points_at": POINTS_AT, "caveat": CAVEAT}
        return ctx
