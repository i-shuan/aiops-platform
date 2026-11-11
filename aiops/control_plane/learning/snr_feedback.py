"""R+SNR 節點的回饋閉環：誤報 -> 門檻倍率（threshold_scale）提案 -> 整段回測＋黃金事故防護罩 -> 人核准。

為什麼另外一條路：開了 snr 的節點門檻由 p_fa 換算（snr 是節點參數，調參改不到），而且告警是軟投票（每票 = 分數 / 門檻，
加總 >= 2）或旁路節點單獨成立。原本的分位數反推只調單一偵測器的 threshold、回測也只跑單一偵測器，對這種節點等於無效。

調什麼：偵測器的 threshold_scale（基底的調參鍵，範圍 1～10，預設 1）：節點門檻 = p_fa 換算的門檻（或 threshold）× 倍率。
  - 分層跟其他調參一樣：(偵測器, 管線, 實體)，存在 active_detector_configs，取最具體的一層。
  - 同一條管線裡同一個演算法的節點吃同一份（例如 detect_rchronic 與旁路 detect_drift_solo 都是 residual_chronic）。
  - 下限 1：只能往少誤報調；YAML 或 target overrides 寫了 threshold_scale 就鎖定（layer_params）。
  - soft_vote 的 votes、旁路的 min_run 不是偵測器的調參鍵（前者不是偵測器、後者寫在 YAML 被鎖定），不在這次範圍。

提案怎麼算（scope=entity）：
  1. 誤報集合 = 這條管線這個實體所有被標成 false_positive 的事件群；每場現抓「時窗 + 前面一整個訓練窗」。
  2. 對每個候選偵測器（回饋指定了就只看它，否則整段偵測裡所有收調參的偵測器）：二分搜黃金事故的安全上限
     （最大能到多大的倍率還「每一場都有告警」），再在 [現行, 上限] 裡找「誤報告警分鐘數降到最低」的最小倍率。
  3. 挑「完全消失的誤報場數最多 > 少掉的誤報分鐘最多 > 倍率最小」的偵測器。一個提案只改一個偵測器
     （active_detector_configs 一列一個偵測器）：軟投票下另外兩票可能還湊得到 2 票等值，剩下的分鐘數照實寫在 reason，
     核准後對還在叫的事件群再送一次誤報回饋，會換下一個偵測器。
  pipeline／global 層級沒有單一序列可以重跑：固定規則 ×1.2（上限 10）。
回測（admin.proposal_backtest）：整段偵測在 time_range 的測試段，現行 vs 提案的告警分鐘數與事件數；
safety = 整段重跑每一場黃金事故，提案讓任何一場從「有告警」變成「沒告警」就 rejected，不能核准。
"""
from __future__ import annotations

import math
from datetime import timedelta
from typing import Any

import pandas as pd

from aiops.control_plane.learning.golden import _governs, _series_of
from aiops.control_plane.learning.replay import (
    exclusion_windows,
    fetch_settings,
    fetch_span,
    incident_series,
    series_context,
)
from aiops.control_plane.learning.segment import WARMUP_POINTS, Segment, SegmentReplay, segment_detectors, snr_nodes
from aiops.control_plane.learning.replay import workflow_layers
from aiops.engine.context import PipelineContext
from aiops.engine.errors import BadRequest, NoDataError
from aiops.engine.params import ANY, pick_most_specific
from aiops.engine.types import Incident, as_utc
from aiops.stages.detectors.base import THRESHOLD_SCALE, THRESHOLD_SCALE_RANGE
from aiops.stages.ingestion.fetch import fetch_target_frame
from aiops.storage.repository import ActiveConfigs, Feedbacks, Incidents, Knowledge

_MIN, _MAX = THRESHOLD_SCALE_RANGE[1], THRESHOLD_SCALE_RANGE[2]
SEARCH_STEPS = 12  # 二分搜次數：[1, 10] 切 12 次 ≈ 0.002 的解析度
EVENT_GAP_MINUTES = 10


def has_snr(pipeline: str) -> bool:
    """這條管線的偵測段有沒有開了 snr 的節點（R+SNR 管線）。"""
    return any(snr_nodes(pipeline, det) for det in segment_detectors(pipeline))


def tuned_layers(pipeline: str, entity: str, detectors: list[str],
                 proposal: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    """每個偵測器在 (pipeline, entity) 實際生效的核准調參；給了 proposal 就把它那個偵測器換成提案參數。"""
    out: dict[str, dict[str, Any]] = {}
    for det in detectors:
        row = pick_most_specific(ActiveConfigs.candidates(det, pipeline, entity), pipeline, entity)
        out[det] = dict((row or {}).get("params", {}))
    if proposal is not None:
        out[proposal["detector_name"]] = dict(proposal["params"])
    return out


def _with_scale(tuned: dict[str, dict[str, Any]], detector: str, scale: float) -> dict[str, dict[str, Any]]:
    return {**tuned, detector: {**tuned.get(detector, {}), THRESHOLD_SCALE: scale}}


def _segment(ctx: PipelineContext, pipeline: str, entity: str, source_type: str, query: str,
             start, end) -> Segment | str:
    """現抓一場時窗的資料（時窗 + 前面一整個訓練窗，跟線上同尺度），建好可以換調參重跑的整段偵測。"""
    from aiops.control_plane.learning.replay import Series

    train_minutes, step = fetch_settings(ctx, pipeline)
    sctx = series_context(ctx, Series(entity, source_type, query), pipeline)
    fetch_start = start - timedelta(minutes=train_minutes) - timedelta(seconds=step * WARMUP_POINTS)
    try:
        df = fetch_span(sctx, fetch_start, end, step)
    except NoDataError as e:
        return str(e)
    return Segment(pipeline, entity, source_type, query, df, start, end,
                   exclusion_windows(ctx, pipeline, entity, start))


def _alerts(seg: Segment, tuned) -> int:
    got = seg.run(tuned)
    return got.alerts if isinstance(got, SegmentReplay) else 0


def _bisect(pred, lo: float, hi: float) -> float:
    """pred 在 [lo, hi] 上單調（False…False True…True）：回傳最小的 True 點（近似，往上取）。"""
    for _ in range(SEARCH_STEPS):
        mid = (lo + hi) / 2
        lo, hi = (lo, mid) if pred(mid) else (mid, hi)
    return hi


def _ceil2(x: float) -> float:
    return math.ceil(x * 100 - 1e-9) / 100


def _floor2(x: float) -> float:
    return math.floor(x * 100 + 1e-9) / 100


def _locked(ctx: PipelineContext, detector: str, pipeline: str, entity: str) -> bool:
    yaml_params, overrides, _info = workflow_layers(ctx, detector, pipeline, entity)
    return THRESHOLD_SCALE in yaml_params or THRESHOLD_SCALE in overrides


def _incident_segments(ctx: PipelineContext, rows: list[dict], pipeline: str, entity: str,
                       series_of) -> tuple[list[tuple[str, Segment]], list[dict]]:
    segs, skipped = [], []
    for row in rows:
        uid, (start, end) = row["uid"], row["window"]
        series = series_of(row)
        if not series:
            skipped.append({"incident_uid": uid, "reason": "找不到這個實體的資料源與查詢"})
            continue
        for s in series:
            seg = _segment(ctx, pipeline, entity, s.source_type, s.query, start, end)
            if isinstance(seg, str) or seg.problem:
                skipped.append({"incident_uid": uid, "reason": seg if isinstance(seg, str) else seg.problem})
                continue
            segs.append((uid, seg))
    return segs, skipped


def _false_positive_rows(pipeline: str, entity: str) -> list[dict]:
    out = []
    for uid in sorted(Feedbacks.incident_uids_with_verdict("false_positive")):
        row = Incidents.get_by_uid(uid)
        if row is None or (row["pipeline"] or ANY) != pipeline or entity not in row["incident"].get("entities", []):
            continue
        if (Knowledge.get(uid) or {}).get("verdict") == "true_positive":
            continue  # 後來又被確認是真異常：不拿來調少
        inc = Incident.from_dict(row["incident"], uid=uid)
        out.append({"uid": uid, "window": (as_utc(inc.start), as_utc(inc.end)), "incident": row["incident"]})
    return out


def _golden_rows(pipeline: str, entity: str) -> list[dict]:
    return [{"uid": r["incident_uid"], "window": (as_utc(r["window_start"]), as_utc(r["window_end"])), "knowledge": r}
            for r in Knowledge.golden(pipeline, entity)]


def propose_scale(ctx: PipelineContext, fb: dict, detector: str | None, pipeline: str,
                  entity: str) -> tuple[str, dict[str, Any], str, dict[str, Any]]:
    """回傳 (偵測器, 提案參數, 理由, 細節)。"""
    detectors = segment_detectors(pipeline)
    candidates = [detector] if detector else detectors
    unknown = [d for d in candidates if d not in detectors]
    if unknown:
        raise BadRequest(f"{unknown} 不在 {pipeline} 的偵測段裡；可調：{detectors}")
    tuned = tuned_layers(pipeline, entity, detectors)
    if entity == ANY:
        if not detector:
            raise BadRequest("管線／全域層級的提案請指定 detector（沒有單一序列可以重跑來挑）")
        current = float(tuned.get(detector, {}).get(THRESHOLD_SCALE, _MIN))
        scale = min(_MAX, _ceil2(current * 1.2))
        return detector, {**tuned.get(detector, {}), THRESHOLD_SCALE: scale}, \
            f"{pipeline}/{entity} 層級沒有單一序列可以重跑：門檻倍率固定 ×1.2（{current:g} -> {scale:g}）", \
            {"rule": "template"}

    fps, fp_skipped = _incident_segments(ctx, _false_positive_rows(pipeline, entity), pipeline, entity,
                                         lambda r: incident_series(r["incident"], entity))
    if not fps:
        raise BadRequest(f"{pipeline}/{entity} 沒有能重跑的誤報事件群：{fp_skipped}")
    goldens, gold_skipped = _incident_segments(ctx, _golden_rows(pipeline, entity), pipeline, entity,
                                               lambda r: _series_of(r["knowledge"], entity))
    best = None
    tried = []
    for det in candidates:
        if _locked(ctx, det, pipeline, entity):
            tried.append({"detector": det, "skipped": "threshold_scale 寫在 YAML／overrides，調參改不到"})
            continue
        current = float(tuned.get(det, {}).get(THRESHOLD_SCALE, _MIN))

        def fp_minutes(scale: float, det=det) -> int:
            return sum(_alerts(seg, _with_scale(tuned, det, scale)) for _uid, seg in fps)

        # 黃金事故的安全上限：最大能到多大的倍率還「每一場都有告警」（現行就抓不到的不算，那不是這個提案造成的）
        safe, golden_notes = _MAX, []
        for uid, seg in goldens:
            if _alerts(seg, _with_scale(tuned, det, current)) == 0:
                golden_notes.append({"incident_uid": uid, "note": "現行參數就抓不到，不限制"})
                continue
            if _alerts(seg, _with_scale(tuned, det, _MAX)) > 0:
                continue
            limit = _bisect(lambda s, seg=seg: _alerts(seg, _with_scale(tuned, det, s)) == 0, current, _MAX)
            safe = min(safe, _floor2(limit - 0.01))
        before, at_safe = fp_minutes(current), fp_minutes(safe) if safe > current else fp_minutes(current)
        entry = {"detector": det, "current": current, "golden_safe_max": safe, "golden_notes": golden_notes,
                 "fp_minutes_before": before, "fp_minutes_at_safe_max": at_safe, "false_positives": len(fps)}
        if safe <= current or at_safe >= before:
            entry["scale"] = None
            entry["note"] = "在黃金事故的安全上限內調這個偵測器，誤報分鐘數不會變少"
            tried.append(entry)
            continue
        # 達到同樣效果的最小倍率（誤報分鐘數對倍率單調不增）：不多調
        scale = _ceil2(_bisect(lambda s, det=det: fp_minutes(s) <= at_safe, current, safe))
        scale = min(scale, safe)
        after = fp_minutes(scale)
        silenced = sum(1 for _uid, seg in fps if _alerts(seg, _with_scale(tuned, det, scale)) == 0)
        entry.update(scale=scale, fp_minutes_after=after, silenced=silenced)
        tried.append(entry)
        rank = (silenced, before - after, -scale)
        if best is None or rank > best["_rank"]:
            best = {**entry, "_rank": rank}
    details = {"rule": "segment_bisect", "candidates": tried, "false_positive_skipped": fp_skipped,
               "golden_skipped": gold_skipped, "golden_checked": len(goldens)}
    if best is None:
        raise BadRequest(f"沒有任何偵測器能在不漏抓黃金事故的前提下讓這些誤報變少（詳情：{tried}）")
    det, scale = best["detector"], best["scale"]
    params = {**tuned.get(det, {}), THRESHOLD_SCALE: scale}
    reason = (f"整段重跑 {best['false_positives']} 場誤報事件群：{det} 的門檻倍率 {best['current']:g} -> {scale:g}，"
              f"誤報告警 {best['fp_minutes_before']} -> {best['fp_minutes_after']} 分鐘、{best['silenced']} 場完全不再告警；"
              f"黃金事故（{len(goldens)} 場）的安全上限 {best['golden_safe_max']:g}")
    if best["fp_minutes_after"]:
        reason += ("；剩下的告警這個偵測器單獨消不掉（軟投票還有別的兩票湊得到 2 票等值、或要更大的倍率會漏抓黃金事故），"
                   "核准後對還在叫的事件群再送一次誤報回饋，會換下一個偵測器")
    return det, params, reason, details


# ── 回測與黃金事故防護罩（整段重跑）──


def _events(minutes: list[pd.Timestamp]) -> int:
    if not minutes:
        return 0
    gaps = sum(1 for a, b in zip(minutes, minutes[1:]) if (b - a) > pd.Timedelta(minutes=EVENT_GAP_MINUTES))
    return 1 + gaps


def backtest(ctx: PipelineContext, row: dict[str, Any], req) -> dict[str, Any]:
    """time_range 前 train_ratio 訓練、後面測試，整段偵測在測試段的告警分鐘數與事件數（現行 vs 提案）。"""
    pipeline, detector = row["pipeline"], row["detector_name"]
    entity = req.entity or (row["entity"] if row["entity"] != ANY else None)
    if entity is None:
        raise BadRequest("整段回測需要 entity（提案不是實體層級時請在回測請求裡指定）")
    detectors = segment_detectors(pipeline)
    tr = req.time_range
    df = fetch_target_frame(ctx, tr.start, tr.end, tr.step_seconds, []).reset_index(drop=True)
    split = int(len(df) * req.train_ratio)
    if split < 1 or split >= len(df):
        raise BadRequest(f"回測資料只有 {len(df)} 點，切不出訓練段與測試段，請拉長時間範圍")
    start, end = as_utc(df["ts"].iloc[split].to_pydatetime()), as_utc(df["ts"].iloc[-1].to_pydatetime())
    seg = Segment(pipeline, entity, req.metric.source_type, req.metric.query, df, start, end,
                  exclusion_windows(ctx, pipeline, entity, start))
    current = seg.run(tuned_layers(pipeline, entity, detectors))
    proposed = seg.run(tuned_layers(pipeline, entity, detectors, row))
    if isinstance(current, str) or isinstance(proposed, str):
        raise BadRequest(f"整段回測跑不了：{current if isinstance(current, str) else proposed}")
    _yaml, _overrides, layers = workflow_layers(ctx, detector, pipeline, entity)
    layers["locked"] = sorted(k for k in row["params"] if k in _yaml or k in _overrides)
    days = max((end - start).total_seconds() / 86400, 1e-9)
    return {"mode": "segment", "metric": req.metric.model_dump(), "entity": entity, "points": len(df),
            "train_points": split, "test_points": len(df) - split, "split_at": start.isoformat(),
            "test_days": round(days, 3), "workflow": layers, "proposed_params": row["params"],
            "current_anomalies": current.alerts, "proposed_anomalies": proposed.alerts,
            "current_events": _events(current.alert_minutes), "proposed_events": _events(proposed.alert_minutes),
            "current_minutes_per_week": round(current.alerts / days * 7, 1),
            "proposed_minutes_per_week": round(proposed.alerts / days * 7, 1),
            "safety": golden_check(ctx, row)}


def golden_check(ctx: PipelineContext, proposal: dict[str, Any], limit: int = 50) -> dict[str, Any]:
    """黃金事故防護罩（整段重跑版）：規則同 golden.check——提案讓任何一場從有告警變成沒告警就不能核准。"""
    rows = Knowledge.golden(proposal["pipeline"], proposal["entity"])
    truncated = max(0, len(rows) - limit)
    rows = rows[-limit:]
    out: dict[str, Any] = {"mode": "segment", "golden_total": len(rows) + truncated, "checked": 0, "missed_golden": [],
                           "baseline_missed": [], "caught": [], "unchecked": [], "skipped_oldest": truncated}
    for row in rows:
        pipeline = row["pipeline"] or ANY
        entities = [proposal["entity"]] if proposal["entity"] != ANY else list(row["entities"] or [])
        start, end = as_utc(row["window_start"]), as_utc(row["window_end"])
        base = {"incident_uid": row["incident_uid"], "pipeline": row["pipeline"], "start": start.isoformat(),
                "end": end.isoformat()}
        for entity in entities:
            item = {**base, "entity": entity}
            shadow, _current_row = _governs(proposal, pipeline, entity)
            if shadow:
                out["unchecked"].append({**item, "reason": f"這裡已有更具體的現行設定（{shadow}），提案核准後不會套到它"})
                continue
            series = _series_of(row, entity)
            if not series:
                out["unchecked"].append({**item, "reason": "經驗庫與事件群快照都找不到這個實體的資料源與查詢"})
                continue
            detectors = segment_detectors(pipeline)
            cur_total = pro_total = 0
            reasons = []
            for s in series:
                seg = _segment(ctx, pipeline, entity, s.source_type, s.query, start, end)
                if isinstance(seg, str) or seg.problem:
                    reasons.append(f"{s.query}：{seg if isinstance(seg, str) else seg.problem}")
                    continue
                cur_total += _alerts(seg, tuned_layers(pipeline, entity, detectors))
                pro_total += _alerts(seg, tuned_layers(pipeline, entity, detectors, proposal))
            if reasons and not (cur_total or pro_total) and len(reasons) == len(series):
                out["unchecked"].append({**item, "reason": "；".join(reasons)})
                continue
            out["checked"] += 1
            item.update(current_anomalies=cur_total, proposed_anomalies=pro_total)
            if pro_total == 0 and cur_total > 0:
                out["missed_golden"].append(item)
            elif pro_total == 0:
                out["baseline_missed"].append(item)
            else:
                out["caught"].append(item)
    out["passed"] = not out["missed_golden"]
    return out
