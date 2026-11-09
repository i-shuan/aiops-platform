"""黃金事故防護罩：調參提案回測時的硬性安全卡口。

黃金事故 = 經驗庫（incident_knowledge）裡 verdict 為 true_positive 的事件群（來源：admin.feedback_submit 的
true_positive 回饋）。回測時把提案影響得到的黃金事故一筆一筆現抓、重跑：

- 撈哪些：提案那一層 (管線, 實體) 的所有黃金事故；管線 "*" = 所有管線、實體 "*" = 該管線所有實體。
  某個 (管線, 實體) 已經有比提案更具體的現行設定時，提案核准後也不會套到它身上，略過並註明。
- 怎麼重跑：每一筆用 fetch_target_frame 現抓「時窗 + 前面一整個訓練窗」（訓練窗長度與 step 取該管線的
  ingestion.fetch 設定，跟線上同尺度），訓練只用時窗之前的資料（跟漏報回測同一套，不偷看答案），
  訓練排除窗跟正式偵測同一套（replay.exclusion_windows）。黃金事故時窗不在回測的 time_range 裡也照樣另外抓。
- 怎麼判：現行參數與提案參數各跑一次，數時窗內的異常點。
  * 提案 0 點、現行 > 0 點 → missed_golden：提案會讓一場確認過的真事故消失，safety.passed = false。
  * 兩邊都 0 點 → baseline_missed：現行參數本來就抓不到（例如那場是別的偵測器抓的），不是這個提案造成的，
    不擋。不這樣做的話，為了補抓它而往下調的漏報提案也會被擋死。
  * 判不了（資料源已經沒有那段資料、訓練資料不足、同儕偵測器沒有同儕）→ unchecked，列出原因，不擋。
    資料源連不上（InfrastructureError）不在此列：整個回測失敗，不讓防護罩在看不到資料時默默放行。
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from aiops.control_plane.learning.replay import (
    Series,
    exclusion_windows,
    fetch_settings,
    fetch_span,
    incident_series,
    replay_window,
    series_context,
    workflow_layers,
)
from aiops.engine.context import PipelineContext
from aiops.engine.errors import NoDataError
from aiops.engine.params import ANY, pick_most_specific
from aiops.engine.types import as_utc
from aiops.stages.detectors.base import DetectorOperator, layer_params
from aiops.storage.repository import ActiveConfigs, Incidents, Knowledge


def _series_of(row: dict[str, Any], entity: str) -> list[Series]:
    """黃金事故在這個實體上的序列：寫入經驗庫時存在 context.series；舊資料沒有就回事件群快照找。"""
    stored = [Series(s["entity"], s["source_type"], s["query"]) for s in (row.get("context") or {}).get("series", [])
              if s.get("entity") == entity]
    if stored:
        return stored
    incident = Incidents.get_by_uid(row["incident_uid"])
    return incident_series(incident["incident"], entity) if incident else []


def _governs(proposal: dict[str, Any], pipeline: str, entity: str) -> tuple[str | None, dict[str, Any] | None]:
    """提案核准後會不會真的套到 (pipeline, entity)：把提案當成它那一層的設定，跟現有各層比誰最具體。
    回 (蓋過提案的那一層 "管線/實體"，None = 提案會生效；核准前這裡實際生效的設定，None = 演算法預設)。"""
    rows = ActiveConfigs.candidates(proposal["detector_name"], pipeline, entity)
    layer = (proposal["pipeline"], proposal["entity"])
    others = [r for r in rows if (r["pipeline"], r["entity"]) != layer]
    winner = pick_most_specific([*others, {"pipeline": layer[0], "entity": layer[1], "_proposal": True}],
                                pipeline, entity)
    shadow = None if winner is None or winner.get("_proposal") else f"{winner['pipeline']}/{winner['entity']}"
    return shadow, pick_most_specific(rows, pipeline, entity)


def check(ctx: PipelineContext, proposal: dict[str, Any], cls: type[DetectorOperator], limit: int) -> dict[str, Any]:
    """回傳 backtest 結果裡的 safety 區塊。"""
    rows = Knowledge.golden(proposal["pipeline"], proposal["entity"])
    truncated = max(0, len(rows) - limit)
    rows = rows[-limit:] if limit else rows  # 太多時只驗最近的 limit 筆，並照實標出來
    out: dict[str, Any] = {"golden_total": len(rows) + truncated, "checked": 0, "missed_golden": [],
                           "baseline_missed": [], "caught": [], "unchecked": [], "skipped_oldest": truncated}
    for row in rows:
        pipeline = row["pipeline"] or ANY
        entities = [proposal["entity"]] if proposal["entity"] != ANY else list(row["entities"] or [])
        start, end = as_utc(row["window_start"]), as_utc(row["window_end"])
        base = {"incident_uid": row["incident_uid"], "pipeline": row["pipeline"], "start": start.isoformat(),
                "end": end.isoformat()}
        for entity in entities:
            item = {**base, "entity": entity}
            shadow, current_row = _governs(proposal, pipeline, entity)
            if shadow:
                out["unchecked"].append({**item, "reason": f"這裡已有更具體的現行設定（{shadow}），提案核准後不會套到它"})
                continue
            series = _series_of(row, entity)
            if not series:
                out["unchecked"].append({**item, "reason": "經驗庫與事件群快照都找不到這個實體的資料源與查詢"})
                continue
            yaml_params, overrides, _info = workflow_layers(ctx, proposal["detector_name"], pipeline, entity)
            current = layer_params(cls, (current_row or {}).get("params", {}), yaml_params, overrides)
            proposed = layer_params(cls, proposal["params"], yaml_params, overrides)
            verdict = _replay_incident(ctx, cls, current, proposed, series, pipeline, entity, start, end)
            if isinstance(verdict, str):
                out["unchecked"].append({**item, "reason": verdict})
                continue
            out["checked"] += 1
            item.update(verdict)
            if verdict["proposed_anomalies"] == 0 and verdict["current_anomalies"] > 0:
                out["missed_golden"].append(item)
            elif verdict["proposed_anomalies"] == 0:
                out["baseline_missed"].append(item)
            else:
                out["caught"].append(item)
    out["passed"] = not out["missed_golden"]
    return out


def _replay_incident(ctx: PipelineContext, cls: type[DetectorOperator], current: dict, proposed: dict,
                     series: list[Series], pipeline: str, entity: str, start, end) -> dict[str, Any] | str:
    train_minutes, step = fetch_settings(ctx, pipeline)
    probe_current, probe_proposed = cls(**current), cls(**proposed)
    lags = sorted({*probe_current.required_lags(), *probe_proposed.required_lags()})
    warmup = max(probe_current.warmup_points(), probe_proposed.warmup_points())
    exclusions = exclusion_windows(ctx, pipeline, entity, start)  # 只套在時窗之前的訓練資料上
    totals = {"current_anomalies": 0, "proposed_anomalies": 0, "series": []}
    reasons = []
    for s in series:
        sctx = series_context(ctx, s, pipeline)
        if cls.requires_peers and not (sctx.target.peers or sctx.target.peer_group):
            reasons.append(f"{s.query}：{cls.operator_name} 需要同儕，這條序列沒有同儕設定")
            continue
        fetch_start = start - timedelta(minutes=train_minutes) - timedelta(seconds=step * warmup)
        try:
            df = fetch_span(sctx, fetch_start, end, step, lags)
        except NoDataError as e:
            reasons.append(f"{s.query}：{e}")
            continue
        cur = replay_window(sctx, cls, current, df, start, end, exclusions)
        pro = replay_window(sctx, cls, proposed, df, start, end, exclusions)
        if isinstance(cur, str) or isinstance(pro, str):
            reasons.append(f"{s.query}：{cur if isinstance(cur, str) else pro}")
            continue
        totals["current_anomalies"] += cur.anomalies
        totals["proposed_anomalies"] += pro.anomalies
        totals["series"].append({**s.to_dict(), "train_points": cur.train_points, "window_points": len(cur.scores),
                                 "current_anomalies": cur.anomalies, "proposed_anomalies": pro.anomalies,
                                 "max_score": cur.max_score})
    if not totals["series"]:
        return "；".join(reasons) or "沒有可重跑的序列"
    if reasons:
        totals["unchecked_series"] = reasons
    return totals
