"""整段偵測重跑（節點開了 snr 的管線回測用）：用正式偵測同一個 Pipeline 跑「管線的整段偵測節點」，而不是單一偵測器。

為什麼要整段：開了 snr 的節點門檻由 p_fa 換算，是在基底 execute 裡套的；單一偵測器的回測（replay.replay_window）
用 fit／detect 直接算，繞過了這一步，對這種節點量不準。整段裡任何一個偵測節點越線就是告警（平台沒有表決節點）。

做法：
- 節點：管線 YAML 裡 scope=target、會讀 ctx.raw_df 的節點全部照抄（不含 ingestion.fetch），參數原樣；
  target 用 YAML 裡同一個實體的 target（保留 overrides、detectors 白名單）；YAML 沒有這個實體就用最小的 target。
- 資料：SegmentHooks 在 load_target 時把 raw_df／train_df、訓練排除窗、調參（依算子名稱給到每個節點）放進 ctx，
  不讀資料庫、不寫基準線。訓練只用時窗之前的資料（跟 replay.replay_window 一樣不偷看答案）。
- 結果：時窗內所有 level=anomaly 的事件的分鐘數，就是正式偵測會發告警的分鐘。

回測（admin.proposal_backtest，backtest）：整段偵測在 time_range 的測試段，現行 vs 提案的告警分鐘數與事件數；
safety（golden_check）= 整段重跑每一場黃金事故，提案讓任何一場從「有告警」變成「沒告警」就 rejected，不能核准。
（以前這兩個函式在 snr_feedback.py，跟「誤報 -> 門檻倍率提案」放在一起；後者已於 2026-11-20 移除。）
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from aiops.control_plane.learning.golden import _governs, _series_of
from aiops.control_plane.learning.replay import (
    Series,
    exclusion_windows,
    fetch_settings,
    fetch_span,
    series_context,
    workflow_layers,
)
from aiops.engine.context import PipelineContext, TrainingExclusion
from aiops.engine.errors import BadRequest, NoDataError, NotFound
from aiops.engine.params import ANY, pick_most_specific
from aiops.engine.pipeline import Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import ANOMALY, as_utc
from aiops.interfaces.runner import workflow_path
from aiops.stages.detectors.base import DetectorOperator
from aiops.stages.ingestion.fetch import fetch_target_frame
from aiops.storage.repository import ActiveConfigs, Knowledge

logger = logging.getLogger(__name__)
WARMUP_POINTS = 60  # 時窗前多帶的點（同 it_otel 的 lookback 60 分鐘），讓滾動窗湊滿；不計分
EVENT_GAP_MINUTES = 10


def snr_nodes(pipeline: str, detector: str) -> list[str]:
    """這條管線裡 detector 算子開了 snr 的節點名稱（沒有 = 這個提案不走整段重跑）。管線讀不到回空清單。"""
    if pipeline == ANY:
        return []
    try:
        wf = load_workflow(workflow_path(pipeline))
    except (NotFound, BadRequest) as e:
        # 提案所屬的管線讀不到（改名、刪掉）：當成沒有開 snr 的節點，走單一偵測器的路，並留下紀錄
        logger.warning("snr_nodes：讀不到 workflow %s（%s），當成沒有開 snr 的節點", pipeline, e)
        return []
    return [n.name for n in wf.nodes if n.operator == detector and (n.params or {}).get("snr") is not None]


def segment_detectors(pipeline: str) -> list[str]:
    """整段偵測裡收調參的偵測器算子（去重、照 YAML 順序）。"""
    wf = load_workflow(workflow_path(pipeline))
    out: list[str] = []
    for n in wf.nodes:
        cls = OperatorRegistry.get(n.operator)
        if isinstance(cls, type) and issubclass(cls, DetectorOperator) and cls.accepts_tuning and n.operator not in out:
            out.append(n.operator)
    return out


@dataclass
class SegmentHooks:
    """不碰資料庫的 Hooks：資料、訓練排除窗、調參都由呼叫端給。tuned：算子名稱 -> 核准調參（同一層的提案或現行設定）。"""

    raw: pd.DataFrame
    train: pd.DataFrame
    exclusions: list[TrainingExclusion]
    tuned: dict[str, dict[str, Any]] = field(default_factory=dict)
    # 這一次重跑訓練出來的基準線（節點名稱 -> DetectorState）。每次 Segment.run 都是新的：現行與提案各自訓練，
    # 提案改的若是訓練時就用到的參數（例如 residual_chronic 的 window），回測看到的才是重訓之後的效果
    states: dict[str, Any] = field(default_factory=dict)

    def load_batch(self, ctx: PipelineContext) -> None: ...

    def load_target(self, ctx: PipelineContext, detector_nodes: list[tuple[str, str]]) -> None:
        ctx.raw_df = self.raw.reset_index(drop=True)
        # 有任何要學習的節點還沒有基準線（第一次、或上次棄權沒存）就給訓練段；全都有就不給（不重訓）
        missing = [n for n in ctx.baseline_nodes if n not in self.states]
        ctx.train_df = self.train.reset_index(drop=True) if missing else None
        ctx.detector_states = dict(self.states)
        ctx.training_exclusions = list(self.exclusions)
        for node, operator in detector_nodes:
            if self.tuned.get(operator):
                ctx.detector_params[node] = dict(self.tuned[operator])

    def persist_target(self, ctx: PipelineContext) -> None:
        self.states.update(ctx.new_detector_states)

    def persist_batch(self, ctx: PipelineContext) -> None: ...

    def finalize_batch(self, ctx: PipelineContext) -> None: ...


@dataclass
class SegmentReplay:
    train_points: int
    window_points: int
    alert_minutes: list[pd.Timestamp]

    @property
    def alerts(self) -> int:
        return len(self.alert_minutes)


def _workflow(pipeline: str, entity: str, source_type: str, query: str) -> Workflow:
    wf = load_workflow(workflow_path(pipeline))
    nodes = [n for n in wf.nodes if n.operator != "ingestion.fetch"
             and getattr(OperatorRegistry.get(n.operator), "scope", "target") == "target"]
    base = next((t for t in wf.targets if t.label == entity), None)
    if base is not None:
        target = base.model_copy(update={"source_type": source_type, "query": query, "entity": entity})
    else:
        target = TargetSpec(source_type=source_type, query=query, entity=entity)
    return Workflow(name=pipeline, nodes=nodes, targets=[target])


class Segment:
    """一條序列的一段時窗：[start 之前] 訓練（扣掉排除窗）、對 [start, end] 跑整段偵測。每次 run 都重新訓練。"""

    def __init__(self, pipeline: str, entity: str, source_type: str, query: str, df: pd.DataFrame,
                 start: datetime, end: datetime, exclusions: list[TrainingExclusion]):
        ts = pd.to_datetime(df["ts"], utc=True)
        begin, stop = int((ts < pd.Timestamp(start)).sum()), int((ts <= pd.Timestamp(end)).sum())
        self.problem = "時窗內沒有資料" if stop <= begin else ("時窗前沒有訓練資料" if begin < 2 else None)
        offset = min(WARMUP_POINTS, begin)
        self.train, self.raw = df.iloc[:begin], df.iloc[max(begin - offset, 0):stop]
        self.window_points = max(stop - begin, 0)
        self.exclusions = exclusions
        self.workflow = _workflow(pipeline, entity, source_type, query)
        self.lo, self.hi = pd.Timestamp(as_utc(start)), pd.Timestamp(as_utc(end))
        self.now = as_utc(end) + timedelta(minutes=1)

    def run(self, tuned: dict[str, dict[str, Any]]) -> SegmentReplay | str:
        """回傳時窗內會告警的分鐘（任何偵測節點越線）。判不了回傳原因字串。"""
        if self.problem:
            return self.problem
        hooks = SegmentHooks(self.raw, self.train, self.exclusions, tuned)
        batch = Pipeline(self.workflow, hooks).run(now=self.now, strict=True)
        minutes = sorted({pd.Timestamp(e.ts) for e in batch.anomalies
                          if e.level == ANOMALY and self.lo <= pd.Timestamp(e.ts) <= self.hi})
        return SegmentReplay(len(self.train), self.window_points, minutes)


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


def _segment(ctx: PipelineContext, pipeline: str, entity: str, source_type: str, query: str,
             start, end) -> Segment | str:
    """現抓一場時窗的資料（時窗 + 前面一整個訓練窗，跟線上同尺度），建好可以換調參重跑的整段偵測。"""
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
