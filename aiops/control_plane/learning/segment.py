"""整段偵測重跑（R+SNR 管線的回饋閉環用）：用正式偵測同一個 Pipeline 跑「管線的整段偵測節點」——所有偵測節點、
軟投票（detector.soft_vote）、旁路節點（hard_limit、不投票的漂移票）——而不是單一偵測器。

為什麼要整段：開了 snr 的節點不是單獨成立，告警是「每票 = 分數 / 門檻、加總 >= 2 票等值」或旁路節點單獨成立。
調高某一票的門檻倍率，那張誤報卡會不會消失、黃金事故還會不會被抓到，只有整段一起跑才知道。單一偵測器的回測
（replay.replay_window）用 fit／detect 直接算，也繞過了基底 execute 的「p_fa 換算門檻」，對這種節點量不準。

做法：
- 節點：管線 YAML 裡 scope=target、會讀 ctx.raw_df 的節點全部照抄（不含 ingestion.fetch），參數原樣；
  target 用 YAML 裡同一個實體的 target（保留 overrides、detectors 白名單）；YAML 沒有這個實體就用最小的 target。
- 資料：SegmentHooks 在 load_target 時把 raw_df／train_df、訓練排除窗、調參（依算子名稱給到每個節點）放進 ctx，
  不讀資料庫、不寫基準線。訓練只用時窗之前的資料（跟 replay.replay_window 一樣不偷看答案）。
- 結果：時窗內所有 level=anomaly 的事件（裁決事件＋旁路事件）的分鐘數，就是正式偵測會發告警的分鐘。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from aiops.engine.context import PipelineContext, TrainingExclusion
from aiops.engine.errors import BadRequest, NotFound
from aiops.engine.params import ANY
from aiops.engine.pipeline import Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import ANOMALY, as_utc
from aiops.interfaces.runner import workflow_path
from aiops.stages.detectors.base import DetectorOperator

logger = logging.getLogger(__name__)
WARMUP_POINTS = 60  # 時窗前多帶的點（同 it_otel 的 lookback 60 分鐘），讓滾動窗湊滿；不計分


def snr_nodes(pipeline: str, detector: str) -> list[str]:
    """這條管線裡 detector 算子開了 snr 的節點名稱（沒有 = 這個提案不走整段重跑）。管線讀不到回空清單。"""
    if pipeline == ANY:
        return []
    try:
        wf = load_workflow(workflow_path(pipeline))
    except (NotFound, BadRequest) as e:
        # 提案所屬的管線讀不到（改名、刪掉）：當成沒有 R+SNR 節點，走原本單一偵測器的路，並留下紀錄
        logger.warning("snr_nodes：讀不到 workflow %s（%s），當成沒有開 snr 的節點", pipeline, e)
        return []
    return [n.name for n in wf.nodes if n.operator == detector and (n.params or {}).get("snr") is not None]


def segment_detectors(pipeline: str) -> list[str]:
    """整段偵測裡收調參的偵測器算子（去重、照 YAML 順序）：回饋提案從這裡挑要調哪一個。"""
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
    # 訓練好的基準線（節點名稱 -> DetectorState）：第一次跑完存起來，同一段資料換調參再跑就不重訓。
    # 門檻倍率、p_fa 換算的門檻都在 detect 時才套，跟訓練無關，所以可以共用
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
    """一條序列的一段時窗：[start 之前] 訓練（扣掉排除窗）、對 [start, end] 跑整段偵測。換調參重跑不重訓。"""

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
        self.states: dict[str, Any] = {}

    def run(self, tuned: dict[str, dict[str, Any]]) -> SegmentReplay | str:
        """回傳時窗內會告警的分鐘（裁決事件＋旁路事件）。判不了回傳原因字串。"""
        if self.problem:
            return self.problem
        hooks = SegmentHooks(self.raw, self.train, self.exclusions, tuned, self.states)
        batch = Pipeline(self.workflow, hooks).run(now=self.now, strict=True)
        minutes = sorted({pd.Timestamp(e.ts) for e in batch.anomalies
                          if e.level == ANOMALY and self.lo <= pd.Timestamp(e.ts) <= self.hi})
        return SegmentReplay(len(self.train), self.window_points, minutes)
