"""多偵測器裁決：把前面幾個偵測節點的結果依策略合成一個判斷。

不加這個算子時，多個偵測器的結果是聯集（任何一個抓到就算）。要更嚴格（降低誤報）就接在偵測節點後面、new_only 前面：

  - {name: vote, operator: detector.ensemble, params: {strategy: majority}}

策略：
  union         任一節點抓到就算（跟不加一樣，但會合成一筆、記下投票者）
  intersection  所有參與節點都抓到才算
  majority      超過一半的參與節點抓到才算
  at_least      至少 min_votes 個節點抓到才算

「同一筆」= 同實體、同指標、同時間點（同一條序列跑多個偵測器，時間點本來就對齊）。
通過的點產生新的裁決事件（detector=detector.ensemble、voted_by=投票節點，分數取各節點最高分），
原本各節點的事件預設拿掉（keep_members=true 則保留）。不是參與者的節點產生的事件原樣保留。
參與者預設是這條序列前面跑過的所有偵測節點（ctx.detector_runs），也可以用 nodes 指定。
"""
from __future__ import annotations

from aiops.engine.context import PipelineContext
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent
from aiops.engine.operator import BaseOperator

STRATEGIES = ("union", "intersection", "majority", "at_least")


@OperatorRegistry.register("detector.ensemble")
class EnsembleOperator(BaseOperator):
    scope = "target"

    def __init__(self, strategy: str = "majority", nodes: list[str] | None = None, min_votes: int | None = None,
                 keep_members: bool = False):
        if strategy not in STRATEGIES:
            raise TypeError(f"strategy 只能是 {', '.join(STRATEGIES)}，不是 {strategy!r}")
        if strategy == "at_least" and (min_votes is None or int(min_votes) < 1):
            raise TypeError("strategy=at_least 必須給 min_votes（至少 1）")
        self.strategy = strategy
        self.nodes = list(nodes) if nodes else None
        self.min_votes = int(min_votes) if min_votes is not None else None
        self.keep_members = bool(keep_members)

    def _needed(self, voters: int) -> int:
        return {"union": 1, "intersection": voters, "majority": voters // 2 + 1,
                "at_least": self.min_votes or 1}[self.strategy]

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        participants = self.nodes or [n for n in ctx.detector_runs if n != self.node_name]
        missing = [n for n in participants if n not in ctx.detector_runs]
        if missing:
            ctx.log(f"ensemble:{self.node_name}:not-run:{','.join(missing)}")  # 沒跑到的節點視為沒投票
        if not participants:
            return ctx
        members = set(participants)
        votes: dict[tuple, list[AnomalyEvent]] = {}
        others: list[AnomalyEvent] = []
        for event in ctx.anomalies:
            if event.detector_node in members:
                votes.setdefault((event.entity, event.metric, event.ts), []).append(event)
            else:
                others.append(event)

        needed = self._needed(len(participants))
        decided: list[AnomalyEvent] = []
        for (entity, metric, ts), events in sorted(votes.items(), key=lambda kv: kv[0][2]):
            voters = tuple(sorted({e.detector_node for e in events}))
            if len(voters) < needed:
                continue
            top = max(events, key=lambda e: e.score)
            decided.append(AnomalyEvent(entity, metric, ts, top.score, dict(top.attrs), top.value,
                                        self.operator_name, self.node_name, top.source_type, voters))
        kept = [e for es in votes.values() for e in es] if self.keep_members else []
        ctx.log(f"ensemble:{self.node_name}:{self.strategy}:voters={len(participants)}:need={needed}:"
                f"{len(votes)}->{len(decided)}")
        ctx.anomalies = others + kept + decided
        return ctx
