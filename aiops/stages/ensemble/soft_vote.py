"""軟投票：每一票換算成「門檻的幾倍」，加總達到 votes 票等值（預設 2）就成立（以前的布林「三票過二」detector.ensemble 已移除）。

  - {name: vote, operator: detector.soft_vote, params: {votes: 2, bypass: [detect_drift_solo]}}

每個參與節點在每個點的權重 w = snr(t) / threshold（逐點 SNR 證據 / 由 p_fa 換算的門檻，都來自 ctx.detector_evidence），
Σw >= votes 就產生一筆裁決事件。所以：
  - 兩票各剛好在門檻上（1 + 1）成立，跟布林多數決一樣；
  - 一票遠遠超過門檻（例如 2.3 倍）單獨就成立——強證據不必等別票；
  - 三票都在門檻的七成（0.7 × 3 = 2.1）也成立——布林多數決會一票都不算。
參與者 = 這條序列真的評估過資料的偵測節點（ctx.detector_runs）扣掉 bypass；權重只看證據，所以參與節點都必須開 snr
（節點參數 snr: {p_fa: ...}），載入期檢查。滾動窗還沒湊滿的點分數是 0，權重 0（等於沒投）。
bypass（旁路漂移票這類單獨成立的節點）：不進權重、事件原樣留下。不投票的節點（votes = False）沒列進 bypass 就報錯。
沒有 warning 級：權重加總只分成立／不成立。
"""
from __future__ import annotations

import math

import pandas as pd

from aiops.engine.context import PipelineContext
from aiops.engine.operator import BaseOperator, NodeInfo
from aiops.engine.params import positive
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import ANOMALY, AnomalyEvent, as_utc


@OperatorRegistry.register("detector.soft_vote")
class SoftVoteOperator(BaseOperator):
    scope = "target"

    def __init__(self, votes: float = 2.0, bypass: list[str] | None = None, keep_members: bool = False):
        positive("votes", votes)
        self.votes = float(votes)
        if bypass is not None and (isinstance(bypass, str) or not isinstance(bypass, (list, tuple))):
            raise TypeError(f"bypass 必須是節點名稱的清單，不是 {bypass!r}")
        self.bypass = frozenset(str(n) for n in bypass or ())
        self.keep_members = bool(keep_members)

    def check_workflow(self, nodes: list[NodeInfo]) -> None:
        me = next(i for i, n in enumerate(nodes) if n.op is self)
        detectors = {n.name: n for n in nodes if getattr(n.op, "has_baseline", False)}
        unknown = sorted(self.bypass - set(detectors))
        if unknown:
            raise ValueError(f"bypass 裡的 {unknown} 不是這條 workflow 的偵測節點；可用：{sorted(detectors)}")
        for n in nodes[:me]:
            if n.name not in detectors or n.name in self.bypass:
                continue
            if not getattr(n.op, "votes", True):
                raise ValueError(f"節點 {n.name}（{n.operator}）不參與表決，必須列進 bypass")
            if getattr(n.op, "snr", None) is None:
                raise ValueError(f"節點 {n.name}（{n.operator}）沒有開 snr（節點參數 snr: {{p_fa: ...}}），"
                                 f"軟投票拿不到它的逐點證據；不投票就列進 bypass")

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        participants = [n for n in dict.fromkeys(ctx.detector_runs) if n not in self.bypass]
        evidence = {n: ctx.detector_evidence[n] for n in participants if n in ctx.detector_evidence}
        if not evidence or ctx.target is None or ctx.raw_df is None:
            return ctx
        weights = pd.DataFrame({n: pd.Series([s / e.threshold if math.isfinite(s) else 0.0 for s in e.snr],
                                             index=e.timestamps) for n, e in evidence.items()}).fillna(0.0)
        scores = pd.DataFrame({n: pd.Series(e.snr, index=e.timestamps) for n, e in evidence.items()}).fillna(0.0)
        total = weights.sum(axis=1)
        values = dict(zip((t.isoformat() for t in ctx.raw_df["ts"]), ctx.raw_df["value"].astype(float)))
        target = ctx.target
        decided = []
        for ts in total.index[total >= self.votes]:
            row = weights.loc[ts]
            voted = tuple(sorted(n for n, w in row.items() if w >= 0.5))  # 有實質貢獻的票（半個門檻以上）
            decided.append(AnomalyEvent(target.entity, target.query, as_utc(pd.Timestamp(ts).to_pydatetime()),
                                        float(scores.loc[ts].max()), dict(target.attrs), values.get(ts, float("nan")),
                                        self.operator_name, self.node_name, target.source_type, voted, ANOMALY))
        members = set(evidence)
        kept = [e for e in ctx.anomalies if e.detector_node not in members or self.keep_members]
        ctx.log(f"soft_vote:{self.node_name}:voters={len(evidence)}:need={self.votes:g}:{int((total > 0).sum())}->"
                f"{len(decided)}")
        ctx.anomalies = kept + decided
        return ctx
