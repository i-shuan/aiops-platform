"""RCA 的兩種可抽換零件：

1. 根因演算法（rca.root_cause.*）：繼承 RootCauseOperator，只寫 rank()，把候選根因排好放進 ctx.rca_root_candidates。
   換演算法 = 在 rca_report.yaml 換一個節點（topological／pagerank／cross_correlation），報告段落不用改。
2. 報告段落（rca.section.*）：繼承 RcaSection，寫 applies()（資料夠不夠）與 build()（內容）。
   資料不夠就略過（記在 ctx.rca_skipped），不輸出空殼。新段落 = 新檔案 + 註冊，rca.report 的 sections 清單就能用。

兩者都透過 OperatorRegistry 註冊（放進 stages/rca/ 底下就會被自動載入），不需要改任何既有程式。
"""
from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from typing import ClassVar

from aiops.engine.context import PipelineContext, ReportSection, RootCandidate
from aiops.engine.errors import NotFound
from aiops.engine.topology import propagation_edges
from aiops.engine.types import Incident
from aiops.engine.operator import BaseOperator

SECTION_PREFIX = "rca.section."
ROOT_CAUSE_PREFIX = "rca.root_cause."


def need_incident(ctx: PipelineContext) -> Incident:
    if not ctx.incidents:
        raise NotFound("沒有可以產生報告的事件群")
    return ctx.incidents[0]


def graph_edges(ctx: PipelineContext) -> set[tuple[str, str]]:
    """異常傳播方向的有向邊（IT：被呼叫端 -> 呼叫端；工廠：上游 -> 下游）。"""
    return propagation_edges(ctx.topology, ctx.lot_edges)


def first_seen(incident: Incident) -> dict[str, tuple[datetime, float]]:
    """每個實體：最早出現時間、最高分數。"""
    seen: dict[str, tuple[datetime, float]] = {}
    for e in incident.events:
        ts, score = seen.get(e.entity, (e.ts, e.score))
        seen[e.entity] = (min(ts, e.ts), max(score, e.score))
    return seen


def in_graph(entity: str, edges: set[tuple[str, str]]) -> bool:
    return any(entity in edge for edge in edges)


class RootCauseOperator(BaseOperator):
    """根因演算法的契約。rank() 回傳由最可能到最不可能排好的候選；回空清單代表資料不足（段落會略過）。"""

    scope: ClassVar[str] = "batch"

    @abstractmethod
    def rank(self, incident: Incident, ctx: PipelineContext) -> list[RootCandidate]: ...

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        incident = need_incident(ctx)
        ctx.rca_root_note = ""
        ctx.rca_root_candidates = self.rank(incident, ctx)
        ctx.rca_root_algorithm = self.operator_name
        ctx.log(f"root_cause:{self.operator_name}:{len(ctx.rca_root_candidates)}")
        return ctx


class RcaSection(BaseOperator):
    """報告段落的契約。可以直接當 YAML 節點用，也可以由 rca.report 依 sections 清單組裝。"""

    scope: ClassVar[str] = "batch"
    key: ClassVar[str] = ""
    title: ClassVar[str] = ""

    def applies(self, incident: Incident, ctx: PipelineContext) -> bool:
        return True

    @abstractmethod
    def build(self, incident: Incident, ctx: PipelineContext) -> str:
        """段落內容（純文字）。"""

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        incident = need_incident(ctx)
        if self.applies(incident, ctx):
            ctx.rca_sections.append(ReportSection(self.key, self.title, self.build(incident, ctx)))
        else:
            ctx.rca_skipped.append(self.key)
        return ctx
