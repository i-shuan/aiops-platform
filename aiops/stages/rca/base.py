"""RCA 的兩種可抽換零件：

1. 根因演算法（rca.root_cause.*）：繼承 RootCauseOperator，只寫 rank()，把候選根因排好放進 ctx.rca_root_candidates。
   換演算法 = 在 rca_report.yaml 換一個節點（evidence／topological／llm），報告段落不用改。
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
from aiops.engine.rca_types import CONCLUSION_TYPES, ConclusionType
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
    return ctx.graph.propagation()


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
    """根因演算法的契約。rank() 回傳由最可能到最不可能排好的候選；回空清單代表資料不足（段落會略過）。

    結論等級（engine/rca_types.ConclusionType）由演算法自己宣告，報告與引擎不看算子名稱：
    - 類別屬性 conclusion：這個演算法「給出候選時」屬於哪一級（必填，沒寫在定義類別時就報錯）
    - 同一個演算法不同情況屬於不同等級時（退回別的演算法、只看得到症狀），在 rank() 裡寫 ctx.rca_conclusion_type
      與 ctx.rca_conclusion_reason 覆寫這一次的等級
    - 沒有候選一律是 undetermined，演算法寫什麼都不算
    """

    scope: ClassVar[str] = "batch"
    conclusion: ClassVar[ConclusionType]

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        declared = getattr(cls, "conclusion", None)
        if declared not in CONCLUSION_TYPES or declared == "undetermined":
            raise TypeError(f"{cls.__name__} 必須宣告 conclusion（給出候選時的結論等級）："
                            f"{[c for c in CONCLUSION_TYPES if c != 'undetermined']}，不是 {declared!r}")

    @abstractmethod
    def rank(self, incident: Incident, ctx: PipelineContext) -> list[RootCandidate]: ...

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        incident = need_incident(ctx)
        ctx.rca_root_note = ""
        ctx.rca_conclusion_reason = ""
        ctx.rca_conclusion_type = declared = self.conclusion  # rank() 可以覆寫這一次的等級
        ctx.rca_root_candidates = self.rank(incident, ctx)
        if not ctx.rca_root_candidates:
            ctx.rca_conclusion_type = "undetermined"
        elif ctx.rca_conclusion_type not in CONCLUSION_TYPES or ctx.rca_conclusion_type == "undetermined":
            ctx.rca_conclusion_type = declared  # 有候選卻寫了不認得的等級：退回演算法宣告的那一級
        ctx.rca_root_algorithm = self.operator_name
        ctx.log(f"root_cause:{self.operator_name}:{len(ctx.rca_root_candidates)}:{ctx.rca_conclusion_type}")
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
