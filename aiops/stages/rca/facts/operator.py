"""rca.facts：認知型 RCA 的證據層算子（docs/COGNITIVE_RCA_DESIGN.md）。

讀 ctx.rca_spans／rca_logs／rca_logs_baseline／rca_mq／rca_series 與 ctx.incidents[0]，算成 list[Fact] 寫進 ctx.rca_facts。
- 不呼叫 LLM、不連網、不 import enrichment；沒有 trace／日誌時輸出空清單，不丟例外。
- 資料不足就不產出那一項，原因逐條寫進 ctx.log（rca.facts:skip:<項目>:<原因>）。
- 確定性：同一組輸入永遠同一份輸出。最後依 (kind, entity, window, detail) 排序，不依賴 dict／set 的迭代順序。
- 拓撲有環（A↔B）不影響：這裡沒有任何沿呼叫圖的走訪，span 父子環也只做單層父子比對。

params：
  min_requests        入口 span／呼叫邊至少幾筆才算比例與增量（預設 20，跟 rca.root_cause.evidence 一樣）
  min_log_count       一個日誌樣板事件期間至少幾筆（加權後）才列（預設 3，跟 rank_patterns 一樣）
  max_templates_per_service  每個服務最多列幾個日誌樣板（預設 5）
  min_messages        一條佇列邊至少幾則訊息才列延遲（預設 1）
  min_series_points   時序在事件前、事件期間各至少幾個點才算位移（預設 3）
"""
from __future__ import annotations

import json
from typing import ClassVar

from aiops.engine.context import PipelineContext
from aiops.engine.operator import BaseOperator
from aiops.engine.params import whole
from aiops.engine.rca_types import Fact
from aiops.engine.registry import OperatorRegistry
from aiops.stages.rca.base import need_incident
from aiops.stages.rca.facts.logs import log_facts
from aiops.stages.rca.facts.mq import mq_facts
from aiops.stages.rca.facts.series import series_facts
from aiops.stages.rca.facts.spans import span_facts
from aiops.stages.rca.facts.windows import windows


def sort_facts(facts: list[Fact]) -> list[Fact]:
    """固定排序：kind → entity → window → detail 的 JSON（鍵排序）。detail 裡有 callee／target／template，同一個實體的多筆也排得開。"""
    return sorted(facts, key=lambda f: (f.kind, f.entity, f.window, json.dumps(f.detail, sort_keys=True, default=str),
                                        f.value))


@OperatorRegistry.register("rca.facts")
class FactsOperator(BaseOperator):
    scope: ClassVar[str] = "batch"

    def __init__(self, min_requests: int = 20, min_log_count: int = 3, max_templates_per_service: int = 5,
                 min_messages: int = 1, min_series_points: int = 3):
        self.min_requests = whole("min_requests", min_requests)
        self.min_log_count = whole("min_log_count", min_log_count)
        self.max_templates_per_service = whole("max_templates_per_service", max_templates_per_service)
        self.min_messages = whole("min_messages", min_messages)
        self.min_series_points = whole("min_series_points", min_series_points)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        incident = need_incident(ctx)
        w = windows(incident, ctx)
        skips: list[str] = []
        facts = [
            *span_facts(ctx.rca_spans, w, self.min_requests, skips),
            *log_facts(ctx.rca_logs, ctx.rca_logs_baseline, ctx.rca_log_window_minutes, ctx.rca_logs_sampled,
                       self.min_log_count, self.max_templates_per_service, skips),
            *mq_facts(ctx.rca_mq, self.min_messages, skips),
            *series_facts(ctx.rca_series, w, self.min_series_points, skips),
        ]
        ctx.rca_facts = sort_facts(facts)
        for s in skips:
            ctx.log(f"rca.facts:skip:{s}")
        ctx.log(f"rca.facts:{len(ctx.rca_facts)}")
        return ctx
