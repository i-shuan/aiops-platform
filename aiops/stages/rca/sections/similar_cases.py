"""歷史相似案例：從經驗庫（確認是真異常、寫了處置的事件群）找出跟這次最像的幾件，列出當時怎麼處理。

相似度 = 實體重疊 × w_entities + 指標重疊 × w_metrics + 訊號重疊 × w_signals（各自用 Jaccard），
同一條管線再加 same_pipeline_bonus。只列 >= min_similarity 的前 top_k 件；一件都沒有就略過這段。
這是規則式的比對（可預期、可解釋），不是語意搜尋。
"""
from __future__ import annotations

from aiops.engine.registry import OperatorRegistry
from aiops.engine.signature import incident_signature, jaccard
from aiops.stages.rca.base import RcaSection


@OperatorRegistry.register("rca.section.similar_cases")
class SimilarCases(RcaSection):
    key, title = "similar_cases", "歷史相似案例與先查清單"

    def __init__(self, top_k: int = 3, min_similarity: float = 0.3, w_entities: float = 0.5,
                 w_metrics: float = 0.3, w_signals: float = 0.2, same_pipeline_bonus: float = 0.1):
        if top_k < 1 or not 0 <= min_similarity <= 1:
            raise ValueError("top_k 至少 1；min_similarity 必須在 0 到 1 之間")
        self.top_k, self.min_similarity = int(top_k), float(min_similarity)
        self.weights = (float(w_entities), float(w_metrics), float(w_signals))
        self.bonus = float(same_pipeline_bonus)

    def _ranked(self, incident, ctx):
        entities, metrics, signals = incident_signature(incident)
        we, wm, ws = self.weights
        scored = []
        for case in ctx.knowledge_cases:
            sim = we * jaccard(entities, case.entities) + wm * jaccard(metrics, case.metrics) \
                + ws * jaccard(signals, case.signals)
            if case.pipeline and case.pipeline == ctx.incident_pipeline:
                sim += self.bonus
            sim = min(sim, 1.0)
            if sim >= self.min_similarity:
                scored.append((sim, case))
        scored.sort(key=lambda sc: (-sc[0], -sc[1].created_at.timestamp()))
        return scored[: self.top_k]

    def applies(self, incident, ctx):
        return bool(self._ranked(incident, ctx))

    def build(self, incident, ctx):
        lines = []
        for i, (sim, case) in enumerate(self._ranked(incident, ctx), 1):
            line = f"{i}. {case.incident_uid}（相似度 {sim:.2f}，{case.created_at.date().isoformat()}，實體 {', '.join(case.entities)}）"
            if case.root_cause:
                line += f"\n   當時根因：{case.root_cause}"
            line += f"\n   當時處置：{case.resolution}"
            lines.append(line)
        return "\n".join(lines)
