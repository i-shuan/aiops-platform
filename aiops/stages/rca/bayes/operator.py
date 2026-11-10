"""rca.root_cause.bayes：事實（ctx.rca_facts）＋ LLM 證據（ctx.rca_evidence）＋ 拓撲 → 帶後驗機率的根因候選。

不連網、不 import enrichment；同一組輸入一定得到同一個排名（所有走訪都先排序，同分依實體名）。
數字全在 priors.py，推論在 model.py，這裡只做組裝與輸出。
"""
from __future__ import annotations

import math

from aiops.engine.context import PipelineContext, RootCandidate
from aiops.engine.params import number, whole
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import Incident
from aiops.stages.rca.base import RootCauseOperator, graph_edges
from aiops.stages.rca.bayes import priors
from aiops.stages.rca.bayes.graph import Graph
from aiops.stages.rca.bayes.model import UNKNOWN, Posterior, infer, observation_likelihood
from aiops.stages.rca.bayes.observe import Observation, collect

_WAIT_SUBTYPES = ("dependency", "conn_pool", "lock", "messaging")


def _pct(p: float) -> str:
    return f"{p:.0%}"


@OperatorRegistry.register("rca.root_cause.bayes")
class BayesRootCause(RootCauseOperator):
    """params：
    - min_posterior（0～1，預設 0.3）：最可能的候選低於這個機率，或「根因不在模型裡」比它還可能，就不指認。
    - report_floor（0～1，預設 0.01）：低於這個機率的候選不列出。
    - min_bayes_factor（>= 1，預設 3）：證據讓最可能的候選比「根因不在模型裡」多可能幾倍（後驗勝算 ÷ 先驗勝算）。
      低於它 = 排名主要來自先驗（例如事件群裡只有一個實體），不是證據，不指認。
      3 取自 Jeffreys／Kass & Raftery 的貝氏因子分級：1～3「不值一提」，3 以上才算「有實質支持」。
    - top_k（預設 5）：最多列幾個候選。
    """

    def __init__(self, min_posterior: float = 0.3, report_floor: float = 0.01, min_bayes_factor: float = 3.0,
                 top_k: int = 5):
        self.min_posterior = number("min_posterior", min_posterior, minimum=0, maximum=1)
        self.min_bayes_factor = number("min_bayes_factor", min_bayes_factor, minimum=1)
        self.report_floor = number("report_floor", report_floor, minimum=0, maximum=1)
        self.top_k = whole("top_k", top_k)
        if self.report_floor > self.min_posterior:
            raise ValueError("report_floor 不能大於 min_posterior")

    def rank(self, incident: Incident, ctx: PipelineContext) -> list[RootCandidate]:
        if ctx.rca_status == "undetermined":
            ctx.rca_root_note = f"無法判定：{ctx.rca_undetermined_reason or 'LLM 證據抽取未完成'}，機率模型不排名"
            return []
        edges = graph_edges(ctx)
        topo = Graph.build(sorted({n for e in edges for n in e}), edges)
        observations = collect(ctx.rca_facts, ctx.rca_evidence, topo.callees, topo.callers)
        if not observations:
            ctx.rca_root_note = "證據不足，不指認根因：沒有任何可用的事實或日誌證據"
            return []
        if not any(o.significant for o in observations):
            ctx.rca_root_note = f"證據不足，不指認根因：{len(observations)} 筆事實／證據都沒有越過顯著門檻"
            return []

        incident_entities = set(incident.entities)
        candidates = set(incident_entities) | {o.entity for o in observations} | \
            {o.ref for o in observations if o.symptom == "child_outlives" and o.ref}
        candidates |= {callee for e in incident_entities for callee in topo.callees.get(e, [])}
        graph = Graph.build(sorted(candidates | set(topo.nodes)), edges)
        weights = {e: priors.INCIDENT_ENTITY_WEIGHT if e in incident_entities else priors.OTHER_ENTITY_WEIGHT
                   for e in sorted(candidates)}
        post = infer(observations, graph, weights)

        ranked = sorted(((post.entity_total(e), e) for e in weights), key=lambda t: (-t[0], t[1]))
        best_p, best_e = ranked[0]
        if post.unknown >= best_p or best_p < self.min_posterior:
            ctx.rca_root_note = (f"證據不足，不指認根因：最可能的 {best_e} 只有 {_pct(best_p)}，"
                                 f"根因不在模型裡的機率 {_pct(post.unknown)}（門檻 {_pct(self.min_posterior)}）")
            return []
        prior_best = (1 - priors.UNKNOWN_PRIOR) * weights[best_e] / sum(weights.values())
        bayes_factor = (best_p / max(post.unknown, 1e-300)) / (prior_best / priors.UNKNOWN_PRIOR)
        if bayes_factor < self.min_bayes_factor:
            ctx.rca_root_note = (f"證據不足，不指認根因：{best_e} 的 {_pct(best_p)} 主要來自先驗，證據只讓它比"
                                 f"「根因不在模型裡」多 {bayes_factor:.1f} 倍可能（門檻 {self.min_bayes_factor:g} 倍）")
            return []
        cycles = graph.cycles()
        out = []
        for p, entity in ranked[:self.top_k]:
            if p < self.report_floor:
                break
            out.append(self._candidate(entity, p, post, observations, graph, cycles))
        return out

    def _candidate(self, entity: str, p: float, post: Posterior, observations: list[Observation],
                   graph: Graph, cycles: list[list[str]]) -> RootCandidate:
        subtypes = post.subtypes(entity)
        best_k = min(subtypes, key=lambda k: (-subtypes[k], k))
        used = []
        for o in observations:
            llr = math.log(max(observation_likelihood(o, entity, best_k, graph), 1e-300)) - \
                math.log(max(observation_likelihood(o, UNKNOWN, "unknown", graph), 1e-300))
            if abs(llr) >= 0.05:
                used.append((llr, o))
        used.sort(key=lambda t: (-abs(t[0]), t[1].summary))
        support = [o.summary for llr, o in sorted(used, key=lambda t: (-t[0], t[1].summary)) if llr > 0]
        role = post.roles.get(entity, {})
        cut = {m: round(role.get(m, 0.0), 4) for m in ("breaker", "timeout")}
        posterior = {
            "self": {k: round(v, 4) for k, v in sorted(subtypes.items(), key=lambda kv: (-kv[1], kv[0]))},
            "self_total": round(p, 4),
            "cut_by_callee": cut,
            "propagated": round(role.get("propagate", 0.0), 4),
            "unknown": round(post.unknown, 4),
        }
        dependency = ""
        if best_k in _WAIT_SUBTYPES:
            waits = [o for o in observations if o.entity == entity and o.symptom == "wait" and o.significant and o.ref]
            waits.sort(key=lambda o: (-dict(o.raw)["value"], o.ref))  # 等待增量最大的那個資源
            dependency = waits[0].ref if waits else ""
        reason = (f"{entity} 是源頭的機率 {_pct(p)}，最可能是「{priors.SUBTYPE_LABEL[best_k]}」（{_pct(subtypes[best_k])}）"
                  + (f"；主要依據：{'、'.join(support[:priors.TOP_REASONS])}" if support else ""))
        cut_total = sum(cut.values())
        if cut_total > p:
            reason += f"；它的異常比較像是被下游切斷（{_pct(cut_total)}）"
        cycle = next((c for c in cycles if entity in c), None)
        if cycle:
            reason += f"；位於循環呼叫 {'↔'.join(cycle)} 中，機率依各自身上的證據分配"
        attribution = {
            "posterior": posterior,
            "subtype": best_k,
            "evidence_used": [dict(o.raw) | {"llr": round(llr, 3), "summary": o.summary}
                              for llr, o in used[:priors.EVIDENCE_USED_LIMIT]],
        }
        if cycle:
            attribution["cycle"] = cycle
        if dependency:
            attribution["resource"] = dependency
        return RootCandidate(entity, round(p, 6), reason, dependency, attribution)
