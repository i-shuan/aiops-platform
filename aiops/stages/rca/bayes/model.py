"""單一根因的貝氏推論：P(根因 = 實體 X、子型 k | 觀察)。純函式、確定性、不連網。

1. 假設 = (實體, 子型) ＋ UNKNOWN（根因不在模型裡）。先驗 = 實體權重 × 子型先驗；UNKNOWN 固定 priors.UNKNOWN_PRIOR。
2. 觀察依「掛在哪個實體」分組。每組的似然看那個實體跟根因的關係（priors.py 開頭的四種），
   受傳導的實體用機制混合：Σ_機制 P(機制) × Π_組內觀察 P(觀察 | 機制)。同一個呼叫端的症狀共用同一個機制，
   所以「失敗很多＋延遲沒漲＋沒有失敗子 span＋熔斷日誌」只有熔斷能一次解釋全部。
3. 每個症狀是 noisy-OR：1 − (1 − 漏失率)(1 − 原因機率)。
4. 後驗在 log 空間算、最後正規化；全部假設加總 = 1，所以各實體的「自身故障」機率加總 = 1 − P(UNKNOWN) ≤ 1。
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

from aiops.stages.rca.bayes import priors
from aiops.stages.rca.bayes.graph import Graph
from aiops.stages.rca.bayes.observe import Observation

UNKNOWN = ""  # UNKNOWN 假設的實體名（空字串不會撞到真實實體）


@dataclass
class Posterior:
    by_hypothesis: dict[tuple[str, str], float]   # (實體, 子型) → 後驗；UNKNOWN 是 ("", "unknown")
    unknown: float
    # 每個實體被「切斷」或「傳導」的機率：Σ_根因 後驗 × 該實體的機制責任
    roles: dict[str, dict[str, float]] = field(default_factory=dict)

    def entity_total(self, entity: str) -> float:
        return sum(p for (e, _), p in self.by_hypothesis.items() if e == entity)

    def subtypes(self, entity: str) -> dict[str, float]:
        return {k: p for (e, k), p in self.by_hypothesis.items() if e == entity}


def _noisy_or(leak: float, p: float) -> float:
    return 1.0 - (1.0 - leak) * (1.0 - min(max(p, 0.0), 1.0))


def _like(obs: Observation, p: float) -> float:
    return obs.lam * p + (1.0 - obs.lam) * (1.0 - p)


def _leak(symptom: str) -> float:
    return priors.LEAK.get(symptom, priors.DEFAULT_LEAK)


def _toward(obs: Observation, root: str, graph: Graph) -> bool:
    """wait／child_outlives 只有在「等的是根因或傳導路徑上的實體」時才算傳導來的症狀。"""
    if root == UNKNOWN:
        return obs.symptom == "wait"  # 看不到的根因：任何等待都可能是它；子 span 晚結束要有具體下游才算
    return bool(obs.ref) and graph.reaches(root, obs.ref)


def _direct_p(obs: Observation, subtype: str, root: str, graph: Graph) -> float:
    p = priors.DIRECT[subtype].get(obs.symptom, priors.DIRECT_ANY.get(obs.symptom, 0.0))
    if obs.symptom == "wait" and obs.ref and obs.ref != root and obs.ref in graph.comp:
        p *= priors.MODELED_RESOURCE_PENALTY
    return p


def _mechanism_p(obs: Observation, mechanism: str, subtype: str, root: str, graph: Graph, factor: float) -> float:
    if obs.symptom in ("wait", "child_outlives") and not _toward(obs, root, graph):
        return 0.0
    table = priors.MECHANISM[mechanism]
    p = table.get(obs.symptom, 0.0)
    if mechanism == "propagate":
        p = priors.PROPAGATE_BY_SUBTYPE.get(subtype, {}).get(obs.symptom, p)
    return p * factor


def relation(anchor: str, root: str, graph: Graph) -> tuple[str, float]:
    """(關係, 傳導強度)。關係：self／affected／callee_of_root／unrelated。"""
    if root == UNKNOWN:
        return "affected", priors.UNKNOWN_REACH
    if anchor == root:
        return "self", 1.0
    hops = graph.hops(root, anchor)
    if hops is not None:
        return "affected", priors.INTRA_SCC_FACTOR if hops == 0 else priors.HOP_DECAY ** (hops - 1)
    if anchor in graph.callees.get(root, []):
        return "callee_of_root", 1.0
    return "unrelated", 1.0


def group_likelihood(group: list[Observation], anchor: str, root: str, subtype: str,
                     graph: Graph) -> tuple[float, dict[str, float]]:
    """一個實體上所有觀察的聯合似然；受傳導時另回傳各機制的責任（後驗占比）。"""
    rel, factor = relation(anchor, root, graph)
    if rel == "affected":
        parts = {}
        for m in sorted(priors.MECHANISM_PRIOR):
            like = priors.MECHANISM_PRIOR[m]
            for o in group:
                like *= _like(o, _noisy_or(_leak(o.symptom), _mechanism_p(o, m, subtype, root, graph, factor)))
            parts[m] = like
        total = sum(parts.values())
        return total, {m: v / total for m, v in parts.items()} if total > 0 else {}
    like = 1.0
    for o in group:
        if rel == "self":
            p = _direct_p(o, subtype, root, graph)
        elif rel == "callee_of_root":
            p = priors.CALLEE_OF_ROOT.get(o.symptom, 0.0)
        else:
            p = 0.0
        like *= _like(o, _noisy_or(_leak(o.symptom), p))
    return like, {}


def observation_likelihood(o: Observation, root: str, subtype: str, graph: Graph) -> float:
    """單一觀察的邊際似然（機制各自邊際化）。只拿來說明「哪筆證據最支持這個候選」，不用在後驗本身。"""
    return group_likelihood([o], o.entity, root, subtype, graph)[0]


def infer(observations: list[Observation], graph: Graph, weights: dict[str, float]) -> Posterior:
    groups: dict[str, list[Observation]] = {}
    for o in observations:
        groups.setdefault(o.entity, []).append(o)
    anchors = sorted(groups)
    total_weight = sum(weights.values())
    hypotheses: list[tuple[str, str, float]] = [
        (e, k, (1 - priors.UNKNOWN_PRIOR) * weights[e] / total_weight * priors.SUBTYPE_PRIOR[k])
        for e in sorted(weights) for k in sorted(priors.SUBTYPE_PRIOR)]
    hypotheses.append((UNKNOWN, "unknown", priors.UNKNOWN_PRIOR))

    log_post: list[float] = []
    resp: list[dict[str, dict[str, float]]] = []
    for root, subtype, prior in hypotheses:
        lp = math.log(prior)
        r: dict[str, dict[str, float]] = {}
        for a in anchors:
            like, mech = group_likelihood(groups[a], a, root, subtype, graph)
            lp += math.log(max(like, 1e-300))
            if mech:
                r[a] = mech
        log_post.append(lp)
        resp.append(r)
    top = max(log_post)
    raw = [math.exp(lp - top) for lp in log_post]
    z = sum(raw)
    post = [x / z for x in raw]

    by_h = {(root, subtype): p for (root, subtype, _), p in zip(hypotheses, post)}
    roles: dict[str, dict[str, float]] = {}
    for (root, _, _), p, r in zip(hypotheses, post, resp):
        for a, mech in r.items():
            role = roles.setdefault(a, {m: 0.0 for m in sorted(priors.MECHANISM_PRIOR)})
            for m, share in mech.items():
                role[m] += p * share
    return Posterior(by_h, by_h[(UNKNOWN, "unknown")], roles)
