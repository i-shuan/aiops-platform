"""把 Fact（硬證據）與 EvidenceItem（軟證據）轉成模型吃的「觀察」。純函式、確定性。

每個觀察帶一個 lam = 「症狀出現」這一端的證據權重（Pearl 的虛擬證據）：
- 事實顯著 → lam = 1；事實不顯著 → lam = 0（不顯著也是證據：例如自身耗時沒漲，就不像 CPU 飽和）
- LLM 證據 present、信心 c → lam = c；absent、信心 c → lam = 1 − c（反證）
似然 = lam × P(出現 | 假設) + (1 − lam) × P(不出現 | 假設)。lam = 0.5 等於沒說。

沒有任何事實或證據提到的症狀 = 沒觀察到，不進似然（不罰也不加分）。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from aiops.engine.rca_types import EVIDENCE_KINDS, EvidenceItem, Fact
from aiops.stages.rca.bayes import priors


@dataclass(frozen=True)
class Observation:
    symptom: str      # priors 裡的症狀名
    entity: str       # 掛在哪個實體上（決定它跟根因的關係）
    lam: float        # 「出現」這一端的證據權重，0～1
    ref: str = ""     # wait：等的資源；child_outlives：子 span 的實體（entity 是父 span 的實體）
    origin: str = ""  # fact／evidence
    summary: str = ""  # 人看的一句（給 evidence_used、reason）
    raw: tuple[tuple[str, Any], ...] = ()  # 給 evidence_used 的結構化欄位

    @property
    def significant(self) -> bool:
        return self.lam > 0.5


def _num(detail: dict[str, Any], key: str) -> float | None:
    v = detail.get(key)
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _ms_significant(kind: str, fact: Fact) -> bool:
    """window=delta：value 是增量；其他 window：value 是事件期間的值，有 detail.before 就扣掉基準。
    沒有基準可比時只看絕對增量（kind 名稱本身就是 *_delta 的那幾種）。"""
    absolute, ratio = priors.MS_THRESHOLD[kind]
    before = _num(fact.detail, "before")
    if fact.window == "delta":
        delta, during = fact.value, (before + fact.value if before is not None else None)
    else:
        delta, during = (fact.value - before if before is not None else fact.value), fact.value
    if delta < absolute:
        return False
    return not (ratio and before is not None and before > 0 and during is not None and during / before < ratio)


def _traffic_drop(fact: Fact) -> float:
    """流量下降比例（0 = 沒降，1 = 全斷）。detail 有 before／during 就用它們算，否則 value 就是下降比例。"""
    before, during = _num(fact.detail, "before"), _num(fact.detail, "during")
    if before is not None and during is not None and before > 0:
        return max(0.0, 1.0 - during / before)
    return fact.value


def fact_significant(fact: Fact) -> bool | None:
    """None = 這個 kind 模型不認得（略過，不猜）。"""
    kind = fact.kind
    if kind in priors.MS_THRESHOLD:
        return _ms_significant(kind, fact)
    if kind == "traffic_drop":
        return _traffic_drop(fact) >= priors.RATIO_THRESHOLD[kind]
    if kind == "entry_failure_rate":
        before = _num(fact.detail, "before")
        rate = fact.value if fact.window != "delta" else (before or 0.0) + fact.value
        increase = fact.value if fact.window == "delta" else rate - (before or 0.0)
        return rate >= priors.RATIO_THRESHOLD[kind] and increase >= priors.FAILURE_MIN_INCREASE
    if kind in priors.RATIO_THRESHOLD:
        return fact.value >= priors.RATIO_THRESHOLD[kind]
    return None


def _ref(fact: Fact) -> str:
    for key in ("resource", "target", "callee", "child"):
        v = fact.detail.get(key)
        if isinstance(v, str) and v:
            return v
    return ""


def from_fact(fact: Fact, callers: dict[str, list[str]]) -> list[Observation]:
    sig = fact_significant(fact)
    symptom = priors.FACT_SYMPTOM.get(fact.kind)
    if sig is None or symptom is None:
        return []
    raw = (("type", "fact"), ("kind", fact.kind), ("entity", fact.entity), ("value", fact.value),
           ("unit", fact.unit), ("window", fact.window), ("significant", sig))
    unit = f" {fact.unit}" if fact.unit else ""
    summary = f"{fact.kind}@{fact.entity}={fact.value:g}{unit}（{'顯著' if sig else '不顯著'}）"
    lam = 1.0 if sig else 0.0
    if symptom == "child_outlives":
        # 事實掛在子 span 的實體上；「誰放棄等待」是父 span 的實體，觀察要掛在父那邊（它的呼叫端行為）
        parent = fact.detail.get("parent")
        parents = [parent] if isinstance(parent, str) and parent else callers.get(fact.entity, [])
        return [Observation(symptom, p, lam, fact.entity, "fact", summary, raw + (("parent", p),)) for p in sorted(parents)]
    ref = _ref(fact) if symptom == "wait" else ""
    if ref:
        raw += (("resource", ref),)
    return [Observation(symptom, fact.entity, lam, ref, "fact", summary, raw)]


def from_evidence(item: EvidenceItem, callees: dict[str, list[str]], callers: dict[str, list[str]]) -> list[Observation]:
    if item.kind not in EVIDENCE_KINDS:
        return []
    c = min(max(float(item.confidence), 0.0), 1.0)
    lam = c if item.state == "present" else 1.0 - c
    symptom = item.kind
    if symptom == "circuit_breaker_open" and not callees.get(item.entity) and callers.get(item.entity):
        symptom = priors.BREAKER_TARGET  # 它沒有下游、卻有呼叫端：日誌講的是「到它的熔斷器」
    raw = (("type", "evidence"), ("kind", item.kind), ("entity", item.entity), ("state", item.state),
           ("confidence", c), ("quote", item.quote), ("log_ts", item.log_ts))
    summary = f"{item.kind}@{item.entity}（{'有' if item.state == 'present' else '否認'}，信心 {c:.2f}）"
    return [Observation(symptom, item.entity, lam, "", "evidence", summary, raw)]


def collect(facts: list[Fact], evidence: list[EvidenceItem], callees: dict[str, list[str]],
            callers: dict[str, list[str]]) -> list[Observation]:
    """同一個（症狀、實體、ref、來源）重複出現時只留最強的一筆：同一件事被證據層或 LLM 講兩次，不能當兩份證據。"""
    best: dict[tuple[str, str, str, str, bool], Observation] = {}
    items = [o for f in facts for o in from_fact(f, callers)] + \
            [o for e in evidence for o in from_evidence(e, callees, callers)]
    for o in items:
        key = (o.symptom, o.entity, o.ref, o.origin, o.lam >= 0.5)
        kept = best.get(key)
        if kept is None or abs(o.lam - 0.5) > abs(kept.lam - 0.5) or \
                (abs(o.lam - 0.5) == abs(kept.lam - 0.5) and o.summary < kept.summary):
            best[key] = o
    return sorted(best.values(), key=lambda o: (o.entity, o.symptom, o.ref, o.origin, o.lam, o.summary))
