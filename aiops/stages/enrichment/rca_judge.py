"""rca.root_cause.llm：把呼叫圖（DAG）＋證據層事實＋驗證過的日誌證據一起交給 LLM，由 LLM 判斷根因。

跟 rca.root_cause.bayes（機率模型）、rca.root_cause.evidence（舊判斷層）並排的第三個根因算子，用 rca_report_llm.yaml 掛上。
放在 enrichment/ 是因為它要呼叫 LLM（stages/ 其他地方不准 import enrichment、不准連外）；契約仍是 RootCauseOperator。

LLM 的每個候選都要過驗證，不過就丟掉並記原因（ctx.log）：
  1. entity 是已知實體（事件群、呼叫圖、trace、事實裡出現過的）
  2. confidence 是 0～1 的數字；subtype 在封閉詞表內
  3. cites 至少引用一筆，且每個編號（F<n> 事實／E<n> 日誌證據）都真的送進過 prompt
  4. reason 裡的數字要能在它引用的事實裡找到（容許四捨五入、比例寫成百分比）；找不到 = LLM 自己算或編的
  5. resource（選填）要是它引用的事實裡出現過的資源名稱；不是就把這一欄清掉（候選保留）

結果怎麼寫：
  - LLM 沒設定、連不上、逾時、不是 JSON、格式不合、候選全部被拒 → rca_status = "undetermined"，原因寫進
    rca_undetermined_reason，不排名、不退回別的演算法、不重試到好
  - LLM 自己回「證據不足」（candidates 空）→ 不是失敗：空清單，原因寫進 rca_root_note
  - 上游已經是 undetermined（證據抽取失敗）→ 不呼叫 LLM

已知限制（不是 bug，是這個做法的性質）：同一組輸入 LLM 可能給出不同排名；信心是 LLM 自評，不是校準過的機率。
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from aiops.engine.context import PipelineContext, RootCandidate
from aiops.engine.params import positive, whole
from aiops.engine.rca_types import EvidenceItem, Fact
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import Incident, as_utc
from aiops.stages.enrichment.base import LLMError, parse_json_reply
from aiops.stages.enrichment.evidence_extractor import (LLM_ALL_REJECTED, LLM_BAD_JSON, LLM_BAD_SCHEMA, MAX_RETRIES,
                                                        CallFailure, check_provider, complete_with_policy,
                                                        known_entities, resolve_provider)
from aiops.stages.rca.base import RootCauseOperator, first_seen

logger = logging.getLogger("aiops.enrichment")

SUBTYPES: dict[str, str] = {
    "error": "程式錯誤／壞版本（自己直接回錯）",
    "cpu": "自身計算變慢（CPU、程式變慢）",
    "dependency": "等待的外部依賴或資源變慢（資料庫、外部 API）",
    "conn_pool": "連線池／執行緒池耗盡",
    "lock": "鎖競爭／死結",
    "gc": "GC 停頓",
    "memory": "記憶體耗盡",
    "messaging": "訊息中介或消費積壓",
    "amplifier": "自己把對下游的呼叫放大（重試風暴、扇出暴增），灌爆下游",
    "ingress_surge": "外部進來的流量暴增（不是哪個服務壞掉）",
}

FACT_GLOSSARY: dict[str, str] = {
    "entry_latency": "入口請求平均耗時（ms，事件期間）；detail.before 是事件前",
    "entry_failure_rate": "入口請求失敗比例（0～1，事件期間）",
    "failed_without_failed_child": "自己失敗、但沒有任何下游呼叫失敗的入口請求比例（高 = 錯是自己產生的，或被熔斷／逾時切斷）",
    "self_time_delta": "每個請求花在自己身上的時間增量（ms，事件期間 − 事件前）",
    "wait_delta": "每個請求花在等 detail.target 這個資源／下游的時間增量（ms）",
    "inbound_rate": "入口流量的前後比（事件期間每分鐘 ÷ 事件前每分鐘；2 = 變兩倍）；detail.by_caller 是各來源的筆數",
    "traffic_drop": "entity 呼叫 detail.callee 的流量前後比（事件期間 ÷ 事件前；0.1 = 掉到剩一成，1 = 沒變，大於 1 = 變多）",
    "fanout_ratio": "entity 每處理一個請求呼叫 detail.callee 幾次的前後比（大於 1 = 放大）",
    "child_outlives_parent": "下游還在跑、呼叫端已經結束的比例（呼叫端逾時放棄的跡象）",
    "child_precedes_parent": "下游 span 開始時間早於上游的比例（時鐘或非同步的跡象）",
    "log_template_surge": "一個錯誤日誌樣板的每分鐘頻率暴增倍率；detail.new = 事件前沒出現過",
    "mq_lag": "訊息從 detail.producer 發出到 entity 消費的延遲 p95（ms）；detail.before 是事件前",
    "metric_shift": "detail.metric 這條指標的平均值位移（事件期間 − 事件前）",
}

SYSTEM = (
    "你是資深 SRE，負責判斷一場事故的根因服務。使用者會給你：呼叫圖、告警先後、平台算好的事實（編號 F）、"
    "從日誌抽出並驗證過的證據（編號 E）。只能根據這些資料判斷，不准使用資料以外的知識去猜這個系統。"
    "根因是「最先出問題、把別人拖下水」的那個服務：下游變慢會拖慢上游，所以上游的延遲與失敗多半是被害症狀；"
    "熔斷打開、呼叫端逾時代表呼叫端是被害者；一個服務把對下游的呼叫放大時，根因是放大的那個服務，不是被灌爆的下游。"
    "呼叫圖的 resources 是服務依賴的資源（資料庫、快取、外部 API），它們不是可以指認的實體："
    "一個服務的時間都花在等某個資源（或對它的呼叫大量失敗）時，候選填那個服務、subtype 選 dependency、resource 填資源名稱。"
    "資料不足以分辨時，回空的 candidates 並在 note 說明缺什麼，不要硬給答案。只輸出 JSON，不要其他文字。"
)

_NUMBER = re.compile(r"\d+(?:\.\d+)?")
_CITE = re.compile(r"\b[FE]\d+\b")
_IDENT = re.compile(r"[A-Za-z][A-Za-z0-9]*(?:[-_.:/@][A-Za-z0-9]+)+")  # ord-svc、tax-gw:443 這類名稱裡的數字不算
_TOLERANCE = 0.10
_DETAIL_CHARS = 600


def _round(v: Any) -> Any:
    """浮點數留 4 位有效數字（省 prompt；驗證用同一份數字）。"""
    if isinstance(v, bool) or not isinstance(v, float):
        return v
    return float(f"{v:.4g}")


def _compact(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _compact(v) for k, v in obj.items() if v is not None and v != ""}
    if isinstance(obj, (list, tuple)):
        return [_compact(v) for v in obj]
    return _round(obj)


def fact_rows(facts: list[Fact], max_facts: int) -> list[dict]:
    rows = []
    for i, f in enumerate(facts[:max_facts], 1):
        row: dict[str, Any] = {"id": f"F{i}", "kind": f.kind, "entity": f.entity, "value": _round(float(f.value))}
        if f.unit:
            row["unit"] = f.unit
        detail = _compact(f.detail)
        if detail and len(json.dumps(detail, ensure_ascii=False, default=str)) <= _DETAIL_CHARS:
            row["detail"] = detail
        elif detail:  # 太長（by_caller 很多來源）：只留純量欄位
            row["detail"] = {k: v for k, v in detail.items() if not isinstance(v, (dict, list))}
        rows.append(row)
    return rows


def evidence_rows(evidence: list[EvidenceItem]) -> list[dict]:
    return [{"id": f"E{i}", "kind": e.kind, "entity": e.entity, "state": e.state, "confidence": e.confidence,
             "quote": e.quote, "log_ts": e.log_ts} for i, e in enumerate(evidence, 1)]


def graph_rows(ctx: PipelineContext, entities: set[str]) -> dict[str, list]:
    """呼叫圖：同步呼叫邊（caller → callee，callee 出事會拖垮 caller）、訊息佇列邊（producer → consumer）、
    資源邊（服務 → 它依賴的資料庫／快取／外部 API）。ctx.topology 只有服務對服務的邊；資源不是實體，
    只出現在 wait_delta 事實的 detail.target，不補進來的話 LLM 看到的圖在「外部依賴變慢」這類事故裡是空的。"""
    calls = sorted({(a, b) for a, b in ctx.topology if a != b})
    mq = sorted({(e.producer, e.consumer, e.queue or "") for e in ctx.rca_mq if e.producer and e.consumer})
    res = sorted({(f.entity, str(f.detail["target"]), str(f.detail.get("target_kind") or ""))
                  for f in ctx.rca_facts if f.kind == "wait_delta" and f.detail.get("target")
                  and f.detail["target"] not in entities})
    return {"calls": [f"{a} -> {b}" for a, b in calls],
            "async": [f"{p} -> {c}" + (f"（queue {q}）" if q else "") for p, c, q in mq],
            "resources": [f"{a} -> {t}" + (f"（{k}）" if k else "") for a, t, k in res]}


def alarm_rows(incident: Incident) -> list[dict]:
    seen = first_seen(incident)
    return [{"entity": e, "first_alarm": as_utc(ts).isoformat(), "max_score": _round(float(score))}
            for e, (ts, score) in sorted(seen.items(), key=lambda kv: (kv[1][0], kv[0]))]


def build_prompt(incident: Incident, graph: dict[str, list], facts: list[dict], evidence: list[dict],
                 entities: list[str], omitted: int, top_k: int) -> str:
    kinds = sorted({f["kind"] for f in facts})
    return (
        "已知實體（entity 只能從這裡挑）：" + json.dumps(entities, ensure_ascii=False) + "\n\n"
        "呼叫圖（calls：A -> B 表示 A 呼叫 B；async：生產者 -> 消費者；resources：服務 -> 它依賴的資源，資源不是實體）：\n" + json.dumps(graph, ensure_ascii=False, indent=1) + "\n\n"
        "告警先後（事件群裡每個實體第一次被偵測到異常的時間；偵測有延遲，先後只能當參考）：\n"
        + json.dumps(alarm_rows(incident), ensure_ascii=False, indent=1) + "\n\n"
        "事實種類說明：\n" + json.dumps({k: FACT_GLOSSARY.get(k, "") for k in kinds}, ensure_ascii=False, indent=1) + "\n\n"
        f"事實（平台從 trace、日誌、指標算出，只陳述不判斷{f'；另有 {omitted} 筆因長度限制沒有列出' if omitted else ''}）：\n"
        + json.dumps(facts, ensure_ascii=False, default=str) + "\n\n"
        "日誌證據（已驗證 quote 逐字出現在原日誌）：\n" + json.dumps(evidence, ensure_ascii=False) + "\n\n"
        "根因類型（subtype 只能從這裡挑）：" + json.dumps(SUBTYPES, ensure_ascii=False) + "\n\n"
        '請輸出：{"candidates": [{"entity": "已知實體", "confidence": 0 到 1, "subtype": "根因類型", '
        '"resource": "它在等的資源名稱（取自事實的 detail.target／callee／queue，沒有就空字串）", '
        '"reason": "一兩句話的理由", "cites": ["F3", "E1"]}], "note": "資料不足或補充說明，沒有就空字串"}。\n'
        f"candidates 由最可能到最不可能，最多 {top_k} 個；cites 至少引用一筆。"
        "reason 裡的數字必須原樣取自你引用的事實（可以四捨五入），不要自己算新的數字（差值、倍數、百分比）。"
    )


def _numbers_of(row: dict) -> set[float]:
    out: set[float] = set()

    def walk(v: Any) -> None:
        if isinstance(v, bool):
            return
        if isinstance(v, (int, float)):
            out.add(float(v))
        elif isinstance(v, str):
            out.update(float(n) for n in _NUMBER.findall(v))
        elif isinstance(v, dict):
            for x in v.values():
                walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)

    walk({k: v for k, v in row.items() if k != "id"})
    return out


def _number_ok(value: float, allowed: set[float], percent: bool) -> bool:
    """容許四捨五入（10% 或 0.5 以內）。只有後面跟著 % 的數字才可以是比例換成的百分比（0.85 → 85%）：
    不這樣限制的話，任何數字乘 100 都算「有根據」，等於沒驗。"""
    for a in allowed:
        for ref in ((a, a * 100) if percent else (a,)):
            if ref == value or abs(ref - value) <= max(abs(ref) * _TOLERANCE, 0.5):
                return True
    return False


def unsupported_number(reason: str, cited: list[dict]) -> str | None:
    """reason 裡第一個在引用資料中找不到的數字；都找得到回 None。名稱（ord-svc-2）與引用編號（F3）裡的數字不算。"""
    text = _CITE.sub(" ", _IDENT.sub(" ", reason))
    allowed: set[float] = set()
    for row in cited:
        allowed |= _numbers_of(row)
    for m in _NUMBER.finditer(text):
        percent = text[m.end():m.end() + 2].lstrip()[:1] in ("%", "％")
        if not _number_ok(float(m.group()), allowed, percent):
            return m.group()
    return None


def _resources(cited: list[dict]) -> set[str]:
    out: set[str] = set()
    for row in cited:
        detail = row.get("detail") or {}
        out.update(str(detail[k]) for k in ("target", "callee", "queue", "producer") if detail.get(k))
    return out


def validate_candidates(raw: list[Any], facts: list[dict], evidence: list[dict],
                        entities: set[str]) -> tuple[list[dict], list[dict]]:
    """回傳 (通過的, 被拒的)。通過的每項是整理好的 dict；被拒的是 {"item": 原樣, "reason": 原因}。同一個實體只留第一個。"""
    by_id = {r["id"]: r for r in [*facts, *evidence]}
    accepted: list[dict] = []
    rejected: list[dict] = []
    seen: set[str] = set()

    def reject(item: Any, why: str) -> None:
        rejected.append({"item": item, "reason": why})

    for item in raw:
        if not isinstance(item, dict):
            reject(item, "不是 JSON 物件")
            continue
        entity, conf, subtype = item.get("entity"), item.get("confidence"), item.get("subtype") or ""
        reason, cites = item.get("reason"), item.get("cites")
        if not isinstance(entity, str) or entity not in entities:
            reject(item, f"entity 不是已知實體：{entity!r}")
            continue
        if isinstance(conf, bool) or not isinstance(conf, (int, float)) or not 0.0 <= conf <= 1.0:
            reject(item, f"confidence 必須是 0～1 的數字：{conf!r}")
            continue
        if subtype and subtype not in SUBTYPES:
            reject(item, f"subtype 不在詞表內：{subtype!r}")
            continue
        if not isinstance(reason, str) or not reason.strip():
            reject(item, "缺少 reason")
            continue
        if not isinstance(cites, list) or not cites or not all(isinstance(c, str) for c in cites):
            reject(item, "cites 必須是至少一筆的編號清單")
            continue
        missing = [c for c in cites if c not in by_id]
        if missing:
            reject(item, f"引用了不存在的編號：{missing}")
            continue
        cited = [by_id[c] for c in dict.fromkeys(cites)]
        bad = unsupported_number(reason, cited)
        if bad is not None:
            reject(item, f"reason 裡的數字 {bad} 在引用的事實裡找不到")
            continue
        if entity in seen:
            continue
        seen.add(entity)
        resource = item.get("resource") if isinstance(item.get("resource"), str) else ""
        dropped = ""
        if resource and resource not in _resources(cited):
            dropped, resource = resource, ""
        accepted.append({"entity": entity, "confidence": float(conf), "subtype": subtype, "reason": reason.strip(),
                         "cites": list(dict.fromkeys(cites)), "resource": resource, "resource_dropped": dropped})
    return accepted, rejected


def _pair(detail: dict, before: str, during: str, cast=lambda v: round(float(v), 1)) -> dict:
    return {"before": cast(detail.get(before) or 0), "during": cast(detail.get(during) or 0)}


def resource_numbers(entity: str, resource: str, facts: list[dict]) -> dict:
    """LLM 指名了資源時，把那個資源的數字從事實原樣抄進 attribution（鍵名跟舊判斷層相同：報告的「建議處置」
    靠 kind／share／failures 配 Runbook）。指名哪個資源是 LLM 的判斷；數字只抄不算，除了 share：
    等待增量 ÷ 自身耗時增量，兩個都是同一個實體的事實值。找不到對應的 wait_delta（資源取自 callee／queue）就回空。"""
    mine = [f for f in facts if f["entity"] == entity]
    wait = next((f for f in mine if f["kind"] == "wait_delta" and (f.get("detail") or {}).get("target") == resource), None)
    if wait is None:
        return {}
    d = wait["detail"]
    kind = str(d.get("target_kind") or "").split("/")
    out: dict[str, Any] = {"kind": kind[0], "subtype": kind[1] if len(kind) > 1 else "",
                           "wait_ms": _pair(d, "before", "during"),
                           "failures": _pair(d, "failures_before", "failures_during", int)}
    own = next((f for f in mine if f["kind"] == "self_time_delta"), None)
    if own is not None:
        out["self_ms"] = _pair(own.get("detail") or {}, "app_before", "app_during")
        total = float(own["value"])
        out["share"] = round(min(max(float(wait["value"]) / total, 0.0), 1.0), 3) if total > 0 else 0.0
    return out


@OperatorRegistry.register("rca.root_cause.llm")
class LLMRootCause(RootCauseOperator):
    """呼叫圖＋事實＋日誌證據 → LLM 判斷根因（見模組說明）。
    params：provider（不寫看環境變數 AIOPS_LLM_PROVIDER；沒有 noop）、model、base_url、json_mode（預設 true）、
    timeout_seconds（預設 60：prompt 比證據抽取長）、retries（預設 0）、max_facts（最多送幾筆事實，預設 150；
    依證據層的固定排序取前面幾筆，超過的只告訴 LLM 筆數）、top_k（最多幾個候選，預設 5）。
    """

    def __init__(self, provider: str | None = None, model: str | None = None, base_url: str | None = None,
                 json_mode: bool = True, timeout_seconds: float = 60.0, retries: int = 0, max_facts: int = 150,
                 top_k: int = 5):
        check_provider(provider)
        self.provider, self.model, self.base_url, self.json_mode = provider, model, base_url, bool(json_mode)
        self.timeout = positive("timeout_seconds", timeout_seconds)
        self.retries = whole("retries", retries, minimum=0)
        if self.retries > MAX_RETRIES:
            raise ValueError(f"retries 不能大於 {MAX_RETRIES}，不是 {self.retries}")
        self.max_facts = whole("max_facts", max_facts)
        self.top_k = whole("top_k", top_k)

    def rank(self, incident: Incident, ctx: PipelineContext) -> list[RootCandidate]:
        if ctx.rca_status == "undetermined":
            ctx.rca_root_note = f"無法判定：{ctx.rca_undetermined_reason or 'LLM 證據抽取未完成'}，不呼叫 LLM 判斷"
            return []
        if not ctx.rca_facts and not ctx.rca_evidence:
            ctx.rca_root_note = "證據不足，不指認根因：沒有任何可用的事實或日誌證據（不呼叫 LLM）"
            return []
        facts, evidence = fact_rows(ctx.rca_facts, self.max_facts), evidence_rows(ctx.rca_evidence)
        entities = known_entities(ctx) | {f.entity for f in ctx.rca_facts}
        graph = graph_rows(ctx, entities)
        prompt = build_prompt(incident, graph, facts, evidence, sorted(entities),
                              max(0, len(ctx.rca_facts) - len(facts)), self.top_k)
        ctx.log(f"root_cause.llm：送入呼叫圖 {sum(len(v) for v in graph.values())} 條邊、事實 {len(facts)}／{len(ctx.rca_facts)} 筆、"
                f"日誌證據 {len(evidence)} 筆，prompt {len(prompt)} 字")
        try:
            text = complete_with_policy(resolve_provider(self.provider), self.model, self.base_url, self.json_mode,
                                        SYSTEM, prompt, self.timeout, self.retries)
        except CallFailure as f:
            return self._undetermined(ctx, f.code, f.detail)
        try:
            reply = parse_json_reply(text)
        except LLMError as e:
            return self._undetermined(ctx, LLM_BAD_JSON, str(e))
        raw = reply.get("candidates")
        if not isinstance(raw, list):
            return self._undetermined(ctx, LLM_BAD_SCHEMA, "LLM 回應缺少 candidates 陣列")
        note = reply.get("note") if isinstance(reply.get("note"), str) else ""
        accepted, rejected = validate_candidates(raw, facts, evidence, entities)
        ctx.log(f"root_cause.llm：LLM 回 {len(raw)} 個候選，採用 {len(accepted)}、拒絕 {len(rejected)}")
        for r in rejected:
            ctx.log(f"root_cause.llm 拒絕：{r['reason']}｜{json.dumps(r['item'], ensure_ascii=False)[:300]}")
        if rejected:
            logger.warning("RootCandidateRejected incidents=%s rejected=%d accepted=%d",
                           [i.uid for i in ctx.incidents], len(rejected), len(accepted))
        if raw and not accepted:
            return self._undetermined(ctx, LLM_ALL_REJECTED,
                                      f"LLM 給了 {len(raw)} 個候選，驗證後全部被拒（{rejected[0]['reason']}）")
        if not accepted:
            ctx.rca_root_note = f"LLM 判斷證據不足，不指認根因：{note.strip() or 'LLM 沒有說明原因'}"[:400]
            return []
        accepted.sort(key=lambda c: -c["confidence"])  # 穩定排序：信心相同時維持 LLM 給的順序
        return [RootCandidate(
            c["entity"], round(c["confidence"], 4),
            f"{c['reason']}（LLM 判斷，自評信心 {c['confidence']:.0%}；引用 {'、'.join(c['cites'])}）",
            c["resource"],
            # cause 是 LLM 選的根因類型；subtype 這個鍵留給資源的種類（http、postgresql），跟舊判斷層同義
            {"judge": "llm", "cause": c["subtype"], "cites": c["cites"],
             **({"resource": c["resource"], **resource_numbers(c["entity"], c["resource"], facts)} if c["resource"] else {}),
             **({"resource_dropped": c["resource_dropped"]} if c["resource_dropped"] else {})},
        ) for c in accepted[:self.top_k]]

    @staticmethod
    def _undetermined(ctx: PipelineContext, code: str, detail: str) -> list[RootCandidate]:
        ctx.rca_status = "undetermined"
        ctx.rca_undetermined_reason = f"{code}：{detail}"
        ctx.rca_root_note = f"無法判定：{ctx.rca_undetermined_reason}"
        ctx.log(f"root_cause.llm：無法判定（{ctx.rca_undetermined_reason}）")
        logger.warning("RcaUndetermined incidents=%s %s", [i.uid for i in ctx.incidents], ctx.rca_undetermined_reason)
        return []
