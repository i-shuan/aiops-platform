"""認知型 RCA 的證據抽取（docs/COGNITIVE_RCA_DESIGN.md 第 5 節「LLM 抽取」）：日誌 → list[EvidenceItem]。

LLM 在這條 RCA 路徑是**必要元件**，所以這個算子沒有 noop 模式：
  - 沒有日誌                 → 不是失敗：rca_evidence = []，rca_status 維持原樣（ok）
  - 沒設定供應商、連不上、逾時、回傳不是 JSON、格式不對、驗證後全部被拒
                             → rca_status = "undetermined"，rca_undetermined_reason = "<代碼>：<人看得懂的說明>"
報告照出，不整份失敗，也不退回舊規則硬猜。逾時、重試次數是節點 params（預設逾時後不重試）。

送進 LLM 的不是整包原始日誌，而是 stages/rca/logs.py 整理好的「事件期間 vs 基準期間」樣板統計，
每個樣板附幾筆代表性原文（編號 L1、L2…）。LLM 回的每一項都要過驗證（validate_items），不過就丟掉並記原因：
  1. kind 在 EVIDENCE_KINDS 封閉詞表內
  2. entity 是事件群、拓撲或這次 trace 裡出現過的已知實體
  3. quote 逐字出現在送入的某一筆日誌原文裡（不是就當幻覺）
  4. state 是 present／absent；confidence 是 0～1 的數字
log_ts 不信 LLM 給的，一律用 quote 實際所在那筆日誌的時間。
"""
from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, replace
from typing import Any

from aiops.engine.context import LogRecord, PipelineContext
from aiops.engine.operator import BaseOperator
from aiops.engine.params import positive, whole
from aiops.engine.rca_types import EVIDENCE_KINDS, EvidenceItem
from aiops.engine.registry import OperatorRegistry
from aiops.engine.graph import EdgeKind
from aiops.engine.types import as_utc
from aiops.stages.enrichment import registry
from aiops.stages.enrichment.base import BaseLLM, LLMError, parse_json_reply
from aiops.stages.enrichment.masking import MASK_FAILED, Masker, MaskError, MaskPolicy, rca_keep_names, send
from aiops.stages.enrichment.providers import build_llm
from aiops.stages.rca.logs import rank_patterns, template_of

logger = logging.getLogger("aiops.enrichment")

# 失敗狀態碼（rca_undetermined_reason 以「代碼：說明」開頭，程式用代碼判斷，人看說明）
LLM_NOT_CONFIGURED = "llm_not_configured"   # 沒設定供應商，或供應商設定不全（缺模型、缺金鑰）
LLM_ERROR = "llm_error"                     # 連不上、HTTP 錯誤、回應缺欄位（供應商丟 LLMError）
LLM_TIMEOUT = "llm_timeout"                 # 超過節點 timeout_seconds 沒回應
LLM_BAD_JSON = "llm_bad_json"               # 回應不是 JSON 物件
LLM_BAD_SCHEMA = "llm_bad_schema"           # 是 JSON，但沒有 evidence 陣列
LLM_ALL_REJECTED = "llm_all_rejected"       # 有回項目，但驗證後一項都不剩
NO_EVIDENCE = "no_evidence"                 # 根因判斷沒有任何事實或日誌證據可用（沒有 trace／日誌／時序），沒有呼叫 LLM
# MASK_FAILED（masking.py）：送出前遮罩敏感資訊出錯，寧可不送也不放行原文
FAILURE_CODES = (LLM_NOT_CONFIGURED, LLM_ERROR, LLM_TIMEOUT, LLM_BAD_JSON, LLM_BAD_SCHEMA, LLM_ALL_REJECTED, MASK_FAILED)

MAX_RETRIES = 3
PROVIDER_ENV = "AIOPS_LLM_PROVIDER"  # 節點沒寫 provider 時的預設；兩邊都沒有 = llm_not_configured

SYSTEM = (
    "你是 SRE 日誌證據抽取器。你只做一件事：從使用者給的日誌原文裡，找出符合指定證據種類的現象。"
    "不要判斷根因、不要推測日誌裡沒寫的事。每一項證據都必須附上日誌原文中逐字複製的片段（quote）。"
    "只輸出 JSON，不要其他文字。"
)


MIN_QUOTE_CHARS = 12  # 引文最短字數（見 validate_items）


class _Timeout(Exception):
    pass


# ── 供應商與呼叫（rca_narrator 也用這兩個）──


def resolve_provider(provider: str | None) -> str:
    """節點 params 優先，其次環境變數 AIOPS_LLM_PROVIDER；都沒有回空字串（= 沒設定）。"""
    return (provider or os.environ.get(PROVIDER_ENV, "")).strip().lower()


def check_provider(provider: str | None) -> None:
    """載入期檢查：寫了就必須是已登記的供應商。noop 不收：這條路徑沒有 noop，不設定就是 undetermined。"""
    if provider is None:
        return
    if not isinstance(provider, str) or provider.strip().lower() not in registry.names():
        raise ValueError(f"provider 只能是 {', '.join(registry.names())}，不是 {provider!r}")
    if provider.strip().lower() == "noop":
        raise ValueError("RCA 路徑的 LLM 是必要元件，沒有 noop 模式；不設定 provider 會讓報告標成 undetermined")


def call_llm(llm: BaseLLM, system: str, prompt: str, timeout: float, masker: Masker) -> str:
    """在背景執行緒經 masking.send（先遮罩再 complete）呼叫，最多等 timeout 秒；逾時丟 _Timeout，
    遮罩失敗丟 MaskError（沒有送出），complete 自己丟的例外原樣往上丟。
    逾時後不等那個執行緒（shutdown(wait=False)）：真實供應商的 HTTP client 自己有逾時，會把連線收掉。
    每次呼叫新建 executor，不留模組層級狀態（CLAUDE.md §100.3）。"""
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="aiops-llm-call")
    try:
        return pool.submit(send, llm, system, prompt, masker).result(timeout=timeout)
    except FutureTimeout as e:
        raise _Timeout() from e
    except (LLMError, MaskError):
        raise
    except Exception as e:  # noqa: BLE001  供應商實作自己的 bug（回應形狀跟預期不同）也算「呼叫失敗」，不准讓整份報告失敗
        raise LLMError(f"供應商回應無法處理（{type(e).__name__}：{e}）") from e
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


@dataclass
class CallFailure(Exception):
    code: str
    detail: str

    def reason(self) -> str:
        return f"{self.code}：{self.detail}"


def complete_with_policy(provider: str, model: str | None, base_url: str | None, json_mode: bool,
                         system: str, prompt: str, timeout: float, retries: int, *, masker: Masker) -> str:
    """建 client → 遮罩後呼叫（逾時、重試依節點設定）。失敗一律丟 CallFailure(代碼, 說明)。
    重試只針對 LLMError 與逾時（連線問題）；設定錯、遮罩失敗不重試。最多呼叫 1 + retries 次，不無限重試。
    masker 是必填：呼叫端應該已經用同一個 masker 遮過結構化輸入，這裡（masking.send）對整份 prompt 再遮一次防漏。"""
    if not provider:
        raise CallFailure(LLM_NOT_CONFIGURED, f"沒有設定 LLM 供應商（節點 provider 或環境變數 {PROVIDER_ENV}）")
    try:
        llm = build_llm(provider, model, base_url, json_mode=json_mode)
    except LLMError as e:
        raise CallFailure(LLM_NOT_CONFIGURED, f"LLM 供應商設定不全：{e}") from e
    if llm is None:  # noop 之類不連外的供應商：這條路徑視同沒設定
        raise CallFailure(LLM_NOT_CONFIGURED, f"provider={provider} 不會呼叫任何模型")
    last: CallFailure | None = None
    for _ in range(1 + retries):
        try:
            return call_llm(llm, system, prompt, timeout, masker)
        except MaskError as e:
            raise CallFailure(MASK_FAILED, f"{e}（敏感資訊遮罩失敗，沒有送出）") from e
        except _Timeout:
            last = CallFailure(LLM_TIMEOUT, f"LLM 超過 {timeout:g} 秒沒有回應（共試 {1 + retries} 次）")
        except LLMError as e:
            last = CallFailure(LLM_ERROR, f"LLM 呼叫失敗：{e}（共試 {1 + retries} 次）")
    assert last is not None
    raise last


# ── 輸入整理 ──


@dataclass(frozen=True)
class SentLine:
    """送進 LLM 的一筆日誌原文。quote 只能從這裡找。"""

    id: str
    service: str
    ts: str
    text: str


def build_input(during: list[LogRecord], baseline: list[LogRecord], minutes: tuple[float, float],
                max_patterns: int, examples_per_pattern: int, max_line_chars: int) -> tuple[list[dict], list[SentLine]]:
    """依暴增倍率排好的樣板（最多 max_patterns 個），每個樣板附最多 examples_per_pattern 筆不重複的原文。
    單筆事件期間的日誌也要列（死結常常只有一兩筆），所以 min_count=1。"""
    patterns = rank_patterns(during, baseline, minutes, min_count=1)[:max_patterns]
    wanted = {(p["service"], p["error_type"], p["template"]): i for i, p in enumerate(patterns)}
    examples: dict[int, list[LogRecord]] = {i: [] for i in range(len(patterns))}
    for r in sorted(during, key=lambda r: as_utc(r.ts)):
        i = wanted.get((r.service, r.error_type, template_of(r.message)))
        if i is None or len(examples[i]) >= examples_per_pattern:
            continue
        if all(e.message != r.message for e in examples[i]):
            examples[i].append(r)
    lines: list[SentLine] = []
    out: list[dict] = []
    for i, p in enumerate(patterns):
        ids = []
        for r in examples[i]:
            line = SentLine(f"L{len(lines) + 1}", r.service, as_utc(r.ts).isoformat(), r.message[:max_line_chars])
            lines.append(line)
            ids.append(line.id)
        out.append({"service": p["service"], "error_type": p["error_type"] or None, "template": p["template"],
                    "count_during": p["during"], "count_baseline": p["baseline"], "ratio": round(p["ratio"], 1),
                    "new": p["new"], "lines": ids})
    return out, lines


def build_prompt(patterns: list[dict], lines: list[SentLine], entities: list[str]) -> str:
    return (
        "已知實體（entity 只能從這裡挑）：" + json.dumps(entities, ensure_ascii=False) + "\n"
        "證據種類（kind 只能從這裡挑）：" + json.dumps(sorted(EVIDENCE_KINDS)) + "\n\n"
        "事件期間的錯誤日誌樣板（count_baseline 是事件前正常期間的筆數，ratio 是每分鐘頻率的暴增倍率，new = 正常期間沒出現過）：\n"
        + json.dumps(patterns, ensure_ascii=False, indent=1)
        + "\n\n日誌原文：\n"
        + json.dumps([{"id": l.id, "service": l.service, "ts": l.ts, "text": l.text} for l in lines], ensure_ascii=False, indent=1)
        + '\n\n請輸出：{"evidence": [{"kind": "詞表內的種類", "entity": "已知實體", "state": "present 或 absent", '
        '"confidence": 0 到 1, "line": "引用的日誌 id", "quote": "從該筆日誌原文逐字複製的片段"}]}。'
        "沒有符合的現象就回 {\"evidence\": []}。"
    )


def known_entities(ctx: PipelineContext) -> set[str]:
    """事件群的實體 ∪ 拓撲（靜態＋這次 trace 還原的）∪ trace 裡出現過的服務。"""
    names: set[str] = set()
    for inc in ctx.incidents:
        names.update(inc.entities)
    graph = ctx.graph
    names.update(graph.nodes(EdgeKind.CALL))
    names.update(n for edge in graph.propagation() for n in edge)
    names.update(s.service for s in ctx.rca_spans if getattr(s, "service", ""))
    names.discard("")
    return names


# ── 驗證 ──


def validate_items(raw: list[Any], lines: list[SentLine], entities: set[str]) -> tuple[list[EvidenceItem], list[dict]]:
    """回傳 (通過的, 被拒的)。被拒的每項是 {"item": 原樣, "reason": 原因}。重複的項目（kind、entity、state、quote 都一樣）只留一筆。"""
    by_id = {l.id: l for l in lines}
    accepted: list[EvidenceItem] = []
    rejected: list[dict] = []
    seen: set[tuple] = set()

    def reject(item: Any, reason: str) -> None:
        rejected.append({"item": item, "reason": reason})

    for item in raw:
        if not isinstance(item, dict):
            reject(item, "不是 JSON 物件")
            continue
        kind, entity, quote = item.get("kind"), item.get("entity"), item.get("quote")
        state, conf = item.get("state", "present"), item.get("confidence", 1.0)
        if not isinstance(kind, str) or kind not in EVIDENCE_KINDS:
            reject(item, f"kind 不在詞表內：{kind!r}")
            continue
        if not isinstance(entity, str) or entity not in entities:
            reject(item, f"entity 不是已知實體：{entity!r}")
            continue
        if state not in ("present", "absent"):
            reject(item, f"state 只能是 present／absent：{state!r}")
            continue
        if isinstance(conf, bool) or not isinstance(conf, (int, float)) or not 0.0 <= conf <= 1.0:
            reject(item, f"confidence 必須是 0～1 的數字：{conf!r}")
            continue
        if not isinstance(quote, str) or not quote.strip():
            reject(item, "缺少 quote")
            continue
        quote = quote.strip()
        line_id = item.get("line")
        if line_id is not None and (not isinstance(line_id, str) or line_id not in by_id):
            reject(item, f"line 不是送入的日誌編號：{line_id!r}")
            continue
        # 引文只在「entity 自己的日誌」裡找：A 服務的日誌提到 B（例如記下來的 user-agent、下游回傳的字串）不是 B 的證據，
        # 那段文字是誰都可以寫進去的。先看 LLM 指的那一筆，再看同一個服務的其他筆；log_ts 取實際找到的那筆
        own = [l for l in lines if l.service == entity]
        cited = by_id.get(line_id) if isinstance(line_id, str) else None
        if cited is not None and cited.service != entity:
            reject(item, f"引用的日誌 {cited.id} 是 {cited.service} 的，不能當 {entity} 的證據")
            continue
        hit = cited if cited is not None and quote in cited.text else next((l for l in own if quote in l.text), None)
        if hit is None:
            elsewhere = next((l for l in lines if quote in l.text), None)
            reject(item, f"quote 只出現在 {elsewhere.service} 的日誌裡，不能當 {entity} 的證據" if elsewhere is not None
                   else "quote 沒有逐字出現在送入的日誌裡（視為幻覺）")
            continue
        # 太短的片段（一個字母、一個常見字）幾乎每筆日誌都有，「逐字出現」沒有任何約束力；整筆原文本來就短的不受此限
        if len(quote) < MIN_QUOTE_CHARS and quote != hit.text.strip():
            reject(item, f"quote 太短（{len(quote)} 字，至少 {MIN_QUOTE_CHARS} 字或整筆原文）：{quote!r}")
            continue
        key = (kind, entity, state, quote)
        if key in seen:
            continue
        seen.add(key)
        accepted.append(EvidenceItem(kind=kind, entity=entity, state=state, confidence=float(conf), quote=quote, log_ts=hit.ts))
    return accepted, rejected


# ── 算子 ──


@OperatorRegistry.register("enrichment.extract_evidence")
class EvidenceExtractor(BaseOperator):
    """日誌 → LLM → 驗證後的 EvidenceItem，放 ctx.rca_evidence。失敗把 rca_status 標成 undetermined（見模組說明），不丟例外。
    params：provider（不寫就看環境變數 AIOPS_LLM_PROVIDER；沒有 noop）、model、base_url、json_mode（預設 true）、
    timeout_seconds（預設 30）、retries（預設 0 = 逾時或連線失敗後不重試）、max_patterns（預設 20）、
    examples_per_pattern（預設 2）、max_line_chars（每筆原文最多送幾個字，預設 400）、
    mask_hostnames（預設 false：主機名是 RCA 線索，理由見 masking.py）。

    敏感資訊遮罩：日誌原文在整理成樣板與代表性原文**之前**就先遮（masking.Masker），所以 LLM 看到的、quote 逐字比對的、
    存進 ctx.rca_evidence（報告與資料庫）的都是遮過的版本；未遮的原文只留在 ctx.rca_logs（本來就在，沒有多流到新地方）。
    """

    scope = "batch"

    def __init__(self, provider: str | None = None, model: str | None = None, base_url: str | None = None,
                 json_mode: bool = True, timeout_seconds: float = 30.0, retries: int = 0, max_patterns: int = 20,
                 examples_per_pattern: int = 2, max_line_chars: int = 400, mask_hostnames: bool = False):
        check_provider(provider)
        if not isinstance(mask_hostnames, bool):
            raise ValueError(f"mask_hostnames 必須是 true／false，不是 {mask_hostnames!r}")
        self.mask = MaskPolicy(mask_hostnames=mask_hostnames)
        self.provider, self.model, self.base_url, self.json_mode = provider, model, base_url, bool(json_mode)
        self.timeout = positive("timeout_seconds", timeout_seconds)
        self.retries = whole("retries", retries, minimum=0)
        if self.retries > MAX_RETRIES:  # 上限：不准把「重試」寫成實質上的無限重試
            raise ValueError(f"retries 不能大於 {MAX_RETRIES}，不是 {self.retries}")
        self.max_patterns = whole("max_patterns", max_patterns)
        self.examples_per_pattern = whole("examples_per_pattern", examples_per_pattern)
        self.max_line_chars = whole("max_line_chars", max_line_chars, minimum=40)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        ctx.rca_evidence = []
        if not ctx.rca_logs:
            ctx.log("extract_evidence：沒有事件期間的錯誤日誌，不呼叫 LLM（rca_evidence = []，不算失敗）")
            return ctx
        entities = known_entities(ctx)
        masker = Masker(self.mask, keep=rca_keep_names(ctx) | entities)
        try:
            during = [replace(r, message=masker.text(r.message)) for r in ctx.rca_logs]
            baseline = [replace(r, message=masker.text(r.message)) for r in ctx.rca_logs_baseline]
        except Exception as e:  # noqa: BLE001  遮罩失敗：寧可不送，不放行原文
            logger.exception("extract_evidence 遮罩失敗")
            return self._undetermined(ctx, MASK_FAILED, f"日誌敏感資訊遮罩失敗（{type(e).__name__}），沒有送出")
        patterns, lines = build_input(during, baseline, ctx.rca_log_window_minutes,
                                      self.max_patterns, self.examples_per_pattern, self.max_line_chars)
        prompt = build_prompt(patterns, lines, sorted(entities))
        ctx.log(f"extract_evidence：送出前遮罩（{masker.summary()}）")
        try:
            text = complete_with_policy(resolve_provider(self.provider), self.model, self.base_url, self.json_mode,
                                        SYSTEM, prompt, self.timeout, self.retries, masker=masker)
        except CallFailure as f:
            return self._undetermined(ctx, f.code, f.detail)
        try:
            reply = parse_json_reply(text)
        except LLMError as e:
            return self._undetermined(ctx, LLM_BAD_JSON, str(e))
        raw = reply.get("evidence")
        if not isinstance(raw, list):
            return self._undetermined(ctx, LLM_BAD_SCHEMA, "LLM 回應缺少 evidence 陣列")
        try:
            accepted, rejected = validate_items(raw, lines, entities)
        except Exception as e:  # noqa: BLE001  欄位型別怪到驗證自己出錯：照「格式不合」處理，報告照出
            logger.exception("extract_evidence 驗證例外")
            return self._undetermined(ctx, LLM_BAD_SCHEMA, f"LLM 回應的欄位型別無法驗證（{type(e).__name__}）")
        ctx.log(f"extract_evidence：送入 {len(patterns)} 個樣板、{len(lines)} 筆原文；LLM 回 {len(raw)} 項，"
                f"採用 {len(accepted)}、拒絕 {len(rejected)}")
        for r in rejected:
            ctx.log(f"extract_evidence 拒絕：{r['reason']}｜{json.dumps(r['item'], ensure_ascii=False)[:200]}")
        if rejected:
            logger.warning("EvidenceRejected incidents=%s rejected=%d accepted=%d",
                           [i.uid for i in ctx.incidents], len(rejected), len(accepted))
        if raw and not accepted:
            return self._undetermined(ctx, LLM_ALL_REJECTED, f"LLM 回了 {len(raw)} 項證據，驗證後全部被拒（{rejected[0]['reason']}）")
        ctx.rca_evidence = accepted
        return ctx

    @staticmethod
    def _undetermined(ctx: PipelineContext, code: str, detail: str) -> PipelineContext:
        ctx.rca_evidence = []
        ctx.rca_status = "undetermined"
        ctx.rca_undetermined_reason = f"{code}：{detail}"
        ctx.log(f"extract_evidence：無法判定（{ctx.rca_undetermined_reason}）")
        logger.warning("RcaUndetermined incidents=%s %s", [i.uid for i in ctx.incidents], ctx.rca_undetermined_reason)
        return ctx
