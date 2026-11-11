"""混沌用的「腳本化假 LLM」：供應商 chaos_scripted，不連網、不是真模型。

!!! 誠實限制（請先讀）!!!
這個假 LLM 只用關鍵字規則從日誌文字產生「符合合約形狀」的 JSON。它測的是**平台怎麼處理 LLM 的輸出**：
接得上嗎、驗證（詞表、實體白名單、quote 逐字比對）有沒有擋、LLM 掛掉／逾時／亂回時報告是不是照出且標成 undetermined。
它**不代表真模型的抽取品質**。規則抓得到的句子真模型不一定抓得到，反過來也一樣；
真模型的品質要另外用真實供應商與錄製回應驗，**不能拿混沌在這個假 LLM 下的通過當成「LLM 抽取有效」的證據**。

放在 scripts/chaos/ 而不是 aiops/：CLAUDE.md §100.2（生產程式不准有測試特化分支），原本的合約文件已隨 cognitive 引擎移除
也寫明假供應商由測試或混沌腳本自己用 registry.register 登記。

模式（用 YAML／harness 傳進來的 model 字串選，factory 介面只有 (model, base_url, api_key)）：
- faithful      照規則抽：日誌含 CircuitBreaker '...' is OPEN → circuit_breaker_open、Pause Full (Allocation Failure) → gc_pause、
                deadlock detected → deadlock、OutOfMemory → oom（另有連線池耗盡一條）。quote 是日誌裡被規則命中的那一段，逐字取自輸入。
- down          丟 LLMError（模擬連不上）
- slow[:秒數]   先睡（預設 35 秒，超過一般 30 秒逾時），再回 faithful 的結果；平台若沒有自己的逾時，就會「等到了」而判成 ok，這正是要抓的
- garbage       回截斷的壞 JSON
- hallucinate   回詞表內、實體已知、但 quote 根本不在日誌裡的項目（測 quote 逐字驗證）
- judge_faithful     給根因判斷節點（rca.root_cause.llm）用：從 prompt 的「已驗證日誌證據」挑第一筆屬於已知實體的，
                     指認那個實體、cites 引用它、reason 不帶任何數字。測的是「判斷節點接得上、驗證不會誤殺一個有根據的答案」，
                     不是判斷品質（它只會照抄證據的實體，不懂呼叫圖）。
- judge_hallucinate  給根因判斷節點用：指認一個不在已知實體裡的服務（測實體白名單；驗證要把它全部拒絕 -> undetermined）

JSON 形狀（合約 §3 EvidenceItem）：
    {"evidence": [{"kind": "circuit_breaker_open", "entity": "ord-svc", "state": "present",
                   "confidence": 0.95, "quote": "<逐字原文>", "log_ts": "2026-11-09T12:03:00.000Z"}]}

日誌怎麼從 prompt 裡找：抽取算子（aiops/stages/enrichment/evidence_extractor.py）還沒寫好，prompt 格式未定，所以兩種都吃：
1. prompt 裡有 JSON（整段或其中一段）：走訪所有物件，帶 entity／service／service.name 的物件底下的字串都當日誌文字
2. 純文字：逐行看，實體從 `service=xxx`、`entity: xxx`、`[xxx]` 開頭抓
找不到實體時 entity 留空字串（平台的實體白名單應該會擋掉）。
"""
from __future__ import annotations

import json
import re
import time

from aiops.stages.enrichment.base import BaseLLM, LLMError
from aiops.stages.enrichment.registry import register

NAME = "chaos_scripted"
MODES = ("faithful", "down", "slow", "garbage", "hallucinate", "judge_faithful", "judge_hallucinate")
SLOW_DEFAULT_S = 35.0

# (證據 kind, 規則, 自評信心)。kind 必須在 aiops.engine.rca_types.EVIDENCE_KINDS
RULES: list[tuple[str, re.Pattern, float]] = [
    ("circuit_breaker_open", re.compile(r"CircuitBreaker '[^']+' is OPEN"), 0.95),
    ("gc_pause", re.compile(r"Pause Full \(Allocation Failure\)"), 0.9),
    ("deadlock", re.compile(r"deadlock detected", re.IGNORECASE), 0.9),
    ("oom", re.compile(r"OutOfMemory\w*"), 0.9),
    ("connection_pool_exhausted", re.compile(r"(?:connection|thread|worker) pool exhausted", re.IGNORECASE), 0.8),
]

HALLUCINATED_QUOTE = "java.lang.OutOfMemoryError: Java heap space [chaos_scripted 編造，不在任何日誌裡]"

_ENTITY_KEYS = ("entity", "service", "service.name", "service_name", "svc")
_TS_KEYS = ("log_ts", "ts", "@timestamp", "timestamp", "time")
_LINE_ENTITY = [re.compile(r"service(?:\.name)?\s*[=:]\s*\"?([A-Za-z0-9_.:\-]+)"),
                re.compile(r"entity\s*[=:]\s*\"?([A-Za-z0-9_.:\-]+)"),
                re.compile(r"^\s*[-*]?\s*\[([A-Za-z0-9_.:\-]+)\]")]
_LINE_TS = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z?")

# 每次呼叫留一筆（模式、system 前 120 字、prompt 長度、回覆前 300 字）：場景用來確認假 LLM 真的被叫到，
# 否則「LLM 全掛也照出報告」可能只是因為 LLM 節點根本沒接上。只在 scripts/ 裡，不碰 aiops/ 的狀態
CALLS: list[dict] = []


def _entity_of(obj: dict, inherited: str) -> str:
    for k in _ENTITY_KEYS:
        v = obj.get(k)
        if isinstance(v, str) and v:
            return v
        if isinstance(v, dict) and isinstance(v.get("name"), str):  # {"service": {"name": ...}}（ECS 形狀）
            return v["name"]
    return inherited


def _ts_of(obj: dict, inherited: str) -> str:
    for k in _TS_KEYS:
        if isinstance(obj.get(k), str):
            return obj[k]
    return inherited


def _walk(node, entity: str, ts: str, out: list[tuple[str, str, str]]) -> None:
    if isinstance(node, dict):
        entity, ts = _entity_of(node, entity), _ts_of(node, ts)
        for v in node.values():
            _walk(v, entity, ts, out)
    elif isinstance(node, list):
        for v in node:
            _walk(v, entity, ts, out)
    elif isinstance(node, str):
        out.append((entity, ts, node))


def _json_chunks(text: str):
    """prompt 整段是 JSON，或夾在說明文字裡的 JSON 區塊（從每個 { / [ 試著解）。"""
    dec = json.JSONDecoder()
    i = 0
    while i < len(text):
        if text[i] in "{[":
            try:
                obj, end = dec.raw_decode(text, i)
                yield obj
                i = end
                continue
            except json.JSONDecodeError:
                pass
        i += 1


def log_texts(prompt: str) -> list[tuple[str, str, str]]:
    """(entity, log_ts, 文字)。JSON 與逐行兩種都收，交給規則去比。"""
    out: list[tuple[str, str, str]] = []
    for chunk in _json_chunks(prompt):
        _walk(chunk, "", "", out)
    for line in prompt.splitlines():
        ent = next((m.group(1) for p in _LINE_ENTITY if (m := p.search(line))), "")
        ts = m.group(0) if (m := _LINE_TS.search(line)) else ""
        out.append((ent, ts, line))
    return out


def extract(prompt: str) -> list[dict]:
    """faithful：每個 (kind, entity) 只留第一筆（帶得出實體的優先）。quote 是規則命中的那一段，逐字取自日誌文字。"""
    found: dict[tuple[str, str], dict] = {}
    for entity, ts, text in log_texts(prompt):
        for kind, pat, conf in RULES:
            m = pat.search(text)
            if not m:
                continue
            item = {"kind": kind, "entity": entity, "state": "present", "confidence": conf, "quote": m.group(0), "log_ts": ts}
            key = (kind, entity)
            if key not in found:
                found[key] = item
    # 同一個 kind 已經有帶實體的，就丟掉實體空白的那筆（逐行掃描時 JSON 的行會再命中一次，但抓不到實體）
    with_entity = {k for k, e in found if e}
    return [v for (k, e), v in found.items() if e or k not in with_entity]


def hallucinate(prompt: str) -> list[dict]:
    """詞表內的 kind、prompt 裡出現過的實體，但 quote 是編的：只有 quote 逐字比對能擋。"""
    ents = [e for e, _, _ in log_texts(prompt) if e]
    return [{"kind": "oom", "entity": ents[0] if ents else "", "state": "present", "confidence": 0.99,
             "quote": HALLUCINATED_QUOTE, "log_ts": ""}]


_KNOWN = "已知實體（entity 只能從這裡挑）："
_EVIDENCE = "日誌證據（已驗證 quote 逐字出現在原日誌）："
HALLUCINATED_ENTITY = "fabricated-svc"  # 不在任何世界裡的服務名


def _json_after(prompt: str, marker: str):
    i = prompt.find(marker)
    return next(_json_chunks(prompt[i + len(marker):]), None) if i >= 0 else None


def judge(prompt: str, faithful: bool) -> dict:
    """根因判斷節點的回覆（形狀照 aiops/stages/enrichment/rca_judge.build_prompt 的「請輸出」）。"""
    if not faithful:
        return {"candidates": [{"entity": HALLUCINATED_ENTITY, "confidence": 0.99, "subtype": "", "resource": "",
                                "reason": "記憶體耗盡導致整條鏈路失敗", "cites": ["E1"]}], "note": ""}
    entities = set(_json_after(prompt, _KNOWN) or [])
    rows = [e for e in (_json_after(prompt, _EVIDENCE) or []) if isinstance(e, dict) and e.get("entity") in entities]
    if not rows:
        return {"candidates": [], "note": "沒有屬於已知實體的已驗證日誌證據"}
    e = rows[0]
    return {"candidates": [{"entity": e["entity"], "confidence": 0.6, "subtype": "", "resource": "",
                            "reason": f"已驗證的日誌證據顯示 {e['kind']}", "cites": [e["id"]]}], "note": ""}


class ScriptedLLM(BaseLLM):
    name = NAME

    def __init__(self, model: str):
        mode, _, arg = (model or "faithful").partition(":")
        if mode not in MODES:
            raise LLMError(f"chaos_scripted 不認得模式 {mode!r}（可用：{', '.join(MODES)}）")
        self.mode = mode
        self.sleep_s = float(arg) if (mode == "slow" and arg) else SLOW_DEFAULT_S

    def complete(self, system: str, prompt: str) -> str:
        rec = {"mode": self.mode, "system": (system or "")[:120], "prompt_chars": len(prompt or ""), "reply": ""}
        CALLS.append(rec)
        if self.mode == "down":
            rec["reply"] = "<LLMError>"
            raise LLMError("chaos_scripted：模擬供應商連不上（down 模式）")
        if self.mode == "garbage":
            rec["reply"] = '{"evidence": [{"kind": "gc_pause", "entity": '
            return rec["reply"]
        if self.mode == "slow":
            time.sleep(self.sleep_s)
        if self.mode.startswith("judge_"):
            reply = json.dumps(judge(prompt or "", self.mode == "judge_faithful"), ensure_ascii=False)
            rec["reply"] = reply[:300]
            return reply
        items = hallucinate(prompt) if self.mode == "hallucinate" else extract(prompt or "")
        rec["reply"] = json.dumps({"evidence": items}, ensure_ascii=False)[:300]
        return json.dumps({"evidence": items}, ensure_ascii=False)


@register(NAME)
def _scripted(model: str, base_url: str, api_key: str | None) -> BaseLLM:
    return ScriptedLLM(model)


# ── 接線：把 RCA workflow 裡的 LLM 節點指到這個假供應商 ──
LLM_OPERATORS = ("enrichment.extract_evidence", "enrichment.rca_narrator", "rca.root_cause.llm")
JUDGE = "rca.root_cause.llm"
JUDGE_MODES = ("down", "slow", "garbage")   # 判斷節點直接用的失效模式；faithful／hallucinate 換成 judge_* 版本
_TIMEOUT_KEYS = ("timeout_seconds", "timeout_s", "timeout")


def node_timeout(params: dict, default: float = 30.0) -> float:
    return float(next((params[k] for k in _TIMEOUT_KEYS if k in params), default))


def judge_mode(mode: str) -> str:
    """判斷節點要接的模式：失效模式原樣；faithful -> judge_faithful、hallucinate -> judge_hallucinate。"""
    return mode if mode in JUDGE_MODES else {"faithful": "judge_faithful", "hallucinate": "judge_hallucinate"}[mode]


def wire(wf, mode: str) -> list[str]:
    """把 LLM 節點的 provider 換成 chaos_scripted（model 欄位帶模式）。回傳接上的節點名稱；一個都沒有就報錯，
    不然「LLM 全掛」場景會在 LLM 根本沒接上的情況下假裝通過。slow 模式睡的秒數 = 節點逾時 + 5。
    根因判斷節點（rca.root_cause.llm）接 judge_mode(mode)：抽證據與判斷的回覆形狀不同。"""
    hit = []
    for n in wf.nodes:
        if n.operator not in LLM_OPERATORS:
            continue
        node_mode = judge_mode(mode) if n.operator == JUDGE else mode
        model = f"slow:{node_timeout(n.params) + 5}" if node_mode == "slow" else node_mode
        n.params = {**n.params, "provider": NAME, "model": model}
        hit.append(n.name)
    if not hit:
        raise RuntimeError(f"workflow {wf.name} 裡沒有 LLM 節點（{'／'.join(LLM_OPERATORS)}），假 LLM 接不上")
    return hit


if __name__ == "__main__":  # 示範：三行日誌（熔斷、GC、死結）在各模式下的實際回傳
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    logs = [
        {"entity": "ord-svc", "log_ts": "2026-11-09T12:03:11.204Z",
         "message": "io.github.resilience4j.circuitbreaker.CallNotPermittedException: CircuitBreaker 'payment-gw' is OPEN and does not permit further calls"},
        {"entity": "inv-svc", "log_ts": "2026-11-09T12:03:12.880Z",
         "message": "[GC (Allocation Failure) ] [Pause Full (Allocation Failure) 1987M->1980M(2048M) 4210.331ms]"},
        {"entity": "billing-db", "log_ts": "2026-11-09T12:03:14.002Z",
         "message": "ERROR: deadlock detected DETAIL: Process 4121 waits for ShareLock on transaction 99812; blocked by process 4188."},
    ]
    prompt = json.dumps({"incident_templates": logs}, ensure_ascii=False)
    for mode in ("faithful", "down", "slow:0.2", "garbage", "hallucinate"):
        try:
            t = time.time()
            reply = ScriptedLLM(mode).complete("你是證據抽取器", prompt)
            print(f"[{mode}] ({time.time() - t:.2f}s) {reply}")
        except LLMError as e:
            print(f"[{mode}] LLMError: {e}")
    print("--- 純文字 prompt（逐行）---")
    text = "\n".join(f"[{d['entity']}] {d['log_ts']} {d['message']}" for d in logs)
    print(ScriptedLLM("faithful").complete("", text))
