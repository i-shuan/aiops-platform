"""RCA 報告的總結敘述：LLM 寫、平台驗，不通過就退回模板。

依據驗證（全部要過）：
1. 格式：summary 是字串、claims 是 1～8 個 {"text", "evidence"}。
2. 每個 claim 都要引用至少一個「存在的」報告段落 key。
3. 不得提到事件群以外的已知實體（例如拓撲裡沒出事的服務）。
4. 文字裡出現的每個數字，都要能在證據段落裡找到原文。
"""
from __future__ import annotations

import json
import re
from typing import Any

SYSTEM = (
    "你是 AIOps 根因分析助理。只能根據使用者提供的報告段落寫結論，"
    "不得加入段落以外的實體、數字或推測。只輸出 JSON，不要其他文字。"
)
_NUMBER = re.compile(r"\d+(?:\.\d+)?")


def build_prompt(sections: list[dict[str, str]]) -> str:
    return (
        "以下是事件群的報告段落（key 是段落代號）：\n"
        + json.dumps(sections, ensure_ascii=False, indent=2)
        + '\n\n請輸出：{"summary": "300 字以內的總結", '
        '"claims": [{"text": "一句結論", "evidence": ["引用的段落 key", ...]}]}，claims 1～8 條。'
    )


def validate(reply: dict[str, Any], sections: list[dict[str, str]], incident_entities: set[str],
             known_entities: set[str]) -> str | None:
    """通過回 None；不通過回原因。"""
    keys = {s["key"] for s in sections}
    summary, claims = reply.get("summary"), reply.get("claims")
    if not isinstance(summary, str) or not summary.strip() or len(summary) > 600:
        return "summary 缺少或超過 600 字"
    if not isinstance(claims, list) or not 1 <= len(claims) <= 8:
        return "claims 必須是 1～8 條"
    for c in claims:
        if not isinstance(c, dict) or not isinstance(c.get("text"), str) or not c["text"].strip():
            return "claim 缺少 text"
        evidence = c.get("evidence")
        if (not isinstance(evidence, list) or not evidence or not all(isinstance(k, str) for k in evidence)
                or not set(evidence) <= keys):
            return f"claim 引用了不存在的段落：{evidence}"

    text = summary + "\n" + "\n".join(c["text"] for c in claims)
    for name in sorted(known_entities - incident_entities):
        if name and name in text:
            return f"提到事件群以外的實體：{name}"
    evidence_text = "\n".join(s["body"] for s in sections)
    for num in _NUMBER.findall(text):
        if num not in evidence_text:
            return f"數字 {num} 在證據中找不到"
    return None


def template(sections: list[dict[str, str]]) -> dict[str, Any]:
    """不靠 LLM 的總結：取最關鍵的段落第一行，每條都附上來源段落。"""
    by_key = {s["key"]: s for s in sections}
    order = ("root_cause_candidates", "summary", "propagation_path", "trace_evidence", "suggested_actions")
    claims = [{"text": by_key[k]["body"].splitlines()[0], "evidence": [k]}
              for k in order if k in by_key and by_key[k]["body"].strip()]
    summary = claims[0]["text"] if claims else "資料不足，無法產生總結"
    return {"summary": summary, "claims": claims}


# ── 三行式止血摘要（enrichment.llm_briefing；docs/RCA_STEP2_DESIGN.md 第 4 節）──
# 跟上面的 summary／claims 總結分開：那一段看全部報告段落寫結論；這一段只吃壓縮過的硬證據，寫給值班的人 3 行就夠。

BRIEFING_SYSTEM = (
    "You are an expert SRE incident commander. Analyze the provided AIOps incident JSON and write a 3-line tactical briefing "
    "in Traditional Chinese.\n"
    "Constraints:\n"
    "1. ONLY reference services, resources, endpoints, trace ids and numbers that appear in the JSON. "
    "Do not invent anything else (no Kafka, no DNS, no other service unless it is in the JSON).\n"
    "2. Keep the numbers as they are in the JSON (rounding is fine).\n"
    "3. Output ONLY a JSON object with exactly these keys: summary_line, action_line, verification_line. "
    "summary_line starts with 【一句話根因】, action_line with 【立即止血動作】, verification_line with 【驗證與排除項】. "
    "Each line is one sentence, at most 200 characters."
)
BRIEFING_KEYS = ("summary_line", "action_line", "verification_line")
_PREFIX = {"summary_line": "【一句話根因】", "action_line": "【立即止血動作】", "verification_line": "【驗證與排除項】"}
# 封閉世界詞表（docs/HARDENING_DESIGN.md 第 3 節）：三行裡每個「識別字 token」（含英數、可用 - _ . : / @ 連接的片段）
# 都要出現在 context 的鍵名或值裡，或在下面這份通用技術詞表裡；不在就退回模板。中文敘述不抽 token：
# 死區——「請重啟資料庫主機」這種不含識別字的中文幻覺，封閉詞表擋不到（報告靠 briefing_generated_by 標來源）。
_IDENT = re.compile(r"[A-Za-z0-9]+(?:[-_.:/@][A-Za-z0-9]+)*")
_SPLIT = re.compile(r"([-_.:/@])")
_IDENT_CHAR = re.compile(r"[A-Za-z0-9\-_.:/@]")
_TRAILING_IDENT = re.compile(r"[A-Za-z0-9\-_.:/@]+$")
_NUM_UNIT = re.compile(r"\d+(?:\.\d+)?([a-z]+)")
# 放行清單（不是黑名單）：協定、單位、通用 SRE 名詞與動作。故意不放任何產品／元件名（kafka、redis、dns、k8s、nginx…）：
# 那些只有 context 裡有才算數。
GENERIC_LEXICON = frozenset("""
http https grpc tcp udp tls ssl url uri api apis db sql nosql io i/o json yaml id ids ip
get post put patch delete head options
ms s sec secs min mins h hr kb mb gb tb rps qps tps iops
p0 p1 p2 p3 p4 p50 p90 p95 p99 4xx 5xx sla slo sli rca aiops llm sre oom cpu gpu mem memory disk
timeout timeouts client clients server servers pool pools connection connections conn max min rate limit limits limiting
rollback roll back retry retries backoff circuit breaker fallback degrade failover hotfix deploy deployment release
config feature flag flags canary hpa replica replicas pod pods node nodes host hosts cluster
trace traces tracing span spans log logs logging error errors exception exceptions stack stacktrace status code
latency throughput traffic saturation load slow query queries lock locks wait index cache health healthcheck check
endpoint endpoints upstream downstream request requests response responses queue thread threads heap gc
baseline threshold metric metrics alert alerts incident runbook ok e.g i.e vs
loop loops tight policy storm throttle throttling fanout fan-out fan amplifier amplification ingress gateway
""".split())


def _context_tokens(obj: Any, out: set[str]) -> None:
    """context 的鍵名與值裡的識別字 token，連同它們用連接符切開的每一段連續片段（java.lang.IllegalStateException → IllegalStateException）。
    鍵名也收：prompt 就是整份 context JSON，模型照欄位名寫「trace_id …、status_code 500」是有據的。"""
    if isinstance(obj, str):
        for tok in _IDENT.findall(obj):
            parts = _SPLIT.split(tok.lower())  # 帶括號的 pattern：奇數位是連接符
            words, seps = parts[0::2], parts[1::2]
            for i in range(len(words)):  # 每一段連續片段：tax-gw:443 → tax、tax-gw、tax-gw:443、gw、gw:443、443
                piece = words[i]
                out.add(piece)
                for j in range(i + 1, len(words)):
                    piece += seps[j - 1] + words[j]
                    out.add(piece)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _context_tokens(str(k), out)
            _context_tokens(v, out)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            _context_tokens(v, out)


def briefing_lexicon(context: dict[str, Any]) -> set[str]:
    """合法識別字（小寫）：context 鍵名與值裡的全部 token ∪ 通用技術詞表。歸因的 kind/subtype 另外補成一個字（模板寫成 external/http）。"""
    words: set[str] = set(GENERIC_LEXICON)
    _context_tokens(context, words)
    a = (context.get("root_cause") or {}).get("attribution") or {}
    if a.get("kind") and a.get("subtype"):
        words.add(f"{a['kind']}/{a['subtype']}".lower())
    return words


def unknown_identifier(text: str, lexicon: set[str]) -> str | None:
    """第一個不在詞表裡的識別字 token（原文大小寫）；全部合法回 None。純數字（含 13.3、12:00）交給數字檢查；
    數字黏單位（101.4ms）只檢查單位。"""
    for tok in _IDENT.findall(text):
        low = tok.lower()
        if not re.search(r"[a-z]", low) or low in lexicon:
            continue
        m = _NUM_UNIT.fullmatch(low)
        if m and m.group(1) in lexicon:
            continue
        return tok
    return None


_TOLERANCE = 0.10
_MAX_LINE = 200


def build_context(ctx, incident) -> dict[str, Any]:
    """只放已經算好的東西，鍵固定；缺的給 None。不塞原始 metrics／span／log。"""
    from datetime import timedelta

    from aiops.engine.types import as_utc
    from aiops.stages.rca.logs import rank_patterns
    from aiops.stages.rca.spans import resource_label, sample_trace

    top = ctx.rca_root_candidates[0] if ctx.rca_root_candidates else None
    sample = None
    if ctx.rca_spans:
        s = sample_trace(ctx.rca_spans, as_utc(incident.start), as_utc(incident.end) + timedelta(minutes=1))
        if s is not None:
            failed = sorted({resource_label(c) for c in ctx.rca_spans
                             if c.trace_id == s.trace_id and c.error and c.kind not in ("", "app")})
            sample = {"trace_id": s.trace_id, "endpoint": s.name or None, "duration_ms": round(s.duration_ms, 1),
                      "status_code": s.status_code or None, "failed": bool(s.error) or s.status_code >= 500,
                      "failed_resource": "、".join(failed) or None}
    logs = []
    if ctx.rca_logs:
        for p in rank_patterns(ctx.rca_logs, ctx.rca_logs_baseline, ctx.rca_log_window_minutes)[:3]:
            logs.append({"service": p["service"], "error_type": p["error_type"] or None, "template": p["template"][:160],
                         "count": round(p["during"]), "ratio": round(p["ratio"], 1)})
    minutes = max(0, round((as_utc(incident.end) - as_utc(incident.start)).total_seconds() / 60))
    return {
        "incident_meta": {"entities": sorted(set(incident.entities)), "duration_minutes": minutes,
                          "severity": incident.severity or None},
        "root_cause": ({"candidate": top.entity, "score": round(top.score, 3), "dependency": top.dependency or None,
                        "attribution": top.attribution or None} if top else None),
        "root_note": ctx.rca_root_note or None,
        "sample_trace": sample,
        "top_error_logs": logs,
        "matched_runbook": ctx.rca_runbook,
    }


def build_briefing_prompt(context: dict[str, Any]) -> str:
    return "Incident context (JSON):\n" + json.dumps(context, ensure_ascii=False, indent=2)


def _numbers_in(obj: Any, out: set[float]) -> None:
    if isinstance(obj, bool):
        return
    if isinstance(obj, (int, float)):
        out.add(float(obj))
    elif isinstance(obj, str):
        out.update(float(n) for n in _NUMBER.findall(obj))
    elif isinstance(obj, dict):
        for v in obj.values():
            _numbers_in(v, out)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            _numbers_in(v, out)


def _number_ok(value: float, allowed: set[float]) -> bool:
    return any(a == value or abs(a - value) <= max(abs(a) * _TOLERANCE, 0.5) for a in allowed)


def briefing_whitelist(context: dict[str, Any]) -> set[str]:
    names = set(context["incident_meta"]["entities"])
    rc = context.get("root_cause") or {}
    if rc.get("candidate"):
        names.add(rc["candidate"])
    if (rc.get("attribution") or {}).get("resource"):
        names.add(rc["attribution"]["resource"])
    names.update(o["resource"] for o in (rc.get("attribution") or {}).get("others") or [])  # 一起變慢的其他資源
    mq = (rc.get("attribution") or {}).get("async") or {}  # 訊息佇列的另一端：處方會叫人回頭查生產者
    names.update(name for name in (mq.get("producer"), mq.get("consumer")) if name)
    amp = (rc.get("attribution") or {}).get("amplifier") or {}  # 被放大者灌爆的下游：處方會叫人去保住它
    surge = (rc.get("attribution") or {}).get("ingress_surge") or {}
    names.update(name for name in (amp.get("victim"), amp.get("via"), surge.get("entry"), *(amp.get("victims") or [])) if name)
    st = context.get("sample_trace") or {}
    for key in ("endpoint", "failed_resource", "trace_id"):
        if st.get(key):
            names.add(str(st[key]))
    if context.get("matched_runbook"):
        names.add(context["matched_runbook"]["id"])
    return names


def validate_briefing(reply: dict[str, Any], context: dict[str, Any], known_entities: set[str]) -> str | None:
    """通過回 None；不通過回原因。
    1. 三個鍵都要是非空字串、≤ 200 字、以規定的【】開頭。
    2. 實體白名單：known_entities（拓撲裡的服務）之中不在白名單的名字不准出現（封閉詞表的特例，留著對照）。
    3. 封閉詞表：每個識別字 token（不分大小寫）都要在 context 的值或通用技術詞表裡（briefing_lexicon）。
    4. 數字：文字裡每個數字都要在 context 的數值（含字串裡的數字）±10% 內找到。
    死區：不含識別字的中文幻覺（「請重啟資料庫主機」）擋不到。"""
    for key in BRIEFING_KEYS:
        line = reply.get(key)
        if not isinstance(line, str) or not line.strip():
            return f"缺少 {key}"
        if len(line) > _MAX_LINE:
            return f"{key} 超過 {_MAX_LINE} 字"
        if not line.strip().startswith(_PREFIX[key]):
            return f"{key} 必須以 {_PREFIX[key]} 開頭"
    text = "\n".join(reply[k] for k in BRIEFING_KEYS)
    allowed_names = briefing_whitelist(context)
    for name in sorted(known_entities - allowed_names):
        if name and name in text:
            return f"提到白名單以外的實體：{name}"
    token = unknown_identifier(text, briefing_lexicon(context))
    if token is not None:
        shown = token if token == token.lower() else f"{token}（{token.lower()}）"
        return f"提到證據與通用詞表裡沒有的識別字：{shown}"
    allowed_numbers: set[float] = set()
    _numbers_in(context, allowed_numbers)
    for num in _NUMBER.findall(text):
        if not _number_ok(float(num), allowed_numbers):
            return f"數字 {num} 在證據中找不到（±10%）"
    return None


def _clip(text: str, n: int) -> str:
    """截到 n 字，但不把識別字切一半（切一半的 inventory-s 會被封閉詞表當成虛構名稱）。"""
    if len(text) <= n:
        return text
    cut = text[:n]
    if _IDENT_CHAR.fullmatch(text[n - 1]) and _IDENT_CHAR.fullmatch(text[n]):
        cut = _TRAILING_IDENT.sub("", cut).rstrip()
    return cut


def _async_clause(rc: dict[str, Any]) -> str:
    """摘要行的訊息佇列補充句。只引用 attribution["async"] 裡現成的字串（lag_text、pub_window_text），數字守門才對得上。"""
    info = (rc.get("attribution") or {}).get("async") or {}
    lag = info.get("lag_text") or {}
    if info.get("role") == "producer":
        return (f"；{rc['candidate']} 是訊息生產者，以上是訊息發布時窗 {info.get('pub_window_text')} 的數字"
                f"（{info.get('consumer')} 事後才消費到，延遲 P95 {lag.get('during')}）")
    if info.get("backlog"):
        return f"；佇列 {info.get('queue') or '（名稱未知）'} 積壓，訊息延遲 P95 {lag.get('before')}→{lag.get('during')}"
    return ""


def _async_verify(rc: dict[str, Any]) -> str:
    """第一名是承接了訊息夾帶失敗的生產者時的驗證行：消費者的失敗算在它發的訊息上，就不能再寫「排除它本身的程式」
    （trace 看不到訊息內容，壞訊息正是它的程式發的）。其他情況回空字串，沿用原本的驗證行。"""
    info = (rc.get("attribution") or {}).get("async") or {}
    if info.get("role") != "producer" or not info.get("moved_error"):
        return ""
    return (f"{_PREFIX['verification_line']}{info.get('consumer')} 處理失敗的訊息是 {rc['candidate']} 在 {info.get('pub_window_text')} 發的；"
            f"trace 看不到訊息內容，不能排除 {rc['candidate']} 當時發出壞訊息，請抽查失敗訊息")


def _amplifier_clause(rc: dict[str, Any]) -> str:
    """摘要行的流量放大句。只引用 attribution["amplifier"]／["ingress_surge"] 裡現成的數字，數字守門才對得上。"""
    a = rc.get("attribution") or {}
    amp, surge = a.get("amplifier") or {}, a.get("ingress_surge") or {}
    if amp:
        fan = amp.get("fanout") or {}
        return (f"{rc['candidate']} 把 {amp.get('victim')} 的流量放大到 {amp.get('load_ratio')} 倍：每處理一個請求呼叫 {amp.get('via')} "
                f"{fan.get('before')}→{fan.get('during')} 次（扇出 {amp.get('fanout_ratio')} 倍）")
    if surge:
        return f"流量由外部入口 {surge.get('entry')} 打進來（{rc['candidate']} 流入 {surge.get('load_ratio')} 倍），沒有內部服務放大"
    return ""


def _amplifier_verify(rc: dict[str, Any]) -> str:
    """第一名是流量放大者時的驗證行：下游的增量是被灌出來的，該排除的是下游本身，不是寫「確認它的程式」就完了。"""
    amp = (rc.get("attribution") or {}).get("amplifier") or {}
    if not amp:
        return ""
    silent = "" if amp.get("in_incident", True) else f"；{rc['candidate']} 自己沒有告警、不在事件裡"
    return (f"{_PREFIX['verification_line']}{amp.get('victim')} 的自身耗時與錯誤增量是被灌流量造成的，已轉記給 {rc['candidate']}，"
            f"排除 {amp.get('victim')} 本身的程式與容量{silent}")


def briefing_template(context: dict[str, Any]) -> dict[str, str]:
    """不靠 LLM 的三行：全部從 attribution、runbook、範例 trace 拼，數字一定精確。"""
    rc = context.get("root_cause") or {}
    a = rc.get("attribution") or {}
    rb = context.get("matched_runbook") or {}
    st = context.get("sample_trace") or {}
    entities = "、".join(context["incident_meta"]["entities"]) or "事件群"
    if rc.get("candidate") and rc.get("dependency"):
        kind = "/".join(p for p in (a.get("kind", ""), a.get("subtype", "")) if p)
        summary = (f"{_PREFIX['summary_line']}{rc['candidate']} 等待 {a['resource']}（{kind}）"
                   f"{a['wait_ms']['before']}→{a['wait_ms']['during']} ms，程式自身 {a['self_ms']['before']}→{a['self_ms']['during']} ms，"
                   f"資源失敗 {a['failures']['before']}→{a['failures']['during']} 筆")
        verify = f"{_PREFIX['verification_line']}{rc['candidate']} 程式自身耗時沒有惡化，排除它本身的程式與機器"
        if st.get("failed_resource"):
            verify += f"；範例 trace {st['trace_id']} 入口 {st.get('status_code') or '失敗'}，失敗在 {st['failed_resource']}"
    elif rc.get("candidate") and a.get("amplifier"):
        summary = f"{_PREFIX['summary_line']}{_amplifier_clause(rc)}（分數 {rc['score']}）"
        verify = _amplifier_verify(rc)
    elif rc.get("candidate"):
        summary = f"{_PREFIX['summary_line']}{rc['candidate']} 自身耗時或自身錯誤率上升（分數 {rc['score']}），沒有單一下游資源主導"
        verify = f"{_PREFIX['verification_line']}依 trace 佐證與日誌段落確認 {rc['candidate']} 的程式與近期變更"
        if st.get("trace_id"):
            verify += f"；範例 trace {st['trace_id']}"
    else:
        note = _clip((context.get("root_note") or "").split("：", 1)[-1], 120)
        summary = f"{_PREFIX['summary_line']}證據不足，不指認單一服務（{entities}）：{note}"
        verify = f"{_PREFIX['verification_line']}先查共用的資料源、網路與未被追蹤的依賴，比對同時間其他告警"
    if rc.get("candidate"):
        summary += _async_clause(rc)
        verify = _async_verify(rc) or verify
        if rc.get("dependency") and a.get("amplifier") or a.get("ingress_surge") and not a.get("amplifier"):
            summary += "；" + _amplifier_clause(rc)
    first = f"[{rb['id']}] {rb['actions'][0]}" if rb.get("actions") else "比對事件前後的變更紀錄（部署、設定、配方）"
    action = _PREFIX["action_line"] + first
    return {"summary_line": _clip(summary, _MAX_LINE), "action_line": _clip(action, _MAX_LINE),
            "verification_line": _clip(verify, _MAX_LINE)}
