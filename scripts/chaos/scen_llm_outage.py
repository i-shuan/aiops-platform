"""場景 24：LLM 全掛（rca_report_llm 的降級；CLAUDE.md §1 第 3 條：RCA 路徑的 LLM 是必要元件，失效時報告照出、標 undetermined）。

世界：ord-svc 呼叫外部金流 payment-gw:443；12:00 起金流全掛，ord-svc 的熔斷器打開，錯誤日誌是
「CircuitBreaker 'payment-gw' is OPEN and does not permit further calls」（Resilience4j 的原句）。
偵測只跑一次，同一個事件群用 rca_report_llm 跑五次，每次假 LLM（scripted_llm.py）換一種模式
（證據抽取與戰情摘要接這個模式；根因判斷節點 rca.root_cause.llm 接 scripted_llm.judge_mode：失效模式原樣，
hallucinate -> judge_hallucinate、faithful -> judge_faithful）：

- 24a down／24b slow／24c garbage／24d hallucinate：LLM 不可用或回的東西不能信。報告要照出、rca_status == "undetermined"、
  原因代碼存在、證據層的事實與原始 trace／日誌入口還在、**沒有編造的根因**（不准退回舊規則硬猜）。
  24d：證據抽取全部被 quote 逐字驗證拒絕 -> 上游已經 undetermined，判斷節點不會被呼叫（rca_judge.py 的既有行為）。
- 24e faithful（對照組）：假 LLM 照規則抽出熔斷證據，驗證要讓它過（quote 逐字在日誌裡）；判斷節點照抄那筆證據的實體，
  驗證也要讓它過，狀態是 ok、有根因候選。這組只證明「平台接得上、驗證不會誤殺」，不證明真模型抽得到、判得對
  （見 scripted_llm.py 檔頭的誠實限制）。

改寫自原本對 rca_report_cognitive（rca.root_cause.bayes，已移除）的版本：斷言條件一條都沒改，只有 24e 一條斷言的
說明文字「機率模型拿到證據有輸出」改成「判斷節點拿到證據有輸出」（條件仍是 >= 1 個候選）。
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from scripts.chaos import harness as H
from scripts.chaos import scripted_llm as LLM
from scripts.chaos.world import Call, Svc, World

UTC = timezone.utc
T0 = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
INC = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
CB_MESSAGE = "CircuitBreaker 'payment-gw' is OPEN and does not permit further calls"

# 只對 LLM 引擎有意義的場景：run.py 不帶場景編號（全部）且 --rca legacy 時不跑，legacy 全量輸出跟以前逐列相同
LLM_ONLY = {24}

MODES = [("24a", "down", "LLM 連不上（丟 LLMError）"),
         ("24b", "slow", "LLM 逾時（睡過節點逾時 + 5 秒）"),
         ("24c", "garbage", "LLM 回截斷的壞 JSON"),
         ("24d", "hallucinate", "LLM 回 quote 不在日誌裡的證據（幻覺）"),
         ("24e", "faithful", "對照組：假 LLM 照規則抽出熔斷證據")]
INJECTED = (f"ord-svc 呼叫 payment-gw:443（external/http）：12:00 起 100% 失敗、熔斷打開快速失敗，ord-svc 交易失敗、"
            f"錯誤日誌「{CB_MESSAGE}」；同一事件群用 rca_report_llm 跑，假 LLM 模式：")


def _mins(lo: datetime, hi: datetime) -> int:
    return int((hi - lo).total_seconds() // 60)


def _results() -> list[H.Result]:
    return [H.Result(n, f"LLM 全掛：{desc}" if mode != "faithful" else desc, INJECTED + mode) for n, mode, desc in MODES]


def _report_text(out: dict) -> str:
    return "\n".join(str(v) for v in out["_sections"].values()) + "\n" + str(out.get("root_cause", {}).get("note", ""))


def _check_undetermined(r: H.Result, out: dict, calls: list[dict], mode: str, elapsed: float, sleep_s: float) -> None:
    ctx = out["_ctx"]
    sec = out["_sections"]
    c = out.get("root_cause", {}).get("candidates", [])
    r.check("假 LLM 真的被叫到（LLM 節點有接上，不是因為沒接才「沒事」）", f"≥1 次 {mode}", f"{len(calls)} 次",
            any(x["mode"] == mode for x in calls))
    r.check("報告照出（有段落、整份沒失敗）", "有段落", f"{len(sec)} 段：{sorted(sec)}", bool(sec))
    r.check("rca_status == undetermined", "undetermined", ctx.rca_status, ctx.rca_status == "undetermined")
    r.check("原因代碼存在", "非空", repr(ctx.rca_undetermined_reason), bool(ctx.rca_undetermined_reason))
    r.check("沒有編造的根因（候選為空，不退回舊規則硬猜）", "0 個候選",
            f"{len(c)} 個：{[(x['entity'], x['score']) for x in c[:3]]}；algorithm={out.get('root_cause', {}).get('algorithm')!r}", not c)
    txt = _report_text(out)
    r.check("根因欄位寫「無法判定」", "報告含「無法判定」", "有" if "無法判定" in txt else "沒有", "無法判定" in txt)
    facts = list(ctx.rca_facts)
    r.check("證據層的事實已算出（ctx.rca_facts）", "≥1 個", f"{len(facts)} 個：{[(f.kind, f.entity) for f in facts[:4]]}", bool(facts))
    # 事實要進報告：合約沒定段落名稱，先認 to_result 的 facts 欄位或 key 含 fact 的段落（主 session 整合時對齊）
    shown = bool(out.get("facts")) or any("fact" in k for k in sec)
    r.check("事實出現在報告裡", "to_result.facts 或 fact 段落", f"facts 欄位={'有' if out.get('facts') else '無'}；段落={sorted(sec)}", shown)
    r.check("原始 trace 入口在報告裡", "trace_evidence 段落", "有" if sec.get("trace_evidence") else "無", bool(sec.get("trace_evidence")))
    r.check("原始日誌入口在報告裡", "log_patterns 段落", "有" if sec.get("log_patterns") else "無", bool(sec.get("log_patterns")))
    if mode == "slow":
        r.check("逾時有生效（沒有等假 LLM 睡完、沒有無限重試）", f"< {sleep_s:.0f} 秒", f"{elapsed:.1f} 秒", elapsed < sleep_s)
    r.notes.append(f"假 LLM 呼叫：{[(x['mode'], x['system'][:40], x['prompt_chars']) for x in calls]}；errors={out['_errors']}")


def _check_faithful(r: H.Result, out: dict, calls: list[dict], log_texts: set[str]) -> None:
    ctx = out["_ctx"]
    ev = list(ctx.rca_evidence)
    r.check("假 LLM 真的被叫到", "≥1 次 faithful", f"{len(calls)} 次", any(x["mode"] == "faithful" for x in calls))
    r.check("rca_status == ok", "ok", f"{ctx.rca_status}（{ctx.rca_undetermined_reason!r}）", ctx.rca_status == "ok")
    cb = [e for e in ev if e.kind == "circuit_breaker_open"]
    r.check("熔斷證據通過驗證進了 ctx.rca_evidence", "circuit_breaker_open @ ord-svc",
            f"{[(e.kind, e.entity, e.confidence) for e in ev]}", any(e.entity == "ord-svc" for e in cb))
    verbatim = bool(cb) and all(any(e.quote in t for t in log_texts) for e in cb)
    r.check("進來的證據 quote 逐字出現在世界的日誌裡", "全部逐字", f"{[e.quote for e in cb]}", verbatim)
    c = out.get("root_cause", {}).get("candidates", [])
    r.check("有根因候選（判斷節點拿到證據有輸出）", "≥1 個", f"{[(x['entity'], x['score']) for x in c[:3]]}", bool(c))
    r.notes.append(f"假 LLM 呼叫：{[(x['mode'], x['system'][:40], x['prompt_chars']) for x in calls]}；errors={out['_errors']}")


def scenario_24() -> list[H.Result]:
    """LLM 全掛（rca_report_llm 的降級）"""
    results = _results()
    end = INC + timedelta(minutes=45)
    w = World(T0, _mins(T0, end + timedelta(minutes=20)), [
        Svc("ord-svc", "POST /orders", 30.0, [Call("payment-gw:443", 1.0, "external", "http", 20.0, 0.0, name="POST payment-gw")],
            error_type="io.github.resilience4j.circuitbreaker.CallNotPermittedException", error_message=CB_MESSAGE),
        Svc("pay-api", "POST /pay", 25.0, [Call("postgres", 1.0, "db", "postgresql", 8.0, name="INSERT pay")]),
    ], {"ord-svc": 25.0, "pay-api": 20.0})
    gw = w.call("ord-svc", "payment-gw:443")
    w.set(gw.ms, INC, end, 2.0)       # 熔斷打開：不再真的等金流，快速失敗
    w.set(gw.fail_p, INC, end, 1.0)
    w.build()
    H.fresh_db()
    with H.es_for(w, INC - timedelta(minutes=80), end + timedelta(minutes=5)) as es:
        log_texts = {str(d.get("message", "")) for d in es.log_docs}
        wf = H.detection_workflow("sample_it_otel", H.targets(["ord-svc"], signal="errors", kind="errors") + H.targets(["pay-api"]))
        rp = H.replay(wf, INC - timedelta(minutes=30), end)
        incs = H.incidents()
        target = H.pick(incs, "ord-svc", INC - timedelta(minutes=5))
        det = f"偵測：異常 {rp.anomalies} 筆、errors {rp.errors[:2]}、事件群 {[(i['id'], i['incident']['entities']) for i in incs]}"
        for r in results:
            r.notes.append(det)
            r.check("偵測到 ord-svc 事件群（失敗數飆升）", "有", bool(target), bool(target))
        if not target:
            return results
        wf_rca = H.load_workflow(H.workflow_path(H.RCA_WORKFLOWS["llm"]))
        sleep_s = max((LLM.node_timeout(n.params) + 5 for n in wf_rca.nodes if n.operator in LLM.LLM_OPERATORS), default=0.0)
        for r, (_, mode, _) in zip(results, MODES):
            before = len(LLM.CALLS)
            t = time.time()
            try:
                out = H.rca(target["id"], mode="llm", llm=mode)
            except Exception as e:  # noqa: BLE001  整份報告失敗本身就是要抓的結果：記成未通過，其他模式照跑
                r.check("報告照出（整份沒失敗）", "產出報告", f"{type(e).__name__}: {str(e)[:200]}", False)
                continue
            elapsed = time.time() - t
            calls = LLM.CALLS[before:]
            if mode == "faithful":
                _check_faithful(r, out, calls, log_texts)
            else:
                _check_undetermined(r, out, calls, mode, elapsed, sleep_s)
    return results
