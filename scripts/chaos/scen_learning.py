"""場景 6～10：LLM 幻覺守門、分位數門檻、黃金事故防護罩、ACF 與假日、PostgreSQL JSONB 併發。"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone

import numpy as np

from scripts.chaos import harness as H
from scripts.chaos.world import Call, Svc, World

UTC = timezone.utc
T0 = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
INC = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)


def _mins(lo: datetime, hi: datetime) -> int:
    return int((hi - lo).total_seconds() // 60)


def _cand(out: dict) -> list[dict]:
    return out.get("root_cause", {}).get("candidates", [])


# ───────────────────────── 場景 6：LLM 幻覺注入 ─────────────────────────
class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record):
        self.lines.append(f"{record.name}:{record.getMessage()}")


def scenario_6() -> H.Result:
    from aiops.stages.enrichment import summarizer
    from aiops.stages.enrichment.base import BaseLLM

    r = H.Result(6, "惡意 LLM 幻覺注入（白名單守門）",
                 "假模型回傳充滿幻覺的三行 JSON：『檢查 Kafka 佇列飽和度並重啟 k8s-node-99』（環境裡根本沒有）；另測只含虛構節點、虛構服務名、虛構數字、與合法回文各一組")
    end = INC + timedelta(minutes=45)
    w = World(T0, _mins(T0, end + timedelta(minutes=20)), [
        Svc("ord-svc", "POST /orders", 40.0, [Call("inventory-db", 1.0, "db", "postgresql", 10.0, name="SELECT inventory"),
                                              Call("payment-gw:443", 1.0, "external", "http", 20.0, 0.0, swallow=True, name="POST payment-gw")]),
        Svc("pay-api", "POST /pay", 25.0, [Call("postgres", 1.0, "db", "postgresql", 8.0, name="INSERT pay")]),
    ], {"ord-svc": 25.0, "pay-api": 20.0})
    gw = w.call("ord-svc", "payment-gw:443")
    w.set(gw.ms, INC, end, 3000.0)
    w.set(gw.fail_p, INC, end, 1.0)
    w.build()
    H.fresh_db()
    with H.es_for(w, INC - timedelta(minutes=80), end + timedelta(minutes=5)):
        wf = H.detection_workflow("it_otel", H.targets(["ord-svc", "pay-api"]))
        H.replay(wf, INC - timedelta(minutes=30), end)
        inc = H.pick(H.incidents(), "ord-svc", INC - timedelta(minutes=5))
        if not r.check("偵測到 ord-svc 事件群", "有", bool(inc), bool(inc)):
            return r
        base = H.rca(inc["id"])
        a = _cand(base)[0]["attribution"]
        valid = {"summary_line": f"【一句話根因】ord-svc 等待 payment-gw:443 由 {a['wait_ms']['before']} ms 升至 {a['wait_ms']['during']} ms",
                 "action_line": "【立即止血動作】檢查 payment-gw:443 的健康狀態與網路延遲，確認 ord-svc 的 HTTP client timeout",
                 "verification_line": "【驗證與排除項】ord-svc 程式自身耗時沒有惡化，排除它本身的程式與機器"}
        variants = {
            "A 使用者原文（Kafka＋k8s-node-99）": {**valid, "action_line": "【立即止血動作】請立即檢查 Kafka 佇列飽和度並重啟 k8s-node-99"},
            "A2 只有 Kafka（無數字，測黑名單）": {**valid, "action_line": "【立即止血動作】請立即檢查 Kafka 佇列飽和度並確認 ord-svc 逾時"},
            "B 只有虛構節點 k8s-node-99（沒有 Kafka，帶數字）": {**valid, "action_line": "【立即止血動作】請立即重啟 k8s-node-99 並檢查 payment-gw:443"},
            "B2 虛構節點 gpu-farm-prod（無數字、不在黑名單）": {**valid, "action_line": "【立即止血動作】請立即重啟 gpu-farm-prod 節點並檢查 payment-gw:443"},
            "C 虛構服務名 payment-gateway": {**valid, "action_line": "【立即止血動作】請檢查 payment-gateway 的設定並確認 ord-svc 逾時"},
            "D 虛構數字（失敗 99 次）": {**valid, "verification_line": "【驗證與排除項】payment-gw:443 失敗 99 次，排除 ord-svc 程式本身"},
            "E 合法回文（對照組）": valid,
        }

        class _Fake(BaseLLM):
            name = "chaos-fake"

            def __init__(self, reply):
                self.reply = reply

            def complete(self, system, prompt):
                return json.dumps(self.reply, ensure_ascii=False)

        def mutate(wf_):
            for n in wf_.nodes:
                if n.operator == "enrichment.llm_briefing":
                    n.params = {**n.params, "provider": "openai", "model": "chaos-model"}

        original = summarizer.build_llm
        rows = {}
        try:
            for label, reply in variants.items():
                summarizer.build_llm = lambda *a, _r=reply, **k: _Fake(_r)
                cap = _Capture()
                root = logging.getLogger()
                root.addHandler(cap)
                old_level = root.level
                root.setLevel(logging.DEBUG)
                try:
                    out = H.rca(inc["id"], mutate)
                finally:
                    root.removeHandler(cap)
                    root.setLevel(old_level)
                n = out["narrative"]
                rows[label] = {"by": n["briefing_generated_by"], "rejected": n["briefing_rejected"], "errors": out["_errors"],
                               "logged": [ln for ln in cap.lines if "hallucination" in ln.lower()],
                               "action": n["briefing"]["action_line"]}
        finally:
            summarizer.build_llm = original
    A, A2, B, B2, C, D, E = (rows[k] for k in variants)
    r.check("A 使用者原文被攔截（降級為模板）", "generated_by=template、有 rejected 原因", f"{A['by']}；{A['rejected']}", A["by"] == "template" and bool(A["rejected"]))
    r.check("A2 只有 Kafka（無數字）被黑名單攔截", "template、原因含 kafka", f"{A2['by']}；{A2['rejected']}", A2["by"] == "template" and "kafka" in (A2["rejected"] or "").lower())
    r.check("A 降級後報告 errors 為空、三行仍由 Runbook 模板填好", "errors=[]、action 含 RB-01", f"errors={A['errors']}；{A['action'][:60]}", not A["errors"] and "RB-01" in A["action"])
    r.check("A 日誌記錄 HallucinationBlocked", "日誌出現 HallucinationBlocked", f"日誌命中 {len(A['logged'])} 行（平台只寫 narrative.briefing_rejected，沒有日誌）", bool(A["logged"]))
    r.check("B 虛構節點 k8s-node-99 被攔截", "template", f"{B['by']}；{B['rejected']}", B["by"] == "template")
    r.check("B2 虛構節點 gpu-farm-prod（無數字、不在黑名單）也必須攔截", "template", f"{B2['by']}；{B2['rejected']}", B2["by"] == "template")
    r.check("C 虛構服務名 payment-gateway 也必須攔截", "template", f"{C['by']}；{C['rejected']}", C["by"] == "template")
    r.notes.append("A、B 被攔截的實際原因是『k8s-node-99』裡的數字 99 找不到證據（數字檢查），不是實體白名單；"
                   "沒有數字、也不在黑名單的虛構名稱（B2、C）靠的是實體白名單，而白名單只擋『拓撲裡已知但不相干』的名字")
    r.check("D 虛構數字被攔截（對照）", "template、數字不在證據裡", f"{D['by']}；{D['rejected']}", D["by"] == "template")
    r.check("E 合法回文被放行（守門不是全擋）", "llm", f"{E['by']}；{E['rejected']}", E["by"] == "llm")
    return r


# ───────────────────────── 場景 7：極端毛刺 × 分位數反推門檻 ─────────────────────────
def _s7_run(spike_minutes: int) -> dict:
    """世界 40 天；在 INC+10min 起注入 spike_minutes 分鐘的瞬間抖動（每分鐘的 mad 分數 ~80）；偵測 → 標誤報 → 反推門檻。"""
    from aiops.engine.requests import FeedbackPropose, FeedbackSubmit
    from aiops.interfaces.runner import run

    t0 = INC - timedelta(days=40)
    end = INC + timedelta(minutes=40)
    w = World(t0, _mins(t0, end + timedelta(minutes=10)), [Svc("bill-svc", "POST /bills", 30.0, [Call("billing-db", 1.0, "db", "postgresql", 10.0)])],
              {"bill-svc": 25.0}, seed=11)
    w.build()
    m0 = w.m(INC + timedelta(minutes=10))
    train = (w.sum_ok["bill-svc"] + w.sum_fail["bill-svc"])[m0 - 10080:m0]
    med = float(np.median(train))
    mad = float(np.median(np.abs(train - med)))
    val = med + 80.0 * mad / 0.6745
    for k in range(spike_minutes):
        w.sum_ok["bill-svc"][m0 + k], w.n_ok["bill-svc"][m0 + k] = val, max(int(w.n_ok["bill-svc"][m0 + k]), 1)
        w.n_fail["bill-svc"][m0 + k], w.sum_fail["bill-svc"][m0 + k] = 0, 0.0
    H.fresh_db()
    out: dict = {"median": med, "mad": mad, "value": val}
    with H.es_for(w) as es:
        wf = H.detection_workflow("it_otel", H.targets(["bill-svc"]))
        rp = H.replay(wf, INC, end)
        incs = H.incidents()
        out["incidents"] = [(i["id"], i["incident"]["start"][11:16], i["incident"]["end"][11:16]) for i in incs]
        out["detect"] = f"{rp.rounds} 輪、異常 {rp.anomalies}、errors {rp.errors[:2]}"
        inc = H.pick(incs, "bill-svc", INC)
        out["inc"] = inc
        if not inc:
            return out
        fb = run("admin_feedback_submit", request=FeedbackSubmit(incident_uid=inc["uid"], verdict="false_positive", detector="detector.mad",
                                                                text="瞬間網路抖動，不是事故", author="chaos"))["feedback"]
        n_before = len(es.requests)
        res = run("admin_feedback_propose", request=FeedbackPropose(feedback_id=fb["id"], proposer="chaos", detector="detector.mad", scope="entity"))
        out["proposal"] = res["proposal"]
        out["generated_by"] = res.get("generated_by")
        # 反推門檻時對 ES 發的查詢：誤報時窗的終點（lt）有沒有涵蓋事件群的最後一個點
        lts = []
        for path, body in es.requests[n_before:]:
            for f in body.get("query", {}).get("bool", {}).get("filter", []):
                if "range" in f and "lt" in f["range"].get("@timestamp", {}):
                    lts.append(f["range"]["@timestamp"]["lt"])
        out["queried_lt_max_near_incident"] = sorted({x for x in lts if x[:10] == inc["incident"]["end"][:10]})[-3:]
        out["incident_end"] = inc["incident"]["end"]
    return out


def scenario_7() -> H.Result:
    r = H.Result(7, "極端毛刺干擾下的分位數反推門檻",
                 "bill-svc 平時穩定；注入瞬間抖動讓 mad 分數每分鐘 ~80（極端離群值），維運標記該事件為 false_positive。分兩組：(a) 1 分鐘單點毛刺、(b) 3 分鐘毛刺")
    for label, minutes in (("a", 1), ("b", 3)):
        o = _s7_run(minutes)
        r.notes.append(f"({label}) 世界：mad 訓練窗 median={o['median']:.0f}、MAD={o['mad']:.0f}（µs/分）；注入值 {o['value']:.0f}（理論 mad 分數 80）；"
                       f"偵測 {o['detect']}；事件群 {o['incidents']}")
        if not r.check(f"({label}) 毛刺被偵測成事件群", "有", bool(o.get("inc")), bool(o.get("inc"))):
            continue
        p = o["proposal"]
        th = (p["params"] or {}).get("threshold")
        reason = p["reason"]
        r.notes.append(f"({label}) 提案 threshold={th}；generated_by={o['generated_by']}；事件群結束={o['incident_end']}；"
                       f"反推時對 ES 的查詢終點(lt)={o['queried_lt_max_near_incident']}；reason={reason[:420]}")
        r.check(f"({label}) 門檻沒有被帶偏到 80", "< 80", th, th is not None and th < 80)
        r.check(f"({label}) 單次調整 ≤ +50%（3.5 → ≤ 5.25）且方向向上", "3.5 < threshold ≤ 5.25", th, th is not None and 3.5 < th <= 5.25 + 1e-9)
        quant = "分位數" in reason or "S_max" in reason or "Q" in reason.split("：")[-1][:4]
        r.check(f"({label}) 走分位數反推（不是退回固定規則 ×1.2）", "reason 不含『固定規則』", reason[:90], "固定規則" not in reason)
        trunc = "固定規則" not in reason and any(k in reason for k in ("步進截斷", "還差", "單次"))
        r.check(f"({label}) reason 註明目標過大、執行步進截斷", "reason 含截斷說明", reason[:90], trunc)
    return r


# ───────────────────────── 場景 8：黃金事故防護罩一票否決 ─────────────────────────
def scenario_8() -> H.Result:
    from fastapi.testclient import TestClient

    os.environ["AIOPS_API_TOKEN"] = "chaos-token"
    from aiops.interfaces.app import create_app

    r = H.Result(8, "黃金事故防護罩一票否決（破壞性調參回測）",
                 "bill-svc 於 12:00 起延遲約 3 倍（P0）並確認為真事故；提案把 zscore 門檻從 3.0 調成 25.0")
    end = INC + timedelta(minutes=30)
    w = World(T0, _mins(T0, end + timedelta(minutes=20)), [Svc("bill-svc", "POST /bills", 30.0, [Call("billing-db", 1.0, "db", "postgresql", 10.0)])],
              {"bill-svc": 25.0}, seed=5)
    w.scale(w.svcs["bill-svc"].self_ms, INC, end, 3.0)
    w.build()
    H.fresh_db()
    client = TestClient(create_app(), headers={"Authorization": "Bearer chaos-token"})
    with H.es_for(w):
        wf = H.detection_workflow("it_apm", H.targets(["bill-svc"]))
        rp = H.replay(wf, INC - timedelta(minutes=20), end)
        incs = H.incidents()
        r.notes.append(f"偵測（it_apm／zscore）：{rp.rounds} 輪、異常 {rp.anomalies}、errors {rp.errors[:2]}、事件群 {[(i['id'], i['incident']['start'][11:16], i['severity']) for i in incs]}")
        inc = H.pick(incs, "bill-svc", INC - timedelta(minutes=5))
        if not r.check("zscore 偵測到 bill-svc 事故", "有", bool(inc), bool(inc)):
            return r
        rf = client.post("/api/v1/tuning/feedback", json={"incident_uid": inc["uid"], "verdict": "true_positive", "text": "確認為真事故（P0）",
                                                         "author": "chaos", "root_cause": "billing-db 變慢", "resolution": "擴充連線池"})
        r.check("確認為真事故，進黃金庫（HTTP 200）", "200", rf.status_code, rf.status_code == 200)
        metric = {"source_type": "elasticsearch", "query": H.metric_query("bill-svc")}
        tr = {"start": (INC - timedelta(hours=6)).isoformat(), "end": (INC - timedelta(hours=1)).isoformat(), "step_seconds": 60}

        def create(threshold):
            return client.post("/api/v1/tuning/proposals", json={"source": "manual", "detector": "detector.zscore", "params": {"threshold": threshold},
                                                                 "proposer": "chaos", "pipeline": "it_apm", "entity": "bill-svc", "reason": "chaos"})

        p25 = create(25.0)
        r.check("使用者指定的 25.0：建立階段就被參數範圍擋下（第一道防線）", "HTTP 400、超出範圍", f"HTTP {p25.status_code}：{p25.text[:70]}", p25.status_code == 400 and "超出範圍" in p25.text)
        r.notes.append("25.0 到不了黃金事故防護罩：zscore.threshold 合法範圍是 [1.0, 10.0]，所以改用範圍上限 10.0 去實測防護罩（事故 z 分數須介於 3 與 10 之間才有意義）")

        def propose(threshold):
            p = create(threshold)
            pid = p.json()["result"]["proposal"]["id"]
            b = client.post("/api/v1/tuning/proposals/backtest", json={"proposal_id": pid, "metric": metric, "time_range": tr, "entity": "bill-svc"})
            return pid, b

        pid, b = propose(10.0)
        res = b.json().get("result", {}).get("proposal", {})
        safety = (res.get("backtest") or {}).get("safety", {})
        missed = safety.get("missed_golden", [])
        r.check("回測 HTTP 200 且 safety.passed 為 false", "passed=false", f"HTTP {b.status_code}；passed={safety.get('passed')}", b.status_code == 200 and safety.get("passed") is False)
        r.check("missed_golden 指名這場事故", f"含 {inc['uid']}", [m.get("incident_uid") for m in missed], any(m.get("incident_uid") == inc["uid"] for m in missed))
        if missed:
            m0 = missed[0]
            r.check("事故窗異常數歸零（現行 → 提案）", "提案 = 0", f"{m0.get('current_anomalies')} → {m0.get('proposed_anomalies')}", m0.get("proposed_anomalies") == 0 and m0.get("current_anomalies", 0) > 0)
        ap = client.post("/api/v1/tuning/proposals/approve", json={"proposal_id": pid, "approver": "chaos"})
        r.check("approve 強制回 HTTP 409", "409", f"{ap.status_code}：{str(ap.json())[:100]}", ap.status_code == 409)
        st = client.get(f"/api/v1/tuning/proposals/{pid}").json()["result"]["proposal"]["status"]
        r.check("提案狀態維持 rejected", "rejected", st, st == "rejected")
        pid2, b2 = propose(3.5)
        s2 = (b2.json().get("result", {}).get("proposal", {}).get("backtest") or {}).get("safety", {})
        ap2 = client.post("/api/v1/tuning/proposals/approve", json={"proposal_id": pid2, "approver": "chaos"})
        r.check("對照：溫和提案（3.0→3.5）通過防護罩並可核准", "passed=true、approve 200", f"passed={s2.get('passed')}；approve {ap2.status_code}", s2.get("passed") is True and ap2.status_code == 200)
    r.notes.append(f"safety 明細：{json.dumps({k: v for k, v in safety.items() if k != 'events'}, ensure_ascii=False, default=str)[:600]}")
    return r


# ───────────────────────── 場景 9：假日前夕流量腰斬 × ACF ─────────────────────────
def scenario_9() -> H.Result:
    from aiops.engine.requests import MetricIn, SeriesProfile
    from aiops.interfaces.runner import run
    from aiops.storage.repository import Proposals

    r = H.Result(9, "假日前夕流量腰斬（ACF 週期檢定邊界）",
                 "bill-svc 的 QPS：連續 3 天連假只有平日的 20%，仍維持日夜起伏；分別在含連假、不含連假、只有 7.5 天資料三種窗口做序列體檢")
    end = datetime(2026, 11, 20, 0, 0, tzinfo=UTC)
    hol_lo = end - timedelta(days=3)
    t0 = end - timedelta(days=21)
    w = World(t0, _mins(t0, end), [Svc("bill-svc", "POST /bills", 30.0, [Call("billing-db", 1.0, "db", "postgresql", 10.0)])], {"bill-svc": 25.0}, seed=3)
    w.rate_mult[w.window(hol_lo, end)] = 0.2
    w.build()
    def qps(agg: str) -> str:
        return json.dumps({"index": "metrics-apm.transaction.1m-*", "agg": agg, "value_field": "transaction.duration.summary.value_count",
                           "filter": {"term": {"service.name": "bill-svc"}}})
    qps_q = qps("avg")
    H.fresh_db()

    def profile(world, at: datetime, q: str | None = None):
        req = SeriesProfile(metric=MetricIn(source_type="elasticsearch", query=q or qps_q), pipeline="it_otel", entity="bill-svc", days=14, step_seconds=300, end=at)
        with H.es_for(world):
            return run("admin_series_profile", request=req, targets=req.to_targets(), now=at)

    with_h = profile(w, end)
    no_h = profile(w, hol_lo)
    short_world = World(end - timedelta(days=7, hours=12), _mins(end - timedelta(days=7, hours=12), end), [Svc("bill-svc", "POST /bills", 30.0, [Call("billing-db", 1.0, "db", "postgresql", 10.0)])], {"bill-svc": 25.0}, seed=3)
    short_world.rate_mult[short_world.window(hol_lo, end)] = 0.2
    short_world.build()
    short = profile(short_world, end)                       # avg 聚合：沒資料的桶是 null，會被丟掉
    short_sum = profile(short_world, end, qps("sum"))       # sum 聚合：真實 ES 對沒資料的桶回 0.0，看起來像「流量為零」
    a, b, c, d = with_h["profile"], no_h["profile"], short["profile"], short_sum["profile"]
    r.notes.append(f"含連假窗口：r24h={a['r_24h']} r168h={a['r_168h']}（配對 {a['r_24h_pairs']}／{a['r_168h_pairs']}）；不含連假：r24h={b['r_24h']} r168h={b['r_168h']}；"
                   f"只有 7.5 天資料（avg 聚合）：r24h={c['r_24h']} r168h={c['r_168h']}（168h 配對 {c['r_168h_pairs']}）；advice={short.get('advice')!r}；"
                   f"同一份資料改用 sum 聚合：r24h={d['r_24h']} r168h={d['r_168h']}（168h 配對 {d['r_168h_pairs']}，points={d['points']}／grid={d['grid_points']}）；advice={short_sum.get('advice')!r}")
    r.check("含連假窗口：精確回報 r(24h)、r(168h)", "兩者都有數值", f"r24h={a['r_24h']}、r168h={a['r_168h']}", a["r_24h"] is not None and a["r_168h"] is not None)
    r.check("連假造成的 r 值變化有被回報（含 vs 不含）", "兩個窗口的 r 值都列出可比", f"Δr24h={None if None in (a['r_24h'], b['r_24h']) else round(a['r_24h'] - b['r_24h'], 4)}、Δr168h={None if None in (a['r_168h'], b['r_168h']) else round(a['r_168h'] - b['r_168h'], 4)}",
            None not in (a["r_24h"], b["r_24h"], a["r_168h"], b["r_168h"]))
    r.check("含連假窗口：仍有日夜週期，不得提議拔除 hourly_baseline", "proposal=None", f"proposal={with_h.get('proposal') is not None}；{with_h.get('advice', '')[:80]}", with_h.get("proposal") is None)
    r.check("168h 配對不足（avg 聚合）：明確標記資料不足、維持既有算子", "r168h=None、advice 說資料不足、proposal=None",
            f"r168h={c['r_168h']}；advice={short.get('advice', '')[:60]}；proposal={short.get('proposal')}", c["r_168h"] is None and "資料不足" in short.get("advice", "") and short.get("proposal") is None)
    r.check("同一份只有 7.5 天的資料改用 sum 聚合：同樣要標記資料不足", "r168h=None、advice 說資料不足", f"r168h={d['r_168h']}；advice={short_sum.get('advice', '')[:60]}",
            d["r_168h"] is None and "資料不足" in short_sum.get("advice", ""))
    advisory = Proposals.with_status("detector.hourly_baseline", "it_otel", "bill-svc", ["advisory"])
    r.check("三次體檢後資料庫沒有任何 advisory 提案", "0 筆", f"{len(advisory)} 筆", not advisory)
    return r


# ───────────────────────── 場景 10：JSONB 併發更新 ─────────────────────────
def scenario_10() -> H.Result:
    import psycopg

    from aiops.storage.repository import Knowledge

    r = H.Result(10, "JSONB 上下文併發更新（真實 PostgreSQL）",
                 "10 個 worker 同時對同一筆事故記錄追加維運標籤、Trace ID 與處方註記（含大量中文與巢狀結構），每個 worker 追加 5 次")
    H.fresh_db()
    uid = "inc-chaos-concurrency"
    t0 = datetime(2026, 11, 3, 15, 40)
    Knowledge.upsert(uid, pipeline="it_otel", entities=["bill-svc"], metrics=["latency"], signals=["latency"], resolution="", root_cause="tax-gw:443",
                     author="amy", verdict="true_positive", severity="P0", window_start=t0, window_end=t0 + timedelta(minutes=30), context={"base": "起始"})
    n_workers, n_rounds = 10, 5
    errors: list[str] = []
    barrier = threading.Barrier(n_workers)

    def worker(i: int):
        try:
            barrier.wait()
            for j in range(n_rounds):
                patch = {f"w{i}_r{j}": {"維運標籤": [f"標籤-{i}", "外部依賴"], "trace_id": f"{i:02d}{j:02d}" + "ab" * 14,
                                        "處方註記": f"worker {i} 第 {j} 次：檢查下游 tax-gw:443 的健康狀態與網路延遲，確認連線池上限"}}
                Knowledge.merge_context(uid, patch)           # 平台的原子追加 API（資料庫端 ||，不經 Python 讀改寫）
        except Exception as e:  # noqa: BLE001
            errors.append(f"w{i}: {type(e).__name__}: {str(e)[:120]}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_workers)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    final = Knowledge.get(uid)["context"]
    expected = n_workers * n_rounds
    got = sum(1 for k in final if k.startswith("w"))
    r.check("沒有 DeadlockDetected／其他資料庫例外", "0 個例外", f"{len(errors)} 個：{errors[:2]}", not errors)
    r.check("平台 API（Knowledge.merge_context）併發追加：context 保留所有 50 個鍵", f"{expected} 個", f"{got} 個（遺失 {expected - got}）", got == expected)
    r.check("併發追加沒有動到既有鍵", "base=起始 仍在", final.get("base"), final.get("base") == "起始")
    kept = [v for k, v in final.items() if k.startswith("w")]
    bad = [v for v in kept if not (isinstance(v, dict) and "維運標籤" in v and "處方註記" in v and "檢查下游" in v["處方註記"])]
    r.check("保留下來的鍵：中文與巢狀結構完整無亂碼", "每個保留的鍵都有維運標籤／處方註記", f"{len(kept) - len(bad)}/{len(kept)} 個完整", bool(kept) and not bad)
    dsn = H.PG_URL.replace("postgresql+psycopg", "postgresql")
    with psycopg.connect(dsn, autocommit=True) as c:
        hit = c.execute("select count(*) from incident_knowledge where context @> %s::jsonb", (json.dumps({"base": "起始"}, ensure_ascii=False),)).fetchone()[0]
        r.check("GIN 索引 @> 立即查得到剛寫入的屬性", "命中 1 筆", hit, hit == 1)
        # 對照組：用 PostgreSQL 原生的原子追加（context = context || patch），看資料庫本身撐不撐得住
        c.execute("update incident_knowledge set context = '{}'::jsonb where incident_uid = %s", (uid,))
    atomic_errors: list[str] = []
    barrier2 = threading.Barrier(n_workers)

    def atomic(i: int):
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                barrier2.wait()
                for j in range(n_rounds):
                    patch = {f"w{i}_r{j}": {"維運標籤": [f"標籤-{i}"], "處方註記": f"原子追加 {i}-{j}"}}
                    conn.execute("update incident_knowledge set context = context || %s::jsonb, updated_at = now() at time zone 'utc' where incident_uid = %s",
                                 (json.dumps(patch, ensure_ascii=False), uid))
        except Exception as e:  # noqa: BLE001
            atomic_errors.append(f"w{i}: {type(e).__name__}: {str(e)[:100]}")

    threads = [threading.Thread(target=atomic, args=(i,)) for i in range(n_workers)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    final2 = Knowledge.get(uid)["context"]
    got2 = sum(1 for k in final2 if k.startswith("w"))
    r.check("對照（PostgreSQL 原生 jsonb || 原子追加）：50 個鍵全保留、無死鎖", f"{expected} 個、0 例外", f"{got2} 個、{len(atomic_errors)} 例外", got2 == expected and not atomic_errors)
    s2 = final2.get("w3_r2", {})
    r.check("對照組中文無亂碼（原生追加）", "處方註記=『原子追加 3-2』", s2.get("處方註記"), s2.get("處方註記") == "原子追加 3-2")
    # 插入競爭：10 個 worker 同時 upsert 同一個「新」uid
    new_uid, ins_errors = "inc-chaos-insert-race", []
    barrier3 = threading.Barrier(n_workers)

    def inserter(i: int):
        try:
            barrier3.wait()
            Knowledge.upsert(new_uid, pipeline="it_otel", entities=["bill-svc"], metrics=[], signals=[], resolution="", root_cause="", author=f"w{i}",
                             verdict="true_positive", severity="P0", window_start=t0, window_end=t0, context={"w": i})
        except Exception as e:  # noqa: BLE001
            ins_errors.append(f"{type(e).__name__}")

    threads = [threading.Thread(target=inserter, args=(i,)) for i in range(n_workers)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    r.check("同一個新事故被 10 個 worker 同時首次寫入：不得有人因唯一鍵衝突失敗", "0 個失敗", f"{len(ins_errors)} 個失敗：{sorted(set(ins_errors))}", not ins_errors)
    r.notes.append(f"平台 API 路徑最終鍵數 {got}/{expected}；原生原子追加 {got2}/{expected}；插入競爭失敗 {len(ins_errors)}/{n_workers}")
    return r
