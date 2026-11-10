"""破壞性端到端場景（1～11 在這個檔；12 非同步佇列在 scen_mq.py、13 流量放大在 scen_traffic_amp.py）。每個場景回傳 Result（注入特徵、逐條斷言的預期與實測、備註）。

原則：斷言寫使用者要求的樣子，不照平台現狀遷就；平台做不到的就老實記成「未通過」並在備註寫原因，不改平台、不放寬斷言。
"""
from __future__ import annotations

import json
import traceback
from datetime import datetime, timedelta, timezone

import numpy as np

from scripts.chaos import harness as H
from scripts.chaos.world import Call, Svc, World

UTC = timezone.utc
T0 = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)          # 世界起點：有 8 天以上歷史給基準線
INC = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)        # 多數場景的事故開始


def _mins(lo: datetime, hi: datetime) -> int:
    return int((hi - lo).total_seconds() // 60)


def _cand(out: dict) -> list[dict]:
    return out.get("root_cause", {}).get("candidates", [])


def _runbook(out: dict) -> str:
    return (out.get("runbook") or {}).get("id", "(無)")


# ───────────────────────── 場景 1：隱性延遲陷阱 ─────────────────────────
def scenario_1() -> H.Result:
    r = H.Result(1, "外部下游超時但應用拋 HTTP 200（隱性延遲陷阱）",
                 "ord-svc 呼叫 payment-gw：20→3000 ms（>95% 耗時在外部）；應用接住逾時、回 PENDING／HTTP 200，交易沒有任何 500")
    end = INC + timedelta(minutes=60)
    w = World(T0, _mins(T0, end + timedelta(minutes=20)), [
        Svc("ord-svc", "POST /orders", 40.0, [Call("inventory-db", 1.0, "db", "postgresql", 10.0, name="SELECT inventory"),
                                              Call("payment-gw:443", 1.0, "external", "http", 20.0, 0.0, swallow=True, name="POST payment-gw")]),
        Svc("cart-api", "GET /cart", 30.0, [Call("redis", 1.0, "db", "redis", 1.0, name="HGETALL cart")]),
        Svc("pay-api", "POST /pay", 25.0, [Call("postgres", 1.0, "db", "postgresql", 8.0, name="INSERT pay")]),
    ], {"ord-svc": 25.0, "cart-api": 25.0, "pay-api": 20.0})
    gw = w.call("ord-svc", "payment-gw:443")
    w.set(gw.ms, INC, end, 3000.0)
    w.set(gw.fail_p, INC, end, 1.0)   # 客戶端 span 逾時（outcome=failure），但被呼叫端接住
    w.build()
    H.fresh_db()
    with H.es_for(w, INC - timedelta(minutes=80), end + timedelta(minutes=5)) as es:
        n500 = sum(1 for d in es.trace_docs if d.get("http", {}).get("response", {}).get("status_code", 0) >= 500)
        n_span_fail = sum(1 for d in es.trace_docs if d["processor"]["event"] == "span" and d["event"]["outcome"] == "failure")
        r.check("世界資料：整段沒有任何 HTTP 500（陷阱成立）", "0 筆 500", f"{n500} 筆 500；{n_span_fail} 個客戶端 span 逾時失敗", n500 == 0 and n_span_fail > 0)
        wf = H.detection_workflow("it_otel", H.targets(["ord-svc", "cart-api", "pay-api"]))
        rp = H.replay(wf, INC - timedelta(minutes=30), end)
        incs = H.incidents()
        r.notes.append(f"偵測：{rp.rounds} 輪、異常 {rp.anomalies} 筆、errors {rp.errors[:2]}、事件群 {[(i['id'], i['incident']['entities']) for i in incs]}")
        inc = next((i for i in incs if "ord-svc" in i["incident"]["entities"]), None)
        if not r.check("偵測到 ord-svc 事件群", "有", bool(inc), bool(inc)):
            return r
        out = H.rca(inc["id"])
    c = _cand(out)
    top = c[0] if c else {}
    a = top.get("attribution", {})
    r.check("RCA 有候選且是 ord-svc", "ord-svc", top.get("entity"), top.get("entity") == "ord-svc")
    r.check("attribution 指向 external/http 的 payment-gw", "payment-gw:443、external、http", f"{a.get('resource')}、{a.get('kind')}、{a.get('subtype')}",
            a.get("resource") == "payment-gw:443" and a.get("kind") == "external" and a.get("subtype") == "http")
    r.check("下鑽佔比 > 90%", "> 0.90", a.get("share"), (a.get("share") or 0) > 0.9)
    r.check("Runbook 命中 RB-01（不是 RB-00）", "RB-01", _runbook(out), _runbook(out) == "RB-01")
    r.notes.append(f"dependency={top.get('dependency')!r}；wait_ms={a.get('wait_ms')}；self_ms={a.get('self_ms')}；errors={out['_errors']}")
    return r


from scripts.chaos import scen_detect_rca as D  # noqa: E402

from scripts.chaos import scen_learning as L  # noqa: E402
from scripts.chaos import scen_mq as Q  # noqa: E402
from scripts.chaos import scen_traffic_amp as A  # noqa: E402

# 場景函式回一個 Result，或一串（12 是 12a～12g 七個子場景、13 是 13a～13c 三個，各自一列）
SCENARIOS = {1: scenario_1, 2: D.scenario_2, 3: D.scenario_3, 4: D.scenario_4, 5: D.scenario_5,
             6: L.scenario_6, 7: L.scenario_7, 8: L.scenario_8, 9: L.scenario_9, 10: L.scenario_10, 11: D.scenario_11,
             12: Q.scenario_12, 13: A.scenario_13}


def run(numbers: list[int] | None = None) -> list[H.Result]:
    results = []
    for n, fn in SCENARIOS.items():
        if numbers and n not in numbers:
            continue
        try:
            out = fn()
            results += out if isinstance(out, list) else [out]
        except Exception as e:  # noqa: BLE001  場景自己崩潰也是結果：回報，不吞
            res = H.Result(n, fn.__doc__ or fn.__name__, "")
            res.crash = f"{type(e).__name__}: {e}\n{traceback.format_exc()[-1500:]}"
            results.append(res)
    return results
