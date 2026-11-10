"""場景 2～5、11：產生 -> 偵測 -> RCA 下鑽 -> Runbook（沿用 scenarios.py 的共用常數與世界模擬器）。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from scripts.chaos import harness as H
from scripts.chaos.world import Call, Svc, World

UTC = timezone.utc
T0 = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
INC = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)


def _mins(lo: datetime, hi: datetime) -> int:
    return int((hi - lo).total_seconds() // 60)


def _cand(out: dict) -> list[dict]:
    return out.get("root_cause", {}).get("candidates", [])


def _runbook(out: dict) -> str:
    return (out.get("runbook") or {}).get("id", "(無)")


def _ts(d: dict) -> datetime:
    return datetime.fromisoformat(d["@timestamp"].replace("Z", "+00:00"))


# ───────────────────────── 場景 2：離峰超低流量（自適應回溯）─────────────────────────
def scenario_2() -> H.Result:
    r = H.Result(2, "離峰超低流量冷啟動（自適應回溯是否發動）",
                 "bill-svc 每 5 分鐘約 2~3 筆請求（事故前 15 分鐘約 8 筆 < 20）；03:00 注入 DB 鎖表：billing-db 8→5000 ms 且 100% 失敗")
    inc = datetime(2026, 11, 9, 3, 0, tzinfo=UTC)
    end = inc + timedelta(minutes=60)
    w = World(T0, _mins(T0, end + timedelta(minutes=20)), [
        Svc("bill-svc", "POST /bills", 30.0, [Call("billing-db", 1.0, "db", "postgresql", 8.0, name="INSERT bill")],
            error_type="PSQLException", error_message="canceling statement due to lock timeout"),
        Svc("pay-api", "POST /pay", 25.0, [Call("postgres", 1.0, "db", "postgresql", 8.0, name="INSERT pay")]),
    ], {"bill-svc": 0.5, "pay-api": 20.0}, seasonal=0.0)
    db_call = w.call("bill-svc", "billing-db")
    w.set(db_call.ms, inc, end, 5000.0)
    w.set(db_call.fail_p, inc, end, 1.0)
    w.build()
    H.fresh_db()
    with H.es_for(w, inc - timedelta(minutes=80), end + timedelta(minutes=5)) as es:
        pre = sum(1 for d in es.trace_docs if d["service"]["name"] == "bill-svc" and d["processor"]["event"] == "transaction"
                  and inc - timedelta(minutes=15) <= _ts(d) < inc)
        r.check("世界資料：事故前 15 分鐘 bill-svc 入口請求 < 20", "< 20（約 8）", f"{pre} 筆", pre < 20)
        # bill-svc 每分鐘 0～3 筆：延遲用「每筆請求的平均」（ES 查詢的 divide_by）。原本的 avg(summary.sum) 是當分鐘總耗時，
        # 01:33 那一分鐘剛好 3 筆 = 平常的 3 倍，注入前就誤報（缺口 9）。世界資料與注入都沒動，只有這條 target 的查詢換了。
        wf = H.detection_workflow("it_otel", H.targets(["bill-svc"], kind="latency_mean") + H.targets(["pay-api"]))
        rp = H.replay(wf, inc - timedelta(minutes=30), end)
        incs = H.incidents()
        r.notes.append(f"偵測：{rp.rounds} 輪、異常 {rp.anomalies} 筆、errors {rp.errors[:2]}、事件群 {[(i['id'], i['incident']['entities']) for i in incs]}")
        early = H.before(incs, "bill-svc", inc - timedelta(minutes=5))
        r.check("注入之前沒有誤報事件群（稀疏流量不能自己亂叫）", "0 個", f"{len(early)} 個（id, 開始時間）={early}", not early)
        target = H.pick(incs, "bill-svc", inc - timedelta(minutes=5))
        if not r.check("稀疏流量下仍偵測到注入的 bill-svc 事件群", "有", bool(target), bool(target)):
            return r
        out = H.rca(target["id"])
    tf = out["_ctx"].result.get("trace_fetch", {})
    c = _cand(out)
    top = c[0] if c else {}
    a = top.get("attribution", {})
    sec = out["_sections"]
    note = out.get("root_cause", {}).get("note", "")
    r.check("RCA 沒有直接放棄（不是「證據不足／trace 不足」）", "有根因候選", f"候選 {len(c)} 個；note={note[:90]!r}", bool(c))
    r.check("基準窗自適應擴展到 60 分鐘", "before_minutes 15 → before_minutes_used 60",
            f"{tf.get('before_minutes')}→{tf.get('before_minutes_used')}；{tf.get('extended_for')!r}", tf.get("before_minutes_used") == 60)
    ok = "擴展至 60 分鐘" in sec.get("trace_evidence", "")
    r.check("報告明確標出「擴展至 60 分鐘」", "trace 佐證段落含『擴展至 60 分鐘』", ok, ok)
    r.check("耗時歸因產出（db）", "billing-db、db", f"{a.get('resource')}、{a.get('kind')}、share={a.get('share')}",
            a.get("resource") == "billing-db" and a.get("kind") == "db")
    fl = a.get("failures") or {}
    r.check("錯誤歸因產出（資源失敗增加）", "failures.during > failures.before", fl, fl.get("during", 0) > fl.get("before", 0))
    r.check("Runbook 命中 RB-02", "RB-02", _runbook(out), _runbook(out) == "RB-02")
    r.notes.append(f"trace_fetch={json.dumps(tf, ensure_ascii=False)[:300]}；errors={out['_errors']}")
    return r


# ───────────────────────── 場景 3：兩個下游平分延遲 ─────────────────────────
def scenario_3() -> H.Result:
    r = H.Result(3, "兩個下游平分延遲（45% vs 45% 死區）",
                 "cart-api 總耗時 +1000 ms：redis +450、postgres +450、自身 +100（各佔 45%／45%／10%，沒有單一資源過 60%）")
    end = INC + timedelta(minutes=45)
    w = World(T0, _mins(T0, end + timedelta(minutes=20)), [
        Svc("cart-api", "GET /cart", 30.0, [Call("redis", 1.0, "db", "redis", 2.0, name="HGETALL cart"),
                                            Call("postgres", 1.0, "db", "postgresql", 5.0, name="SELECT cart")]),
        Svc("pay-api", "POST /pay", 25.0, [Call("pg-pay", 1.0, "db", "postgresql", 8.0, name="INSERT pay")]),
    ], {"cart-api": 25.0, "pay-api": 20.0})
    w.set(w.svcs["cart-api"].self_ms, INC, end, 130.0)           # +100
    w.set(w.call("cart-api", "redis").ms, INC, end, 452.0)        # +450
    w.set(w.call("cart-api", "postgres").ms, INC, end, 455.0)     # +450
    w.build()
    H.fresh_db()
    with H.es_for(w, INC - timedelta(minutes=80), end + timedelta(minutes=5)):
        wf = H.detection_workflow("it_otel", H.targets(["cart-api", "pay-api"]))
        rp = H.replay(wf, INC - timedelta(minutes=30), end)
        incs = H.incidents()
        r.notes.append(f"偵測：{rp.rounds} 輪、異常 {rp.anomalies} 筆、errors {rp.errors[:2]}、事件群 {[(i['id'], i['incident']['entities']) for i in incs]}")
        target = H.pick(incs, "cart-api", INC - timedelta(minutes=5))
        if not r.check("偵測到 cart-api 事件群", "有", bool(target), bool(target)):
            return r
        out = H.rca(target["id"])
    c = _cand(out)
    top = c[0] if c else {}
    a = top.get("attribution", {})
    reason = top.get("reason", "")
    r.check("候選是 cart-api（不是 redis 或 postgres）", "cart-api", top.get("entity"), top.get("entity") == "cart-api")
    r.check("dependency 人類字串留空（沒有單一資源 > 60%）", "''", repr(top.get("dependency")), top.get("dependency") == "")
    r.check("attribution.by 為空（沒有主導資源）", "''", repr(a.get("by")), a.get("by") == "")
    blob = json.dumps(a, ensure_ascii=False)
    both = "redis" in blob and "postgres" in blob
    r.check("attribution 結構體記錄『兩個』資源的數值", "redis 與 postgres 都在 attribution 裡", f"resource={a.get('resource')!r}（share {a.get('share')}）；others={[(o['resource'], o['share']) for o in a.get('others', [])]}", both)
    both_text = "redis" in reason and "postgres" in reason
    r.check("理由文字（reason）列出兩個資源的等待", "reason 含 redis 與 postgres", both_text, both_text)
    rb = _runbook(out)
    r.check("Runbook 為 RB-06 或優雅退回 RB-00，不得把罪名扣給單一資源", "RB-06／RB-00（不是 RB-01／RB-02）", rb, rb in ("RB-06", "RB-00"))
    r.notes.append(f"attribution={blob}；reason 摘要={reason[:380]}；建議處置前 3 行={out['_sections'].get('suggested_actions', '').splitlines()[:3]}")
    return r


# ───────────────────────── 場景 4：跨服務連鎖雪崩 ─────────────────────────
def topology_refresh(lo: datetime, hi: datetime) -> dict:
    """不手動灌邊：跟生產排程同一條路（admin.topology_refresh），從世界的 trace 來源（平台的 elasticsearch 資料源）
    撈 [lo, hi] 的 span、算出服務呼叫邊、沉澱進 service_edges。偵測時 ctx.topology 預設就讀這張表。"""
    from aiops.engine.requests import TopologyRefresh
    from aiops.interfaces.runner import run

    return run("admin_topology_refresh",
               request=TopologyRefresh(source_type="elasticsearch", end=hi, lookback_minutes=_mins(lo, hi)))


def scenario_4() -> H.Result:
    r = H.Result(4, "跨服務連鎖雪崩（上游背黑鍋）",
                 "gateway→order-api→inventory-svc→oracle；oracle 12→4000 ms 且 50% 逾時失敗，三個服務同時變慢，gateway 流量最大")
    end = INC + timedelta(minutes=45)
    w = World(T0, _mins(T0, end + timedelta(minutes=20)), [
        Svc("gateway", "GET /checkout", 5.0, [Call("order-api", 1.0, "service", ms=1.5)]),
        Svc("order-api", "POST /orders", 20.0, [Call("inventory-svc", 1.0, "service", ms=1.5)]),
        Svc("inventory-svc", "GET /stock", 15.0, [Call("oracle", 1.0, "db", "oracle", 12.0, name="SELECT stock")]),
    ], {"gateway": 40.0})
    orc = w.call("inventory-svc", "oracle")
    w.set(orc.ms, INC, end, 4000.0)
    w.set(orc.fail_p, INC, end, 0.5)
    w.build()
    H.fresh_db()
    with H.es_for(w, INC - timedelta(minutes=90), end + timedelta(minutes=5)) as es:
        topo = topology_refresh(INC - timedelta(minutes=90), INC - timedelta(minutes=30))  # 偵測回放開始前、事故前 60 分鐘
        r.notes.append(f"拓撲（admin.topology_refresh，事故前 {topo['window']['start']}～{topo['window']['end']}）："
                       f"span {topo['spans']}（truncated={topo['truncated']}）、邊 {topo['edges']}、"
                       f"新增 {topo['edges_added']}／更新 {topo['edges_updated']}／移除 {topo['edges_pruned']}")
        seen = {(e["caller"], e["callee"]) for e in topo["edges"]}
        r.check("拓撲由排程動作從 trace 沉澱（不手動灌邊）", "gateway→order-api、order-api→inventory-svc",
                sorted(seen), seen == {("gateway", "order-api"), ("order-api", "inventory-svc")})
        wf = H.detection_workflow("it_otel", H.targets(["gateway", "order-api", "inventory-svc"]))
        rp = H.replay(wf, INC - timedelta(minutes=30), end)
        incs = H.incidents()
        first = sorted(rp.first_alarm.items(), key=lambda kv: kv[1])
        r.notes.append(f"偵測：異常 {rp.anomalies} 筆、errors {rp.errors[:2]}；最早告警順序 {[(k, v.strftime('%H:%M')) for k, v in first]}；"
                       f"事件群 {[(i['id'], i['incident']['entities']) for i in incs]}")
        groups = [set(i["incident"]["entities"]) for i in incs]
        together = any({"gateway", "order-api", "inventory-svc"} <= g for g in groups)
        r.check("三個服務歸為同一個 Incident", "1 個事件群含 gateway、order-api、inventory-svc", [sorted(g) for g in groups], together)
        target = H.pick(incs, "inventory-svc", INC - timedelta(minutes=5))
        if not target:
            return r
        out = H.rca(target["id"])
    c = _cand(out)
    top = c[0] if c else {}
    a = top.get("attribution", {})
    r.check("根因第 1 名是 inventory-svc（不是最早哀號的 gateway）", "inventory-svc", [x["entity"] for x in c], top.get("entity") == "inventory-svc")
    r.check("下鑽到 oracle（db）", "oracle、db", f"{a.get('resource')}、{a.get('kind')}、share={a.get('share')}",
            a.get("resource") == "oracle" and a.get("kind") == "db")
    r.check("Runbook RB-02", "RB-02", _runbook(out), _runbook(out) == "RB-02")
    r.notes.append(f"候選排序：{[(x['entity'], x['score']) for x in c]}；傳播路徑段落是否產出：{'propagation_path' in out['_sections']}")
    return r


# ───────────────────────── 場景 5：純程式 Exception、沒有子 Span ─────────────────────────
def scenario_5() -> H.Result:
    r = H.Result(5, "純程式 Exception，完全沒有子 Span（葉節點判斷邊界）",
                 "cart-api 遇髒資料，controller 第一行拋 NullPointerException（HTTP 500）；整條 trace 只有 root transaction，沒有任何子 span")
    end = INC + timedelta(minutes=45)
    w = World(T0, _mins(T0, end + timedelta(minutes=20)), [
        Svc("cart-api", "GET /cart", 30.0, [Call("redis", 1.0, "db", "redis", 2.0, name="HGETALL cart")], own_fail_p=0.01,
            error_type="java.lang.NullPointerException", error_message="Cannot invoke Cart.getItems() because cart is null"),
        Svc("pay-api", "POST /pay", 25.0, [Call("postgres", 1.0, "db", "postgresql", 8.0, name="INSERT pay")]),
    ], {"cart-api": 25.0, "pay-api": 20.0})
    w.set(w.svcs["cart-api"].crash_p, INC, end, 1.0)
    w.build()
    H.fresh_db()
    with H.es_for(w, INC - timedelta(minutes=80), end + timedelta(minutes=5)) as es:
        inc_docs = [d for d in es.trace_docs if d["service"]["name"] == "cart-api" and INC <= _ts(d) < end]
        kids = sum(1 for d in inc_docs if d["processor"]["event"] == "span")
        tx500 = sum(1 for d in inc_docs if d["processor"]["event"] == "transaction" and d["http"]["response"]["status_code"] == 500)
        r.check("世界資料：事故期間 cart-api 只有 root transaction", "0 個子 span、有 500", f"{kids} 個子 span、{tx500} 筆 500", kids == 0 and tx500 > 0)
        # 事故期間延遲反而變短，看延遲偵測不到：cart-api 用「每分鐘失敗請求數」
        wf = H.detection_workflow("it_otel", H.targets(["cart-api"], signal="errors", kind="errors") + H.targets(["pay-api"]))
        rp = H.replay(wf, INC - timedelta(minutes=30), end)
        incs = H.incidents()
        r.notes.append(f"偵測：異常 {rp.anomalies} 筆、errors {rp.errors[:2]}、事件群 {[(i['id'], i['incident']['entities']) for i in incs]}")
        early = H.before(incs, "cart-api", INC - timedelta(minutes=5))
        r.check("注入之前沒有誤報事件群（零基準的失敗數序列不能自己亂叫）", "0 個", f"{len(early)} 個（id, 開始時間）={early}", not early)
        target = H.pick(incs, "cart-api", INC - timedelta(minutes=5))
        if not r.check("偵測到 cart-api 事件群（失敗數飆升）", "有", bool(target), bool(target)):
            return r
        out = H.rca(target["id"])
    c = _cand(out)
    top = c[0] if c else {}
    a = top.get("attribution", {})
    sec = out["_sections"]
    r.check("RCA 沒有因為沒有子 span 而崩潰", "errors 為空、報告有產出", f"errors={out['_errors']}；段落 {sorted(sec)[:5]}…", not out["_errors"] and bool(sec))
    r.check("候選是 cart-api", "cart-api", top.get("entity"), top.get("entity") == "cart-api")
    r.check("attribution 為空（沒有任何外部資源）", "{}", a, a == {})
    r.check("Runbook 命中 RB-03（應用邏輯錯誤）", "RB-03", _runbook(out), _runbook(out) == "RB-03")
    acts = sec.get("suggested_actions", "")
    r.check("處方含檢查入參與回滾（Rollback）", "含『入參』與『回滾』", f"入參={'入參' in acts}、回滾={'回滾' in acts}", "入參" in acts and "回滾" in acts)
    r.notes.append(f"reason 摘要={top.get('reason', '')[:300]}；trace 佐證前 3 行={sec.get('trace_evidence', '').splitlines()[:3]}")
    return r


# ───────────────────────── 場景 11：並行子 span（區間聯集）─────────────────────────
def _legacy_self(spans) -> dict[str, float]:
    """改版前的自身耗時：耗時 − 直接子 span 耗時加總（下限 0）。只用來對照「並行會被雙重扣成 0」。"""
    kids: dict[str, float] = {}
    for s in spans:
        if s.parent_id:
            kids[s.parent_id] = kids.get(s.parent_id, 0.0) + s.duration_ms
    return {s.span_id: max(0.0, s.duration_ms - kids.get(s.span_id, 0.0)) for s in spans}


def _per_request(spans, selfs, svc: str, lo: datetime, hi: datetime) -> tuple[float, dict[str, float]]:
    """(服務每請求自身耗時含等待, 資源 -> 每請求等待)：跟 stages/rca/spans.window_stats 同一個定義，自身耗時換成 selfs。"""
    service_of = {s.span_id: s.service for s in spans}
    has_kids = {s.parent_id for s in spans if s.parent_id}
    own, n, wait = 0.0, 0, {}
    for s in spans:
        if s.service != svc or not (lo <= s.ts < hi):
            continue
        own += selfs[s.span_id]
        if s.span_id not in has_kids and s.kind not in ("", "app"):
            res = s.resource or s.subtype or s.kind
            wait[res] = wait.get(res, 0.0) + selfs[s.span_id]
        if s.parent_id is None or service_of.get(s.parent_id) != svc:
            n += 1
    return (own / n, {k: v / n for k, v in wait.items()}) if n else (0.0, {})


def scenario_11() -> H.Result:
    from aiops.stages.rca.spans import self_times_detail, window_stats
    from scripts.chaos.world import iso

    r = H.Result(11, "並行子 span（asyncio.gather 同時打 DB 與外部金流）",
                 "ord-svc 自身 40 ms，同一起點並行呼叫 inventory-db 200 ms 與 payment-gw:443 200 ms；"
                 "12:00 起 payment-gw 200→800 ms、同時 ord-svc 自身 40→240 ms（程式也變慢）。"
                 "真值：程式自身 40→240、payment-gw 等待 200→800，歸因佔比 = 600 ÷ (200 + 600) = 0.75")
    end = INC + timedelta(minutes=40)
    w = World(T0, _mins(T0, end + timedelta(minutes=20)), [
        Svc("ord-svc", "POST /orders", 40.0, [Call("inventory-db", 1.0, "db", "postgresql", 200.0, name="SELECT inventory"),
                                              Call("payment-gw:443", 1.0, "external", "http", 200.0, name="POST payment-gw")],
            parallel=True),
        Svc("cart-api", "GET /cart", 30.0, [Call("redis", 1.0, "db", "redis", 1.0, name="HGETALL cart")]),
        Svc("pay-api", "POST /pay", 25.0, [Call("postgres", 1.0, "db", "postgresql", 8.0, name="INSERT pay")]),
    ], {"ord-svc": 25.0, "cart-api": 25.0, "pay-api": 20.0})
    w.set(w.call("ord-svc", "payment-gw:443").ms, INC, end, 800.0)
    w.set(w.svcs["ord-svc"].self_ms, INC, end, 240.0)
    w.build()
    H.fresh_db()
    with H.es_for(w, INC - timedelta(minutes=80), end + timedelta(minutes=5)) as es:
        by_tx: dict[str, list[dict]] = {}
        for d in es.trace_docs:
            if d["service"]["name"] == "ord-svc" and d["processor"]["event"] == "span":
                by_tx.setdefault(d["parent"]["id"], []).append(d)
        same_start = sum(1 for ks in by_tx.values() if len(ks) == 2 and ks[0]["timestamp"]["us"] == ks[1]["timestamp"]["us"])
        r.check("世界資料：ord-svc 兩個子 span 同一起點（並行成立）", "全部同一起點", f"{same_start}／{len(by_tx)} 個請求",
                same_start == len(by_tx) > 0)
        wf = H.detection_workflow("it_otel", H.targets(["ord-svc", "cart-api", "pay-api"]))
        rp = H.replay(wf, INC - timedelta(minutes=30), end)
        incs = H.incidents()
        r.notes.append(f"偵測：{rp.rounds} 輪、異常 {rp.anomalies} 筆、errors {rp.errors[:2]}、"
                       f"事件群 {[(i['id'], i['incident']['entities']) for i in incs]}")
        target = H.pick(incs, "ord-svc", INC - timedelta(minutes=5))
        if not r.check("偵測到 ord-svc 事件群", "有", bool(target), bool(target)):
            return r
        out = H.rca(target["id"])
        ctx = out["_ctx"]
        spans = ctx.rca_spans
        inc = ctx.incidents[0]
        start, stop = inc.start, inc.end + timedelta(minutes=1)
        first = min(s.ts for s in spans)
        before, during = window_stats(spans, first, start), window_stats(spans, start, stop)
        c = _cand(out)
        top = c[0] if c else {}
        a = top.get("attribution", {})
        sm, wm = a.get("self_ms", {}), a.get("wait_ms", {})
        r.check("候選是 ord-svc、下鑽到 payment-gw:443", "ord-svc／payment-gw:443", f"{top.get('entity')}／{a.get('resource')}",
                top.get("entity") == "ord-svc" and a.get("resource") == "payment-gw:443")
        r.check("事件前程式自身 ≈ 40 ms（不再被雙重扣成 0）", "40 ±6 ms", sm.get("before"), abs((sm.get("before") or 0) - 40) <= 6)
        r.check("事件中程式自身 ≈ 240 ms", "240 ±36 ms", sm.get("during"), abs((sm.get("during") or 0) - 240) <= 36)
        r.check("payment-gw 等待 200→800 ms", "200 ±30 → 800 ±120", f"{wm.get('before')}→{wm.get('during')}",
                abs((wm.get("before") or 0) - 200) <= 30 and abs((wm.get("during") or 0) - 800) <= 120)
        # 改版前（加總扣除）對同一份 span 算的佔比：並行時程式自身被吃掉，等待佔比被灌水
        legacy = _legacy_self(spans)
        lb, lwb = _per_request(spans, legacy, "ord-svc", first, start)
        ld, lwd = _per_request(spans, legacy, "ord-svc", start, stop)
        gw = "payment-gw:443"
        legacy_share = round(min(max((lwd.get(gw, 0.0) - lwb.get(gw, 0.0)) / (ld - lb), 0.0), 1.0), 3) if ld > lb else 0.0
        legacy_app_before = round(lb - sum(lwb.values()), 1)
        legacy_app_during = round(ld - sum(lwd.values()), 1)
        share = a.get("share") or 0.0
        r.check("歸因佔比 ≈ 真值 0.75（不被灌水）", "0.75 ±0.05，且低於改版前算法", f"{share}（改版前算法 {legacy_share}）",
                abs(share - 0.75) <= 0.05 and share < legacy_share)
        skew = {s: (before[s].clock_skew_spans if s in before else 0, during[s].clock_skew_spans) for s in during}
        r.check("沒有時鐘偏移：clock_skew_spans 全部 0", "全部 (0, 0)", skew, all(x == 0 for pair in skew.values() for x in pair))
        has_line = "時鐘偏移：" in out["_sections"].get("trace_evidence", "")
        r.check("報告沒有時鐘偏移那一行", "不含『時鐘偏移：』", has_line, not has_line)
        r.notes.append(f"改版前算法（耗時 − 子 span 加總，下限 0）對同一份 span：程式自身 {legacy_app_before}→{legacy_app_during} ms"
                       f"（真值 40→240）、佔比 {legacy_share}（真值 0.75）；新算法 {sm.get('before')}→{sm.get('during')} ms、佔比 {share}")

        # 變體：時鐘偏移——事件期間挑 20 個 ord-svc 請求，把 payment-gw span 的起點挪到父 span 之前 30 ms、
        # 耗時拉長到蓋過整個父 span：子區間夾進父區間後蓋滿 -> 父 span 自身被夾成 0，並記一筆時鐘偏移
        tx = {d["transaction"]["id"]: d for d in es.trace_docs
              if d["processor"]["event"] == "transaction" and d["service"]["name"] == "ord-svc"}
        picked = 0
        for d in es.trace_docs:
            if picked >= 20:
                break
            if not (d["processor"]["event"] == "span" and d["service"]["name"] == "ord-svc"
                    and d["span"]["destination"]["service"]["resource"] == gw):
                continue
            p = tx[d["parent"]["id"]]
            if not (start + timedelta(minutes=2) <= _ts(p) < stop - timedelta(minutes=2)):
                continue
            new_us = p["timestamp"]["us"] - 30_000
            d["timestamp"]["us"] = new_us
            d["@timestamp"] = iso(datetime.fromtimestamp(new_us / 1e6, tz=UTC))
            d["span"]["duration"]["us"] = p["transaction"]["duration"]["us"] + 60_000
            picked += 1
        es.trace_docs.sort(key=lambda d: (d["@timestamp"], d["trace"]["id"]))
        out2 = H.rca(target["id"])
        sp2 = out2["_ctx"].rca_spans
        selfs2, skewed2 = self_times_detail(sp2)
        moved = [s for s in sp2 if s.service == "ord-svc" and s.parent_id is None and s.span_id in skewed2]
        zero = sum(1 for s in moved if selfs2[s.span_id] == 0.0)
        d2 = window_stats(sp2, start, stop)
        body = out2["_sections"].get("trace_evidence", "")
        r.check("變體：被挪的請求父 span 自身被夾成 0", f"{picked} 筆都是 0", f"{zero}／{len(moved)}", zero == len(moved) == picked > 0)
        r.check("變體：clock_skew_spans 計數", f"ord-svc 事件期間 {picked}", d2["ord-svc"].clock_skew_spans if "ord-svc" in d2 else None,
                "ord-svc" in d2 and d2["ord-svc"].clock_skew_spans == picked)
        line = next((ln.strip() for ln in body.splitlines() if "時鐘偏移" in ln), "(沒有)")
        r.check("變體：報告印出時鐘偏移那一行", f"含『ord-svc {picked} 筆』", line, f"ord-svc {picked} 筆" in body)
    return r
