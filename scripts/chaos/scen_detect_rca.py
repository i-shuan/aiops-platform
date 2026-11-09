"""場景 2～5：產生 -> 偵測 -> RCA 下鑽 -> Runbook（沿用 scenarios.py 的共用常數與世界模擬器）。"""
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
        wf = H.detection_workflow("it_otel", H.targets(["bill-svc", "pay-api"]))
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
    r.check("attribution 結構體記錄『兩個』資源的數值", "redis 與 postgres 都在 attribution 裡", f"attribution 只記單一 resource={a.get('resource')!r}", both)
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
