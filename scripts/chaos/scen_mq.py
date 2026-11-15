"""場景 12：訊息佇列（非同步邊）——事故在生產者發布的那段時間，症狀卻在消費者、而且晚了一段延遲才出現。

七個子場景共用同一個世界骨架：ord-api（入口，寫 orders-db 後把事件送進 kafka/order-events）→ ship-worker（消費者，寫 ship-db），
外加一個不相干的 cart-api 當對照。每個子場景都跑完整的偵測回放與 rca_report，RCA 跑兩次：
  改前 = fetch_traces 的 follow_messaging 關掉（等同還沒有這個功能）；改後 = 管線 YAML 的現行設定。
斷言照設計稿（docs/MQ_QPS_DESIGN.md 第 4 節）寫，不照平台現狀遷就；做不到的子場景（12e）用「已知死區」標記留著，不改波形粉飾。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np

from scripts.chaos import harness as H
from scripts.chaos.world import Call, Svc, World

UTC = timezone.utc
T0 = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
INC = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
PRODUCER, CONSUMER, BROKER, QUEUE = "ord-api", "ship-worker", "kafka/order-events", "order-events"


def _mins(lo: datetime, hi: datetime) -> int:
    return int((hi - lo).total_seconds() // 60)


def _hm(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%H:%M:%S")


def _world(end: datetime, rate: float = 30.0, **send) -> World:
    return World(T0, _mins(T0, end + timedelta(minutes=20)), [
        Svc(PRODUCER, "POST /orders", 30.0, [
            Call("orders-db", 1.0, "db", "postgresql", 20.0, name="INSERT orders"),
            Call(BROKER, 1.0, "messaging", "kafka", 2.0, name=f"Kafka SEND to {QUEUE}", consumer=CONSUMER, queue=QUEUE, **send)]),
        Svc(CONSUMER, f"Kafka RECEIVE from {QUEUE}", 20.0, [Call("ship-db", 1.0, "db", "postgresql", 200.0, name="UPDATE shipments")],
            own_fail_p=0.002, error_type="ShipmentError", error_message="shipment update failed"),
        Svc("cart-api", "GET /cart", 30.0, [Call("redis", 1.0, "db", "redis", 2.0, name="HGETALL cart")]),
    ], {PRODUCER: rate, "cart-api": 25.0})


def _bad_release(w: World, lo: datetime, hi: datetime, poison: float = 0.5, db_ms: float | None = 400.0) -> None:
    """生產者的壞版本：這段時間發出去的訊息有 poison 的比例 payload 壞掉；db_ms 給了的話它寫 orders-db 也同時變慢。"""
    if db_ms is not None:
        w.set(w.call(PRODUCER, "orders-db").ms, lo, hi, db_ms)
    w.set(w.call(PRODUCER, BROKER).poison, lo, hi, poison)


def _targets(consumer_signal: str) -> list:
    """生產者與對照組看延遲；消費者看延遲（12a：它自己變慢）或每分鐘失敗筆數（毒訊息：耗時不變，只是失敗）。"""
    kind = "errors" if consumer_signal == "errors" else "latency"
    return H.targets([PRODUCER, "cart-api"]) + H.targets([CONSUMER], signal=consumer_signal, kind=kind)


def _follow(on: bool):
    def mutate(wf) -> None:
        next(n for n in wf.nodes if n.name == "fetch_traces").params["follow_messaging"] = on
    return mutate


def _cand(out: dict) -> list[dict]:
    return out.get("root_cause", {}).get("candidates", [])


def _names(out: dict) -> list[str]:
    return [c["entity"] for c in _cand(out)]


def _top(out: dict) -> dict:
    c = _cand(out)
    return c[0] if c else {}


def _of(out: dict, entity: str) -> dict:
    return next((c for c in _cand(out) if c["entity"] == entity), {})


def _rank(out: dict, entity: str) -> int | None:
    names = _names(out)
    return names.index(entity) + 1 if entity in names else None


def _runbook(out: dict) -> str:
    return (out.get("runbook") or {}).get("id", "(無)")


def _mq(out: dict) -> dict:
    return out["_ctx"].result.get("trace_fetch", {}).get("mq") or {}


def _note(out: dict) -> str:
    return out.get("root_cause", {}).get("messaging", {}).get("note", "")


def _edges(out: dict) -> list[dict]:
    return out.get("root_cause", {}).get("messaging", {}).get("edges", [])


def _line(tag: str, out: dict, requests: int) -> str:
    """驗收表的一列：第一名、生產者名次、Runbook、span 數與查詢輪數。"""
    tf = out["_ctx"].result.get("trace_fetch", {})
    mq = tf.get("mq") or {}
    scores = "、".join(f"{c['entity']} {c['score']:.2f}" for c in _cand(out)) or f"（無候選：{out['root_cause'].get('note', '')[:80]}）"
    return (f"{tag}：候選 [{scores}]；生產者名次 {_rank(out, PRODUCER)}；Runbook {_runbook(out)}；span {tf.get('spans')} 筆"
            f"（跨時窗補撈 {mq.get('extra_spans', 0)} 筆、{mq.get('queries', 0)} 輪查詢）；RCA 期間 ES 請求 {requests} 次")


def _play(r: H.Result, w: World, end: datetime, consumer_signal: str, victim: str, not_before: datetime,
          trace_lo: datetime | None = None, refresh: tuple[datetime, datetime] | None = None) -> dict | None:
    """建世界 → 偵測回放 → 對 victim 的事件群跑 RCA 兩次（改前／改後）。偵測不到就記一條未通過並回 None。"""
    w.build()
    H.fresh_db()
    with H.es_for(w, trace_lo or INC - timedelta(minutes=80), end + timedelta(minutes=5)) as es:
        topo = None
        if refresh:
            from scripts.chaos.scen_detect_rca import topology_refresh
            topo = topology_refresh(*refresh)
        rp = H.replay(H.detection_workflow("sample_it_otel", _targets(consumer_signal)), INC - timedelta(minutes=30), end)
        incs = H.incidents()
        r.notes.append(f"偵測：{rp.rounds} 輪、異常 {rp.anomalies} 筆、errors {rp.errors[:2]}、"
                       f"事件群 {[(i['id'], i['incident']['entities'], i['incident']['start'][11:16]) for i in incs]}")
        early = H.before(incs, victim, not_before)
        r.check(f"注入的效果傳到 {victim} 之前沒有誤報事件群", "0 個", f"{len(early)} 個（id, 開始時間）={early}", not early)
        target = H.pick(incs, victim, not_before)
        if not r.check(f"偵測到 {victim} 事件群", "有", bool(target), bool(target)):
            return None
        n0 = len(es.requests)
        old = H.rca(target["id"], _follow(False))
        n1 = len(es.requests)
        new = H.rca(target["id"])
        n2 = len(es.requests)
    inc = new["_ctx"].incidents[0]
    r.notes.append(_line("改前（follow_messaging: false）", old, n1 - n0))
    r.notes.append(_line("改後（管線現行設定）", new, n2 - n1))
    r.notes.append(f"改後第一名理由：{_top(new).get('reason', '')[:600]}")
    if new["_errors"] or old["_errors"]:
        r.notes.append(f"errors：改前 {old['_errors']}；改後 {new['_errors']}")
    return {"old": old, "new": new, "incs": incs, "target": target, "start": inc.start, "end": inc.end, "topo": topo}


def _pub_window_check(r: H.Result, w: World, p: dict, fault: tuple[datetime, datetime] | None = None) -> dict | None:
    """平台還原的發布時窗 vs 世界真值（事件期間第一則與最後一則被消費的訊息，各自是幾點發的）。兩端誤差都要在 3 分鐘內
    （時窗是 T_pub 的 P5～P95 再各留 2 分鐘緩衝，所以本來就不會剛好等於頭尾）。"""
    edge = next((e for e in _edges(p["new"]) if e["consumer"] == CONSUMER), None)
    if not r.check("還原出 ord-api → ship-worker 這條非同步邊", "有", [(e["producer"], e["consumer"], e["linked_by"]) for e in _edges(p["new"])], bool(edge)):
        return None
    truth_lo = w.published_at(PRODUCER, BROKER, p["start"])
    truth_hi = w.published_at(PRODUCER, BROKER, p["end"] + timedelta(minutes=1))
    lo, hi = (datetime.fromisoformat(x) for x in edge["pub_window"])
    err_lo, err_hi = (lo - truth_lo).total_seconds() / 60, (hi - truth_hi).total_seconds() / 60
    r.check("發布時窗（pub_window）對得上真值", "兩端誤差都在 ±3 分鐘內",
            f"平台 {_hm(lo)}–{_hm(hi)}；真值 {_hm(truth_lo)}–{_hm(truth_hi)}（事件 {_hm(p['start'])}–{_hm(p['end'])} 消費的那批）；"
            f"誤差 起 {err_lo:+.1f} 分、迄 {err_hi:+.1f} 分", abs(err_lo) <= 3 and abs(err_hi) <= 3)
    if fault:
        overlap = (min(hi, fault[1]) - max(lo, fault[0])).total_seconds() / 60
        r.notes.append(f"發布時窗與注入區間 {_hm(fault[0])}–{_hm(fault[1])} 重疊 {max(overlap, 0):.1f} 分鐘（注入共 {_mins(*fault)} 分鐘）；"
                       f"edge = {edge}")
    return edge


# ───────────────────────── 12a：消費者自己慢（對照組）─────────────────────────
def scenario_12a() -> H.Result:
    r = H.Result("12a", "消費者自己的 DB 變慢、處理不完而積壓（對照組：不准怪生產者）",
                 "12:00 起 ship-worker 的 ship-db 200→800 ms（單一 worker：處理能力 272→73 則／分，進件約 91 則／分），佇列開始積壓；ord-api 完全正常")
    end = INC + timedelta(minutes=40)
    w = _world(end, rate=120.0, workers=1)
    w.set(w.call(CONSUMER, "ship-db").ms, INC, end, 800.0)
    w.build()
    q = w.queues[(PRODUCER, BROKER)]
    lag = q.lag_minutes()
    m0, m1 = w.m(INC), w.m(end)
    r.check("世界資料：事故前不積壓、事故期間延遲一路拉長", "事故前延遲 < 0.1 分；事故結束時 > 5 分",
            f"事故前 30 分鐘最大延遲 {lag[m0 - 30:m0].max():.2f} 分；12:15 {lag[m0 + 15]:.1f} 分、12:30 {lag[m0 + 30]:.1f} 分、12:39 {lag[m1 - 1]:.1f} 分；"
            f"事故前處理能力 {272} 則／分、進件 {q.served[m0 - 1]:.0f} 則／分；事故期間每分鐘處理 {q.served[m0 + 10]:.0f} 則",
            lag[m0 - 30:m0].max() < 0.1 and lag[m1 - 1] > 5)
    p = _play(r, w, end, "latency", CONSUMER, INC - timedelta(minutes=5))
    if p is None:
        return r
    new, old = p["new"], p["old"]
    top, a = _top(new), _top(new).get("attribution", {})
    mq = a.get("async") or {}
    r.check("第一名是消費者 ship-worker", CONSUMER, _names(new), top.get("entity") == CONSUMER)
    r.check("下鑽歸因指到它自己的 DB", "ship-db（db）", f"{a.get('resource')}（{a.get('kind')}），share={a.get('share')}",
            a.get("resource") == "ship-db" and a.get("kind") == "db" and bool(top.get("dependency")))
    r.check("生產者 ord-api 不在前 2 名（新邏輯不准亂指）", "不在前 2", f"名次 {_rank(new, PRODUCER)}", (_rank(new, PRODUCER) or 99) > 2)
    r.check("attribution.async 標成消費者、判定為積壓", "role=consumer、backlog=True", {k: mq.get(k) for k in ("role", "backlog", "lag_text", "linked_by")},
            mq.get("role") == "consumer" and mq.get("backlog") is True)
    truth = float(np.percentile(lag[w.m(p["start"]):w.m(p["end"]) + 1], 95)) * 60000
    got = (mq.get("lag_ms") or {}).get("during") or 0.0
    r.check("平台算出的延遲 P95 對得上真值", "與真值相差 25% 以內", f"平台 {got / 60000:.2f} 分；真值 {truth / 60000:.2f} 分", truth > 0 and abs(got - truth) / truth <= 0.25)
    r.check("理由寫明回頭看過生產者、它沒有異常", "含「未見異常，不轉記」", top.get("reason", "")[-260:], "未見異常，不轉記" in top.get("reason", ""))
    r.check("Runbook 開資料庫的處方（積壓是結果，該查的是那個 DB）", "RB-02", _runbook(new), _runbook(new) == "RB-02")
    r.check("跟改前比：第一名沒有變", _top(old).get("entity"), top.get("entity"), _top(old).get("entity") == top.get("entity"))
    r.check("沒有無端懷疑訊息內容（消費者沒有錯誤增量）", "attribution.async 不帶 suspect_payload", mq.get("suspect_payload"), not mq.get("suspect_payload"))
    _pub_window_check(r, w, p)
    return r


# ───────────────────────── 12b／12c：訊息夾帶的失敗 ─────────────────────────
def _carried(n: str, title: str, link_mode: str, linked_by: str) -> H.Result:
    r = H.Result(n, title,
                 "12:00–12:20 ord-api 出壞版本：寫 orders-db 20→400 ms，同時發出去的訊息 50% payload 壞掉；佇列固定延遲 20 分鐘（delay queue），"
                 f"所以 ship-worker 要到 12:20 才開始處理失敗，那時 ord-api 已經恢復。link_mode={link_mode}")
    fault = (INC, INC + timedelta(minutes=20))
    end = INC + timedelta(minutes=45)
    w = _world(end, delay_ms=20 * 60000.0, link_mode=link_mode)
    _bad_release(w, *fault)
    p = _play(r, w, end, "errors", CONSUMER, INC + timedelta(minutes=15))
    if p is None:
        return r
    new, old = p["new"], p["old"]
    r.check("改前：第一名是消費者（只看事件時窗，錯全在它身上）", CONSUMER, _names(old), _top(old).get("entity") == CONSUMER)
    r.check("改後：第一名是生產者 ord-api", PRODUCER, _names(new), _top(new).get("entity") == PRODUCER)
    edge = _pub_window_check(r, w, p, fault)
    if edge:
        r.check(f"發布時間的來源是 {linked_by}", linked_by, edge.get("linked_by"), edge.get("linked_by") == linked_by)
        r.check("延遲 P95 約 20 分鐘", "19～21 分鐘", f"{edge['lag_ms']['p95'] / 60000:.2f} 分鐘（{edge['messages']} 則、其中消費者自己失敗 {edge['failed']} 則）",
                19 <= edge["lag_ms"]["p95"] / 60000 <= 21)
    prod, cons = _of(new, PRODUCER), _of(new, CONSUMER)
    pa = (prod.get("attribution") or {}).get("async") or {}
    r.check("消費者的錯誤增量轉記給生產者", "生產者 attribution.async.moved_error > 0，消費者理由寫「改記在 ord-api」",
            f"moved_error={pa.get('moved_error')}；消費者理由含轉記：{'改記在 ' + PRODUCER in cons.get('reason', '')}",
            (pa.get("moved_error") or 0) > 0 and "改記在 " + PRODUCER in cons.get("reason", ""))
    a = prod.get("attribution") or {}
    r.check("生產者的下鑽歸因是它當時在等的 orders-db", "orders-db（db）", f"{a.get('resource')}（{a.get('kind')}），dependency={prod.get('dependency')!r}",
            a.get("resource") == "orders-db" and bool(prod.get("dependency")))
    r.check("Runbook 命中 RB-07（回到發布時段查生產者）", "RB-07", _runbook(new), _runbook(new) == "RB-07")
    r.check("不相干的 cart-api 沒有被列為候選", "不在候選", _names(new), "cart-api" not in _names(new))
    mq = _mq(new)
    r.check("跨時窗的代價有上限", "補撈查詢 <= 2 輪、沒有被截斷", {k: mq.get(k) for k in ("consumers", "linked_candidates", "linked_traces", "extra_spans", "queries", "truncated", "pub_fetch")},
            mq.get("queries", 99) <= 2 and not mq.get("truncated"))
    first = ((new.get("runbook") or {}).get("actions") or [""])[0]
    r.check("處方第一條是回發布時段查生產者（不是先看積壓深度：這條佇列是固定延遲，沒有積壓）", "以「回到訊息發布時段」開頭", first[:60],
            first.startswith("回到訊息發布時段"))
    briefing = new["narrative"].get("briefing") or {}
    r.check("三行摘要沒有寫「排除它本身的程式」（壞訊息正是它的程式發的）", "驗證行不含「排除它本身的程式」、要求抽查失敗訊息",
            briefing.get("verification_line"), "排除它本身的程式" not in str(briefing) and "抽查失敗訊息" in str(briefing.get("verification_line")))
    r.notes.append(f"Runbook 處方：{(new.get('runbook') or {}).get('actions')}")
    r.notes.append(f"三行摘要（{new['narrative'].get('briefing_generated_by')}）：{briefing}")
    return r


def scenario_12b() -> H.Result:
    return _carried("12b", "訊息夾帶的失敗：生產者 12:00 出事、消費者 12:20 才失敗（parent 傳遞）", "parent", "parent")


def scenario_12c() -> H.Result:
    return _carried("12c", "同 12b，但消費者是新的 trace、只用 span.links 指回生產者", "links", "link")


# ───────────────────────── 12d：舊版 agent，沒有 link 也沒有 age ─────────────────────────
def scenario_12d() -> H.Result:
    r = H.Result("12d", "舊版 agent：消費者沒有 parent、沒有 span.links、沒有 message.age（跨不了時窗）",
                 "同 12b 的注入，但 link_mode=none、emit_age=False：trace 裡沒有任何東西能把消費者的訊息對回發布時間")
    fault = (INC, INC + timedelta(minutes=20))
    end = INC + timedelta(minutes=45)
    w = _world(end, delay_ms=20 * 60000.0, link_mode="none", emit_age=False)
    _bad_release(w, *fault)
    p = _play(r, w, end, "errors", CONSUMER, INC + timedelta(minutes=15))
    if p is None:
        return r
    new, old = p["new"], p["old"]
    r.check("沒有硬湊出非同步邊", "0 條", _edges(new), not _edges(new))
    r.check("報告寫明為什麼跨不了時窗", "含「無法跨時窗」", _note(new), "無法跨時窗" in _note(new))
    r.check("這段說明真的出現在報告段落裡（不是只在 API 欄位）", "根因或 Trace 佐證段落含「無法跨時窗」",
            [k for k, v in new["_sections"].items() if "無法跨時窗" in v], any("無法跨時窗" in v for v in new["_sections"].values()))
    r.check("沒有亂指生產者", "ord-api 不在候選", _names(new), PRODUCER not in _names(new))
    same = [(c["entity"], c["score"]) for c in _cand(new)] == [(c["entity"], c["score"]) for c in _cand(old)]
    r.check("排名與分數跟改前一模一樣", [(c["entity"], c["score"]) for c in _cand(old)], [(c["entity"], c["score"]) for c in _cand(new)], same)
    r.check("Runbook 跟改前一樣", _runbook(old), _runbook(new), _runbook(old) == _runbook(new))
    mq = _mq(new)
    r.check("沒有多撈任何 span", "extra_spans = 0", {k: mq.get(k) for k in ("consumers", "extra_spans", "queries", "edges", "unresolved")}, mq.get("extra_spans", 0) == 0)
    return r


# ───────────────────────── 12e：生產者毫無症狀的毒訊息（已知死區）─────────────────────────
def scenario_12e() -> H.Result:
    r = H.Result("12e", "生產者毫無症狀的毒訊息（已知死區：trace 只看得到耗時與錯誤，看不到訊息內容）",
                 "12:00–12:20 ord-api 發出去的訊息 50% payload 壞掉，但它自己的耗時、錯誤率、依賴全部正常；固定延遲 20 分鐘，ship-worker 12:20 起處理失敗")
    fault = (INC, INC + timedelta(minutes=20))
    end = INC + timedelta(minutes=45)
    w = _world(end, delay_ms=20 * 60000.0)
    _bad_release(w, *fault, db_ms=None)
    p = _play(r, w, end, "errors", CONSUMER, INC + timedelta(minutes=15))
    if p is None:
        return r
    new = p["new"]
    r.check("第一名是真正的根因 ord-api（它的程式變更）", PRODUCER, _names(new), _top(new).get("entity") == PRODUCER, dead=True)
    _pub_window_check(r, w, p, fault)
    cons = _of(new, CONSUMER)
    r.check("平台確實回頭看過生產者的發布時窗、照實說它沒有異常", "消費者理由含「未見異常，不轉記」", cons.get("reason", "")[-330:], "未見異常，不轉記" in cons.get("reason", ""))
    r.check("理由要求人工抽查失敗訊息與 dead-letter queue（不能讓人以為生產者已經排除）", "含「dead-letter」", "dead-letter" in cons.get("reason", ""), "dead-letter" in cons.get("reason", ""))
    r.check("沒有為了有答案硬把分數轉給生產者", "生產者不在候選", _names(new), PRODUCER not in _names(new))
    ca = (cons.get("attribution") or {}).get("async") or {}
    r.check("attribution.async 標記 suspect_payload", "True", ca.get("suspect_payload"), ca.get("suspect_payload") is True)
    actions = new["_sections"].get("suggested_actions", "")
    hit = next((line for line in actions.splitlines() if "dead-letter" in line and PRODUCER in line), "")
    r.check("建議處置有一條叫人抽查失敗訊息並核對生產者在發布時段的變更", "含 dead-letter 與 ord-api 的一行", hit or actions[:200], bool(hit))
    r.notes.append(f"Runbook：{_runbook(new)}（主處方仍是消費者自己的程式：trace 分不出是它的 bug 還是訊息內容，兩邊都要查）；"
                   f"處方 {(new.get('runbook') or {}).get('actions')}")
    r.notes.append(f"建議處置全文：{actions}")
    return r


# ───────────────────────── 12f：延遲超過 max_lag_minutes ─────────────────────────
def scenario_12f() -> H.Result:
    r = H.Result("12f", "訊息延遲 3 小時，超過 max_lag_minutes（120）：不往回撈",
                 "同 12b 的注入，但佇列固定延遲 180 分鐘：ship-worker 15:00 才開始失敗")
    fault = (INC, INC + timedelta(minutes=20))
    end = INC + timedelta(minutes=205)
    w = _world(end, delay_ms=180 * 60000.0)
    _bad_release(w, *fault)
    p = _play(r, w, end, "errors", CONSUMER, INC + timedelta(minutes=175), trace_lo=INC - timedelta(minutes=80))
    if p is None:
        return r
    new, old = p["new"], p["old"]
    mq = _mq(new)
    r.check("報告寫明延遲超過上限", "含「延遲超過上限」", _note(new), "延遲超過上限" in _note(new))
    r.check("真的沒有往回撈", "skipped=over_max_lag、queries=0、extra_spans=0", mq,
            mq.get("skipped") == "over_max_lag" and mq.get("queries") == 0 and mq.get("extra_spans") == 0)
    r.check("訊息年齡估得對", "約 180 分鐘（±5）", mq.get("age_p95_minutes"), abs((mq.get("age_p95_minutes") or 0) - 180) <= 5)
    r.check("沒有亂指生產者", "ord-api 不在候選", _names(new), PRODUCER not in _names(new))
    same = [(c["entity"], c["score"]) for c in _cand(new)] == [(c["entity"], c["score"]) for c in _cand(old)]
    r.check("排名與分數跟改前一模一樣", [(c["entity"], c["score"]) for c in _cand(old)], [(c["entity"], c["score"]) for c in _cand(new)], same)
    return r


# ───────────────────────── 12g：偵測端同輪合併（alert.messaging_link）─────────────────────────
def scenario_12g() -> H.Result:
    r = H.Result("12g", "偵測端：生產者與消費者同一輪都異常時併成一張事件卡（alert.messaging_link，要先沉澱過非同步邊）",
                 "同 12b 的注入，但佇列延遲只有 3 分鐘：兩邊的異常落在同一輪偵測裡。跑兩次：沒有沉澱拓撲 vs 先跑 admin.topology_refresh")
    fault = (INC, INC + timedelta(minutes=20))
    end = INC + timedelta(minutes=30)

    def world() -> World:
        w = _world(end, delay_ms=3 * 60000.0)
        _bad_release(w, *fault)
        return w

    cold = H.Result("12g-cold", "", "")
    p0 = _play(cold, world(), end, "errors", CONSUMER, INC - timedelta(minutes=5))
    r.notes += [f"（未沉澱）{n}" for n in cold.notes[:1]]
    if p0 is None:
        r.check("未沉澱拓撲時偵測到 ship-worker 事件群", "有", False, False)
        return r
    apart = [i for i in p0["incs"] if {PRODUCER, CONSUMER} & set(i["incident"]["entities"])]
    r.check("沒有非同步邊資料時：兩個服務各自一張卡（沒有時間窗硬併）", "2 張、各含一個服務",
            [(i["id"], i["incident"]["entities"]) for i in apart],
            len(apart) == 2 and all(len(set(i["incident"]["entities"]) & {PRODUCER, CONSUMER}) == 1 for i in apart))
    p = _play(r, world(), end, "errors", CONSUMER, INC - timedelta(minutes=5), refresh=(INC - timedelta(minutes=60), INC - timedelta(minutes=5)))
    if p is None:
        return r
    topo = p["topo"] or {}
    stored = [e for e in topo.get("async_edges", []) if e.get("producer") == PRODUCER and e.get("consumer") == CONSUMER]
    lag_min = (stored[0].get("lag_ms_p95", 0) / 60000) if stored else None
    r.check("admin.topology_refresh 沉澱出 ord-api → ship-worker 與它的平常延遲", "有這條邊、延遲約 3 分鐘（±0.5）",
            f"{topo.get('async_edges')}", bool(stored) and abs(lag_min - 3) <= 0.5)
    together = [i for i in p["incs"] if {PRODUCER, CONSUMER} <= set(i["incident"]["entities"])]
    either = [i for i in p["incs"] if {PRODUCER, CONSUMER} & set(i["incident"]["entities"])]
    r.check("有非同步邊資料時：併成一張卡", "1 張、同時含 ord-api 與 ship-worker",
            [(i["id"], i["incident"]["entities"], i["incident"].get("grouped_by")) for i in either], len(together) == 1 and len(either) == 1)
    new = p["new"]
    r.check("併卡後 RCA 第一名是生產者 ord-api", PRODUCER, _names(new), _top(new).get("entity") == PRODUCER)
    r.check("Runbook 命中 RB-07", "RB-07", _runbook(new), _runbook(new) == "RB-07")
    return r


def scenario_12() -> list[H.Result]:
    out = []
    for fn in (scenario_12a, scenario_12b, scenario_12c, scenario_12d, scenario_12e, scenario_12f, scenario_12g):
        try:
            out.append(fn())
        except Exception as e:  # noqa: BLE001  子場景崩潰也是結果：回報，不吞，也不拖累其他子場景
            import traceback
            res = H.Result(fn.__name__.split("_")[-1], fn.__doc__ or fn.__name__, "")
            res.crash = f"{type(e).__name__}: {e}\n{traceback.format_exc()[-1500:]}"
            out.append(res)
    return out
