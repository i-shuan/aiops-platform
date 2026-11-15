"""場景 13：流量放大者——下游被灌爆，但灌它的不是外面的流量，是上游服務自己把呼叫次數放大了（重試風暴、迴圈呼叫）。

世界骨架：ingress-gw（入口）→ order-svc → payment-svc，外加不相干的 cart-api 當對照。外部請求率全程不變。
  13a 放大者在事件裡：order-svc 每個請求打 payment-svc 的次數 1 → 25（依序呼叫），自己的程式耗時沒變、總耗時被拉長而告警。
  13b 放大者不在事件裡：同樣的放大，但 order-svc 沒有被監控（不是偵測 target），事件群裡沒有它——平台要自己把它補進候選。
  13c 對照組：外部流量真的漲了 6 倍（每一跳扇出都沒變）——不准指認內部放大者，要開容量的處方（RB-04）。
斷言照設計稿（docs/MQ_QPS_DESIGN.md P1）寫，不照平台現狀遷就。

模擬器的限制（照實寫，不粉飾）：
  * 世界沒有容量模型：payment-svc「被灌爆」是場景直接把它的自身耗時與自身失敗率調高，不是由流量算出來的。
  * 沒有 fire-and-forget：order-svc 一定會等 payment-svc 回來，所以它的總耗時一定變長；13b 的「不在事件裡」是靠不把它列為
    偵測 target 做到的（等同這個服務沒有接告警），不是它真的毫無症狀。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from scripts.chaos import harness as H
from scripts.chaos.world import Call, Svc, World

UTC = timezone.utc
T0 = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
INC = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
GW, AMP, VICTIM, CONTROL = "ingress-gw", "order-svc", "payment-svc", "cart-api"
FANOUT = 25.0


def _mins(lo: datetime, hi: datetime) -> int:
    return int((hi - lo).total_seconds() // 60)


def _world(end: datetime, rate: float = 8.0, parallel: bool = False) -> World:
    return World(T0, _mins(T0, end + timedelta(minutes=20)), [
        Svc(GW, "POST /checkout", 5.0, [Call(AMP, 1.0, ms=1.0)]),
        # 呼叫 payment-svc 的網路來回只給 0.2 ms：放大 25 次也只多 5 ms，order-svc 自己的自身耗時增量過不了顯著門檻，
        # 它上榜只能靠「把下游流量放大」這條證據（比較難的那種）；swallow：payment 失敗它接住，自己照樣回 200
        Svc(AMP, "POST /orders", 30.0, [Call(VICTIM, 1.0, ms=0.2, swallow=True)], parallel=parallel),
        Svc(VICTIM, "POST /payments", 20.0, error_type="PaymentOverloaded", error_message="payment worker pool exhausted"),
        Svc(CONTROL, "GET /cart", 30.0, [Call("redis", 1.0, "db", "redis", 2.0, name="HGETALL cart")]),
    ], {GW: rate, CONTROL: 25.0})


def _overload(w: World, lo: datetime, hi: datetime, self_ms: float = 120.0, fail_p: float = 0.3) -> None:
    """payment-svc 被灌爆的樣子：自身耗時 20 → 120 ms，三成請求回 500（worker pool 用光）。"""
    w.set(w.svcs[VICTIM].self_ms, lo, hi, self_ms)
    w.set(w.svcs[VICTIM].own_fail_p, lo, hi, fail_p)


def _cand(out: dict) -> list[dict]:
    return out.get("root_cause", {}).get("candidates", [])


def _names(out: dict) -> list[str]:
    return [c["entity"] for c in _cand(out)]


def _top(out: dict) -> dict:
    c = _cand(out)
    return c[0] if c else {}


def _runbook(out: dict) -> str:
    return (out.get("runbook") or {}).get("id", "(無)")


def _hits(out: dict) -> list[str]:
    rb = out.get("runbook") or {}
    return [rb.get("id", "(無)"), *rb.get("also", [])]


def _play(r: H.Result, w: World, end: datetime, names: list[str], victim: str = VICTIM) -> dict | None:
    """建世界 → 偵測回放（names 是有接告警的服務）→ 對 victim 的事件群跑 rca_report（管線現行設定，不改任何參數）。"""
    w.build()
    H.fresh_db()
    with H.es_for(w, INC - timedelta(minutes=80), end + timedelta(minutes=5)):
        # 服務拓撲跟生產一樣由 admin.topology_refresh 從事故前的 trace 沉澱（收斂才知道誰呼叫誰），不手動灌邊
        from scripts.chaos.scen_detect_rca import topology_refresh
        topology_refresh(INC - timedelta(minutes=60), INC - timedelta(minutes=5))
        rp = H.replay(H.detection_workflow("sample_it_otel", H.targets(names)), INC - timedelta(minutes=30), end)
        incs = H.incidents()
        r.notes.append(f"偵測：{rp.rounds} 輪、異常 {rp.anomalies} 筆、errors {rp.errors[:2]}、"
                       f"事件群 {[(i['id'], i['incident']['entities'], i['incident']['start'][11:16]) for i in incs]}")
        early = H.before(incs, victim, INC - timedelta(minutes=5))
        r.check(f"注入之前 {victim} 沒有誤報事件群", "0 個", f"{len(early)} 個（id, 開始時間）={early}", not early)
        target = H.pick(incs, victim, INC - timedelta(minutes=5))
        if not r.check(f"偵測到 {victim} 事件群", "有", bool(target), bool(target)):
            return None
        out = H.rca(target["id"])
    tf = out["_ctx"].result.get("trace_fetch", {})
    scores = "、".join(f"{c['entity']} {c['score']:.2f}" for c in _cand(out)) or f"（無候選：{out['root_cause'].get('note', '')[:120]}）"
    r.notes.append(f"RCA：候選 [{scores}]；Runbook {_hits(out)}；span {tf.get('spans')} 筆；errors {out['_errors']}")
    r.notes.append(f"第一名理由：{_top(out).get('reason', '')[:700]}")
    r.notes.append(f"第一名 attribution：{ {k: v for k, v in (_top(out).get('attribution') or {}).items() if k in ('amplifier', 'ingress_surge', 'resource', 'kind', 'share')} }")
    b = (out.get("narrative") or {}).get("briefing") or {}
    r.notes.append(f"三行摘要（{(out.get('narrative') or {}).get('briefing_generated_by')}）：{b.get('summary_line', '')}｜{b.get('action_line', '')}｜{b.get('verification_line', '')}")
    return {"out": out, "target": target, "incs": incs}


def _amplifier_checks(r: H.Result, out: dict, in_incident: bool) -> None:
    top = _top(out)
    amp = (top.get("attribution") or {}).get("amplifier") or {}
    r.check("第一名是放大者 order-svc（不是被灌爆的 payment-svc）", AMP, _names(out), top.get("entity") == AMP)
    r.check("attribution.amplifier 標成 traffic_amplifier、受害者是 payment-svc", "type=traffic_amplifier、victim=payment-svc",
            {k: amp.get(k) for k in ("type", "victim", "via", "in_incident")},
            amp.get("type") == "traffic_amplifier" and amp.get("victim") == VICTIM and amp.get("in_incident") is in_incident)
    fan = amp.get("fanout") or {}
    r.check("扇出數字對得上真值（每個請求 1 次 → 25 次）", "事件前 1.0（±0.1）、事件期間 25（±2.5）、倍數 ≥ 20",
            f"事件前 {fan.get('before')}、事件期間 {fan.get('during')}、扇出倍數 {amp.get('fanout_ratio')}、payment-svc 流入 {amp.get('load_ratio')} 倍",
            abs((fan.get("before") or 0) - 1.0) <= 0.1 and abs((fan.get("during") or 0) - FANOUT) <= 2.5 and (amp.get("fanout_ratio") or 0) >= 20)
    r.check("payment-svc 仍在候選裡、排在 order-svc 後面（沒有被藏起來）", "名次 ≥ 2", _names(out),
            VICTIM in _names(out) and _names(out).index(VICTIM) >= 1)
    r.check("對照組 cart-api 與入口 ingress-gw 不是第一名", "不是第一名", _names(out), top.get("entity") not in (CONTROL, GW))
    r.check("Runbook 主處方是 RB-08（查重試與迴圈，不是擴容）", "RB-08", _hits(out), _runbook(out) == "RB-08")
    r.check("沒有開 RB-04 擴容處方（流量不是外面來的）", "不含 RB-04", _hits(out), "RB-04" not in _hits(out))
    reason = top.get("reason", "")
    r.check("理由寫明它是流量放大者與扇出次數", "含「流量放大者」", reason[-320:], "流量放大者" in reason)
    b = (out.get("narrative") or {}).get("briefing") or {}
    r.check("三行摘要點名放大者與受害者", "summary_line 同時有 order-svc 與 payment-svc、action_line 引用 RB-08",
            f"{b.get('summary_line')}｜{b.get('action_line')}",
            AMP in (b.get("summary_line") or "") and VICTIM in (b.get("summary_line") or "") and "RB-08" in (b.get("action_line") or ""))


# ───────────────────────── 13a：放大者在事件裡 ─────────────────────────
def scenario_13a() -> H.Result:
    r = H.Result("13a", "重試風暴：order-svc 把對 payment-svc 的呼叫放大 25 倍（放大者自己也告警）",
                 "12:00–12:25 order-svc 每個請求打 payment-svc 1 → 25 次（依序），外部請求率不變；payment-svc 被灌爆：自身耗時 20→120 ms、"
                 "三成回 500。order-svc 自己的程式耗時沒變，但總耗時被 25 次呼叫拉長。四個服務都有接延遲告警")
    lo, end = INC, INC + timedelta(minutes=25)
    w = _world(end)
    w.set(w.call(AMP, VICTIM).times, lo, end, FANOUT)
    _overload(w, lo, end)
    w.build()
    m0 = w.m(INC)
    r.check("世界資料：外部請求率不變、payment-svc 流量約 25 倍", "ingress-gw 事故前後請求率相差 < 20%；payment-svc 20～30 倍",
            f"ingress-gw {w.rate[GW][m0 - 10:m0].mean():.1f} → {w.rate[GW][m0:m0 + 10].mean():.1f} 筆／分；"
            f"payment-svc {w.rate[VICTIM][m0 - 10:m0].mean():.1f} → {w.rate[VICTIM][m0:m0 + 10].mean():.1f} 筆／分",
            abs(w.rate[GW][m0:m0 + 10].mean() / w.rate[GW][m0 - 10:m0].mean() - 1) < 0.2
            and 20 <= w.rate[VICTIM][m0:m0 + 10].mean() / w.rate[VICTIM][m0 - 10:m0].mean() <= 30)
    p = _play(r, w, end, [GW, AMP, VICTIM, CONTROL])
    if p is None:
        return r
    entities = p["target"]["incident"]["entities"]
    r.check("前提：order-svc 在事件群裡", "在", entities, AMP in entities)
    _amplifier_checks(r, p["out"], in_incident=True)
    return r


# ───────────────────────── 13b：放大者不在事件裡 ─────────────────────────
def scenario_13b() -> H.Result:
    r = H.Result("13b", "放大者完全沒有告警：order-svc 不在事件裡，平台要自己把它補進候選",
                 "同 13a 的放大（1 → 25 次，這次是並行呼叫），但 order-svc 沒有接告警（不是偵測 target）。事件群裡只有被灌爆的 payment-svc"
                 "（與等它的入口）；trace 仍然看得到 order-svc 的 span")
    lo, end = INC, INC + timedelta(minutes=25)
    w = _world(end, parallel=True)
    w.set(w.call(AMP, VICTIM).times, lo, end, FANOUT)
    _overload(w, lo, end)
    p = _play(r, w, end, [GW, VICTIM, CONTROL])
    if p is None:
        return r
    entities = p["target"]["incident"]["entities"]
    r.check("前提：order-svc 不在事件群裡", "不在", entities, AMP not in entities)
    _amplifier_checks(r, p["out"], in_incident=False)
    r.check("理由寫明它不在事件裡、是補進來的", "含「它不在事件裡」", _top(p["out"]).get("reason", "")[-200:],
            "它不在事件裡" in _top(p["out"]).get("reason", ""))
    return r


# ───────────────────────── 13c：外部流量真的漲了（對照組）─────────────────────────
def scenario_13c() -> H.Result:
    r = H.Result("13c", "外部流量突波（對照組）：沒有內部放大者，不准亂指；要開容量處方",
                 "12:00–12:25 外部打進 ingress-gw 的請求 ×6，每一跳的扇出都沒變（1 次）；payment-svc 被灌爆：自身耗時 20→120 ms、三成回 500。"
                 "四個服務都有接延遲告警")
    lo, end = INC, INC + timedelta(minutes=25)
    w = _world(end)
    w._entries[GW] = w._a(w._entries[GW])  # 只漲 ingress-gw 這個入口（rate_mult 會連對照組 cart-api 一起漲）
    w.scale(w._entries[GW], lo, end, 6.0)
    _overload(w, lo, end)
    p = _play(r, w, end, [GW, AMP, VICTIM, CONTROL])
    if p is None:
        return r
    out = p["out"]
    top = _top(out)
    a = top.get("attribution") or {}
    surge = a.get("ingress_surge") or {}
    r.check("第一名是 payment-svc（它自己真的變慢、出錯）", VICTIM, _names(out), top.get("entity") == VICTIM)
    r.check("沒有任何候選被標成流量放大者", "沒有 attribution.amplifier",
            [(c["entity"], bool((c.get("attribution") or {}).get("amplifier"))) for c in _cand(out)],
            not any((c.get("attribution") or {}).get("amplifier") for c in _cand(out)))
    r.check("attribution.ingress_surge 指到外部入口 ingress-gw", "entry=ingress-gw、流入 ≥ 4 倍", surge,
            surge.get("entry") == GW and (surge.get("load_ratio") or 0) >= 4)
    r.check("Runbook 命中 RB-04（範例 trace 有失敗也照開：被灌爆本來就會拋錯）", "含 RB-04、不含 RB-08", _hits(out),
            "RB-04" in _hits(out) and "RB-08" not in _hits(out))
    r.check("理由寫明流量是外部入口打進來的", "含「外部入口」", top.get("reason", "")[-260:], "外部入口" in top.get("reason", ""))
    return r


def scenario_13() -> list[H.Result]:
    out = []
    for fn in (scenario_13a, scenario_13b, scenario_13c):
        try:
            out.append(fn())
        except Exception as e:  # noqa: BLE001  子場景崩潰也是結果：回報，不吞，也不拖累其他子場景
            import traceback
            res = H.Result(fn.__name__.split("_")[-1], fn.__doc__ or fn.__name__, "")
            res.crash = f"{type(e).__name__}: {e}\n{traceback.format_exc()[-1500:]}"
            out.append(res)
    return out
