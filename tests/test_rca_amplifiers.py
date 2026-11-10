"""流量放大者與 QPS 歸因（docs/MQ_QPS_DESIGN.md P1）。

涵蓋：rca.root_cause.evidence 的扇出判準（attribution["amplifier"]／["ingress_surge"]、不在事件裡的放大者補候選列、
min_fanout_ratio）、Runbook RB-08 與放寬後的 RB-04、三行摘要模板與封閉詞表。端到端在 scripts/chaos/run.py 13。

已知死區：
  xfail strict：透過訊息佇列放大（生產者把訊息量放大）——非同步邊不算流量倍數，指不到生產者。
  照現況寫死的行為（不是 xfail，因為是刻意不判）：事件前這條呼叫邊不到 min_requests 筆（新上線的呼叫路徑）算不出扇出倍數，
  不標 amplifier；RB-04 靠入口突波命中時，若同時有 errors 訊號＋失敗範例，主處方仍是 RB-03（80 > 70），RB-04 列在 also。
"""
from datetime import datetime, timedelta, timezone

import pytest

from aiops.engine import mq
from aiops.engine.context import PipelineContext, RootCandidate, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.enrichment import narrative
from aiops.stages.rca.sections.actions import RUNBOOK_RULES, RunbookFacts, evaluate

T = datetime(2026, 11, 3, 14, 0, tzinfo=timezone.utc)
GW, AMP, VICTIM = "ingress-gw", "order-svc", "payment-svc"
ALL = (GW, AMP, VICTIM)


def chain(*, fanout=1, base_fanout=1, victim_extra=0.0, victim_fail=False, amp_extra=0.0, surge=1, per_minute=4,
          minutes=range(-15, 30)):
    """ingress-gw -> order-svc -> payment-svc。事件期間（t >= 0）：
    fanout       order-svc 每個請求呼叫 payment-svc 幾次（事件前是 base_fanout；0 = 事件前完全不呼叫）
    victim_extra payment-svc 每個 span 自身多花幾 ms；victim_fail = 它的 span 全部失敗（order-svc 接住，自己照樣成功）
    amp_extra    order-svc 自身多花幾 ms
    surge        外部打進 ingress-gw 的請求數變幾倍"""
    spans, n = [], 0
    for m in minutes:
        during = m >= 0
        for k in range(per_minute * (surge if during else 1)):
            n += 1
            tid, ts = f"t{n}", T + timedelta(minutes=m, seconds=(k * 7) % 60)
            pay = [20.0 + (victim_extra if during else 0.0)] * (fanout if during else base_fanout)
            order_ms = 30.0 + (amp_extra if during else 0.0) + sum(pay)
            spans.append(SpanRecord(tid, f"{tid}g", None, GW, ts, 5.0 + order_ms))
            spans.append(SpanRecord(tid, f"{tid}o", f"{tid}g", AMP, ts, order_ms))
            # 一次接一次呼叫（重試是循序的）：子 span 不重疊，order-svc 的自身耗時才不會因為放大而被算高
            spans += [SpanRecord(tid, f"{tid}p{i}", f"{tid}o", VICTIM, ts + timedelta(milliseconds=i * d), d,
                                 error=during and victim_fail) for i, d in enumerate(pay)]
    return spans


def rank(spans, entities=ALL, **params):
    incident = Incident([AnomalyEvent(e, "latency", T + timedelta(minutes=m), 5.0) for e in entities for m in range(30)])
    ctx = PipelineContext(pipeline="rca", now=T + timedelta(minutes=30))
    ctx.incidents, ctx.incidents_built = [incident], True
    ctx.rca_spans = spans
    return OperatorRegistry.create("rca.root_cause.evidence", params).rank(incident, ctx), ctx


def scores(ranked):
    return [(c.entity, round(c.score, 2)) for c in ranked]


def by_entity(ranked):
    return {c.entity: c for c in ranked}


# ── 扇出判準：誰是放大者 ──


def test_amplifier_with_no_symptom_of_its_own_is_named_by_fan_out_alone():
    """13a 的單元版：order-svc 自身耗時完全沒變，只是每個請求打 payment-svc 5 次；payment-svc 被灌到自身多 60 ms。"""
    ranked, ctx = rank(chain(fanout=5, victim_extra=60.0))
    assert ranked[0].entity == AMP and ctx.rca_root_note == ""
    amp = ranked[0].attribution["amplifier"]
    assert amp == {"type": "traffic_amplifier", "victim": VICTIM, "via": VICTIM, "load_ratio": 5.0,
                   "fanout": {"before": 1.0, "during": 5.0}, "fanout_ratio": 5.0,
                   "moved_self_ms": 48.0, "moved_error": 0.0, "victims": [VICTIM], "in_incident": True}
    # 轉記 (1 − 1/5) × 60 ms = 48 ms；payment-svc 只留 12 ms
    victim = by_entity(ranked)[VICTIM]
    assert "amplifier" not in victim.attribution and f"流量被 {AMP} 放大" in victim.reason
    assert "扇出 5.00 倍" in victim.reason and "流量放大者" in ranked[0].reason
    assert "不在事件裡" not in ranked[0].reason


def test_silent_amplifier_outside_the_incident_gets_a_candidate_row():
    """13b 的單元版：order-svc 沒有告警、不在事件裡。扇出判準成立才補一列，並標 in_incident = False。"""
    ranked, _ = rank(chain(fanout=5, victim_extra=60.0), entities=(VICTIM,))
    assert [c.entity for c in ranked] == [AMP, VICTIM]
    amp = ranked[0].attribution["amplifier"]
    assert (amp["in_incident"], amp["victim"], amp["fanout_ratio"], amp["moved_self_ms"]) == (False, VICTIM, 5.0, 48.0)
    assert "它不在事件裡（自己沒有告警）" in ranked[0].reason


def test_errors_caused_by_the_flood_move_to_the_amplifier_too():
    """被灌爆的下游拋錯（自身耗時沒變）：錯誤率增量照同一個比例轉記。"""
    ranked, _ = rank(chain(fanout=4, victim_fail=True), entities=(VICTIM,))
    assert ranked[0].entity == AMP
    amp = ranked[0].attribution["amplifier"]
    assert (amp["moved_error"], amp["moved_self_ms"]) == (0.75, 0.0)  # (1 − 1/4) × 100%
    assert "自身錯誤率增量中 75% 也改記在這裡" in ranked[0].reason


def test_fan_out_below_the_threshold_keeps_the_old_transfer_but_names_no_amplifier():
    """扇出 1.6 倍 < min_fanout_ratio 2.0：舊的流量轉記照做（流量倍數 1.6 >= min_load_ratio 1.5），但不標 amplifier、RB-08 不命中。"""
    ranked, _ = rank(chain(fanout=8, base_fanout=5, victim_extra=60.0))
    assert all("amplifier" not in c.attribution for c in ranked)
    assert f"流量被 {AMP} 放大" in by_entity(ranked)[VICTIM].reason  # 舊行為沒有被拿掉


def test_silent_caller_below_the_fan_out_threshold_is_not_added():
    """不在事件裡的呼叫者、扇出沒過門檻：跟以前一樣只寫在受害者的理由裡，不補列、不轉分數。"""
    ranked, _ = rank(chain(fanout=8, base_fanout=5, victim_extra=60.0), entities=(VICTIM,))
    assert scores(ranked) == [(VICTIM, 1.0)]
    assert AMP in ranked[0].reason


def test_min_fanout_ratio_is_a_yaml_param():
    spans = chain(fanout=8, base_fanout=5, victim_extra=60.0)  # 扇出 1.6 倍
    ranked, _ = rank(spans, min_fanout_ratio=1.5)
    amp = by_entity(ranked)[AMP].attribution["amplifier"]
    assert (amp["fanout_ratio"], amp["moved_self_ms"]) == (1.6, 22.5)  # 只轉 (1 − 1/1.6) × 60 ms
    # 放大倍數小，轉走的不到一半：payment-svc 留 37.5 ms 仍是第一名，order-svc 排第二（標了 amplifier，RB-08 看第一名所以不命中）
    assert [c.entity for c in ranked[:2]] == [VICTIM, AMP]
    ranked, _ = rank(chain(fanout=5, victim_extra=60.0), min_fanout_ratio=6.0)  # 門檻調高：5 倍也不算
    assert all("amplifier" not in c.attribution for c in ranked)


@pytest.mark.parametrize("bad", [1.0, 0.5, 0])
def test_min_fanout_ratio_must_be_above_one(bad):
    with pytest.raises(ValueError, match="min_fanout_ratio"):
        OperatorRegistry.create("rca.root_cause.evidence", {"min_fanout_ratio": bad})


def test_victim_without_a_significant_change_names_no_amplifier():
    """扇出暴增但下游扛得住（自身耗時、錯誤率都沒變）：沒有受害者就沒有放大者，照實說證據不足。"""
    ranked, ctx = rank(chain(fanout=5))
    assert ranked == [] and ctx.rca_root_note.startswith("證據不足")


def test_flooded_amplifier_only_takes_the_share_its_fan_out_caused():
    """外部流量 2 倍、order-svc 又把每個請求放大 3 倍：payment-svc 流入 6 倍，但只轉扇出那一份 (1 − 1/3)，其餘是外部流量的份。"""
    ranked, _ = rank(chain(fanout=3, surge=2, victim_extra=60.0))
    amp = by_entity(ranked)[AMP].attribution["amplifier"]
    assert (amp["load_ratio"], amp["fanout_ratio"], amp["moved_self_ms"]) == (6.0, 3.0, 40.0)
    assert ranked[0].entity == AMP


# ── 入口突波：流量是外面打進來的 ──


def test_ingress_surge_blames_no_internal_service_and_marks_the_flooded_one():
    """13c 的單元版：外部請求 4 倍，扇出沒變。沒有內部放大者，被灌的服務標 ingress_surge，分數不轉給任何人。"""
    ranked, _ = rank(chain(surge=4, victim_extra=60.0))
    assert ranked[0].entity == VICTIM
    assert all("amplifier" not in c.attribution for c in ranked)
    assert ranked[0].attribution["ingress_surge"] == {"entry": GW, "load_ratio": 4.0, "entry_load_ratio": 4.0}
    assert f"往上游追到 {GW} 的外部請求" in ranked[0].reason and "不轉記" in ranked[0].reason


# ── 死區 ──


def test_dead_zone_new_call_path_without_a_baseline_has_no_fan_out_ratio():
    """事件前 order-svc 完全不呼叫 payment-svc（新上線的呼叫路徑）：扇出倍數的分母是 0，算不出來，不標 amplifier。
    刻意不判：事件前不到 min_requests 筆的邊，倍數沒有統計意義。payment-svc 事件前沒有流量，本身也不成候選。"""
    ranked, _ = rank(chain(fanout=5, base_fanout=0, victim_extra=60.0))
    assert all("amplifier" not in c.attribution for c in ranked)


def _mq_world(messages_during: int):
    """ord-api 每個請求發 1 則訊息到 kafka，事件期間變成 messages_during 則；ship-worker 幾秒後消費，被灌到每則多花 300 ms。"""
    spans, i = [], 0
    for m in range(-60, 20):
        during = m >= 0
        for k in range(4):
            pub, tid = T + timedelta(minutes=m, seconds=k * 15), f"p{m}-{k}"
            spans.append(SpanRecord(tid, f"{tid}-e", None, "ord-api", pub, 32.0, name="POST /orders"))
            for j in range(messages_during if during else 1):
                i += 1
                send = f"{tid}-s{j}"
                spans.append(SpanRecord(tid, send, f"{tid}-e", "ord-api", pub, 2.0,
                                        kind="messaging", subtype="kafka", resource="kafka/order-events"))
                work = 320.0 if during else 20.0
                spans.append(SpanRecord(tid, f"c{i}-e", send, "ship-worker", pub + timedelta(seconds=1 + j), work,
                                        name="Kafka RECEIVE from order-events", consumer=True, queue="order-events"))
    return spans


def _mq_rank(spans):
    incident = Incident([AnomalyEvent("ship-worker", "latency", T + timedelta(minutes=m), 5.0) for m in range(20)])
    ctx = PipelineContext(pipeline="rca", now=T + timedelta(minutes=30))
    ctx.incidents, ctx.incidents_built = [incident], True
    ctx.rca_spans, ctx.rca_main_start = spans, T - timedelta(minutes=15)
    ctx.rca_mq = mq.build_edges(spans, ["ship-worker"], T, T + timedelta(minutes=20), T - timedelta(minutes=15),
                                timedelta(minutes=2))[0]
    OperatorRegistry.create("rca.root_cause.evidence", {}).execute(ctx)
    return ctx.rca_root_candidates


@pytest.mark.xfail(strict=True, reason="已知死區：透過訊息佇列放大。非同步邊的呼叫者記成 mq:<生產者>，不算流量倍數與扇出，"
                                       "生產者把訊息量放大 5 倍也指不到它（docs/MQ_QPS_DESIGN.md 缺口 5，本次不做）")
def test_dead_zone_amplification_through_a_message_queue_is_not_attributed_to_the_producer():
    ranked = _mq_rank(_mq_world(messages_during=5))
    assert ranked[0].entity == "ord-api" and ranked[0].attribution["amplifier"]["victim"] == "ship-worker"


def test_message_queue_amplification_still_names_the_flooded_consumer_without_inventing_an_amplifier():
    """死區的另一面：指不到生產者，但也不准亂指——消費者照自己的症狀上榜，沒有人被標成放大者。"""
    ranked = _mq_rank(_mq_world(messages_during=5))
    assert ranked[0].entity == "ship-worker"
    assert all("amplifier" not in c.attribution for c in ranked)


# ── Runbook：RB-08 與放寬後的 RB-04 ──

AMP_ATTR = {"amplifier": {"type": "traffic_amplifier", "victim": VICTIM, "via": VICTIM, "load_ratio": 22.87,
                          "fanout": {"before": 1.0, "during": 25.0}, "fanout_ratio": 25.0, "moved_self_ms": 95.1,
                          "moved_error": 0.275, "victims": [VICTIM], "in_incident": False}}
SURGE_ATTR = {"ingress_surge": {"entry": GW, "load_ratio": 6.01, "entry_load_ratio": 6.01}}
DB_DEP = {"resource": "orders-db", "kind": "db", "subtype": "postgresql", "share": 0.9, "by": "latency"}
FAILED = SpanRecord("t1", "x", None, VICTIM, T, 900.0, True, status_code=503, name="POST /pay")  # 失敗的範例 trace


def facts(signals, attribution=None, sample=None, entity=AMP):
    top = RootCandidate(entity, 1.5, "r", "", attribution or {})
    return RunbookFacts(set(signals), set(), top, sample, "", [], False)


def ids(f):
    return [r.id for r in evaluate(f)]


def test_rb08_sits_between_rb02_and_rb03():
    priority = {r.id: r.priority for r in RUNBOOK_RULES}
    assert priority["RB-02"] > priority["RB-08"] > priority["RB-03"] and priority["RB-08"] == 85


def test_rb08_fires_on_amplifier_attribution_and_outranks_rb03():
    # 被灌爆的下游在拋錯、範例 trace 也失敗：RB-03 也命中，但主處方是 RB-08（查重試與熔斷，不是查程式邏輯）
    got = ids(facts({"errors", "latency"}, AMP_ATTR, FAILED))
    assert got[0] == "RB-08" and "RB-03" in got and "RB-04" not in got


def test_rb08_does_not_fire_without_the_amplifier_type():
    assert "RB-08" not in ids(facts({"traffic", "latency"}))
    assert "RB-08" not in ids(facts({"latency"}, {"amplifier": {"victim": VICTIM}}))  # 沒有 type：不認


def test_rb08_actions_quote_the_fan_out_numbers():
    f = facts({"latency"}, AMP_ATTR)
    text = " ".join(a.format(**f.fmt()) for a in evaluate(f)[0].actions)
    assert f"{AMP} 呼叫 {VICTIM} 的 retry policy 與 backoff" in text and "由 1.0 次變成 25.0 次（扇出 25.0 倍）" in text
    assert f"擴容 {VICTIM} 沒有用" in text and "circuit breaker" in text


def test_rb04_fires_on_ingress_surge_even_when_the_sample_trace_failed():
    """放寬的那一半：流量確定是外部入口打進來的，範例 trace 失敗也算容量飽和（被灌爆本來就會拋錯）。"""
    assert ids(facts({"latency"}, SURGE_ATTR, FAILED, entity=VICTIM)) == ["RB-04"]
    assert ids(facts({"latency"}, SURGE_ATTR, entity=VICTIM)) == ["RB-04"]


def test_rb04_original_condition_is_unchanged_without_ingress_evidence():
    """沒有入口突波證據時維持原規則：traffic＋latency 而且範例沒失敗才命中；範例失敗就不是 RB-04。"""
    assert "RB-04" in ids(facts({"traffic", "latency"}, entity=VICTIM))
    assert "RB-04" not in ids(facts({"traffic", "latency"}, sample=FAILED, entity=VICTIM))


def test_open_decision_rb03_still_outranks_rb04_when_errors_and_a_failed_sample_coexist():
    """照現況寫死（待決）：入口突波＋errors 訊號＋失敗範例時 RB-03（80）仍排在 RB-04（70）前面，RB-04 只列在 also。
    要讓 RB-04 當主處方得調優先序或讓 RB-03 讓位，這次沒有動（會影響既有 RB-03 案例）。"""
    assert ids(facts({"errors", "latency"}, SURGE_ATTR, FAILED, entity=VICTIM)) == ["RB-03", "RB-04"]


def test_resource_dominated_amplifier_keeps_rb02_first():
    """放大者自己同時卡在資料庫：RB-02（90）在前、RB-08（85）跟著列出，兩條都要查。"""
    assert ids(facts({"latency"}, {**DB_DEP, **AMP_ATTR}))[:2] == ["RB-02", "RB-08"]


# ── 三行摘要：模板與封閉詞表 ──


def _briefing_context(attribution, entities, top, dependency=""):
    incident = Incident([AnomalyEvent(e, "latency", T + timedelta(minutes=m), 6.0, {"signal": "latency"})
                         for e in entities for m in range(0, 30, 5)])
    incident.severity = "P0"
    ctx = PipelineContext(pipeline="rca_report", now=T + timedelta(hours=1))
    ctx.incidents, ctx.incidents_built = [incident], True
    ctx.rca_root_candidates = [RootCandidate(top, 1.5, "自身耗時 30.0→30.2", dependency, attribution)]
    OperatorRegistry.create("rca.section.suggested_actions", {}).execute(ctx)
    return narrative.build_context(ctx, incident)


def _valid(briefing, context):
    return narrative.validate_briefing(briefing, context, set(context["incident_meta"]["entities"]))


@pytest.mark.parametrize("in_incident", [True, False])
def test_amplifier_template_briefing_passes_the_closed_lexicon(in_incident):
    attr = {"amplifier": {**AMP_ATTR["amplifier"], "in_incident": in_incident}}
    context = _briefing_context(attr, ALL if in_incident else (VICTIM,), AMP)
    assert context["matched_runbook"]["id"] == "RB-08"
    briefing = narrative.briefing_template(context)
    assert _valid(briefing, context) is None, briefing
    assert f"{AMP} 把 {VICTIM} 的流量放大到 22.87 倍" in briefing["summary_line"] and "1.0→25.0 次（扇出 25.0 倍）" in briefing["summary_line"]
    assert f"排除 {VICTIM} 本身的程式與容量" in briefing["verification_line"]
    assert ("不在事件裡" in briefing["verification_line"]) is (not in_incident)


def test_silent_amplifier_and_its_victim_are_whitelisted_even_though_only_one_is_in_the_incident():
    """13b：order-svc 不在事件裡，三行摘要仍要能寫它的名字（它是第一名候選），不能被當成編造的實體擋掉。"""
    context = _briefing_context(AMP_ATTR, (VICTIM,), AMP)
    assert AMP not in context["incident_meta"]["entities"]
    assert _valid(narrative.briefing_template(context), context) is None


def test_ingress_surge_template_briefing_passes_the_closed_lexicon():
    context = _briefing_context(SURGE_ATTR, ALL, VICTIM)
    assert context["matched_runbook"]["id"] == "RB-04"
    briefing = narrative.briefing_template(context)
    assert _valid(briefing, context) is None, briefing
    assert f"流量由外部入口 {GW} 打進來（{VICTIM} 流入 6.01 倍），沒有內部服務放大" in briefing["summary_line"]


def test_llm_wording_for_retry_storms_is_not_rejected():
    """PR B 封閉詞表：LLM 照 RB-08 處方寫 retry policy／backoff／tight loop／fan-out／circuit breaker／rate limiting 不准被誤拒。"""
    context = _briefing_context(AMP_ATTR, (VICTIM,), AMP)
    reply = {
        "summary_line": f"【一句話根因】{AMP} 的 retry storm 把 {VICTIM} 的流量放大到 22.87 倍，fan-out 由 1.0 升至 25.0 次。",
        "action_line": f"【立即止血動作】檢查 {AMP} 的 retry policy 與 backoff，排查 tight loop，啟用 circuit breaker 或對 {VICTIM} 做 rate limiting。",
        "verification_line": f"【驗證與排除項】確認 {AMP} 對 {VICTIM} 的 fan-out 回到 1.0 次；{VICTIM} 本身的容量不是源頭，throttling 後應回落。",
    }
    assert _valid(reply, context) is None


def test_closed_lexicon_still_rejects_an_invented_service_and_an_invented_number():
    """詞表放寬的是通用術語，不是守門：編一個不存在的服務、或 context 沒有的倍數，照樣擋。"""
    context = _briefing_context(AMP_ATTR, (VICTIM,), AMP)
    ok = narrative.briefing_template(context)
    assert _valid({**ok, "summary_line": ok["summary_line"].replace(AMP, "checkout-svc")}, context) is not None
    assert _valid({**ok, "summary_line": ok["summary_line"].replace("22.87", "80.5")}, context) is not None
