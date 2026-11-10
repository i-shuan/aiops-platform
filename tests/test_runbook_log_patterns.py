"""Runbook 接錯誤日誌樣板（RB-09 記憶體耗盡、RB-10 連線池耗盡、RB-11 死結）與 RB-03 的 HTTP／訊息措辭。

鐵律：日誌關鍵字只在「問題在服務自己或它的資料庫／快取」時開處方；外部依賴主導、證據不足、別的服務的日誌、
長年都在噴的樣板一律不算。全部用 ctx 測，不連外。
"""
from datetime import datetime, timedelta, timezone

import pytest

from aiops.engine.context import LogRecord, PipelineContext, RootCandidate, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.rca.sections.actions import (LOG_TOP_N, MIN_BURST_RATIO, RUNBOOK_RULES, RunbookFacts, evaluate,
                                               match_log_signatures)

T0 = datetime(2026, 11, 3, 15, 40, tzinfo=timezone.utc)


def _pattern(error_type="", template="", new=True, ratio=0.0, service="bill-svc", during=40.0, baseline=0.0, example=""):
    return {"service": service, "error_type": error_type, "template": template, "during": during, "baseline": baseline,
            "rate_during": during / 10, "rate_baseline": baseline / 60, "ratio": ratio, "new": new,
            "example": example or template, "trace_id": "t1"}


OOM = _pattern("java.lang.OutOfMemoryError", "Java heap space")
POOL = _pattern("SQLTransientConnectionException", "HikariPool-<n> - Connection is not available, request timed out after <n>ms.")
DEADLOCK = _pattern("PSQLException", "ERROR: deadlock detected Detail: Process <n> waits for ShareLock on transaction <n>")


def _attr(kind, share, resource="orders-db", fail_during=0):
    return {"resource": resource, "kind": kind, "subtype": "postgresql" if kind == "db" else "http", "share": share,
            "by": "latency" if share >= 0.6 else "", "failures": {"before": 0, "during": fail_during}}


def _facts(patterns, attribution=None, signals=("errors",), sample=None, insufficient=False):
    top = RootCandidate("bill-svc", 1.0, "r", "", attribution or {})
    return RunbookFacts(set(signals), set(), top, sample, "", list(patterns), insufficient)


def _ids(facts):
    return [r.id for r in evaluate(facts)]


# 1. 關鍵字比對
@pytest.mark.parametrize("text, key", [
    ("java.lang.OutOfMemoryError: Java heap space", "oom"),
    ("Container was OOMKilled (exit code 137)", "oom"),
    ("GC overhead limit exceeded", "oom"),
    ("FATAL ERROR: Reached heap limit Allocation failed - JavaScript heap out of memory", "oom"),
    ("ConnectionPoolExhausted: no free connection", "pool"),
    ("HikariPool-1 - Connection is not available, request timed out after 30000ms.", "pool"),
    ("QueuePool limit of size 5 overflow 10 reached, connection timed out", "pool"),
    ("FATAL: sorry, too many clients already", "pool"),
    ("Too many connections", "pool"),
    ("Unable to acquire JDBC Connection", "pool"),
    ("Deadlock found when trying to get lock; try restarting transaction", "deadlock"),
    ("ERROR: deadlock detected", "deadlock"),
    ("Lock wait timeout exceeded; try restarting transaction", "deadlock"),
    ("org.hibernate.exception.LockAcquisitionException: could not execute statement", "deadlock"),
])
def test_signature_keywords_match_real_world_messages(text, key):
    assert list(match_log_signatures([_pattern(template=text)])) == [key]


@pytest.mark.parametrize("text", [
    "payment worker pool exhausted",                 # 執行緒池不是連線池（混沌場景 13 的受害者日誌）
    "connection refused to 10.0.0.1:5432",           # 連不上不是池子滿
    "Downstream tax-gw:443 returned 503",
    "memory usage 71% of limit",                     # 有 memory 這個字不是 OOM
    "lock acquired for order 1234",
    "Read timed out",
])
def test_lookalike_messages_do_not_match(text):
    assert match_log_signatures([_pattern(template=text)]) == {}


def test_error_type_alone_is_enough_and_matching_ignores_case():
    assert list(match_log_signatures([_pattern("OutOfMemoryError", "allocation of <n> bytes failed")])) == ["oom"]
    assert list(match_log_signatures([_pattern("", "DEADLOCK DETECTED")])) == ["deadlock"]


def test_only_burst_patterns_count_a_chronic_deadlock_is_not_this_incident():
    chronic = _pattern("PSQLException", "ERROR: deadlock detected", new=False, ratio=1.1, baseline=220.0)
    assert match_log_signatures([chronic]) == {}
    assert match_log_signatures([{**chronic, "ratio": MIN_BURST_RATIO - 0.01}]) == {}
    assert list(match_log_signatures([{**chronic, "ratio": MIN_BURST_RATIO}])) == ["deadlock"]


def test_only_the_top_patterns_are_looked_at_and_the_highest_ranked_hit_is_kept():
    noise = [_pattern(template=f"request {i} failed") for i in range(LOG_TOP_N)]
    assert match_log_signatures(noise + [OOM]) == {}  # 第 4 名：不看
    first, second = _pattern(template="Java heap space", during=90.0), _pattern(template="OutOfMemoryError", during=5.0)
    assert match_log_signatures([first, second])["oom"] is first
    assert sorted(match_log_signatures([POOL, DEADLOCK, OOM])) == ["deadlock", "oom", "pool"]


# 2. 什麼時候才看日誌：自身異常（沒有主導的下游）或資料庫／快取
def test_self_fault_with_oom_logs_gets_the_memory_runbook_ahead_of_the_generic_app_rule():
    sample = SpanRecord("t", "s", None, "bill-svc", T0, 237.0, True, status_code=500, name="POST /invoice")
    assert _ids(_facts([OOM], sample=sample)) == ["RB-09", "RB-03"]
    assert _ids(_facts([OOM], signals=("latency",))) == ["RB-09"]  # 只有延遲告警、沒有失敗範例也開得出來
    assert _ids(_facts([], sample=sample)) == ["RB-03"]


def test_database_attribution_with_pool_or_deadlock_logs_gets_the_precise_runbook_ahead_of_rb02():
    db = _attr("db", 0.95)
    assert _ids(_facts([POOL], db)) == ["RB-10", "RB-02"]
    assert _ids(_facts([DEADLOCK], db)) == ["RB-11", "RB-02"]
    assert _ids(_facts([POOL, DEADLOCK, OOM], db)) == ["RB-09", "RB-10", "RB-11", "RB-02"]
    assert _ids(_facts([POOL], _attr("cache", 0.1, "redis", fail_during=25))) == ["RB-10", "RB-02"]
    assert _ids(_facts([], db)) == ["RB-02"]


def test_external_dependency_dominating_means_the_logs_are_a_symptom_and_are_ignored():
    assert _ids(_facts([POOL, OOM], _attr("external", 1.0, "tax-gw:443"))) == ["RB-01"]
    assert _ids(_facts([POOL], _attr("external", 0.5, "tax-gw:443"))) == ["RB-00"]  # 0.3～0.6：下游佔了一部分，不靠日誌下結論
    assert _ids(_facts([POOL], _attr("external", 0.29, "tax-gw:443"), signals=("latency",))) == ["RB-10"]


def test_insufficient_evidence_never_opens_a_log_prescription():
    assert _ids(_facts([OOM, POOL, DEADLOCK], insufficient=True, signals=("latency",))) == ["RB-06"]


def test_new_runbooks_sit_between_the_queue_rule_and_the_generic_database_rule():
    priority = {r.id: r.priority for r in RUNBOOK_RULES}
    assert priority["RB-07"] > priority["RB-09"] > priority["RB-10"] > priority["RB-11"] > priority["RB-02"]
    assert len(set(priority.values())) == len(priority)


def test_every_action_of_every_runbook_formats_with_no_facts_at_all():
    facts = RunbookFacts(set(), set(), None, None, "", [], False)
    texts = [a.format(**facts.fmt()) for r in RUNBOOK_RULES for a in r.actions]
    assert len(texts) >= 30 and not any("{" in t for t in texts)


# 3. 整段：ctx.rca_logs → 處方文字
def _logs(service, message, error_type, n, start_min=0, span_min=10):
    return [LogRecord(T0 + timedelta(minutes=start_min + i % span_min), service, message.format(i), error_type, f"t{i}") for i in range(n)]


def _ctx(candidates, spans=(), signal="latency", entity="bill-svc"):
    ctx = PipelineContext(pipeline="rca_report", now=T0 + timedelta(hours=1))
    ctx.incidents = [Incident([AnomalyEvent(entity, "m", T0, 6.0, {"signal": signal})])]
    ctx.incidents_built = True
    ctx.rca_spans = list(spans)
    ctx.rca_root_candidates = list(candidates)
    ctx.rca_log_window_minutes = (10.0, 60.0)
    return ctx


def _run(ctx):
    section = OperatorRegistry.create("rca.section.suggested_actions", {})
    return section.execute(ctx).rca_sections[0].body


def test_report_quotes_the_log_evidence_and_names_the_database():
    cand = RootCandidate("bill-svc", 1.0, "r", "orders-db（db/postgresql）", _attr("db", 0.97))
    ctx = _ctx([cand])
    ctx.rca_logs = _logs("bill-svc", "HikariPool-1 - Connection is not available, request timed out after 3000{}ms.",
                         "SQLTransientConnectionException", 42)
    body = _run(ctx)
    assert "[RB-10] 連線池耗盡" in body
    assert ("bill-svc 的錯誤日誌出現連線池耗盡：「SQLTransientConnectionException：HikariPool-<NUM> - Connection is not available, "
            "request timed out after <NUM>ms.」事件期間 42 筆（正常期間沒出現過）") in body
    assert "確認是池子打滿，而不是 orders-db 拒絕連線" in body
    assert ctx.rca_runbook["id"] == "RB-10" and ctx.rca_runbook["also"] == ["RB-02"]
    assert ctx.rca_runbook["log_signatures"] == ["pool"]


def test_report_states_the_burst_ratio_for_a_pattern_that_existed_before():
    ctx = _ctx([RootCandidate("bill-svc", 1.0, "r", "", {})])
    ctx.rca_logs = _logs("bill-svc", "ERROR: deadlock detected, process {} waits", "PSQLException", 50)
    ctx.rca_logs_baseline = _logs("bill-svc", "ERROR: deadlock detected, process {} waits", "PSQLException", 6, start_min=-70, span_min=60)
    body = _run(ctx)
    assert "[RB-11] 資料庫死結／鎖等待" in body and "事件期間 50 筆（正常期間 6 筆，頻率 50 倍）" in body
    assert "到 資料庫 查死結紀錄" in body  # 沒有 db 歸因：不亂填資源名


def test_logs_of_other_services_never_open_a_prescription_for_the_suspect():
    # 放大者把下游灌爆：受害者滿地都是連線池耗盡，嫌疑服務（第一名候選）自己的日誌沒有
    ctx = _ctx([RootCandidate("ord-api", 1.0, "r", "", {})], entity="pay-svc")
    ctx.rca_logs = _logs("pay-svc", "ConnectionPoolExhausted after {} ms", "PoolError", 80) + _logs("ord-api", "retry {} scheduled", "", 30)
    _run(ctx)
    assert ctx.rca_runbook["id"] == "RB-00" and ctx.rca_runbook["log_signatures"] == []


def test_without_candidates_the_incident_entities_are_the_suspects_and_service_names_ignore_case():
    ctx = _ctx([], entity="Cart-API")
    ctx.rca_logs = _logs("cart-api", "java.lang.OutOfMemoryError: Java heap space ({})", "OutOfMemoryError", 12)
    body = _run(ctx)
    assert ctx.rca_runbook["id"] == "RB-09" and "cart-api 的錯誤日誌出現記憶體耗盡" in body
    assert "OOMKilled" in body and "heap dump" in body


def test_no_logs_at_all_changes_nothing():
    ctx = _ctx([RootCandidate("bill-svc", 1.0, "r", "orders-db", _attr("db", 0.97))])
    _run(ctx)
    assert ctx.rca_runbook["id"] == "RB-02" and ctx.rca_runbook["log_signatures"] == []


# 4. RB-03 的措辭：HTTP 與訊息消費分開
def _rb03_first_action(sample):
    facts = _facts([], sample=sample)
    hit = evaluate(facts)[0]
    assert hit.id == "RB-03"
    return hit.ordered(facts)[0].format(**facts.fmt())


def test_rb03_http_request_keeps_its_status_code():
    sample = SpanRecord("t", "s", None, "bill-svc", T0, 237.0, True, status_code=500, name="POST /invoice")
    assert _rb03_first_action(sample) == "點下方範例 trace 檢查 POST /invoice（HTTP 500）的入參與堆疊日誌"


def test_rb03_message_consumer_is_not_described_as_an_http_failure():
    sample = SpanRecord("t", "s", None, "ship-worker", T0, 220.0, True, name="Kafka RECEIVE from order-events",
                        consumer=True, queue="order-events")
    text = _rb03_first_action(sample)
    assert text == "點下方範例 trace 檢查 Kafka RECEIVE from order-events（訊息消費失敗，佇列 order-events）的訊息內容（payload、headers）與堆疊日誌"
    assert "HTTP" not in text and "入參" not in text
    nameless = SpanRecord("t", "s", None, "ship-worker", T0, 220.0, True, name="Kafka RECEIVE", consumer=True)
    assert "（訊息消費失敗，佇列名稱未知）" in _rb03_first_action(nameless)


def test_rb03_failure_without_a_status_code_no_longer_claims_http():
    sample = SpanRecord("t", "s", None, "bill-svc", T0, 237.0, True, name="NightlyJob#run")
    assert _rb03_first_action(sample) == "點下方範例 trace 檢查 NightlyJob#run（失敗）的入參與堆疊日誌"
