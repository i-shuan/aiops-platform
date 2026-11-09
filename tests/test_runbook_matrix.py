"""建議處置的確定性 Runbook Matrix（docs/RCA_STEP2_DESIGN.md 第 3 節、第 6.3 節）。

鐵律：不看 metric 名稱的子字串（duration 不准給 io）；訊號類別先看 attrs.signal、再看整字 token；
命中的規則全列、全部不命中才保底並寫明；沒有 span、沒有候選（工廠）照樣跑不丟例外。全部用 ctx 測，不連外。
"""
from datetime import datetime, timedelta, timezone

from aiops.engine.context import PipelineContext, RootCandidate, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.rca.sections.actions import FALLBACK, RUNBOOK_RULES, RunbookFacts, evaluate, signal_of

T0 = datetime(2026, 11, 3, 15, 40, tzinfo=timezone.utc)
MIN = timedelta(minutes=1)
ES_QUERY = ('{"index": "metrics-apm.transaction.1m-*", "agg": "avg", "value_field": "transaction.duration.summary.sum", '
            '"filter": {"term": {"service.name": "bill-svc"}}}')


def _ids(rules):
    return [r.id for r in rules]


def _attr(kind, subtype, share, fail_before=0, fail_during=0, resource="tax-gw:443"):
    return {"resource": resource, "kind": kind, "subtype": subtype, "share": share, "by": "latency" if share >= 0.6 else "",
            "self_ms": {"before": 13.2, "during": 12.6}, "wait_ms": {"before": 13.3, "during": 101.4},
            "failures": {"before": fail_before, "during": fail_during}}


def _facts(signals=(), tokens=(), attribution=None, sample=None, insufficient=False):
    top = RootCandidate("bill-svc", 1.0, "r", "x" if attribution and attribution["by"] else "", attribution or {}) if attribution is not None else None
    return RunbookFacts(set(signals), set(tokens), top, sample, "", [], insufficient)


# 1. 訊號類別判定
def test_signal_comes_from_attrs_first_then_whole_word_tokens_never_substrings():
    assert signal_of(ES_QUERY, {"signal": "latency"}) == "latency"
    assert signal_of("whatever", {"signal": "errors"}) == "errors"
    assert signal_of(ES_QUERY, {}) == "latency"  # transaction.duration.summary.sum：token duration
    assert signal_of("http_request_duration_seconds", None) == "latency"  # durat-io-n 不是 io
    assert signal_of("node_disk_io_time_seconds_total", None) == "saturation"
    assert signal_of("http_requests_total", None) == "traffic"
    assert signal_of('rate(http_requests_total{code=~"5.."}[5m])', None) == "traffic"
    assert signal_of("cpu{service=\"cart\"}", None) == "saturation"
    assert signal_of("request_duration_errors", None) == ""  # 同時像兩類：不認
    assert signal_of("rf_power", None) == ""  # 工廠參數不是黃金訊號


# 2. 八條規則各自的命中與不命中
def test_rb01_external_dependency_dominates():
    assert _ids(evaluate(_facts(attribution=_attr("external", "http", 1.0)))) == ["RB-01"]
    assert "RB-01" not in _ids(evaluate(_facts(attribution=_attr("external", "http", 0.5))))
    assert "RB-01" not in _ids(evaluate(_facts(attribution=_attr("db", "oracle", 1.0))))


def test_rb02_database_or_cache_by_share_or_failures():
    assert _ids(evaluate(_facts(attribution=_attr("db", "oracle", 0.99, resource="oracle")))) == ["RB-02"]
    assert _ids(evaluate(_facts(attribution=_attr("cache", "redis", 0.1, fail_before=1, fail_during=25, resource="redis")))) == ["RB-02"]
    assert "RB-02" not in _ids(evaluate(_facts(attribution=_attr("db", "oracle", 0.1, fail_before=3, fail_during=3))))


def _sample(status=500, error=True, name="POST /invoice"):
    return SpanRecord("t", "s", None, "bill-svc", T0, 237.0, error, status_code=status, name=name)


def test_rb03_application_error_needs_errors_signal_failed_sample_and_no_dominant_resource():
    assert _ids(evaluate(_facts({"errors"}, sample=_sample()))) == ["RB-03"]
    assert _ids(evaluate(_facts({"errors"}, attribution=_attr("external", "http", 0.2), sample=_sample()))) == ["RB-03"]
    assert "RB-03" not in _ids(evaluate(_facts({"errors"}, attribution=_attr("external", "http", 0.9), sample=_sample())))
    assert "RB-03" not in _ids(evaluate(_facts({"latency"}, sample=_sample())))  # 11/3 真實資料只有 latency 訊號
    assert "RB-03" not in _ids(evaluate(_facts({"errors"}, sample=_sample(200, False))))


def test_rb04_capacity_needs_traffic_and_latency_without_failed_sample():
    assert _ids(evaluate(_facts({"traffic", "latency"}))) == ["RB-04"]
    assert _ids(evaluate(_facts({"traffic", "latency"}, sample=_sample(200, False)))) == ["RB-04"]
    assert "RB-04" not in _ids(evaluate(_facts({"traffic", "latency"}, sample=_sample())))
    assert "RB-04" not in _ids(evaluate(_facts({"latency"})))


def test_rb05_disk_only_on_whole_word_tokens():
    assert _ids(evaluate(_facts(tokens={"node", "disk", "io", "time"}))) == ["RB-05"]
    assert "RB-05" not in _ids(evaluate(_facts(tokens={"http", "request", "duration", "seconds"})))


def test_rb06_insufficient_evidence_and_factory_and_fallback():
    assert _ids(evaluate(_facts(insufficient=True))) == ["RB-06"]
    assert _ids(evaluate(_facts(tokens={"rf", "power"}))) == ["RB-F1"]
    assert _ids(evaluate(_facts(signals={"rf_power"}))) == ["RB-F1"]
    assert evaluate(_facts()) == [FALLBACK]


# 3. 多條同時命中依 priority；保底只在全部不命中時
def test_multiple_hits_are_ordered_by_priority_and_fallback_never_mixes_in():
    hits = evaluate(_facts({"errors"}, tokens={"disk", "io"}, sample=_sample()))
    assert _ids(hits) == ["RB-03", "RB-05"] and FALLBACK not in hits
    assert sorted({r.priority for r in RUNBOOK_RULES}, reverse=True) == [r.priority for r in sorted(RUNBOOK_RULES, key=lambda r: -r.priority)]


def _ctx(events, spans=(), candidates=(), note=""):
    ctx = PipelineContext(pipeline="rca_report", now=T0 + timedelta(hours=1))
    ctx.incidents = [Incident(list(events))]
    ctx.incidents_built = True
    ctx.rca_spans = list(spans)
    ctx.rca_root_candidates = list(candidates)
    ctx.rca_root_note = note
    return ctx


def _body(ctx):
    section = OperatorRegistry.create("rca.section.suggested_actions", {})
    return section.execute(ctx).rca_sections[0].body, ctx


# 4. 處方文字帶入資源名與範例 trace；5. ctx.rca_runbook
def test_actions_are_formatted_with_resource_and_endpoint_and_runbook_is_recorded_on_ctx():
    spans = [SpanRecord("t1", "x1", None, "bill-svc", T0, 237.0, True, status_code=500, name="POST /invoice"),
             SpanRecord("t1", "w1", "x1", "bill-svc", T0, 223.0, True, kind="external", subtype="http", resource="tax-gw:443")]
    cand = RootCandidate("bill-svc", 1.0, "自身耗時 26.5→114.0", "tax-gw:443（external/http）", _attr("external", "http", 1.0))
    body, ctx = _body(_ctx([AnomalyEvent("bill-svc", ES_QUERY, T0, 6.0, {"signal": "latency"})], spans, [cand]))
    assert body.splitlines()[0] == "1. 優先處理可能的源頭：bill-svc"
    assert "bill-svc 的增量主要在等待 tax-gw:443（external/http）" in body
    assert "[RB-01] 下游外部服務超時或激增" in body
    assert "檢查下游依賴目標 tax-gw:443 的健康狀態與網路延遲" in body
    assert "磁碟" not in body and "RB-00" not in body
    assert ctx.rca_runbook["id"] == "RB-01" and ctx.rca_runbook["signals"] == ["latency"] and ctx.rca_runbook["also"] == []
    assert ctx.rca_runbook["actions"][0].startswith("檢查下游依賴目標 tax-gw:443")


def test_factory_without_spans_or_candidates_gets_the_process_rule_and_never_raises():
    body, ctx = _body(_ctx([AnomalyEvent("ETCH-01", 'rf_power{tool="ETCH-01"}', T0, 5.0, {"dept": "fab-etch", "signal": "rf_power"})]))
    assert "[RB-F1] 製程機台參數偏移" in body and "保養（PM）紀錄" in body and ctx.rca_runbook["id"] == "RB-F1"
    body, ctx = _body(_ctx([AnomalyEvent("x", "metric_a", T0, 5.0)]))
    assert "[RB-00] 沒有命中任何故障特徵" in body and "證據不足以分類" in body and ctx.rca_runbook["id"] == "RB-00"


# 6. 回歸：duration 不再觸發磁碟處方（第一步驗收報告看到的那條）
def test_pure_latency_incident_never_gets_a_disk_prescription():
    for metric in ("http_request_duration_seconds", ES_QUERY):
        body, _ = _body(_ctx([AnomalyEvent("cart-api", metric, T0, 6.0)]))
        assert "磁碟" not in body and "I/O" not in body, metric
    body, _ = _body(_ctx([AnomalyEvent("node-1", "node_disk_io_time_seconds_total", T0, 6.0)]))
    assert "[RB-05] 磁碟 I/O 壓力" in body


def test_insufficient_evidence_maps_to_common_external_cause_rule():
    body, ctx = _body(_ctx([AnomalyEvent("cart-api", ES_QUERY, T0, 6.0)], note="證據不足，不指認根因：沒有任何服務的自身耗時明顯上升"))
    assert "[RB-06] 共同外部因素" in body and "優先處理可能的源頭" not in body and ctx.rca_runbook["id"] == "RB-06"


# 7. attrs.signal 讓黃金訊號加權在真實 ES target 上活過來（以前 metric 是查詢 JSON，永遠對不上）
def test_golden_signals_boost_fires_once_targets_declare_a_signal():
    plain = _ctx([AnomalyEvent("bill-svc", ES_QUERY, T0, 6.0, {"dept": "payments"})])
    tagged = _ctx([AnomalyEvent("bill-svc", ES_QUERY, T0, 6.0, {"dept": "payments", "signal": "latency"})])
    before = (plain.incidents[0].score, tagged.incidents[0].score)
    op = OperatorRegistry.create("alert.golden_signals", {"boost": 2.0})
    op.execute(plain)
    op.execute(tagged)
    assert plain.incidents[0].score == before[0] and tagged.incidents[0].score == before[1] * 2.0
