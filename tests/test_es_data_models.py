"""Elasticsearch trace／日誌資料模型（sources/es_models.py）：Elastic APM（預設，行為不變）與 OTel Collector（otel mapping）。

OTel 的期望值一律來自 tests/fixtures/otel_es/ 的真實文件（Collector 0.165.0 + ES 8.19.23 實際寫出來的），
不是手寫的假文件；文件形狀的實測結論在那個資料夾的 README。
"""
import json
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from aiops.engine.errors import BadRequest
from aiops.engine.requests import MappingSubmit
from aiops.interfaces.runner import run
from aiops.sources.base import InvalidQueryError, as_utc_datetime
from aiops.sources.grafana import _span
from aiops.sources.es_models import DATA_MODEL_ENV, ELASTIC_APM, OTEL, current_model, get_path, otel_sampling_weight
from aiops.stages.ingestion.fetch import IncidentTracesFetch
from aiops.stages.rca import spans as rca_spans
from grafana_fake import grafana_spec

FIXTURES = Path(__file__).parent / "fixtures" / "otel_es"


def _docs(name: str) -> list[dict]:
    return [d["_source"] for d in json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))]


OTEL_SPANS = _docs("traces-generic.otel-default")
OTEL_LOGS = _docs("logs-generic.otel-default")


# ── 時間戳 ──


def test_numeric_timestamps_are_epoch_millis_like_elasticsearch_not_nanoseconds():
    # OTel exporter 在 _source 存的是「毫秒.次毫秒」字串；以前直接丟 pd.Timestamp 會溢位，整數會被解成 1970 年
    assert as_utc_datetime("1794487341966.212300") == datetime(2026, 11, 12, 12, 42, 21, 966212, tzinfo=timezone.utc)
    assert as_utc_datetime(1794487341966) == datetime(2026, 11, 12, 12, 42, 21, 966000, tzinfo=timezone.utc)
    assert as_utc_datetime("1794487341966") == as_utc_datetime(1794487341966)
    assert as_utc_datetime("2026-11-12T12:43:27.635552900Z") == datetime(2026, 11, 12, 12, 43, 27, 635552, tzinfo=timezone.utc)


# ── 選模型 ──


def test_default_model_is_elastic_apm_and_unknown_names_are_rejected(monkeypatch):
    monkeypatch.delenv(DATA_MODEL_ENV, raising=False)
    assert current_model() is ELASTIC_APM
    monkeypatch.setenv(DATA_MODEL_ENV, " OTel ")
    assert current_model() is OTEL
    monkeypatch.setenv(DATA_MODEL_ENV, "ecs")  # ECS 模式沒有耗時欄位，不提供（見下方死區）
    with pytest.raises(InvalidQueryError):
        current_model()


def test_elastic_apm_queries_are_exactly_what_they_were_before_profiles():
    """APM 是現況：查詢用的 _source、排序、索引必須跟拆成 profile 之前一字不差（改之前的常數抄在這裡）。"""
    r = ELASTIC_APM.resolve(None)
    before_span_fields = ["@timestamp", "trace.id", "span.id", "transaction.id", "parent.id", "service.name",
                          "span.duration.us", "transaction.duration.us", "event.outcome",
                          "span.type", "span.subtype", "span.destination.service.resource",
                          "http.response.status_code", "transaction.name",
                          "transaction.type", "transaction.message.queue.name", "transaction.message.age.ms", "span.links"]
    # 唯一刻意多撈的：取樣還原（階段 2）要的 representative_count；沒有這兩個欄位的索引權重一律 1
    added_for_sampling = ["transaction.representative_count", "span.representative_count"]
    # 環境區隔：每筆帶回 service.environment（Workflow.environment 的過濾與呼叫端的第二道檢查用）；沒設環境時查詢條件不變
    added_for_environment = ["service.environment"]
    assert sorted(r.span_source) == sorted(before_span_fields + added_for_sampling + added_for_environment)
    assert r.span_sort == [{"@timestamp": "asc"}, {"trace.id": "asc"}, {"span.id": {"order": "asc", "missing": "_first"}}]
    assert sorted(r.log_source) == sorted(["@timestamp", "service.name", "trace.id", "message", "error.exception",
                                           "error.log", "error.culprit", *added_for_environment])
    assert ELASTIC_APM.log_sort == [{"@timestamp": "asc"}, {"error.id": {"order": "asc", "missing": "_last"}}]
    assert (ELASTIC_APM.trace_index, ELASTIC_APM.log_index) == ("traces-apm-*", "logs-apm.*")
    assert ELASTIC_APM.log_filters == ()


# ── OTel span（真實樣本）──


def test_otel_spans_from_real_collector_output_keep_duration_errors_and_parents():
    r = OTEL.resolve(None)
    parsed = [r.span(d) for d in OTEL_SPANS]
    assert len(parsed) == 57 and all(parsed)
    for doc, s in zip(OTEL_SPANS, parsed):
        assert s["duration_ms"] == doc["duration"] / 1_000_000  # 奈秒 -> 毫秒（APM 是微秒，差 1000 倍）
        assert s["error"] == ((doc.get("status") or {}).get("code") == "Error")
        assert s["parent_id"] == doc.get("parent_span_id")
        assert s["service"] == doc["resource"]["attributes"]["service.name"]
        assert s["ts"] == as_utc_datetime(doc["@timestamp"])
    assert sum(s["duration_ms"] for s in parsed) == pytest.approx(4495.0)
    assert sum(s["error"] for s in parsed) == 8


def test_otel_span_kinds_map_onto_the_apm_semantics_downstream_already_uses():
    r = OTEL.resolve(None)
    parsed = [r.span(d) for d in OTEL_SPANS]
    kinds = Counter((s["kind"], s["subtype"], s["consumer"]) for s in parsed)
    assert kinds == {("", "", False): 16,               # Server：入口
                     ("", "kafka", True): 7,             # Consumer：入口＋訊息消費者（6 筆單筆 parent、1 筆批次 links）
                     ("external", "http", False): 14,    # Client（HTTP）：呼叫 cart-api 8、外部 tax-gw 6
                     ("db", "postgresql", False): 8,     # Client（db.system.name）
                     ("db", "redis", False): 6,
                     ("messaging", "kafka", False): 6}   # Producer
    resources = Counter(s["resource"] for s in parsed if s["resource"])
    assert resources == {"cart-api": 8,                   # peer.service 優先
                         "pg-main:5432": 8, "redis-cache:6379": 6, "tax-gw.example.net:443": 6,  # server.address:port
                         "kafka/orders": 6}               # 跟 APM 一樣的「系統/佇列」寫法
    consumers = [s for s in parsed if s["consumer"]]
    assert {s["queue"] for s in consumers} == {"orders"}
    assert all(s["age_ms"] is None for s in consumers)  # OTel 沒有標準的訊息年齡屬性
    batch = [s for s in consumers if s["links"]]
    assert len(batch) == 1 and len(batch[0]["links"]) == 3
    # 入口名稱只給入口 span（同 APM 的 transaction.name）
    assert Counter(s["name"] for s in parsed if s["name"]) == {"POST /checkout": 8, "POST /cart": 8, "orders process": 7}
    assert Counter(s["status_code"] for s in parsed) == {0: 27, 200: 24, 500: 4, 502: 2}


def test_rca_statistics_read_otel_spans_with_the_same_meaning_as_apm():
    """語意對照（不含取樣還原）：把權重拿掉，數字是逐 span 的原始統計。取樣還原另見 test_sampling_weights_*。"""
    r = OTEL.resolve(None)
    recs = [replace(IncidentTracesFetch._record(r.span(d), lambda x: x), weight=1.0) for d in OTEL_SPANS]
    lo, hi = min(s.ts for s in recs) - timedelta(seconds=1), max(s.ts for s in recs) + timedelta(seconds=1)
    stats = rca_spans.window_stats(recs, lo, hi)
    assert {k: v.requests for k, v in stats.items()} == {"checkout": 8, "cart-api": 8, "ord-svc": 7}
    # cart-api 的時間主要在等 PostgreSQL；錯誤是資料庫連帶的，不是 cart-api 自己的
    cart = stats["cart-api"]
    assert cart.wait_ms_per_request == pytest.approx({"pg-main:5432": 92.5, "redis-cache:6379": 1.5})
    assert (cart.error_rate, cart.own_error_rate) == (0.25, 0.0)
    assert rca_spans.inbound_callers(recs, lo, hi) == {"checkout": {"": 8}, "cart-api": {"checkout": 8},
                                                       "ord-svc": {"mq:checkout": 7}}
    sample = rca_spans.sample_trace(recs, lo, hi)
    assert (sample.service, sample.status_code) == ("checkout", 502)
    assert rca_spans.sample_trace_reason(sample, recs) == "入口 502，cart-api、pg-main:5432 失敗"


def test_otel_old_semconv_attributes_still_map_and_field_maps_override_the_profile():
    doc = {"@timestamp": "1794487341966.0", "trace_id": "t", "span_id": "s", "kind": "Client", "duration": 5_000_000,
           "status": {}, "resource": {"attributes": {"service.name": "Cart-API"}},
           "attributes": {"db.system": "mysql", "net.peer.name": "db-1", "http.status_code": 0,
                          "custom.backend": "orders-db"}}
    s = OTEL.resolve(None).span(doc)
    assert (s["kind"], s["subtype"], s["resource"], s["service"], s["duration_ms"]) == ("db", "mysql", "db-1", "cart-api", 5.0)
    # 已核准的欄位對照把自訂屬性指成下游服務名稱：蓋過 profile 的預設來源欄
    s = OTEL.resolve({"attributes.custom.backend": "span.peer_service", "@timestamp": "ts"}).span(doc)
    assert s["resource"] == "orders-db"


def test_field_map_overrides_also_change_the_query_fields():
    r = OTEL.resolve({"resource.attributes.app": "span.service", "attributes.my.trace": "span.trace_id"})
    assert r.query_field("span.service") == "resource.attributes.app"
    assert r.span_sort[1] == {"attributes.my.trace": "asc"}
    assert "resource.attributes.app" in r.span_source and "resource.attributes.service.name" in r.span_source


def test_apm_field_map_override_is_used_only_when_set():
    doc = {"@timestamp": "2026-11-03T14:00:00Z", "trace": {"id": "t"}, "service": {"name": "a"},
           "transaction": {"id": "x", "duration": {"us": 2000}, "name": "GET /"}, "labels": {"route": "GET /v2"}}
    assert ELASTIC_APM.resolve(None).span(doc) == _span(doc)
    assert ELASTIC_APM.resolve(None).span(doc)["name"] == "GET /"
    assert ELASTIC_APM.resolve({"labels.route": "span.name"}).span(doc)["name"] == "GET /v2"


def test_get_path_reads_nested_objects_and_flat_dotted_keys():
    doc = {"attributes": {"db.system.name": "redis"}, "span": {"destination": {"service": {"resource": "r"}}},
           "a": {"b.c": {"d": 1}}}
    assert get_path(doc, "attributes.db.system.name") == "redis"
    assert get_path(doc, "span.destination.service.resource") == "r"
    assert get_path(doc, "a.b.c.d") == 1
    assert get_path(doc, "attributes.db.system") is None


# ── OTel 日誌（真實樣本）──


def test_otel_logs_take_body_text_or_exception_message_and_structured_bodies():
    r = OTEL.resolve(None)
    parsed = [r.log(d) for d in OTEL_LOGS]
    assert all(parsed)
    by_kind = {(d.get("event_name"), d.get("severity_text")): p for d, p in zip(OTEL_LOGS, parsed)}
    exc = by_kind[("exception", None)]  # span 的 exception 事件：沒有 body，訊息取 exception.message
    assert (exc["message"], exc["error_type"]) == ("FATAL: remaining connection slots are reserved",
                                                   "org.postgresql.util.PSQLException")
    assert exc["trace_id"]
    err = by_kind[(None, "ERROR")]
    assert err["message"].startswith(("checkout failed", "rebalance failed"))
    warn = by_kind[(None, "WARN")]  # 結構化 body：取 msg
    assert warn["message"] == "consumer lag high"


def test_otel_log_query_keeps_only_errors_because_the_stream_mixes_all_levels(monkeypatch):
    """查詢層的過濾：severity_number >= 17 或 exception 事件。用 MockTransport 照 ES 的語意套用同一個過濾，
    7 筆真實日誌裡 INFO、WARN 被濾掉，剩 5 筆（2 筆 exception 事件、3 筆 ERROR）。"""
    monkeypatch.setenv(DATA_MODEL_ENV, "otel")
    seen = []

    def handler(req):
        body = json.loads(req.content)
        seen.append((req.url.path, body))
        keep = [d for d in OTEL_LOGS if (d.get("severity_number") or 0) >= 17 or d.get("event_name") == "exception"]
        hits = [{"_source": d, "sort": [i]} for i, d in enumerate(keep)] if "search_after" not in body else []
        return httpx.Response(200, json={"hits": {"total": {"value": len(keep)}, "hits": hits}})

    spec = grafana_spec(handler)[0]
    t = datetime(2026, 11, 12, tzinfo=timezone.utc)
    logs, cut = spec.fetch_logs(t, t + timedelta(hours=1), ["cart-api"])
    assert len(logs) == 5 and not cut
    path, body = seen[0]
    assert path == "/logs-*.otel-*/_search"
    filters = body["query"]["bool"]["filter"]
    assert {"range": {"severity_number": {"gte": 17}}} in filters[-1]["bool"]["should"]
    assert {"term": {"event_name": "exception"}} in filters[-1]["bool"]["should"]
    assert filters[1]["bool"]["should"][0]["term"] == {"resource.attributes.service.name": {"value": "cart-api",
                                                                                            "case_insensitive": True}}


def test_otel_span_query_uses_otel_ids_for_sort_and_trace_lookups(monkeypatch):
    monkeypatch.setenv(DATA_MODEL_ENV, "otel")
    seen = []

    def handler(req):
        body = json.loads(req.content)
        seen.append((req.url.path, body))
        hits = [] if "search_after" in body else [{"_source": d, "sort": [i]} for i, d in enumerate(OTEL_SPANS)]
        return httpx.Response(200, json={"hits": {"total": {"value": len(OTEL_SPANS)}, "hits": hits}})

    spec = grafana_spec(handler)[0]
    t = datetime(2026, 11, 12, tzinfo=timezone.utc)
    spans, cut = spec.fetch_spans(t, t + timedelta(hours=1))
    assert len(spans) == 57 and not cut
    assert seen[0][0] == "/traces-*.otel-*/_search"
    assert seen[0][1]["sort"] == [{"@timestamp": "asc"}, {"trace_id": "asc"},
                                  {"span_id": {"order": "asc", "missing": "_first"}}]
    seen.clear()
    spec.fetch_linked_spans(["abc"], t, t + timedelta(hours=1))
    assert {"terms": {"trace_id": ["abc"]}} in seen[0][1]["query"]["bool"]["filter"]


# ── 取樣還原（階段 2）──


def test_otel_tracestate_threshold_and_legacy_p_value_become_adjusted_counts():
    # OTel tracestate probability sampling：權重 = 2^56 / (2^56 − T)；舊版 p:n -> 2^n
    assert otel_sampling_weight("ot=th:c") == 4.0      # 拒絕 75%，取樣 25%
    assert otel_sampling_weight("ot=th:8") == 2.0
    assert otel_sampling_weight("ot=th:0") == 1.0
    assert otel_sampling_weight("ot=th:c;rv:abcdef01234567") == 4.0
    assert otel_sampling_weight("vendor=x,ot=th:8") == 2.0
    assert otel_sampling_weight("ot=p:2") == 4.0
    for no_info in (None, "", "vendor=x", "ot=p:63", "ot=th:zz", "ot=rv:abcdef01234567", 4):
        assert otel_sampling_weight(no_info) == 1.0
    # 真實樣本裡的 trace_state 原樣進來
    assert Counter(OTEL.resolve(None).span(d)["weight"] for d in OTEL_SPANS) == {4.0: 28, 1.0: 21, 2.0: 8}


def test_apm_representative_count_is_the_weight_and_bad_values_mean_unsampled():
    base = {"@timestamp": "2026-11-03T14:00:00Z", "trace": {"id": "t"}, "service": {"name": "a"}}
    txn = {**base, "transaction": {"id": "x", "duration": {"us": 1000}, "representative_count": 10}}
    assert _span(txn)["weight"] == 10.0
    span = {**base, "span": {"id": "s", "duration": {"us": 1000}, "representative_count": "2.5"},
            "transaction": {"id": "x", "representative_count": 10}}
    assert _span(span)["weight"] == 2.5  # span 文件看 span 自己的，不是所屬 transaction 的
    for bad in (None, 0, -3, "abc", True, float("nan"), float("inf")):
        assert _span({**base, "transaction": {"id": "x", "representative_count": bad}})["weight"] == 1.0


def test_sampling_weights_restore_request_counts_and_rates_but_not_evidence_counts():
    """真實樣本：checkout 撈到 8 個入口 span，權重 1,1,4,2,4,4（正常）＋ 1,4（錯誤）-> 估計 21 個請求。
    錯誤率 5/21（不是 2/8）；平均耗時是加權平均；樣本數（requests、wait_failures）仍是實際筆數。"""
    r = OTEL.resolve(None)
    recs = [IncidentTracesFetch._record(r.span(d), lambda x: x) for d in OTEL_SPANS]
    lo, hi = min(s.ts for s in recs) - timedelta(seconds=1), max(s.ts for s in recs) + timedelta(seconds=1)
    stats = rca_spans.window_stats(recs, lo, hi)
    checkout, cart = stats["checkout"], stats["cart-api"]
    assert (checkout.requests, checkout.weighted_requests) == (8, 21)
    assert checkout.error_rate == pytest.approx(5 / 21)
    assert checkout.total_ms_mean == pytest.approx((120 * 16 + 300 * 5) / 21)
    assert cart.total_ms_mean == pytest.approx((75 * 16 + 270 * 5) / 21)
    assert cart.wait_ms_per_request["pg-main:5432"] == pytest.approx((40 * 16 + 250 * 5) / 21)
    assert cart.wait_failures == {"pg-main:5432": 2, "redis-cache:6379": 0}  # 筆數，不加權
    assert (stats["ord-svc"].requests, stats["ord-svc"].weighted_requests) == (7, 17)  # 批次消費者沒有 trace_state
    assert rca_spans.inbound_callers(recs, lo, hi) == {"checkout": {"": 21}, "cart-api": {"checkout": 21},
                                                       "ord-svc": {"mq:checkout": 17}}


def test_unit_weights_leave_every_statistic_exactly_as_before():
    r = OTEL.resolve(None)
    recs = [replace(IncidentTracesFetch._record(r.span(d), lambda x: x), weight=1.0) for d in OTEL_SPANS]
    lo, hi = min(s.ts for s in recs) - timedelta(seconds=1), max(s.ts for s in recs) + timedelta(seconds=1)
    for st in rca_spans.window_stats(recs, lo, hi).values():
        assert st.weighted_requests == st.requests and isinstance(st.weighted_requests, int)
    callers = rca_spans.inbound_callers(recs, lo, hi)
    assert all(isinstance(n, int) for v in callers.values() for n in v.values())


# ── 欄位對照提交 ──


def test_mapping_submit_rejects_standard_fields_nobody_reads():
    with pytest.raises(BadRequest, match="span.peer_servic"):
        run("admin_mapping_submit", request=MappingSubmit(source_type="grafana",
                                                          fields={"attributes.backend": "span.peer_servic"}, submitter="amy"))
    ok = run("admin_mapping_submit", request=MappingSubmit(source_type="grafana",
                                                           fields={"attributes.backend": "span.peer_service",
                                                                   "@timestamp": "ts"}, submitter="amy"))
    assert ok["mapping"]["status"] == "pending"


# ── 死區：Collector 的 ecs mapping 模式 ──


@pytest.mark.xfail(strict=True, reason="死區：OTel Collector 0.165 的 ecs mapping 模式寫出的 span 沒有任何耗時欄位"
                                       "（也沒有 trace_state），自身耗時、延遲佐證、取樣還原都做不到；所以不提供 ecs profile")
def test_ecs_mode_spans_carry_a_duration():
    docs = _docs("traces-generic-default")
    assert len(docs) == 57

    def keys(d, prefix=""):
        for k, v in d.items():
            yield from keys(v, f"{prefix}{k}.") if isinstance(v, dict) else [f"{prefix}{k}"]

    assert any("duration" in k for d in docs for k in keys(d))
