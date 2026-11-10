"""訊息佇列（非同步邊）接進管線的那幾段：撈取、資料源、偵測端收斂、拓撲沉澱（docs/MQ_QPS_DESIGN.md P0）。

純函式（engine/mq.py）、根因排序、Runbook 與摘要在 tests/test_rca_messaging.py；這裡測的是「接線」：
- ingestion.incident_traces 的 follow_messaging：什麼時候補撈、補撈哪一段、補撈失敗怎麼退回
- ElasticsearchSpec：messaging transaction 文件怎麼讀、fetch_linked_spans 送出什麼查詢
- alert.messaging_link：有方向、時間窗跟著延遲走；跟 alert.absorb_open 的邊界（已知做不到的用 xfail 留著）
- service_async_edges：admin.topology_refresh 寫入與汰除、StorageHooks 載入、內建 YAML 的節點位置
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

import aiops.sources as sources
from aiops.engine.context import OpenIncident, PipelineContext
from aiops.engine.errors import InfrastructureError
from aiops.engine.pipeline import Pipeline, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import ExternalAnomalyIn, IngestAnomalies, TopologyRefresh
from aiops.engine.types import AnomalyEvent, Incident
from aiops.interfaces.runner import run, workflow_path
from aiops.sources.base import BaseDataSourceSpec, InvalidQueryError
from aiops.sources.elasticsearch import ElasticsearchSpec
from aiops.sources.elasticsearch import _links as es_links
from aiops.sources.elasticsearch import _span as es_span
from aiops.storage.hooks import StorageHooks
from aiops.storage.repository import AsyncEdges, ServiceEdges

T = datetime(2026, 11, 3, 14, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
PRODUCER, CONSUMER, QUEUE = "ord-api", "ship-worker", "order-events"


def at(minutes: float = 0, seconds: float = 0) -> datetime:
    return T + timedelta(minutes=minutes, seconds=seconds)


# ── ingestion.incident_traces：follow_messaging ──


def sd(trace, sid, parent, svc, ts, **kw) -> dict:
    """資料源回傳的 span（dict，跟 ElasticsearchSpec._span 同一組鍵）。"""
    return {"trace_id": trace, "span_id": sid, "parent_id": parent, "service": svc, "ts": ts, "duration_ms": 5.0,
            "error": False, **kw}


def published(i: int, how: str = "parent") -> list[dict]:
    """第 i 則訊息：ord-api 在事件前 20 分鐘發（主撈取窗只往前 15 分鐘，看不到），ship-worker 在事件期間才消費到。
    how：parent = 消費者接在 send span 底下（同一條 trace）；link = 消費者另起一條 trace、用 span.links 指回去；
    age = 只有 message.age；none = 三個都沒有。"""
    pub, got = at(-20, 30 * i), at(0, 30 * i)
    producer = [sd(f"t{i}", f"p{i}", None, PRODUCER, pub, duration_ms=30.0),
                sd(f"t{i}", f"s{i}", f"p{i}", PRODUCER, pub + timedelta(milliseconds=10), duration_ms=2.0,
                   kind="messaging", subtype="kafka", resource=f"kafka/{QUEUE}")]
    c = {"consumer": True, "queue": QUEUE, "duration_ms": 200.0}
    if how == "parent":
        return producer + [sd(f"t{i}", f"c{i}", f"s{i}", CONSUMER, got, **c)]
    if how == "link":
        return producer + [sd(f"b{i}", f"c{i}", None, CONSUMER, got, links=[(f"t{i}", f"s{i}")], **c)]
    if how == "age":
        return [sd(f"b{i}", f"c{i}", None, CONSUMER, got, age_ms=20 * 60 * 1000.0, **c)]
    return [sd(f"b{i}", f"c{i}", None, CONSUMER, got, **c)]


def world(n: int = 10, how: str = "parent") -> list[dict]:
    spans = [s for i in range(n) for s in published(i, how)]
    spans.append(sd("m0", "gw0", None, "gateway", at(-10)))  # 主撈取窗裡最早的一筆：rca_main_start 要指到它
    spans.append(sd("x0", "other0", None, "other-api", at(-25)))  # 只會被「發布時窗」那一次補撈帶回來
    return spans


class _Traces:
    """像真的 trace 來源：依時間窗（與 trace id）過濾，記下每次被問了什麼。"""

    def __init__(self, spans, linked_error: Exception | None = None):
        self.spans, self.linked_error, self.calls = spans, linked_error, []

    def fetch_spans(self, start, end, services=None, max_spans=20000):
        self.calls.append(("spans", start, end, services, max_spans))
        return [s for s in self.spans if start <= s["ts"] <= end][:max_spans], False

    def fetch_linked_spans(self, trace_ids, start, end, max_spans=20000):
        self.calls.append(("linked", tuple(trace_ids), start, end, max_spans))
        if self.linked_error:
            raise self.linked_error
        ids = set(trace_ids)
        return [s for s in self.spans if s["trace_id"] in ids and start <= s["ts"] <= end][:max_spans], False


def fetch_traces(monkeypatch, spec, **params) -> PipelineContext:
    """事件群：ship-worker 在 14:00～14:05 異常。before_minutes 預設 15 → 主撈取窗 13:45～14:05。"""
    from aiops.stages.ingestion import fetch

    monkeypatch.setattr(fetch, "get_datasource_spec", lambda _t: spec)
    monkeypatch.setattr(fetch, "is_configured", lambda _t: True)
    ctx = PipelineContext(pipeline="rca", now=at(60))
    ctx.incidents = [Incident([AnomalyEvent(CONSUMER, "latency", T, 5.0), AnomalyEvent(CONSUMER, "latency", at(5), 5.0)])]
    return OperatorRegistry.create("ingestion.incident_traces", params).execute(ctx)


def test_follow_messaging_off_fetches_exactly_what_it_did_before(monkeypatch):
    spec = _Traces(world())
    ctx = fetch_traces(monkeypatch, spec)
    assert spec.calls == [("spans", at(-15), at(5), None, 20000)]
    assert sorted(s.span_id for s in ctx.rca_spans) == sorted(["gw0", *(f"c{i}" for i in range(10))])
    assert "mq" not in ctx.result["trace_fetch"]
    assert (ctx.rca_mq, ctx.rca_mq_note, ctx.rca_main_start, ctx.errors) == ([], "", None, [])


@pytest.mark.parametrize("how", ["parent", "link"])
def test_follow_messaging_goes_back_to_when_the_messages_were_published(monkeypatch, how):
    spec = _Traces(world(how=how))
    ctx = fetch_traces(monkeypatch, spec, follow_messaging=True)
    main, linked, window = spec.calls
    assert main == ("spans", at(-15), at(5), None, 20000)
    # 第一輪：消費者指到的生產者 span 不在手上 → 依 trace id 往回最多 max_lag_minutes 撈
    assert linked == ("linked", tuple(f"t{i}" for i in range(10)), at(-120), at(5), 20000)
    (edge,) = ctx.rca_mq
    assert (edge.producer, edge.consumer, edge.queue, edge.messages, edge.linked_by, edge.failed) \
        == (PRODUCER, CONSUMER, QUEUE, 10, how, 0)
    assert edge.lag_ms_p95 == pytest.approx(20 * 60 * 1000 - 10)  # 20 分鐘，扣掉 send span 晚 10 ms 才開始
    # 發布時間 13:40:00.010～13:44:30.010（每 30 秒一則）：P5 = +13.5 秒、P95 = +256.5 秒，兩側各留 2 分鐘
    sent = timedelta(milliseconds=10)
    assert edge.pub_start == at(-22, 13.5) + sent and edge.pub_end == at(-14, 16.5) + sent
    # 第二輪：發布時窗＋前面等長的基準，早於主撈取窗的那一段，所有服務都撈（生產者等下游的時間才扣得掉）
    width = edge.pub_end - edge.pub_start
    assert window == ("spans", edge.pub_start - width, at(-15), None, 20000 - 20)  # 預算扣掉第一輪補進來的 20 筆
    assert {s.span_id for s in ctx.rca_spans} >= {"other0", "p0", "s0", "p9", "s9", "c0", "gw0"}
    assert ctx.result["trace_fetch"]["mq"] == {
        "consumers": 10, "linked_traces": 10, "linked_candidates": 10, "extra_spans": 21, "queries": 2,
        "truncated": False, "pub_fetch": [(edge.pub_start - width).isoformat(), at(-15).isoformat()],
        "edges": 1, "unresolved": 0}
    assert ctx.result["trace_fetch"]["spans"] == 11 + 21 == len(ctx.rca_spans)
    assert ctx.rca_main_start == at(-10)  # 事件前基準仍從主撈取窗算起，不被補撈的早期 span 拉走
    assert ctx.result["trace_fetch"]["start"] == at(-15).isoformat()  # 回報的主撈取窗不變
    assert (ctx.rca_mq_note, ctx.errors, ctx.rca_traces_failed) == ("", [], False)


def test_follow_messaging_samples_linked_traces_evenly_and_the_window_fetch_recovers_the_rest(monkeypatch):
    spec = _Traces(world())
    ctx = fetch_traces(monkeypatch, spec, follow_messaging=True, max_linked_traces=4)
    # 等距抽 4 條（不是最早的 4 條：那樣發布時窗會偏向一開始）
    assert spec.calls[1][1] == ("t0", "t2", "t5", "t7")
    info = ctx.result["trace_fetch"]["mq"]
    assert (info["linked_traces"], info["linked_candidates"], info["queries"]) == (4, 10, 2)
    # 抽樣只用來找出發布時窗；那一段補撈回來之後，沒被抽到的 6 則也對得到生產者
    assert (ctx.rca_mq[0].messages, info["unresolved"]) == (10, 0)


def test_follow_messaging_does_nothing_below_min_consumer_spans(monkeypatch):
    spec = _Traces(world(n=4))
    ctx = fetch_traces(monkeypatch, spec, follow_messaging=True)
    assert len(spec.calls) == 1
    assert ctx.result["trace_fetch"]["mq"] == {"consumers": 4, "linked_traces": 0, "extra_spans": 0, "queries": 0, "edges": 0}
    assert (ctx.rca_mq, ctx.rca_mq_note, ctx.rca_main_start) == ([], "", None)
    # 門檻可調：降到 4 就會跨時窗
    lowered = fetch_traces(monkeypatch, _Traces(world(n=4)), follow_messaging=True, min_consumer_spans=4)
    assert [e.messages for e in lowered.rca_mq] == [4]


def test_follow_messaging_refuses_to_look_back_past_max_lag_and_says_so(monkeypatch):
    spans = [sd(f"b{i}", f"c{i}", f"s{i}", CONSUMER, at(0, 30 * i), consumer=True, queue=QUEUE, age_ms=180 * 60 * 1000.0)
             for i in range(10)]
    spec = _Traces(spans)
    ctx = fetch_traces(monkeypatch, spec, follow_messaging=True)
    assert len(spec.calls) == 1  # 不補撈
    assert ctx.result["trace_fetch"]["mq"] == {"consumers": 10, "linked_traces": 0, "extra_spans": 0, "queries": 0, "edges": 0,
                                               "skipped": "over_max_lag", "age_p95_minutes": 180.0}
    assert ctx.rca_mq == []
    assert ctx.rca_mq_note == ("訊息延遲超過上限：消費者記的訊息年齡 P95 約 180 分鐘，超過 max_lag_minutes（120），"
                               "不往回撈生產者那段時間的 trace；請直接查佇列積壓與生產者當時的狀況")
    # 上限調高就照做
    raised = fetch_traces(monkeypatch, _Traces(spans), follow_messaging=True, max_lag_minutes=240)
    assert raised.rca_mq_note != ctx.rca_mq_note


def test_follow_messaging_says_so_when_consumers_carry_no_link_parent_or_age(monkeypatch):
    spec = _Traces(world(how="none"))
    ctx = fetch_traces(monkeypatch, spec, follow_messaging=True)
    assert len(spec.calls) == 1  # 沒有東西可以追
    assert ctx.rca_mq == []
    assert ctx.rca_mq_note == ("偵測到 messaging consumer，但沒有 span link、parent 或 message.age，無法跨時窗"
                               "（事件期間 10 筆消費者入口都對不到發布時間；往回只找 120 分鐘）")
    assert ctx.result["trace_fetch"]["mq"] == {"consumers": 10, "linked_traces": 0, "extra_spans": 0, "queries": 0, "edges": 0,
                                               "unresolved": 10}
    assert ctx.errors == []  # 不是錯誤：資料本來就沒帶


def test_follow_messaging_with_only_message_age_knows_when_but_not_who(monkeypatch):
    spec = _Traces(world(how="age"))
    ctx = fetch_traces(monkeypatch, spec, follow_messaging=True)
    assert len(spec.calls) == 1  # 不知道生產者是誰，也就沒有哪條 trace、哪個服務可以補撈
    (edge,) = ctx.rca_mq
    assert (edge.producer, edge.consumer, edge.linked_by, edge.messages) == ("", CONSUMER, "age", 10)
    assert edge.lag_ms_p95 == pytest.approx(20 * 60 * 1000)
    assert ctx.result["trace_fetch"]["mq"] == {"consumers": 10, "linked_traces": 0, "extra_spans": 0, "queries": 0,
                                               "edges": 1, "unresolved": 0}
    assert ctx.rca_main_start is None  # 沒有補撈任何東西


def test_follow_messaging_failure_keeps_the_main_spans_and_reports_it(monkeypatch):
    spec = _Traces(world(), linked_error=InfrastructureError("elasticsearch 連不上"))
    ctx = fetch_traces(monkeypatch, spec, follow_messaging=True)
    assert sorted(s.span_id for s in ctx.rca_spans) == sorted(["gw0", *(f"c{i}" for i in range(10))])  # 主撈取照用
    assert ctx.result["trace_fetch"]["mq"] == {"skipped": "elasticsearch 連不上"}
    assert ctx.result["trace_fetch"]["spans"] == 11
    assert ctx.rca_mq == [] and ctx.rca_main_start is None
    assert ctx.rca_mq_note == "訊息佇列跨時窗撈取失敗，沒有回到生產者發布時段比對：elasticsearch 連不上"
    assert [(e.entity, e.message) for e in ctx.errors] \
        == [("node:incident_traces", "訊息佇列跨時窗撈取失敗，根因只看事件時窗：elasticsearch 連不上")]
    assert ctx.rca_traces_failed is False  # trace 本身撈得到：根因算子照常用事件時窗的證據

    bug = fetch_traces(monkeypatch, _Traces(world(), linked_error=KeyError("span_id")), follow_messaging=True)
    assert bug.result["trace_fetch"]["mq"] == {"skipped": "KeyError: 'span_id'"}  # 非預期例外帶型別
    assert len(bug.rca_spans) == 11


def test_follow_messaging_on_a_source_that_cannot_fetch_by_trace_degrades_the_same_way(monkeypatch):
    class _OnlySpans(BaseDataSourceSpec):
        source_type = "onlyspans"

        def fetch_data(self, *a, **k):
            raise NotImplementedError

        def fetch_spans(self, start, end, services=None, max_spans=20000):
            return [s for s in world() if start <= s["ts"] <= end], False

        def check(self):
            return {"ok": True, "detail": ""}

    with pytest.raises(InvalidQueryError, match="onlyspans 不支援依 trace 撈 span"):
        _OnlySpans().fetch_linked_spans(["t0"], at(-120), at(5))
    ctx = fetch_traces(monkeypatch, _OnlySpans(), follow_messaging=True)
    assert len(ctx.rca_spans) == 11 and ctx.rca_mq == []
    assert ctx.result["trace_fetch"]["mq"] == {"skipped": "onlyspans 不支援依 trace 撈 span（訊息佇列跨時窗）"}
    assert "不支援依 trace 撈 span" in ctx.rca_mq_note


@pytest.mark.parametrize("params, message", [
    ({"follow_messaging": "yes"}, "follow_messaging 只能是 true／false"),
    ({"max_lag_minutes": 0}, "max_lag_minutes、max_linked_traces 必須大於 0"),
    ({"max_linked_traces": 0}, "max_lag_minutes、max_linked_traces 必須大於 0"),
    ({"min_consumer_spans": 0}, "min_consumer_spans 至少 1"),
    ({"mq_pad_minutes": -1}, "mq_pad_minutes 不能小於 0"),
])
def test_follow_messaging_params_are_checked_when_the_workflow_loads(params, message):
    with pytest.raises(ValueError, match=message):
        OperatorRegistry.create("ingestion.incident_traces", params)


# ── ElasticsearchSpec：messaging 文件與 fetch_linked_spans ──


def _txn_doc(**txn) -> dict:
    return {"@timestamp": "2026-11-03T14:00:00Z", "trace": {"id": "t1"}, "service": {"name": "Ship-Worker "},
            "transaction": {"id": "c1", "type": "messaging", "name": "Kafka RECEIVE from order-events",
                            "duration": {"us": 250000}, **txn},
            "event": {"outcome": "failure"}}


def test_es_reads_a_messaging_transaction_as_a_consumer_entry():
    doc = _txn_doc(message={"queue": {"name": QUEUE}, "age": {"ms": 1200000}})
    doc["parent"] = {"id": "s1"}
    span = es_span(doc)
    assert (span["consumer"], span["queue"], span["age_ms"], span["links"]) == (True, QUEUE, 1200000.0, ())
    assert (span["span_id"], span["parent_id"], span["service"], span["duration_ms"], span["error"], span["kind"]) \
        == ("c1", "s1", "ship-worker", 250.0, True, "")
    assert span["name"] == "Kafka RECEIVE from order-events"


def test_es_batch_consumer_transaction_carries_span_links_but_is_still_a_transaction():
    """批次消費的 transaction 文件有 span.links（所以有 span 物件）但沒有 span.id：
    不能因為有 span 物件就當成 span 文件，否則 span_id 是空的（整筆被丟）或耗時讀成 0。"""
    doc = _txn_doc()
    doc["span"] = {"links": [{"trace": {"id": "t1"}, "span": {"id": "s1"}}, {"trace": {"id": "t2"}, "span": {"id": "s2"}},
                             {"trace": {"id": "t3"}}, "garbage"]}
    span = es_span(doc)
    assert (span["span_id"], span["duration_ms"], span["consumer"]) == ("c1", 250.0, True)
    assert span["links"] == (("t1", "s1"), ("t2", "s2"))  # 缺 span.id 的、不是物件的丟掉
    assert es_links({"trace": {"id": "t9"}, "span": {"id": "s9"}}) == (("t9", "s9"),)  # 索引裡只有一筆時是單一物件
    assert es_links(None) == () and es_links("x") == ()


def test_es_spans_inside_a_messaging_transaction_and_send_spans_are_not_consumers():
    child = {"@timestamp": "2026-11-03T14:00:00Z", "trace": {"id": "t1"}, "service": {"name": "ship-worker"},
             "parent": {"id": "c1"}, "transaction": {"id": "c1", "type": "messaging"},
             "span": {"id": "d1", "type": "db", "subtype": "postgresql", "duration": {"us": 200000},
                      "destination": {"service": {"resource": "ship-db"}}}}
    span = es_span(child)
    assert (span["span_id"], span["consumer"], span["kind"], span["resource"], span["duration_ms"]) \
        == ("d1", False, "db", "ship-db", 200.0)
    send = {"@timestamp": "2026-11-03T13:40:00Z", "trace": {"id": "t1"}, "service": {"name": "ord-api"},
            "parent": {"id": "p1"}, "transaction": {"id": "p1"},
            "span": {"id": "s1", "type": "messaging", "subtype": "kafka", "duration": {"us": 2000},
                     "destination": {"service": {"resource": f"kafka/{QUEUE}"}}}}
    span = es_span(send)
    assert (span["consumer"], span["kind"], span["subtype"], span["resource"]) == (False, "messaging", "kafka", f"kafka/{QUEUE}")
    # 一般的 request transaction：不是消費者，訊息欄位都是空的
    plain = es_span({"@timestamp": "2026-11-03T14:00:00Z", "trace": {"id": "t1"}, "service": {"name": "cart-api"},
                     "transaction": {"id": "tx1", "type": "request", "duration": {"us": 25000}}})
    assert (plain["consumer"], plain["queue"], plain["age_ms"], plain["links"]) == (False, "", None, ())


@pytest.mark.parametrize("age", [True, "1200", None, {"ms": 5}])
def test_es_message_age_that_is_not_a_number_is_treated_as_missing(age):
    assert es_span(_txn_doc(message={"age": {"ms": age}}))["age_ms"] is None


def _es(pages, status=200):
    seen = []

    def handler(req):  # 像真的 ES：依請求順序回下一頁，並遵守 size
        body = json.loads(req.content)
        seen.append((req.method, req.url.path, body))
        if status != 200:
            return httpx.Response(status, json={"error": {"reason": "all shards failed"}})
        page = len(seen) - 1
        docs = (pages[page] if page < len(pages) else [])[: body["size"]]
        hits = [{"_source": d, "sort": [page, i]} for i, d in enumerate(docs)]
        return httpx.Response(200, json={"hits": {"hits": hits}})

    spec = ElasticsearchSpec("http://es", api_key="k", client=httpx.Client(transport=httpx.MockTransport(handler)))
    return spec, seen


def _send_doc(i: int) -> dict:
    return {"@timestamp": "2026-11-03T13:40:00Z", "trace": {"id": f"t{i}"}, "service": {"name": "ord-api"},
            "parent": {"id": f"p{i}"}, "transaction": {"id": f"p{i}"},
            "span": {"id": f"s{i}", "type": "messaging", "subtype": "kafka", "duration": {"us": 2000}}}


def test_es_fetch_linked_spans_queries_by_trace_id_inside_the_window_and_only_reads(monkeypatch):
    monkeypatch.delenv("AIOPS_ES_TRACE_INDEX", raising=False)
    spec, seen = _es([[_send_doc(1), _send_doc(2)]])
    spans, truncated = spec.fetch_linked_spans(["t2", "t1", "t1", ""], at(-120), at(5))
    assert [s["span_id"] for s in spans] == ["s1", "s2"] and truncated is False
    assert [(m, p) for m, p, _ in seen] == [("POST", "/traces-apm-*/_search")] * 2  # 第二次是空頁，確認撈完
    first, second = (body for _, _, body in seen)
    assert first["query"] == {"bool": {"filter": [
        {"range": {"@timestamp": {"gte": at(-120).isoformat(), "lte": at(5).isoformat()}}},
        {"terms": {"trace.id": ["t1", "t2"]}}]}}  # 去重、去空、排序；不帶服務過濾（生產者是誰撈回來才知道）
    assert first["size"] == 5000 and "search_after" not in first
    assert second["search_after"] == [0, 1]
    assert {"transaction.type", "transaction.message.queue.name", "transaction.message.age.ms", "span.links"} <= set(first["_source"])


def test_es_fetch_linked_spans_stops_at_max_spans_and_reports_truncation():
    spec, seen = _es([[_send_doc(1), _send_doc(2), _send_doc(3)]])
    spans, truncated = spec.fetch_linked_spans(["t1", "t2", "t3"], at(-120), at(5), max_spans=2)
    assert [s["span_id"] for s in spans] == ["s1", "s2"] and truncated is True
    assert len(seen) == 1 and seen[0][2]["size"] == 2


def test_es_fetch_linked_spans_batches_trace_ids_and_skips_the_query_when_there_are_none():
    spec, seen = _es([])
    assert spec.fetch_linked_spans([], at(-120), at(5)) == ([], False) and seen == []
    ids = [f"t{i:04d}" for i in range(501)]
    assert spec.fetch_linked_spans(ids, at(-120), at(5)) == ([], False)
    sizes = [len(body["query"]["bool"]["filter"][1]["terms"]["trace.id"]) for _, _, body in seen]
    assert sizes == [500, 1]  # terms 一次最多 500 個 trace id


def test_es_fetch_linked_spans_query_error_names_what_it_was_doing():
    spec, _ = _es([], status=400)
    with pytest.raises(InvalidQueryError, match="撈訊息生產者 trace失敗（HTTP 400）：all shards failed"):
        spec.fetch_linked_spans(["t1"], at(-120), at(5))


# ── alert.messaging_link ──


def _ev(entity, minutes=0.0, score=3.0):
    return AnomalyEvent(entity, "latency", at(minutes), score)


def link(events, edges, **params) -> PipelineContext:
    ctx = PipelineContext(pipeline="wf", now=at(600))
    ctx.anomalies = list(events)
    ctx.async_edges = dict(edges)
    return OperatorRegistry.create("alert.messaging_link", params).execute(ctx)


def _groups(ctx) -> list[list[str]]:
    return sorted(sorted(set(i.entities)) for i in ctx.incidents)


EDGE = {(PRODUCER, CONSUMER): 1000.0}  # 延遲 P95 1 秒


def test_messaging_link_merges_producer_then_consumer_and_tags_it():
    ctx = link([_ev(PRODUCER, 0), _ev(CONSUMER, 8), _ev("unrelated", 3)], EDGE)
    assert _groups(ctx) == [[PRODUCER, CONSUMER], ["unrelated"]]
    merged = next(i for i in ctx.incidents if len(set(i.entities)) == 2)
    assert "messaging_link" in merged.tags
    assert "messaging_link" not in next(i for i in ctx.incidents if i.entities == ["unrelated"]).tags


def test_messaging_link_has_a_direction_consumer_first_is_not_carried_by_messages():
    ctx = link([_ev(CONSUMER, 0), _ev(PRODUCER, 8)], EDGE)
    assert _groups(ctx) == [[PRODUCER], [CONSUMER]]
    # 同一分鐘開始算「生產者不晚於消費者」
    assert _groups(link([_ev(CONSUMER, 0), _ev(PRODUCER, 0)], EDGE)) == [[PRODUCER, CONSUMER]]


@pytest.mark.parametrize("lag_minutes, gap_minutes, merged", [
    (1 / 60, 10, True),     # 延遲 1 秒：窗口墊到 min_window 10 分鐘
    (1 / 60, 11, False),
    (20, 30, True),         # 延遲 20 分鐘 × lag_factor 1.5 = 30 分鐘
    (20, 31, False),
    (180, 120, True),       # 延遲 3 小時：窗口封頂在 max_window 120 分鐘
    (180, 121, False),
])
def test_messaging_link_window_follows_the_edge_lag_within_min_and_max(lag_minutes, gap_minutes, merged):
    ctx = link([_ev(PRODUCER, 0), _ev(CONSUMER, gap_minutes)], {(PRODUCER, CONSUMER): lag_minutes * 60 * 1000.0})
    assert _groups(ctx) == ([[PRODUCER, CONSUMER]] if merged else [[PRODUCER], [CONSUMER]])


def test_messaging_link_params_change_the_window():
    events = [_ev(PRODUCER, 0), _ev(CONSUMER, 25)]
    edge = {(PRODUCER, CONSUMER): 10 * 60 * 1000.0}
    assert _groups(link(events, edge)) == [[PRODUCER], [CONSUMER]]            # 10 分鐘 × 1.5 = 15 < 25
    assert _groups(link(events, edge, lag_factor=3)) == [[PRODUCER, CONSUMER]]  # 30 分鐘
    assert _groups(link(events, edge, min_window_minutes=25)) == [[PRODUCER, CONSUMER]]
    assert _groups(link(events, {(PRODUCER, CONSUMER): 3600 * 1000.0}, max_window_minutes=20)) == [[PRODUCER], [CONSUMER]]


def test_messaging_link_without_async_edges_leaves_incidents_alone():
    ctx = link([_ev(PRODUCER, 0), _ev(CONSUMER, 8)], {})
    assert _groups(ctx) == [[PRODUCER], [CONSUMER]] and ctx.incidents_built is True
    # 自己發給自己的邊不算；方向相反的邊（ship-worker -> ord-api）也不會把「生產者先、消費者後」併起來
    assert _groups(link([_ev(PRODUCER, 0), _ev(PRODUCER, 8)], {(PRODUCER, PRODUCER): 1000.0})) == [[PRODUCER], [PRODUCER]]
    assert _groups(link([_ev(PRODUCER, 0), _ev(CONSUMER, 8)], {(CONSUMER, PRODUCER): 1000.0})) == [[PRODUCER], [CONSUMER]]


@pytest.mark.parametrize("params", [{"min_window_minutes": -1}, {"min_window_minutes": 30, "max_window_minutes": 20},
                                    {"lag_factor": 0}])
def test_messaging_link_rejects_bad_params(params):
    with pytest.raises(ValueError, match="lag_factor 必須大於 0"):
        OperatorRegistry.create("alert.messaging_link", params)


def _open(entity, minutes) -> OpenIncident:
    return OpenIncident(1, "inc-open", "wf", "P2", Incident([_ev(entity, m) for m in minutes], uid="inc-open"))


def test_producer_still_firing_this_round_is_merged_and_absorbed_into_its_open_incident():
    """生產者那一群上一輪就開了、這一輪還有異常：messaging_link 先併成一群，absorb_open 再照「共同實體」吸收進同一個 uid。"""
    ctx = PipelineContext(pipeline="wf", now=at(10))
    ctx.open_incidents = [_open(PRODUCER, [-10, -5])]
    ctx.anomalies = [_ev(PRODUCER, 0), _ev(CONSUMER, 3)]
    ctx.async_edges = dict(EDGE)
    OperatorRegistry.create("alert.messaging_link").execute(ctx)
    OperatorRegistry.create("alert.absorb_open").execute(ctx)
    assert ctx.incidents == []  # 沒有另開新卡
    (opened,) = ctx.open_incidents
    assert sorted(set(opened.incident.entities)) == [PRODUCER, CONSUMER] and opened.incident.uid == "inc-open"
    assert {"messaging_link", "absorbed"} <= set(opened.incident.tags)


@pytest.mark.xfail(strict=True, reason="已知邊界（決策 D3，P0 不做）：alert.absorb_open 只認共同實體、不認非同步邊；"
                                       "生產者這一輪已經安靜、消費者才出事時會另開一張卡")
def test_dead_zone_quiet_producer_incident_does_not_absorb_the_late_consumer():
    ctx = PipelineContext(pipeline="wf", now=at(10))
    ctx.open_incidents = [_open(PRODUCER, [-10, -5])]
    ctx.anomalies = [_ev(CONSUMER, 3)]  # 生產者這一輪沒有異常
    ctx.async_edges = dict(EDGE)
    OperatorRegistry.create("alert.messaging_link").execute(ctx)
    OperatorRegistry.create("alert.absorb_open").execute(ctx)
    assert ctx.incidents == []
    assert sorted(set(ctx.open_incidents[0].incident.entities)) == [PRODUCER, CONSUMER]


# ── service_async_edges：寫入、汰除、載入 ──


def test_async_edges_accumulate_messages_overwrite_lag_and_prune_by_last_seen():
    assert AsyncEdges.record({}) == {"added": 0, "updated": 0}
    first = AsyncEdges.record({(PRODUCER, CONSUMER): (1200.0, 10), ("a", "b"): (50.0, 3)}, seen_at=NOW - timedelta(days=8))
    assert first == {"added": 2, "updated": 0}
    again = AsyncEdges.record({(PRODUCER, CONSUMER): (480000.0, 5)}, seen_at=NOW)
    assert again == {"added": 0, "updated": 1}
    rows = {(r["producer"], r["consumer"]): r for r in AsyncEdges.all()}
    edge = rows[(PRODUCER, CONSUMER)]
    # 訊息數累加；延遲是最近一次看到的值（覆蓋，不平均：積壓時要反映現在的延遲）
    assert (edge["messages"], edge["lag_ms_p95"], edge["last_seen"]) == (15, 480000.0, NOW.replace(tzinfo=None))
    assert AsyncEdges.prune(NOW - timedelta(days=7)) == 1
    assert [(r["producer"], r["consumer"]) for r in AsyncEdges.all()] == [(PRODUCER, CONSUMER)]


class _FakeTraces(BaseDataSourceSpec):
    source_type = "faketrace"
    spans: list[dict] = []

    def fetch_data(self, *a, **k):
        raise NotImplementedError

    def fetch_spans(self, start, end, services=None, max_spans=20000):
        return list(type(self).spans), False

    def check(self):
        return {"ok": True, "detail": ""}


@pytest.fixture
def fake(monkeypatch):
    spec = type("_Fake", (_FakeTraces,), {"spans": []})
    monkeypatch.setitem(sources._SPECS, "faketrace", spec)
    return spec


def _queue_spans(n: int, lag: timedelta) -> list[dict]:
    """gateway -> ord-api（同步）；ord-api 發訊息，ship-worker lag 之後消費（非同步），消費者再同步呼叫 ship-db-proxy。"""
    spans = []
    for i in range(n):
        pub = NOW - timedelta(minutes=30) + timedelta(seconds=i)
        spans += [sd(f"t{i}", f"g{i}", None, "gateway", pub), sd(f"t{i}", f"p{i}", f"g{i}", PRODUCER, pub),
                  sd(f"t{i}", f"s{i}", f"p{i}", PRODUCER, pub, kind="messaging"),
                  sd(f"t{i}", f"c{i}", f"s{i}", CONSUMER, pub + lag, consumer=True, queue=QUEUE),
                  sd(f"t{i}", f"x{i}", f"c{i}", "ship-db-proxy", pub + lag)]
    return spans


def test_topology_refresh_stores_queue_edges_apart_from_sync_calls(fake):
    fake.spans = _queue_spans(6, timedelta(minutes=3))
    out = run("admin_topology_refresh", request=TopologyRefresh(source_type="faketrace"), now=NOW)
    # 同步拓撲裡沒有 ord-api -> ship-worker：那不是呼叫，傳播方向也相反
    assert out["edges"] == [{"caller": "gateway", "callee": PRODUCER, "calls": 6},
                            {"caller": CONSUMER, "callee": "ship-db-proxy", "calls": 6}]
    assert (PRODUCER, CONSUMER) not in {(e["caller"], e["callee"]) for e in ServiceEdges.all()}
    assert (out["async_edges_seen"], out["async_edges_pruned"]) == (1, 0)
    assert out["async_edges"] == [{"producer": PRODUCER, "consumer": CONSUMER, "lag_ms_p95": 180000.0, "messages": 6}]
    (row,) = AsyncEdges.all()
    assert (row["producer"], row["consumer"], row["messages"], row["lag_ms_p95"], row["last_seen"]) \
        == (PRODUCER, CONSUMER, 6, 180000.0, NOW.replace(tzinfo=None))


def test_topology_refresh_updates_the_lag_and_prunes_async_edges_not_seen_for_n_days(fake):
    AsyncEdges.record({("old-producer", "gone-worker"): (900.0, 4)}, seen_at=NOW - timedelta(days=8))
    AsyncEdges.record({(PRODUCER, CONSUMER): (1000.0, 2)}, seen_at=NOW - timedelta(days=2))
    fake.spans = _queue_spans(4, timedelta(minutes=20))  # 積壓了：延遲從 1 秒變 20 分鐘
    out = run("admin_topology_refresh", request=TopologyRefresh(source_type="faketrace", prune_days=7), now=NOW)
    assert (out["async_edges_seen"], out["async_edges_pruned"]) == (1, 1)
    (row,) = AsyncEdges.all()
    assert (row["producer"], row["messages"], row["lag_ms_p95"]) == (PRODUCER, 6, 1200000.0)

    fake.spans = []  # 這個窗沒有任何 trace：不新增，也不因此把還在保存期內的邊刪掉
    quiet = run("admin_topology_refresh", request=TopologyRefresh(source_type="faketrace"), now=NOW + timedelta(days=1))
    assert (quiet["async_edges_seen"], quiet["async_edges_pruned"], len(AsyncEdges.all())) == (0, 0, 1)


def test_storage_hooks_load_async_edges_unless_the_workflow_pins_its_own_topology():
    AsyncEdges.record({(PRODUCER, CONSUMER): (1200.0, 10)}, seen_at=NOW)
    ctx = PipelineContext(pipeline="it_otel", now=NOW)
    StorageHooks().load_batch(ctx)
    assert ctx.async_edges == {(PRODUCER, CONSUMER): 1200.0}
    pinned = PipelineContext(pipeline="it_otel", now=NOW)
    pinned.topology_explicit = True  # workflow 自己寫了 topology（含空清單）：存的同步邊與非同步邊都不載
    StorageHooks().load_batch(pinned)
    assert pinned.async_edges == {} and pinned.topology == []


def _detect(now, pipeline="it_otel") -> list[list[str]]:
    """生產者 12 分鐘前開始異常、消費者 4 分鐘前才開始：照存起來的拓撲跑一輪內建偵測管線，回各事件群的實體。"""
    items = [ExternalAnomalyIn(external_id=f"mq{i}", entity=e, metric="latency", ts=now - timedelta(minutes=m), score=5)
             for i, (e, m) in enumerate([(PRODUCER, 12), (CONSUMER, 4)])]
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline=pipeline, source="x", anomalies=items))
    result = Pipeline(load_workflow(workflow_path(pipeline)), StorageHooks()).run(now=now, targets=[]).to_result()
    return sorted(sorted(i["entities"]) for i in result["incidents"])


def test_builtin_detection_keeps_two_cards_until_the_async_edge_has_been_learned(now):
    assert _detect(now) == [[PRODUCER], [CONSUMER]]


def test_builtin_detection_merges_producer_and_consumer_once_the_async_edge_is_stored(now):
    AsyncEdges.record({(PRODUCER, CONSUMER): (180000.0, 50)}, seen_at=now)
    assert _detect(now) == [[PRODUCER, CONSUMER]]


def test_a_stored_async_edge_never_enters_the_sync_topology_used_for_grouping_and_rca(now):
    """非同步邊只給 alert.messaging_link 用：不能混進 ctx.topology（那裡的方向是 callee 出事傳給 caller）。"""
    AsyncEdges.record({(PRODUCER, CONSUMER): (180000.0, 50)}, seen_at=now)
    ctx = PipelineContext(pipeline="it_otel", now=now)
    StorageHooks().load_batch(ctx)
    assert ctx.topology == []


# ── 內建 YAML 的接線 ──


@pytest.mark.parametrize("name", ["it_otel", "it_apm"])
def test_builtin_it_workflows_link_messaging_after_topology_and_before_scoring_and_absorb(name):
    ops = [n.operator for n in load_workflow(workflow_path(name)).nodes]
    link_at = ops.index("alert.messaging_link")
    assert ops.index("alert.topology_chain") < link_at < ops.index("alert.golden_signals") < ops.index("alert.absorb_open")


@pytest.mark.parametrize("name", ["rca_report", "rca_report_correlation"])
def test_builtin_rca_workflows_follow_messaging(name):
    node = next(n for n in load_workflow(workflow_path(name)).nodes if n.name == "fetch_traces")
    assert (node.operator, node.params["follow_messaging"], node.params["max_lag_minutes"]) \
        == ("ingestion.incident_traces", True, 120)
