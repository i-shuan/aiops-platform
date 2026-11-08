"""做法 B：RCA 時才隨需撈 trace、現場還原呼叫關係（ingestion.incident_traces + rca.build_dynamic_topology）。"""
import json
from collections import Counter
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from aiops.engine.context import PipelineContext, SpanRecord
from aiops.engine.pipeline import Pipeline, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import ExternalAnomalyIn, IngestAnomalies
from aiops.engine.topology import service_edges
from aiops.engine.types import AnomalyEvent, Incident
from aiops.interfaces.runner import run, workflow_path
from aiops.sources.elasticsearch import ElasticsearchSpec
from aiops.sources.mock import MockSpec
from aiops.storage.hooks import StorageHooks

T = datetime(2026, 11, 3, 15, 0, tzinfo=timezone.utc)


def _span(trace, sid, parent, svc):
    return SpanRecord(trace, sid, parent, svc, T, 1.0)


# ── 邊怎麼算 ──


def test_service_edges_follow_parent_ids_across_services_only():
    spans = [_span("t1", "a1", None, "cart-api"), _span("t1", "a2", "a1", "cart-api"),  # 同服務內：不算
             _span("t1", "b1", "a2", "ord-svc"),  # 下游 transaction 的 parent 是上游的 client span
             _span("t1", "c1", "b1", "bill-svc"), _span("t2", "x", "missing", "pay-api")]  # 父節點沒撈到：不算
    assert service_edges(spans) == {("cart-api", "ord-svc"): 1, ("ord-svc", "bill-svc"): 1}


def test_mock_traces_form_a_call_chain():
    spans, truncated = MockSpec().fetch_spans(T, T + timedelta(minutes=2), ["a", "b", "c"])
    assert not truncated and len({s["trace_id"] for s in spans}) == 3
    recs = [SpanRecord(**s) for s in spans]
    assert service_edges(recs) == {("a", "b"): 3, ("b", "c"): 3}
    assert MockSpec().fetch_spans(T, T + timedelta(minutes=2), ["a", "b"], max_spans=5) == (
        MockSpec().fetch_spans(T, T + timedelta(minutes=2), ["a", "b"])[0][:5], True)


# ── Elasticsearch：欄位對應、分頁、截斷、只讀 ──


def _es_docs():
    return [
        {"@timestamp": "2026-11-03T15:00:00Z", "trace": {"id": "t1"}, "service": {"name": "cart-api"},
         "transaction": {"id": "tx1", "duration": {"us": 25000}}, "event": {"outcome": "success"}},
        {"@timestamp": "2026-11-03T15:00:00Z", "trace": {"id": "t1"}, "service": {"name": "cart-api"},
         "span": {"id": "sp1", "duration": {"us": 20000}}, "parent": {"id": "tx1"}, "transaction": {"id": "tx1"}},
        {"@timestamp": "2026-11-03T15:00:01Z", "trace": {"id": "t1"}, "service": {"name": "ord-svc"},
         "transaction": {"id": "tx2", "duration": {"us": 18000}}, "parent": {"id": "sp1"}, "event": {"outcome": "failure"}},
    ]


def _es(pages):
    seen = []

    def handler(req):  # 像真的 ES：依請求順序回下一頁，並遵守 size
        body = json.loads(req.content)
        seen.append((req.method, req.url.path, body))
        page = len(seen) - 1
        docs = (pages[page] if page < len(pages) else [])[: body["size"]]
        hits = [{"_source": d, "sort": [page, i]} for i, d in enumerate(docs)]
        return httpx.Response(200, json={"hits": {"hits": hits}})

    return ElasticsearchSpec("http://es", api_key="k", client=httpx.Client(transport=httpx.MockTransport(handler))), seen


def test_es_fetch_spans_maps_apm_fields_and_pages():
    docs = _es_docs()
    spec, seen = _es([docs[:2], docs[2:]])
    spans, truncated = spec.fetch_spans(T, T + timedelta(minutes=5), ["cart-api", "ord-svc"])
    assert not truncated and [s["span_id"] for s in spans] == ["tx1", "sp1", "tx2"]
    assert spans[2]["parent_id"] == "sp1" and spans[2]["error"] is True and spans[0]["duration_ms"] == 25.0
    assert service_edges(SpanRecord(**s) for s in spans) == {("cart-api", "ord-svc"): 1}
    assert all(m == "POST" and p == "/traces-apm-*/_search" for m, p, _ in seen)  # 只讀：只打 _search
    assert {"terms": {"service.name": ["cart-api", "ord-svc"]}} in seen[0][2]["query"]["bool"]["filter"]


def test_es_fetch_spans_stops_at_max_spans(monkeypatch):
    monkeypatch.setenv("AIOPS_ES_TRACE_INDEX", "traces-otel-*")
    spec, seen = _es([_es_docs(), _es_docs()])
    spans, truncated = spec.fetch_spans(T, T, None, max_spans=2)
    assert truncated and len(spans) == 2 and seen[0][1] == "/traces-otel-*/_search" and seen[0][2]["size"] == 2


def _iso(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


class _FakeES:
    """會解讀查詢的假 ES：@timestamp 範圍、service.name／trace.id terms、random_score、track_total_hits、search_after。"""

    def __init__(self, docs):
        self.docs, self.bodies = docs, []

    @staticmethod
    def _match(doc, filters):
        for f in filters:
            if "range" in f:
                r, ts = f["range"]["@timestamp"], _iso(doc["@timestamp"])
                if not (_iso(r["gte"]) <= ts <= _iso(r["lte"])):
                    return False
            elif "service.name" in f.get("terms", {}):
                if doc["service"]["name"] not in f["terms"]["service.name"]:
                    return False
            elif "trace.id" in f.get("terms", {}):
                if doc["trace"]["id"] not in f["terms"]["trace.id"]:
                    return False
        return True

    def handler(self, req):
        body = json.loads(req.content)
        self.bodies.append(body)
        q = body["query"]
        random_seed = None
        if "function_score" in q:
            random_seed, q = q["function_score"]["random_score"]["seed"], q["function_score"]["query"]
        matched = [(i, d) for i, d in enumerate(self.docs) if self._match(d, q["bool"]["filter"])]
        if random_seed is not None:
            matched.sort(key=lambda p: hash((random_seed, p[0])) & 0xFFFFFFFF)
        start = body["search_after"][0] + 1 if "search_after" in body else 0
        page = matched[start:start + body["size"]]
        hits = [{"_source": d, "sort": [start + k]} for k, (_, d) in enumerate(page)]
        out = {"hits": {"hits": hits}}
        if "track_total_hits" in body:
            out["hits"]["total"] = {"value": min(len(matched), body["track_total_hits"]), "relation": "eq"}
        return httpx.Response(200, json=out)


def _traffic(minutes=15, traces_per_minute=200):
    """每條 trace 4 個 span：cart-api tx -> span -> ord-svc tx -> span（跨服務一次）。時間依序排好，像真的 ES。"""
    docs = []
    for m in range(minutes):
        for t in range(traces_per_minute):
            ts = (T + timedelta(minutes=m, seconds=t * 60 / traces_per_minute)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            tid = f"t{m:02d}{t:04d}"
            docs += [
                {"@timestamp": ts, "trace": {"id": tid}, "service": {"name": "cart-api"}, "transaction": {"id": tid + "a"}},
                {"@timestamp": ts, "trace": {"id": tid}, "service": {"name": "cart-api"}, "span": {"id": tid + "b"},
                 "parent": {"id": tid + "a"}, "transaction": {"id": tid + "a"}},
                {"@timestamp": ts, "trace": {"id": tid}, "service": {"name": "ord-svc"}, "transaction": {"id": tid + "c"},
                 "parent": {"id": tid + "b"}},
                {"@timestamp": ts, "trace": {"id": tid}, "service": {"name": "ord-svc"}, "span": {"id": tid + "d"},
                 "parent": {"id": tid + "c"}, "transaction": {"id": tid + "c"}},
            ]
    return docs


def _minutes_covered(spans):
    return sorted({int((s["ts"] - T).total_seconds() // 60) for s in spans})


def test_es_fetch_spans_samples_whole_traces_evenly_when_over_the_cap():
    fake = _FakeES(_traffic())  # 15 分鐘 × 200 條 × 4 = 12000 span
    spec = ElasticsearchSpec("http://es", client=httpx.Client(transport=httpx.MockTransport(fake.handler)))
    spans, truncated = spec.fetch_spans(T, T + timedelta(minutes=15), None, max_spans=2000)
    assert truncated and 1900 <= len(spans) <= 2000
    by_trace = {}
    for s in spans:
        by_trace.setdefault(s["trace_id"], []).append(s)
    assert all(len(v) == 4 for v in by_trace.values())  # 只收整條 trace，父子接得起來
    assert _minutes_covered(spans) == list(range(15))  # 整個窗口都有，不是只有開頭
    per_minute = Counter(int((s["ts"] - T).total_seconds() // 60) for s in spans)
    assert max(per_minute.values()) <= 2 * min(per_minute.values())
    edges = service_edges(SpanRecord(**s) for s in spans)
    assert edges == {("cart-api", "ord-svc"): len(by_trace)}
    assert all("random_score" not in json.dumps(b) or b["_source"] == ["trace.id"] for b in fake.bodies)
    # 固定 seed：重跑抽到一樣的 trace
    again, _ = spec.fetch_spans(T, T + timedelta(minutes=15), None, max_spans=2000)
    assert {s["trace_id"] for s in again} == set(by_trace)


def test_es_fetch_spans_head_fetch_would_have_covered_only_the_start():
    """對照組：舊做法（依時間取前 max_spans 筆）在同一份流量只涵蓋開頭 2～3 分鐘。"""
    docs = _traffic()
    head = docs[:2000]
    assert _minutes_covered([{"ts": datetime.fromisoformat(d["@timestamp"].replace("Z", "+00:00"))} for d in head]) == [0, 1, 2]


def test_es_fetch_spans_is_complete_when_under_the_cap():
    fake = _FakeES(_traffic(minutes=2, traces_per_minute=10))
    spec = ElasticsearchSpec("http://es", client=httpx.Client(transport=httpx.MockTransport(fake.handler)))
    spans, truncated = spec.fetch_spans(T, T + timedelta(minutes=2), None, max_spans=2000)
    assert not truncated and len(spans) == 80
    assert all("function_score" not in b["query"] for b in fake.bodies)  # 沒超過上限就不抽樣


# ── 算子：降級，不擋報告 ──


def _ctx(entities=("cart-api", "ord-svc")):
    ctx = PipelineContext(pipeline="rca_report", now=T)
    ctx.incidents = [Incident([AnomalyEvent(e, "lat", T, 5.0) for e in entities])]
    return ctx


def test_incident_traces_skips_quietly_when_the_source_is_not_configured(monkeypatch):
    monkeypatch.delenv("AIOPS_ES_URL", raising=False)
    ctx = OperatorRegistry.create("ingestion.incident_traces", {}).execute(_ctx())
    assert ctx.rca_spans == [] and not ctx.errors and "沒有設定" in ctx.result["trace_fetch"]["skipped"]


def test_incident_traces_records_an_error_but_does_not_raise_when_es_is_down(monkeypatch):
    monkeypatch.setenv("AIOPS_ES_URL", "http://127.0.0.1:1")  # 沒人在聽
    op = OperatorRegistry.create("ingestion.incident_traces", {})
    op.node_name = "fetch_traces"
    ctx = op.execute(_ctx())
    assert ctx.rca_spans == [] and ctx.errors[0].entity == "node:fetch_traces" and "撈 trace 失敗" in ctx.errors[0].message


def test_incident_traces_window_and_service_filter():
    op = OperatorRegistry.create("ingestion.incident_traces",
                                 {"source_type": "mock", "before_minutes": 15, "only_incident_services": True})
    ctx = op.execute(_ctx())
    info = ctx.result["trace_fetch"]
    assert info["start"] == (T - timedelta(minutes=15)).isoformat() and info["services"] == ["cart-api", "ord-svc"]
    assert len(ctx.rca_spans) == 16 * 2 * 2  # 16 分鐘 × 2 個服務 × (transaction + db span)


def test_build_topology_merge_replace_and_min_calls():
    def build(params, spans, topology=()):
        ctx = _ctx()
        ctx.rca_spans, ctx.topology = spans, list(topology)
        return OperatorRegistry.create("rca.build_dynamic_topology", params).execute(ctx)

    spans = [_span("t1", "a", None, "cart-api"), _span("t1", "b", "a", "ord-svc"),
             _span("t2", "c", None, "cart-api"), _span("t2", "d", "c", "ord-svc"),
             _span("t3", "e", None, "web"), _span("t3", "f", "e", "cart-api")]
    merged = build({}, spans, [("x", "y")])
    assert merged.topology == [("x", "y"), ("cart-api", "ord-svc"), ("web", "cart-api")]
    assert build({"mode": "replace"}, spans, [("x", "y")]).topology == [("cart-api", "ord-svc"), ("web", "cart-api")]
    assert build({"min_calls": 2}, spans).topology == [("cart-api", "ord-svc")]
    # 有撈到 span 但沒有任何跨服務呼叫：replace 也不清空既有拓撲，並留下紀錄
    lonely = build({"mode": "replace"}, [_span("t1", "a", None, "cart-api")], [("x", "y")])
    assert lonely.topology == [("x", "y")] and "dynamic_topology:no-cross-service-calls" in lonely.audit
    with pytest.raises(TypeError):
        OperatorRegistry.get("rca.build_dynamic_topology")(mode="overwrite")


# ── 端到端：沒有事先存的拓撲，靠現撈的 trace 排出根因 ──


def _incident_without_topology(now):
    items = [ExternalAnomalyIn(external_id=f"dyn{i}", entity=e, metric="latency", ts=now - timedelta(minutes=m), score=s)
             for i, (e, m, s) in enumerate([("svc-a", 1, 2), ("svc-b", 2, 3), ("svc-c", 3, 6)])]
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline="it_otel", source="x", anomalies=items))
    wf = load_workflow(workflow_path("it_otel"))
    wf.topology = [("svc-a", "svc-b"), ("svc-b", "svc-c")]  # 只用來把三個實體收斂成一件事；不會存成 RCA 用的拓撲
    result = Pipeline(wf, StorageHooks()).run(now=now, targets=[]).to_result()
    return result["incidents"][0]["id"]


def _rca(incident_id, now, source_type):
    wf = load_workflow(workflow_path("rca_report"))
    next(n for n in wf.nodes if n.name == "fetch_traces").params["source_type"] = source_type
    return Pipeline(wf, StorageHooks()).run(incident_id=incident_id, now=now).to_result()


def test_rca_ranks_root_causes_from_traces_fetched_on_demand(now, monkeypatch):
    from aiops.storage import crud
    from aiops.storage.models import IncidentRow

    monkeypatch.delenv("AIOPS_ES_URL", raising=False)
    inc_id = _incident_without_topology(now)
    # 事件快照會存收斂當下用到的邊；清掉它，模擬「平台事先不知道任何拓撲」
    crud.update(IncidentRow, inc_id, context={})

    without = _rca(inc_id, now, "elasticsearch")  # 測試環境沒有 ES：略過，跟以前一樣排不出根因
    assert "root_cause_candidates" in without["skipped"] and without["trace_fetch"]["spans"] == 0

    out = _rca(inc_id, now, "mock")  # mock 的 trace：svc-a 呼叫 svc-b、svc-b 呼叫 svc-c
    edges = {(e["caller"], e["callee"]) for e in out["dynamic_topology"]["edges"]}
    assert edges == {("svc-a", "svc-b"), ("svc-b", "svc-c")}
    assert out["root_cause"]["candidates"][0]["entity"] == "svc-c"  # 被呼叫的最底層依賴是源頭
    sections = {s["key"]: s["body"] for s in out["sections"]}
    assert sections["propagation_path"].startswith("svc-c（") and not out.get("errors")


def test_daily_detection_never_fetches_traces():
    for name in ("it_otel", "it_apm", "factory", "metrics_only"):
        ops = [n.operator for n in load_workflow(workflow_path(name)).nodes]
        assert "ingestion.incident_traces" not in ops and "rca.build_dynamic_topology" not in ops, name
