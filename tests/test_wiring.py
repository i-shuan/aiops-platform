"""三條原本斷掉的管線，從 API 一路走到結果：欄位對照、trace 與 Lot 履歷落庫後自動被用、拓撲自動推導。"""
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from aiops.datasource_specs.elasticsearch import ElasticsearchSpec

T0 = datetime(2026, 11, 8, tzinfo=timezone.utc)
WINDOW = {"start": "2026-11-08T00:00:00", "end": "2026-11-08T01:00:00", "step_seconds": 60}


# ── 1. 欄位對照：核准後偵測真的用它（資料源自己不查資料庫，由 Command 讀出傳入）──


@pytest.fixture
def es_bodies(monkeypatch):
    """把 ES 換成模擬伺服器，記下平台送出的查詢；回傳一條第 30 分鐘有尖峰的 p95 序列。"""
    seen: list[dict] = []

    def handler(req):
        seen.append(json.loads(req.content))
        buckets = [{"key": int((T0 + timedelta(minutes=i)).timestamp() * 1000), "doc_count": 1,
                    "v": {"values": {"95.0": 900.0 if i == 30 else 100.0 + i % 3}}} for i in range(60)]
        return httpx.Response(200, json={"aggregations": {"series": {"buckets": buckets}}})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(ElasticsearchSpec, "from_env", classmethod(lambda cls: cls("http://es", client=client)))
    return seen


def _approve(client, fields):
    mid = client.post("/api/v1/ingestion/mappings", json={"source_type": "elasticsearch", "fields": fields,
                                                          "submitter": "amy"}).json()["result"]["id"]
    assert client.post(f"/api/v1/ingestion/mappings/{mid}/approve", json={"approver": "bob"}).status_code == 200


ES_QUERY = json.dumps({"index": "traces-apm-*", "agg": "p95", "entity": "cart"})


def _detect_es(client):
    return client.post("/api/v1/detection/run", json={"metric": {"source_type": "elasticsearch", "query": ES_QUERY},
                                                      "time_range": WINDOW})


def test_detection_uses_approved_field_mapping(client, es_bodies):
    _approve(client, {"@timestamp": "ts", "transaction.duration.us": "value", "service.name": "entity"})
    r = _detect_es(client)
    assert r.status_code == 200, r.text
    assert r.json()["result"]["is_anomaly"][30]

    body = es_bodies[-1]
    hist = body["aggs"]["series"]
    assert hist["date_histogram"]["field"] == "@timestamp"
    assert hist["aggs"]["v"]["percentiles"]["field"] == "transaction.duration.us"
    assert {"term": {"service.name": "cart"}} in body["query"]["bool"]["filter"]


def test_without_approved_mapping_the_short_query_is_rejected(client, es_bodies):
    assert _detect_es(client).status_code == 400  # 沒有對照就不知道 entity、value 對到哪個欄位
    assert es_bodies == []  # 根本沒送出去


def test_newly_approved_mapping_replaces_the_old_one(client, es_bodies):
    _approve(client, {"@timestamp": "ts", "transaction.duration.us": "value", "service.name": "entity"})
    _approve(client, {"event.ingested": "ts", "span.duration.us": "value", "labels.svc": "entity"})
    _detect_es(client)
    body = es_bodies[-1]
    assert body["aggs"]["series"]["date_histogram"]["field"] == "event.ingested"
    assert {"term": {"labels.svc": "cart"}} in body["query"]["bool"]["filter"]


# ── 2. trace：合成後落庫，偵測用 source_type=trace 直接讀；服務拓撲自動推導 ──


def _spans(minutes=60, slow_minute=30):
    """frontend 每分鐘呼叫一次 cart；cart 在 slow_minute 那分鐘變慢。"""
    spans = []
    for i in range(minutes):
        ts = (T0 + timedelta(minutes=i)).isoformat()
        spans.append({"trace_id": f"t{i}", "span_id": f"f{i}", "service": "frontend", "start": ts, "duration_ms": 80})
        spans.append({"trace_id": f"t{i}", "span_id": f"c{i}", "parent_id": f"f{i}", "service": "cart", "start": ts,
                      "duration_ms": 2000 if i == slow_minute else 50 + i % 3, "error": i == slow_minute})
    return spans


@pytest.fixture
def traces(client):
    r = client.post("/api/v1/ingestion/traces/merge", json={"spans": _spans()})
    assert r.status_code == 200, r.text
    return r.json()["result"]


def test_trace_merge_derives_service_call_edges(traces):
    assert traces["edges"] == [{"caller": "frontend", "callee": "cart", "calls": 60}]
    assert len(traces["series"]) == 120 and traces["persisted"] is True


def test_detection_reads_trace_series_directly(client, traces):
    body = {"metric": {"source_type": "trace", "query": "cart/p95_ms"}, "time_range": WINDOW}
    r = client.post("/api/v1/detection/run", json=body)
    assert r.status_code == 200, r.text
    flagged = [i for i, f in enumerate(r.json()["result"]["is_anomaly"]) if f]
    assert flagged == [30]


@pytest.mark.parametrize("query, status", [("cart/latency", 400), ("cart", 400), ("nobody/p95_ms", 404)])
def test_bad_trace_queries(client, traces, query, status):
    body = {"metric": {"source_type": "trace", "query": query}, "time_range": WINDOW}
    assert client.post("/api/v1/detection/run", json=body).status_code == status


def test_scheduled_job_can_use_trace_source(client, traces):
    from aiops.tasks.config import Job
    from aiops.tasks.pipeline import run_job

    job = Job(name="trace-job", preset="it_otel", lookback_minutes=60, train_lookback_minutes=60,
              targets=[{"source_type": "trace", "query": "cart/p95_ms", "entity": "cart"}])
    r = run_job(job, now=datetime(2026, 11, 8, 1, 0))
    assert r["persisted_anomalies"] >= 1 and r["notified_incidents"] >= 1


# ── 3. 拓撲自動帶入 alerts 與 rca ──


EVENTS = [
    {"entity": "frontend", "metric": "latency", "ts": "2026-11-08T00:31:00", "score": 3},
    {"entity": "cart", "metric": "latency", "ts": "2026-11-08T00:30:00", "score": 6},
]


def _process(client, preset, events, context=None):
    body = {"preset": preset, "events": events, **({"context": context} if context is not None else {})}
    r = client.post("/api/v1/alerts/process", json=body)
    assert r.status_code == 200, r.text
    return r.json()["result"]


def test_alerts_use_derived_topology_without_context(client, traces):
    incidents = _process(client, "it_otel", EVENTS)
    assert [i["entities"] for i in incidents] == [["cart", "frontend"]]
    assert "topology_chain" in incidents[0]["tags"]


def test_explicit_context_wins_over_derived_topology(client, traces):
    incidents = _process(client, "it_otel", EVENTS, context={"topology": []})
    assert sorted(i["entities"] for i in incidents) == [["cart"], ["frontend"]]


def test_rca_gets_topology_and_trace_evidence_automatically(client, traces):
    inc = _process(client, "it_otel", EVENTS)[0]
    r = client.post("/api/v1/rca/generate", json={"incident_id": inc["id"], "preset": "it_otel"}).json()["result"]
    sections = {s["key"]: s["body"] for s in r["sections"]}
    assert sections["root_cause_candidates"].startswith("1. cart")  # 被呼叫的依賴是源頭
    assert sections["propagation_path"].startswith("cart（00:30:00） → frontend（00:31:00）")
    assert "cart：" in sections["trace_evidence"] and "錯誤率最高 100%" in sections["trace_evidence"]


# ── 4. Lot 履歷：落庫後工廠收斂與根因自動用 ──


LOT_STEPS = [
    {"lot_id": f"L{n}", "station": st, "equipment": eq,
     "start": (T0 + timedelta(minutes=10 * k)).isoformat(), "end": (T0 + timedelta(minutes=10 * k + 8)).isoformat()}
    for n in range(3) for k, (st, eq) in enumerate((("litho", "LITHO-02"), ("etch", "ETCH-01")))
]
FAB_EVENTS = [
    {"entity": "ETCH-01", "metric": "rf_power", "ts": "2026-11-08T00:40:00", "score": 5},
    {"entity": "LITHO-02", "metric": "focus", "ts": "2026-11-08T00:10:00", "score": 4},
]


def test_factory_uses_stored_lot_paths(client):
    client.post("/api/v1/ingestion/lot-paths/merge", json={"steps": LOT_STEPS})
    client.post("/api/v1/ingestion/lot-paths/merge", json={"steps": LOT_STEPS})  # 重送不會重複計算流向
    incidents = _process(client, "factory", FAB_EVENTS)
    assert [i["entities"] for i in incidents] == [["ETCH-01", "LITHO-02"]]
    assert "lot_path_chain" in incidents[0]["tags"]

    r = client.post("/api/v1/rca/generate", json={"incident_id": incidents[0]["id"], "preset": "factory"}).json()
    roots = next(s["body"] for s in r["result"]["sections"] if s["key"] == "root_cause_candidates")
    assert roots.startswith("1. LITHO-02")  # 上游機台是源頭（方向跟 IT 相反）


def test_lot_flow_counts_distinct_lots(client):
    from aiops.domains.alerts.dao import LotPathReadDAO
    from aiops.plugins.base import lot_edges_from_paths

    client.post("/api/v1/ingestion/lot-paths/merge", json={"steps": LOT_STEPS})
    client.post("/api/v1/ingestion/lot-paths/merge", json={"steps": LOT_STEPS})
    assert lot_edges_from_paths(LotPathReadDAO.paths()) == [{"from": "LITHO-02", "to": "ETCH-01", "lots": 3}]
