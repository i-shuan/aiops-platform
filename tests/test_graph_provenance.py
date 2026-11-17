"""關係圖的出處（engine/graph.Provenance）：每條邊帶「哪裡來的、最後什麼時候觀測到」，RCA 報告的「關係圖依據」段落照實寫出來。

出處只用來交代，不參與判斷：兩條邊相不相等、傳播方向、收斂結果都不看它。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from aiops.engine.context import LotEdge, PipelineContext, SpanRecord
from aiops.engine.graph import (ORIGIN_DECLARED, ORIGIN_INCIDENT_TRACE, ORIGIN_LOT, ORIGIN_TRACE, ORIGIN_WORKFLOW, UNKNOWN, Edge,
                                EdgeKind, Provenance, ServiceGraph)
from aiops.engine.pipeline import GraphEdgeSpec, Pipeline, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import ExternalAnomalyIn, GraphImport, IngestAnomalies
from aiops.engine.types import AnomalyEvent, Incident
from aiops.interfaces.runner import run, workflow_path
from aiops.storage.hooks import StorageHooks
from aiops.storage.repository import Incidents, ServiceEdges

NOW = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
SEEN = NOW - timedelta(hours=3, minutes=5)


def _section(result: dict, key: str) -> str:
    return next(s["body"] for s in result["sections"] if s["key"] == key)


# ── 圖本身 ──

def test_provenance_never_changes_what_an_edge_is():
    """出處不算進相等與雜湊：帶不帶出處都是同一條邊，傳播圖、依賴圖的答案一模一樣。"""
    bare = Edge("a", "b", EdgeKind.CALL)
    known = Edge("a", "b", EdgeKind.CALL, provenance=Provenance(ORIGIN_TRACE, "累計 3 次呼叫", SEEN))
    assert bare == known and hash(bare) == hash(known) and bare.provenance == UNKNOWN
    meta = {("call", "a", "b"): known.provenance, ("flow", "m1", "m2"): Provenance(ORIGIN_LOT)}
    plain = ServiceGraph.from_parts([("a", "b")], [LotEdge("m1", "m2", 4)], {("p", "c"): 80.0})
    tagged = ServiceGraph.from_parts([("a", "b")], [LotEdge("m1", "m2", 4)], {("p", "c"): 80.0}, provenance=meta)
    assert plain.propagation() == tagged.propagation() and plain.dependencies() == tagged.dependencies()
    assert plain.edges() == tagged.edges()
    assert [e.provenance.origin for e in tagged.edges()] == [ORIGIN_TRACE, ORIGIN_LOT, ""]  # 沒記的就是出處不明，不猜


def test_between_lists_every_origin_of_an_edge_and_as_of_is_the_newest_time_per_origin():
    declared = Edge("a", "b", EdgeKind.CALL, provenance=Provenance(ORIGIN_DECLARED, "cmdb", NOW - timedelta(days=30)))
    graph = ServiceGraph.from_parts(
        [("a", "b"), ("b", "c"), ("x", "x")], declared=[declared],
        provenance={("call", "a", "b"): Provenance(ORIGIN_TRACE, last_seen=SEEN),
                    ("call", "b", "c"): Provenance(ORIGIN_TRACE, last_seen=SEEN - timedelta(hours=1))})
    assert [(e.pair, e.provenance.origin) for e in graph.between({"a", "b", "x"})] == [(("a", "b"), ORIGIN_TRACE),
                                                                                    (("a", "b"), ORIGIN_DECLARED)]
    assert graph.as_of() == {ORIGIN_TRACE: SEEN, ORIGIN_DECLARED: NOW - timedelta(days=30)}
    assert ServiceGraph.from_parts([("a", "b")]).as_of() == {}


# ── 載入：資料表本來就有的來源與時間，組圖時不再丟掉 ──

def test_edges_loaded_from_the_tables_keep_their_origin_and_time():
    ServiceEdges.add_calls({("web", "db"): 7}, seen_at=SEEN.replace(tzinfo=None))
    run("admin_graph_import", request=GraphImport(edges=[{"from": "db", "to": "web", "kind": "supply", "weight": 1}],
                                                  mode="replace", source="cmdb-export"))
    ctx = PipelineContext(pipeline="p", now=NOW)
    StorageHooks().load_batch(ctx)
    by_kind = {e.kind: e.provenance for e in ctx.graph.between({"web", "db"})}
    assert by_kind[EdgeKind.CALL] == Provenance(ORIGIN_TRACE, "累計 7 次呼叫", SEEN)
    declared = by_kind[EdgeKind.SUPPLY]
    assert (declared.origin, declared.detail) == (ORIGIN_DECLARED, "cmdb-export") and declared.last_seen is not None


def test_edges_written_in_the_workflow_say_so():
    wf = load_workflow(workflow_path("it_otel")).model_copy(update={
        "name": "wf-edges", "topology": [("web", "db")], "graph": [GraphEdgeSpec(**{"from": "ups", "to": "db", "kind": "supply"})]})
    ctx = Pipeline(wf, StorageHooks()).run(now=NOW, targets=[])
    assert {e.pair: e.provenance.origin for e in ctx.graph.edges()} == {("web", "db"): ORIGIN_WORKFLOW, ("ups", "db"): ORIGIN_WORKFLOW}


# ── RCA 報告 ──

def _card(name: str, anomalies: dict[str, int]) -> int:
    items = [ExternalAnomalyIn(external_id=f"{name}-{e}", entity=e, metric="latency", ts=NOW - timedelta(minutes=m), score=5)
             for e, m in anomalies.items()]
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline=name, source="x", anomalies=items))
    wf = load_workflow(workflow_path("it_otel")).model_copy(update={"name": name})
    Pipeline(wf, StorageHooks()).run(now=NOW, targets=[])
    (row,) = Incidents.list_open(name)
    return row["id"]


def test_a_topology_inference_report_says_where_each_edge_came_from_and_how_old_it_is():
    """沒有 trace、靠關係圖推測的報告：要交代這條「web 依賴 db」是人宣告的、還是平台多久前從 trace 看到的。"""
    ServiceEdges.add_calls({("web", "db"): 7}, seen_at=(NOW - timedelta(hours=3, minutes=5)).replace(tzinfo=None))
    run("admin_graph_import", request=GraphImport(edges=[{"from": "web", "to": "db", "kind": "call", "weight": 1}],
                                                  mode="replace", source="team-a 宣告"))
    result = run("rca_report", incident_id=_card("prov", {"db": 6, "web": 4}))
    assert result["root_cause"]["conclusion_type"] == "topology_inference"
    body = _section(result, "graph_basis")
    assert "web 呼叫 db（call）" in body
    assert "平台從 trace 沉澱（累計 7 次呼叫；最後觀測 2026-11-12 08:55 UTC，事件開始前 2 小時 59 分）" in body
    assert "人工宣告（匯入）（team-a 宣告；最後更新" in body
    assert "圖資時間（整張圖每種來源最新的一筆）：" in body
    basis = result["graph_basis"]
    assert [(e["source"], e["target"], e["kind"]) for e in basis["edges"]] == [("web", "db", "call")]
    assert [p["origin"] for p in basis["edges"][0]["provenance"]] == [ORIGIN_TRACE, ORIGIN_DECLARED]
    assert set(basis["as_of"]) == {ORIGIN_TRACE, ORIGIN_DECLARED}


def test_the_report_keeps_the_origin_as_it_was_when_the_card_was_created():
    """可重現：建卡之後拓撲又被刷新（last_seen 往後推），舊事故卡的報告仍然寫建卡當下的時間。"""
    ServiceEdges.add_calls({("web", "db"): 7}, seen_at=SEEN.replace(tzinfo=None))
    incident_id = _card("frozen", {"db": 6, "web": 4})
    ServiceEdges.add_calls({("web", "db"): 100}, seen_at=(NOW + timedelta(days=2)).replace(tzinfo=None))
    body = _section(run("rca_report", incident_id=incident_id), "graph_basis")
    assert "累計 7 次呼叫；最後觀測 2026-11-12 08:55 UTC" in body and "累計 107" not in body


def test_cards_created_before_provenance_was_recorded_say_the_origin_is_unknown():
    """舊事件群的快照沒有記出處：寫「來源未記錄」，不拿資料表現在的時間充數。"""
    ServiceEdges.add_calls({("web", "db"): 7}, seen_at=SEEN.replace(tzinfo=None))
    incident_id = _card("legacy", {"db": 6, "web": 4})
    row = Incidents.get(incident_id)
    old = {k: v for k, v in row["context"].items() if k != "provenance"}
    from aiops.storage import crud
    from aiops.storage.models import IncidentRow
    crud.update(IncidentRow, incident_id, context=old)
    body = _section(run("rca_report", incident_id=incident_id), "graph_basis")
    assert "來源未記錄" in body and "最後觀測" not in body


def test_edges_seen_in_this_incidents_own_spans_are_marked_as_observed_now():
    """rca.build_dynamic_topology 從這次撈到的 span 看到的邊：出處是「這次事件的 trace」，時間是那條邊最後一筆 span。"""
    t = NOW - timedelta(minutes=5)
    ctx = PipelineContext(pipeline="rca", now=NOW)
    ctx.incidents = [Incident([AnomalyEvent("web", "latency", t, 5.0), AnomalyEvent("db", "latency", t, 5.0)])]
    ctx.topology = [("web", "db")]
    ctx.edge_provenance = {("call", "web", "db"): Provenance(ORIGIN_TRACE, "累計 7 次呼叫", SEEN)}
    ctx.rca_spans = [SpanRecord("t1", "s1", None, "web", t, 20.0, False), SpanRecord("t1", "s2", "s1", "db", t, 10.0, False),
                     SpanRecord("t2", "s3", None, "web", t + timedelta(minutes=1), 20.0, False),
                     SpanRecord("t2", "s4", "s3", "db", t + timedelta(minutes=1), 10.0, False)]
    OperatorRegistry.create("rca.build_dynamic_topology", {}).execute(ctx)
    (edge,) = ctx.graph.between({"web", "db"})
    assert edge.provenance == Provenance(ORIGIN_INCIDENT_TRACE, "2 次呼叫", t + timedelta(minutes=1))
    OperatorRegistry.create("rca.section.graph_basis", {}).execute(ctx)
    assert "這次事件的 trace 直接觀測到（2 次呼叫；最後一筆 2026-11-12 11:56 UTC，事件開始後 1 分）" in ctx.rca_sections[0].body


def test_no_edge_between_the_incidents_entities_means_no_section():
    ctx = PipelineContext(pipeline="rca", now=NOW)
    ctx.incidents = [Incident([AnomalyEvent("web", "latency", NOW, 5.0)])]
    ctx.topology = [("web", "db")]  # db 不在事件群裡：這條邊這份報告沒用到
    OperatorRegistry.create("rca.section.graph_basis", {}).execute(ctx)
    assert ctx.rca_sections == [] and ctx.rca_skipped == ["graph_basis"]
