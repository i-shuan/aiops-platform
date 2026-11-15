"""宣告的關係圖：沒有 trace 的環境靠人宣告依賴關係（Workflow 的 graph、admin.graph_import）。

每條邊必寫 kind，傳播方向由型別決定（engine/graph.KINDS），寫的人不用自己翻方向：
  call A -> B（A 依賴 B）與 supply B -> A（B 供應 A）是同一個關係，收斂、抑制、根因的結果一樣。
實體名稱都是中性的（chiller、press…只是例子）：程式裡沒有任何設備名或行業規則。
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from aiops.engine.context import PipelineContext
from aiops.engine.errors import BadRequest, WorkflowError
from aiops.engine.graph import KINDS, Edge, EdgeKind, ServiceGraph, contradictions
from aiops.engine.pipeline import GraphEdgeSpec, Pipeline, Workflow, load_workflow
from aiops.engine.requests import ExternalAnomalyIn, GraphImport, IngestAnomalies
from aiops.interfaces.runner import run, workflow_path
from aiops.storage import models
from aiops.storage.hooks import StorageHooks
from aiops.storage.repository import DeclaredEdges, Incidents, ServiceEdges

NOW = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
MIGRATION = Path(__file__).resolve().parents[1] / "migrations" / "postgres" / "0012_declared_edges.sql"


def E(a, b, kind, weight=1.0):
    return {"from": a, "to": b, "kind": kind, "weight": weight}


def _import(edges, mode="replace", environment=""):
    return run("admin_graph_import", request=GraphImport(edges=edges, mode=mode, environment=environment, source="test"))


# ── 方向由型別決定 ──

def test_every_kind_has_a_direction_and_declared_edges_follow_it():
    g = ServiceGraph([Edge("app", "db", EdgeKind.CALL), Edge("chiller", "press", EdgeKind.SUPPLY),
                      Edge("etch", "litho", EdgeKind.FLOW), Edge("producer", "consumer", EdgeKind.ASYNC)])
    assert g.propagation() == {("db", "app"), ("chiller", "press"), ("etch", "litho")}   # 先出事的 -> 被影響的
    assert g.dependencies() == {("app", "db"), ("press", "chiller")}                      # 依賴別人的 -> 被依賴的
    assert set(KINDS) == set(EdgeKind)                                                     # 新增型別一定要在表裡填方向


def test_call_and_supply_are_the_same_relation_written_from_either_end():
    as_call = ServiceGraph([Edge("press", "chiller", EdgeKind.CALL)])
    as_supply = ServiceGraph([Edge("chiller", "press", EdgeKind.SUPPLY)])
    assert as_call.propagation() == as_supply.propagation() == {("chiller", "press")}
    assert as_call.dependencies() == as_supply.dependencies()
    assert as_call.dependency_adjacent() == as_supply.dependency_adjacent()


def test_graphs_with_only_call_edges_answer_the_new_queries_exactly_like_the_old_ones():
    g = ServiceGraph.from_parts([("a", "b"), ("b", "c"), ("c", "c")])
    assert g.dependencies() == g.pairs(EdgeKind.CALL) and g.dependency_adjacent() == g.adjacent(EdgeKind.CALL)


# ── 載入期防呆 ──

def _wf(graph) -> dict:
    return {"name": "plant", "nodes": [{"name": "group", "operator": "alert.same_entity"}], "graph": graph}


def test_kind_is_mandatory_and_closed():
    with pytest.raises(ValueError, match="kind"):
        Workflow.model_validate(_wf([{"from": "a", "to": "b"}]))                 # 沒寫 kind：不猜
    with pytest.raises(ValueError, match="kind"):
        Workflow.model_validate(_wf([E("a", "b", "depends_on")]))                # 不認得的型別
    with pytest.raises(ValueError):
        Workflow.model_validate(_wf([{"from": "a", "to": "b", "kind": "call", "direction": "up"}]))  # 多的欄位


def test_contradicting_directions_and_self_loops_fail_at_load_time(tmp_path):
    with pytest.raises(ValueError, match="方向互相矛盾"):
        Workflow.model_validate(_wf([E("a", "b", "call"), E("a", "b", "supply")]))   # b 拖垮 a 又 a 拖垮 b
    with pytest.raises(ValueError, match="自己指自己"):
        Workflow.model_validate(_wf([E("a", "a", "supply")]))
    path = tmp_path / "bad.yaml"
    path.write_text("name: plant\nnodes: [{name: group, operator: alert.same_entity}]\n"
                    "graph: [{from: a, to: b, kind: call}, {from: a, to: b, kind: supply}]\n", encoding="utf-8")
    with pytest.raises(WorkflowError, match="方向互相矛盾"):
        load_workflow(path)
    ok = Workflow.model_validate(_wf([E("a", "b", "call"), E("b", "a", "supply"), E("b", "a", "call")]))
    assert len(ok.graph) == 3   # 同一個關係寫兩次、同型別的互相依賴（真的環）都不是矛盾


def test_contradictions_ignore_kinds_that_do_not_propagate():
    assert contradictions([Edge("a", "b", EdgeKind.CALL), Edge("a", "b", EdgeKind.ASYNC)]) == []


# ── 匯入（admin.graph_import）──

def test_import_replace_and_merge_only_touch_declared_edges_of_their_environment():
    ServiceEdges.add_calls({("web", "api"): 3}, seen_at=NOW)                     # trace 觀測到的：匯入不能動它
    out = _import([E("chiller", "press-1", "supply"), E("chiller", "press-2", "supply")])
    assert (out["written"], out["removed"], out["by_kind"]) == (2, 0, {"supply": 2})
    _import([E("ups", "rack", "supply")], environment="plant-b")
    out = _import([E("app", "db", "call")], mode="merge")
    assert out["declared_total"] == 3 and out["by_kind"] == {"call": 1, "supply": 2}
    out = _import([E("chiller", "press-1", "supply")])                            # replace：整份換掉
    assert (out["removed"], out["declared_total"]) == (3, 1)
    assert [(r["source"], r["target"]) for r in DeclaredEdges.all("plant-b")] == [("ups", "rack")]
    assert [(r["caller"], r["callee"]) for r in ServiceEdges.all()] == [("web", "api")]
    assert _import([], mode="replace")["declared_total"] == 0                     # 空清單 + replace = 清空


def test_import_rejects_the_whole_batch_when_the_result_would_contradict_itself():
    _import([E("press", "chiller", "call")])
    with pytest.raises(BadRequest, match="方向互相矛盾"):
        _import([E("press", "chiller", "supply"), E("ups", "rack", "supply")], mode="merge")   # 跟庫裡既有的矛盾
    assert [(r["source"], r["target"], r["kind"]) for r in DeclaredEdges.all()] == [("press", "chiller", "call")]  # 什麼都沒寫


def test_import_reports_declarations_that_run_against_observed_calls_without_blocking():
    ServiceEdges.add_calls({("api", "web"): 9}, seen_at=NOW)                     # trace 看到的是 api 呼叫 web
    out = _import([E("web", "api", "call")])                                      # 人宣告成 web 呼叫 api
    assert out["conflicts_with_observed"] == [{"from": "web", "to": "api", "kind": "call"}] and out["written"] == 1


def test_api_import_requires_kind(client):
    from tests.conftest import AUTH

    bad = client.post("/api/v1/ingestion/graph/import", headers=AUTH, json={"mode": "replace", "edges": [{"from": "a", "to": "b"}]})
    assert bad.status_code == 422
    ok = client.post("/api/v1/ingestion/graph/import", headers=AUTH, json={"mode": "replace", "edges": [E("a", "b", "supply")]})
    assert ok.status_code == 200 and ok.json()["result"]["by_kind"] == {"supply": 1}


def test_migration_matches_the_model():
    sql = MIGRATION.read_text(encoding="utf-8")
    (uq,) = [c for c in models.DeclaredEdgeRow.__table__.constraints if c.__class__.__name__ == "UniqueConstraint"]
    cols = ", ".join(c.name for c in uq.columns)
    assert re.search(rf"ADD CONSTRAINT {uq.name} UNIQUE \({cols}\)", sql)
    for col in models.DeclaredEdgeRow.__table__.columns:
        assert re.search(rf"^\s+{col.name}\s", sql, re.M), col.name


# ── 端到端：沒有 trace，收斂、抑制、根因都靠宣告的圖 ──

def _detect(name: str, anomalies: dict[str, int], graph=()) -> Workflow:
    """實體 -> 幾分鐘前開始異常。照內建 it_otel 的批次節點跑一輪（targets 是空的：異常由外部匯入，沒有任何 trace）。"""
    items = [ExternalAnomalyIn(external_id=f"{name}-{e}", entity=e, metric="temperature", ts=NOW - timedelta(minutes=m), score=5)
             for e, m in anomalies.items()]
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline=name, source="x", anomalies=items))
    wf = load_workflow(workflow_path("it_otel")).model_copy(update={"name": name, "graph": [GraphEdgeSpec(**e) for e in graph]})
    Pipeline(wf, StorageHooks()).run(now=NOW, targets=[])
    return wf


def _cards(name: str) -> list[list[str]]:
    return sorted(sorted(r["incident"]["entities"]) for r in Incidents.list_open(name))


def _root(name: str, entity: str) -> list[str]:
    (row,) = [r for r in Incidents.list_open(name) if entity in r["incident"]["entities"]]
    result = run("rca_report", incident_id=row["id"])
    return [c["entity"] for c in result["root_cause"]["candidates"]]


SICK = {"chiller": 6, "press-1": 4, "press-2": 3, "oven": 4}   # chiller 先出事，兩台 press 跟著；oven 跟它們無關


def test_without_any_graph_nothing_is_grouped_and_rca_cannot_tell_upstream_from_downstream():
    _detect("plant", SICK)
    assert _cards("plant") == [["chiller"], ["oven"], ["press-1"], ["press-2"]]
    assert _root("plant", "press-1") == []    # 沒有圖：不指認


@pytest.mark.parametrize("edges", [
    [E("chiller", "press-1", "supply"), E("chiller", "press-2", "supply")],     # 照「誰供應誰」寫
    [E("press-1", "chiller", "call"), E("press-2", "chiller", "call")],         # 照「誰依賴誰」寫
    [E("chiller", "press-1", "supply"), E("press-2", "chiller", "call")],       # 混著寫
])
def test_imported_graph_groups_the_cascade_and_names_the_supplier_whichever_way_it_was_declared(edges):
    _import(edges)
    _detect("plant", SICK)
    assert _cards("plant") == [["chiller", "press-1", "press-2"], ["oven"]]
    assert _root("plant", "press-1") == ["chiller"]


def test_rca_from_a_declared_graph_is_labelled_as_topology_inference_not_confirmed():
    """合併後的接合點：沒有 trace、只靠宣告的圖給出的根因，結論等級必須是 topology_inference（拓撲推測），
    不准看起來像有證據的 confirmed。"""
    _import([E("chiller", "press-1", "supply"), E("chiller", "press-2", "supply")])
    _detect("plant", SICK)
    (row,) = [r for r in Incidents.list_open("plant") if "press-1" in r["incident"]["entities"]]
    root = run("rca_report", incident_id=row["id"])["root_cause"]
    assert [c["entity"] for c in root["candidates"]] == ["chiller"]
    assert root["conclusion_type"] == "topology_inference"


def test_workflow_declared_graph_works_without_any_import():
    _detect("plant", SICK, graph=[E("chiller", "press-1", "supply"), E("chiller", "press-2", "supply")])
    assert _cards("plant") == [["chiller", "press-1", "press-2"], ["oven"]]
    assert _root("plant", "press-2") == ["chiller"]


def test_a_wrongly_declared_kind_points_the_other_way_which_is_why_kind_is_mandatory():
    """對照：同樣的兩個端點，型別寫成 call（chiller 依賴 press）根因就變成 press——方向完全由型別決定，沒有別的地方會翻。"""
    _import([E("chiller", "press-1", "call"), E("chiller", "press-2", "call")])
    _detect("plant", SICK)
    assert sorted(_root("plant", "chiller")) == ["press-1", "press-2"]


def test_rca_uses_the_graph_as_declared_when_the_incident_was_created():
    _import([E("chiller", "press-1", "supply"), E("chiller", "press-2", "supply")])
    _detect("plant", SICK)
    _import([], mode="replace")                                  # 之後 CMDB 清掉了
    assert _root("plant", "press-1") == ["chiller"]             # 舊事件群的報告照建立當下的圖


def test_imported_graph_is_scoped_to_the_pipelines_environment():
    _import([E("chiller", "press-1", "supply")], environment="plant-a")
    ctx = PipelineContext(pipeline="x", now=NOW, environment="plant-b")
    StorageHooks().load_batch(ctx)
    assert not ctx.graph
    ctx = PipelineContext(pipeline="x", now=NOW, environment="plant-a")
    StorageHooks().load_batch(ctx)
    assert ctx.graph.propagation() == {("chiller", "press-1")}
