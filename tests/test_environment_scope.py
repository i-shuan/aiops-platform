"""環境區隔：Workflow.environment、關係圖三張表（service_edges／service_async_edges／lot_paths）分環境。

兩條不同 environment 的管線監控同名服務、各自有不同的呼叫關係：拓撲、收斂、RCA 載入的圖互不干擾；
environment 留空（''）時跟加這個欄位之前完全一樣（既有測試全部沒改照過）。
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import aiops.sources as sources
from aiops.engine.context import LotEdge, PipelineContext
from aiops.engine.errors import WorkflowError
from aiops.engine.graph import EdgeKind
from aiops.engine.pipeline import Pipeline, TargetSpec, Workflow, environment_in_entity, load_workflow
from aiops.engine.requests import (ExternalAnomalyIn, IngestAnomalies, LotPathsMerge, LotStepIn, SpanIn,
                                   TopologyRefresh, TraceMerge)
from aiops.interfaces.runner import run, workflow_path
from aiops.sources.base import BaseDataSourceSpec
from aiops.storage import crud, models
from aiops.storage.hooks import StorageHooks
from aiops.storage.repository import AsyncEdges, Incidents, LotPaths, ServiceEdges

NOW = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
MIGRATION = Path(__file__).resolve().parents[1] / "migrations" / "postgres" / "0009_graph_environment_scope.sql"

# 同名服務、不同呼叫關係：正式環境 cart 呼叫 inventory；測試環境 cart 呼叫 pricing（inventory 在測試環境是假的、不經 cart）
PROD = {("cart", "inventory"): 5}
STAGING = {("cart", "pricing"): 7}


def _seed_edges() -> None:
    ServiceEdges.add_calls(PROD, seen_at=NOW, environment="prod")
    ServiceEdges.add_calls(STAGING, seen_at=NOW, environment="staging")


def _loaded(environment: str, **kw) -> PipelineContext:
    ctx = PipelineContext(pipeline=f"it_{environment or 'none'}", now=NOW, environment=environment, **kw)
    StorageHooks().load_batch(ctx)
    return ctx


def _workflow(name: str, environment: str) -> Workflow:
    return load_workflow(workflow_path("it_otel")).model_copy(update={"name": name, "environment": environment})


def _detect(name: str, environment: str) -> list[list[str]]:
    """cart、inventory、pricing 同時異常：照這條管線環境的拓撲跑一輪內建偵測管線（it_otel 的節點），回各事件群的實體。"""
    items = [ExternalAnomalyIn(external_id=f"{name}-{e}", entity=e, metric="latency", ts=NOW - timedelta(minutes=3), score=5)
             for e in ("cart", "inventory", "pricing")]
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline=name, source="x", anomalies=items))
    result = Pipeline(_workflow(name, environment), StorageHooks()).run(now=NOW, targets=[]).to_result()
    return sorted(sorted(i["entities"]) for i in result["incidents"])


# ── Workflow.environment 與實體命名規則（B1、B2）──


def test_environment_is_optional_and_defaults_to_no_scope():
    assert Workflow(name="w", nodes=[{"name": "n", "operator": "notifier.log"}]).environment == ""
    assert load_workflow(workflow_path("it_otel")).environment == ""  # 出貨的管線都沒設：行為不變


@pytest.mark.parametrize("bad", ["prod env", "../prod", "prod/eu", "x" * 65])
def test_environment_name_is_a_plain_token(bad):
    with pytest.raises(ValueError):
        Workflow(name="w", nodes=[{"name": "n", "operator": "notifier.log"}], environment=bad)


@pytest.mark.parametrize(("entity", "environment", "hit"), [
    ("cart-prod", "prod", True), ("prod.cart", "prod", True), ("cart@PROD", "prod", True), ("cart_staging", "staging", True),
    ("product-api", "prod", False), ("cart", "prod", False), ("cart-prod", "", False),
])
def test_entity_names_must_not_carry_the_environment(entity, environment, hit):
    assert environment_in_entity(entity, environment) is hit


def test_workflow_rejects_targets_whose_entity_carries_the_environment_at_load_and_run():
    wf = _workflow("it_prod", "prod")
    bad = TargetSpec(source_type="mock", query="q", entity="cart-prod")
    with pytest.raises(WorkflowError, match="夾帶了環境名"):
        Pipeline(wf.model_copy(update={"targets": [bad]}), StorageHooks())
    with pytest.raises(WorkflowError, match="夾帶了環境名"):
        Pipeline(wf, StorageHooks()).run(now=NOW, targets=[bad])
    # 沒寫 entity 時名稱就是查詢字串，裡面本來就可能有 env="prod" 篩選：不檢查
    Pipeline(wf, StorageHooks()).check_target(TargetSpec(source_type="mock", query='latency{env="prod",svc="cart"}'))


# ── 資料表：唯一鍵含 environment，讀寫與汰除都只碰自己的環境（C2）──


def test_same_pair_in_two_environments_is_two_rows_and_counts_separately():
    ServiceEdges.add_calls({("cart", "inventory"): 5}, seen_at=NOW, environment="prod")
    again = ServiceEdges.add_calls({("cart", "inventory"): 2}, seen_at=NOW, environment="staging")
    assert again == {"added": 1, "updated": 0}  # 別的環境有同一對，不算「已存在」
    assert [e["calls"] for e in ServiceEdges.all("prod")] == [5] and [e["calls"] for e in ServiceEdges.all("staging")] == [2]
    assert ServiceEdges.all() == []  # '' 是「不分環境」那一份，不是「全部」


def test_prune_only_touches_its_own_environment():
    old = NOW - timedelta(days=10)
    ServiceEdges.add_calls({("a", "b"): 1}, seen_at=old, environment="prod")
    ServiceEdges.add_calls({("a", "b"): 1}, seen_at=old, environment="staging")
    AsyncEdges.record({("p", "c"): (100.0, 1)}, seen_at=old, environment="prod")
    AsyncEdges.record({("p", "c"): (100.0, 1)}, seen_at=old, environment="staging")
    assert ServiceEdges.prune(NOW, environment="staging") == 1 and AsyncEdges.prune(NOW, environment="staging") == 1
    assert len(ServiceEdges.all("prod")) == 1 and len(AsyncEdges.all("prod")) == 1


def test_same_lot_id_in_two_environments_keeps_both_paths():
    lot = {"lot_id": "L1", "start": NOW, "end": NOW}
    LotPaths.upsert_many([{**lot, "path": ["E1", "E2"]}], environment="fab-a")
    LotPaths.upsert_many([{**lot, "path": ["E9", "E8"]}], environment="fab-b")
    LotPaths.upsert_many([{**lot, "path": ["E1", "E3"]}], environment="fab-a")  # 同環境同 lot_id：覆蓋
    assert LotPaths.paths("fab-a") == [["E1", "E3"]] and LotPaths.paths("fab-b") == [["E9", "E8"]] and LotPaths.paths() == []


# ── 載入：每條管線只看到自己環境的圖 ──


def test_hooks_load_only_the_pipelines_environment():
    _seed_edges()
    AsyncEdges.record({("cart", "mailer"): (900.0, 3)}, seen_at=NOW, environment="staging")
    LotPaths.upsert_many([{"lot_id": "L1", "path": ["E1", "E2"], "start": NOW, "end": NOW}], environment="prod")
    prod, staging, none = _loaded("prod"), _loaded("staging"), _loaded("")
    assert prod.graph.pairs(EdgeKind.CALL) == {("cart", "inventory")}
    assert staging.graph.pairs(EdgeKind.CALL) == {("cart", "pricing")}
    assert prod.async_edges == {} and staging.async_edges == {("cart", "mailer"): 900.0}
    assert prod.lot_edges == [LotEdge("E1", "E2", 1)] and staging.lot_edges == []
    assert len(none.graph) == 0  # 不分環境的管線看不到任何環境專屬的邊


def test_two_environments_with_same_services_group_by_their_own_topology():
    _seed_edges()
    assert _detect("it_prod", "prod") == [["cart", "inventory"], ["pricing"]]
    assert _detect("it_staging", "staging") == [["cart", "pricing"], ["inventory"]]


def test_without_environments_both_call_graphs_collapse_into_one_card():
    """對照組：同樣的邊不分環境存（舊行為），兩個環境的呼叫關係混在一起，三個服務被併成一張卡。"""
    ServiceEdges.add_calls({**PROD, **STAGING}, seen_at=NOW)
    assert _detect("it_shared", "") == [["cart", "inventory", "pricing"]]


# ── RCA：圖跟著事件群的環境，不跟 RCA 管線 ──


def _incident(pipeline: str, entity: str) -> dict:
    (row,) = [r for r in Incidents.list_open(pipeline) if entity in r["incident"]["entities"]]
    return row


def test_incident_snapshot_records_environment_and_only_its_own_edges():
    _seed_edges()
    _detect("it_prod", "prod")
    _detect("it_staging", "staging")
    prod_cart, staging_cart = _incident("it_prod", "cart"), _incident("it_staging", "cart")
    assert prod_cart["context"]["environment"] == "prod" and prod_cart["context"]["topology"] == [["cart", "inventory"]]
    assert staging_cart["context"]["environment"] == "staging" and staging_cart["context"]["topology"] == [["cart", "pricing"]]
    rca = _loaded("", incident_id=staging_cart["id"])  # RCA 管線本身不分環境
    assert rca.incident_environment == "staging" and rca.graph.pairs(EdgeKind.CALL) == {("cart", "pricing")}


def test_rca_without_snapshot_topology_loads_the_incidents_environment_not_the_rca_pipelines():
    _seed_edges()
    ServiceEdges.add_calls({("cart", "legacy"): 1}, seen_at=NOW)  # 不分環境的那一份：不該被 staging 的 RCA 載入
    _detect("it_staging", "staging")
    lone = _incident("it_staging", "inventory")  # 在測試環境 inventory 沒有任何邊：快照的拓撲是空的
    assert lone["context"]["topology"] == []
    rca = _loaded("", incident_id=lone["id"])
    assert rca.incident_environment == "staging" and rca.graph.pairs(EdgeKind.CALL) == {("cart", "pricing")}


def test_old_snapshots_without_environment_keep_the_old_behaviour():
    """加這個欄位之前建的事件群，快照沒有 environment：沿用 RCA 管線自己的（''），跟以前一樣載不分環境的那一份。"""
    ServiceEdges.add_calls({("cart", "inventory"): 1}, seen_at=NOW)
    _detect("it_otel", "")
    lone = _incident("it_otel", "pricing")
    crud.update(models.IncidentRow, lone["id"], context={"topology": []})  # 舊版程式寫的快照長這樣
    rca = _loaded("", incident_id=lone["id"])
    assert rca.incident_environment == "" and rca.graph.pairs(EdgeKind.CALL) == {("cart", "inventory")}


# ── 寫入動作接受 environment ──


class _FakeTraces(BaseDataSourceSpec):
    source_type = "faketrace"

    def fetch_data(self, *a, **k):
        raise NotImplementedError

    def fetch_spans(self, start, end, services=None, max_spans=20000):
        def span(sid, parent, svc, env):
            return {"trace_id": sid[0], "span_id": sid, "parent_id": parent, "service": svc, "ts": NOW,
                    "duration_ms": 1.0, "error": False, "environment": env}
        # 同一個 trace 來源混了兩個環境（deployment.environment 不同）
        return [span("a1", None, "cart", "prod"), span("a2", "a1", "inventory", "prod"),
                span("b1", None, "cart", "staging"), span("b2", "b1", "pricing", "staging")], False

    def check(self):
        return {"ok": True, "detail": ""}


@pytest.fixture
def mixed_source(monkeypatch):
    monkeypatch.setitem(sources._SPECS, "faketrace", _FakeTraces)


def test_topology_refresh_writes_and_prunes_only_its_environment(mixed_source):
    ServiceEdges.add_calls({("old", "gone"): 1}, seen_at=NOW - timedelta(days=30), environment="staging")
    out = run("admin_topology_refresh", request=TopologyRefresh(source_type="faketrace", environment="prod"), now=NOW)
    assert out["environment"] == "prod" and out["edges_pruned"] == 0
    assert {(e["caller"], e["callee"]) for e in ServiceEdges.all("staging")} == {("old", "gone")}  # 別環境的舊邊沒被 prune
    assert ServiceEdges.all() == []


@pytest.mark.xfail(strict=True, reason="缺口：撈 span 還沒依 deployment.environment 過濾（aiops/sources 另案重寫 fetch_spans 中），"
                                       "同一個 trace 來源混了多個環境時，別環境的呼叫也會記進這個環境。兩邊合流後接上就會轉綠。")
def test_topology_refresh_only_counts_spans_of_its_own_deployment_environment(mixed_source):
    run("admin_topology_refresh", request=TopologyRefresh(source_type="faketrace", environment="prod"), now=NOW)
    assert {(e["caller"], e["callee"]) for e in ServiceEdges.all("prod")} == {("cart", "inventory")}


def test_merge_traces_and_lot_paths_write_into_the_requested_environment():
    spans = [SpanIn(trace_id="t", service="cart", start=NOW, duration_ms=1.0, span_id="1"),
             SpanIn(trace_id="t", service="pricing", start=NOW, duration_ms=1.0, span_id="2", parent_id="1")]
    out = run("admin_merge_traces", request=TraceMerge(spans=spans, environment="staging"))
    assert out["environment"] == "staging"
    assert [(e["caller"], e["callee"]) for e in ServiceEdges.all("staging")] == [("cart", "pricing")] and ServiceEdges.all() == []
    steps = [LotStepIn(lot_id="L1", station="s1", equipment="E1", start=NOW, end=NOW),
             LotStepIn(lot_id="L1", station="s2", equipment="E2", start=NOW + timedelta(minutes=1), end=NOW + timedelta(minutes=2))]
    run("admin_merge_lot_paths", request=LotPathsMerge(steps=steps, environment="fab-a"))
    assert LotPaths.paths("fab-a") == [["E1", "E2"]] and LotPaths.paths() == []


def test_api_rejects_bad_environment_names(client):
    assert client.post("/api/v1/ingestion/topology/refresh", json={"environment": "prod env"}).status_code == 422
    assert client.post("/api/v1/ingestion/traces/merge", json={
        "environment": "../x", "spans": [{"trace_id": "t", "service": "a", "start": NOW.isoformat(), "duration_ms": 1}]}
    ).status_code == 422


# ── migration 與程式建表的唯一鍵名稱一致 ──


def test_migration_creates_the_same_named_unique_keys_as_the_models():
    sql = MIGRATION.read_text(encoding="utf-8")
    for row in (models.ServiceEdgeRow, models.ServiceAsyncEdgeRow, models.LotPathRow):
        (uq,) = [c for c in row.__table__.constraints if c.__class__.__name__ == "UniqueConstraint"]
        cols = ", ".join(c.name for c in uq.columns)
        assert re.search(rf"ADD CONSTRAINT {uq.name} UNIQUE \({cols}\)", sql), (uq.name, cols)
        assert f"ALTER TABLE {row.__tablename__} ADD COLUMN IF NOT EXISTS environment VARCHAR(64) NOT NULL DEFAULT ''" in sql
