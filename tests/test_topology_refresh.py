"""admin.topology_refresh：從 trace 來源排程沉澱服務拓撲（HARDENING_DESIGN 第 4 節）。

累加與 last_seen、prune（NULL 不刪）、空 trace、抽樣截斷、資料源沒設定、連不上、API、YAML 載入、merge_traces 也更新 last_seen。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest

import aiops.sources as sources
from aiops.engine.errors import BadRequest, InfrastructureError
from aiops.engine.requests import SpanIn, TopologyRefresh, TraceMerge
from aiops.interfaces.runner import build_pipeline, run, run_context
from aiops.sources.base import BaseDataSourceSpec
from aiops.sources.elasticsearch import ElasticsearchSpec
from aiops.sources.mock import MOCK_TRACE_CHAIN
from aiops.storage import crud
from aiops.storage.models import ServiceEdgeRow
from aiops.storage.repository import ServiceEdges

NOW = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)


def _span(trace: str, sid: str, parent: str | None, svc: str) -> dict:
    return {"trace_id": trace, "span_id": sid, "parent_id": parent, "service": svc, "ts": NOW, "duration_ms": 5.0,
            "error": False}


def _chain_spans(n: int) -> list[dict]:
    """n 條 trace：gateway -> order-api -> inventory-svc，另有同服務內的 db 子 span（不算跨服務）。"""
    spans = []
    for i in range(n):
        spans += [_span(f"t{i}", f"g{i}", None, "gateway"), _span(f"t{i}", f"o{i}", f"g{i}", "order-api"),
                  _span(f"t{i}", f"i{i}", f"o{i}", "inventory-svc"), _span(f"t{i}", f"d{i}", f"i{i}", "inventory-svc")]
    return spans


class _FakeTraces(BaseDataSourceSpec):
    """測試用 trace 來源：回傳指定的 span 與截斷旗標，記下被問的時間窗。"""

    source_type = "faketrace"
    spans: list[dict] = []
    truncated = False
    calls: list[tuple] = []

    def fetch_data(self, *a, **k):
        raise NotImplementedError

    def fetch_spans(self, start, end, services=None, max_spans=20000):
        type(self).calls.append((start, end, services, max_spans))
        return list(type(self).spans), type(self).truncated

    def check(self):
        return {"ok": True, "detail": ""}


@pytest.fixture
def fake(monkeypatch):
    spec = type("_Fake", (_FakeTraces,), {"spans": [], "truncated": False, "calls": []})
    monkeypatch.setitem(sources._SPECS, "faketrace", spec)
    return spec


def _refresh(**kw) -> dict:
    return run("admin_topology_refresh", request=TopologyRefresh(source_type="faketrace", **kw), now=NOW)


def _edges() -> dict[tuple[str, str], dict]:
    return {(e["caller"], e["callee"]): e for e in ServiceEdges.all()}


def test_yaml_loads_and_wires_the_operator():
    pipe = build_pipeline("admin_topology_refresh")
    assert [n.operator for n in pipe.workflow.nodes] == ["admin.topology_refresh"]


def test_refresh_accumulates_calls_and_sets_last_seen(fake):
    fake.spans = _chain_spans(3)
    out = _refresh(lookback_minutes=30)
    assert out["edges"] == [{"caller": "gateway", "callee": "order-api", "calls": 3},
                            {"caller": "order-api", "callee": "inventory-svc", "calls": 3}]
    assert (out["spans"], out["truncated"], out["edges_seen"], out["edges_added"], out["edges_updated"], out["edges_pruned"]) \
        == (12, False, 2, 2, 0, 0)
    assert out["window"] == {"start": (NOW - timedelta(minutes=30)).isoformat(), "end": NOW.isoformat()}
    assert fake.calls[-1] == (NOW - timedelta(minutes=30), NOW, None, 20000)  # 不限服務：拓撲要看全部

    later = NOW + timedelta(hours=1)
    fake.spans = _chain_spans(2)
    again = run("admin_topology_refresh", request=TopologyRefresh(source_type="faketrace"), now=later)
    assert (again["edges_added"], again["edges_updated"]) == (0, 2)
    rows = _edges()
    assert rows[("gateway", "order-api")]["calls"] == 5 and rows[("order-api", "inventory-svc")]["calls"] == 5
    assert all(r["last_seen"] == later.replace(tzinfo=None) for r in rows.values())  # 牆鐘（ctx.now），存 naive UTC


def test_end_replays_a_historical_window_but_last_seen_is_wall_clock(fake):
    fake.spans = _chain_spans(1)
    end = datetime(2026, 11, 3, 16, 20)  # 沒帶時區：視為 UTC
    out = _refresh(end=end, lookback_minutes=140)
    assert out["window"] == {"start": "2026-11-03T14:00:00+00:00", "end": "2026-11-03T16:20:00+00:00"}
    assert fake.calls[-1][:2] == (datetime(2026, 11, 3, 14, 0, tzinfo=timezone.utc), datetime(2026, 11, 3, 16, 20, tzinfo=timezone.utc))
    assert {r["last_seen"] for r in _edges().values()} == {NOW.replace(tzinfo=None)}


def test_prune_removes_only_edges_unseen_for_n_days_and_keeps_null(fake):
    ServiceEdges.add_calls({("old", "gone"): 4}, seen_at=NOW - timedelta(days=8))
    ServiceEdges.add_calls({("recent", "kept"): 2}, seen_at=NOW - timedelta(days=2))
    crud.create(ServiceEdgeRow, caller="legacy", callee="unknown-age", calls=9, last_seen=None)  # 0006 之前的舊列
    out = _refresh()  # 空 trace
    assert out["edges_pruned"] == 1
    assert set(_edges()) == {("recent", "kept"), ("legacy", "unknown-age")}
    assert ServiceEdges.prune(NOW + timedelta(days=365)) == 1  # NULL 永遠不被 prune 刪
    assert set(_edges()) == {("legacy", "unknown-age")}


def test_prune_days_is_configurable(fake):
    ServiceEdges.add_calls({("a", "b"): 1}, seen_at=NOW - timedelta(days=2))
    assert _refresh(prune_days=1)["edges_pruned"] == 1 and _edges() == {}


def test_empty_traces_report_zero_edges_without_error(fake):
    fake.spans = [_span("t", "x", None, "cart-api"), _span("t", "y", "x", "cart-api")]  # 只有同服務內的子 span
    ServiceEdges.add_calls({("a", "b"): 1}, seen_at=NOW - timedelta(days=1))
    out = _refresh()
    assert (out["spans"], out["edges_seen"], out["edges_added"], out["edges_updated"], out["edges_pruned"], out["edges"]) \
        == (2, 0, 0, 0, 0, [])
    assert set(_edges()) == {("a", "b")}  # 沒看到邊不代表既有的邊消失：不清空


def test_truncated_sampling_is_reported_as_is(fake):
    fake.spans, fake.truncated = _chain_spans(2), True
    out = _refresh(max_spans=8)
    assert out["truncated"] is True and fake.calls[-1][3] == 8 and out["edges_seen"] == 2


def test_mock_source_truncates_at_max_spans():
    out = run("admin_topology_refresh", request=TopologyRefresh(source_type="mock", lookback_minutes=10, max_spans=5), now=NOW)
    assert out["spans"] == 5 and out["truncated"] is True


def test_unconfigured_source_is_skipped_without_touching_edges(monkeypatch):
    monkeypatch.delenv("AIOPS_ES_URL", raising=False)
    ServiceEdges.add_calls({("a", "b"): 1}, seen_at=NOW - timedelta(days=30))
    out = run("admin_topology_refresh", request=TopologyRefresh(), now=NOW)  # 預設 elasticsearch
    assert "沒有設定" in out["skipped"] and out["edges_seen"] == 0 and out["edges_pruned"] == 0
    assert set(_edges()) == {("a", "b")}  # 沒撈就不 prune


def test_no_request_uses_defaults_for_scheduled_run_once(monkeypatch):
    monkeypatch.delenv("AIOPS_ES_URL", raising=False)
    ctx = run_context("admin_topology_refresh", now=NOW, strict=False)
    assert ctx.result["source_type"] == "elasticsearch" and "skipped" in ctx.result
    assert ctx.result["window"]["start"] == (NOW - timedelta(minutes=60)).isoformat()


def test_unknown_source_is_bad_request():
    with pytest.raises(BadRequest):
        run("admin_topology_refresh", request=TopologyRefresh(source_type="nope"), now=NOW)


def test_unreachable_source_raises_infrastructure_error(monkeypatch):
    def refuse(request):
        raise httpx.ConnectError("connection refused", request=request)

    client = httpx.Client(transport=httpx.MockTransport(refuse))

    class _Down(ElasticsearchSpec):
        @classmethod
        def from_env(cls):
            return cls("http://es-down", api_key="k", client=client)

        @classmethod
        def configured(cls):
            return True

    monkeypatch.setitem(sources._SPECS, "elasticsearch", _Down)
    ServiceEdges.add_calls({("a", "b"): 1}, seen_at=NOW - timedelta(days=30))
    with pytest.raises(InfrastructureError):
        run("admin_topology_refresh", request=TopologyRefresh(), now=NOW)
    assert set(_edges()) == {("a", "b")}  # 失敗的那一輪不 prune


def test_merge_traces_also_updates_last_seen():
    spans = [SpanIn(trace_id="t", service="a", start=NOW, duration_ms=1.0, span_id="1"),
             SpanIn(trace_id="t", service="b", start=NOW, duration_ms=1.0, span_id="2", parent_id="1")]
    ServiceEdges.add_calls({("a", "b"): 1}, seen_at=NOW - timedelta(days=6))
    run("admin_merge_traces", request=TraceMerge(spans=spans))
    row = _edges()[("a", "b")]
    assert row["calls"] == 2 and row["last_seen"] > (NOW - timedelta(days=1)).replace(tzinfo=None)


def test_api_endpoint_validates_and_runs(client):
    assert client.post("/api/v1/ingestion/topology/refresh", json={"lookback_minutes": 0}).status_code == 422
    assert client.post("/api/v1/ingestion/topology/refresh", json={"max_spans": -1}).status_code == 422
    assert client.post("/api/v1/ingestion/topology/refresh", json={"end": "not-a-date"}).status_code == 422
    resp = client.post("/api/v1/ingestion/topology/refresh",
                       json={"source_type": "mock", "lookback_minutes": 5, "end": "2026-11-09T12:00:00Z"})
    assert resp.status_code == 200
    body = resp.json()["result"]
    chain = list(MOCK_TRACE_CHAIN)
    assert [(e["caller"], e["callee"]) for e in body["edges"]] == sorted(zip(chain, chain[1:]))
    assert body["edges_added"] == len(chain) - 1 and body["pipeline"] == "admin_topology_refresh"
