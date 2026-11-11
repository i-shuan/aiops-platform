"""資料源短暫抖動的重試（sources/_http.request），以及 RCA 撈時序時資料源連不上的降級。

重試：連線失敗／逾時與 429、502、503、504 重試 2 次（間隔 0.5、1.5 秒）；認證失敗、查詢錯誤、其他 5xx 不重試。
降級：ingestion.incident_series（目前沒掛在內建管線上，這裡用最小 workflow 跑）遇到資料源連不上，寫進 errors、略過，報告照出（以前整份失敗）。
"""
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from aiops.engine.errors import InfrastructureError
from aiops.engine.pipeline import NodeSpec, Pipeline, Workflow
from aiops.engine.types import AnomalyEvent, Incident
from aiops.sources import _http
from aiops.storage.hooks import StorageHooks
from aiops.storage.repository import Incidents


def _client(*replies):
    """依序回應：Exception 實例就丟出，數字就回那個狀態碼。回傳 (client, calls)。"""
    calls = []

    def handler(req):
        calls.append(req)
        r = replies[min(len(calls), len(replies)) - 1]
        if isinstance(r, Exception):
            raise r
        return httpx.Response(r, json={}, request=req)

    return httpx.Client(transport=httpx.MockTransport(handler)), calls


def _get(client):
    return _http.request(client, "es", "POST", "http://es/_search")


@pytest.mark.parametrize("first", [httpx.ConnectError("refused"), httpx.ReadTimeout("slow"), 429, 502, 503, 504])
def test_transient_failures_are_retried_then_succeed(first, retry_waits):
    client, calls = _client(first, 200)
    assert _get(client).status_code == 200
    assert len(calls) == 2 and retry_waits == [0.5]


def test_retries_twice_then_reports_how_many_times_it_tried(retry_waits):
    client, calls = _client(503, 503, 503, 200)
    with pytest.raises(InfrastructureError, match=r"HTTP 503）（已重試 2 次）"):
        _get(client)
    assert len(calls) == 3 and retry_waits == [0.5, 1.5]
    client, calls = _client(httpx.ConnectError("refused"))
    with pytest.raises(InfrastructureError, match="連不上（已重試 2 次）"):
        _get(client)
    assert len(calls) == 3


@pytest.mark.parametrize("status, outcome", [(401, "error"), (403, "error"), (500, "error"), (400, "returned"),
                                             (404, "returned"), (422, "returned")])
def test_non_transient_answers_are_not_retried(status, outcome, retry_waits):
    client, calls = _client(status, 200)
    if outcome == "error":
        with pytest.raises(InfrastructureError):
            _get(client)
    else:
        assert _get(client).status_code == status
    assert len(calls) == 1 and retry_waits == []


# ── RCA 撈時序／trace／日誌：資料源連不上時降級，不再整份失敗 ──

T = datetime(2026, 11, 3, 14, 6, tzinfo=timezone.utc)
ES_QUERY = '{"index": "metrics-apm.transaction.1m-*", "agg": "count"}'


def _series_report_workflow() -> Workflow:
    """原本 rca_report_correlation 的撈取節點＋報告（根因演算法已移除）：三個旁路撈取節點都連不上 ES。"""
    return Workflow(name="series_report", nodes=[
        NodeSpec(name="series", operator="ingestion.incident_series", optional=True,
                 params={"before_minutes": 30, "after_minutes": 10, "step_seconds": 60, "max_series": 20}),
        NodeSpec(name="fetch_traces", operator="ingestion.incident_traces",
                 params={"source_type": "elasticsearch", "before_minutes": 15, "after_minutes": 0, "max_spans": 20000,
                         "max_before_minutes": 60, "follow_messaging": True, "max_lag_minutes": 120}),
        NodeSpec(name="build_topology", operator="rca.build_dynamic_topology", params={"mode": "merge", "min_calls": 1}),
        NodeSpec(name="fetch_logs", operator="ingestion.incident_logs",
                 params={"source_type": "elasticsearch", "before_minutes": 5, "after_minutes": 5, "baseline_minutes": 60,
                         "max_logs": 5000}),
        NodeSpec(name="report", operator="rca.report",
                 params={"sections": ["summary", "timeline", "affected_entities", "propagation_path", "log_patterns"]}),
    ])


def test_series_report_survives_an_unreachable_source(monkeypatch, retry_waits):
    from aiops.sources import elasticsearch

    monkeypatch.setenv("AIOPS_ES_URL", "http://es-down:9200")
    real_client = httpx.Client

    def refused(req):  # 每個請求都連線被拒（跟 ES 重啟中一樣），不真的去等網路逾時
        raise httpx.ConnectError("[WinError 10061] 目標電腦拒絕連線", request=req)

    monkeypatch.setattr(elasticsearch.httpx, "Client", lambda **kw: real_client(transport=httpx.MockTransport(refused)))
    events = [AnomalyEvent(e, ES_QUERY, T + timedelta(minutes=m + d), 6.0, source_type="elasticsearch")
              for e, d in (("cart-api", 0), ("ord-svc", 6), ("bill-svc", 16)) for m in range(30)]
    inc = Incident(events)
    row = Incidents.create(inc.uid, "it_otel", inc.to_dict(), "P1", None, "it_otel",
                           {"topology": [["cart-api", "ord-svc"], ["ord-svc", "bill-svc"]], "lot_edges": []})
    res = Pipeline(_series_report_workflow(), StorageHooks()).run(incident_id=row["id"]).to_result()

    fetch = res["series_fetch"]
    assert fetch["fetched"] == 0 and len(fetch["skipped"]) == 3
    assert sum("同一資料源前面已失敗" in s["reason"] for s in fetch["skipped"]) == 2  # 第一條失敗後不再一條條等
    messages = [e["message"] for e in res["errors"]]
    assert any(m.startswith("elasticsearch 連不上，RCA 不含用它的時序") for m in messages)
    assert any(m.startswith("撈 trace 失敗") for m in messages) and any(m.startswith("撈日誌失敗") for m in messages)
    assert "propagation_path" in [s["key"] for s in res["sections"]]  # 既有拓撲照樣畫傳播路徑
    # 三個撈取節點各自重試 2 次（時序只試第一條）：0.5、1.5 各三次
    assert sorted(retry_waits) == [0.5, 0.5, 0.5, 1.5, 1.5, 1.5]
