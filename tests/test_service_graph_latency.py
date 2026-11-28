"""service graph 的延遲邊列表：每條呼叫在事故期間與事故前 30 分鐘（基線）的請求量、失敗率、p95、平均。

只做算術與排序，沒有「算不算異常」的門檻。數字的換算用 record_service_graph.py 從真的 Tempo 2.9.0 → VictoriaMetrics v1.110.0
錄下來的回應重播（service_graph.json 的 with_latency）：基線 20 條快的請求、6 分半之後 20 條慢的（mes-db 每次 1.5～2.5 秒、
每個請求查 5 次、4 條失敗）。

已知限制（沒有粉飾）：
  - p95 是直方圖的桶內插出來的。Tempo 預設的桶是 0.1、0.2、0.4…12.8 秒：基線 15～25 毫秒的呼叫，算出來的 p95 是桶內的 95 毫秒左右，
    不是真的 p95。平均（耗時總和 ÷ 次數）不受桶寬影響，所以一起給。
  - VictoriaTraces 的 service graph 沒有這些數字，只有誰呼叫誰。
"""
from __future__ import annotations

import json
import pathlib
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import evidence as gather
from src import graph, promql
from src.db import Database
from src.grafana import Grafana
from src.grouping import Alert, converge
from src.main import Settings, relations_for
from tests.conftest import recorded_service_graph
from tests.test_service_graph import replaying

MINIMAL = {"grafana": {"url": "http://g", "token": "t"}, "webhook": {"token": "w"}}
NOW = datetime(2026, 11, 27, 14, 0, tzinfo=timezone.utc)


# ── 兩跳、排序、上限 ──

def edge(caller, callee, **numbers):
    return {"caller": caller, "callee": callee, "requests": 10, "failed": 0, "kind": "", **numbers}


@pytest.fixture
def known(db_url):
    database = Database(db_url)
    database.record_calls([("portal", "api"), ("api", "db"), ("api", "cache"), ("db", "storage"), ("batch", "storage"), ("x", "y")],
                          "service_graph", NOW, 30)
    return database


def relations(database, services, edges):
    alerts = [Alert(f"f{i}", "firing", f"r{i}", f"{s} 告警", s, "P2", NOW, None, {}) for i, s in enumerate(services)]
    [inc] = converge([], alerts, [], [], now=NOW, close_after_seconds=300)
    return relations_for(Settings.from_dict(MINIMAL), database, inc, edges, NOW)


def test_relations_reach_two_hops_and_unrelated_calls_stay_out(known):
    got = {(r["caller"], r["callee"]): r["hops"] for r in relations(known, ["api"], [])}
    assert got == {("portal", "api"): 1, ("api", "db"): 1, ("api", "cache"): 1, ("db", "storage"): 2}
    # batch → storage 隔了三跳（api → db → storage ← batch）、x → y 跟這張事故無關：都不放


def test_one_hop_comes_first_then_the_biggest_slowdown_and_numbers_are_carried_as_they_are(known):
    edges = [edge("api", "db", p95_ms=2400.0, p95_baseline_ms=95.0, p95_ratio=25.26, latency_from="client", rps=0.6, rps_baseline=0.01),
             edge("api", "cache", avg_ms=30.0, avg_baseline_ms=10.0, avg_ratio=3.0, latency_from="server"),   # 沒有 p95：照平均的倍數排
             edge("portal", "api"),                                                                          # 沒有倍數：排在有倍數的後面
             edge("db", "storage", p95_ratio=99.0)]                                                          # 倍數最大，但隔兩跳：還是排在一跳的後面
    got = relations(known, ["api"], edges)
    assert [(r["caller"], r["callee"]) for r in got] == [("api", "db"), ("api", "cache"), ("portal", "api"), ("db", "storage")]
    assert {k: got[0][k] for k in ("p95_ms", "p95_baseline_ms", "p95_ratio", "latency_from", "rps", "rps_baseline", "hops")} == \
           {"p95_ms": 2400.0, "p95_baseline_ms": 95.0, "p95_ratio": 25.26, "latency_from": "client", "rps": 0.6, "rps_baseline": 0.01, "hops": 1}
    shown = gather.collect_evidence(converge([], [Alert("f", "firing", "r", "api", "api", "P2", NOW, None, {})], [], [], now=NOW,
                                          close_after_seconds=300)[0], got, Grafana("http://g", "t"), id_fields=[], now=NOW)["edges"]
    assert {k: shown[0][k] for k in ("upstream", "downstream", "hops", "p95_ms", "p95_ratio", "latency_from")} == \
           {"upstream": "db", "downstream": "api", "hops": 1, "p95_ms": 2400.0, "p95_ratio": 25.26, "latency_from": "client"}
    assert shown[0]["requests"] == 10 and shown[0]["failed"] == 0


def test_no_more_than_the_limit_are_given(db_url):
    database = Database(db_url)
    database.record_calls([("hub", f"svc-{n:02d}") for n in range(30)], "service_graph", NOW, 30)
    assert len(relations(database, ["hub"], [])) == gather.MAX_RELATIONS


# ── 延遲換算：照錄下來的回應重播 ──

FIX = recorded_service_graph("with_latency")
_FILE = json.loads((pathlib.Path(__file__).parent / "fixtures/grafana13/service_graph.json").read_text(encoding="utf-8"))
START, END = (datetime.fromisoformat(_FILE[k].replace("Z", "+00:00")) for k in ("incident_start", "recorded_at"))
EMPTY = {**FIX[0]["response"], "data": {**FIX[0]["response"]["data"], "result": []}}   # 真的回應，只是沒有序列（VictoriaMetrics 查不到時的樣子）
BAD = recorded_service_graph("bad_query")[0]


def replay(change=lambda query, reply: reply):
    """照順序重播 with_latency 的 12 個回應，並檢查送出的查詢與時間點跟錄的時候一樣。change 可以換掉某些回應（模擬現場的情況）。"""
    queue = list(FIX)

    def handler(request):
        reply = queue.pop(0)
        assert dict(request.url.params) == reply["request"]["params"]
        status, body = change(request.url.params["query"], (reply["status"], reply["response"]))
        return httpx.Response(status, json=body)
    return Grafana("http://grafana.test", "t", transport=httpx.MockTransport(handler), retries=0)


def edges_by_pair(grafana):
    return {(e["caller"], e["callee"]): e for e in graph.service_graph(grafana, "vm-uid", START, END, baseline_minutes=30)}


def test_the_edge_list_has_rps_error_rate_p95_and_the_baseline_from_real_tempo_metrics():
    got = edges_by_pair(replay())
    minutes = 3   # 事故時間窗 13:57:06～13:59:27，查詢的區間進位到 3 分鐘
    db, portal, queue = got[("mes-api", "mes-db")], got[("web-portal", "mes-api")], got[("mes-api", "report-worker")]
    # 沒埋點的資料庫也有完整的一組數字。Tempo 2.9.0 實測：它的 server 端與 client 端數字一模一樣（server 那一份是拿呼叫端量的填的）
    assert {k: db[k] for k in ("requests", "failed", "kind", "latency_from")} == {"requests": 100, "failed": 4, "kind": "database", "latency_from": "server"}
    assert db["rps"] == promql.round4(100 / (minutes * 60)) and db["rps_baseline"] == promql.round4(20 / 1800)     # 事故 20 個請求 × 5 次；基線 20 × 1 次
    assert (db["error_rate_pct"], db["error_rate_baseline_pct"]) == (4.0, 0.0)
    assert (db["p95_ms"], db["p95_baseline_ms"], db["p95_ratio"]) == (3111.0, 95.0, promql.round4(3111.0 / 95.0))
    assert (db["avg_ms"], db["avg_baseline_ms"]) == (2000.0, 12.55)
    assert db["avg_ratio"] == promql.round4(1.99955 / 0.01255) == 159.3   # 倍數用原始值算（錄到的 1.99955 秒 ÷ 0.01255 秒），不用四捨五入後的
    # 已知限制：基線的呼叫 15～25 毫秒，p95 卻是 95 毫秒（落在預設最小的桶 0～100 毫秒裡，內插出來的）；平均才是 12.55
    assert db["p95_baseline_ms"] == 95.0 and db["avg_baseline_ms"] < 25
    # 上下游的請求量：資料庫的每秒請求數是上游的 5 倍（每個請求查 5 次）。程式不說這是 N+1，只給數字
    assert db["requests"] == 5 * portal["requests"] and db["rps"] == promql.round4(5 * portal["requests"] / (minutes * 60))
    assert (portal["error_rate_pct"], portal["p95_ratio"] > 100) == (20.0, True)
    assert queue["kind"] == "messaging" and queue["p95_ratio"] == 1.0          # 經訊息佇列的那一段沒有變慢
    assert got[("orders-api", "orders-db")]["rps"] == 0.0 and "p95_ms" not in got[("orders-api", "orders-db")]   # 這段時間沒請求：沒有延遲可算，不寫 0


def test_when_the_server_side_has_no_histogram_the_client_side_is_used():
    no_server = lambda query, reply: (reply[0], EMPTY) if "server_seconds" in query else reply
    db = edges_by_pair(replay(no_server))[("mes-api", "mes-db")]
    assert db["latency_from"] == "client" and (db["p95_ms"], db["p95_baseline_ms"], db["avg_ms"]) == (3111.0, 95.0, 2000.0)


def test_without_histograms_or_when_the_latency_queries_fail_only_counts_are_kept():
    keep = ("caller", "callee", "requests", "failed", "kind")
    no_histograms = lambda query, reply: (reply[0], EMPTY) if "_seconds" in query else reply
    got = edges_by_pair(replay(no_histograms))[("mes-api", "mes-db")]
    assert "latency_from" not in got and "p95_ms" not in got and got["rps"] > 0 and got["error_rate_pct"] == 4.0
    broken = lambda query, reply: (BAD["status"], BAD["response"]) if "_seconds" in query else reply
    got = edges_by_pair(replay(broken))                                         # 延遲的查詢回 422：整輪不中斷，只剩次數
    assert {k: got[("mes-api", "mes-db")][k] for k in keep} == {"caller": "mes-api", "callee": "mes-db", "requests": 100, "failed": 4, "kind": "database"}
    assert all(set(e) == set(keep) for e in got.values())


def test_victoriatraces_has_no_latency_numbers_and_says_nothing_about_them():
    edges = graph.read_service_graph(replaying("victoriatraces_dependencies"), traces_type="victoriatraces", traces_uid="vt-uid",
                                     metrics_uid="", start=END - timedelta(minutes=15), end=END, baseline_minutes=30)
    assert edges and all(set(e) == {"caller", "callee", "requests", "failed", "kind"} for e in edges)
