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

from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import graph, rca
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
    shown = rca.collect_evidence(converge([], [Alert("f", "firing", "r", "api", "api", "P2", NOW, None, {})], [], [], now=NOW,
                                          close_after_seconds=300)[0], got, Grafana("http://g", "t"), id_fields=[], now=NOW)["edges"]
    assert {k: shown[0][k] for k in ("upstream", "downstream", "hops", "p95_ms", "p95_ratio", "latency_from")} == \
           {"upstream": "db", "downstream": "api", "hops": 1, "p95_ms": 2400.0, "p95_ratio": 25.26, "latency_from": "client"}
    assert shown[0]["requests"] == 10 and shown[0]["failed"] == 0


def test_no_more_than_the_limit_are_given(db_url):
    database = Database(db_url)
    database.record_calls([("hub", f"svc-{n:02d}") for n in range(30)], "service_graph", NOW, 30)
    assert len(relations(database, ["hub"], [])) == rca.MAX_RELATIONS
