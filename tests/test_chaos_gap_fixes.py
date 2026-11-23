"""混沌場景 2、3、5 的三個缺口（docs/CHAOS_E2E_REPORT.md 缺口 6、7、9）。

- 缺口 6（場景 3）：兩個下游資源各佔一半時，attribution 結構體只有等待增量最大的那一個 -> 其他有份的資源列在 others。
- 缺口 7（場景 5）：事件前有呼叫資源、事件期間程式在入口就崩潰（一次都沒呼叫到）-> attribution 不該掛一個無關的資源。
- 缺口 9（場景 2）：avg(summary.sum) 是當分鐘總耗時，低流量服務多來幾筆就像延遲暴增 -> 查詢要算每筆平均（Σ總耗時 ÷ Σ筆數）。
  平台不再直連 Elasticsearch（2026-11-20）：divide_by 的查詢組裝搬到混沌模擬器自己的 scripts/chaos/metric_query.py，
  這裡只留純函式的測試；ElasticsearchSpec.fetch_data 的測試隨它一起刪除。
"""
import json
from datetime import datetime, timedelta, timezone

import httpx
import numpy as np
import pandas as pd
import pytest

from aiops.engine.context import PipelineContext, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.detectors.hourly_baseline import HourlyBaselineDetector
from aiops.stages.enrichment.narrative import briefing_whitelist
from aiops.stages.rca.algorithms.evidence import OTHER_RESOURCE_SHARE
from scripts.chaos.metric_query import build_search_body, bucket_value, parse_es_query

T0 = datetime(2026, 11, 3, 14, 50, tzinfo=timezone.utc)
MIN = timedelta(minutes=1)
REDIS = ("db", "redis", "redis")
PG = ("db", "postgresql", "postgres")


def _request(tid, ts, app_ms, waits, error=False):
    """服務 A 一個請求：入口 transaction（總耗時 = 程式自身 + 等資源）、每個資源一個葉節點。waits = [((kind, subtype, resource), ms, 失敗?)]"""
    total = app_ms + sum(ms for _, ms, _ in waits)
    spans = [SpanRecord(tid, f"{tid}x", None, "A", ts, total, error, status_code=500 if error else 200, name="POST /x")]
    at = ts
    for k, ((kind, subtype, resource), ms, failed) in enumerate(waits):  # 一個接一個呼叫（同時呼叫的話重疊的等待只算一次）
        spans.append(SpanRecord(tid, f"{tid}w{k}", f"{tid}x", "A", at, ms, failed, kind=kind, subtype=subtype, resource=resource))
        at += timedelta(milliseconds=ms)
    return spans


def _traffic(before, during, before_n=40, during_n=60):
    """before／during：(app_ms, waits, error)。事件前 15 分鐘、事件期間 30 分鐘。"""
    spans = []
    for i in range(before_n):
        spans += _request(f"b{i}", T0 - timedelta(minutes=15) + i * (15 * MIN / before_n), *before)
    for i in range(during_n):
        spans += _request(f"d{i}", T0 + i * (30 * MIN / during_n), *during)
    return spans


def _top(spans):
    ctx = PipelineContext(pipeline="rca", now=T0 + timedelta(hours=1))
    ctx.rca_spans = spans
    ctx.incidents = [Incident([AnomalyEvent("A", "latency", T0 + m * MIN, 6.0) for m in range(0, 30, 5)])]
    ctx.incidents_built = True
    ranked = OperatorRegistry.create("rca.root_cause.evidence", {}).rank(ctx.incidents[0], ctx)
    assert ranked and ranked[0].entity == "A"
    return ranked[0]


# ---------- 缺口 6：兩個資源各佔一半 ----------

def test_two_resources_sharing_the_increase_are_both_in_the_attribution_struct():
    # redis 2→92（+90）、postgres 8→96（+88）、程式自身 20→42（+22）：各佔約 45%，沒有人過 60%
    top = _top(_traffic((20.0, [(REDIS, 2.0, False), (PG, 8.0, False)], False),
                        (42.0, [(REDIS, 92.0, False), (PG, 96.0, False)], False)))
    a = top.attribution
    assert top.dependency == "" and a["by"] == ""  # 沒有主導資源：人類字串與 by 照舊留空
    assert a["resource"] == "redis" and a["share"] == pytest.approx(0.45, abs=0.01)
    assert a["others"] == [{"resource": "postgres", "kind": "db", "subtype": "postgresql",
                            "wait_ms": {"before": 8.0, "during": 96.0}, "failures": {"before": 0, "during": 0},
                            "share": pytest.approx(0.44, abs=0.01)}]
    assert a["share"] + a["others"][0]["share"] < 1.0  # 佔比是對同一個分母（自身耗時增量）算的，加起來不會超過 1


def test_a_dominant_resource_keeps_the_old_struct_and_small_contributors_stay_out():
    # redis +90（佔 ~95%）、postgres +3（~3%，不到 OTHER_RESOURCE_SHARE）：結構體跟以前一樣，沒有 others 這個鍵
    top = _top(_traffic((20.0, [(REDIS, 2.0, False), (PG, 8.0, False)], False),
                        (22.0, [(REDIS, 92.0, False), (PG, 11.0, False)], False)))
    assert top.dependency == "redis（db/redis）" and top.attribution["by"] == "latency"
    assert set(top.attribution) == {"resource", "kind", "subtype", "by", "self_ms", "wait_ms", "failures", "share"}
    assert 3.0 / 95.0 < OTHER_RESOURCE_SHARE


def test_a_resource_that_only_fails_more_is_listed_even_without_extra_wait():
    # redis 變慢是主因；postgres 等待沒變、但事件期間每一筆都回錯：失敗變多的資源也要看得到
    top = _top(_traffic((20.0, [(REDIS, 2.0, False), (PG, 8.0, False)], False),
                        (20.0, [(REDIS, 92.0, False), (PG, 8.0, True)], False)))
    [other] = top.attribution["others"]
    assert other["resource"] == "postgres" and other["share"] == 0.0
    assert other["failures"]["before"] == 0 and other["failures"]["during"] >= 50


def test_briefing_whitelist_accepts_the_other_resources_by_name():
    context = {"incident_meta": {"entities": ["A"]},
               "root_cause": {"candidate": "A", "attribution": {"resource": "redis", "others": [{"resource": "postgres"}]}}}
    assert {"A", "redis", "postgres"} <= briefing_whitelist(context)


# ---------- 缺口 7：資源跟這次事故無關 ----------

def test_resource_seen_only_before_the_incident_does_not_show_up_as_attribution():
    # 事件前每個請求呼叫 redis 2 ms；事件期間程式在入口就崩潰（0.5 ms、HTTP 500），一次都沒呼叫到 redis
    top = _top(_traffic((30.0, [(REDIS, 2.0, False)], False), (0.5, [], True)))
    assert top.attribution == {} and top.dependency == ""
    assert "redis" in top.reason  # 理由的拆解句照舊列出等待變化（2.0→0.0），資訊沒有被藏起來


def test_resource_that_got_faster_is_not_blamed_either():
    # 程式自身 20→60、redis 等待 10→4（變快）：沒有資源變慢也沒有失敗變多
    top = _top(_traffic((20.0, [(REDIS, 10.0, False)], False), (60.0, [(REDIS, 4.0, False)], False)))
    assert top.attribution == {}


def test_failing_resource_without_extra_wait_still_gets_attribution():
    # 等待沒變長、但失敗全在 redis：以前就會填（by=errors），不能被「沒有變慢就不填」誤傷
    top = _top(_traffic((20.0, [(REDIS, 1.0, False)], False), (40.0, [(REDIS, 1.0, True)], True)))
    assert top.attribution["resource"] == "redis" and top.attribution["by"] == "errors"
    assert top.attribution["failures"] == {"before": 0, "during": top.attribution["failures"]["during"]}
    assert top.attribution["failures"]["during"] >= 50


# ---------- 缺口 9：divide_by ----------

MEAN_Q = json.dumps({"index": "metrics-apm.transaction.1m-*", "agg": "sum", "value_field": "transaction.duration.summary.sum",
                     "divide_by": "transaction.duration.summary.value_count", "filter": {"term": {"service.name": "bill-svc"}}})


def test_divide_by_builds_numerator_and_denominator_sums_and_divides_per_bucket():
    q = parse_es_query(MEAN_Q)
    body = build_search_body(q, T0, T0 + 5 * MIN, 60)
    assert body["aggs"]["series"]["aggs"] == {"v": {"sum": {"field": "transaction.duration.summary.sum"}},
                                             "d": {"sum": {"field": "transaction.duration.summary.value_count"}}}
    assert bucket_value(q, {"v": {"value": 114000.0}, "d": {"value": 3.0}}) == pytest.approx(38000.0)
    # 沒有請求的桶：真實 ES 的 sum 回 0.0（不是 null）。分母 0 = 沒資料，不是「延遲 0」
    assert bucket_value(q, {"v": {"value": 0.0}, "d": {"value": 0.0}}) is None
    assert bucket_value(q, {"v": {"value": None}, "d": {"value": None}}) is None
    assert bucket_value(q, {"v": {"value": 5.0}}) is None


def test_query_without_divide_by_is_built_exactly_as_before():
    q = parse_es_query('{"index": "m-*", "agg": "avg", "value_field": "x"}')
    assert build_search_body(q, T0, T0 + MIN, 60)["aggs"]["series"]["aggs"] == {"v": {"avg": {"field": "x"}}}
    assert bucket_value(q, {"v": {"value": 0.0}}) == 0.0


@pytest.mark.parametrize("raw", [
    {"index": "m-*", "agg": "avg", "value_field": "x", "divide_by": "n"},   # 只有 sum 可以除
    {"index": "m-*", "agg": "count", "divide_by": "n"},
    {"index": "m-*", "agg": "p95", "value_field": "x", "divide_by": "n"},
    {"index": "m-*", "agg": "sum", "value_field": "x", "divide_by": " "},
    {"index": "m-*", "agg": "sum", "value_field": "x", "divid_by": "n"},    # 打錯字照舊在載入期擋下
])
def test_divide_by_misuse_is_rejected_at_parse_time(raw):
    with pytest.raises(ValueError, match="Elasticsearch 查詢格式錯誤"):
        parse_es_query(json.dumps(raw))


def _sparse_minutes(seed=7, minutes=4000, rate=0.5, latency_us=38000.0):
    """每分鐘 0～數筆請求的低流量服務（Poisson 0.5）；每筆約 38 ms ± 5%。回 (有請求的分鐘的筆數, 當分鐘總耗時)。"""
    r = np.random.default_rng(seed)
    n = r.poisson(rate, minutes)
    n = n[n > 0]
    total = np.array([float(np.sum(latency_us * (1 + 0.05 * r.standard_normal(k)))) for k in n])
    return n, total


def _frame(values):
    return pd.DataFrame({"ts": pd.date_range(T0, periods=len(values), freq="min"), "value": values})


def test_mean_latency_of_a_sparse_service_does_not_alarm_on_a_three_request_minute():
    n, total = _sparse_minutes()
    assert (n == 3).sum() >= 20  # 3 筆的分鐘並不罕見（這批資料有幾十個）
    mean = total / n
    det = HourlyBaselineDetector().fit(_frame(mean))
    result = det.detect(_frame(mean[n >= 3]))
    assert int(np.sum(result.is_anomaly)) == 0  # 每筆平均：請求數多寡不影響數值
    # 真的變慢（每筆 5 秒）照樣抓得到
    assert bool(det.detect(_frame(np.array([5_000_000.0]))).is_anomaly[0])


@pytest.mark.xfail(strict=True, reason="死區：avg(summary.sum) 是當分鐘總耗時，低流量服務 3 筆的那一分鐘 = 平常 3 倍，逐點偵測器一定判異常。"
                                       "偵測器分不出『請求變多』與『變慢』，只能從查詢下手（改查每筆平均）")
def test_total_duration_of_a_sparse_service_does_not_alarm_on_a_three_request_minute():
    n, total = _sparse_minutes()
    det = HourlyBaselineDetector().fit(_frame(total))
    result = det.detect(_frame(total[n >= 3]))
    assert int(np.sum(result.is_anomaly)) == 0
