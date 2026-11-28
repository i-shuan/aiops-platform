"""指標走勢（RCA 的第 7 包證據）。Grafana 的回應是真的 Grafana 13.0.7 → VictoriaMetrics v1.110.0 錄下來的（prom_query_range_*.json，
錄製腳本 record_metrics.py）：RabbitMQ 3.12.1 的一個佇列，正常收送 180 秒後消費者停掉、生產者繼續送，佇列真的堆到一千多筆
（序列第一次存在 Prometheus 3.5.0；指標一律是 VictoriaMetrics 之後，同一批序列寫進 VictoriaMetrics 重錄，數值一樣）。"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import evidence as gather
from src import rca
from src.grafana import Grafana, GrafanaError
from src.grouping import Alert, converge
from src.main import ConfigError, Settings
from tests.conftest import collect_evidence, recorded
from tests.test_main import world  # noqa: F401（fixture）

STOPPED = datetime(2026, 11, 25, 4, 10, 28, tzinfo=timezone.utc)    # 消費者停掉的時間
FINISHED = datetime(2026, 11, 25, 4, 12, 18, tzinfo=timezone.utc)
EDGES = [("cim-eap", "mq-broker"), ("mq-broker", "amhs-mcs")]
READY = gather.MetricQuery("佇列待處理訊息數", 'sum by (queue) (rabbitmq_queue_messages_ready{service="$service"})')
CONSUMERS = gather.MetricQuery("佇列的消費者數", 'sum by (queue) (rabbitmq_queue_consumers{service="$service"})')
BROKEN = gather.MetricQuery("寫錯的查詢", 'sum(rate(rabbitmq_queue_messages_ready{service="$service"}[1m)')
MINIMAL = "grafana: {url: 'http://g', token: 't'}\nwebhook: {token: 'w'}\n"


def incident(*services):
    alerts = [Alert(f"f{i}", "firing", f"r{i}", f"{s} 告警", s, "P2", STOPPED + timedelta(seconds=30 + i), None, {"A": 1})
              for i, s in enumerate(services or ("mq-broker", "amhs-mcs"))]
    [inc] = converge([], alerts, [], EDGES, now=FINISHED, close_after_seconds=300)
    inc.started_at = STOPPED   # 基準與事故期間的分界對齊實際停掉的時間，下面的數字才好核對
    return inc


def collect(fake, *queries, inc=None, **options):
    grafana = Grafana("http://grafana.test", "t", transport=fake.transport, retries=0)
    return collect_evidence(inc or incident(), EDGES, grafana, logs_datasource_uid="", id_fields=["lot_id"], now=FINISHED,
                                metrics=gather.MetricOptions(datasource_uid="prom-uid", queries=queries, lookback_minutes=5,
                                                          step_seconds=10, **options))


# ── 查詢 ──

def test_query_range_sends_what_was_recorded_and_parses_the_series(fake):
    was = recorded("prom_query_range_queue_ready")["request"]
    grafana = Grafana("http://grafana.test", "t", transport=fake.transport)
    series = grafana.query_range("prom-uid", was["params"]["query"], datetime.fromtimestamp(int(was["params"]["start"]), timezone.utc),
                                 datetime.fromtimestamp(int(was["params"]["end"]), timezone.utc), 10)
    sent = fake.metric_requests[0]
    assert sent.url.path == was["path"] and dict(sent.url.params) == was["params"]   # 送出去的跟錄製時一模一樣
    [one] = series
    assert one["labels"] == {"queue": "eap.events"} and len(one["points"]) == 29
    assert one["points"][0][1] == 0 and one["points"][-1][1] > 1000 and one["points"][0][0].tzinfo is not None


def test_a_query_with_a_syntax_error_is_a_grafana_error_that_says_why(fake):
    grafana = Grafana("http://grafana.test", "t", transport=fake.transport, retries=0)
    with pytest.raises(GrafanaError, match=r"HTTP 422.*unexpected token"):   # VictoriaMetrics 回 422 與它自己的錯誤說明
        grafana.query_range("prom-uid", BROKEN.expr, STOPPED, FINISHED, 10)


@pytest.mark.parametrize("body, message", [
    ({"status": "success", "data": {"resultType": "vector", "result": []}}, "matrix"),
    ({"status": "error", "error": "boom"}, "不是預期的格式"),
    ([], "不是預期的格式"),
])
def test_unexpected_metric_responses_are_reported(body, message):
    grafana = Grafana("http://grafana.test", "t", transport=httpx.MockTransport(lambda r: httpx.Response(200, json=body)))
    with pytest.raises(GrafanaError, match=message):
        grafana.query_range("u", "up", STOPPED, FINISHED, 10)


def test_nan_and_infinite_points_are_dropped():
    body = {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {}, "values": [[1795579658, "NaN"], [1795579668, "+Inf"], [1795579678, "3.5"], [1795579688, "-Inf"]]}]}}
    grafana = Grafana("http://grafana.test", "t", transport=httpx.MockTransport(lambda r: httpx.Response(200, json=body)))
    assert [v for _t, v in grafana.query_range("u", "a/b", STOPPED, FINISHED, 10)[0]["points"]] == [3.5]


# ── 壓成給 LLM 看的走勢 ──

def test_real_backlog_is_summarized_as_baseline_then_growth(fake):
    ev = collect(fake, READY, CONSUMERS)
    ready, consumers = ev["metrics"]
    assert (ready["system"], ready["metric"], ready["labels"]) == ("mq-broker", "佇列待處理訊息數", {"queue": "eap.events"})
    assert ready["baseline"] == 0 and ready["during"]["last"] > 1000 and ready["during"]["min"] <= ready["during"]["max"]
    assert "last_vs_baseline" not in ready            # 基準是 0：不算倍數（除以零沒有意義）
    # 停掉的那一秒抓到的值還是 1（抓取有幾秒延遲），之後才變 0：照實呈現，不修飾
    assert consumers["baseline"] == 1 and (consumers["during"]["min"], consumers["during"]["last"]) == (0, 0)
    assert consumers["last_vs_baseline"] == 0
    assert len(ready["points"]) <= 12 + 1 and ready["points"][-1]["v"] == ready["during"]["last"]   # 最後一個點一定留
    assert not any("指標" in note for note in ev["notes"])
    assert len(fake.metric_requests) == 4             # 兩條查詢 × 兩個系統；amhs-mcs 沒有資料，不列也不算錯
    prompt = rca.build_prompt(ev)
    assert "【7. 指標走勢" in prompt and "佇列的消費者數" in prompt and "不代表因果" in prompt


def test_series_summary_arithmetic():
    t0 = datetime(2026, 11, 25, 1, 0, tzinfo=timezone.utc)
    points = [(t0 + timedelta(minutes=i), 10.0 if i < 30 else 10.0 + (i - 30) * 20) for i in range(40)]
    got = gather.summarize_series(points, t0 + timedelta(minutes=30), 6)
    assert got["baseline"] == 10 and got["during"] == {"min": 10, "max": 190, "last": 190, "max_at": "01:39:00Z"}
    assert got["last_vs_baseline"] == 19 and got["max_vs_baseline"] == 19
    assert [p["t"] for p in got["points"]][0] == "01:00:00" and got["points"][-1] == {"t": "01:39:00", "v": 190}
    only_during = gather.summarize_series(points[30:], t0 + timedelta(minutes=30), 6)
    assert only_during["baseline"] is None and "last_vs_baseline" not in only_during   # 事故前沒有資料：照實給 None
    only_before = gather.summarize_series(points[:30], t0 + timedelta(minutes=30), 6)
    assert only_before["baseline"] == 10 and "during" not in only_before
    assert gather.summarize_series([], t0, 6) is None


def test_the_summary_has_no_threshold_a_flat_series_is_reported_as_flat():
    """這一包不判斷「算不算異常」：沒變化的數值照樣列出來，倍數是 1。判斷交給 Grafana 規則與 LLM。"""
    t0 = datetime(2026, 11, 25, 1, 0, tzinfo=timezone.utc)
    got = gather.summarize_series([(t0 + timedelta(minutes=i), 42.0) for i in range(20)], t0 + timedelta(minutes=10), 5)
    assert got["baseline"] == 42 and got["last_vs_baseline"] == 1 and got["max_vs_baseline"] == 1


# ── 失敗與邊界 ──

def test_one_broken_query_does_not_stop_the_others_or_the_analysis(fake):
    ev = collect(fake, BROKEN, READY)
    assert [m["metric"] for m in ev["metrics"]] == ["佇列待處理訊息數"]
    assert any("指標查詢失敗：寫錯的查詢（" in note for note in ev["notes"])


def test_metrics_datasource_down_leaves_a_note_and_the_rest_of_the_evidence(fake):
    fake.fail.add("/api/datasources/proxy/uid/prom-uid/")
    ev = collect(fake, READY)
    assert ev["metrics"] == [] and ev["anomalies"] and any("指標查詢失敗" in note for note in ev["notes"])
    assert "【7." not in rca.build_prompt(ev)


def test_queries_that_return_nothing_say_so(fake):
    ev = collect(fake, READY, inc=incident("amhs-mcs"))
    assert ev["metrics"] == [] and "設定的指標查詢在這段時間都沒有資料" in ev["notes"]


def test_a_query_limited_to_some_services_is_not_run_for_the_others(fake):
    collect(fake, replace(READY, services=("mq-broker",)), CONSUMERS)
    asked = [(r.url.params["query"].split("(rabbitmq_")[1].split("{")[0], r.url.params["query"].split('service="')[1].split('"')[0])
             for r in fake.metric_requests]
    assert sorted(asked) == [("queue_consumers", "amhs-mcs"), ("queue_consumers", "mq-broker"), ("queue_messages_ready", "mq-broker")]


def test_many_series_keep_only_the_largest_and_say_how_many_were_dropped():
    def handle(request):
        values = lambda peak: [[1795579658 + i * 10, str(peak if i > 15 else 1)] for i in range(29)]
        return httpx.Response(200, json={"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {"queue": f"q{n}"}, "values": values(n * 10)} for n in range(1, 8)]}})

    grafana = Grafana("http://grafana.test", "t", transport=httpx.MockTransport(handle))
    ev = collect_evidence(incident("mq-broker"), EDGES, grafana, logs_datasource_uid="", id_fields=["lot_id"], now=FINISHED,
                              metrics=gather.MetricOptions(datasource_uid="u", queries=(READY,), lookback_minutes=5, step_seconds=10, max_series=3))
    assert [m["labels"]["queue"] for m in ev["metrics"]] == ["q7", "q6", "q5"]
    assert any("有 7 條序列，只列數值最大的 3 條" in note for note in ev["notes"])


@pytest.mark.parametrize("name", ['mq"} or vector(1) #', "a\\b", "line\nbreak", 'x"y'])
def test_service_names_from_alert_labels_cannot_break_out_of_the_query_string(name):
    """系統名稱來自告警的標籤（外部輸入）。跳脫後整段仍然是一個字串：引號前面都有反斜線、沒有裸的換行。"""
    escaped = gather.promql_string(name)
    assert "\n" not in escaped
    for i, ch in enumerate(escaped):
        if ch == '"':
            backslashes = len(escaped[:i]) - len(escaped[:i].rstrip("\\"))
            assert backslashes % 2 == 1, escaped


def test_without_metrics_settings_nothing_is_queried(fake):
    ev = collect(fake)
    assert ev["metrics"] == [] and fake.metric_requests == [] and not any("指標" in n for n in ev["notes"])
    no_uid = collect_evidence(incident(), EDGES, Grafana("http://grafana.test", "t", transport=fake.transport),
                                  logs_datasource_uid="", id_fields=["lot_id"], now=FINISHED,
                                  metrics=gather.MetricOptions(queries=(READY,)))
    assert no_uid["metrics"] == [] and fake.metric_requests == []


# ── 設定 ──

def load(tmp_path, text):
    path = tmp_path / "config.yaml"
    path.write_text(MINIMAL + text, encoding="utf-8")
    return Settings.load(str(path))


def test_metrics_settings_are_read_from_config(tmp_path):
    s = load(tmp_path, """metrics:
  datasource_uid: vm-uid
  lookback_minutes: 30
  step_seconds: 15
  max_points: 8
  max_series: 2
  queries:
    - {name: 請求量, expr: 'sum(rate(http_requests_total{service="$service"}[5m]))'}
    - {name: 佇列, expr: 'sum by (queue) (rabbitmq_queue_messages_ready{service="$service"})', services: [mq-broker]}
""")
    assert s.metrics == gather.MetricOptions(datasource_uid="vm-uid", lookback_minutes=30, step_seconds=15, max_points=8, max_series=2, queries=(
        gather.MetricQuery("請求量", 'sum(rate(http_requests_total{service="$service"}[5m]))'),
        gather.MetricQuery("佇列", 'sum by (queue) (rabbitmq_queue_messages_ready{service="$service"})', ("mq-broker",))))
    assert load(tmp_path, "").metrics == gather.MetricOptions()


@pytest.mark.parametrize("text, message", [
    ("metrics: {datasource: x}\n", "metrics 有不認得的設定"),
    ("metrics: {queries: [{name: a, expr: 'up'}]}\n", r"沒有 \$service"),
    ("metrics: {queries: [{name: a}]}\n", "都要有 name 與 expr"),
    ("metrics: {queries: [{name: a, expr: 'x{s=\"$service\"}', query: y}]}\n", "metrics.queries 有不認得的設定"),
    ("metrics: {queries: [{name: a, expr: 'x{s=\"$service\"}'}, {name: a, expr: 'y{s=\"$service\"}'}]}\n", "兩條都叫"),
    ("metrics: {queries: [{name: a, expr: 'x{s=\"$service\"}', services: mq}]}\n", "services 要是系統名稱清單"),
    ("metrics: {queries: nope}\n", "要是清單"),
    ("metrics: {queries: [" + ", ".join("{name: q%d, expr: 'x{s=\"$service\"}'}" % i for i in range(21)) + "]}\n", "最多 20 條"),
    ("metrics: {step_seconds: 1}\n", "step_seconds"),
    ("metrics: {lookback_minutes: 0}\n", "lookback_minutes"),
])
def test_bad_metrics_settings_are_rejected_at_startup(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load(tmp_path, text)


# ── 整輪 ──

def test_a_round_with_metrics_configured_sends_the_trend_to_the_llm(world):  # noqa: F811
    w = world
    settings = replace(w.settings, metrics=gather.MetricOptions(datasource_uid="prom-uid", queries=(READY, CONSUMERS)))
    w.receive("firing_mq", "firing_cim")
    summary = w.run(60, settings=settings)
    assert summary["status"] == "ok" and w.fake.metric_requests
    prompt = w.llm_calls[0]["messages"][1]["content"]
    assert "【7. 指標走勢" in prompt and "佇列待處理訊息數" in prompt and '"system": "mq-broker"' in prompt


def test_a_round_without_metrics_configured_is_unchanged(world):  # noqa: F811
    w = world
    w.receive("firing_mq", "firing_cim")
    assert w.run(60)["status"] == "ok" and w.fake.metric_requests == []
    assert "【7." not in w.llm_calls[0]["messages"][1]["content"]
