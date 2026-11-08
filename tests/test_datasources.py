"""真實資料源 Spec：用 httpx.MockTransport 模擬伺服器，涵蓋成功與各種錯誤的分工。"""
import json
from datetime import datetime, timezone

import httpx
import pytest

from aiops.datasource_specs import check_sources, get_datasource_spec
from aiops.datasource_specs.base import InvalidQueryError, NoDataError
from aiops.datasource_specs.elasticsearch import ElasticsearchSpec
from aiops.datasource_specs.prometheus import PrometheusSpec, VictoriaMetricsSpec
from aiops.infrastructure.errors import InfrastructureError
from aiops.metric_query.elasticsearch import parse_es_query
from aiops.metric_query.promql import validate_promql
from aiops.metric_query.window import align_window

START = datetime(2026, 11, 8, 0, 0, 30, tzinfo=timezone.utc)
END = datetime(2026, 11, 8, 0, 10, 45, tzinfo=timezone.utc)


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def _prom_ok(series):
    return httpx.Response(200, json={"status": "success", "data": {"resultType": "matrix", "result": series}})


# ── metric_query ──


def test_align_window_floors_to_step():
    s, e = align_window(START, END, 60)
    assert (s.isoformat(), e.isoformat()) == ("2026-11-08T00:00:00+00:00", "2026-11-08T00:10:00+00:00")


@pytest.mark.parametrize("bad", ["", "sum(rate(x[5m])", "cpu[5m]", 'up{job="a}'])
def test_promql_rejects_obvious_mistakes(bad):
    with pytest.raises(ValueError):
        validate_promql(bad)


def test_promql_accepts_normal_queries():
    assert validate_promql(' sum by (svc) (rate(http_requests_total{code=~"5.."}[5m])) ').startswith("sum")


def test_es_query_needs_value_field_unless_count():
    with pytest.raises(ValueError):
        parse_es_query('{"index": "m-*", "agg": "avg"}')
    assert parse_es_query('{"index": "m-*", "agg": "count"}').agg == "count"


# ── Prometheus／VictoriaMetrics ──


def test_prometheus_single_series_to_dataframe():
    seen = {}

    def handler(req):
        seen.update(path=req.url.path, params=dict(req.url.params), auth=req.headers.get("authorization"))
        return _prom_ok([{"metric": {}, "values": [[1762560000, "1.5"], [1762560060, "NaN"], [1762560120, "3"]]}])

    df = PrometheusSpec("http://prom:9090/", token="t", client=_client(handler)).fetch_data("up", START, END, 60)
    assert seen["path"] == "/api/v1/query_range" and seen["auth"] == "Bearer t"
    assert seen["params"]["start"] == str(align_window(START, END, 60)[0].timestamp())
    assert df["value"].tolist() == [1.5, 3.0]  # NaN 丟掉
    assert str(df["ts"].dt.tz) == "UTC"


def test_prometheus_multiple_series_is_invalid_query():
    two = [{"metric": {"a": "1"}, "values": [[1, "1"]]}, {"metric": {"a": "2"}, "values": [[1, "1"]]}]
    spec = PrometheusSpec("http://prom", client=_client(lambda r: _prom_ok(two)))
    with pytest.raises(InvalidQueryError, match="2 條序列"):
        spec.fetch_data("up", START, END, 60)


def test_prometheus_empty_result_is_no_data():
    spec = PrometheusSpec("http://prom", client=_client(lambda r: _prom_ok([])))
    with pytest.raises(NoDataError):
        spec.fetch_data("up", START, END, 60)


def test_prometheus_bad_query_is_invalid_query():
    resp = httpx.Response(400, json={"status": "error", "errorType": "bad_data", "error": "parse error"})
    spec = PrometheusSpec("http://prom", client=_client(lambda r: resp))
    with pytest.raises(InvalidQueryError, match="parse error"):
        spec.fetch_data("up", START, END, 60)


@pytest.mark.parametrize("status", [401, 403, 500, 503])
def test_prometheus_auth_or_server_errors_are_infrastructure(status):
    spec = PrometheusSpec("http://prom", client=_client(lambda r: httpx.Response(status, text="nope")))
    with pytest.raises(InfrastructureError):
        spec.fetch_data("up", START, END, 60)


def test_prometheus_unreachable_is_infrastructure():
    def boom(req):
        raise httpx.ConnectError("refused", request=req)

    with pytest.raises(InfrastructureError, match="連不上"):
        PrometheusSpec("http://prom", client=_client(boom)).fetch_data("up", START, END, 60)


def test_victoriametrics_uses_its_own_health_path():
    paths = []

    def handler(req):
        paths.append(req.url.path)
        return httpx.Response(200, text="OK")

    assert VictoriaMetricsSpec("http://vm:8428", client=_client(handler)).check()["ok"] is True
    assert paths == ["/health"]


# ── Elasticsearch ──


ES_QUERY = json.dumps({"index": "metrics-*", "value_field": "cpu.pct", "agg": "avg",
                       "filter": {"term": {"service.name": "cart"}}})


def test_es_date_histogram_to_dataframe():
    seen = {}

    def handler(req):
        seen.update(path=req.url.path, body=json.loads(req.content), auth=req.headers.get("authorization"))
        return httpx.Response(200, json={"aggregations": {"series": {"buckets": [
            {"key": 1762560000000, "doc_count": 3, "v": {"value": 0.5}},
            {"key": 1762560060000, "doc_count": 0, "v": {"value": None}},
            {"key": 1762560120000, "doc_count": 2, "v": {"value": 0.9}},
        ]}}})

    df = ElasticsearchSpec("http://es:9200", api_key="k", client=_client(handler)).fetch_data(ES_QUERY, START, END, 60)
    assert seen["path"] == "/metrics-*/_search" and seen["auth"] == "ApiKey k"
    assert seen["body"]["aggs"]["series"]["date_histogram"]["fixed_interval"] == "60s"
    assert {"term": {"service.name": "cart"}} in seen["body"]["query"]["bool"]["filter"]
    assert df["value"].tolist() == [0.5, 0.9]  # 空格子丟掉


def test_es_count_and_percentile():
    buckets = [{"key": 1762560000000, "doc_count": 7, "v": {"values": {"95.0": 120.0}}}]
    spec = ElasticsearchSpec("http://es", client=_client(lambda r: httpx.Response(
        200, json={"aggregations": {"series": {"buckets": buckets}}})))
    assert spec.fetch_data('{"index": "m", "agg": "count"}', START, END, 60)["value"].tolist() == [7.0]
    assert spec.fetch_data('{"index": "m", "agg": "p95", "value_field": "d"}', START, END, 60)["value"].tolist() == [120.0]


def test_es_missing_index_is_invalid_query():
    resp = httpx.Response(404, json={"error": {"type": "index_not_found_exception", "reason": "no such index [x]"}})
    spec = ElasticsearchSpec("http://es", client=_client(lambda r: resp))
    with pytest.raises(InvalidQueryError, match="no such index"):
        spec.fetch_data('{"index": "x", "agg": "count"}', START, END, 60)


def test_es_bad_query_json_is_invalid_query():
    with pytest.raises(InvalidQueryError):
        ElasticsearchSpec("http://es", client=_client(lambda r: None)).fetch_data("not json", START, END, 60)


# ── 設定與自我檢查 ──


def test_missing_url_is_infrastructure_error(monkeypatch):
    monkeypatch.delenv("AIOPS_PROMETHEUS_URL", raising=False)
    with pytest.raises(InfrastructureError, match="AIOPS_PROMETHEUS_URL"):
        get_datasource_spec("prometheus")


def test_check_sources_marks_unconfigured(monkeypatch):
    for var in ("AIOPS_PROMETHEUS_URL", "AIOPS_VICTORIAMETRICS_URL", "AIOPS_ES_URL"):
        monkeypatch.delenv(var, raising=False)
    by_type = {r["source_type"]: r for r in check_sources()}
    assert by_type["mock"]["ok"] is True
    assert by_type["prometheus"]["configured"] is False and by_type["prometheus"]["ok"] is None


def test_check_sources_reports_unreachable(monkeypatch):
    monkeypatch.setenv("AIOPS_ES_URL", "http://127.0.0.1:1")  # 不會有人在聽
    es = next(r for r in check_sources() if r["source_type"] == "elasticsearch")
    assert es["configured"] is True and es["ok"] is False


def test_source_check_api_and_unconfigured_source_returns_503(client, monkeypatch):
    for var in ("AIOPS_PROMETHEUS_URL", "AIOPS_VICTORIAMETRICS_URL", "AIOPS_ES_URL"):
        monkeypatch.delenv(var, raising=False)
    assert client.get("/api/v1/ingestion/sources/check").json()["result"]["ok"] is True

    body = {"metric": {"source_type": "prometheus", "query": "up"},
            "time_range": {"start": "2026-11-08T00:00:00", "end": "2026-11-08T01:00:00"}}
    assert client.post("/api/v1/detection/run", json=body).status_code == 503
