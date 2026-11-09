"""深度連結（告警卡片、RCA 報告直達 Grafana 的指標大盤、trace、log）。

鐵律：基底網址只來自環境變數 AIOPS_GRAFANA_URL；沒設就一個連結都不產生、卡片其他內容一字不差（可選能力無害化）。
全部直接用 ctx 測，不經資料庫、不連外。
"""
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from aiops.engine.context import OpenIncident, PipelineContext, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident, Notification
from aiops.stages.notification.links import GRAFANA_URL_ENV, explore_url, lucene_any
from aiops.stages.notification.transport import AlertmanagerTransport

T0 = datetime(2026, 11, 3, 14, 50, tzinfo=timezone.utc)
LINKS = {"dashboard": "d/apm-overview", "var": "service", "pad_minutes": 15,
         "datasources": {"traces": "apm-traces", "logs": "apm-logs"}}


def _ev(entity, minutes=0, score=11.0, **attrs):
    return AnomalyEvent(entity, "latency", T0 + timedelta(minutes=minutes), score, attrs)


def _payload(*events, now=T0 + timedelta(hours=1), **params):
    ctx = PipelineContext(pipeline="it_otel", now=now)
    ctx.anomalies = list(events)
    ctx = OperatorRegistry.create("alert.same_entity", {"window_minutes": 60}).execute(ctx)
    return OperatorRegistry.create("alert.notify_payload", {"links": LINKS, **params}).execute(ctx)


def _ms(dt):
    return str(int(dt.timestamp() * 1000))


def test_without_grafana_url_nothing_changes(monkeypatch):
    monkeypatch.delenv(GRAFANA_URL_ENV, raising=False)
    ctx = _payload(_ev("cart-api", 0), _ev("cart-api", 30))
    [n] = ctx.notifications
    assert ctx.grafana_links is None and n.generator_url is None
    assert not [k for k in n.annotations if k.startswith("grafana_")]
    assert "generatorURL" not in AlertmanagerTransport("http://am").to_body([n])[0]


def test_card_links_lock_service_and_incident_window(monkeypatch):
    monkeypatch.setenv(GRAFANA_URL_ENV, "https://grafana.example.com/")
    ctx = _payload(_ev("cart-api", 0), _ev("cart-api", 30))
    [n] = ctx.notifications
    dash = urlparse(n.annotations["grafana_dashboard_url"])
    assert dash.scheme == "https" and dash.netloc == "grafana.example.com" and dash.path == "/d/apm-overview"
    q = parse_qs(dash.query)
    assert q["var-service"] == ["cart-api"]
    assert q["from"] == [_ms(T0 - timedelta(minutes=15))] and q["to"] == [_ms(T0 + timedelta(minutes=45))]
    assert n.generator_url == n.annotations["grafana_dashboard_url"]
    assert AlertmanagerTransport("http://am").to_body([n])[0]["generatorURL"] == n.generator_url

    for name, uid in LINKS["datasources"].items():
        explore = urlparse(n.annotations[f"grafana_{name}_url"])
        assert explore.path == "/explore"
        panes = json.loads(unquote(parse_qs(explore.query)["panes"][0]))["aiops"]
        assert panes["datasource"] == uid and panes["queries"][0]["datasource"] == {"type": "elasticsearch", "uid": uid}
        assert panes["queries"][0]["query"] == 'service.name:("cart-api")'
        assert panes["range"] == {"from": _ms(T0 - timedelta(minutes=15)), "to": _ms(T0 + timedelta(minutes=45))}
    # labels 一個都沒多：連結只在 annotations，不影響 Alertmanager 辨識同一則告警
    assert set(n.labels) == {"alertname", "incident_uid", "severity", "level", "preset", "dept", "entities"}


def test_multi_entity_card_lists_every_service(monkeypatch):
    monkeypatch.setenv(GRAFANA_URL_ENV, "http://grafana:3000")
    ctx = _payload(_ev("bill-svc", 0), _ev("cart-api", 1))
    ctx.incidents = [Incident.combine(ctx.incidents)]
    ctx.notifications.clear()
    ctx = OperatorRegistry.create("alert.notify_payload", {"links": LINKS}).execute(ctx)
    [n] = ctx.notifications
    assert parse_qs(urlparse(n.annotations["grafana_dashboard_url"]).query)["var-service"] == ["bill-svc", "cart-api"]
    panes = json.loads(unquote(parse_qs(urlparse(n.annotations["grafana_traces_url"]).query)["panes"][0]))["aiops"]
    assert panes["queries"][0]["query"] == 'service.name:("bill-svc" OR "cart-api")'


def test_heartbeat_resend_and_resolve_keep_the_links(monkeypatch):
    monkeypatch.setenv(GRAFANA_URL_ENV, "http://grafana:3000")
    inc = Incident([_ev("cart-api", 0)])
    opened = OpenIncident(1, inc.uid, "it_otel", "P1", inc)
    ctx = PipelineContext(pipeline="it_otel", now=T0 + timedelta(minutes=10))
    ctx.open_incidents = [opened]
    ctx = OperatorRegistry.create("alert.notify_payload", {"links": LINKS}).execute(ctx)
    ctx = OperatorRegistry.create("alert.heartbeat", {"resolve_after_minutes": 15}).execute(ctx)
    [beat] = ctx.notifications
    assert beat.ends_at is None and beat.annotations["grafana_dashboard_url"].startswith("http://grafana:3000/d/apm-overview?")
    done = PipelineContext(pipeline="it_otel", now=T0 + timedelta(minutes=40))
    done.open_incidents = [opened]
    done = OperatorRegistry.create("alert.notify_payload", {"links": LINKS}).execute(done)
    done = OperatorRegistry.create("alert.heartbeat", {"resolve_after_minutes": 15}).execute(done)
    [resolved] = done.notifications
    assert resolved.ends_at is not None and resolved.labels == beat.labels
    assert resolved.annotations["grafana_dashboard_url"] == beat.annotations["grafana_dashboard_url"]  # 結案通知也帶，事後回看


def test_payload_without_links_param_ignores_grafana_url(monkeypatch):
    monkeypatch.setenv(GRAFANA_URL_ENV, "http://grafana:3000")
    ctx = PipelineContext(pipeline="factory", now=T0)
    ctx.anomalies = [_ev("LITHO-01")]
    ctx = OperatorRegistry.create("alert.same_entity", {}).execute(ctx)
    ctx = OperatorRegistry.create("alert.notify_payload", {}).execute(ctx)
    assert ctx.grafana_links is None and not [k for k in ctx.notifications[0].annotations if k.startswith("grafana_")]


def test_links_are_rebuilt_every_round_from_the_environment(monkeypatch):
    """常駐進程：環境變數在兩輪之間被拿掉，下一輪就不該再帶連結（旗標掛在每輪新建的 ctx 上，不是算子）。"""
    op = OperatorRegistry.create("alert.notify_payload", {"links": LINKS})
    monkeypatch.setenv(GRAFANA_URL_ENV, "http://grafana:3000")
    first = PipelineContext(pipeline="it_otel", now=T0)
    first.anomalies = [_ev("cart-api")]
    op.execute(OperatorRegistry.create("alert.same_entity", {}).execute(first))
    assert first.grafana_links is not None
    monkeypatch.delenv(GRAFANA_URL_ENV)
    second = PipelineContext(pipeline="it_otel", now=T0)
    second.anomalies = [_ev("cart-api")]
    op.execute(OperatorRegistry.create("alert.same_entity", {}).execute(second))
    assert second.grafana_links is None and "grafana_dashboard_url" not in second.notifications[0].annotations


def test_lucene_escapes_quotes_and_backslashes():
    assert lucene_any("service.name", ['a"b', "c\\d"]) == 'service.name:("a\\"b" OR "c\\\\d")'
    url = explore_url("http://g", "ds", lucene_any("service.name", ['a"b']), T0, T0)
    assert json.loads(unquote(parse_qs(urlparse(url).query)["panes"][0]))["aiops"]["queries"][0]["query"] == 'service.name:("a\\"b")'


def _trace_ctx():
    ctx = PipelineContext(pipeline="rca_report", now=T0 + timedelta(hours=1))
    ctx.incidents = [Incident([_ev("cart-api", 0), _ev("cart-api", 5)])]
    ctx.incidents_built = True
    ts = T0 + timedelta(minutes=2)
    ctx.rca_spans = [SpanRecord("t-slow", "s1", None, "api-gw", ts, 900.0), SpanRecord("t-slow", "s2", "s1", "cart-api", ts, 850.0),
                     SpanRecord("t-fast", "s3", None, "api-gw", ts, 50.0)]
    return ctx


@pytest.mark.parametrize("env, ds, expect_link", [("http://grafana:3000", "apm-traces", True), (None, "apm-traces", False),
                                                  ("http://grafana:3000", None, False)])
def test_rca_trace_section_links_the_slowest_trace_only_when_configured(monkeypatch, env, ds, expect_link):
    if env:
        monkeypatch.setenv(GRAFANA_URL_ENV, env)
    else:
        monkeypatch.delenv(GRAFANA_URL_ENV, raising=False)
    ctx = OperatorRegistry.create("rca.section.trace_evidence", {"traces_datasource": ds}).execute(_trace_ctx())
    text = ctx.rca_sections[0].body
    assert "t-slow" in text
    links = [line.strip() for line in text.splitlines() if line.strip().startswith("http://grafana:3000/explore?")]
    assert bool(links) is expect_link
    if expect_link:
        panes = json.loads(unquote(parse_qs(urlparse(links[0]).query)["panes"][0]))["aiops"]
        assert panes["datasource"] == "apm-traces" and panes["queries"][0]["query"] == 'trace.id:"t-slow"'


def test_notification_dataclass_default_has_no_generator_url():
    n = Notification({"alertname": "x"}, {}, T0)
    assert n.generator_url is None and "generatorURL" not in AlertmanagerTransport("http://am").to_body([n])[0]
