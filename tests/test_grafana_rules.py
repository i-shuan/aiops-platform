"""Grafana 告警規則 -> 偵測對象與時序（sources/grafana_rules.py）。

fixtures/grafana13/ 是 2026-11-20 從演練叢集的 Grafana 13.0.7 擷取的真實回應（規則清單、一條規則的查詢重放 6 小時），
不是照文件想像出來的形狀。
"""
import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from aiops.engine.pipeline import TargetSpec
from aiops.sources import grafana_rules as gr

FIXTURES = Path(__file__).parent / "fixtures" / "grafana13"
RULES = json.loads((FIXTURES / "alert_rules.json").read_text(encoding="utf-8"))
RESPONSE = json.loads((FIXTURES / "ds_query_range.json").read_text(encoding="utf-8"))
T = datetime(2026, 11, 20, 6, 0, tzinfo=timezone.utc)


def _labelled(uid: str, **labels) -> list[dict]:
    rules = copy.deepcopy(RULES)
    next(r for r in rules if r["uid"] == uid)["labels"].update(labels)
    return rules


def test_only_rules_labelled_for_this_pipeline_become_targets():
    assert gr.detection_targets(RULES, "grafana_rules") == []  # 真實的規則都還沒貼標籤
    rules = _labelled("drill-wip-latency", aiops_detect="grafana_rules", severity="P2")
    rules = [*rules, *_labelled("drill-mes-latency", aiops_detect="another_pipeline")[:1]]
    (target,) = gr.detection_targets(rules, "grafana_rules", "https://grafana.example.internal/")
    assert target == {"source_type": "grafana", "query": "drill-wip-latency", "entity": "rca-wip-worker", "attrs": {
        "grafana_rule_uid": "drill-wip-latency", "grafana_rule_title": "WipWorkerProbeSlow", "severity": "P2",
        "grafana_rule_url": "https://grafana.example.internal/alerting/grafana/drill-wip-latency/view"}}
    TargetSpec(**target)  # 引擎收得下（欄位拼錯會在這裡被 StrictModel 擋下）


@pytest.mark.parametrize("label, expected", [("P0", "P0"), ("p3", "P3"), (" P2 ", "P2"), ("critical", None), ("high", None),
                                             ("P4", None), ("", None)])
def test_severity_is_only_taken_when_it_is_one_of_the_four_levels(label, expected):
    """真實規則上的 severity 是 critical／high 這類給人看的字：不猜它對應哪一級，當成沒寫（用管線的預設）。"""
    (target,) = gr.detection_targets(_labelled("drill-mes-errors", aiops_detect="p", severity=label), "p")
    assert target["attrs"].get("severity") == expected


def test_entity_falls_back_from_aiops_entity_to_service_to_the_rule_title():
    assert gr.rule_entity({"uid": "u", "title": "T", "labels": {"aiops_entity": "a", "service": "s"}}) == "a"
    assert gr.rule_entity({"uid": "u", "title": "T", "labels": {"service": "s"}}) == "s"
    assert gr.rule_entity({"uid": "u", "title": "T", "labels": {}}) == "T"
    assert gr.rule_entity({"uid": "u", "title": "", "labels": None}) == "u"


def test_targets_come_out_in_the_same_order_every_round():
    rules = _labelled("drill-wip-latency", aiops_detect="p")
    rules = _labelled("drill-mes-errors", aiops_detect="p") + rules
    uids = [t["query"] for t in gr.detection_targets(rules, "p")]
    assert uids == ["drill-mes-errors", "drill-wip-latency"]  # 依 uid 排序，不是依 Grafana 回的順序


def test_the_rules_own_query_is_replayed_as_a_range_and_expressions_are_skipped():
    rule = next(r for r in RULES if r["uid"] == "drill-wip-latency")
    body = gr.range_query(rule, T - timedelta(hours=6), T, 60)
    (query,) = body["queries"]
    assert query["expr"] == 'avg_over_time(probe_duration_seconds{service="rca-wip-worker"}[2m])'  # 使用者寫的那一條，一字不改
    assert query["datasource"] == {"uid": "team-prometheus"} and query["refId"] == "A"
    assert (query["instant"], query["range"], query["intervalMs"], query["maxDataPoints"]) == (False, True, 60000, 361)
    assert body["from"] == str(int((T - timedelta(hours=6)).timestamp() * 1000)) and body["to"] == str(int(T.timestamp() * 1000))
    assert rule["data"][0]["model"]["instant"] is True  # 規則本身沒被改到
    with pytest.raises(ValueError, match="只有運算式"):
        gr.range_query({"uid": "only-expr", "data": [d for d in rule["data"] if d["datasourceUid"] == "__expr__"]}, T, T, 60)


def test_a_backend_without_instant_and_range_only_gets_the_common_fields():
    rule = {"uid": "es", "data": [{"refId": "A", "datasourceUid": "logs", "model": {"query": "level:error", "metrics": [{"type": "count"}]}}]}
    (query,) = gr.range_query(rule, T - timedelta(minutes=10), T, 60)["queries"]
    assert "instant" not in query and "range" not in query and query["intervalMs"] == 60000 and query["query"] == "level:error"


def test_the_real_response_parses_into_one_series_with_every_point():
    ((labels, points),) = gr.series_from_response(RESPONSE)
    assert labels == {"instance": "wip-probe.drill-team.svc:9464", "job": "wip-probe", "service": "rca-wip-worker"}
    assert len(points) == 361 and points[0] == (1795153500000, 0.05375)
    assert [b[0] - a[0] for a, b in zip(points, points[1:])] == [60000] * 360  # 每分鐘一點，沒有缺
    assert all(0.0 < v < 5.0 for _, v in points)  # 探測耗時（秒）


def test_nulls_and_non_finite_values_are_dropped_and_broken_responses_say_why():
    frame = {"schema": {"fields": [{"name": "Time", "type": "time"}, {"name": "Value", "type": "number", "labels": {"a": "b"}}]},
             "data": {"values": [[1000, 2000, 3000, 4000], [1.5, None, float("nan"), 2.5]]}}
    assert gr.series_from_response({"results": {"A": {"frames": [frame]}}}) == [({"a": "b"}, [(1000, 1.5), (4000, 2.5)])]
    assert gr.series_from_response({"results": {"A": {"frames": []}}}) == []
    # 實測：查詢區間早於 Prometheus 保留期時，Grafana 回的是「沒有欄位的空 frame」，不是空的 frames 清單
    assert gr.series_from_response({"results": {"A": {"status": 200, "frames": [{"schema": {"fields": []}, "data": {"values": []}}]}}}) == []
    for payload, message in [({"results": {}}, "沒有 results.A"), ({"results": {"A": {"error": "bad expr"}}}, "bad expr"),
                             ({"results": {"A": {"frames": [{"schema": {"fields": [{"type": "string"}]}, "data": {"values": [["x"]]}}]}}},
                              "不是時序")]:
        with pytest.raises(ValueError, match=message):
            gr.series_from_response(payload)


# ── 連線那一半（sources/grafana.GrafanaSpec）：用假的 Grafana，回的是上面那兩份真實回應 ──

def _grafana(handler):
    import httpx
    from aiops.sources.grafana import GrafanaSpec
    seen = []

    def record(request):
        seen.append((request.method, request.url.path, request.content))
        return handler(request)

    return GrafanaSpec("http://grafana.test", token="t", client=httpx.Client(transport=httpx.MockTransport(record))), seen


def _real(request):
    import httpx
    path = request.url.path
    if path == "/api/v1/provisioning/alert-rules":
        return httpx.Response(200, json=_labelled("drill-wip-latency", aiops_detect="grafana_detect", severity="P2"))
    if path.startswith("/api/v1/provisioning/alert-rules/"):
        rule = next((r for r in RULES if r["uid"] == path.rsplit("/", 1)[1]), None)
        return httpx.Response(200, json=rule) if rule else httpx.Response(404, json={"message": "not found"})
    if path == "/api/ds/query":
        return httpx.Response(200, json=RESPONSE)
    return httpx.Response(500)


def test_targets_are_read_from_grafana_with_a_link_back_to_the_rule(monkeypatch):
    monkeypatch.setenv("AIOPS_GRAFANA_PUBLIC_URL", "https://grafana.example.internal")
    spec, seen = _grafana(_real)
    (target,) = spec.detection_targets("grafana_detect")
    assert (target["query"], target["entity"], target["attrs"]["severity"]) == ("drill-wip-latency", "rca-wip-worker", "P2")
    assert target["attrs"]["grafana_rule_url"] == "https://grafana.example.internal/alerting/grafana/drill-wip-latency/view"
    assert [(m, p) for m, p, _ in seen] == [("GET", "/api/v1/provisioning/alert-rules")]
    monkeypatch.delenv("AIOPS_GRAFANA_PUBLIC_URL")
    assert spec.detection_targets("grafana_detect")[0]["attrs"]["grafana_rule_url"].startswith("http://grafana.test/")


def test_a_rule_uid_is_fetched_as_a_series_through_grafana():
    spec, seen = _grafana(_real)
    df = spec.fetch_data("drill-wip-latency", T - timedelta(hours=6), T, 60)
    assert list(df.columns) == ["ts", "value"] and len(df) == 361 and str(df.ts.dt.tz) == "UTC"
    assert [(m, p) for m, p, _ in seen] == [("GET", "/api/v1/provisioning/alert-rules/drill-wip-latency"), ("POST", "/api/ds/query")]
    sent = json.loads(seen[1][2])
    assert sent["queries"][0]["expr"].startswith("avg_over_time(") and sent["queries"][0]["range"] is True


def test_a_long_window_is_asked_for_in_pieces_because_grafana_silently_widens_the_step():
    """實測（Grafana 13.0.7＋Prometheus）：一次要 14 天、每 60 秒一點（20161 點），Grafana 不報錯，回來是每 120 秒一點。
    所以一次最多要 QUERY_MAX_POINTS 點，分段要、接起來。"""
    from aiops.sources.grafana import QUERY_MAX_POINTS
    import httpx

    def chunked(request):
        if request.url.path != "/api/ds/query":
            return _real(request)
        body = json.loads(request.content)
        lo, hi = int(body["from"]), int(body["to"])
        assert (hi - lo) // 60000 <= QUERY_MAX_POINTS
        stamps = list(range(lo, hi + 1, 60000))
        frame = {"schema": {"fields": [{"name": "Time", "type": "time"}, {"name": "Value", "type": "number", "labels": {}}]},
                 "data": {"values": [stamps, [1.0] * len(stamps)]}}
        return httpx.Response(200, json={"results": {"A": {"status": 200, "frames": [frame]}}})

    spec, seen = _grafana(chunked)
    df = spec.fetch_data("drill-wip-latency", T - timedelta(days=14), T, 60)
    assert len(df) == 14 * 1440 + 1 and (df.ts.diff().dropna().dt.total_seconds() == 60).all()  # 每分鐘一點、接縫沒有重複
    assert sum(1 for m, p, _ in seen if p == "/api/ds/query") == 5


def test_bad_rules_and_bad_answers_are_query_errors_and_an_empty_answer_is_no_data():
    import httpx
    from aiops.engine.errors import InfrastructureError, NoDataError
    from aiops.sources.base import InvalidQueryError

    def answer(payload, status=200):
        return lambda request: _real(request) if request.url.path != "/api/ds/query" else httpx.Response(status, json=payload)

    two = copy.deepcopy(RESPONSE)
    two["results"]["A"]["frames"] *= 2
    with pytest.raises(InvalidQueryError, match="回了 2 條序列"):
        _grafana(answer(two))[0].fetch_data("drill-wip-latency", T - timedelta(hours=1), T, 60)
    with pytest.raises(InvalidQueryError, match="bad_data"):
        _grafana(answer({"results": {"A": {"error": "bad_data: parse error", "status": 400}}}, 400))[0].fetch_data(
            "drill-wip-latency", T - timedelta(hours=1), T, 60)
    with pytest.raises(NoDataError):
        _grafana(answer({"results": {"A": {"status": 200, "frames": []}}}))[0].fetch_data("drill-wip-latency", T - timedelta(hours=1), T, 60)
    with pytest.raises(InvalidQueryError, match="找不到告警規則"):
        _grafana(_real)[0].fetch_data("deleted-rule", T - timedelta(hours=1), T, 60)
    with pytest.raises(InfrastructureError, match="讀告警規則失敗"):
        _grafana(lambda request: httpx.Response(200, json={"message": "not a list"}))[0].alert_rules()


# ── 接進排程：Workflow.targets_from 與嚴重度 ──

def test_a_pipeline_cannot_have_both_its_own_targets_and_targets_from_grafana():
    from pydantic import ValidationError
    from aiops.engine.pipeline import Workflow
    nodes = [{"name": "fetch", "operator": "ingestion.fetch"}]
    assert Workflow(name="w", nodes=nodes, targets_from="grafana").targets_from == "grafana"
    with pytest.raises(ValidationError, match="只能擇一"):
        Workflow(name="w", nodes=nodes, targets_from="grafana", targets=[{"source_type": "grafana", "query": "u"}])
    with pytest.raises(ValidationError):
        Workflow(name="w", nodes=nodes, targets_from="prometheus")


def test_the_scheduled_run_asks_grafana_for_its_targets_and_a_caller_can_still_pass_its_own(monkeypatch):
    from aiops.interfaces import runner
    asked = []
    monkeypatch.setattr(runner, "grafana_targets", lambda pipeline: asked.append(pipeline) or [
        {"source_type": "grafana", "query": "u1", "entity": "mq-broker", "attrs": {"severity": "P0"}}])
    pipeline = runner.build_pipeline("grafana_detect")
    (target,) = runner.resolve_targets(pipeline, None)
    assert asked == ["grafana_detect"] and (target.source_type, target.query, target.attrs) == ("grafana", "u1", {"severity": "P0"})
    assert runner.resolve_targets(pipeline, []) == [] and asked == ["grafana_detect"]          # 呼叫端給了就不問 Grafana
    assert runner.resolve_targets(runner.build_pipeline("it_otel"), None) is None            # 沒寫 targets_from 的管線不問


def test_grafana_being_unreachable_fails_the_round_instead_of_looking_like_nothing_to_watch(monkeypatch):
    from aiops.engine.errors import InfrastructureError
    from aiops.interfaces import runner
    for name in ("AIOPS_GRAFANA_URL", "AIOPS_GRAFANA_TOKEN", "AIOPS_GRAFANA_USER", "AIOPS_GRAFANA_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(InfrastructureError):
        runner.run_scheduled("grafana_detect")


def test_aiops_severity_wins_over_severity_so_one_rule_can_serve_both_grafana_and_the_platform():
    """演練環境的真實規則：severity 是 critical／high（Grafana 自己發告警時給接收端對照用）。平台要的等級另外貼 aiops_severity。"""
    (target,) = gr.detection_targets(_labelled("drill-mes-errors", aiops_detect="p", aiops_severity="P2"), "p")
    assert target["attrs"]["severity"] == "P2"                       # severity: critical 不是四級之一，不影響
    (target,) = gr.detection_targets(_labelled("drill-mes-errors", aiops_detect="p", aiops_severity="P3", severity="P0"), "p")
    assert target["attrs"]["severity"] == "P3"                       # 兩個都是四級之一：aiops_severity 優先
    (target,) = gr.detection_targets(_labelled("drill-mes-errors", aiops_detect="p", aiops_severity="urgent", severity="P0"), "p")
    assert target["attrs"]["severity"] == "P0"                       # aiops_severity 寫錯：退回 severity

