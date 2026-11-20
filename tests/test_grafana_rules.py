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
    for payload, message in [({"results": {}}, "沒有 results.A"), ({"results": {"A": {"error": "bad expr"}}}, "bad expr"),
                             ({"results": {"A": {"frames": [{"schema": {"fields": [{"type": "string"}]}, "data": {"values": [["x"]]}}]}}},
                              "不是時序")]:
        with pytest.raises(ValueError, match=message):
            gr.series_from_response(payload)
