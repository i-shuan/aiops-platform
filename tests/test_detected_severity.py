"""演算法判出來的事件群嚴重度（alert.notify_payload 的 detected_severity）。

使用者 2026-11-20 的決定：平台自己的偵測演算法產生的告警一律 P1，人可以在管線 YAML 調整。
外部告警（各團隊的 Grafana 推進來的，沒有偵測器血緣）不受影響，照分數門檻定級。
"""
from datetime import datetime, timezone

import pytest

from aiops.engine.errors import WorkflowError
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import WARNING, AnomalyEvent, Incident

T = datetime(2026, 11, 20, 3, 0, tzinfo=timezone.utc)


def _detected(score: float, level: str = "anomaly") -> AnomalyEvent:
    return AnomalyEvent("web", "latency", T, score, {}, 1.0, "detector.residual_shift", "detect_rshift", level=level)


def _external(score: float) -> AnomalyEvent:
    # 外部匯入的異常帶的血緣是 external:<來源>（storage/hooks 載入時填的），不是平台的偵測器
    return AnomalyEvent("web", "HighLatency", T, score, detector="external:team-a")


def _severity(events, **params) -> str:
    payload = OperatorRegistry.create("alert.notify_payload", params)
    return payload.severity_of(Incident(list(events), score=max(e.score for e in events)))


@pytest.mark.parametrize("score", [0.5, 3.0, 6.0, 10.0, 500.0])
def test_algorithm_cards_are_p1_whatever_the_score(score):
    """演算法的分數是「偏離幾個雜訊單位」：再小也是 P1，再大也不會自己變 P0。"""
    assert _severity([_detected(score)]) == "P1"


@pytest.mark.parametrize("level", ["P0", "P2", "P3"])
def test_the_level_can_be_set_in_the_pipeline_yaml(level):
    assert _severity([_detected(4.0)], detected_severity=level) == level


def test_score_means_algorithm_cards_follow_the_score_thresholds_like_before():
    assert [_severity([_detected(s)], detected_severity="score") for s in (1.0, 4.0, 7.0, 12.0)] == ["P3", "P2", "P1", "P0"]


@pytest.mark.parametrize("score, expected", [(1.0, "P3"), (4.0, "P2"), (7.0, "P1"), (12.0, "P0")])
def test_external_alerts_are_not_touched(score, expected):
    assert _severity([_external(score)]) == expected


def test_mixed_card_takes_the_more_severe_of_the_two_and_ignores_the_algorithm_score():
    assert _severity([_detected(4.0), _external(12.0)]) == "P0"   # 外部告警自己就是 P0
    assert _severity([_detected(4.0), _external(1.0)]) == "P1"    # 外部只有 P3：演算法那一半撐到 P1
    assert _severity([_detected(500.0), _external(1.0)]) == "P1"  # 演算法分數再高也不會把外部那一半墊成 P0
    assert _severity([_detected(500.0), _external(1.0)], detected_severity="P3") == "P3"


def test_a_warning_only_card_stays_a_warning():
    assert _severity([_detected(2.0, level=WARNING)]) == WARNING


@pytest.mark.parametrize("bad", ["p1", "P4", "", "high"])
def test_a_misspelled_level_fails_when_the_pipeline_is_loaded(bad):
    with pytest.raises(WorkflowError, match="detected_severity"):
        OperatorRegistry.create("alert.notify_payload", {"detected_severity": bad})


# ── 序列自己訂的等級（Grafana 告警規則的 severity 標籤 -> target attrs -> 事件 attrs）──

def _labelled(severity, score=4.0, entity="web", url=None) -> AnomalyEvent:
    attrs = {"severity": severity} if severity is not None else {}
    if url:
        attrs["grafana_rule_url"] = url
    return AnomalyEvent(entity, "rule-uid", T, score, attrs, 1.0, "detector.residual_shift", "detect_shift")


@pytest.mark.parametrize("label, expected", [("P0", "P0"), ("P2", "P2"), ("P3", "P3"), ("p3", "P3"), (None, "P1"),
                                             ("critical", "P1"), ("", "P1")])
def test_the_level_set_on_the_series_wins_over_the_pipeline_default(label, expected):
    assert _severity([_labelled(label)]) == expected


def test_a_card_takes_the_most_severe_of_its_series_and_the_score_never_matters():
    assert _severity([_labelled("P3"), _labelled("P0", entity="db")]) == "P0"
    assert _severity([_labelled("P3", score=900.0), _labelled("P3", entity="db")]) == "P3"
    assert _severity([_labelled("P3"), _labelled(None, entity="db")]) == "P1"                     # 沒訂的那條是預設 P1
    assert _severity([_labelled("P3"), _labelled(None, entity="db")], detected_severity="P3") == "P3"
    assert _severity([_labelled(None, score=12.0)], detected_severity="score") == "P0"             # score：沒訂的照分數
    assert _severity([_labelled("P3", score=12.0)], detected_severity="score") == "P3"             # 訂了的還是照訂的
    assert _severity([_labelled("P3"), _external(12.0)]) == "P0" and _severity([_labelled("P3"), _external(1.0)]) == "P3"


def test_the_card_links_back_to_the_grafana_rule_it_came_from():
    from aiops.engine.context import PipelineContext

    def card(events):
        ctx = PipelineContext(pipeline="line_detect", now=T)
        ctx.incidents, ctx.incidents_built = [Incident(list(events), score=4.0)], True
        return OperatorRegistry.create("alert.notify_payload", {}).execute(ctx).notifications[0]

    one = card([_labelled("P2", url="https://g/alerting/grafana/a/view")])
    assert one.annotations["grafana_rule_url"] == "https://g/alerting/grafana/a/view" and one.labels["severity"] == "P2"
    two = card([_labelled("P2", url="https://g/alerting/grafana/b/view"), _labelled("P2", entity="db", url="https://g/alerting/grafana/a/view")])
    assert two.annotations["grafana_rule_url"] == "https://g/alerting/grafana/a/view https://g/alerting/grafana/b/view"
    assert "grafana_rule_url" not in card([_external(4.0)]).annotations and "grafana_rule_url" not in one.labels

