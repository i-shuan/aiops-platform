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
