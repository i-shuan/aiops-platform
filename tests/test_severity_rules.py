"""嚴重度規則（engine/types.SeverityRule）：平台演算法判出來的哪一類異常是哪個嚴重度。

使用者 2026-11-20 的決定：演算法判出來的告警預設 P1；等級由使用者在 Grafana 上改（告警規則的標籤），平台不另外存、
不做自己的設定頁。這裡只測引擎那一半：規則怎麼比對、卡片怎麼定級。規則只改「多大聲」，不改偵測；
外部告警（各團隊的 Grafana 推進來的）不受影響。
"""
from datetime import datetime, timezone


from aiops.engine.context import PipelineContext
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident, SeverityRule, severity_rule_for

T = datetime(2026, 11, 20, 3, 0, tzinfo=timezone.utc)
SHIFT, CHRONIC = "detector.residual_shift", "detector.residual_chronic"


def _detected(entity="web", metric="latency", detector=SHIFT, score=4.0) -> AnomalyEvent:
    return AnomalyEvent(entity, metric, T, score, {}, 1.0, detector, "node")


def _external(score: float) -> AnomalyEvent:
    # 外部匯入的異常帶的血緣是 external:<來源>（storage/hooks 載入時填的），不是平台的偵測器
    return AnomalyEvent("web", "HighLatency", T, score, detector="external:team-a")


def _severity(events, rules, pipeline="p", **params) -> str:
    """走一輪 alert.notify_payload：規則由 ctx.severity_rules 帶進去，回傳事件群最後定的嚴重度。"""
    ctx = PipelineContext(pipeline=pipeline, now=T)
    ctx.incidents = [Incident(list(events), score=max(e.score for e in events))]
    ctx.incidents_built = True
    ctx.severity_rules = list(rules)
    OperatorRegistry.create("alert.notify_payload", params).execute(ctx)
    return ctx.incidents[0].severity


# ── 比對：最具體的一條 ──

def test_the_most_specific_matching_rule_wins():
    rules = [SeverityRule(severity="P3"), SeverityRule(anomaly_class=SHIFT, severity="P2"),
             SeverityRule(pipeline="p", anomaly_class=SHIFT, severity="P1"), SeverityRule(entity="web", severity="P0")]
    assert severity_rule_for(rules, "p", _detected()).severity == "P1"         # 兩個條件 > 一個條件
    assert severity_rule_for(rules, "other", _detected()).severity == "P0"     # 一樣一個條件：實體 > 分類
    assert severity_rule_for(rules, "other", _detected(entity="db")).severity == "P2"
    assert severity_rule_for(rules, "other", _detected(entity="db", detector=CHRONIC)).severity == "P3"
    assert severity_rule_for([SeverityRule(metric="errors", severity="P0")], "p", _detected()) is None


def test_a_rule_overrides_the_pipeline_default_and_no_rule_means_the_default():
    assert _severity([_detected()], []) == "P1"
    assert _severity([_detected()], [SeverityRule(anomaly_class=SHIFT, severity="P3")]) == "P3"
    assert _severity([_detected()], [SeverityRule(anomaly_class=SHIFT, severity="P0")]) == "P0"
    # 管線 YAML 寫了預設 P2：規則還是優先；規則管不到的分類用管線的預設
    assert _severity([_detected()], [SeverityRule(anomaly_class=SHIFT, severity="P3")], detected_severity="P2") == "P3"
    assert _severity([_detected(detector=CHRONIC)], [SeverityRule(anomaly_class=SHIFT, severity="P3")], detected_severity="P2") == "P2"


def test_a_card_takes_the_most_severe_of_its_anomalies():
    rules = [SeverityRule(anomaly_class=SHIFT, severity="P3"), SeverityRule(anomaly_class=CHRONIC, severity="P0")]
    assert _severity([_detected(), _detected(detector=CHRONIC)], rules) == "P0"
    assert _severity([_detected(), _detected(entity="db")], rules) == "P3"


def test_rules_do_not_touch_external_alerts():
    everything = [SeverityRule(severity="P3")]
    assert [_severity([_external(s)], everything) for s in (1.0, 4.0, 7.0, 12.0)] == ["P3", "P2", "P1", "P0"]
    assert _severity([_detected(), _external(12.0)], everything) == "P0"   # 外部那一半自己是 P0
    assert _severity([_detected(score=500.0), _external(1.0)], everything) == "P3"  # 演算法分數再高也照規則


def test_rules_only_apply_to_their_own_pipeline():
    rules = [SeverityRule(pipeline="team-a", severity="P3")]
    assert _severity([_detected()], rules, pipeline="team-a") == "P3"
    assert _severity([_detected()], rules, pipeline="team-b") == "P1"
