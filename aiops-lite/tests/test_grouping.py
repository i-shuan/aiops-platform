"""跨系統收斂：同實體、拓撲鏈、併入進行中事故、嚴重度只升不降、連續 N 輪無異常結案。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.grouping import Signal, converge, group_signals

T0 = datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc)
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")]


def sig(rule: str, service: str, severity: str = "P2", minute: int = 0, count: int = 3) -> Signal:
    return Signal(rule, f"rule {rule}", service, severity, T0 + timedelta(minutes=minute),
                  T0 + timedelta(minutes=minute + count - 1), count, 5.0, ("hourly_baseline",))


def services(groups) -> list[list[str]]:
    return [sorted({s.service for s in g}) for g in groups]


def round_(open_, signals, edges=EDGES, blind=(), close_after=3):
    return converge(open_, signals, edges, blind_services=set(blind), close_after_rounds=close_after)


def test_two_rules_on_the_same_service_are_one_group():
    assert services(group_signals([sig("a", "cim-eap"), sig("b", "cim-eap")], [])) == [["cim-eap"]]


def test_services_joined_by_an_edge_are_one_group_in_either_direction():
    assert services(group_signals([sig("a", "cim-eap"), sig("b", "mq-broker")], EDGES)) == [["cim-eap", "mq-broker"]]
    assert services(group_signals([sig("a", "mq-broker"), sig("b", "cim-eap")], EDGES)) == [["cim-eap", "mq-broker"]]


def test_chain_is_followed_through_anomalous_services():
    got = group_signals([sig("a", "amhs-mcs"), sig("b", "mq-broker"), sig("c", "cim-eap")], EDGES)
    assert services(got) == [["amhs-mcs", "cim-eap", "mq-broker"]]


def test_chain_is_not_followed_through_a_service_that_is_not_anomalous():
    got = group_signals([sig("a", "amhs-mcs", minute=1), sig("b", "mq-broker")], EDGES)
    assert services(got) == [["mq-broker"], ["amhs-mcs"]]


def test_unrelated_services_stay_separate_and_cycles_do_not_hang():
    cyc = EDGES + [("amhs-mcs", "mq-broker")]
    got = group_signals([sig("a", "mq-broker"), sig("b", "erp", minute=2), sig("c", "amhs-mcs", minute=1)], cyc)
    assert services(got) == [["amhs-mcs", "mq-broker"], ["erp"]]


def test_new_group_opens_an_incident_with_the_highest_severity_and_total_count():
    [inc] = round_([], [sig("a", "mq-broker", "P2", count=4), sig("b", "cim-eap", "P1", minute=2, count=6)])
    assert inc.status == "open" and inc.severity == "P1" and inc.alert_count == 10
    assert inc.services == ["cim-eap", "mq-broker"] and inc.started_at == T0
    assert inc.last_seen_at == T0 + timedelta(minutes=7) and inc.fresh and inc.changed and not inc.annotated


def test_later_anomalies_are_absorbed_into_the_open_incident_and_severity_never_drops():
    [inc] = round_([], [sig("a", "cim-eap", "P1")])
    uid = inc.uid
    inc.annotated = True
    [again] = round_([inc], [sig("b", "amhs-mcs", "P3", minute=10, count=2)])  # 下游、比較輕
    assert again.uid == uid and again.severity == "P1" and again.alert_count == 5
    assert again.services == ["amhs-mcs", "cim-eap"] and set(again.signals) == {"a", "b"}
    assert not again.annotated  # 內容變了，註記要重寫
    [up] = round_([again], [sig("a", "cim-eap", "P0", minute=20)])
    assert up.severity == "P0" and up.signals["a"]["count"] == 6 and up.signals["a"]["first_ts"] == T0


def test_unrelated_anomaly_opens_a_second_incident():
    [inc] = round_([], [sig("a", "mq-broker")])
    both = round_([inc], [sig("z", "erp", minute=5)])
    assert len(both) == 2 and {i.status for i in both} == {"open"} and both[0].uid != both[1].uid


def test_incident_closes_after_n_quiet_rounds_and_not_before():
    [inc] = round_([], [sig("a", "mq-broker")])
    for expected in (1, 2):
        [inc] = round_([inc], [])
        assert inc.status == "open" and inc.quiet_rounds == expected
    [inc] = round_([inc], [])
    assert inc.status == "closed" and inc.ended_at == inc.last_seen_at and not inc.annotated


def test_new_anomaly_resets_the_quiet_counter():
    [inc] = round_([], [sig("a", "mq-broker")])
    [inc] = round_([inc], [])
    [inc] = round_([inc], [])
    [inc] = round_([inc], [sig("a", "mq-broker", minute=15)])
    assert inc.quiet_rounds == 0 and inc.status == "open"
    [inc] = round_([inc], [])
    assert inc.quiet_rounds == 1 and inc.status == "open"


def test_a_round_without_data_does_not_count_as_quiet():
    [inc] = round_([], [sig("a", "mq-broker")])
    for _ in range(5):
        [inc] = round_([inc], [], blind=["mq-broker"])
    assert inc.status == "open" and inc.quiet_rounds == 0 and not inc.changed


def test_group_touching_two_open_incidents_folds_them_into_the_older_one():
    [a] = round_([], [sig("a", "mq-broker", "P3")])
    b = round_([a], [sig("b", "amhs-mcs", "P1", minute=5)])[1]
    assert a.uid != b.uid  # 中間的 cim-eap 還沒異常：兩張
    result = round_([a, b], [sig("c", "cim-eap", "P2", minute=10)])
    kept, folded = result[0], result[1]
    assert kept.uid == a.uid and kept.status == "open" and kept.severity == "P1" and kept.alert_count == 9
    assert kept.services == ["amhs-mcs", "cim-eap", "mq-broker"]
    assert folded.status == "closed" and kept.uid in folded.summary and not folded.annotated
