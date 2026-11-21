"""跨系統收斂：同實體、拓撲鏈、併入進行中事故、嚴重度只升不降、全部 resolved 並安靜一段時間才結案。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.grouping import Alert, converge, group_alerts

T0 = datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc)
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")]


def alert(fp: str, service: str, severity: str = "P2", second: int = 0, status: str = "firing") -> Alert:
    return Alert(fp, status, f"rule-{fp}", f"rule {fp}", service, severity, T0 + timedelta(seconds=second),
                 T0 + timedelta(seconds=second + 60) if status == "resolved" else None, {"A": 1})


def done(fp: str, service: str = "x") -> Alert:
    return alert(fp, service, status="resolved")


def services(groups) -> list[list[str]]:
    return [sorted({a.service for a in g}) for g in groups]


def round_(open_, firing=(), resolved=(), edges=EDGES, at=0, close_after=300):
    return converge(open_, list(firing), list(resolved), edges, now=T0 + timedelta(seconds=at),
                    close_after_seconds=close_after)


def test_two_alerts_on_the_same_service_are_one_group():
    assert services(group_alerts([alert("a", "cim-eap"), alert("b", "cim-eap")], [])) == [["cim-eap"]]


def test_services_joined_by_an_edge_are_one_group_in_either_direction():
    assert services(group_alerts([alert("a", "cim-eap"), alert("b", "mq-broker")], EDGES)) == [["cim-eap", "mq-broker"]]
    assert services(group_alerts([alert("a", "mq-broker"), alert("b", "cim-eap")], EDGES)) == [["cim-eap", "mq-broker"]]


def test_chain_is_followed_through_alerting_services_regardless_of_arrival_order():
    """下游先響、上游晚響（門檻比較嚴的先到）：順序不影響結果。"""
    got = group_alerts([alert("a", "amhs-mcs"), alert("b", "mq-broker", second=40), alert("c", "cim-eap", second=20)], EDGES)
    assert services(got) == [["amhs-mcs", "cim-eap", "mq-broker"]]


def test_chain_is_not_followed_through_a_service_without_alerts():
    got = group_alerts([alert("a", "amhs-mcs", second=1), alert("b", "mq-broker")], EDGES)
    assert services(got) == [["mq-broker"], ["amhs-mcs"]]


def test_unrelated_services_stay_separate_and_cycles_do_not_hang():
    cyc = EDGES + [("amhs-mcs", "mq-broker")]
    got = group_alerts([alert("a", "mq-broker"), alert("b", "erp", second=2), alert("c", "amhs-mcs", second=1)], cyc)
    assert services(got) == [["amhs-mcs", "mq-broker"], ["erp"]]


def test_new_group_opens_one_incident_with_the_highest_severity():
    [inc] = round_([], [alert("a", "mq-broker", "P2"), alert("b", "cim-eap", "P1", second=20)], at=60)
    assert inc.status == "open" and inc.severity == "P1" and inc.alert_count == 2
    assert inc.services == ["cim-eap", "mq-broker"] and inc.started_at == T0
    assert inc.last_seen_at == T0 + timedelta(seconds=60) and inc.fresh and inc.changed and not inc.annotated
    assert inc.signals["a"]["status"] == "firing" and inc.signals["a"]["values"] == {"A": 1}


def test_later_alerts_are_absorbed_and_severity_never_drops():
    [inc] = round_([], [alert("a", "cim-eap", "P1")])
    uid, inc.annotated = inc.uid, True
    [again] = round_([inc], [alert("b", "amhs-mcs", "P3", second=90)], at=150)  # 下游、比較輕
    assert again.uid == uid and again.severity == "P1" and again.alert_count == 2 and again.fresh
    assert again.services == ["amhs-mcs", "cim-eap"] and not again.annotated
    [up] = round_([again], [alert("c", "cim-eap", "P0", second=200)], at=260)
    assert up.severity == "P0" and up.alert_count == 3 and up.started_at == T0


def test_repeat_notification_of_a_member_is_not_fresh():
    [inc] = round_([], [alert("a", "cim-eap")])
    [again] = round_([inc], [alert("a", "cim-eap")], at=14400)  # Grafana 的 repeat_interval 重複通知
    assert again.alert_count == 1 and not again.fresh


def test_unrelated_alert_opens_a_second_incident():
    [inc] = round_([], [alert("a", "mq-broker")])
    both = round_([inc], [alert("z", "erp", second=5)], at=60)
    assert len(both) == 2 and {i.status for i in both} == {"open"} and both[0].uid != both[1].uid


def test_incident_stays_open_while_any_member_is_firing():
    [inc] = round_([], [alert("a", "mq-broker"), alert("b", "cim-eap")])
    [inc] = round_([inc], resolved=[done("a")], at=100)
    assert inc.status == "open" and inc.resolved_at is None and inc.signals["a"]["status"] == "resolved"
    [inc] = round_([inc], at=10000)  # 過再久，還有一筆在響就不結案
    assert inc.status == "open" and not inc.changed


def test_incident_closes_only_after_all_resolved_and_quiet_long_enough():
    [inc] = round_([], [alert("a", "mq-broker")])
    [inc] = round_([inc], resolved=[done("a")], at=100)
    assert inc.status == "open" and inc.resolved_at == T0 + timedelta(seconds=100) and not inc.annotated
    [inc] = round_([inc], at=399)
    assert inc.status == "open"
    [inc] = round_([inc], at=400)
    assert inc.status == "closed" and inc.ended_at == T0 + timedelta(seconds=60) and not inc.annotated


def test_refiring_before_close_keeps_the_same_incident():
    [inc] = round_([], [alert("a", "mq-broker")])
    uid = inc.uid
    [inc] = round_([inc], resolved=[done("a")], at=100)
    [inc] = round_([inc], [alert("a", "mq-broker", second=200)], at=260)  # 剛恢復又響
    assert inc.uid == uid and inc.status == "open" and inc.resolved_at is None and inc.alert_count == 1
    [inc] = round_([inc], at=2000)
    assert inc.status == "open"


def test_alert_that_fired_and_resolved_inside_one_window_still_leaves_an_incident():
    [inc] = round_([], [alert("a", "mq-broker")], [done("a")], at=60)
    assert inc.status == "open" and inc.signals["a"]["status"] == "resolved" and inc.resolved_at is not None
    [inc] = round_([inc], at=360)
    assert inc.status == "closed"


def test_resolved_for_something_we_never_saw_does_nothing():
    assert round_([], resolved=[done("ghost")]) == []


def test_group_touching_two_open_incidents_folds_them_into_the_older_one():
    [a] = round_([], [alert("a", "mq-broker", "P3")])
    b = round_([a], [alert("b", "amhs-mcs", "P1", second=5)], at=65)[1]
    assert a.uid != b.uid  # 中間的 cim-eap 還沒響：兩張
    kept, folded = round_([a, b], [alert("c", "cim-eap", "P2", second=70)], at=130)
    assert kept.uid == a.uid and kept.status == "open" and kept.severity == "P1" and kept.alert_count == 3
    assert kept.services == ["amhs-mcs", "cim-eap", "mq-broker"]
    assert folded.status == "closed" and kept.uid in folded.summary and not folded.annotated
