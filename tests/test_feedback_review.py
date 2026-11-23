"""事故卡的分項覆核（control_plane/learning/review.py）：收斂、RCA、結案、嚴重度各自對不對。

這一層只收資料與出報表，不改任何設定。資料全是模擬的事故卡與模擬的覆核（平台還沒上線、沒有真實的人工覆核），
測的是結構：填錯會被擋、平台當時的說法有存下來、統計的分母是「有人看過的」。
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from aiops.engine.pipeline import Pipeline, load_workflow
from aiops.engine.requests import ExternalAnomalyIn, FeedbackSubmit, IngestAnomalies, ReviewIn
from aiops.interfaces.runner import run, workflow_path
from aiops.storage.hooks import StorageHooks
from aiops.storage import crud
from aiops.storage.models import ActiveDetectorConfigRow, FeedbackRow, IncidentRow, TuningProposalRow
from aiops.storage.repository import Feedbacks, Incidents, ServiceEdges

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
URL = "/api/v1/tuning/feedback"


def _card(name: str, anomalies: dict[str, int], score: float = 5.0) -> dict:
    """一張模擬的事故卡：外部告警進來、跑一輪 it_otel 的收斂。管線與實體名稱都帶 name：
    不同卡片不共用實體（同一個實體短時間內的第二張卡會被冷卻抑制，那不是這裡要測的）。"""
    anomalies = {f"{name}-{e}": m for e, m in anomalies.items()}
    items = [ExternalAnomalyIn(external_id=f"{name}-{e}", entity=e, metric="latency", ts=NOW - timedelta(minutes=m), score=score)
             for e, m in anomalies.items()]
    names = list(anomalies)
    if len(names) > 1:  # 多個實體要有關係邊才會併成一張卡
        ServiceEdges.add_calls({pair: 7 for pair in zip(names, names[1:])}, seen_at=(NOW - timedelta(hours=3)).replace(tzinfo=None))
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline=name, source="x", anomalies=items))
    wf = load_workflow(workflow_path("it_otel")).model_copy(update={"name": name})
    Pipeline(wf, StorageHooks()).run(now=NOW, targets=[])
    (row,) = Incidents.list_open(name)
    return row


def _post(client, uid: str, review: dict | None = None, verdict: str = "true_positive"):
    body = {"incident_uid": uid, "verdict": verdict, "text": "覆核", "author": "amy"}
    if review is not None:
        body["review"] = review
    return client.post(URL, json=body)


# ── 請求的形狀：填錯在進門時就擋 ──

@pytest.mark.parametrize("review, message", [
    ({}, "至少要填一項"),
    ({"grouping": "ok", "unrelated_entities": ["db"]}, "merged_wrong"),
    ({"grouping": "merged_wrong", "same_as": ["inc-1"]}, "split_wrong"),
    ({"rca": "ok", "actual_root_entity": "db"}, "wrong_root"),
    ({"severity": "ok", "actual_root_entity": "db"}, "wrong_root"),
])
def test_details_are_only_accepted_with_the_judgement_they_explain(review, message):
    with pytest.raises(ValidationError, match=message):
        ReviewIn(**review)


def test_unknown_values_and_fields_are_rejected():
    for bad in ({"grouping": "wrong"}, {"rca": "bad"}, {"groupping": "ok"}, {"resolve": "late"}):
        with pytest.raises(ValidationError):
            ReviewIn(**bad)


def test_a_missed_alarm_without_a_card_cannot_carry_a_review():
    with pytest.raises(ValidationError, match="沒有事故卡可以覆核"):
        FeedbackSubmit(verdict="missed", text="t", author="a", pipeline="p", entity="e",
                       time_range={"start": NOW - timedelta(hours=1), "end": NOW}, review={"rca": "ok"})


def test_only_the_reviewed_aspects_are_kept():
    assert ReviewIn(rca="ok").to_dict() == {"rca": "ok"}
    assert ReviewIn(grouping="merged_wrong", unrelated_entities=["db"], severity="too_high").to_dict() == {
        "grouping": "merged_wrong", "severity": "too_high", "unrelated_entities": ["db"]}


# ── 寫入：連同平台當時的說法一起存 ──

def test_a_review_is_stored_with_what_the_platform_said_at_the_time(client):
    card = _card("rv-store", {"web": 4, "db": 6})
    Incidents.record_rca(card["id"], "topology_inference", NOW, "db")
    r = _post(client, card["uid"], {"rca": "wrong_root", "actual_root_entity": "cache", "grouping": "ok"})
    assert r.status_code == 200, r.text
    stored = r.json()["result"]["feedback"]["review"]
    assert stored == {"grouping": "ok", "rca": "wrong_root", "actual_root_entity": "cache",
                      "platform": {"severity": card["severity"], "status": "open", "entities": card["incident"]["entities"],
                                   "rca_conclusion_type": "topology_inference", "rca_root_entity": "db"}}
    # 之後重跑 RCA 改了答案：這筆覆核仍然對得回「當時平台指的是 db」
    Incidents.record_rca(card["id"], "confirmed", NOW + timedelta(hours=1), "cache")
    assert Feedbacks.get(r.json()["result"]["feedback"]["id"])["review"]["platform"]["rca_root_entity"] == "db"


def test_feedback_without_a_review_is_stored_as_before(client):
    card = _card("rv-plain", {"web": 4})
    r = _post(client, card["uid"])
    assert r.status_code == 200 and r.json()["result"]["feedback"]["review"] == {}


def test_details_must_match_the_card(client):
    card = _card("rv-check", {"web": 4, "db": 6})
    other = _card("rv-check-2", {"cache": 4})
    cases = [
        ({"grouping": "merged_wrong", "unrelated_entities": ["nope"]}, 400, "不在其中"),
        ({"grouping": "merged_wrong", "unrelated_entities": ["rv-check-web", "rv-check-db"]}, 400, "false_positive"),
        ({"grouping": "split_wrong", "same_as": [card["uid"]]}, 400, "不能是這張卡自己"),
        ({"grouping": "split_wrong", "same_as": ["inc-missing"]}, 404, "找不到事件群"),
    ]
    for review, status, message in cases:
        r = _post(client, card["uid"], review)
        assert r.status_code == status and message in r.text, r.text
    assert Feedbacks.created_between(NOW - timedelta(days=1), datetime.now(timezone.utc) + timedelta(days=1)) == []  # 被擋的一筆都沒寫
    assert _post(client, card["uid"], {"grouping": "merged_wrong", "unrelated_entities": ["rv-check-web"]}).status_code == 200
    assert _post(client, card["uid"], {"grouping": "split_wrong", "same_as": [other["uid"]]}).status_code == 200


def test_rca_records_who_it_pointed_at(client):
    """跑過 RCA 的事件群記下第一候選；沒有候選是空字串。"""
    card = _card("rv-rca", {"web": 4, "db": 6})
    assert card["rca_root_entity"] == ""  # 還沒跑過
    result = run("rca_report", incident_id=card["id"])
    row = Incidents.get(card["id"])
    candidates = result["root_cause"]["candidates"]
    assert row["rca_root_entity"] == (candidates[0]["entity"] if candidates else "")
    assert row["rca_conclusion_type"] == result["root_cause"]["conclusion_type"]


# ── 統計 ──

def test_the_report_counts_only_what_someone_reviewed_and_takes_the_latest_judgement(client):
    """模擬一批覆核：6 張卡，其中 5 張有人看過。分母是每一項「有人看過的卡」，同一張卡改判以最後一次為準。"""
    cards = {name: _card(f"rv-{name}", entities, score) for name, entities, score in [
        ("a", {"web": 4, "db": 6}, 5.0), ("b", {"web": 4, "db": 6}, 5.0), ("c", {"web": 4, "db": 6}, 5.0),
        ("d", {"web": 4}, 12.0), ("e", {"web": 4}, 5.0), ("unreviewed", {"web": 4}, 5.0)]}
    for name, conclusion, root in [("a", "confirmed", "db"), ("b", "confirmed", "db"), ("c", "topology_inference", "db")]:
        Incidents.record_rca(cards[name]["id"], conclusion, NOW, root)
    _post(client, cards["a"]["uid"], {"rca": "ok", "grouping": "ok", "resolve": "ok"})
    _post(client, cards["b"]["uid"], {"rca": "ok"})
    _post(client, cards["b"]["uid"], {"rca": "wrong_root", "actual_root_entity": "cache"})  # 後來的人改判
    _post(client, cards["c"]["uid"], {"rca": "wrong_root", "grouping": "merged_wrong", "unrelated_entities": ["rv-c-web"]})
    _post(client, cards["d"]["uid"], {"severity": "too_high"}, verdict="false_positive")
    _post(client, cards["e"]["uid"], {"resolve": "early", "rca": "missed_root"})
    _post(client, cards["unreviewed"]["uid"])  # 只給了判定、沒有分項覆核

    report = client.get("/api/v1/tuning/review").json()["result"]
    assert (report["incidents_with_feedback"], report["incidents_reviewed"]) == (6, 5)
    assert report["aspects"] == {
        "grouping": {"reviewed": 2, "ok": 1, "merged_wrong": 1, "split_wrong": 0},
        "rca": {"reviewed": 4, "ok": 1, "wrong_root": 2, "missed_root": 1},
        "resolve": {"reviewed": 2, "ok": 1, "early": 1},
        "severity": {"reviewed": 1, "ok": 0, "too_high": 1, "too_low": 0}}
    assert report["rca_by_conclusion"] == {
        "confirmed": {"reviewed": 2, "ok": 1, "wrong_root": 1, "missed_root": 0},
        "not_run": {"reviewed": 1, "ok": 0, "wrong_root": 0, "missed_root": 1},
        "topology_inference": {"reviewed": 1, "ok": 0, "wrong_root": 1, "missed_root": 0}}
    level = cards["d"]["severity"]
    assert report["false_positive_by_severity"] == {level: 1} and report["severity_too_high_by_level"] == {level: 1}
    # 每一種「錯」都有指到要看哪個設定；每一種「對」不需要
    wrong = {f"{aspect}.{value}" for aspect, counts in report["aspects"].items() for value in counts if value not in ("reviewed", "ok")}
    assert set(report["points_at"]) == wrong and "不要拿比例去調設定" in report["caveat"]


def test_the_report_can_be_scoped_to_one_pipeline_and_a_time_window(client):
    card = _card("rv-scope-a", {"web": 4})
    other = _card("rv-scope-b", {"web": 4})
    _post(client, card["uid"], {"resolve": "early"})
    _post(client, other["uid"], {"resolve": "ok"})
    scoped = client.get("/api/v1/tuning/review?pipeline=rv-scope-a").json()["result"]
    assert scoped["aspects"]["resolve"] == {"reviewed": 1, "ok": 0, "early": 1} and scoped["pipeline"] == "rv-scope-a"
    assert client.get("/api/v1/tuning/review?pipeline=nobody").json()["result"]["incidents_reviewed"] == 0
    assert client.get("/api/v1/tuning/review?lookback_days=0").status_code == 422
    old = run("admin_review_metrics", now=NOW - timedelta(days=400))  # 那時候還沒有任何覆核
    assert old["incidents_reviewed"] == 0 and old["aspects"]["resolve"]["reviewed"] == 0


def test_reviews_change_no_setting_and_create_no_proposal(client):
    """這一層只收資料：送覆核、出報表都不產生調參提案、不改現行參數。"""
    card = _card("rv-readonly", {"web": 4, "db": 6})
    _post(client, card["uid"], {"grouping": "merged_wrong", "rca": "wrong_root", "resolve": "early", "severity": "too_high"})
    client.get("/api/v1/tuning/review")
    assert crud.find(TuningProposalRow) == [] and crud.find(ActiveDetectorConfigRow) == []
    assert Incidents.get(card["id"])["severity"] == card["severity"]


# ── 回饋頁、migration ──

def test_the_feedback_page_offers_every_judgement_the_api_accepts(client):
    page = client.get("/feedback").text
    from aiops.control_plane.learning.review import ASPECTS
    for aspect, values in ASPECTS.items():
        assert f'data-aspect="{aspect}"' in page
        for value in values:
            assert f'<option value="{value}">' in page, (aspect, value)
    assert "payload.review = reviewed" in page


def test_migration_adds_both_columns_idempotently_with_defaults_for_old_rows():
    sql = (ROOT / "migrations/postgres/0013_feedback_review.sql").read_text(encoding="utf-8")
    assert "ALTER TABLE incidents ADD COLUMN IF NOT EXISTS rca_root_entity VARCHAR(256) NOT NULL DEFAULT ''" in sql
    assert "ALTER TABLE feedback ADD COLUMN IF NOT EXISTS review JSONB NOT NULL DEFAULT '{}'::jsonb" in sql
    root, review = IncidentRow.__table__.columns["rca_root_entity"], FeedbackRow.__table__.columns["review"]
    assert root.type.length == 256 and root.server_default.arg == "" and review.server_default.arg == "{}"
