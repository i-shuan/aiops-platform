"""學習閉環步驟三：黃金事故防護罩、誤報的分位數反推門檻、序列體檢（ACF）。

合成資料：query 以 synth: 開頭的序列由這裡依「時間戳」產生（同一時刻不管從哪個時間窗查都是同一個值），
雜訊 N(50, 2)，再依名稱在指定時段疊上尖峰。所以「事故時窗另外抓」與「回測時窗」看到的是同一份資料。
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import inspect

from aiops.control_plane.learning import tuning_rules
from aiops.engine.errors import Conflict, NoDataError
from aiops.engine.params import ANY
from aiops.engine.requests import FeedbackPropose, FeedbackSubmit, ProposalApprove, ProposalBacktest, ProposalCreate
from aiops.engine.types import AnomalyEvent, Incident
from aiops.interfaces.runner import run
from aiops.sources.mock import MockSpec, _splitmix
from aiops.storage import db, models
from aiops.storage.repository import ActiveConfigs, Incidents, Knowledge, Proposals

T0 = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)
GOLDEN = (T0 - timedelta(hours=6), T0 - timedelta(hours=5, minutes=50))  # 真事故：+12（z ≈ 6）
TINY = (T0 - timedelta(hours=9), T0 - timedelta(hours=8, minutes=50))  # 小事故：+1（z ≈ 0.5，現行門檻 3 本來就抓不到）
FALSE_ALARM = (T0 - timedelta(hours=2), T0 - timedelta(hours=1, minutes=55))  # 誤報：+10（z ≈ 5）
ZSCORE = "detector.zscore"
PIPELINE = "sample_metrics_only"  # ingestion.fetch：train_lookback 1440 分鐘、step 60 秒；偵測節點是 detector.zscore

_BUMPS = {"synth:svc": [(GOLDEN, 12.0), (TINY, 1.0), (FALSE_ALARM, 10.0)]}


def _synth(query, ts):
    epoch_min = (pd.DatetimeIndex(ts).as_unit("s").asi8 // 60).astype(np.uint64)
    u1 = (_splitmix(epoch_min, 7) + 0.5) / 2.0**53
    u2 = (_splitmix(epoch_min, 11) + 0.5) / 2.0**53
    values = 50 + 2 * np.sqrt(-2 * np.log(u1)) * np.cos(2 * np.pi * u2)
    for (start, end), size in _BUMPS.get(query, []):
        values = values + size * ((ts >= start) & (ts <= end))
    return values


@pytest.fixture(autouse=True)
def synth_source(monkeypatch):
    real = MockSpec.fetch_data

    def fetch(self, query, start, end, step, field_map=None):
        if not query.startswith("synth:"):
            return real(self, query, start, end, step, field_map)
        ts = pd.date_range(pd.Timestamp(start).ceil(f"{step}s"), end, freq=f"{step}s")
        return pd.DataFrame({"ts": ts, "value": _synth(query, ts)})

    monkeypatch.setattr(MockSpec, "fetch_data", fetch)


def _incident(window, entity="svc", query="synth:svc", pipeline=PIPELINE) -> str:
    start, end = window
    events = [AnomalyEvent(entity, query, start + timedelta(minutes=m), 6.0, {}, 62.0, ZSCORE, "detect", "mock")
              for m in range(int((end - start).total_seconds() // 60) + 1)]
    inc = Incident(events, score=6.0, severity="P1")
    Incidents.create(inc.uid, "default", inc.to_dict(), "P1", None, pipeline)
    return inc.uid


def _confirm(uid, **kw):
    return run("admin_feedback_submit", request=FeedbackSubmit(
        incident_uid=uid, verdict="true_positive", text="真的掛了", author="amy", **kw))


def _proposal(threshold, pipeline=PIPELINE, entity="svc"):
    return run("admin_proposal_create", request=ProposalCreate(
        source="manual", detector=ZSCORE, params={"threshold": threshold}, proposer="a",
        pipeline=pipeline, entity=entity))["proposal"]["id"]


def _backtest(pid):
    # 回測時窗刻意不涵蓋黃金事故（只有最後 2 小時）：防護罩要自己另外抓
    req = ProposalBacktest(proposal_id=pid, metric={"source_type": "mock", "query": "synth:svc"}, entity="svc",
                           time_range={"start": T0 - timedelta(hours=1), "end": T0})
    return run("admin_proposal_backtest", request=req, targets=req.to_targets(), now=T0)["proposal"]


# ── 黃金事故庫 ──


def test_true_positive_always_enters_the_golden_set_even_without_a_resolution():
    uid = _incident(GOLDEN)
    k = _confirm(uid, context={"root_cause": "tax-gw:443", "trace_id": "abc"})["knowledge"]
    assert k["verdict"] == "true_positive" and k["resolution"] == "" and k["severity"] == "P1"
    assert (k["window_start"], k["window_end"]) == (GOLDEN[0].replace(tzinfo=None), GOLDEN[1].replace(tzinfo=None))
    assert k["context"]["root_cause"] == "tax-gw:443"
    assert k["context"]["series"] == [{"entity": "svc", "source_type": "mock", "query": "synth:svc"}]
    assert [g["incident_uid"] for g in Knowledge.golden(PIPELINE, "svc")] == [uid]
    assert Knowledge.golden(PIPELINE, "other") == [] and Knowledge.golden("sample_factory", "svc") == []
    # 沒寫處置的不進 RCA 相似案例（那一段要列「當時怎麼處理」）
    assert Knowledge.recent(10) == []
    _confirm(uid, resolution="切到備援 tax-gw")
    assert [r["incident_uid"] for r in Knowledge.recent(10)] == [uid]
    assert Knowledge.get(uid)["context"]["root_cause"] == "tax-gw:443"  # 再寫一次不會把之前的 context 蓋掉


def test_a_later_false_positive_verdict_demotes_the_golden_incident():
    uid = _incident(GOLDEN)
    _confirm(uid)
    run("admin_feedback_submit", request=FeedbackSubmit(incident_uid=uid, verdict="false_positive", text="其實是演練",
                                                        author="bob"))
    assert Knowledge.get(uid)["verdict"] == "false_positive" and Knowledge.golden(PIPELINE, "svc") == []


# ── 黃金事故防護罩 ──


def test_a_proposal_that_loses_a_golden_incident_is_rejected_and_cannot_be_approved():
    uid = _incident(GOLDEN)
    _confirm(uid)
    pid = _proposal(8.0)  # z ≈ 6 的真事故在門檻 8 底下一點都抓不到
    p = _backtest(pid)
    safety = p["backtest"]["safety"]
    assert p["status"] == "rejected" and safety["passed"] is False
    assert [m["incident_uid"] for m in safety["missed_golden"]] == [uid]
    missed = safety["missed_golden"][0]
    assert missed["current_anomalies"] > 0 and missed["proposed_anomalies"] == 0
    assert missed["series"][0]["train_points"] >= 1000  # 時窗前一整個訓練窗（metrics_only：1440 分鐘）
    with pytest.raises(Conflict, match="黃金事故"):
        run("admin_proposal_approve", request=ProposalApprove(proposal_id=pid, approver="boss"))
    with pytest.raises(Conflict, match="rejected"):
        _backtest(pid)
    assert ActiveConfigs.get(ZSCORE, PIPELINE, "svc") is None


def test_a_proposal_that_still_catches_every_golden_incident_passes():
    uid = _incident(GOLDEN)
    _confirm(uid)
    pid = _proposal(4.0)
    p = _backtest(pid)
    safety = p["backtest"]["safety"]
    assert p["status"] == "backtested" and safety["passed"] is True and safety["checked"] == 1
    assert [c["incident_uid"] for c in safety["caught"]] == [uid] and safety["caught"][0]["proposed_anomalies"] > 0
    out = run("admin_proposal_approve", request=ProposalApprove(proposal_id=pid, approver="boss"))
    assert out["proposal"]["status"] == "approved"


def test_a_wildcard_proposal_checks_golden_incidents_of_every_pipeline_and_entity():
    uid = _incident(GOLDEN, pipeline="sample_factory")
    _confirm(uid)
    p = _backtest(_proposal(8.0, pipeline=ANY, entity=ANY))
    assert p["status"] == "rejected"
    assert [(m["incident_uid"], m["pipeline"], m["entity"]) for m in p["backtest"]["safety"]["missed_golden"]] == \
        [(uid, "sample_factory", "svc")]


def test_golden_incidents_the_current_params_already_miss_do_not_block():
    """現行門檻 3 本來就抓不到 z ≈ 0.5 的小事故：不是這個提案造成的，不擋（不然補抓它的漏報提案也會被擋死）。"""
    uid = _incident(TINY)
    _confirm(uid)
    p = _backtest(_proposal(4.0))
    safety = p["backtest"]["safety"]
    assert p["status"] == "backtested" and safety["passed"] is True
    assert [m["incident_uid"] for m in safety["baseline_missed"]] == [uid]


def test_golden_layers_shadowed_by_a_more_specific_config_are_not_counted():
    """管線層的提案：這個實體已經有自己的設定（管線×實體），提案核准後也不會套到它，列進 unchecked 不擋。"""
    _confirm(_incident(GOLDEN))
    ActiveConfigs.set(ZSCORE, {"threshold": 3.0}, "manual", 0, expected_version=0, pipeline=PIPELINE, entity="svc")
    p = _backtest(_proposal(8.0, entity=ANY))
    safety = p["backtest"]["safety"]
    assert safety["passed"] is True and "更具體" in safety["unchecked"][0]["reason"]


def test_golden_incidents_without_data_are_listed_not_silently_passed(monkeypatch):
    _confirm(_incident(GOLDEN))
    real = MockSpec.fetch_data

    def no_old_data(self, query, start, end, step, field_map=None):
        if end <= T0 - timedelta(hours=2):
            raise NoDataError("retention 過期")
        return real(self, query, start, end, step, field_map)

    monkeypatch.setattr(MockSpec, "fetch_data", no_old_data)
    safety = _backtest(_proposal(8.0))["backtest"]["safety"]
    assert safety["checked"] == 0 and safety["unchecked"] and safety["passed"] is True


def test_old_backtests_without_a_safety_block_must_be_rerun():
    pid = _proposal(4.0)
    Proposals.update(pid, status="backtested", backtest={"points": 10})
    with pytest.raises(Conflict, match="重新回測"):
        run("admin_proposal_approve", request=ProposalApprove(proposal_id=pid, approver="boss"))


# ── 誤報：分位數反推門檻 ──


def _false_alarm_feedback():
    uid = _incident(FALSE_ALARM)
    return run("admin_feedback_submit", request=FeedbackSubmit(
        incident_uid=uid, verdict="false_positive", detector=ZSCORE, text="批次作業，不是事故", author="amy"))["feedback"]["id"]


def _propose(fid, scope="entity"):
    return run("admin_feedback_propose", request=FeedbackPropose(feedback_id=fid, proposer="bot", scope=scope))


def test_false_positive_threshold_clears_the_false_alarm_and_is_not_below_the_normal_tail():
    ActiveConfigs.set(ZSCORE, {"threshold": 4.0}, "manual", 0, expected_version=0, pipeline=PIPELINE, entity="svc")
    _confirm(_incident(GOLDEN))  # 黃金事故的時段不能算進「正常分數」
    out = _propose(_false_alarm_feedback())
    q = out["quantile"]
    assert out["generated_by"] == "quantile" and out["proposal"]["source"] == "auto"
    new = out["proposal"]["params"]["threshold"]
    assert 4.5 < q["s_max"] < 6.0  # +10 的誤報 ≈ 5 個標準差
    assert 2.5 < q["q"] < 4.5  # 常態雜訊的 |z| 千分之一尾端 ≈ 3.3
    assert q["q_points"] > 30 * 1440 * 0.9 and q["q_days"] == 30
    assert q["target"] == pytest.approx(max(q["s_max"] * 1.05, q["q"]), abs=1e-3)
    assert new == q["target"] and new > q["s_max"] and new >= q["q"]  # 這一場誤報會消失，而且不低於日常尾端
    assert "S_max" in out["proposal"]["reason"] and str(q["s_max"]) in out["proposal"]["reason"]


def test_golden_windows_are_excluded_from_the_normal_score_distribution():
    """排除窗 = 正式偵測同一套（這條管線的事故、維護窗）＋經驗庫裡這個實體的黃金事故（不限管線）。
    10 天前的真事故記在另一條管線：只有黃金事故這一層排得掉它；它被降級成誤報後，Q 會被那 2 小時 z ≈ 7 拉高。"""
    old = (T0 - timedelta(days=10), T0 - timedelta(days=10) + timedelta(minutes=120))
    _BUMPS["synth:svc"] = [(old, 14.0), (FALSE_ALARM, 10.0)]
    try:
        uid = _incident(old, pipeline="sample_factory")
        _confirm(uid)
        with_exclusion = _propose(_false_alarm_feedback())["quantile"]
        Knowledge.upsert(uid, verdict="false_positive")
        without = _propose(_false_alarm_feedback())["quantile"]
    finally:
        _BUMPS["synth:svc"] = [(GOLDEN, 12.0), (TINY, 1.0), (FALSE_ALARM, 10.0)]
    assert with_exclusion["q"] < 4.5 < without["q"]
    assert with_exclusion["excluded_windows"] == without["excluded_windows"] + 1


def test_quantile_step_caps_at_fifty_percent_and_keeps_the_direction():
    tunable = {"threshold": (3.0, 1.0, 10.0)}
    params, note = tuning_rules.quantile_step(9.0, tunable, {"threshold": 3.0})
    assert params == {"threshold": 4.5} and "還差" in note  # 單次最多 +50%，剩下的寫在 reason
    params, note = tuning_rules.quantile_step(2.0, tunable, {"threshold": 3.0})
    assert params is None and "不高於現行" in note  # 誤報只能調高
    params, _ = tuning_rules.quantile_step(30.0, tunable, {"threshold": 8.0})
    assert params == {"threshold": 10.0}  # 夾在 tunable 範圍內


def test_pipeline_scope_and_missing_data_fall_back_to_the_fixed_rule(monkeypatch):
    out = _propose(_false_alarm_feedback(), scope="pipeline")  # 用一條序列的分佈去定整條管線的門檻不合理
    assert out["generated_by"] == "template" and out["proposal"]["params"] == {"threshold": 3.6}

    def down(self, *a, **k):
        raise NoDataError("ES 裡已經沒有這段")

    monkeypatch.setattr(MockSpec, "fetch_data", down)
    out = _propose(_false_alarm_feedback())
    assert out["generated_by"] == "template" and out["proposal"]["params"] == {"threshold": 3.6}
    assert "沒用分位數反推" in out["proposal"]["reason"] and "撈不到資料" in out["quantile"]["fallback"]


# ── 序列體檢（ACF）──


def _profile(client, query, entity):
    body = {"metric": {"source_type": "mock", "query": query}, "pipeline": "sample_it_otel", "entity": entity,
            "end": T0.isoformat()}
    r = client.post("/api/v1/tuning/profile", json=body)
    assert r.status_code == 200, r.text
    return r.json()["result"]


def test_periodic_series_keep_the_hourly_baseline(client):
    out = _profile(client, "seasonal:cart", "cart-x")  # mock 的 seasonal：日週期 + 週末
    assert out["profile"]["r_24h"] > 0.8 and out["proposal"] is None and "有週期" in out["advice"]


def test_white_noise_gets_an_advisory_that_cannot_be_approved(client):
    out = _profile(client, "synth:noise", "noise-svc")
    prof, proposal = out["profile"], out["proposal"]
    assert abs(prof["r_24h"]) < 0.3 and abs(prof["r_168h"]) < 0.3
    assert prof["r_24h_lag_points"] == 288 and prof["r_168h_lag_points"] == 2016  # step 300 秒
    assert abs(prof["skewness"]) < 0.3 and abs(prof["kurtosis"]) < 0.5
    assert proposal["status"] == "advisory" and proposal["source"] == "profile" and proposal["params"] == {}
    assert proposal["detector_name"] == "detector.hourly_baseline" and proposal["entity"] == "noise-svc"
    assert "exclude_entities" in proposal["reason"] and "detect" in out["hourly_nodes"]
    assert _profile(client, "synth:noise", "noise-svc")["existing"] is True  # 不重複開
    r = client.post("/api/v1/tuning/proposals/approve", json={"proposal_id": proposal["id"], "approver": "boss"})
    assert r.status_code == 409 and "YAML" in r.json()["message"]


# ── migration ──

MIGRATION = Path(__file__).resolve().parents[1] / "migrations" / "postgres" / "0005_knowledge_golden_incidents.sql"


def test_migration_0005_is_rerunnable_postgres_sql():
    sql = MIGRATION.read_text(encoding="utf-8")
    for column in ("verdict", "severity", "window_start", "window_end", "context", "updated_at"):
        assert f"ADD COLUMN IF NOT EXISTS {column} " in sql
    assert "context JSONB" in sql and "USING GIN (context)" in sql
    for index in ("ix_incident_knowledge_pipeline_window", "ix_incident_knowledge_verdict_window",
                  "ix_incident_knowledge_context_gin"):
        assert f"CREATE INDEX IF NOT EXISTS {index}" in sql
    assert "WHERE i.uid = k.incident_uid AND k.window_start IS NULL" in sql  # 回填只動還沒填過的列
    assert [line for line in sql.splitlines() if line.strip() == "BEGIN;"] == ["BEGIN;"] and sql.rstrip().endswith("COMMIT;") and "lock_timeout" in sql


def test_models_create_the_new_columns_and_indexes_and_create_all_is_idempotent():
    models.create_all()  # 已經建過一次（fixture），再跑一次不能出錯
    insp = inspect(db.get_engine())
    cols = {c["name"] for c in insp.get_columns("incident_knowledge")}
    assert {"verdict", "severity", "window_start", "window_end", "context", "updated_at"} <= cols
    indexes = {tuple(i["column_names"]) for i in insp.get_indexes("incident_knowledge")}
    assert ("pipeline", "window_start") in indexes and ("verdict", "window_start") in indexes
