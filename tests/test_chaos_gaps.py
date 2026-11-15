"""破壞性驗收（docs/CHAOS_E2E_REPORT.md）抓到的三個平台缺口的回歸測試。

場景 10：事故經驗庫的併發寫入——首次寫入撞唯一鍵（IntegrityError）、context 讀改寫互相覆蓋。
場景 7 ：學習閉環現抓現算的時窗少抓最後一個點（資料源區間含頭不含尾），單點誤報事件群退回固定 ×1.2。
場景 9 ：序列體檢把 sum 聚合的空桶（真實 ES 回 0.0）當成流量為 0，用 7.5 天資料算出 168h 自相關並宣稱有週期。
"""
import json
import threading
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from aiops.control_plane.learning.profile import profile
from aiops.control_plane.learning.replay import fetch_span
from aiops.engine.context import PipelineContext, Target
from aiops.engine.requests import FeedbackSubmit
from aiops.interfaces.runner import run, run_context
from aiops.storage import crud, db, models
from aiops.storage.models import IncidentKnowledgeRow
from aiops.storage.repository import Knowledge

T0 = datetime(2026, 11, 3, 15, 40)


def _row(uid, **over):
    base = dict(pipeline="sample_it_otel", entities=["bill-svc"], metrics=["latency"], signals=["latency"], resolution="", root_cause="",
                author="amy", verdict="true_positive", severity="P0", window_start=T0, window_end=T0 + timedelta(minutes=30), context={})
    return Knowledge.upsert(uid, **{**base, **over})


# ── 場景 10：upsert 與 merge_context ──


def test_upsert_is_a_single_insert_on_conflict_statement_not_select_then_insert():
    first = _row("inc-u", author="amy", context={"a": 1})
    second = _row("inc-u", author="bob")  # 同一個鍵再寫：更新，不是第二列、也不丟 IntegrityError
    assert first["id"] == second["id"] and second["author"] == "bob" and second["context"] == {}  # upsert 是整欄覆蓋
    with db.read_session() as s:
        from sqlalchemy import func, select
        assert s.execute(select(func.count()).select_from(IncidentKnowledgeRow)).scalar() == 1


def test_ten_workers_first_writing_the_same_incident_at_once_all_succeed(tmp_path):
    """記憶體 SQLite 共用一條連線測不出競爭；用檔案 SQLite 讓每個 thread 真的各自連線。"""
    db.configure(f"sqlite:///{tmp_path / 'race.db'}")
    models.reset_all()
    errors: list[str] = []
    barrier = threading.Barrier(10)

    def first_writer(i: int):
        try:
            barrier.wait()
            _row("inc-race", author=f"w{i}", context={"w": i})
        except Exception as e:  # noqa: BLE001
            errors.append(type(e).__name__)

    threads = [threading.Thread(target=first_writer, args=(i,)) for i in range(10)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert errors == [], errors  # 以前先查再寫：4～6／10 個 IntegrityError
    row = Knowledge.get("inc-race")
    assert row is not None and row["author"].startswith("w")


def test_merge_context_appends_keys_atomically_and_keeps_existing_ones():
    _row("inc-m", context={"base": "起始", "n": 1})
    Knowledge.merge_context("inc-m", {"w1": {"維運標籤": ["外部依賴"], "處方註記": "檢查 tax-gw:443"}})
    row = Knowledge.merge_context("inc-m", {"w2": {"trace_id": "ab" * 16}, "n": 2})
    assert row["context"] == {"base": "起始", "n": 2, "w1": {"維運標籤": ["外部依賴"], "處方註記": "檢查 tax-gw:443"}, "w2": {"trace_id": "ab" * 16}}
    assert row["updated_at"] >= row["created_at"]
    with pytest.raises(KeyError):
        Knowledge.merge_context("inc-nope", {"x": 1})


def test_ten_workers_appending_fifty_keys_lose_nothing(tmp_path):
    db.configure(f"sqlite:///{tmp_path / 'merge.db'}")
    models.reset_all()
    _row("inc-c", context={"base": "起始"})
    errors: list[str] = []
    barrier = threading.Barrier(10)

    def appender(i: int):
        try:
            barrier.wait()
            for j in range(5):
                Knowledge.merge_context("inc-c", {f"w{i}_r{j}": {"處方註記": f"中文 {i}-{j}"}})
        except Exception as e:  # noqa: BLE001
            errors.append(f"{type(e).__name__}: {e}"[:80])

    threads = [threading.Thread(target=appender, args=(i,)) for i in range(10)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    final = Knowledge.get("inc-c")["context"]
    assert errors == [], errors
    assert sum(k.startswith("w") for k in final) == 50 and final["base"] == "起始"  # 讀改寫版本只留 5～7 個
    assert final["w3_r2"] == {"處方註記": "中文 3-2"}


def test_merge_json_sql_uses_database_side_concatenation_on_postgresql():
    """沒有 PostgreSQL 也要確認 PG 分支產生的 SQL 是 context || patch（不是 Python 讀改寫）；真 PG 的驗證在 scripts/chaos 場景 10。"""
    from sqlalchemy import cast, literal
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.dialects.postgresql import JSONB

    expr = cast(IncidentKnowledgeRow.context, JSONB).op("||")(cast(literal(json.dumps({"k": "v"})), JSONB))
    sql = str(expr.compile(dialect=postgresql.dialect()))
    assert "||" in sql and "CAST(incident_knowledge.context AS JSONB)" in sql


def test_confirming_the_same_incident_twice_merges_both_contexts(client, now):
    """admin.feedback_submit 的 true_positive 兩次（兩個人各補不同的 context）：以前整包寫回會蓋掉先寫的。"""
    ctx = run_context("sample_metrics_only", now=now, strict=False)
    uid = ctx.incidents[0].uid
    run("admin_feedback_submit", request=FeedbackSubmit(incident_uid=uid, verdict="true_positive", text="真的", author="amy",
                                                      resolution="重啟收集器", context={"tags": ["collector"]}))
    k = run("admin_feedback_submit", request=FeedbackSubmit(incident_uid=uid, verdict="true_positive", text="補 trace", author="bob",
                                                          context={"sample_trace_id": "dd487b01"}))["knowledge"]
    assert k["context"]["tags"] == ["collector"] and k["context"]["sample_trace_id"] == "dd487b01" and "series" in k["context"]
    assert k["resolution"] == "重啟收集器" and k["author"] == "bob"  # 沒寫處置不蓋掉；作者更新為最後確認的人


# ── 場景 7：fetch_span 含終點 ──


def test_fetch_span_includes_the_end_point_so_single_point_windows_are_not_empty():
    ctx = PipelineContext(pipeline="sample_metrics_only", now=datetime(2026, 11, 8, 12, tzinfo=timezone.utc),
                          target=Target("mock", "cpu", "metric_a"))
    end = datetime(2026, 11, 8, 6, 0, tzinfo=timezone.utc)
    df = fetch_span(ctx, end - timedelta(minutes=30), end, 60)
    ts = pd.to_datetime(df["ts"], utc=True)
    assert ts.iloc[-1] == pd.Timestamp(end) and len(df) == 31  # 含頭含尾：30 分鐘 = 31 個點
    one = fetch_span(ctx, end, end, 60)  # 單點時窗（start == end）：以前抓到 0 筆
    assert len(one) == 1 and pd.Timestamp(one["ts"].iloc[0]) == pd.Timestamp(end)


def test_quantile_inference_now_covers_a_one_minute_false_positive(monkeypatch, now):
    """單點誤報事件群（start == end）走完整分位數反推：以前 fetch_span 少抓最後一點 → 『時窗內沒有資料』→ 退回固定 ×1.2。
    資料源用「含頭不含尾」的假源（跟 Elasticsearch 一樣），尖峰固定在一個時間戳上。"""
    from aiops.control_plane.learning import threshold
    from aiops.control_plane.learning.replay import Series
    from aiops.stages.detectors.mad import MADDetector
    from aiops.stages.ingestion import fetch

    spike = datetime(2026, 11, 8, 6, 0, tzinfo=timezone.utc)

    class HalfOpenSpec:
        def fetch_data(self, query, start, end, step_seconds, field_map=None):
            ts = pd.date_range(start, end, freq=f"{step_seconds}s", tz="UTC", inclusive="left")  # [start, end)
            seed = (ts.view("int64") // 60_000_000_000) % 1000
            value = 50 + 2 * np.sin(np.asarray(seed) / 7.0)
            value = np.where(ts == pd.Timestamp(spike), 200.0, value)
            return pd.DataFrame({"ts": ts, "value": value})

    monkeypatch.setattr(fetch, "get_datasource_spec", lambda _t: HalfOpenSpec())
    ctx = PipelineContext(pipeline="sample_metrics_only", now=now, target=Target("elasticsearch", "q", "metric_a"))
    got = threshold.infer(ctx, MADDetector, {"threshold": 3.5}, [Series("metric_a", "elasticsearch", "q")],
                          "sample_metrics_only", "metric_a", spike, spike)
    assert isinstance(got, dict), got  # 舊版回字串「時窗內沒有資料」
    assert got["s_max"] > 3.5 and got["target"] >= got["s_max"] * 1.05 and got["series"][0]["window_points"] == 1


# ── 場景 9：序列體檢的空桶 ──


def _series(days: int, step: int = 300, flat_zero_last_days: float = 0):
    idx = pd.date_range("2026-11-01", periods=days * 86400 // step, freq=f"{step}s", tz="UTC")
    hours = (idx.hour + idx.minute / 60).to_numpy()
    value = 100 + 40 * np.sin(2 * np.pi * (hours - 14) / 24) + np.random.default_rng(1).normal(0, 2, len(idx))
    docs = pd.Series(np.ones(len(idx)), index=idx)
    if flat_zero_last_days:
        cut = len(idx) - int(flat_zero_last_days * 86400 // step)
        value[cut:] = 0.0  # sum 聚合對沒資料的桶回 0.0
        docs.iloc[cut:] = 0.0
    return pd.DataFrame({"ts": idx, "value": value}), docs


def test_profile_treats_zero_doc_buckets_as_missing_not_as_zero_traffic():
    df, docs = _series(14, flat_zero_last_days=6.5)  # 實際只有 7.5 天資料（跟破壞性場景 9 一樣），後 6.5 天是補零的空桶
    naive = profile(df, 300)
    masked = profile(df, 300, docs)
    assert naive["r_168h"] is not None and naive["points"] == len(df)  # 沒遮罩：把 0 當資料，硬算出 168h 自相關
    assert masked["empty_buckets"] == 1872 and masked["points"] == 2160 and masked["coverage"] == pytest.approx(7.5 / 14, abs=1e-3)
    assert masked["r_24h"] is not None  # 7.5 天夠算日週期
    assert masked["r_168h_pairs"] == 144 and masked["r_168h"] is None  # 168h 只有半天配對（< 一天門檻 288）：不給數字


def test_profile_without_doc_counts_is_unchanged_for_sources_that_omit_missing_points():
    df, _ = _series(14)
    a, b = profile(df, 300), profile(df, 300, None)
    assert a == b and a["empty_buckets"] == 0 and a["coverage"] == 1.0 and a["r_24h"] > 0.9


def test_series_profile_marks_low_coverage_as_insufficient_and_makes_no_proposal(monkeypatch, now):
    """走 admin.series_profile：覆蓋率不到門檻就寫明資料不足，不算週期、不提案（即使 r 算得出來）。"""
    from aiops.control_plane.learning import profile as P
    from aiops.engine.requests import MetricIn, SeriesProfile
    from aiops.storage.repository import Proposals

    df, docs = _series(14, flat_zero_last_days=8)
    monkeypatch.setattr(P, "fetch_span", lambda ctx, start, end, step, lags=(): df)
    monkeypatch.setattr(P, "doc_counts", lambda ctx, start, end, step: docs)
    q = json.dumps({"index": "metrics-apm.transaction.1m-*", "agg": "sum", "value_field": "transaction.duration.summary.value_count",
                    "filter": {"term": {"service.name": "bill-svc"}}})
    req = SeriesProfile(metric=MetricIn(source_type="elasticsearch", query=q), pipeline="sample_it_otel", entity="bill-svc", days=14, step_seconds=300, end=now)
    out = run("admin_series_profile", request=req, targets=req.to_targets(), now=now)
    assert out["proposal"] is None and "資料不足" in out["advice"] and "43%" in out["advice"]
    assert out["profile"]["coverage"] == pytest.approx(6 / 14, abs=1e-3) and not Proposals.with_status("detector.hourly_baseline", "sample_it_otel", "bill-svc", ["advisory"])


def test_doc_counts_query_is_the_same_filter_with_count_aggregation(monkeypatch):
    from aiops.control_plane.learning import profile as P

    seen = {}

    def fake_fetch_span(ctx, start, end, step, lags=()):
        seen["query"] = json.loads(ctx.target.query)
        return pd.DataFrame({"ts": pd.date_range(start, end, freq=f"{step}s", tz="UTC"), "value": 1.0})

    monkeypatch.setattr(P, "fetch_span", fake_fetch_span)
    q = {"index": "metrics-apm.transaction.1m-*", "agg": "sum", "value_field": "x", "filter": {"term": {"service.name": "bill-svc"}}}
    ctx = PipelineContext(pipeline="sample_it_otel", now=datetime(2026, 11, 8, tzinfo=timezone.utc), target=Target("elasticsearch", json.dumps(q), "bill-svc"))
    s = P.doc_counts(ctx, datetime(2026, 11, 7, tzinfo=timezone.utc), datetime(2026, 11, 8, tzinfo=timezone.utc), 300)
    assert seen["query"] == {"index": q["index"], "agg": "count", "filter": q["filter"]} and s is not None and len(s) > 0
    assert P.doc_counts(PipelineContext(pipeline="p", now=ctx.now, target=Target("mock", "cpu", "e")), ctx.now, ctx.now, 60) is None
