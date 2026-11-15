"""資料健康度：每條資料流（管線 × 實體 × 查詢）與每條管線執行結果的狀態區間（aiops/engine/health.py、
storage 的 data_stream_health／pipeline_run_health、GET /api/v1/data-health/*）。

要驗的事：事後查得到「那個時間點資料有沒有進來、是誰的問題」——斷流、從未有過資料、查詢寫錯、資料源連不上、
整輪失敗、管線定義載入失敗各自分得出來；狀態沒變不會每輪多一列；資料庫寫不進去時不影響那一輪原本的結果。
資料源是 mock（把 MockSpec.fetch_data 換成可以逐輪切換行為的版本），資料庫是測試用的 SQLite。
Grafana 儀表板的 SQL 也在這裡驗（對同一份 ORM 建出來的表實際執行），helm 相關的在 tests/test_helm_grafana.py。
"""
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import text

from aiops.engine.errors import BadRequest, InfrastructureError, NoDataError, NotFound, WorkflowError
from aiops.engine.health import RUN_STATUSES, STREAM_STATUSES, run_status, stream_status
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import IdRef
from aiops.interfaces.cli.__main__ import run_once
from aiops.interfaces.runner import run, run_context, run_scheduled, workflow_path
from aiops.sources.base import InvalidQueryError
from aiops.sources.mock import MockSpec
from aiops.storage import db
from aiops.storage.hooks import StorageHooks
from aiops.storage.models import DataStreamHealthRow, PipelineRunHealthRow
from aiops.storage.repository import Anomalies, RunHealth, StreamHealth

ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 11, 8, 11, 0, tzinfo=timezone.utc)
DASHBOARD = ROOT / "deploy" / "helm" / "aiops-grafana" / "files" / "dashboards" / "data-health.json"


def _t(minutes: int) -> datetime:
    return T0 + timedelta(minutes=minutes)


def _naive(minutes: int) -> datetime:
    return _t(minutes).replace(tzinfo=None)


@pytest.fixture
def source(monkeypatch):
    """可以逐輪切換行為的 mock 資料源：mode[查詢] = ok（預設）／empty（NoDataError）／zero（回 200 但零筆）／
    bad（查詢寫錯）／down（連不上）／crash（資料源程式自己的非預期例外）。"""
    mode: dict[str, str] = {}
    real = MockSpec.fetch_data

    def fetch(self, query, start, end, step_seconds, field_map=None):
        m = mode.get(query, "ok")
        if m == "empty":
            raise NoDataError(f"mock 在這段時間沒有資料：{query}")
        if m == "zero":
            return pd.DataFrame({"ts": pd.Series([], dtype="datetime64[ns, UTC]"), "value": pd.Series([], dtype=float)})
        if m == "bad":
            raise InvalidQueryError(f"查詢寫錯：{query}（欄位 latency_ms 不存在）")
        if m == "down":
            raise InfrastructureError(f"mock 連不上：{query}")
        if m == "crash":
            raise KeyError("hits")
        return real(self, query, start, end, step_seconds, field_map)

    monkeypatch.setattr(MockSpec, "fetch_data", fetch)
    return mode


def _pipeline(queries, hooks=None, **workflow_fields) -> Pipeline:
    wf = load_workflow(workflow_path("metrics_only"))
    data = wf.model_dump() | {"targets": [TargetSpec(source_type="mock", query=q, entity=f"svc-{q}") for q in queries]}
    return Pipeline(Workflow.model_validate(data | workflow_fields), hooks or StorageHooks())


def _streams(**filters) -> list[tuple]:
    """(實體, 狀態, 開始, 最後確認, 輪數, 是不是最新)，由舊到新。"""
    rows = sorted(StreamHealth.list(**filters), key=lambda r: (r["entity"], r["started_at"], r["id"]))
    return [(r["entity"], r["status"], r["started_at"], r["last_checked_at"], r["checks"], r["is_current"]) for r in rows]


def _runs(**filters) -> list[tuple]:
    rows = sorted(RunHealth.list(**filters), key=lambda r: (r["pipeline"], r["started_at"], r["id"]))
    return [(r["status"], r["started_at"], r["last_checked_at"], r["runs"], r["is_current"]) for r in rows]


# ── 狀態怎麼判（純函式）──


def test_stream_status_only_looks_at_whether_data_arrived():
    ok = dict(fetched=True, empty=False, seen_before=True)
    assert stream_status(**ok, error=None) == ("ok", "")
    assert stream_status(**ok, error=ValueError("偵測器 bug")) == ("ok", "")  # 資料有進來，後面的錯不算在資料流頭上
    assert stream_status(fetched=True, empty=True, error=None, seen_before=True)[0] == "no_data"
    assert stream_status(fetched=True, empty=True, error=None, seen_before=False)[0] == "never_seen"
    before = dict(fetched=False, empty=False)
    assert stream_status(**before, error=NoDataError("沒資料"), seen_before=True) == ("no_data", "沒資料")
    assert stream_status(**before, error=NoDataError("沒資料"), seen_before=False) == ("never_seen", "沒資料")
    assert stream_status(**before, error=InvalidQueryError("no such index"), seen_before=True) == ("config_error", "no such index")
    assert stream_status(**before, error=BadRequest("未知的資料源"), seen_before=False)[0] == "config_error"
    assert stream_status(**before, error=InfrastructureError("連不上"), seen_before=True)[0] == "unreachable"
    assert stream_status(**before, error=NotFound("查不到"), seen_before=True)[0] == "error"
    assert stream_status(**before, error=KeyError("hits"), seen_before=True) == ("error", "KeyError: 'hits'")
    assert stream_status(**before, error=None, seen_before=True) is None  # 這條管線沒有抓取節點：不記
    status, message = stream_status(**before, error=InvalidQueryError("x" * 5000), seen_before=True)
    assert status == "config_error" and len(message) == 1000  # 訊息截到欄位寬度


def test_run_status_classifies_the_whole_round():
    assert run_status(None, targets=3, failed_entities=[]).status == "ok"
    degraded = run_status(None, targets=3, failed_entities=["a", "a", "node:summary"])
    assert (degraded.status, degraded.targets, degraded.failed) == ("degraded", 3, 2) and "a" in degraded.message
    assert run_status(InfrastructureError("資料斷流"), targets=2, failed_entities=["a"]).status == "infrastructure_error"
    assert run_status(WorkflowError("YAML 寫錯"), targets=0, failed_entities=[]).status == "workflow_error"
    assert run_status(RuntimeError("boom"), targets=1, failed_entities=[]).status == "error"
    assert {"ok", "no_data", "never_seen", "config_error"} <= set(STREAM_STATUSES)
    assert {"infrastructure_error", "workflow_error"} <= set(RUN_STATUSES)


# ── 狀態區間：沒變只推最後確認時間 ──


def test_unchanged_status_extends_one_interval_instead_of_a_row_per_round(source):
    pipeline = _pipeline(["a", "b"])
    for m in range(5):
        pipeline.run(now=_t(m))
    assert _streams() == [("svc-a", "ok", _naive(0), _naive(4), 5, True), ("svc-b", "ok", _naive(0), _naive(4), 5, True)]
    assert _runs() == [("ok", _naive(0), _naive(4), 5, True)]


def test_outage_and_recovery_leave_three_intervals_that_prove_when_there_was_no_data(source):
    pipeline = _pipeline(["a", "b"])
    plan = ["ok", "ok", "empty", "empty", "empty", "ok"]
    for m, mode in enumerate(plan):
        source["a"] = mode
        pipeline.run(now=_t(m))
    assert _streams(entity="svc-a") == [("svc-a", "ok", _naive(0), _naive(1), 2, False),
                                         ("svc-a", "no_data", _naive(2), _naive(4), 3, False),
                                         ("svc-a", "ok", _naive(5), _naive(5), 1, True)]
    assert _streams(entity="svc-b") == [("svc-b", "ok", _naive(0), _naive(5), 6, True)]  # 沒出事的那條不受影響
    # 事後查「11:03 那個時間點 svc-a 有沒有資料」：落在 no_data 那一段
    [row] = StreamHealth.list(entity="svc-a", since=_t(3), until=_t(3))
    assert row["status"] == "no_data" and "沒有資料" in row["message"] and row["source_type"] == "mock"
    assert _runs() == [("ok", _naive(0), _naive(1), 2, False), ("degraded", _naive(2), _naive(4), 3, False),
                       ("ok", _naive(5), _naive(5), 1, True)]


def test_a_stream_that_never_had_data_is_told_apart_from_one_that_stopped(source):
    source["new"] = "empty"  # 從第一輪就沒有資料
    pipeline = _pipeline(["new", "old", "fine"])  # fine 一直有資料：不讓「全部斷流」把整輪變成失敗
    pipeline.run(now=_t(0))
    source["old"] = "empty"  # 有過資料之後斷流
    pipeline.run(now=_t(1))
    current = {e: s for e, s, *_ in _streams(current=True)}
    assert current == {"svc-new": "never_seen", "svc-old": "no_data", "svc-fine": "ok"}
    source["new"] = "ok"
    pipeline.run(now=_t(2))
    source["new"] = "empty"
    pipeline.run(now=_t(3))
    assert [s for _, s, *_ in _streams(entity="svc-new")] == ["never_seen", "ok", "no_data"]  # 有過資料之後就不再是「從未」


def test_a_stream_with_stored_anomalies_from_before_this_feature_is_not_never_seen(source):
    """加這個功能之前就在跑的資料流沒有健康度紀錄：已經存過異常點（或基準線）就算「以前有過資料」。"""
    Anomalies.create_many([{"pipeline": "metrics_only", "entity": "svc-a", "metric": "a", "ts": _t(-600), "value": 1.0,
                            "score": 9.0, "detector_name": "detector.zscore", "detector_node": "detect",
                            "level": "anomaly", "attrs": {}}])
    source["a"] = source["b"] = "empty"
    _pipeline(["a", "b", "c"]).run(now=_t(0))
    assert {e: s for e, s, *_ in _streams()} == {"svc-a": "no_data", "svc-b": "never_seen", "svc-c": "ok"}


def test_an_empty_frame_counts_as_no_data_too(source):
    pipeline = _pipeline(["a", "b"])
    pipeline.run(now=_t(0))
    source["a"] = "zero"  # 資料源回 200，但零筆
    pipeline.run(now=_t(1))
    [row] = StreamHealth.list(entity="svc-a", current=True)
    assert row["status"] == "no_data" and "零筆" in row["message"]


def test_a_bad_query_is_a_configuration_error_with_the_source_message(source):
    source["a"] = "bad"
    ctx = _pipeline(["a", "b"]).run(now=_t(0))
    assert [e.entity for e in ctx.errors] == ["svc-a"]
    [row] = StreamHealth.list(status="config_error")
    assert (row["entity"], row["query"], row["is_current"]) == ("svc-a", "a", True)
    assert "欄位 latency_ms 不存在" in row["message"]
    [run_row] = RunHealth.list()
    assert (run_row["status"], run_row["targets"], run_row["failed_targets"]) == ("degraded", 2, 1)


def test_an_unexpected_exception_from_the_source_is_an_error_with_its_type(source):
    source["a"] = "crash"
    _pipeline(["a", "b"]).run(now=_t(0))
    [row] = StreamHealth.list(entity="svc-a")
    assert row["status"] == "error" and row["message"].startswith("KeyError")


@OperatorRegistry.register("data_health_test.bug_after_fetch")
class _BugAfterFetch(BaseOperator):
    scope = "target"

    def execute(self, ctx):
        if ctx.target.entity == "svc-a":
            raise ValueError("偵測器 bug")
        return ctx


def test_a_detector_bug_after_the_data_arrived_does_not_blame_the_data_stream(source):
    wf = Workflow(name="health_bug", targets=[TargetSpec(source_type="mock", query=q, entity=f"svc-{q}") for q in "ab"],
                  nodes=[NodeSpec(name="fetch", operator="ingestion.fetch"),
                         NodeSpec(name="bug", operator="data_health_test.bug_after_fetch")])
    ctx = Pipeline(wf, StorageHooks()).run(now=_t(0))
    assert [e.entity for e in ctx.errors] == ["svc-a"]
    assert {e: s for e, s, *_ in _streams()} == {"svc-a": "ok", "svc-b": "ok"}  # 資料有進來
    assert [r[0] for r in _runs()] == ["degraded"]  # 但這一輪有序列出錯，管線層級看得到


# ── 整輪失敗也要留下紀錄 ──


def test_starvation_fails_the_round_but_the_history_still_says_which_streams_had_no_data(source):
    pipeline = _pipeline(["a", "b"])
    pipeline.run(now=_t(0))
    source["a"] = source["b"] = "empty"
    for m in (1, 2):
        with pytest.raises(InfrastructureError, match="資料斷流"):
            pipeline.run(now=_t(m))
    assert _streams(status="no_data") == [("svc-a", "no_data", _naive(1), _naive(2), 2, True),
                                          ("svc-b", "no_data", _naive(1), _naive(2), 2, True)]
    assert _runs() == [("ok", _naive(0), _naive(0), 1, False), ("infrastructure_error", _naive(1), _naive(2), 2, True)]
    [failed] = RunHealth.list(status="infrastructure_error")
    assert "資料斷流" in failed["message"] and "2/2" in failed["message"]


def test_an_unreachable_source_marks_that_stream_and_keeps_what_was_already_checked(source):
    source["b"] = "down"
    with pytest.raises(InfrastructureError, match="連不上"):
        _pipeline(["a", "b", "c"]).run(now=_t(0))
    # a 在 b 之前已經檢查過；b 連不上；c 還沒輪到（沒有紀錄 = 沒有檢查到，不是正常）
    assert {e: s for e, s, *_ in _streams()} == {"svc-a": "ok", "svc-b": "unreachable"}
    [failed] = RunHealth.list()
    assert failed["status"] == "infrastructure_error" and "連不上" in failed["message"]


def test_a_broken_workflow_definition_is_recorded_even_though_nothing_ran(tmp_path):
    """YAML 載入期的 WorkflowError：管線根本沒跑，由排程入口用的 runner.run_scheduled 補記一筆管線層級的 workflow_error。"""
    path = tmp_path / "nightly_factory.yaml"
    good = {"name": "nightly_factory", "targets": [{"source_type": "mock", "query": "q", "entity": "svc"}],
            "nodes": [{"name": "fetch", "operator": "ingestion.fetch"}]}
    path.write_text(json.dumps(good | {"nodes": [{"name": "fetch", "operator": "ingestion.fetch",
                                                 "params": {"lookback_minutes": -5}}]}), encoding="utf-8")  # YAML 是 JSON 的超集
    with pytest.raises(WorkflowError):
        run_scheduled(str(path), _t(0))
    assert run_once(str(path), _t(1)) == 1  # 排程入口本身：結束碼非 0，紀錄照留
    [row] = RunHealth.list(pipeline="nightly_factory")
    assert (row["status"], row["runs"], row["started_at"], row["last_checked_at"]) == ("workflow_error", 2, _naive(0), _naive(1))
    assert "lookback_minutes" in row["message"] and StreamHealth.list() == []
    path.write_text(json.dumps(good), encoding="utf-8")  # 修好之後：同一條管線接上 ok，載入失敗那段留在歷史裡
    assert run_once(str(path), _t(2)) == 0
    assert _runs(pipeline="nightly_factory") == [("workflow_error", _naive(0), _naive(1), 2, False),
                                                 ("ok", _naive(2), _naive(2), 1, True)]


def test_an_unknown_workflow_name_is_the_callers_mistake_and_is_not_recorded():
    with pytest.raises(NotFound):
        run_scheduled("no_such_pipeline", _t(0))
    assert RunHealth.list() == []


def test_api_and_manual_runs_do_not_record_a_broken_definition(tmp_path):
    """只有排程補記載入失敗：API／手動動作的設定錯誤直接回給呼叫的人，而且維持「報錯之前不碰資料庫」
    （tests/test_fail_fast.py 的保證；這裡確認健康度沒有偷偷寫一筆）。"""
    path = tmp_path / "broken.yaml"
    path.write_text(json.dumps({"name": "broken", "nodes": [{"name": "x", "operator": "detector.nope"}]}), encoding="utf-8")
    with pytest.raises(WorkflowError):
        run_context(str(path), now=_t(0), strict=False, allow_file=True)
    assert RunHealth.list() == []


# ── 寫不進去、不該記的情況 ──


def test_when_the_health_record_cannot_be_written_the_round_keeps_its_own_outcome(source, caplog):
    """已知限制：資料庫連不上時健康度也寫不進去。那一輪原本的結果（成功／原本的例外）不變，只留一行 log。"""
    class NoHealth(StorageHooks):
        def record_health(self, ctx):
            raise InfrastructureError("資料庫無法使用")

    ctx = _pipeline(["a"], NoHealth()).run(now=_t(0))
    assert ctx.errors == [] and ctx.run_check.status == "ok"
    assert "資料健康度寫入失敗" in caplog.text and StreamHealth.list() == [] and RunHealth.list() == []
    source["a"] = "empty"
    with pytest.raises(InfrastructureError, match="資料斷流"):  # 原本的例外照丟，不被紀錄失敗蓋掉
        _pipeline(["a"], NoHealth()).run(now=_t(1))


def test_manual_actions_and_rca_do_not_write_health_records(source):
    """只記排程模式（strict=False）而且有逐序列節點的管線：手動動作的失敗直接回給呼叫的人，不進健康度歷史。"""
    with pytest.raises(NotFound):
        run("admin_incident_get", request=IdRef(id=999))
    source["a"] = "empty"
    with pytest.raises(NoDataError):
        _pipeline(["a"]).run(now=_t(0), strict=True)
    assert StreamHealth.list() == [] and RunHealth.list() == []


def test_replaying_an_older_time_does_not_rewrite_history(source):
    pipeline = _pipeline(["a"])
    pipeline.run(now=_t(10))
    source["a"] = "bad"
    pipeline.run(now=_t(5))  # 回放較早的時間：不寫
    assert _streams() == [("svc-a", "ok", _naive(10), _naive(10), 1, True)]
    assert _runs() == [("ok", _naive(10), _naive(10), 1, True)]


def test_checks_live_on_the_per_round_context_and_do_not_leak_between_rounds(source):
    """CLAUDE.md §100.3：常駐 loop 重複用同一個 Pipeline 物件，每一輪的檢查結果只屬於那一輪的 ctx。"""
    pipeline = _pipeline(["a", "b"])
    first = pipeline.run(now=_t(0))
    source["a"] = "bad"
    second = pipeline.run(now=_t(1))
    assert [(c.entity, c.status) for c in first.stream_checks] == [("svc-a", "ok"), ("svc-b", "ok")]
    assert [(c.entity, c.status) for c in second.stream_checks] == [("svc-a", "config_error"), ("svc-b", "ok")]
    assert first.run_check.status == "ok" and second.run_check.status == "degraded"
    assert not any(isinstance(v, list) and v and hasattr(v[0], "status") for v in vars(pipeline).values())


# ── 查詢 API ──


def test_api_lists_current_problems_and_point_in_time_history(source, client):
    pipeline = _pipeline(["a", "b"])
    pipeline.run(now=_t(0))
    source["a"], source["b"] = "empty", "bad"
    pipeline.run(now=_t(1))
    current = client.get("/api/v1/data-health/streams", params={"current": "true"}).json()["result"]
    assert {(r["entity"], r["status"]) for r in current} == {("svc-a", "no_data"), ("svc-b", "config_error")}
    bad = client.get("/api/v1/data-health/streams", params={"status": "config_error", "pipeline": "metrics_only"}).json()["result"]
    assert [r["entity"] for r in bad] == ["svc-b"] and bad[0]["query"] == "b"
    at_t0 = client.get("/api/v1/data-health/streams", params={"entity": "svc-a", "since": _t(0).isoformat(),
                                                              "until": _t(0).isoformat()}).json()["result"]
    assert [r["status"] for r in at_t0] == ["ok"]
    runs = client.get("/api/v1/data-health/runs", params={"pipeline": "metrics_only"}).json()["result"]
    assert [r["status"] for r in runs] == ["degraded", "ok"]  # 由新到舊
    assert client.get("/api/v1/data-health/streams", params={"limit": 0}).status_code == 422


def test_api_requires_the_token(source):
    from fastapi.testclient import TestClient

    from aiops.interfaces.app import create_app

    anonymous = TestClient(create_app())
    assert anonymous.get("/api/v1/data-health/streams").status_code == 401
    assert anonymous.get("/api/v1/data-health/runs").status_code == 401


# ── migration 與 ORM 一致 ──


def test_migration_creates_the_same_columns_as_the_models():
    sql = (ROOT / "migrations" / "postgres" / "0011_data_health.sql").read_text(encoding="utf-8")
    for model in (DataStreamHealthRow, PipelineRunHealthRow):
        body = re.search(rf"CREATE TABLE IF NOT EXISTS {model.__tablename__} \((.*?)\n\);", sql, re.S).group(1)
        declared = {line.split()[0]: line for line in (ln.strip().rstrip(",") for ln in body.splitlines()) if line}
        assert set(declared) == {c.name for c in model.__table__.columns}, model.__tablename__
        for column in model.__table__.columns:
            length = getattr(column.type, "length", None)
            assert length is None or f"VARCHAR({length})" in declared[column.name], column.name
            assert column.primary_key or "NOT NULL" in declared[column.name], column.name
        for index in model.__table__.indexes:
            assert f"CREATE INDEX IF NOT EXISTS {index.name} ON {model.__tablename__} ({', '.join(c.name for c in index.columns)})" in sql
    assert "GRANT SELECT ON data_stream_health, pipeline_run_health" in sql  # 唯讀帳號只開這兩張表


# ── Grafana 儀表板：SQL 對 ORM 建出來的表實際執行 ──


def _dashboard() -> dict:
    return json.loads(DASHBOARD.read_text(encoding="utf-8"))


def _sql(raw: str, *, pipelines=("metrics_only",), services=("svc-a", "svc-b"), scope="problem",
         time_from=_t(-60), time_to=_t(60)) -> str:
    """把 Grafana 的巨集與變數換成它實際會展開的樣子（多選變數 = 逗號分隔的單引號字串），並把 PostgreSQL 的
    時間加減換成 SQLite 的寫法——只有這一處方言差異，其餘（資料表、欄位、CASE、UNION、EXISTS）原樣執行。"""
    quoted = lambda values: ", ".join(f"'{v}'" for v in values)  # noqa: E731
    stamp = lambda t: f"'{t.strftime('%Y-%m-%d %H:%M:%S')}'"  # noqa: E731
    out = (raw.replace("$__timeFrom()", stamp(time_from)).replace("$__timeTo()", stamp(time_to))
           .replace("$pipeline", quoted(pipelines)).replace("$service", quoted(services)).replace("$scope", scope))
    out = out.replace("h.last_checked_at + interval '1 second'", "datetime(h.last_checked_at, '+1 second')")
    assert "$" not in out and "interval" not in out, out
    return out


def _query(raw: str, **kw) -> list[dict]:
    with db.read_session() as s:
        return [dict(r._mapping) for r in s.execute(text(_sql(raw, **kw)))]


def _panels() -> dict[int, dict]:
    return {p["id"]: p for p in _dashboard()["panels"]}


def _raw(panel_id: int) -> str:
    [target] = _panels()[panel_id]["targets"]
    return target["rawSql"]


def test_dashboard_loads_and_every_query_runs_against_the_real_tables():
    board = _dashboard()
    assert board["uid"] == "data-health" and board["editable"] is False and board["time"]["from"].startswith("now-")
    statements = [(f"panel {p['id']}", t["rawSql"]) for p in board["panels"] for t in p["targets"]]
    statements += [(f"var {v['name']}", v["query"]) for v in board["templating"]["list"] if v["type"] == "query"]
    assert len(statements) == 9  # 7 個面板 + 2 個下拉選單
    for name, raw in statements:
        _query(raw)  # 資料表或欄位不存在會在這裡丟 OperationalError
        tables = set(re.findall(r"(?:FROM|JOIN)\s+([a-z_]+)", raw))
        assert tables and tables <= {"data_stream_health", "pipeline_run_health"}, f"{name} 讀了別的表：{tables}"
        assert not re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT)\b", raw, re.I), name
    for p in board["panels"]:
        assert p["datasource"] == {"type": "grafana-postgresql-datasource", "uid": "aiops-platform-db"}, p["title"]
        assert p["type"] in ("table", "state-timeline") and p["description"]
        [target] = p["targets"]
        assert target["format"] == ("time_series" if p["type"] == "state-timeline" else "table")


def test_dashboard_variables_are_multi_select_dropdowns_filled_from_sql_with_all_as_default():
    variables = {v["name"]: v for v in _dashboard()["templating"]["list"]}
    for name in ("pipeline", "service"):
        v = variables[name]
        assert v["type"] == "query" and v["multi"] is True and v["includeAll"] is True
        assert v["current"]["value"] == ["$__all"]  # 預設 All：不用先選就看得到
        assert v["datasource"]["uid"] == "aiops-platform-db"
    assert "$pipeline" in variables["service"]["query"]  # 服務清單跟著管線篩選


def test_dashboard_queries_return_the_right_rows(source):
    pipeline = _pipeline(["a", "b", "c"])
    pipeline.run(now=_t(0))
    source["a"], source["b"] = "empty", "bad"
    pipeline.run(now=_t(1))
    pipeline.run(now=_t(2))
    all_services = ("svc-a", "svc-b", "svc-c")

    # 下拉選單
    assert [r["pipeline"] for r in _query(_dashboard()["templating"]["list"][0]["query"])] == ["metrics_only"]
    assert [r["entity"] for r in _query(_dashboard()["templating"]["list"][1]["query"])] == list(all_services)

    # 面板 1：目前有問題的資料流（狀態、訊息、從什麼時候開始）——不含正常的 svc-c
    problems = _query(_raw(1), services=all_services)
    assert [(r["服務"], r["狀態"], r["連續幾輪"]) for r in problems] == [("svc-a", "no_data", 2), ("svc-b", "config_error", 2)]
    assert all(str(r["從什麼時候開始"]).startswith("2026-11-08 11:01") and r["訊息"] for r in problems)
    assert [r["服務"] for r in _query(_raw(1), services=("svc-b",))] == ["svc-b"]  # 下拉選單往下看單一服務

    # 面板 2：管線目前的執行結果
    [current_run] = _query(_raw(2))
    assert (current_run["狀態"], current_run["序列數"], current_run["出錯的序列"]) == ("degraded", 3, 2)

    # 面板 3：狀態時間軸。每條資料流的狀態依時間是 ok(0) -> 斷流(1)／設定錯誤(3)，最新一段之後用 -1（沒有紀錄）收尾
    points = _query(_raw(3), services=all_services)
    by_stream: dict[str, list] = {}
    for r in points:
        by_stream.setdefault(r["metric"], []).append(r["value"])
    assert set(by_stream) == {"metrics_only / svc-a / a", "metrics_only / svc-b / b"}  # 預設只畫出過問題的
    assert by_stream["metrics_only / svc-a / a"] == [0, 1, 1, -1]
    assert by_stream["metrics_only / svc-b / b"] == [0, 3, 3, -1]
    everything = {r["metric"] for r in _query(_raw(3), services=all_services, scope="all")}
    assert "metrics_only / svc-c / c" in everything
    assert [str(r["time"])[:16] for r in points] == sorted(str(r["time"])[:16] for r in points)  # 依時間排好

    # 面板 4、5：管線每輪的執行結果
    assert [r["value"] for r in _query(_raw(4))] == [0, 1, 1, -1]
    assert [(r["狀態"], r["幾輪"]) for r in _query(_raw(5))] == [("degraded", 2), ("ok", 1)]

    # 面板 6：設定錯誤清單；面板 7：出問題的紀錄
    assert [(r["層級"], r["服務"]) for r in _query(_raw(6), services=all_services)] == [("資料流", "svc-b")]
    # 兩段同一時間開始，先後順序不保證：比內容
    assert sorted((r["服務"], r["狀態"]) for r in _query(_raw(7), services=all_services)) == [("svc-a", "no_data"), ("svc-b", "config_error")]

    # 時間範圍外的區間不出現
    assert _query(_raw(7), services=all_services, time_from=_t(100), time_to=_t(200)) == []


def test_dashboard_status_mappings_cover_every_status_the_platform_can_write():
    """畫面上每個狀態都有中文標示與顏色：平台多一種狀態而儀表板沒跟上時這裡會紅。"""
    panels = _panels()

    def mapped(panel_id: int) -> set[str]:
        overrides = panels[panel_id]["fieldConfig"]["overrides"]
        status = next(o for o in overrides if o["matcher"]["options"] == "狀態")
        return set(next(p for p in status["properties"] if p["id"] == "mappings")["value"][0]["options"])

    assert mapped(1) == set(STREAM_STATUSES) and mapped(7) == set(STREAM_STATUSES)
    assert mapped(2) == set(RUN_STATUSES) and mapped(5) == set(RUN_STATUSES)
    for panel_id, statuses in ((3, STREAM_STATUSES), (4, RUN_STATUSES)):
        defaults = panels[panel_id]["fieldConfig"]["defaults"]
        numbers = defaults["mappings"][0]["options"]
        raw = _raw(panel_id)
        assert set(numbers) == {str(n) for n in range(len(statuses))} | {"-1"}  # -1 = 最後一次檢查之後沒有紀錄
        assert numbers["-1"]["color"] == "transparent" and "-1 AS v" in raw
        # 多條序列合併後別的序列的時間點在這條序列上是空值：不接起來的話色塊會被切斷（在 Grafana 13.0.7 上實際看過）
        assert defaults["custom"]["spanNulls"] is True
        for status in statuses:
            n = re.search(rf"WHEN '{status}' THEN (\d+)", raw).group(1)
            assert n in numbers, f"面板 {panel_id} 的 {status} 沒有對應的顯示文字"


def test_overlapping_rounds_never_leave_two_current_rows_for_one_stream():
    """兩輪重疊執行（排程重疊）各開了一列時，下一次寫入只留最新的當「目前狀態」；舊的關掉，不會永遠掛在「目前有問題」。"""
    from aiops.storage import crud

    key = {"pipeline": "p", "entity": "svc", "query": "q"}
    for status in ("no_data", "ok"):  # 模擬併發：兩列都標成目前狀態
        crud.create(DataStreamHealthRow, **key, source_type="mock", status=status, message="", started_at=_t(0),
                    last_checked_at=_t(0), checks=1, is_current=True)
    StreamHealth.record("p", "svc", "q", "mock", "ok", "", _t(1))
    assert [(r["status"], r["checks"]) for r in StreamHealth.list(current=True)] == [("ok", 2)]
    assert [r["status"] for r in StreamHealth.list(current=False)] == ["no_data"]
