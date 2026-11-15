"""資料健康度：三張表（aiops/engine/health.py 判狀態、StorageHooks.record_health 寫入、GET /api/v1/data-health/* 查詢）
  data_stream_health      每條資料流（管線 × 實體 × 查詢）的狀態區間
  pipeline_runs           管線每一輪的執行結果（一輪一列）
  data_stream_first_seen  每條資料流第一次收到資料的時間

要驗的事：事後查得到「那個時間點資料有沒有進來、是誰的問題」——斷流、從未有過資料、查詢寫錯、資料源連不上、
整輪失敗、管線定義載入失敗各自分得出來；資料流的狀態沒變不會每輪多一列；超過保留期限的舊紀錄會被清掉、
清掉之後仍分得出「從未有過資料」；資料庫寫不進去時不影響那一輪原本的結果。
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
from aiops.engine.health import FAILURE_TYPES, RUN_STATUSES, STREAM_STATUSES, run_status, stream_status
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
from aiops.storage.models import DataStreamFirstSeenRow, DataStreamHealthRow, PipelineRunRow
from aiops.storage.repository import Anomalies, PipelineRuns, StreamFirstSeen, StreamHealth

ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 11, 8, 11, 0, tzinfo=timezone.utc)
DASHBOARD = ROOT / "deploy" / "helm" / "aiops-grafana" / "files" / "dashboards" / "data-health.json"
GRAFANA_TABLES = {"data_stream_health", "pipeline_runs"}  # 唯讀帳號讀得到的表（data_stream_first_seen 不開放）


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
    """(輪次時間, 結果)，由舊到新。結果 = 失敗類型（failed 才有），否則 status。"""
    rows = sorted(PipelineRuns.list(**filters), key=lambda r: (r["pipeline"], r["run_at"]))
    assert all((r["status"] == "failed") == bool(r["failure_type"]) for r in rows)  # 失敗類型只有 failed 才有
    return [(r["run_at"], r["failure_type"] or r["status"]) for r in rows]


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
    ok = run_status(None, targets=3, failed_entities=[])
    assert (ok.status, ok.failure_type) == ("ok", "")
    degraded = run_status(None, targets=3, failed_entities=["a", "a", "node:summary"])
    assert (degraded.status, degraded.failure_type, degraded.targets, degraded.failed) == ("degraded", "", 3, 2)
    assert "a" in degraded.message

    def failed(error):
        check = run_status(error, targets=2, failed_entities=["a"])
        return check.status, check.failure_type

    assert failed(InfrastructureError("資料斷流")) == ("failed", "infrastructure_error")
    assert failed(WorkflowError("YAML 寫錯")) == ("failed", "workflow_error")
    assert failed(RuntimeError("boom")) == ("failed", "error")
    assert run_status(RuntimeError("boom"), targets=1, failed_entities=[]).message.startswith("RuntimeError")
    assert RUN_STATUSES == ("ok", "degraded", "failed") and set(FAILURE_TYPES) == {"infrastructure_error", "workflow_error", "error"}
    assert {"ok", "no_data", "never_seen", "config_error"} <= set(STREAM_STATUSES)


# ── 資料流：狀態區間（沒變只推最後確認時間）；管線：一輪一列 ──


def test_unchanged_stream_status_extends_one_interval_while_every_round_gets_its_own_run_row(source):
    pipeline = _pipeline(["a", "b"])
    for m in range(5):
        pipeline.run(now=_t(m))
    assert _streams() == [("svc-a", "ok", _naive(0), _naive(4), 5, True), ("svc-b", "ok", _naive(0), _naive(4), 5, True)]
    assert _runs() == [(_naive(m), "ok") for m in range(5)]


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
    assert [r for _, r in _runs()] == ["ok", "ok", "degraded", "degraded", "degraded", "ok"]


def test_a_round_that_never_ran_is_visible_as_a_missing_run_row(source):
    """一輪一列的用意：排程少跑的那幾輪，那個時間就是沒有列（狀態區間看不出這件事）。"""
    pipeline = _pipeline(["a"])
    for m in (0, 5, 20, 25):  # 11:10、11:15 兩輪沒跑
        pipeline.run(now=_t(m))
    assert [at for at, _ in _runs()] == [_naive(0), _naive(5), _naive(20), _naive(25)]
    assert PipelineRuns.list(since=_t(6), until=_t(19)) == []
    assert _streams() == [("svc-a", "ok", _naive(0), _naive(25), 4, True)]  # 區間照樣連著：只能從輪數（4）看出不對


def test_rerunning_the_same_round_overwrites_its_run_row(source):
    pipeline = _pipeline(["a", "b"])
    pipeline.run(now=_t(0))
    source["a"] = "bad"
    pipeline.run(now=_t(0))
    assert _runs() == [(_naive(0), "degraded")]


# ── 「從未有過資料」：另一張表記第一次收到資料的時間 ──


def test_a_stream_that_never_had_data_is_told_apart_from_one_that_stopped(source):
    source["new"] = "empty"  # 從第一輪就沒有資料
    pipeline = _pipeline(["new", "old", "fine"])  # fine 一直有資料：不讓「全部斷流」把整輪變成失敗
    pipeline.run(now=_t(0))
    source["old"] = "empty"  # 有過資料之後斷流
    pipeline.run(now=_t(1))
    current = {e: s for e, s, *_ in _streams(current=True)}
    assert current == {"svc-new": "never_seen", "svc-old": "no_data", "svc-fine": "ok"}
    assert {r["entity"] for r in StreamFirstSeen.list()} == {"svc-old", "svc-fine"}
    source["new"] = "ok"
    pipeline.run(now=_t(2))
    source["new"] = "empty"
    pipeline.run(now=_t(3))
    assert [s for _, s, *_ in _streams(entity="svc-new")] == ["never_seen", "ok", "no_data"]  # 有過資料之後就不再是「從未」


def test_first_seen_keeps_the_first_time_and_is_written_once(source):
    pipeline = _pipeline(["a"])
    for m in range(3):
        pipeline.run(now=_t(m))
    [row] = StreamFirstSeen.list()
    assert (row["pipeline"], row["entity"], row["query"], row["first_data_at"]) == ("metrics_only", "svc-a", "a", _naive(0))
    assert StreamFirstSeen.record_many("metrics_only", [("svc-a", "a")], _t(9)) == 1  # 再寫一次不會改掉最早那一次
    assert [r["first_data_at"] for r in StreamFirstSeen.list()] == [_naive(0)]


def test_a_stream_with_stored_anomalies_from_before_this_feature_is_not_never_seen(source):
    """加這個功能之前就在跑的資料流沒有「第一次收到資料」的紀錄：已經存過異常點（或基準線）就算「以前有過資料」。"""
    Anomalies.create_many([{"pipeline": "metrics_only", "entity": "svc-a", "metric": "a", "ts": _t(-600), "value": 1.0,
                            "score": 9.0, "detector_name": "detector.zscore", "detector_node": "detect",
                            "level": "anomaly", "attrs": {}}])
    source["a"] = source["b"] = "empty"
    _pipeline(["a", "b", "c"]).run(now=_t(0))
    assert {e: s for e, s, *_ in _streams()} == {"svc-a": "no_data", "svc-b": "never_seen", "svc-c": "ok"}


# ── 各種狀態 ──


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
    [run_row] = PipelineRuns.list()
    assert (run_row["status"], run_row["failure_type"], run_row["targets"], run_row["failed_targets"]) == ("degraded", "", 2, 1)


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
    assert [r for _, r in _runs()] == ["degraded"]  # 但這一輪有序列出錯，管線層級看得到


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
    assert _runs() == [(_naive(0), "ok"), (_naive(1), "infrastructure_error"), (_naive(2), "infrastructure_error")]
    failed = PipelineRuns.list(status="failed", failure_type="infrastructure_error")
    assert len(failed) == 2 and all("資料斷流" in r["message"] and "2/2" in r["message"] for r in failed)


def test_an_unreachable_source_marks_that_stream_and_keeps_what_was_already_checked(source):
    source["b"] = "down"
    with pytest.raises(InfrastructureError, match="連不上"):
        _pipeline(["a", "b", "c"]).run(now=_t(0))
    # a 在 b 之前已經檢查過；b 連不上；c 還沒輪到（沒有紀錄 = 沒有檢查到，不是正常）
    assert {e: s for e, s, *_ in _streams()} == {"svc-a": "ok", "svc-b": "unreachable"}
    [failed] = PipelineRuns.list()
    assert (failed["status"], failed["failure_type"]) == ("failed", "infrastructure_error") and "連不上" in failed["message"]


def test_a_broken_workflow_definition_is_recorded_even_though_nothing_ran(tmp_path):
    """YAML 載入期的 WorkflowError：管線根本沒跑，由排程入口用的 runner.run_scheduled 補記一列 workflow_error。"""
    path = tmp_path / "nightly_factory.yaml"
    good = {"name": "nightly_factory", "targets": [{"source_type": "mock", "query": "q", "entity": "svc"}],
            "nodes": [{"name": "fetch", "operator": "ingestion.fetch"}]}
    path.write_text(json.dumps(good | {"nodes": [{"name": "fetch", "operator": "ingestion.fetch",
                                                 "params": {"lookback_minutes": -5}}]}), encoding="utf-8")  # YAML 是 JSON 的超集
    with pytest.raises(WorkflowError):
        run_scheduled(str(path), _t(0))
    assert run_once(str(path), _t(1)) == 1  # 排程入口本身：結束碼非 0，紀錄照留
    rows = PipelineRuns.list(pipeline="nightly_factory")
    assert [(r["status"], r["failure_type"], r["targets"]) for r in rows] == [("failed", "workflow_error", 0)] * 2
    assert all("lookback_minutes" in r["message"] for r in rows) and StreamHealth.list() == []
    path.write_text(json.dumps(good), encoding="utf-8")  # 修好之後：同一條管線接上 ok，載入失敗那兩輪留在歷史裡
    assert run_once(str(path), _t(2)) == 0
    assert _runs(pipeline="nightly_factory") == [(_naive(0), "workflow_error"), (_naive(1), "workflow_error"), (_naive(2), "ok")]


def test_an_unknown_workflow_name_is_the_callers_mistake_and_is_not_recorded():
    with pytest.raises(NotFound):
        run_scheduled("no_such_pipeline", _t(0))
    assert PipelineRuns.list() == []


def test_api_and_manual_runs_do_not_record_a_broken_definition(tmp_path):
    """只有排程補記載入失敗：API／手動動作的設定錯誤直接回給呼叫的人，而且維持「報錯之前不碰資料庫」
    （tests/test_fail_fast.py 的保證；這裡確認健康度沒有偷偷寫一筆）。"""
    path = tmp_path / "broken.yaml"
    path.write_text(json.dumps({"name": "broken", "nodes": [{"name": "x", "operator": "detector.nope"}]}), encoding="utf-8")
    with pytest.raises(WorkflowError):
        run_context(str(path), now=_t(0), strict=False, allow_file=True)
    assert PipelineRuns.list() == []


# ── 保留期限 ──


def test_records_older_than_the_retention_are_pruned_on_every_write(source):
    """每輪寫入時順手清掉這條管線超過 health_retention_days 的紀錄（跟拓撲刷新寫完就 prune 同一個做法）。
    清的是：已經結束的狀態區間、舊的輪次。不清的是：每條資料流目前的那一列、第一次收到資料的紀錄。"""
    pipeline = _pipeline(["a", "b"], health_retention_days=7)
    day = 24 * 60
    pipeline.run(now=_t(0))               # 第 0 天：都正常
    source["a"] = "empty"
    pipeline.run(now=_t(1 * day))         # 第 1 天：a 斷流（開了第二段）
    source["a"] = "ok"
    pipeline.run(now=_t(2 * day))         # 第 2 天：a 恢復（第三段）
    assert len(_streams(entity="svc-a")) == 3 and len(_runs()) == 3
    pipeline.run(now=_t(8 * day + 60))    # 第 8 天多：第 0、1 天的東西超過 7 天
    # a 的前兩段（最後確認在第 0、1 天）被清掉，目前那一段留著；b 一直是同一段（目前狀態）不受影響
    assert _streams(entity="svc-a") == [("svc-a", "ok", _naive(2 * day), _naive(8 * day + 60), 2, True)]
    assert _streams(entity="svc-b") == [("svc-b", "ok", _naive(0), _naive(8 * day + 60), 4, True)]
    assert [at for at, _ in _runs()] == [_naive(2 * day), _naive(8 * day + 60)]
    assert {r["entity"]: r["first_data_at"] for r in StreamFirstSeen.list()} == {"svc-a": _naive(0), "svc-b": _naive(0)}


def test_after_pruning_a_stream_that_stops_is_still_no_data_not_never_seen(source):
    """狀態區間會被清掉，所以「以前有沒有過資料」不能靠它：第一次收到資料的紀錄不受保留期限影響。"""
    wf = Workflow(name="plain_fetch", health_retention_days=1,
                  targets=[TargetSpec(source_type="mock", query=q, entity=f"svc-{q}") for q in "ab"],
                  nodes=[NodeSpec(name="fetch", operator="ingestion.fetch")])  # 沒有偵測器：沒有基準線可以當旁證
    pipeline = Pipeline(wf, StorageHooks())
    day = 24 * 60
    pipeline.run(now=_t(0))
    source["a"] = "empty"
    pipeline.run(now=_t(5 * day))
    source["a"] = "ok"
    pipeline.run(now=_t(10 * day))
    source["a"] = "empty"
    pipeline.run(now=_t(15 * day))
    assert _streams(entity="svc-a") == [("svc-a", "no_data", _naive(15 * day), _naive(15 * day), 1, True)]


def test_pruning_only_touches_the_pipeline_that_is_running(source):
    day = 24 * 60
    other = Workflow(name="other_pipeline", targets=[TargetSpec(source_type="mock", query="x", entity="svc-x")],
                     nodes=[NodeSpec(name="fetch", operator="ingestion.fetch")])
    Pipeline(other, StorageHooks()).run(now=_t(0))
    pipeline = _pipeline(["a"], health_retention_days=1)
    pipeline.run(now=_t(0))
    pipeline.run(now=_t(30 * day))
    assert [at for at, _ in _runs(pipeline="metrics_only")] == [_naive(30 * day)]
    assert [at for at, _ in _runs(pipeline="other_pipeline")] == [_naive(0)]  # 別條管線的紀錄不動


def test_retention_is_a_workflow_setting_validated_at_load():
    wf = load_workflow(workflow_path("metrics_only"))
    assert wf.health_retention_days == 180  # 預設
    for bad in (0, -1, 5000):
        with pytest.raises(Exception, match="health_retention_days"):
            Workflow.model_validate(wf.model_dump() | {"health_retention_days": bad})


# ── 寫不進去、不該記的情況 ──


def test_when_the_health_record_cannot_be_written_the_round_keeps_its_own_outcome(source, caplog):
    """已知限制：資料庫連不上時健康度也寫不進去。那一輪原本的結果（成功／原本的例外）不變，只留一行 log。"""
    class NoHealth(StorageHooks):
        def record_health(self, ctx):
            raise InfrastructureError("資料庫無法使用")

    ctx = _pipeline(["a"], NoHealth()).run(now=_t(0))
    assert ctx.errors == [] and ctx.run_check.status == "ok"
    assert "資料健康度寫入失敗" in caplog.text and StreamHealth.list() == [] and PipelineRuns.list() == []
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
    assert StreamHealth.list() == [] and PipelineRuns.list() == [] and StreamFirstSeen.list() == []


def test_replaying_an_older_time_does_not_rewrite_stream_history(source):
    pipeline = _pipeline(["a"])
    pipeline.run(now=_t(10))
    source["a"] = "bad"
    pipeline.run(now=_t(5))  # 回放較早的時間：狀態區間不改寫；那一輪本身照記（一輪一列，不會動到別的列）
    assert _streams() == [("svc-a", "ok", _naive(10), _naive(10), 1, True)]
    assert _runs() == [(_naive(5), "degraded"), (_naive(10), "ok")]


def test_checks_live_on_the_per_round_context_and_do_not_leak_between_rounds(source):
    """CLAUDE.md §100.3：常駐 loop 重複用同一個 Pipeline 物件，每一輪的檢查結果只屬於那一輪的 ctx。"""
    pipeline = _pipeline(["a", "b"])
    first = pipeline.run(now=_t(0))
    source["a"] = "bad"
    second = pipeline.run(now=_t(1))
    assert [(c.entity, c.status, c.known) for c in first.stream_checks] == [("svc-a", "ok", False), ("svc-b", "ok", False)]
    assert [(c.entity, c.status, c.known) for c in second.stream_checks] == [("svc-a", "config_error", True), ("svc-b", "ok", True)]
    assert first.run_check.status == "ok" and second.run_check.status == "degraded"
    assert not any(isinstance(v, list) and v and hasattr(v[0], "status") for v in vars(pipeline).values())


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


# ── 查詢 API ──


def test_api_lists_current_problems_point_in_time_history_runs_and_first_seen(source, client):
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
    only_t0 = client.get("/api/v1/data-health/runs", params={"since": _t(0).isoformat(), "until": _t(0).isoformat()}).json()["result"]
    assert [r["status"] for r in only_t0] == ["ok"]
    assert client.get("/api/v1/data-health/runs", params={"status": "failed"}).json()["result"] == []
    first = client.get("/api/v1/data-health/first-seen", params={"pipeline": "metrics_only"}).json()["result"]
    assert {r["entity"] for r in first} == {"svc-a", "svc-b"}
    assert client.get("/api/v1/data-health/streams", params={"limit": 0}).status_code == 422


def test_api_requires_the_token(source):
    from fastapi.testclient import TestClient

    from aiops.interfaces.app import create_app

    anonymous = TestClient(create_app())
    for path in ("streams", "runs", "first-seen"):
        assert anonymous.get(f"/api/v1/data-health/{path}").status_code == 401, path


# ── migration 與 ORM 一致 ──


def test_migration_creates_the_same_tables_columns_and_indexes_as_the_models():
    sql = (ROOT / "migrations" / "postgres" / "0011_data_health.sql").read_text(encoding="utf-8")
    assert set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", sql)) == {"data_stream_health", "pipeline_runs", "data_stream_first_seen"}
    for model in (DataStreamHealthRow, PipelineRunRow, DataStreamFirstSeenRow):
        table = model.__table__
        body = re.search(rf"CREATE TABLE IF NOT EXISTS {table.name} \((.*?)\n\);", sql, re.S).group(1)
        lines = [ln.strip().rstrip(",") for ln in body.splitlines() if ln.strip()]
        declared = {ln.split()[0]: ln for ln in lines if not ln.startswith("CONSTRAINT")}
        assert set(declared) == {c.name for c in table.columns}, table.name
        for column in table.columns:
            length = getattr(column.type, "length", None)
            assert length is None or f"VARCHAR({length})" in declared[column.name], column.name
            assert column.primary_key or "NOT NULL" in declared[column.name], column.name
        for index in table.indexes:
            columns = ", ".join(c.name for c in index.columns)
            assert f"CREATE INDEX IF NOT EXISTS {index.name} ON {table.name} ({columns})" in sql, index.name
        for constraint in table.constraints:
            if constraint.name and constraint.name.startswith("uq_"):
                columns = ", ".join(c.name for c in constraint.columns)
                assert f"CONSTRAINT {constraint.name} UNIQUE ({columns})" in body, constraint.name
    # 查詢會用到的欄位都有索引：時間、管線、實體、狀態
    stream_indexed = {c.name for i in DataStreamHealthRow.__table__.indexes for c in i.columns}
    assert {"pipeline", "entity", "status", "started_at", "last_checked_at"} <= stream_indexed
    run_keys = {c.name for i in PipelineRunRow.__table__.indexes for c in i.columns} | {"pipeline"}  # 唯一鍵 (pipeline, run_at)
    assert {"pipeline", "run_at"} <= run_keys
    # 唯讀帳號只開 Grafana 要讀的兩張表
    assert "GRANT SELECT ON data_stream_health, pipeline_runs TO aiops_grafana_ro" in sql


# ── Grafana 儀表板：SQL 對 ORM 建出來的表實際執行 ──


def _dashboard() -> dict:
    return json.loads(DASHBOARD.read_text(encoding="utf-8"))


def _sql(raw: str, *, pipelines=("metrics_only",), services=("svc-a", "svc-b"), scope="problem",
         time_from=_t(-60), time_to=_t(60)) -> str:
    """把 Grafana 的巨集與變數換成它實際會展開的樣子（多選變數 = 逗號分隔的單引號字串；$__timeFilter(欄位) =
    欄位 BETWEEN 起 AND 迄），並把三處 PostgreSQL 的時間運算換成 SQLite 的寫法（時間加一秒、依分鐘分格、取 epoch）。
    其餘（資料表、欄位、CASE、JOIN、UNION、EXISTS、GROUP BY）原樣執行。真的 PostgreSQL 上另外驗過（docs/DATA_HEALTH.md）。"""
    quoted = lambda values: ", ".join(f"'{v}'" for v in values)  # noqa: E731
    stamp = lambda t: f"'{t.strftime('%Y-%m-%d %H:%M:%S')}'"  # noqa: E731
    out = re.sub(r"\$__timeFilter\(([\w.]+)\)", rf"\1 BETWEEN {stamp(time_from)} AND {stamp(time_to)}", raw)
    out = (out.replace("$__timeFrom()", stamp(time_from)).replace("$__timeTo()", stamp(time_to))
           .replace("$pipeline", quoted(pipelines)).replace("$service", quoted(services)).replace("$scope", scope))
    out = out.replace("h.last_checked_at + interval '1 second'", "datetime(h.last_checked_at, '+1 second')")
    out = out.replace("$__timeGroup(run_at, $__interval)", "(CAST(strftime('%s', run_at) AS INTEGER) / 60) * 60")
    out = out.replace("extract(epoch from MAX(run_at)) + 1", "CAST(strftime('%s', MAX(run_at)) AS INTEGER) + 1")
    assert "$" not in out and "interval" not in out and "extract(" not in out, out
    return out


def _query(raw: str, **kw) -> list[dict]:
    with db.read_session() as s:
        return [dict(r._mapping) for r in s.execute(text(_sql(raw, **kw)))]


def _panels() -> dict[int, dict]:
    return {p["id"]: p for p in _dashboard()["panels"]}


def _raw(panel_id: int) -> str:
    [target] = _panels()[panel_id]["targets"]
    return target["rawSql"]


def _statements() -> list[tuple[str, str]]:
    board = _dashboard()
    return ([(f"panel {p['id']}", t["rawSql"]) for p in board["panels"] for t in p["targets"]]
            + [(f"var {v['name']}", v["query"]) for v in board["templating"]["list"] if v["type"] == "query"])


def test_dashboard_loads_and_every_query_runs_against_the_real_tables():
    board = _dashboard()
    assert board["uid"] == "data-health" and board["editable"] is False and board["time"]["from"].startswith("now-")
    statements = _statements()
    assert len(statements) == 9  # 7 個面板 + 2 個下拉選單
    for name, raw in statements:
        _query(raw)  # 資料表或欄位不存在會在這裡丟 OperationalError
        tables = set(re.findall(r"(?:FROM|JOIN)\s+([a-z_]+)", raw))
        assert tables and tables <= GRAFANA_TABLES, f"{name} 讀了別的表：{tables}"
        assert not re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT)\b", raw, re.I), name
    for p in board["panels"]:
        assert p["datasource"] == {"type": "grafana-postgresql-datasource", "uid": "aiops-platform-db"}, p["title"]
        assert p["type"] in ("table", "state-timeline") and p["description"]
        [target] = p["targets"]
        assert target["format"] == ("time_series" if p["type"] == "state-timeline" else "table")


def test_every_dashboard_query_is_bounded_by_the_time_range_including_the_dropdowns():
    """Grafana 每次開頁、每次自動重新整理都會下這些 SQL：每一個 SELECT 讀的每一張表都要有時間範圍條件，不准掃全表。
    pipeline_runs 用 $__timeFilter(run_at)；狀態區間用「跟時間範圍有重疊」（最後確認 >= 起、開始 <= 迄）或
    $__timeFilter(last_checked_at)。"""
    for name, raw in _statements():
        # 每出現一次「FROM 某張表」，就要有一個對應的時間條件（子查詢、UNION 的每一支都算一次）
        run_refs = len(re.findall(r"\bFROM\s+pipeline_runs\b", raw))
        run_bounds = len(re.findall(r"\$__timeFilter\((?:\w+\.)?run_at\)", raw))
        assert run_bounds >= run_refs, f"{name}：讀 pipeline_runs {run_refs} 次，只有 {run_bounds} 個時間條件"
        stream_refs = len(re.findall(r"\bFROM\s+data_stream_health\b", raw))
        overlap_from = len(re.findall(r"last_checked_at >= \$__timeFrom\(\)", raw))
        overlap_to = len(re.findall(r"started_at <= \$__timeTo\(\)", raw))
        assert overlap_from == overlap_to, f"{name}：重疊條件要成對（最後確認 >= 起、開始 <= 迄）"
        stream_bounds = overlap_from + len(re.findall(r"\$__timeFilter\((?:\w+\.)?last_checked_at\)", raw))
        assert stream_bounds >= stream_refs, f"{name}：讀 data_stream_health {stream_refs} 次，只有 {stream_bounds} 個時間條件"
        assert run_refs + stream_refs > 0, name
    for v in _dashboard()["templating"]["list"]:
        if v["type"] == "query":
            assert v["refresh"] == 2, f"{v['name']}：時間範圍一變就要重查（refresh = 2）"


def test_nothing_outside_the_time_range_is_returned_by_any_query(source):
    pipeline = _pipeline(["a", "b"])
    pipeline.run(now=_t(0))
    source["a"] = "bad"
    pipeline.run(now=_t(1))
    for name, raw in _statements():
        assert _query(raw, time_from=_t(-60), time_to=_t(60)), f"{name} 在有資料的範圍內應該有列"
        assert _query(raw, time_from=_t(600), time_to=_t(700)) == [], f"{name} 回了時間範圍以外的列"


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

    # 面板 2：管線最近一輪的執行結果，加上時間範圍內的輪數
    [latest] = _query(_raw(2))
    assert (latest["狀態"], latest["序列數"], latest["出錯的序列"], latest["範圍內輪數"], latest["範圍內失敗輪數"]) == ("degraded", 3, 2, 3, 0)
    assert str(latest["最後一輪"]).startswith("2026-11-08 11:02")

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

    # 面板 4：管線每輪的執行結果（一分鐘一格），最後一輪之後用 -1 收尾；面板 5：沒有成功的輪次，最新的在上面
    timeline = _query(_raw(4))
    assert [r["value"] for r in timeline] == [0, 1, 1, -1]
    assert [r["time"] for r in timeline] == sorted(r["time"] for r in timeline) and timeline[-1]["time"] == timeline[-2]["time"] + 1
    assert [(str(r["輪次時間"])[11:16], r["狀態"]) for r in _query(_raw(5))] == [("11:02", "degraded"), ("11:01", "degraded")]

    # 面板 6：設定錯誤清單；面板 7：出問題的紀錄
    assert [(r["層級"], r["服務"]) for r in _query(_raw(6), services=all_services)] == [("資料流", "svc-b")]
    # 兩段同一時間開始，先後順序不保證：比內容
    assert sorted((r["服務"], r["狀態"]) for r in _query(_raw(7), services=all_services)) == [("svc-a", "no_data"), ("svc-b", "config_error")]


def test_dashboard_shows_failed_rounds_with_their_failure_type_and_groups_broken_definitions():
    """整輪失敗的輪次，畫面上的「狀態」是失敗類型；管線定義載入失敗在設定錯誤清單裡同一個訊息合成一列。"""
    for m in (0, 1, 2):
        PipelineRuns.record("metrics_only", _t(m), "failed", "workflow_error", "nodes.0.params：lookback_minutes 必須大於 0")
    PipelineRuns.record("metrics_only", _t(3), "failed", "infrastructure_error", "資料斷流：2/2", 2, 2)
    PipelineRuns.record("metrics_only", _t(4), "ok", "", "", 2, 0)
    assert [r["狀態"] for r in _query(_raw(5))] == ["infrastructure_error", "workflow_error", "workflow_error", "workflow_error"]
    [latest] = _query(_raw(2))
    assert (latest["狀態"], latest["範圍內輪數"], latest["範圍內失敗輪數"]) == ("ok", 5, 4)
    [grouped] = _query(_raw(6))
    assert (grouped["層級"], grouped["幾輪"]) == ("管線定義", 3) and "lookback_minutes" in grouped["訊息"]
    assert str(grouped["開始"])[11:16] == "11:00" and str(grouped["最後一次"])[11:16] == "11:02"
    # 時間軸一格裡取最嚴重的：這裡一分鐘一格，依序是載入失敗 ×3、整輪失敗、成功、收尾
    codes = {k: int(v) for k, v in re.findall(r"WHEN '(\w+)' THEN (\d+)", _raw(4))}
    assert [r["value"] for r in _query(_raw(4))] == [codes["workflow_error"]] * 3 + [codes["infrastructure_error"], codes["ok"], -1]


def test_dashboard_status_mappings_cover_every_status_the_platform_can_write():
    """畫面上每個狀態都有中文標示與顏色：平台多一種狀態而儀表板沒跟上時這裡會紅。"""
    panels = _panels()
    run_codes = {"ok", "degraded", *FAILURE_TYPES}  # 管線每一輪顯示的代碼：失敗類型（failed 才有），否則 status
    assert set(RUN_STATUSES) - {"failed"} | set(FAILURE_TYPES) == run_codes

    def mapped(panel_id: int) -> set[str]:
        overrides = panels[panel_id]["fieldConfig"]["overrides"]
        status = next(o for o in overrides if o["matcher"]["options"] == "狀態")
        return set(next(p for p in status["properties"] if p["id"] == "mappings")["value"][0]["options"])

    assert mapped(1) == set(STREAM_STATUSES) and mapped(7) == set(STREAM_STATUSES)
    assert mapped(2) == run_codes and mapped(5) == run_codes
    for panel_id, statuses in ((3, set(STREAM_STATUSES)), (4, run_codes)):
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
    # 管線時間軸一格裡取最嚴重的（MAX）：成功 < 有序列出錯 < 任何一種整輪失敗
    codes = {k: int(v) for k, v in re.findall(r"WHEN '(\w+)' THEN (\d+)", _raw(4))}
    assert codes["ok"] < codes["degraded"] < min(codes[f] for f in FAILURE_TYPES)
