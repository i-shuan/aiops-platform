"""運行期防線：非有限數值在領域模型入口拒絕（#3）、同一事件群的 RCA 同時只跑一份（#5）、
anomalies 唯一鍵與冪等寫入（#2）。"""
import json
import math
import threading
import time
from datetime import timedelta

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from aiops.engine.context import AnomalyResult, PipelineContext
from aiops.engine.pipeline import Pipeline, load_workflow
from aiops.engine.requests import ExternalAnomalyIn, IngestAnomalies
from aiops.engine.single_flight import SingleFlight
from aiops.engine.types import AnomalyEvent
from aiops.interfaces import runner
from aiops.interfaces.runner import run, run_context, workflow_path
from aiops.stages.enrichment.base import BaseLLM
from aiops.storage import db
from aiops.storage.hooks import StorageHooks
from aiops.storage.models import AnomalyRow, ExternalAnomalyRow
from aiops.storage.repository import Anomalies

# ── #3 非有限數值 ──


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), np.float64("nan"), np.inf, None, "5.0", True])
def test_anomaly_event_rejects_a_score_that_is_not_a_finite_number(now, bad):
    with pytest.raises(ValueError, match="AnomalyEvent.score 必須是有限數值"):
        AnomalyEvent("svc", "latency", now, bad)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), np.float32("inf"), "12", False])
def test_anomaly_event_rejects_a_value_that_is_not_a_finite_number(now, bad):
    with pytest.raises(ValueError, match="AnomalyEvent.value 必須是有限數值"):
        AnomalyEvent("svc", "latency", now, 5.0, value=bad)


def test_finite_numbers_of_any_numeric_type_are_accepted_and_value_may_be_absent(now):
    for score, value in [(5.0, None), (0, 0), (np.float64(3.5), np.int64(7)), (np.float32(1.5), -2.5), (1e308, -1e308)]:
        event = AnomalyEvent("svc", "latency", now, score, value=value)
        assert math.isfinite(event.score) and (event.value is None or math.isfinite(event.value))


def test_from_dict_goes_through_the_same_gate(now):
    """事件群快照（資料庫 JSON）讀回來也是走建構子：Python 的 json 會把 NaN 寫成 NaN、讀回 float('nan')。"""
    good = AnomalyEvent("svc", "latency", now, 5.0, value=1.0).to_dict()
    assert AnomalyEvent.from_dict(json.loads(json.dumps(good))).score == 5.0
    with pytest.raises(ValueError, match="score"):
        AnomalyEvent.from_dict(json.loads(json.dumps({**good, "score": float("nan")})))
    with pytest.raises(ValueError, match="value"):
        AnomalyEvent.from_dict({**good, "value": float("inf")})


def test_a_detector_that_scores_infinity_fails_that_series_instead_of_emitting_an_event(now, sent, monkeypatch):
    """偵測器算出 Inf（分母是 0 這類 bug）且判成異常：排程（strict=False）把每條序列記成錯誤、沒有事件也不落地；
    手動（strict=True）直接丟 ValueError。以前 Inf 分數會一路進事件群（嚴重度直接 P0）並寫進資料庫。"""
    from aiops.stages.detectors.hourly_baseline import HourlyBaselineDetector

    def broken(self, df: pd.DataFrame) -> AnomalyResult:
        scores = pd.Series(np.inf, index=df.index)
        return self._result(df, scores, scores > 3)

    healthy = run_context("sample_metrics_only", now=now, strict=False)
    assert healthy.incidents and not healthy.errors  # 對照組：同一條管線正常時有事件、沒錯誤
    with db.read_session() as s:
        before = s.scalar(select(func.count()).select_from(AnomalyRow))

    monkeypatch.setattr(HourlyBaselineDetector, "detect", broken)
    ctx = run_context("sample_metrics_only", now=now + timedelta(hours=2), strict=False)
    assert ctx.errors and all("ValueError" in e.message and "AnomalyEvent.score" in e.message and "inf" in e.message
                              for e in ctx.errors)
    assert ctx.detected_anomalies == []
    with db.read_session() as s:
        assert s.scalar(select(func.count()).select_from(AnomalyRow)) == before
    with pytest.raises(ValueError, match="AnomalyEvent.score"):
        run_context("sample_metrics_only", now=now + timedelta(hours=2), strict=True)


def test_api_rejects_non_finite_external_anomalies_at_the_door(client, now):
    """ge=0 擋得住 NaN（NaN >= 0 是 False）但擋不住 Infinity：要在入口就回 422，不能收進資料庫。"""
    for field, bad in [("score", "Infinity"), ("score", "NaN"), ("value", "Infinity"), ("value", "-Infinity"), ("value", "NaN")]:
        item = {"external_id": f"x-{field}-{bad}", "entity": "ETCH-01", "metric": "rf", "ts": now.isoformat(), "score": 5, field: 0}
        body = json.dumps({"pipeline": "sample_factory", "source": "fdc", "anomalies": [item]}).replace(f'"{field}": 0', f'"{field}": {bad}')
        r = client.post("/api/v1/ingestion/anomalies", content=body, headers={"Content-Type": "application/json"})
        assert r.status_code == 422, (field, bad, r.text)
    for field, bad in [("score", float("inf")), ("value", float("nan"))]:
        with pytest.raises(ValidationError):
            ExternalAnomalyIn(external_id="e", entity="E", metric="m", ts=now, **{"score": 5, field: bad})
    with db.read_session() as s:
        assert s.scalar(select(func.count()).select_from(ExternalAnomalyRow)) == 0


def test_a_poisoned_external_row_is_dropped_with_an_error_and_does_not_fail_every_round(now, sent):
    """繞過 API 寫進資料庫的 Inf（舊資料、手動補資料）：那一筆記錯誤並標成已處理，同批其他筆照常開單；
    下一輪不會再撞到它（不然每一輪都失敗、也永遠不會好）。"""
    body = IngestAnomalies(pipeline="sample_factory", source="fdc", anomalies=[ExternalAnomalyIn(
        external_id="ok", entity="ETCH-01", metric="rf", ts=now, score=5, attrs={"dept": "fab-etch"})])
    assert run("admin_ingest_anomalies", request=body)["accepted"] == 1
    with db.write_session() as s:
        s.add(ExternalAnomalyRow(pipeline="sample_factory", source="fdc", external_id="bad", entity="LITHO-02", metric="focus",
                                 ts=now.replace(tzinfo=None), score=float("inf"), value=None, attrs={}))
    ctx = run_context("sample_factory", now=now, strict=False, targets=[])
    assert [i.entities for i in ctx.incidents] == [["ETCH-01"]]
    assert [(e.entity, e.message.startswith("external:fdc:AnomalyEvent.score"), "inf" in e.message) for e in ctx.errors] == [
        ("LITHO-02", True, True)]
    with db.read_session() as s:
        assert set(s.scalars(select(ExternalAnomalyRow.status))) == {"processed"}
    again = run_context("sample_factory", now=now, strict=False, targets=[])
    assert again.errors == []


# ── #5 RCA single-flight ──


def _until(condition, seconds=5.0):
    """等到 condition() 成立。不用 time.sleep：conftest 的 retry_waits 把它換成不睡了。"""
    deadline = time.monotonic() + seconds
    while not condition() and time.monotonic() < deadline:
        threading.Event().wait(0.005)
    return condition()


def test_single_flight_runs_once_for_concurrent_callers_and_again_for_later_ones():
    flight, calls, started, release = SingleFlight(), [], threading.Event(), threading.Event()

    def work():
        calls.append(1)
        started.set()
        assert release.wait(5)
        return {"report": len(calls)}

    results: list = []
    threads = [threading.Thread(target=lambda: results.append(flight.do("k", work))) for _ in range(10)]
    threads[0].start()
    assert started.wait(5)
    for t in threads[1:]:
        t.start()
    assert _until(lambda: flight.waiting() == 9)  # 後到的 9 個都在等第一個
    assert flight.in_flight() == 1
    release.set()
    for t in threads:
        t.join(5)
    assert len(calls) == 1 and len(results) == 10 and all(r is results[0] for r in results)
    assert flight.in_flight() == 0
    release.set()
    assert flight.do("k", work) == {"report": 2}  # 不是快取：上一份結束後再來的請求會重新算


def test_single_flight_keys_do_not_block_each_other_and_failures_are_shared_then_forgotten():
    flight, entered, release = SingleFlight(), threading.Event(), threading.Event()

    def slow():
        entered.set()
        assert release.wait(5)
        return "a"

    t = threading.Thread(target=lambda: flight.do(("rca_report", 1), slow))
    t.start()
    assert entered.wait(5)
    assert flight.do(("rca_report", 2), lambda: "b") == "b"  # 別的事件群不用等
    release.set()
    t.join(5)

    attempts, gate, inside = [], threading.Event(), threading.Event()

    def boom():
        attempts.append(1)
        inside.set()
        assert gate.wait(5)
        raise RuntimeError("ES 掛了")

    errors: list = []

    def call():
        try:
            flight.do("x", boom)
        except RuntimeError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=call) for _ in range(5)]
    threads[0].start()
    assert inside.wait(5)
    for th in threads[1:]:
        th.start()
    assert _until(lambda: flight.waiting() == 4)
    gate.set()
    for th in threads:
        th.join(5)
    assert len(attempts) == 1 and len(errors) == 5 and all(e is errors[0] for e in errors)  # 等的人拿到同一個錯，不各自重撞
    assert flight.in_flight() == 0 and flight.do("x", lambda: "recovered") == "recovered"  # 失敗不會卡住鑰匙


class _CountingSpec:
    """假的 trace／日誌來源：記被叫幾次。hold 有設的時候，每次查詢都卡到 hold() 成立才回（讓 10 個請求一定重疊）。"""

    def __init__(self):
        self.calls, self._lock, self.hold = [], threading.Lock(), None

    def _hit(self, kind):
        with self._lock:
            self.calls.append(kind)
        assert self.hold is None or _until(self.hold)

    def fetch_spans(self, start, end, services=None, max_spans=1):
        self._hit("spans")
        return [], False

    def fetch_logs(self, start, end, services=None, max_logs=1):
        self._hit("logs")
        return [], False


class _CountingLLM(BaseLLM):
    name = "fake"

    def __init__(self):
        self.calls = 0

    def complete(self, system, prompt):
        self.calls += 1
        return json.dumps({"summary": "svc-b 最可能是源頭，svc-a 受到牽連。",
                           "claims": [{"text": "svc-b 上游沒有其他異常", "evidence": ["root_cause_candidates"]}]}, ensure_ascii=False)


def _rca_incident(now):
    items = [ExternalAnomalyIn(external_id="sf1", entity="svc-a", metric="latency", ts=now - timedelta(minutes=1), score=3),
             ExternalAnomalyIn(external_id="sf2", entity="svc-b", metric="latency", ts=now - timedelta(minutes=2), score=6)]
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline="sample_it_otel", source="x", anomalies=items))
    wf = load_workflow(workflow_path("sample_it_otel"))
    wf.topology = [("svc-a", "svc-b")]
    return Pipeline(wf, StorageHooks()).run(now=now, targets=[]).to_result()["incidents"][0]["id"]


def test_ten_concurrent_rca_requests_for_one_incident_hit_the_data_source_and_the_llm_once(client, now, monkeypatch):
    """驗收條件：同一個事件群同時來 10 個 /rca，資料源與 LLM 只被打一輪，10 份報告一樣。
    對照組在同一個測試裡：先單獨跑一次量出「一輪 RCA 打資料源幾次」，10 個並發的增量必須等於這個數（不是 10 倍）。"""
    from aiops.stages.ingestion import fetch

    spec, llm = _CountingSpec(), _CountingLLM()
    monkeypatch.setattr(fetch, "get_datasource_spec", lambda _t: spec)
    monkeypatch.setattr(fetch, "is_configured", lambda *_: True)
    monkeypatch.setattr("aiops.stages.enrichment.summarizer.build_llm", lambda *a, **k: llm)
    original = runner.load_workflow

    def with_llm(path):  # 內建 rca_report 預設 provider: noop；這裡打開總結節點的 LLM（簡報節點維持 noop）
        wf = original(path)
        for node in wf.nodes:
            if node.operator == "enrichment.llm_summary":
                node.params["provider"] = "openai"
        return wf

    monkeypatch.setattr(runner, "load_workflow", with_llm)
    incident_id = _rca_incident(now)

    solo = client.post("/api/v1/rca", json={"incident_id": incident_id, "workflow": "rca_report"})
    assert solo.status_code == 200 and solo.json()["result"]["narrative"]["generated_by"] == "llm"
    per_run = list(spec.calls)
    assert per_run.count("spans") >= 1 and per_run.count("logs") >= 1 and llm.calls == 1

    # 第一個進到資料源的請求卡住，等另外 9 個都排在它後面才放行：重疊是確定的，不靠時間差
    flight = client.app.state.rca_flight
    spec.hold = lambda: flight.waiting() == 9
    responses: list = []
    barrier = threading.Barrier(10)

    def call():
        barrier.wait(5)
        responses.append(client.post("/api/v1/rca", json={"incident_id": incident_id, "workflow": "rca_report"}))

    threads = [threading.Thread(target=call) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert [r.status_code for r in responses] == [200] * 10
    assert len(spec.calls) - len(per_run) == len(per_run)  # 10 個請求 = 1 輪資料源查詢，不是 10 輪
    assert llm.calls == 2  # 單獨那次 1 + 並發這批 1
    bodies = [json.dumps(r.json(), sort_keys=True, ensure_ascii=False) for r in responses]
    assert len(set(bodies)) == 1 and responses[0].json()["result"]["narrative"]["generated_by"] == "llm"
    assert client.app.state.rca_flight.in_flight() == 0

    # 不同事件群不共用：不存在的事件群照樣 404，不會拿到別人的報告
    assert client.post("/api/v1/rca", json={"incident_id": 999999}).status_code == 404


def test_each_app_owns_its_own_flight_so_nothing_is_shared_at_module_level():
    from aiops.interfaces.app import create_app

    a, b = create_app(), create_app()
    assert isinstance(a.state.rca_flight, SingleFlight) and a.state.rca_flight is not b.state.rca_flight


# ── #2 anomalies 唯一鍵與冪等寫入 ──


def _anomaly_count() -> int:
    with db.read_session() as s:
        return s.scalar(select(func.count()).select_from(AnomalyRow))


def _row(now, **over) -> dict:
    return {"pipeline": "p", "entity": "svc", "metric": "latency", "ts": now, "value": 9.0, "score": 5.0,
            "detector_name": "detector.mad", "detector_node": "mad", "level": "anomaly", "attrs": {}, **over}


def test_anomalies_declare_the_natural_key_and_the_database_really_enforces_it(now):
    """唯一鍵要真的在資料表上（不是只靠寫入那一段自律）：繞過冪等寫入、用一般新增寫同一個鍵，資料庫要擋。"""
    from sqlalchemy.exc import IntegrityError

    from aiops.storage import crud

    keys = {c.name: [col.name for col in c.columns] for c in AnomalyRow.__table__.constraints if c.name}
    assert keys["uq_anomalies_key"] == ["pipeline", "entity", "metric", "ts", "detector_node"]
    crud.create_many(AnomalyRow, [_row(now)])
    with pytest.raises(IntegrityError):
        crud.create_many(AnomalyRow, [_row(now, score=7.0)])
    assert _anomaly_count() == 1


def test_writing_the_same_rows_again_adds_nothing_and_keeps_the_first_write(now):
    rows = [_row(now), _row(now + timedelta(minutes=1)), _row(now, detector_node="zscore", detector_name="detector.zscore"),
            _row(now, entity="other"), _row(now, metric="errors"), _row(now, pipeline="q")]
    Anomalies.create_many(rows)
    assert _anomaly_count() == 6  # 鍵裡任何一欄不同都是不同的列：兩個節點抓到同一個點是兩筆
    Anomalies.create_many(rows)
    Anomalies.create_many([_row(now, score=99.0, level="warning")])  # 同一個鍵、內容不同：留先寫的，不覆蓋
    assert _anomaly_count() == 6
    with db.read_session() as s:
        first = s.scalars(select(AnomalyRow).where(AnomalyRow.pipeline == "p", AnomalyRow.entity == "svc",
                                                   AnomalyRow.metric == "latency", AnomalyRow.ts == now.replace(tzinfo=None),
                                                   AnomalyRow.detector_node == "mad")).all()
        assert [(r.score, r.level) for r in first] == [(5.0, "anomaly")]


def test_duplicates_inside_one_batch_and_an_empty_batch_are_harmless(now):
    Anomalies.create_many([])
    Anomalies.create_many([_row(now), _row(now, score=6.0), _row(now + timedelta(minutes=1))])
    assert _anomaly_count() == 2


def test_two_overlapping_rounds_do_not_double_the_anomalies(now, sent, monkeypatch):
    """detector.new_only 只擋得住「前一輪已經寫完」的重跑（它讀的是資料庫裡最後一筆異常的時間）。兩個 worker 重疊時，
    第二個在第一個寫入之前就讀了那個時間，兩邊都認為這批點是新的、都去寫。這一段要靠唯一鍵：數量不變，也沒有 IntegrityError。"""
    from aiops.storage import repository

    first = run_context("sample_metrics_only", now=now, strict=False)
    assert first.detected_anomalies and not first.errors
    keys = {(e.entity, e.metric, e.ts, e.detector_node) for e in first.detected_anomalies}
    assert len(keys) == len(first.detected_anomalies)  # 一輪之內本來就不會撞鍵：唯一鍵沒有吃掉任何合法的列
    written = _anomaly_count()
    assert written == len(first.detected_anomalies)

    sequential = run_context("sample_metrics_only", now=now, strict=True)  # 對照組：依序重跑，new_only 自己就擋掉了
    assert sequential.detected_anomalies == [] and _anomaly_count() == written

    # 重疊：第二個 worker 讀到的是第一個寫入之前的狀態（還沒有任何已存的異常）
    monkeypatch.setattr(repository.Anomalies, "latest_ts", staticmethod(lambda *a, **k: None))
    racing = run_context("sample_metrics_only", now=now, strict=True)  # strict：有 IntegrityError 就會直接丟出來
    assert {(e.entity, e.metric, e.ts, e.detector_node) for e in racing.detected_anomalies} == keys and not racing.errors
    assert _anomaly_count() == written

    StorageHooks().persist_batch(first)  # 同一個 ctx 整批再落地一次（寫入成功但行程在回報前被殺、上層重試）
    assert _anomaly_count() == written


def test_a_retried_transaction_writes_the_anomalies_once(now, sent, monkeypatch, db_retry_waits):
    """persist_batch 的交易後半段斷線：整筆回復後重試。異常點是交易的第一個寫入，重試後只有一份。"""
    from sqlalchemy.exc import OperationalError

    from aiops.storage import repository

    real_mark, calls = repository.ExternalAnomalies.mark_processed, {"n": 0}

    def mark(ids):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OperationalError("UPDATE", {}, Exception("connection reset"))
        return real_mark(ids)

    monkeypatch.setattr(repository.ExternalAnomalies, "mark_processed", staticmethod(mark))
    ctx = run_context("sample_metrics_only", now=now, strict=True)
    assert calls["n"] == 2 and db_retry_waits == [0.5]
    assert ctx.detected_anomalies and _anomaly_count() == len(ctx.detected_anomalies)
