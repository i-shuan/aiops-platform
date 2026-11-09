"""端對端：從 workflow YAML 出發，經 storage 載入與寫入，驗證整條管線與三條原本斷掉的接線。"""
import json
from datetime import timedelta

import httpx
import pytest

from aiops.engine.requests import (
    LotPathsMerge,
    MappingApprove,
    MappingSubmit,
    SilenceCreate,
    TraceMerge,
)
from aiops.sources.elasticsearch import ElasticsearchSpec
from aiops.interfaces.runner import run, run_context
from aiops.storage.repository import Anomalies, DetectorModels, Incidents, Notifications

WF = "metrics_only"


def _go(now, workflow=WF, **kw):
    return run_context(workflow, now=now, strict=False, **kw)


# ── 基本流程 ──


def test_first_run_detects_groups_notifies_and_persists(now, sent):
    ctx = _go(now)
    result = ctx.to_result()
    assert result["anomalies"] >= 1 and len(result["incidents"]) == 1
    assert sent and sent[0].labels["dept"] == "unassigned"
    row = Incidents.get(result["incidents"][0]["id"])
    assert row["status"] == "open" and row["uid"] == result["incidents"][0]["uid"]
    assert sent[0].labels["incident_uid"] == row["uid"]  # 通知帶的就是存起來的那個 uid
    assert DetectorModels.get(WF, "metric_a", "metric_a", "detector.zscore", "detect")["points"] > 0  # 基準線有存
    assert Notifications.since(now.replace(tzinfo=None) - timedelta(days=1))


def _go_with(now, resolve_after_minutes):
    """跟內建 workflow 一樣，只是把心跳的結案門檻換掉。"""
    from aiops.engine.pipeline import Pipeline, load_workflow
    from aiops.interfaces.runner import workflow_path
    from aiops.storage.hooks import StorageHooks

    wf = load_workflow(workflow_path(WF))
    next(n for n in wf.nodes if n.operator == "alert.heartbeat").params["resolve_after_minutes"] = resolve_after_minutes
    return Pipeline(wf, StorageHooks()).run(now=now)


def test_second_run_same_window_only_heartbeats_and_never_duplicates(now, sent):
    _go_with(now, resolve_after_minutes=60)
    first = [(n.labels, n.ends_at) for n in sent]
    again = _go_with(now, resolve_after_minutes=60).to_result()
    assert again.get("anomalies", 0) == 0 and "incidents" not in again
    assert [(n.labels, n.ends_at) for n in sent[len(first):]] == first  # 只重送心跳，labels 一模一樣、沒有結案
    assert [i["status"] for i in [Incidents.get(1)]] == ["open"]


def test_incident_resolves_once_quiet_long_enough_with_identical_labels(now, sent):
    """mock 的尖峰在窗口中間（30 分鐘前）：結案門檻 15 分鐘 -> 第二輪就結案，送出帶 endsAt 的同一筆。"""
    _go_with(now, resolve_after_minutes=15)
    opened = sent[0]
    _go_with(now, resolve_after_minutes=15)
    closing = sent[-1]
    assert closing.ends_at is not None and closing.labels == opened.labels
    assert Incidents.get(1)["status"] == "resolved" and Incidents.list_open(WF) == []
    before = len(sent)
    _go_with(now, resolve_after_minutes=15)
    assert len(sent) == before  # 結案後不再重送


def test_manual_resolve_sends_ends_at_and_cannot_be_done_twice(now, sent):
    from aiops.engine.errors import Conflict
    from aiops.engine.requests import UidRef

    _go_with(now, resolve_after_minutes=60)
    uid = Incidents.get(1)["uid"]
    assert run("admin_incident_resolve", request=UidRef(uid=uid), now=now)["resolved"] is True
    assert sent[-1].ends_at is not None and sent[-1].labels["incident_uid"] == uid
    assert Incidents.get(1)["status"] == "resolved"
    with pytest.raises(Conflict):
        run("admin_incident_resolve", request=UidRef(uid=uid), now=now)


def test_notify_failure_fails_the_run_keeps_the_incident_open_and_records_no_notification(now, monkeypatch, sent):
    """通知在寫入之後才送：送不出去時事件群已經是進行中，但「已通知」不能記（根本沒送到）。
    以前是「通知沒送就不存事件群、下一輪重新處理」，實際上異常點已經先寫進去，下一輪會被 new_only 丟掉，那次告警永遠發不出去。"""
    monkeypatch.setenv("AIOPS_NOTIFIER", "alertmanager")
    monkeypatch.setenv("AIOPS_ALERTMANAGER_URL", "http://127.0.0.1:1")  # 沒人在聽
    from aiops.engine.errors import InfrastructureError

    with pytest.raises(InfrastructureError):
        _go(now)
    assert len(Incidents.list_open(WF)) == 1  # 事件群存成進行中，下一輪心跳會用同一組標籤補送
    assert Notifications.since(now.replace(tzinfo=None) - timedelta(days=1)) == []
    monkeypatch.setenv("AIOPS_NOTIFIER", "log")
    uid = Incidents.list_open(WF)[0]["uid"]
    _go(now + timedelta(minutes=5))
    # 補送了同一張卡（同一個 incident_uid）、沒有重開一張。mock 的尖峰在 11:30，到 12:05 已安靜 35 分鐘，
    # 所以補送的這一則同時就是結案（endsAt），事件群也在通知送成功後才標成已結案
    assert [n.labels["incident_uid"] for n in sent] == [uid] and sent[0].ends_at is not None
    assert Incidents.list_open(WF) == [] and Incidents.get_by_uid(uid)["status"] == "resolved"
    assert len(Notifications.since(now.replace(tzinfo=None) - timedelta(days=1))) == 1


def test_unknown_notifier_is_infrastructure_error(now, monkeypatch):
    monkeypatch.setenv("AIOPS_NOTIFIER", "carrier-pigeon")
    from aiops.engine.errors import InfrastructureError

    with pytest.raises(InfrastructureError, match="carrier-pigeon"):
        _go(now)


# ── 靜音（輸入從 DB 載入 ctx，算子本身不碰 DB）──


def test_silence_created_through_admin_suppresses_the_next_run(now, sent):
    run("admin_silence_create", request=SilenceCreate(kind="maintenance", start=now - timedelta(days=1),
                                                      end=now + timedelta(days=1), reason="維護", created_by="amy"))
    result = _go(now).to_result()
    assert result["incidents"][0]["suppressed_by"].startswith("silence_window") and sent == []
    assert Incidents.get(result["incidents"][0]["id"])["status"] == "suppressed"  # 被抑制的也留紀錄


# ── 外部異常匯入 ──


def test_external_anomalies_join_the_next_run_once(now, sent):
    from aiops.engine.requests import ExternalAnomalyIn, IngestAnomalies

    body = IngestAnomalies(pipeline="factory", source="fdc", anomalies=[ExternalAnomalyIn(
        external_id="a1", entity="ETCH-01", metric="rf", ts=now, score=5, attrs={"dept": "fab-etch"})])
    assert run("admin_ingest_anomalies", request=body)["accepted"] == 1
    assert run("admin_ingest_anomalies", request=body) == {"pipeline": "factory", "source": "fdc", "accepted": 0, "duplicates": 1}
    ctx = run_context("factory", now=now, strict=False, targets=[])
    assert [i.entities for i in ctx.incidents] == [["ETCH-01"]] and sent[0].labels["dept"] == "fab-etch"
    assert run_context("factory", now=now, strict=False, targets=[]).incidents == []  # 處理過就不再讀


def test_external_anomalies_record_which_source_judged_them(now, sent):
    """匯入的異常沒有偵測節點，但要留下「誰判的」：detector = external:<source>，事件群快照裡也看得到。
    外部異常不寫進 anomalies 表（那張表只記本平台偵測節點的結果，models.AnomalyRow 註解），這裡一併釘住。"""
    from aiops.engine.requests import ExternalAnomalyIn, IngestAnomalies
    from aiops.storage import crud
    from aiops.storage.models import AnomalyRow

    body = IngestAnomalies(pipeline="factory", source="fdc", anomalies=[ExternalAnomalyIn(
        external_id="lineage1", entity="ETCH-01", metric="rf", ts=now, score=5)])
    run("admin_ingest_anomalies", request=body)
    ctx = run_context("factory", now=now, strict=False, targets=[])
    assert [(e.detector, e.detector_node) for e in ctx.incidents[0].events] == [("external:fdc", "")]
    snapshot = Incidents.get(ctx.incident_ids[ctx.incidents[0].fingerprint])["incident"]
    assert [e["detector"] for e in snapshot["events"]] == ["external:fdc"]
    assert crud.find(AnomalyRow, pipeline="factory") == []


# ── 接線 1：欄位對照 ──


@pytest.fixture
def es_bodies(monkeypatch, now):
    seen: list[dict] = []

    def handler(req):
        seen.append(json.loads(req.content))
        buckets = [{"key": int((now - timedelta(minutes=59 - i)).timestamp() * 1000), "doc_count": 1,
                    "v": {"values": {"95.0": 900.0 if i == 30 else 100.0 + i % 3}}} for i in range(60)]
        return httpx.Response(200, json={"aggregations": {"series": {"buckets": buckets}}})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(ElasticsearchSpec, "from_env", classmethod(lambda cls: cls("http://es", client=client)))
    return seen


ES_TARGET = {"source_type": "elasticsearch", "query": json.dumps({"index": "traces-apm-*", "agg": "p95", "entity": "cart"}),
             "entity": "cart"}


def _approve(fields):
    mid = run("admin_mapping_submit", request=MappingSubmit(source_type="elasticsearch", fields=fields,
                                                            submitter="amy"))["mapping"]["id"]
    run("admin_mapping_approve", request=MappingApprove(mapping_id=mid, approver="bob"))


def test_approved_mapping_reaches_the_es_query(now, es_bodies):
    from aiops.engine.pipeline import TargetSpec

    _approve({"@timestamp": "ts", "transaction.duration.us": "value", "service.name": "entity"})
    ctx = _go(now, targets=[TargetSpec(**ES_TARGET)])
    assert ctx.errors == [] and ctx.anomalies and ctx.anomalies[0].entity == "cart"
    body = es_bodies[-1]
    assert body["aggs"]["series"]["date_histogram"]["field"] == "@timestamp"
    assert body["aggs"]["series"]["aggs"]["v"]["percentiles"]["field"] == "transaction.duration.us"
    assert {"term": {"service.name": "cart"}} in body["query"]["bool"]["filter"]


def test_without_a_mapping_the_short_query_is_a_target_error_and_nothing_is_sent(now, es_bodies):
    from aiops.engine.pipeline import TargetSpec

    ctx = _go(now, targets=[TargetSpec(**ES_TARGET)])
    assert [e.entity for e in ctx.errors] == ["cart"] and es_bodies == []


def test_a_newer_approved_mapping_replaces_the_old_one(now, es_bodies):
    from aiops.engine.pipeline import TargetSpec

    _approve({"@timestamp": "ts", "transaction.duration.us": "value", "service.name": "entity"})
    _approve({"event.ingested": "ts", "span.duration.us": "value", "labels.svc": "entity"})
    _go(now, targets=[TargetSpec(**ES_TARGET)])
    body = es_bodies[-1]
    assert body["aggs"]["series"]["date_histogram"]["field"] == "event.ingested"
    assert {"term": {"labels.svc": "cart"}} in body["query"]["bool"]["filter"]


# ── 接線 2：trace 合成 -> 偵測 -> 服務拓撲 ──


def _spans(now, minutes=60, slow=30):
    from aiops.engine.requests import SpanIn

    out = []
    for i in range(minutes):
        ts = now - timedelta(minutes=minutes - 1 - i)
        out.append(SpanIn(trace_id=f"t{i}", span_id=f"f{i}", service="frontend", start=ts, duration_ms=80))
        out.append(SpanIn(trace_id=f"t{i}", span_id=f"c{i}", parent_id=f"f{i}", service="cart", start=ts,
                          duration_ms=2000 if i == slow else 50 + i % 3, error=i == slow))
    return out


@pytest.fixture
def traces(now):
    return run("admin_merge_traces", request=TraceMerge(spans=_spans(now)))


def test_trace_merge_derives_call_edges_and_persists(traces):
    assert traces["edges"] == [{"caller": "frontend", "callee": "cart", "calls": 60}]
    assert len(traces["series"]) == 120 and traces["persisted"] is True


def test_detection_reads_the_trace_source_like_any_metric(now, traces):
    from aiops.engine.pipeline import TargetSpec

    ctx = _go(now, targets=[TargetSpec(source_type="trace", query="cart/p95_ms", entity="cart")])
    assert ctx.errors == [] and len(ctx.anomalies) == 1


@pytest.mark.parametrize("query, why", [("cart/latency", "trace 欄位只能是 count, error_rate, p95_ms"),
                                        ("cart", "trace 查詢格式是「服務/欄位」")])
def test_bad_trace_queries_are_target_errors(now, traces, query, why):
    from aiops.engine.pipeline import TargetSpec

    ctx = _go(now, targets=[TargetSpec(source_type="trace", query=query, entity="x")])
    # 只記在這條 target、原因要對得上；不能是別的錯誤剛好讓 errors 非空
    assert [e.entity for e in ctx.errors] == ["x"] and why in ctx.errors[0].message
    assert ctx.anomalies == []


def test_the_only_target_having_no_data_is_data_starvation_not_a_quiet_round(now, traces):
    """查詢合法但整段都沒資料：這一輪唯一的序列抓不到 = 1/1 斷流，整輪是基礎設施錯誤（以前只記錯誤、當成功）。"""
    from aiops.engine.errors import InfrastructureError
    from aiops.engine.pipeline import TargetSpec

    with pytest.raises(InfrastructureError, match="資料斷流：1/1") as e:
        _go(now, targets=[TargetSpec(source_type="trace", query="nobody/p95_ms", entity="x")])
    assert "x" in str(e.value)


def test_derived_topology_merges_incidents_and_explicit_empty_topology_disables_it(now, traces):
    from aiops.engine.pipeline import TargetSpec
    from aiops.engine.types import AnomalyEvent

    targets = [TargetSpec(source_type="trace", query="cart/p95_ms", entity="cart"),
               TargetSpec(source_type="trace", query="frontend/p95_ms", entity="frontend")]
    from aiops.engine.pipeline import Pipeline, load_workflow
    from aiops.interfaces.runner import workflow_path
    from aiops.storage.hooks import StorageHooks

    wf = load_workflow(workflow_path("it_otel"))
    ctx = Pipeline(wf, StorageHooks()).run(now=now, targets=targets)
    assert ctx.topology == [("frontend", "cart")] and ctx.errors == []
    wf.topology = []
    assert Pipeline(wf, StorageHooks()).run(now=now, targets=targets[:0]).topology == []
    assert AnomalyEvent  # 只是確認 import 沒被誤刪


# ── 接線 3：Lot 履歷 -> 工廠收斂與根因 ──


def _lot_steps(now):
    from aiops.engine.requests import LotStepIn

    return [LotStepIn(lot_id=f"L{n}", station=st, equipment=eq, start=now + timedelta(minutes=10 * k),
                      end=now + timedelta(minutes=10 * k + 8))
            for n in range(3) for k, (st, eq) in enumerate((("litho", "LITHO-02"), ("etch", "ETCH-01")))]


def test_lot_paths_flow_is_derived_idempotently_and_has_no_topology_field(now):
    out = run("admin_merge_lot_paths", request=LotPathsMerge(steps=_lot_steps(now)))
    run("admin_merge_lot_paths", request=LotPathsMerge(steps=_lot_steps(now)))  # 重送
    assert out["lot_edges"] == [{"from": "LITHO-02", "to": "ETCH-01", "lots": 3}] and "topology" not in out
    ctx = run_context("factory", now=now, strict=False, targets=[])
    assert [(e.upstream, e.downstream, e.lots) for e in ctx.lot_edges] == [("LITHO-02", "ETCH-01", 3)]


def test_factory_groups_and_root_cause_points_to_the_upstream_tool(now, sent):
    from aiops.engine.requests import ExternalAnomalyIn, IngestAnomalies

    run("admin_merge_lot_paths", request=LotPathsMerge(steps=_lot_steps(now)))
    items = [ExternalAnomalyIn(external_id="e", entity="ETCH-01", metric="rf_power", ts=now, score=5),
             ExternalAnomalyIn(external_id="l", entity="LITHO-02", metric="focus", ts=now - timedelta(minutes=30), score=4)]
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline="factory", source="fdc", anomalies=items))
    result = run_context("factory", now=now, strict=False, targets=[]).to_result()
    assert [i["entities"] for i in result["incidents"]] == [["ETCH-01", "LITHO-02"]]
    assert "lot_path_chain" in result["incidents"][0]["tags"]

    rca = run("rca_report", incident_id=result["incidents"][0]["id"])
    roots = next(s["body"] for s in rca["sections"] if s["key"] == "root_cause_candidates")
    assert roots.startswith("1. LITHO-02")  # 上游機台是源頭（方向跟 IT 相反）


def test_rca_gets_topology_and_trace_evidence_without_the_caller_passing_anything(now, traces, sent):
    from aiops.engine.requests import ExternalAnomalyIn, IngestAnomalies

    items = [ExternalAnomalyIn(external_id="f", entity="frontend", metric="latency", ts=now - timedelta(minutes=29), score=3),
             ExternalAnomalyIn(external_id="c", entity="cart", metric="latency", ts=now - timedelta(minutes=30), score=6)]
    run("admin_ingest_anomalies", request=IngestAnomalies(pipeline="it_otel", source="x", anomalies=items))
    result = run_context("it_otel", now=now, strict=False, targets=[]).to_result()
    assert [i["entities"] for i in result["incidents"]] == [["cart", "frontend"]]  # 拓撲自動推導 -> 併成一件
    rca = run("rca_report", incident_id=result["incidents"][0]["id"])
    sections = {s["key"]: s["body"] for s in rca["sections"]}
    assert sections["root_cause_candidates"].startswith("1. cart")
    assert "錯誤率最高 100%" in sections["trace_evidence"]
