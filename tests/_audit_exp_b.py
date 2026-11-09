"""架構稽核實驗 B（Q7、Q10、Q11、Q12）。.venv/Scripts/python -m pytest tests/_audit_exp_b.py -s -q"""
import copy
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError

from aiops.engine.errors import InfrastructureError
from aiops.engine.operator import BaseOperator
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent
from aiops.interfaces import runner
from aiops.storage import db, models, repository
from aiops.storage.hooks import StorageHooks

T0 = datetime(2026, 11, 8, 11, 0, tzinfo=timezone.utc)
SPIKE = T0 + timedelta(minutes=40)


def show(title, obj):
    print(f"\n### {title}\n{json.dumps(obj, ensure_ascii=False, default=str, indent=1)}")


# ── Q7 ──

def exp_q7_paths(client, tmp_path):
    payloads = ["../x", "..\\x", "a/b", "C:/Windows/win.ini", "/etc/passwd", "metrics_only.yaml", "metrics_only\n",
                "metrics_only\x00", "", "metrics_only", "METRICS_ONLY", "nope"]
    out = {}
    for w in payloads:
        r1 = client.post("/api/v1/runs", json={"workflow": w, "targets": []})
        r2 = client.post("/api/v1/rca", json={"workflow": w, "incident_id": 1})
        detail = r1.json().get("detail") or r1.json().get("message") or r1.json().get("result")
        out[repr(w)] = {"/runs": r1.status_code, "/rca": r2.status_code,
                        "runs body": str(detail)[:110]}
    show("Q7 API workflow 欄位", out)

    # runner 層（API 擋不到時的第二道）
    direct = {}
    for w in ["../x", "a/b", "metrics_only.yaml", "metrics_only\n", "C:\\x"]:
        try:
            runner.workflow_path(w)
            direct[repr(w)] = "通過"
        except Exception as e:  # noqa: BLE001
            direct[repr(w)] = f"{type(e).__name__}：{str(e)[:60]}"
    show("Q7 runner.workflow_path 直接呼叫", direct)

    # 符號連結：pipelines/detection/audit_link.yaml -> 外面的檔案
    outside = tmp_path / "evil.yaml"
    outside.write_text("name: evil\nnodes: [{name: x, operator: notifier.log}]\n", encoding="utf-8")
    link = runner.WORKFLOW_DIR / "detection" / "audit_link.yaml"
    try:
        os.symlink(outside, link)
        made = "建立成功"
    except OSError as e:
        made = f"無法建立 symlink：{e}"
    res = {"symlink": made}
    if link.is_symlink():
        try:
            r = client.post("/api/v1/runs", json={"workflow": "audit_link", "targets": []})
            res["POST /runs audit_link"] = {"status": r.status_code, "body": r.text[:150]}
        finally:
            link.unlink()
    # 資料夾層級的連結：pipelines/audit_dir -> tmp_path（junction 不需要權限）
    dir_link = runner.WORKFLOW_DIR / "audit_dir"
    (tmp_path / "evil2.yaml").write_text(outside.read_text(encoding="utf-8").replace("evil", "evil2"), encoding="utf-8")
    try:
        os.symlink(tmp_path, dir_link, target_is_directory=True)
        res["dir symlink"] = "建立成功"
    except OSError as e:
        import subprocess
        p = subprocess.run(["cmd", "/c", "mklink", "/J", str(dir_link), str(tmp_path)], capture_output=True, text=True)
        res["dir symlink"] = f"symlink 失敗（{e.winerror if hasattr(e, 'winerror') else e}），改用 junction：rc={p.returncode}"
    try:
        if dir_link.exists():
            r = client.post("/api/v1/runs", json={"workflow": "evil2", "targets": []})
            res["POST /runs evil2（在連結資料夾裡）"] = {"status": r.status_code, "body": r.text[:150],
                                                     "is_symlink": dir_link.is_symlink(),
                                                     "is_junction": getattr(dir_link, "is_junction", lambda: None)()}
    finally:
        if dir_link.exists() or dir_link.is_symlink():
            os.rmdir(dir_link) if not dir_link.is_symlink() else dir_link.unlink()
    show("Q7 symlink", res)


# ── 共用：固定尖峰管線 ──

@OperatorRegistry.register("audit.spike")
class Spike(BaseOperator):
    scope = "target"

    def execute(self, ctx):
        if ctx.now - timedelta(minutes=60) <= SPIKE <= ctx.now:
            ctx.anomalies = [AnomalyEvent(ctx.target.entity, "q", SPIKE, 8.0, {"dept": "it"},
                                          detector="detector.audit", detector_node="detect")]
        return ctx


def spike_pipeline(notifier="notifier.auto", endpoint=None, watchdog=False):
    nodes = [NodeSpec(name="detect", operator="audit.spike"),
             NodeSpec(name="new_only", operator="detector.new_only"),
             NodeSpec(name="group", operator="alert.same_entity"),
             NodeSpec(name="absorb_open", operator="alert.absorb_open"),
             NodeSpec(name="dedup", operator="alert.suppress_dedup"),
             NodeSpec(name="payload", operator="alert.notify_payload"),
             NodeSpec(name="heartbeat", operator="alert.heartbeat", params={"resolve_after_minutes": 15}),
             NodeSpec(name="notify", operator=notifier, params={"endpoint": endpoint} if endpoint else {})]
    if watchdog:
        nodes.append(NodeSpec(name="watchdog", operator="notifier.watchdog"))
    return Pipeline(Workflow(name="spike", targets=[TargetSpec(source_type="mock", query="q", entity="svc")],
                             nodes=nodes), StorageHooks())


@pytest.fixture
def fake_am():
    got: list = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            got.append(json.loads(body))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}", got
    srv.shutdown()


# ── Q10 ──

def exp_q10_no_notification_when_persist_fails(monkeypatch, fake_am, db_retry_waits):
    url, got = fake_am
    monkeypatch.setenv("AIOPS_NOTIFIER", "alertmanager")
    monkeypatch.setenv("AIOPS_ALERTMANAGER_URL", url)
    out = {}
    real_create, real_many = repository.Incidents.create, repository.Anomalies.create_many
    for label, patch in {
        "Incidents.create OperationalError（每次）": ("Incidents", "create", OperationalError("INSERT", {}, Exception("db gone"))),
        "Anomalies.create_many OperationalError（每次）": ("Anomalies", "create_many", OperationalError("INSERT", {}, Exception("db gone"))),
        "Incidents.create IntegrityError（不重試）": ("Incidents", "create", IntegrityError("INSERT", {}, Exception("dup"))),
        "Incidents.create RuntimeError（程式錯誤）": ("Incidents", "create", RuntimeError("bug")),
    }.items():
        models.reset_all()
        got.clear()
        db_retry_waits.clear()
        cls_name, meth, exc = patch

        def boom(*a, _e=exc, **k):
            raise _e

        monkeypatch.setattr(getattr(repository, cls_name), meth, staticmethod(boom))
        try:
            spike_pipeline(notifier="notifier.alertmanager", endpoint=url, watchdog=True).run(now=SPIKE + timedelta(minutes=5))
            res = "成功（不該）"
        except Exception as e:  # noqa: BLE001
            res = f"{type(e).__name__}"
        monkeypatch.setattr(repository.Incidents, "create", staticmethod(real_create))
        monkeypatch.setattr(repository.Anomalies, "create_many", staticmethod(real_many))
        with db.read_session() as s:
            rows = (s.query(models.AnomalyRow).count(), s.query(models.IncidentRow).count())
        out[label] = {"結果": res, "送到假 AM 的請求數": len(got), "重試等待": list(db_retry_waits),
                      "anomalies/incidents 列數": rows}
    models.reset_all()
    got.clear()
    spike_pipeline(notifier="notifier.alertmanager", endpoint=url, watchdog=True).run(now=SPIKE + timedelta(minutes=5))
    out["對照組：資料庫正常"] = {"送到假 AM 的請求數": len(got),
                          "alertnames": [a["labels"].get("alertname") for req in got for a in req]}
    show("Q10 persist_batch 失敗時外部通知數", out)


# ── Q11 ──

def _ctx_fingerprint(ctx):
    return {
        "detected_anomalies": [e.to_dict() for e in ctx.detected_anomalies],
        "anomalies": [e.to_dict() for e in ctx.anomalies],
        "incidents": [(i.uid, i.to_dict(), i.suppressed_by) for i in ctx.incidents],
        "incident_ids": dict(ctx.incident_ids),
        "open_incidents": [(o.id, o.uid, o.severity, o.changed, o.incident.to_dict()) for o in ctx.open_incidents],
        "external_anomaly_ids": list(ctx.external_anomaly_ids),
        "notifications": [(n.labels, n.ends_at) for n in ctx.notifications],
        "resolved_incident_ids": list(ctx.resolved_incident_ids),
        "errors": [(e.entity, e.message) for e in ctx.errors],
    }


def exp_q11_retry_does_not_touch_ctx(monkeypatch, db_retry_waits):
    """第一次嘗試：異常已寫入、事件群寫到一半失敗；比對嘗試前後的 ctx。第二輪做一樣的事，但失敗在 update_snapshot。"""
    global SPIKE
    real_create, real_update = repository.Incidents.create, repository.Incidents.update_snapshot
    real_persist = StorageHooks.persist_batch
    snaps = {}

    def wrap_persist(self, ctx):
        snaps["before"] = copy.deepcopy(_ctx_fingerprint(ctx))
        snaps["ctx"] = ctx
        out = real_persist(self, ctx)
        snaps["after"] = copy.deepcopy(_ctx_fingerprint(ctx))
        return out

    calls = {"create": 0, "update": 0}

    def flaky_create(*a, **k):
        calls["create"] += 1
        if calls["create"] == 1:
            snaps["mid"] = copy.deepcopy(_ctx_fingerprint(snaps["ctx"]))  # 失敗當下的 ctx
            raise OperationalError("INSERT", {}, Exception("connection reset mid-write"))
        return real_create(*a, **k)

    def flaky_update(*a, **k):
        calls["update"] += 1
        if calls["update"] == 1:
            snaps["mid2"] = copy.deepcopy(_ctx_fingerprint(snaps["ctx"]))
            raise OperationalError("UPDATE", {}, Exception("connection reset mid-update"))
        return real_update(*a, **k)

    monkeypatch.setattr(StorageHooks, "persist_batch", wrap_persist)
    monkeypatch.setattr(repository.Incidents, "create", staticmethod(flaky_create))
    monkeypatch.setattr(repository.Incidents, "update_snapshot", staticmethod(flaky_update))

    ctx = spike_pipeline(notifier="notifier.log").run(now=SPIKE + timedelta(minutes=5))
    b, m, a = snaps["before"], snaps["mid"], snaps["after"]
    diff_mid = [k for k in b if b[k] != m[k]]
    diff_after = [k for k in b if b[k] != a[k]]
    with db.read_session() as s:
        counts = {"anomalies": s.query(models.AnomalyRow).count(), "incidents": s.query(models.IncidentRow).count()}
    show("Q11a 第 1 輪：Anomalies 已寫、Incidents.create 第一次失敗", {
        "create 呼叫次數": calls["create"], "重試等待": list(db_retry_waits),
        "嘗試前 vs 失敗當下 不同的欄位": diff_mid, "嘗試前 vs 完成後 不同的欄位": diff_after,
        "完成後 incident_ids": a["incident_ids"], "資料庫列數": counts})

    # 第 2 輪：同一個實體 6 分鐘後又異常 -> 吸收進進行中事件群 -> update_snapshot 第一次失敗
    first = SPIKE
    SPIKE = first + timedelta(minutes=6)
    db_retry_waits.clear()
    try:
        spike_pipeline(notifier="notifier.log").run(now=SPIKE + timedelta(minutes=1))
    finally:
        SPIKE = first
    b, m, a = snaps["before"], snaps["mid2"], snaps["after"]
    with db.read_session() as s:
        inc = s.query(models.IncidentRow).one()
        counts = {"anomalies": s.query(models.AnomalyRow).count(), "incidents": s.query(models.IncidentRow).count(),
                  "事件群快照 events": len(inc.incident["events"])}
    show("Q11b 第 2 輪：吸收進進行中事件群、update_snapshot 第一次失敗", {
        "update 呼叫次數": calls["update"], "重試等待": list(db_retry_waits),
        "嘗試前 vs 失敗當下 不同的欄位": [k for k in b if b[k] != m[k]],
        "嘗試前 vs 完成後 不同的欄位": [k for k in b if b[k] != a[k]], "資料庫": counts})


# ── Q12 ──

def exp_q12_finalize_fails(monkeypatch, fake_am, db_retry_waits):
    url, got = fake_am
    monkeypatch.setenv("AIOPS_NOTIFIER", "alertmanager")
    monkeypatch.setenv("AIOPS_ALERTMANAGER_URL", url)
    real_record, real_resolve = repository.Notifications.record, repository.Notifications.record
    real_mark = repository.Incidents.mark_resolved
    rounds = []

    def boom(*a, **k):
        raise OperationalError("INSERT", {}, Exception("db gone after push"))

    def run(at, label, *, break_record=False, break_mark=False):
        got.clear()
        monkeypatch.setattr(repository.Notifications, "record", staticmethod(boom if break_record else real_record))
        monkeypatch.setattr(repository.Incidents, "mark_resolved", staticmethod(boom if break_mark else real_mark))
        try:
            spike_pipeline(notifier="notifier.alertmanager", endpoint=url, watchdog=True).run(now=at)
            res = "成功"
        except Exception as e:  # noqa: BLE001
            res = f"{type(e).__name__}：{str(e)[:50]}"
        with db.read_session() as s:
            inc = [(r.status, r.severity) for r in s.query(models.IncidentRow).all()]
            notif = s.query(models.NotificationRow).count()
        sent = [(a["labels"].get("alertname"), a["labels"].get("incident_uid", "")[:8], "endsAt" in a and a["labels"].get("alertname") != "AIOpsWatchdog")
                for req in got for a in req]
        rounds.append({"輪": label, "結果": res, "送出 (alertname, uid, 帶 endsAt)": sent, "事件群 (status, severity)": inc,
                       "notifications 列數": notif})

    run(SPIKE + timedelta(minutes=5), "1 初發：finalize 的 Notifications.record 失敗", break_record=True)
    run(SPIKE + timedelta(minutes=10), "2 正常")
    run(SPIKE + timedelta(minutes=20), "3 安靜滿 15 分：送 endsAt，但 mark_resolved 失敗", break_mark=True)
    run(SPIKE + timedelta(minutes=25), "4 正常：再送一次 endsAt")
    run(SPIKE + timedelta(minutes=30), "5 正常")
    show("Q12 finalize_batch 寫入失敗", rounds)


EXPERIMENTS = [n for n in dir() if n.startswith("exp_")]


@pytest.mark.parametrize("name", EXPERIMENTS)
def test_experiment(name, request):
    import inspect
    fn = globals()[name]
    fn(**{p: request.getfixturevalue(p) for p in inspect.signature(fn).parameters})
