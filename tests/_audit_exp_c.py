"""架構稽核實驗 C（追加）：Q8 用 httpx.MockTransport 模擬「HTTP 200 但零筆／格式怪」；Q7／Q15 各層擋下的時機與副作用。
.venv/Scripts/python -m pytest tests/_audit_exp_c.py -s -q"""
import json
import math
import shutil
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import pytest
from sqlalchemy import event
from sqlalchemy.engine import Engine

from aiops.engine.errors import PlatformError
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow, load_workflow
from aiops.engine.registry import OperatorRegistry
from aiops.interfaces import runner
from aiops.sources.elasticsearch import ElasticsearchSpec
from aiops.sources.prometheus import PrometheusSpec, VictoriaMetricsSpec
from aiops.storage.hooks import StorageHooks

T0 = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)
ES_QUERY = json.dumps({"index": "metrics-*", "value_field": "cpu.pct", "agg": "avg"})


def show(title, obj):
    print(f"\n### {title}\n{json.dumps(obj, ensure_ascii=False, default=str, indent=1)}")


# ── 共用：副作用計數 ──

@pytest.fixture
def side_effects(monkeypatch):
    """SQL 依動詞計數（含連線建立）、非 testserver 的對外 HTTP 請求計數、watchdog 心跳次數。"""
    c = {"sql": {}, "connect": 0, "http_out": [], "beats": 0}

    def on_sql(conn, cursor, statement, params, context, executemany):
        verb = statement.lstrip().split(None, 1)[0].upper()
        c["sql"][verb] = c["sql"].get(verb, 0) + 1

    def on_connect(*a):
        c["connect"] += 1

    event.listen(Engine, "before_cursor_execute", on_sql)
    event.listen(Engine, "connect", on_connect)
    real_send = httpx.Client.send

    def send(self, request, *a, **k):
        if request.url.host != "testserver":
            c["http_out"].append(f"{request.method} {request.url.host}{request.url.path}")
        return real_send(self, request, *a, **k)

    monkeypatch.setattr(httpx.Client, "send", send)
    cls = OperatorRegistry.get("notifier.watchdog")
    real_beat = cls.heartbeat
    monkeypatch.setattr(cls, "heartbeat", lambda self, ctx, now: (c.__setitem__("beats", c["beats"] + 1), real_beat(self, ctx, now))[1])
    yield c
    event.remove(Engine, "before_cursor_execute", on_sql)
    event.remove(Engine, "connect", on_connect)


def reset(c):
    c["sql"].clear()
    c["connect"] = 0
    c["http_out"].clear()
    c["beats"] = 0


def writes(c):
    return {k: v for k, v in c["sql"].items() if k in ("INSERT", "UPDATE", "DELETE")}


@pytest.fixture
def fake_am():
    got: list = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            got.append(json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0)))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}", got
    srv.shutdown()


# ── Q8：HTTP 200 但零筆／格式怪 ──

def _ts_ms(i):
    return int((T0 - timedelta(minutes=60 - i)).timestamp() * 1000)


def es_body(kind):
    if kind == "buckets 空陣列":
        return {"hits": {"total": {"value": 0}}, "aggregations": {"series": {"buckets": []}}}
    if kind == "每桶 v.value 都是 null":
        return {"aggregations": {"series": {"buckets": [{"key": _ts_ms(i), "doc_count": 0, "v": {"value": None}}
                                                     for i in range(60)]}}}
    if kind == "沒有 aggregations（回應形狀不對）":
        return {"hits": {"hits": []}}
    if kind == "每桶都是 NaN（JSON NaN 字面值）":
        return {"aggregations": {"series": {"buckets": [{"key": _ts_ms(i), "v": {"value": float("nan")}} for i in range(60)]}}}
    if kind == "每桶都是 Infinity":
        return {"aggregations": {"series": {"buckets": [{"key": _ts_ms(i), "v": {"value": float("inf")}} for i in range(60)]}}}
    if kind == "桶缺 key 欄位":
        return {"aggregations": {"series": {"buckets": [{"v": {"value": 1.0}} for _ in range(60)]}}}
    if kind == "value 是字串 abc":
        return {"aggregations": {"series": {"buckets": [{"key": _ts_ms(i), "v": {"value": "abc"}} for i in range(60)]}}}
    if kind == "對照：正常 60 桶":
        return {"aggregations": {"series": {"buckets": [{"key": _ts_ms(i), "v": {"value": 50.0 + i % 3}} for i in range(60)]}}}
    raise KeyError(kind)


def prom_body(kind):
    vals = lambda v: [[(T0 - timedelta(minutes=60 - i)).timestamp(), v] for i in range(60)]  # noqa: E731
    if kind == "data.result 空陣列":
        return {"status": "success", "data": {"resultType": "matrix", "result": []}}
    if kind == "沒有 data 欄位":
        return {"status": "success"}
    if kind == "values 全是 NaN":
        return {"status": "success", "data": {"result": [{"metric": {}, "values": vals("NaN")}]}}
    if kind == "values 全是 +Inf":
        return {"status": "success", "data": {"result": [{"metric": {}, "values": vals("+Inf")}]}}
    if kind == "序列沒有 values 欄位":
        return {"status": "success", "data": {"result": [{"metric": {}}]}}
    if kind == "value 是字串 abc":
        return {"status": "success", "data": {"result": [{"metric": {}, "values": vals("abc")}]}}
    if kind == "200 但 status=error":
        return {"status": "error", "errorType": "bad_data", "error": "parse error"}
    if kind == "回兩條序列":
        return {"status": "success", "data": {"result": [{"metric": {"a": "1"}, "values": vals("1")},
                                                        {"metric": {"a": "2"}, "values": vals("1")}]}}
    if kind == "對照：正常 60 點":
        return {"status": "success", "data": {"result": [{"metric": {}, "values": vals("50")}]}}
    raise KeyError(kind)


def _mock_client(body, hits):
    def handler(request):
        hits.append(request.url.path)
        return httpx.Response(200, content=json.dumps(body, allow_nan=True).encode(), headers={"content-type": "application/json"})
    return httpx.Client(transport=httpx.MockTransport(handler))


def _wf(source_type, query, am_url):
    return Workflow(name="q8x", targets=[TargetSpec(source_type=source_type, query=query, entity=e) for e in ("a", "b")],
                    nodes=[NodeSpec(name="fetch", operator="ingestion.fetch", params={"lookback_minutes": 60}),
                           NodeSpec(name="z", operator="detector.zscore", params={"min_train_points": 10}),
                           NodeSpec(name="group", operator="alert.same_entity"),
                           NodeSpec(name="payload", operator="alert.notify_payload"),
                           NodeSpec(name="notify", operator="notifier.alertmanager", params={"endpoint": am_url}),
                           NodeSpec(name="watchdog", operator="notifier.watchdog")])


def exp_q8_http200_empty(monkeypatch, side_effects, fake_am):
    am_url, am_got = fake_am
    monkeypatch.setenv("AIOPS_NOTIFIER", "alertmanager")
    monkeypatch.setenv("AIOPS_ALERTMANAGER_URL", am_url)
    out = {}
    cases = [("elasticsearch", ElasticsearchSpec, ES_QUERY, es_body,
              ["buckets 空陣列", "每桶 v.value 都是 null", "沒有 aggregations（回應形狀不對）", "每桶都是 NaN（JSON NaN 字面值）",
               "每桶都是 Infinity", "桶缺 key 欄位", "value 是字串 abc", "對照：正常 60 桶"]),
             ("prometheus", PrometheusSpec, "up", prom_body,
              ["data.result 空陣列", "沒有 data 欄位", "values 全是 NaN", "values 全是 +Inf", "序列沒有 values 欄位",
               "value 是字串 abc", "200 但 status=error", "回兩條序列", "對照：正常 60 點"]),
             ("victoriametrics", VictoriaMetricsSpec, "up", prom_body, ["data.result 空陣列", "values 全是 NaN"])]
    for source_type, cls, query, body_of, kinds in cases:
        for kind in kinds:
            hits: list = []
            body = body_of(kind)
            monkeypatch.setattr(cls, "from_env", classmethod(lambda c, _b=body, _h=hits: c("http://src", client=_mock_client(_b, _h))))
            # 1) 資料源層直接呼叫
            try:
                df = cls("http://src", client=_mock_client(body, [])).fetch_data(query, T0 - timedelta(minutes=60), T0, 60)
                direct = f"回 DataFrame {len(df)} 列；NaN {int(df['value'].isna().sum())}、Inf {int(df['value'].map(lambda v: math.isinf(v)).sum())}"
            except Exception as e:  # noqa: BLE001
                direct = f"{type(e).__name__}（PlatformError={isinstance(e, PlatformError)}）：{str(e)[:50]}"
            # 2) 經過引擎
            reset(side_effects)
            am_got.clear()
            try:
                ctx = Pipeline(_wf(source_type, query, am_url), StorageHooks()).run(now=T0)
                eng = f"成功；errors={[(e.entity, e.message[:35]) for e in ctx.errors]}；anomalies={len(ctx.anomalies)}"
            except Exception as e:  # noqa: BLE001
                eng = f"{type(e).__name__}：{str(e)[:70]}"
            out[f"{source_type} | {kind}"] = {
                "資料源層": direct, "引擎": eng, "資料源請求數": len(hits),
                "watchdog 心跳": side_effects["beats"],
                "送到 AM 的告警": [a["labels"].get("alertname") for req in am_got for a in req],
                "DB 寫入": writes(side_effects)}
    show("Q8 HTTP 200 但零筆／格式怪（每組 2 條 target，max_failed_target_ratio=1.0）", out)


# ── Q7／Q15：各層擋下的時機與副作用 ──

BROKEN = {
    "bad_operator": "name: bad_operator\nnodes:\n  - {name: x, operator: detector.nope}\n",
    "bad_yaml": "name: bad_yaml\nnodes: [\n",
    "bad_detectors": ("name: bad_detectors\nnodes:\n  - {name: fetch, operator: ingestion.fetch}\n"
                      "  - {name: detect, operator: detector.zscore}\n"
                      "targets:\n  - {source_type: mock, query: q, entity: e, detectors: [detcet]}\n"),
    "bad_target_field": ("name: bad_target_field\nnodes:\n  - {name: fetch, operator: ingestion.fetch}\n"
                         "  - {name: detect, operator: detector.zscore}\n"
                         "targets:\n  - {source_type: mock, query: q, entity: e, detector: [detect]}\n"),
    "bad_scope_order": ("name: bad_scope_order\nnodes:\n  - {name: n, operator: notifier.log}\n"
                        "  - {name: fetch, operator: ingestion.fetch}\n"),
    "good": ("name: good\nnodes:\n  - {name: fetch, operator: ingestion.fetch}\n  - {name: detect, operator: detector.zscore}\n"
             "  - {name: group, operator: alert.same_entity}\n  - {name: notify, operator: notifier.log}\n"
             "targets:\n  - {source_type: mock, query: q, entity: e}\n"),
}


def exp_q7_q15_fail_fast(client, monkeypatch, tmp_path, side_effects):
    shutil.copytree(runner.WORKFLOW_DIR, tmp_path, dirs_exist_ok=True)
    for name, text in BROKEN.items():
        (tmp_path / "detection" / f"{name}.yaml").write_text(text, encoding="utf-8")
    monkeypatch.setattr(runner, "WORKFLOW_DIR", tmp_path)

    # 記下每一層有沒有被呼叫到
    calls: list = []
    for obj, attr in ((runner, "workflow_path"), (runner, "load_workflow"), (Pipeline, "__init__"), (Pipeline, "run"),
                      (Pipeline, "check_target"), (StorageHooks, "load_batch"), (StorageHooks, "load_target")):
        real = getattr(obj, attr)

        def wrap(*a, _r=real, _n=f"{getattr(obj, '__name__', obj)}.{attr}".replace("aiops.interfaces.runner", "runner"), **k):
            calls.append(_n)
            return _r(*a, **k)
        monkeypatch.setattr(obj, attr, wrap)

    ok_target = {"source_type": "mock", "query": "q", "entity": "e"}
    cases = {
        "workflow='../x'": {"workflow": "../x"},
        "workflow='x.yaml'": {"workflow": "x.yaml"},
        "workflow='nope'（不存在）": {"workflow": "nope"},
        "YAML 語法錯": {"workflow": "bad_yaml"},
        "YAML 算子未註冊": {"workflow": "bad_operator"},
        "YAML scope 順序錯": {"workflow": "bad_scope_order"},
        "YAML target 的 detectors 拼錯": {"workflow": "bad_detectors"},
        "YAML target 欄位拼錯（detector:）": {"workflow": "bad_target_field"},
        "API target 欄位拼錯（detector:）": {"workflow": "good", "targets": [{**ok_target, "detector": ["detect"]}]},
        "API target detectors=[]": {"workflow": "good", "targets": [{**ok_target, "detectors": []}]},
        "API target detectors 拼錯（第 2 條）": {"workflow": "good", "targets": [ok_target, {**ok_target, "entity": "e2", "detectors": ["detcet"]}]},
        "API target overrides 範圍錯": {"workflow": "good", "targets": [{**ok_target, "overrides": {"detect": {"threshold": 999}}}]},
        "對照：正常": {"workflow": "good"},
    }
    out = {}
    for label, body in cases.items():
        reset(side_effects)
        calls.clear()
        r = client.post("/api/v1/runs", json=body)
        msg = r.json().get("message") or r.json().get("detail") or "ok"
        out[label] = {"HTTP": r.status_code, "訊息": str(msg)[:80], "走到的層": list(dict.fromkeys(calls)),
                      "SQL": dict(side_effects["sql"]), "對外 HTTP": len(side_effects["http_out"])}
    show("Q7／Q15 POST /api/v1/runs：擋在哪一層、擋下前的副作用", out)

    # RCA 入口：incident_id 不存在
    reset(side_effects)
    calls.clear()
    r = client.post("/api/v1/rca", json={"incident_id": 999999})
    show("Q15 POST /api/v1/rca incident_id 不存在", {"HTTP": r.status_code, "訊息": r.text[:120],
                                                  "走到的層": list(dict.fromkeys(calls)), "SQL": dict(side_effects["sql"])})

    # CLI 入口（allow_file，可以給 YAML 路徑）
    cli = {}
    for name in ("bad_detectors", "bad_operator"):
        reset(side_effects)
        calls.clear()
        try:
            runner.run_context(str(tmp_path / "detection" / f"{name}.yaml"), allow_file=True, strict=False, now=T0)
            res = "成功"
        except Exception as e:  # noqa: BLE001
            res = f"{type(e).__name__}：{str(e)[:60]}"
        cli[name] = {"結果": res, "走到的層": list(dict.fromkeys(calls)), "SQL": dict(side_effects["sql"])}
    show("Q15 CLI run_context(allow_file=True)", cli)


EXPERIMENTS = [n for n in dir() if n.startswith("exp_")]


@pytest.mark.parametrize("name", EXPERIMENTS)
def test_experiment(name, request):
    import inspect
    fn = globals()[name]
    fn(**{p: request.getfixturevalue(p) for p in inspect.signature(fn).parameters})
