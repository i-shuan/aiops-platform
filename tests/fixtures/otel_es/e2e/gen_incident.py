"""經真的 OTel Collector 灌一段 90 分鐘的劇本進拋棄式 ES（127.0.0.1:24318 -> 29200）。

checkout（入口）-> cart-api -> pg-main（PostgreSQL）、redis-cache；checkout -> tax-gw（外部）；checkout -> Kafka orders -> ord-svc。
最後 20 分鐘：pg-main 變慢（40 -> 400 ms）且 30% 失敗 -> cart-api 500 -> checkout 502。標準答案：cart-api（等 pg-main）。
取樣：head sampling 50%（ot=th:8），沒抽中的 trace 不送（跟 SDK 的 TraceIdRatioBased 一樣，整條 trace 一起丟）。
"""
import importlib.util
import random
import time
from pathlib import Path

# 跑法（拋棄式環境見 ../make_samples.py 開頭）：python tests/fixtures/otel_es/e2e/gen_incident.py
spec = importlib.util.spec_from_file_location("ms", Path(__file__).resolve().parents[1] / "make_samples.py")
ms = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ms)
span, attr, post = ms.span, ms.attr, ms.post

OTLP = "http://127.0.0.1:24318"
rng = random.Random(7)
MS = 1_000_000
now = time.time_ns()
start = now - 90 * 60 * 1000 * MS
incident = now - 20 * 60 * 1000 * MS


def ids(bits):
    return f"{rng.getrandbits(bits):0{bits // 4}x}"


def resource(svc):
    return {"attributes": [attr("service.name", svc), attr("deployment.environment.name", "e2e"),
                           attr("telemetry.sdk.language", "java")]}


stats = {"traces": 0, "sent": 0, "spans": 0, "errors": 0, "logs": 0}
t = start
batch_spans: dict[str, list] = {}
batch_logs: dict[str, list] = {}


def flush():
    scope = {"name": "e2e-incident", "version": "1.0"}
    if batch_spans:
        post(f"{OTLP}/v1/traces", {"resourceSpans": [{"resource": resource(s), "scopeSpans": [{"scope": scope, "spans": sp}]}
                                                     for s, sp in batch_spans.items()]})
    if batch_logs:
        post(f"{OTLP}/v1/logs", {"resourceLogs": [{"resource": resource(s), "scopeLogs": [{"scope": scope, "logRecords": lr}]}
                                                  for s, lr in batch_logs.items()]})
    batch_spans.clear()
    batch_logs.clear()


last_flush = t
while t < now - 5 * 1000 * MS:
    stats["traces"] += 1
    sampled = rng.random() < 0.5
    bad = t >= incident
    tid = ids(128)
    root, call, cart, db, redis, ext, prod, cons = (ids(64) for _ in range(8))
    db_ms = rng.gauss(400, 40) if bad else rng.gauss(40, 4)
    db_err = bad and rng.random() < 0.3
    red_ms = max(0.5, rng.gauss(2, 0.3))
    cart_self = max(1.0, rng.gauss(30, 3))
    cart_ms = 6 + db_ms + red_ms + cart_self
    call_ms = cart_ms + 4
    ext_ms = max(5.0, rng.gauss(25, 3))
    root_ms = 2 + call_ms + 3 + ext_ms + 5 + 3
    st = "ERROR" if db_err else "UNSET"
    tsx = "ot=th:8"
    if sampled:
        stats["sent"] += 1
        stats["errors"] += db_err
        sp = batch_spans.setdefault("checkout", [])
        sp += [span(tid, root, None, "POST /checkout", "SERVER", t, root_ms, status=st, trace_state=tsx,
                    attrs={"http.request.method": "POST", "http.route": "/checkout",
                           "http.response.status_code": 502 if db_err else 200}),
               span(tid, call, root, "POST", "CLIENT", t + 2 * MS, call_ms, status=st, trace_state=tsx,
                    attrs={"http.request.method": "POST", "server.address": "cart-api", "server.port": 8080,
                           "peer.service": "cart-api", "http.response.status_code": 500 if db_err else 200}),
               span(tid, ext, root, "POST", "CLIENT", t + int((2 + call_ms + 3) * MS), ext_ms, trace_state=tsx,
                    attrs={"http.request.method": "POST", "server.address": "tax-gw.example.net", "server.port": 443,
                           "http.response.status_code": 200}),
               span(tid, prod, root, "orders publish", "PRODUCER", t + int((root_ms - 6) * MS), 3.0, trace_state=tsx,
                    attrs={"messaging.system": "kafka", "messaging.destination.name": "orders",
                           "messaging.operation.type": "publish"})]
        sp = batch_spans.setdefault("cart-api", [])
        sp += [span(tid, cart, call, "POST /cart", "SERVER", t + 4 * MS, cart_ms, status=st, trace_state=tsx,
                    message="connection pool exhausted" if db_err else "",
                    attrs={"http.request.method": "POST", "http.route": "/cart",
                           "http.response.status_code": 500 if db_err else 200}),
               span(tid, db, cart, "SELECT shop.items", "CLIENT", t + 10 * MS, db_ms, status=st, trace_state=tsx,
                    attrs={"db.system.name": "postgresql", "db.namespace": "shop", "server.address": "pg-main",
                           "server.port": 5432}),
               span(tid, redis, cart, "GET", "CLIENT", t + int((10 + db_ms + 1) * MS), red_ms, trace_state=tsx,
                    attrs={"db.system.name": "redis", "server.address": "redis-cache", "server.port": 6379})]
        batch_spans.setdefault("ord-svc", []).append(
            span(tid, cons, prod, "orders process", "CONSUMER", t + int((root_ms + 300) * MS), max(5.0, rng.gauss(30, 3)),
                 trace_state=tsx, attrs={"messaging.system": "kafka", "messaging.destination.name": "orders",
                                         "messaging.operation.type": "process"}))
        stats["spans"] += 8
        if db_err:
            stats["logs"] += 1
            batch_logs.setdefault("cart-api", []).append({
                "timeUnixNano": str(t + int((10 + db_ms) * MS)), "severityNumber": 17, "severityText": "ERROR",
                "traceId": tid, "spanId": cart,
                "body": {"stringValue": f"checkout failed: FATAL: remaining connection slots are reserved (pool {rng.randint(1, 99)})"},
                "attributes": [attr("exception.type", "org.postgresql.util.PSQLException")]})
    # 平常偶爾的無關錯誤（基準期也有日誌，暴增倍率才算得出來）
    if rng.random() < 0.02:
        batch_logs.setdefault("cart-api", []).append({
            "timeUnixNano": str(t), "severityNumber": 17, "severityText": "ERROR", "traceId": tid,
            "body": {"stringValue": f"cache miss storm for key user:{rng.randint(1000, 9999)}"},
            "attributes": [attr("exception.type", "java.util.concurrent.TimeoutException")]})
        stats["logs"] += 1
    t += 10 * 1000 * MS  # 每 10 秒一條 trace
    if t - last_flush > 5 * 60 * 1000 * MS:
        flush()
        last_flush = t
flush()
print(stats, "incident_start_ns", incident)
