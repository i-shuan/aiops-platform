"""重新產生 tests/fixtures/otel_es/ 的真實樣本：自己組 OTLP/HTTP JSON 送進 OTel Collector，
由 Collector 的 elasticsearchexporter 寫進一個「拋棄式」Elasticsearch，再把寫進去的文件原樣撈出來存檔。

不是測試，也不會被 pytest 收集。樣本是平台對 OTel 資料形狀的唯一依據，換 Collector／ES 版本時重跑一次比對。

  # 拋棄式環境（本機 nerdctl；絕對不要指到正式或叢集裡的 ES）
  nerdctl network create otelsample
  nerdctl run -d --name otelsample-es --network otelsample -p 127.0.0.1:29200:9200 \\
      -e discovery.type=single-node -e xpack.security.enabled=false -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \\
      docker.io/library/elasticsearch:8.19.23
  nerdctl run -d --name otelsample-col --network otelsample -p 127.0.0.1:24318:4318 \\
      docker.io/otel/opentelemetry-collector-contrib:0.165.0 \\
      '--config=yaml:{receivers: {otlp: {protocols: {http: {endpoint: "0.0.0.0:4318"}}}},
        exporters: {elasticsearch: {endpoint: "http://otelsample-es:9200"}},
        service: {pipelines: {traces: {receivers: [otlp], exporters: [elasticsearch]},
                              logs: {receivers: [otlp], exporters: [elasticsearch]}}}}'
  python tests/fixtures/otel_es/make_samples.py send  --otlp http://127.0.0.1:24318
  # ECS 模式的 trace 會路由到 traces-generic-default，裸 ES 沒有 traces-*-* 的樣板（要裝 APM 整合）而寫入失敗。
  # 為了拿到 ECS span 的 _source 形狀，在拋棄式 ES 補一個最小樣板後只重送 ECS：
  curl -XPUT 127.0.0.1:29200/_index_template/sample-traces-generic -H 'Content-Type: application/json'       -d '{"index_patterns":["traces-generic-*"],"data_stream":{},"priority":50}'
  python tests/fixtures/otel_es/make_samples.py send  --modes ecs
  python tests/fixtures/otel_es/make_samples.py dump  --es http://127.0.0.1:29200 --out tests/fixtures/otel_es

每份資料送兩次：scope 不帶 elastic.mapping.mode（= exporter 的預設模式），以及 scope 帶 elastic.mapping.mode=ecs
（0.165 已經拿掉 mapping.mode 設定鍵，README 說改用這個 scope 屬性）。兩次用不同的 trace_id，服務名稱相同。
"""
import argparse
import json
import random
import sys
import time
import urllib.request
from pathlib import Path

KIND = {"INTERNAL": 1, "SERVER": 2, "CLIENT": 3, "PRODUCER": 4, "CONSUMER": 5}
STATUS = {"UNSET": 0, "OK": 1, "ERROR": 2}
# 取樣（OTel probability sampling, tracestate 的 ot 欄位）：
#   th:<hex> = rejection threshold（56 位元），取樣機率 = 1 - th/2^56；th:c -> 25%、th:8 -> 50%
#   p:<n>    = 舊版 p-value，取樣機率 = 2^-n
SAMPLING = {"t25": "ot=th:c", "t50": "ot=th:8", "p2": "ot=p:2", "none": ""}


def attr(k, v):
    if isinstance(v, bool):
        return {"key": k, "value": {"boolValue": v}}
    if isinstance(v, int):
        return {"key": k, "value": {"intValue": str(v)}}
    if isinstance(v, float):
        return {"key": k, "value": {"doubleValue": v}}
    return {"key": k, "value": {"stringValue": str(v)}}


class Ids:
    def __init__(self, seed):
        self.r = random.Random(seed)

    def trace(self):
        return f"{self.r.getrandbits(128):032x}"

    def span(self):
        return f"{self.r.getrandbits(64):016x}"


def span(trace_id, span_id, parent, name, kind, t0, dur_ms, attrs=None, status="UNSET", message="",
         events=None, links=None, trace_state=""):
    s = {"traceId": trace_id, "spanId": span_id, "name": name, "kind": KIND[kind],
         "startTimeUnixNano": str(t0), "endTimeUnixNano": str(t0 + int(dur_ms * 1e6)),
         "attributes": [attr(k, v) for k, v in (attrs or {}).items()],
         "status": {"code": STATUS[status], **({"message": message} if message else {})}}
    if parent:
        s["parentSpanId"] = parent
    if trace_state:
        s["traceState"] = trace_state
    if events:
        s["events"] = events
    if links:
        s["links"] = links
    return s


def build(ids: Ids, now_ns: int, mode: str | None):
    """回傳 (resource_spans, resource_logs)。服務：checkout（入口）、cart-api、ord-svc（Kafka 消費者）。"""
    scope_attrs = [attr("elastic.mapping.mode", mode)] if mode else []
    by_service: dict[str, list] = {"checkout": [], "cart-api": [], "ord-svc": []}
    logs: dict[str, list] = {"checkout": [], "cart-api": [], "ord-svc": []}
    ms = 1_000_000

    def trace_ok(t, sampling):
        tid = ids.trace()
        ts = SAMPLING[sampling]
        root, call, cart, db, redis, ext, prod = (ids.span() for _ in range(7))
        by_service["checkout"] += [
            span(tid, root, None, "POST /checkout", "SERVER", t, 120.0, trace_state=ts, attrs={
                "http.request.method": "POST", "http.route": "/checkout", "url.path": "/checkout",
                "http.response.status_code": 200, "url.scheme": "http"}),
            span(tid, call, root, "POST", "CLIENT", t + 2 * ms, 80.0, trace_state=ts, attrs={
                "http.request.method": "POST", "server.address": "cart-api", "server.port": 8080,
                "url.full": "http://cart-api:8080/cart", "http.response.status_code": 200,
                "peer.service": "cart-api"}),
            span(tid, ext, root, "POST", "CLIENT", t + 85 * ms, 25.0, trace_state=ts, attrs={
                "http.request.method": "POST", "server.address": "tax-gw.example.net", "server.port": 443,
                "url.full": "https://tax-gw.example.net/v1/quote", "http.response.status_code": 200}),
            span(tid, prod, root, "orders publish", "PRODUCER", t + 112 * ms, 3.0, trace_state=ts, attrs={
                "messaging.system": "kafka", "messaging.destination.name": "orders",
                "messaging.operation.type": "publish", "messaging.operation": "publish",
                "server.address": "kafka-0", "server.port": 9092}),
        ]
        by_service["cart-api"] += [
            span(tid, cart, call, "POST /cart", "SERVER", t + 4 * ms, 75.0, trace_state=ts, attrs={
                "http.request.method": "POST", "http.route": "/cart", "http.response.status_code": 200}),
            span(tid, db, cart, "SELECT shop.items", "CLIENT", t + 10 * ms, 40.0, trace_state=ts, attrs={
                "db.system": "postgresql", "db.system.name": "postgresql", "db.namespace": "shop",
                "db.operation.name": "SELECT", "db.collection.name": "items",
                "server.address": "pg-main", "server.port": 5432}),
            span(tid, redis, cart, "GET", "CLIENT", t + 55 * ms, 2.0, trace_state=ts, attrs={
                "db.system": "redis", "db.system.name": "redis", "db.operation.name": "GET",
                "server.address": "redis-cache", "server.port": 6379}),
        ]
        # 單筆消費：消費者 span 的 parent 是生產者 span（同一條 trace）
        cons = ids.span()
        by_service["ord-svc"].append(
            span(tid, cons, prod, "orders process", "CONSUMER", t + 400 * ms, 30.0, trace_state=ts, attrs={
                "messaging.system": "kafka", "messaging.destination.name": "orders",
                "messaging.operation.type": "process", "messaging.operation": "process",
                "messaging.kafka.consumer.group": "ord-svc"}))
        return tid, prod

    def trace_err(t, sampling):
        tid = ids.trace()
        ts = SAMPLING[sampling]
        root, call, cart, db = (ids.span() for _ in range(4))
        exc = {"name": "exception", "timeUnixNano": str(t + 60 * ms), "attributes": [
            attr("exception.type", "org.postgresql.util.PSQLException"),
            attr("exception.message", "FATAL: remaining connection slots are reserved"),
            attr("exception.stacktrace", "org.postgresql.util.PSQLException: FATAL ...\n\tat Pool.get(Pool.java:42)")]}
        by_service["checkout"] += [
            span(tid, root, None, "POST /checkout", "SERVER", t, 300.0, status="ERROR", trace_state=ts, attrs={
                "http.request.method": "POST", "http.route": "/checkout", "http.response.status_code": 502}),
            span(tid, call, root, "POST", "CLIENT", t + 2 * ms, 280.0, status="ERROR", trace_state=ts, attrs={
                "http.request.method": "POST", "server.address": "cart-api", "server.port": 8080,
                "http.response.status_code": 500, "peer.service": "cart-api", "error.type": "500"}),
        ]
        by_service["cart-api"] += [
            span(tid, cart, call, "POST /cart", "SERVER", t + 4 * ms, 270.0, status="ERROR",
                 message="connection pool exhausted", events=[exc], trace_state=ts, attrs={
                     "http.request.method": "POST", "http.route": "/cart", "http.response.status_code": 500,
                     "error.type": "org.postgresql.util.PSQLException"}),
            span(tid, db, cart, "SELECT shop.items", "CLIENT", t + 10 * ms, 250.0, status="ERROR", trace_state=ts,
                 attrs={"db.system": "postgresql", "db.system.name": "postgresql", "db.namespace": "shop",
                        "server.address": "pg-main", "server.port": 5432, "error.type": "PSQLException"}),
        ]
        logs["cart-api"].append({
            "timeUnixNano": str(t + 60 * ms), "observedTimeUnixNano": str(t + 61 * ms),
            "severityNumber": 17, "severityText": "ERROR", "traceId": tid, "spanId": cart,
            "body": {"stringValue": "checkout failed: FATAL: remaining connection slots are reserved"},
            "attributes": [attr("exception.type", "org.postgresql.util.PSQLException"),
                           attr("exception.message", "FATAL: remaining connection slots are reserved"),
                           attr("code.function.name", "Pool.get")]})
        return tid

    t = now_ns - 120_000 * ms  # 兩分鐘前開始，每條 trace 間隔 1 秒
    producers = []
    for i, sampling in enumerate(["none", "none", "t25", "t50", "p2", "t25"]):
        producers.append(trace_ok(t + i * 1000 * ms, sampling))
    for i, sampling in enumerate(["none", "t25"]):
        trace_err(t + (10 + i) * 1000 * ms, sampling)
    # 批次消費：新 trace 的 CONSUMER span 用 links 指回前三個生產者 span
    tid = ids.trace()
    by_service["ord-svc"].append(span(
        tid, ids.span(), None, "orders process", "CONSUMER", t + 20_000 * ms, 45.0, attrs={
            "messaging.system": "kafka", "messaging.destination.name": "orders",
            "messaging.operation.type": "process", "messaging.batch.message_count": 3},
        links=[{"traceId": p_tid, "spanId": p_span} for p_tid, p_span in producers[:3]]))
    # 一般日誌：INFO（不是錯誤）、body 是結構化 map 的 WARN、沒有 trace 的 ERROR
    logs["checkout"].append({"timeUnixNano": str(t + 5000 * ms), "severityNumber": 9, "severityText": "INFO",
                             "body": {"stringValue": "checkout started"}, "attributes": []})
    logs["ord-svc"].append({"timeUnixNano": str(t + 21_000 * ms), "severityNumber": 13, "severityText": "WARN",
                            "body": {"kvlistValue": {"values": [attr("msg", "consumer lag high"), attr("lag", 1200)]}},
                            "attributes": []})
    logs["ord-svc"].append({"timeUnixNano": str(t + 22_000 * ms), "severityNumber": 17, "severityText": "ERROR",
                            "body": {"stringValue": "rebalance failed: coordinator not available"},
                            "attributes": [attr("exception.type", "org.apache.kafka.common.errors.CoordinatorNotAvailableException")]})

    def resource(svc):
        return {"attributes": [attr("service.name", svc), attr("service.version", "1.4.2"),
                               attr("deployment.environment.name", "sample"),
                               attr("k8s.namespace.name", "shop"), attr("k8s.pod.name", f"{svc}-7d9f8-abcde"),
                               attr("telemetry.sdk.language", "java")]}

    scope = {"name": "sample-generator", "version": "1.0", "attributes": scope_attrs}
    rs = [{"resource": resource(s), "scopeSpans": [{"scope": scope, "spans": sp}]} for s, sp in by_service.items() if sp]
    rl = [{"resource": resource(s), "scopeLogs": [{"scope": scope, "logRecords": lr}]} for s, lr in logs.items() if lr]
    return rs, rl


def post(url, body):
    req = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read().decode()[:200]


def send(otlp: str, modes: list[str]) -> None:
    now = time.time_ns()
    for seed, mode in ((1, None), (2, "ecs")):
        if (mode or "default") not in modes:
            continue
        rs, rl = build(Ids(seed), now, mode)
        print(mode or "default", "traces", post(f"{otlp}/v1/traces", {"resourceSpans": rs}))
        print(mode or "default", "logs", post(f"{otlp}/v1/logs", {"resourceLogs": rl}))


def es(base, path, body=None, method=None):
    req = urllib.request.Request(f"{base}/{path}", json.dumps(body).encode() if body is not None else None,
                                 {"Content-Type": "application/json"},
                                 method=method or ("POST" if body is not None else "GET"))
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def dump(base: str, out: Path) -> None:
    es(base, "_refresh", method="POST")
    streams = es(base, "_data_stream/*?expand_wildcards=all")["data_streams"]
    meta = {"elasticsearch": es(base, "")["version"]["number"], "collector": "otel/opentelemetry-collector-contrib:0.165.0",
            "data_streams": sorted(d["name"] for d in streams), "docs": {}}
    out.mkdir(parents=True, exist_ok=True)
    streams = [d for d in streams if d["name"].startswith(("traces-", "logs-"))]
    meta["data_streams"] = sorted(d["name"] for d in streams)
    meta["note"] = ("traces-generic-default（ECS 模式的 span）是在拋棄式 ES 手動補了最小樣板 sample-traces-generic 才寫得進去；"
                    "_source 由 exporter 產生，跟樣板無關。ECS 送了兩次（第一次 trace 因沒有樣板寫入失敗，補樣板後重送），"
                    "trace_id 相同、時間戳不同，所以 logs-generic-default／logs-apm.error-default 是其他模式的兩倍")
    for d in sorted(streams, key=lambda d: d["name"]):
        name = d["name"]
        hits = es(base, f"{name}/_search", {"size": 1000, "sort": [{"@timestamp": "asc"}]})["hits"]["hits"]
        docs, seen = [], set()
        for h in hits:  # 同一個 seed 重送的文件（_source 完全相同）只留一份
            key = json.dumps(h["_source"], sort_keys=True)
            if key not in seen:
                seen.add(key)
                docs.append({"_index": h["_index"], "_source": h["_source"]})
        meta["docs"][name] = len(docs)
        (out / f"{name}.json").write_text(json.dumps(docs, ensure_ascii=False, indent=1), encoding="utf-8")
        mapping = es(base, f"{name}/_mapping")
        (out / f"{name}.mapping.json").write_text(json.dumps(next(iter(mapping.values()))["mappings"],
                                                              ensure_ascii=False, indent=1), encoding="utf-8")
    (out / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(meta, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["send", "dump"])
    ap.add_argument("--otlp", default="http://127.0.0.1:24318")
    ap.add_argument("--es", default="http://127.0.0.1:29200")
    ap.add_argument("--out", default=str(Path(__file__).parent))
    ap.add_argument("--modes", default="default,ecs", help="send：要送哪幾種（default、ecs）")
    a = ap.parse_args()
    if a.cmd == "send":
        send(a.otlp, a.modes.split(","))
    else:
        if "29200" not in a.es and "localhost" not in a.es:
            raise SystemExit("dump 只准對拋棄式 ES（預設 127.0.0.1:29200）")
        dump(a.es, Path(a.out))
