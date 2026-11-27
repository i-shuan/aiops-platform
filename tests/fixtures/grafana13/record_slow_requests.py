"""錄製 slow_requests.json 的腳本（留著是為了交代來源，測試不會執行它）。

情境：延遲類的告警。orders-api 變慢但沒有任何請求失敗、沒有任何錯誤日誌；只有警告（慢查詢），而且日誌帶 trace_id。
  Trace   三條都成功：orders-api → orders-db，耗時 4200／300／90 毫秒
  日誌    每條 Trace 各有幾筆帶 trace_id 的日誌（INFO、WARN），另外有幾筆不帶 trace_id 的 WARN
同一批資料送進兩組元件：VictoriaLogs v1.17.0＋VictoriaTraces v0.5.0，以及 Elasticsearch 8.19.23＋Tempo 2.9.0。
寫入是這支腳本唯一直連的地方；查詢全部**用 Editor 服務帳號的 Token 經 Grafana 13.0.7 的資料源代理**，請求與回應原樣記下來：
  search_traces（不限失敗的搜尋）、用 trace id 查日誌（search_timeline 換一個欄位）、兩組各一次完整的 collect_evidence。

用法：python record_slow_requests.py <Editor Token 的檔案>
前提：Grafana :23000（admin/admin）、VictoriaLogs :29428、VictoriaTraces :29429、Elasticsearch :29200、Tempo :23200（OTLP/HTTP :24318）。
"""
import json
import pathlib
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from src import rca  # noqa: E402
from src.grafana import Grafana, GrafanaError  # noqa: E402
from src.grouping import Alert, converge  # noqa: E402
from src.logs import Elasticsearch, LogFields, VictoriaLogs  # noqa: E402
from src.traces import Tempo, VictoriaTraces  # noqa: E402

G, VL, VT, ES, TEMPO = ("http://127.0.0.1:23000", "http://127.0.0.1:29428", "http://127.0.0.1:29429", "http://127.0.0.1:29200",
                        "http://127.0.0.1:23200")
OTLP = {"vt": f"{VT}/insert/opentelemetry/v1/traces", "tempo": "http://127.0.0.1:24318/v1/traces"}
HERE = pathlib.Path(__file__).parent
token = pathlib.Path(sys.argv[1]).read_text().strip()
admin = httpx.Client(base_url=G, auth=("admin", "admin"), timeout=30)
BASE = datetime.now(timezone.utc).replace(microsecond=0)
T0 = BASE - timedelta(seconds=90)                      # 三條 Trace 從這裡開始
run = f"{int(BASE.timestamp()):x}"[-8:]                # 每次錄製的編號不同：Trace 資料庫不能刪資料，重錄不能撞到舊的
IDS = {"slow": f"aa{run}" + "a1" * 11, "medium": f"bb{run}" + "b2" * 11, "fast": f"cc{run}" + "c3" * 11}
DURATIONS = {"slow": 4200, "medium": 300, "fast": 90}   # 毫秒
OFFSETS = {"slow": 0, "medium": 20_000, "fast": 40_000}
DATASET, INDEX = f"rec-slow-{run}", f"rec-slow-{run}"
SERVICES = ["orders-api"]


def attr(key, value):
    return {"key": key, "value": {"stringValue": value}}


def span(trace_id, sid, parent, name, start_ms, dur_ms, kind, attrs=()):
    began = int(T0.timestamp() * 1000) + start_ms
    s = {"traceId": trace_id, "spanId": sid, "name": name, "kind": kind, "startTimeUnixNano": str(began * 1_000_000),
         "endTimeUnixNano": str((began + dur_ms) * 1_000_000), "attributes": list(attrs), "status": {"code": 1}}
    if parent:
        s["parentSpanId"] = parent
    return s


def trace(which):
    tid, total, at = IDS[which], DURATIONS[which], OFFSETS[which]
    api = [span(tid, "a1" * 8, "", "GET /orders/history", at, total, 2, [attr("http.response.status_code", "200")]),
           span(tid, "a2" * 8, "a1" * 8, "SELECT lot_history", at + 5, total - 20, 3,
                [attr("db.system", "postgresql"), attr("peer.service", "orders-db")])]
    db = [span(tid, "d1" * 8, "a2" * 8, "query", at + 8, total - 30, 2)]
    res = lambda name, spans: {"resource": {"attributes": [attr("service.name", name)]}, "scopeSpans": [{"scope": {"name": "demo"}, "spans": spans}]}
    return {"resourceSpans": [res("orders-api", api), res("orders-db", db)]}


def stamp(offset_ms):
    return (T0 + timedelta(milliseconds=offset_ms)).strftime("%Y-%m-%dT%H:%M:%S.") + f"{(T0.microsecond // 1000 + offset_ms) % 1000:03d}Z"


LOGS = []
for which in IDS:
    at, total = OFFSETS[which], DURATIONS[which]
    LOGS.append((at + 1, "orders-api", "INFO", f"GET /orders/history carrier=FOUP0912 started", IDS[which]))
    if which == "slow":
        LOGS.append((at + total - 15, "orders-api", "WARN",
                     "slow query 4180ms: SELECT * FROM lot_history WHERE carrier_id=? (no index used, rows examined 2400000)", IDS[which]))
        LOGS.append((at + total - 20, "orders-db", "INFO", "seq scan on lot_history finished, 2400000 rows", IDS[which]))
    LOGS.append((at + total, "orders-api", "INFO", f"GET /orders/history done in {total}ms status=200", IDS[which]))
LOGS += [(o, "orders-api", "WARN", f"connection pool usage {70 + n}% (14/20 active)", "") for n, o in enumerate((10_000, 30_000, 50_000))]
DOCS = [{"@timestamp": stamp(o), "service": {"name": s}, "log": {"level": lvl}, "message": m, **({"trace_id": t} if t else {})}
        for o, s, lvl, m, t in LOGS]

for which in IDS:
    for url in OTLP.values():
        r = httpx.post(url, json=trace(which), timeout=30)
        assert r.status_code < 300, r.text
r = httpx.post(f"{VL}/insert/jsonline", params={"_stream_fields": "dataset", "_msg_field": "message", "_time_field": "@timestamp"},
               content="\n".join(json.dumps({**d, "dataset": DATASET}) for d in DOCS), timeout=30)
assert r.status_code == 200, r.text
es = httpx.Client(base_url=ES, timeout=30)
es.put(f"/{INDEX}", json={"mappings": {"properties": {
    "@timestamp": {"type": "date"}, "service": {"properties": {"name": {"type": "keyword"}}},
    "log": {"properties": {"level": {"type": "keyword"}}}, "message": {"type": "text"}, "trace_id": {"type": "keyword"}}}}).raise_for_status()
lines = [x for d in DOCS for x in (json.dumps({"index": {}}), json.dumps(d))]
r = es.post(f"/{INDEX}/_bulk?refresh=true", content="\n".join(lines) + "\n", headers={"Content-Type": "application/x-ndjson"})
assert r.status_code == 200 and not r.json()["errors"], r.text[:300]
print("資料送完，等 Tempo 搜尋得到（60 秒）…")
time.sleep(60)


def datasource(name, kind, url, json_data=None):
    old = admin.get(f"/api/datasources/name/{name}")
    if old.status_code == 200:
        return old.json()["uid"]
    made = admin.post("/api/datasources", json={"name": name, "type": kind, "access": "proxy", "url": url, "jsonData": json_data or {}})
    made.raise_for_status()
    return made.json()["datasource"]["uid"]


uids = {"vl": datasource("victoria-logs", "victoriametrics-logs-datasource", VL), "vt": datasource("victoria-traces", "jaeger", f"{VT}/select/jaeger"),
        "es": datasource("rec-slow-es", "elasticsearch", ES, {"index": "rec-slow-*", "timeField": "@timestamp"}), "tempo": datasource("tempo", "tempo", TEMPO)}
seen: list[dict] = []


def recording(request: httpx.Request) -> httpx.Response:
    with httpx.Client(timeout=30) as http:
        r = http.request(request.method, str(request.url), headers={k: v for k, v in request.headers.items() if k != "host"},
                         content=request.content)
    path = request.url.path
    for name, uid in uids.items():
        path = path.replace(uid, f"<{name}>")
    seen.append({"request": {"method": request.method, "path": path, "params": dict(request.url.params), "body": request.content.decode()},
                 "status": r.status_code, "response": r.text})
    return httpx.Response(r.status_code, content=r.content, headers={"Content-Type": r.headers.get("content-type", "")})


grafana = Grafana(G, token, transport=httpx.MockTransport(recording), retries=0)
exchanges: dict[str, list[dict]] = {}
START, END = BASE - timedelta(minutes=10), BASE


def record(name, call):
    try:
        shown = json.dumps(call(), ensure_ascii=False, default=str)
    except GrafanaError as e:
        shown = f"GrafanaError: {e}"
    exchanges[name] = seen[:]
    seen.clear()
    print(f"{name}: {len(exchanges[name])} 個請求 -> {shown[:260]}")


vlogs = VictoriaLogs(grafana, uids["vl"], LogFields(), f'{{dataset="{DATASET}"}}')
eslogs = Elasticsearch(grafana, uids["es"], LogFields(), INDEX)
vtraces, tempo = VictoriaTraces(grafana, uids["vt"]), Tempo(grafana, uids["tempo"])
brief = lambda found: sorted((t["id"], t["duration"]) for t in found)
record("vt_search_traces", lambda: brief(vtraces.search_traces(SERVICES, START, END, 20)))
record("vt_search_error_traces", lambda: brief(vtraces.search_error_traces(SERVICES, START, END, 20)))
record("tempo_search_traces", lambda: brief(tempo.search_traces(SERVICES, START, END, 20)))
record("tempo_search_error_traces", lambda: brief(tempo.search_error_traces(SERVICES, START, END, 20)))
wanted = [IDS["slow"], IDS["fast"], "ff" * 16]
record("vl_logs_of_traces", lambda: vlogs.search_timeline("trace_id", wanted, START, END, 10))
record("es_logs_of_traces", lambda: eslogs.search_timeline("trace_id", wanted, START, END, 10))
record("es_logs_of_unknown_trace", lambda: eslogs.search_timeline("trace_id", ["11" * 16], START, END, 10))   # 日誌沒有這條 Trace 的紀錄

alert = Alert("slow-1", "firing", "rule-slow", "Orders API latency p95", "orders-api", "P2", BASE - timedelta(seconds=60), None, {"A": 4.2})
[incident] = converge([], [alert], [], [], now=BASE, close_after_seconds=300)
collected = {}
for name, logs, traces in (("victoria", vlogs, vtraces), ("es_tempo", eslogs, tempo)):
    record(f"collect_evidence_{name}", lambda: collected.setdefault(name, rca.collect_evidence(
        incident, [], grafana, logs=logs, traces=traces, id_fields=["eqp_id", "lot_id", "carrier_id"], now=BASE)))
(HERE / "slow_requests.json").write_text(json.dumps({
    "versions": {"grafana": admin.get("/api/health").json()["version"], "victorialogs": "v1.17.0", "victoriatraces": "v0.5.0",
                 "elasticsearch": es.get("/").json()["version"]["number"], "tempo": "2.9.0"},
    "token_role": "Editor", "base": BASE.isoformat(), "trace_ids": IDS, "durations_ms": DURATIONS, "dataset": DATASET, "index": INDEX,
    "inserted": DOCS, "exchanges": exchanges}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
admin.delete(f"/api/datasources/uid/{uids['es']}")
for name, ev in collected.items():
    print(f"\n== {name}: chains={[(c['trace_id'][:6], c.get('sample'), c['spans'][0]['duration_ms']) for c in ev['call_chains']]} "
          f"warnings={[w['example'][:40] for w in ev['warnings']]} trace_logs={[(t['trace_id'][:6], len(t['events'])) for t in ev.get('trace_logs', [])]} "
          f"notes={ev['notes']}")
