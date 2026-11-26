"""錄製 victoria_logs.json、victoria_traces.json 的腳本（留著是為了交代來源，測試不會執行它）。

前提：本機拋棄式的 Grafana 13.0.7（127.0.0.1:23000，admin/admin，已裝外掛 victoriametrics-logs-datasource 0.34.0）、
VictoriaLogs v1.17.0（:29428）、VictoriaTraces v0.5.0（:29429）。兩個 Victoria 元件用官方執行檔、預設參數啟動。

做的事：
  1. 把假資料直接寫進 VictoriaLogs（/insert/jsonline）與 VictoriaTraces（OTLP/HTTP JSON）。寫入是這支腳本唯一直連的地方。
  2. **用 Editor 服務帳號的 Token**，呼叫 src/grafana.py 的每一個查詢方法；它實際送給 Grafana 資料源代理的請求與 Grafana 回的內容
     原樣記下來（回應是文字，不重新排版）。

日誌分成幾個 dataset（stream 欄位），各代表一種情況：
  factory           6 筆錯誤 + 1 筆 INFO，內容與時間跟原本從 Elasticsearch 錄的樣本一樣（多數測試用這一批）
  count-mismatch    有日誌，但 service.name 寫的是別的名字，對不到告警的系統名稱
  count-no-errors   這些系統有日誌，但等級都是 INFO／WARN
  count-empty       沒有任何資料
  unparsed          沒拆欄位的容器日誌：系統名稱在 kubernetes.container_name，等級只在訊息文字裡

用法：python record_victoria.py <Editor Token 的檔案>
重跑前要先清掉 VictoriaLogs 的資料目錄（它不能刪單筆）；腳本發現 dataset 已經有資料就不會重複寫。
"""
import json
import pathlib
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from src.grafana import Grafana, GrafanaError, LogFields  # noqa: E402

G, VL, VT = "http://127.0.0.1:23000", "http://127.0.0.1:29428", "http://127.0.0.1:29429"
HERE = pathlib.Path(__file__).parent
token = pathlib.Path(sys.argv[1]).read_text().strip()
admin = httpx.Client(base_url=G, auth=("admin", "admin"), timeout=30)
BASE = datetime(2026, 11, 21, 0, 36, 38, tzinfo=timezone.utc)   # 原本 ES 樣本的錄製時間；日誌的時間照它往回推
START, END = BASE - timedelta(minutes=60), BASE + timedelta(minutes=1)
SERVICES = ["mq-broker", "cim-eap", "amhs-mcs"]
iso = lambda ago: (BASE - timedelta(seconds=ago)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

FACTORY = [
    (600, "cim-eap", "ERROR", "T3 timeout waiting S6F12 reply", "EQP-CVD-07", "LOT24A0031", "FOUP0912"),
    (540, "cim-eap", "ERROR", "T3 timeout waiting S2F42 reply", "EQP-CVD-07", "LOT24A0031", "FOUP0912"),
    (500, "cim-eap", "ERROR", "equipment disconnected (HSMS NOT SELECTED)", "EQP-CVD-07", "LOT24A0044", "FOUP0377"),
    (420, "amhs-mcs", "ERROR", "transfer job aborted: destination port occupied", "STK01", "LOT24A0031", "FOUP0912"),
    (400, "cim-eap", "ERROR", "T3 timeout waiting S6F12 reply", "EQP-ETCH-02", "LOT24A0058", "FOUP0120"),
    (380, "mq-broker", "ERROR", "consumer lag exceeded on queue lot.dispatch", None, None, None),
    (300, "cim-eap", "INFO", "lot track-in complete", "EQP-CVD-03", "LOT24A0060", "FOUP0555"),
]
DATASETS: dict[str, list[dict]] = {"factory": [], "count-mismatch": [], "count-no-errors": [], "unparsed": []}
for ago, svc, lvl, msg, eqp, lot, car in FACTORY:
    doc = {"@timestamp": iso(ago), "dataset": "factory", "service": {"name": svc}, "log": {"level": lvl}, "message": msg}
    doc.update({k: v for k, v in (("eqp_id", eqp), ("lot_id", lot), ("carrier_id", car)) if v})
    DATASETS["factory"].append(doc)
for i, (svc, lvl) in enumerate([("cim_eap", "ERROR"), ("AMHS-MCS", "ERROR"), ("rabbitmq", "ERROR"), ("instana-agent", "INFO")]):
    DATASETS["count-mismatch"].append({"@timestamp": iso(400 + i), "dataset": "count-mismatch", "service": {"name": svc},
                                       "log": {"level": lvl}, "message": "x"})
for i, (svc, lvl, msg) in enumerate([("mq-broker", "INFO", "queue lot.dispatch ready"), ("cim-eap", "WARN", "connection pool usage 92% (184/200)"),
                                     ("cim-eap", "INFO", "lot track-in complete"), ("amhs-mcs", "INFO", "transfer job done"),
                                     ("amhs-mcs", "WARN", "stocker STK01 port P3 slow to respond")]):
    DATASETS["count-no-errors"].append({"@timestamp": iso(400 + i), "dataset": "count-no-errors", "service": {"name": svc},
                                        "log": {"level": lvl}, "message": msg})
for i, (svc, lvl, msg) in enumerate([("cim-eap", "ERROR", "publish to eap.events timed out after 5000ms"),
                                     ("cim-eap", "INFO", "health probe: 0 error(s) in last minute"),
                                     ("cim-eap", "INFO", "lot track-in complete"),
                                     ("mq-broker", "FATAL", "cannot append to queue eap.events: no space left on device")]):
    stamp = iso(400 - i * 10)
    DATASETS["unparsed"].append({"@timestamp": stamp, "dataset": "unparsed", "stream": "stdout",
                                 "kubernetes": {"container_name": svc, "namespace_name": "factory"},
                                 "message": f"{stamp} | {lvl:<5} | worker-1 | {msg}"})

vl = httpx.Client(base_url=VL, timeout=30)
for name, docs in DATASETS.items():
    have = vl.post("/select/logsql/query", data={"query": f'{{dataset="{name}"}} | stats count() as n', "start": "2026-11-01T00:00:00Z"})
    if json.loads(have.text.splitlines()[0])["n"] != "0":
        print(f"{name}：已經有資料，不重複寫")
        continue
    r = vl.post("/insert/jsonline", params={"_stream_fields": "dataset", "_msg_field": "message", "_time_field": "@timestamp"},
                content="\n".join(json.dumps(d) for d in docs))
    assert r.status_code == 200, r.text

# ── Trace：兩條失敗、一條成功（web-portal → mes-api → mes-db），跟原本送進 Tempo 的是同一組 ──
T0 = int((BASE - timedelta(seconds=560)).timestamp() * 1_000_000_000)
attr = lambda key, value: {"key": key, "value": {"stringValue": value}}


def span(trace, sid, parent, name, start_ms, dur_ms, kind, error=None, attrs=()):
    s = {"traceId": trace, "spanId": sid, "name": name, "kind": kind, "startTimeUnixNano": str(T0 + start_ms * 1_000_000),
         "endTimeUnixNano": str(T0 + (start_ms + dur_ms) * 1_000_000), "attributes": list(attrs),
         "status": {"code": 2, "message": error} if error else {"code": 1}}
    if parent:
        s["parentSpanId"] = parent
    return s


def trace(trace_id, failed, offset_ms):
    err = "connection refused: mes-db:5432" if failed else None
    portal = [span(trace_id, "a1" * 8, "", "POST /lots/track-in", offset_ms, 5010 if failed else 120, 2,
                   "upstream returned 503" if failed else None, [attr("http.response.status_code", "503" if failed else "200")]),
              span(trace_id, "a2" * 8, "a1" * 8, "HTTP POST mes-api /api/track-in", offset_ms + 3, 5000 if failed else 110, 3,
                   "503 Service Unavailable" if failed else None)]
    api = [span(trace_id, "b1" * 8, "a2" * 8, "POST /api/track-in", offset_ms + 5, 4990 if failed else 100, 2,
                "database unavailable" if failed else None),
           span(trace_id, "b2" * 8, "b1" * 8, "SELECT lot_state", offset_ms + 8, 4980 if failed else 12, 3, err,
                [attr("db.system", "postgresql"), attr("peer.service", "mes-db")])]
    db = [] if failed else [span(trace_id, "c1" * 8, "b2" * 8, "query", offset_ms + 9, 10, 2)]
    res = lambda name, spans: {"resource": {"attributes": [attr("service.name", name)]},
                               "scopeSpans": [{"scope": {"name": "demo"}, "spans": spans}]}
    return {"resourceSpans": [r for r in (res("web-portal", portal), res("mes-api", api), res("mes-db", db)) if r["scopeSpans"][0]["spans"]]}


have = httpx.get(f"{VT}/select/jaeger/api/traces/{'11' * 16}", timeout=30)
if have.status_code == 200 and have.json().get("data"):
    print("trace：已經有資料，不重複寫")
else:
    for tid, failed, offset in (("11" * 16, True, 0), ("22" * 16, True, 2000), ("33" * 16, False, 4000)):
        r = httpx.post(f"{VT}/insert/opentelemetry/v1/traces", json=trace(tid, failed, offset), timeout=30)
        assert r.status_code < 300, r.text
time.sleep(10)   # 寫入後要等幾秒才查得到（第一次跑只等 5 秒時，後寫的幾個 dataset 還查不到）


# ── 資料源與錄製 ──
def datasource(name, kind, url):
    old = admin.get(f"/api/datasources/name/{name}")
    if old.status_code == 200:
        return old.json()["uid"]
    r = admin.post("/api/datasources", json={"name": name, "type": kind, "access": "proxy", "url": url})
    r.raise_for_status()
    return r.json()["datasource"]["uid"]


uids = {"logs": datasource("victoria-logs", "victoriametrics-logs-datasource", VL),
        "traces": datasource("victoria-traces", "jaeger", f"{VT}/select/jaeger")}
seen: list[dict] = []


def recording(request: httpx.Request) -> httpx.Response:
    with httpx.Client(timeout=30) as http:
        r = http.request(request.method, str(request.url), headers={k: v for k, v in request.headers.items() if k != "host"},
                         content=request.content)
    path = request.url.path
    for uid in uids.values():
        path = path.replace(uid, "<uid>")
    seen.append({"request": {"method": request.method, "path": path, "params": dict(request.url.params),
                             "form": dict(httpx.QueryParams(request.content.decode())) if request.method == "POST" else None},
                 "status": r.status_code, "response": r.text})
    return httpx.Response(r.status_code, content=r.content, headers={"Content-Type": r.headers.get("content-type", "")})


grafana = Grafana(G, token, transport=httpx.MockTransport(recording), retries=0)
exchanges: dict[str, list[dict]] = {}


def record(name, call):
    try:
        result = call()
        shown = json.dumps(result, ensure_ascii=False, default=str)
    except GrafanaError as e:
        shown = f"GrafanaError: {e}"
    exchanges[name] = seen[:]
    seen.clear()
    print(f"{name}: {len(exchanges[name])} 個請求 -> {shown[:150]}")


L, ds = uids["logs"], lambda name: f'{{dataset="{name}"}}'
K8S = LogFields(service="kubernetes.container_name", level="")
for name in (*DATASETS, "count-empty"):
    record(f"dump_{name}", lambda: grafana._logsql(L, "* | sort by (_time)", START, END, extra_filter=ds(name)))
record("search_errors", lambda: grafana.search_errors(L, SERVICES, START, END, 50, extra_filter=ds("factory")))
record("search_errors_size_2", lambda: grafana.search_errors(L, SERVICES, START, END, 2, extra_filter=ds("factory")))
record("search_errors_window", lambda: grafana.search_errors(L, SERVICES, BASE - timedelta(seconds=450), BASE - timedelta(seconds=390), 50,
                                                           extra_filter=ds("factory")))
record("search_errors_unparsed", lambda: grafana.search_errors(L, ["cim-eap", "mq-broker"], START, END, 50, log_fields=K8S,
                                                             extra_filter=ds("unparsed")))
record("error_profile", lambda: grafana.error_profile(L, SERVICES, START, END, warn_levels=("WARN", "WARNING"), first_n=2,
                                                    extra_filter=ds("factory")))
record("error_profile_warnings", lambda: grafana.error_profile(L, SERVICES, START, END, warn_levels=("WARN", "WARNING"),
                                                             extra_filter=ds("count-no-errors")))
record("count_phrases", lambda: grafana.count_phrases(
    L, [("cim-eap", ["timeout", "waiting", "reply"], ("ERROR",)), ("cim-eap", ["equipment", "disconnected"], ("ERROR",)),
        ("mq-broker", ["consumer", "lag", "exceeded", "on", "queue", "lot.dispatch"], ("ERROR",)),
        ("amhs-mcs", ["no", "such", "words"], ("ERROR",))],
    BASE - timedelta(seconds=450), END, START, extra_filter=ds("factory")))
record("count_phrases_unparsed", lambda: grafana.count_phrases(
    L, [("cim-eap", ["publish", "to", "eap.events", "timed", "out"], ("ERROR", "error")), ("cim-eap", ["health", "probe"], ("ERROR",))],
    START, END, START - timedelta(minutes=30), log_fields=K8S, extra_filter=ds("unparsed")))
for name in ("factory", "count-mismatch", "count-no-errors", "count-empty"):
    record(f"count_logs_{name}", lambda: grafana.count_logs(L, SERVICES, START, END, extra_filter=ds(name)))
record("search_timeline", lambda: grafana.search_timeline(L, "lot_id", ["LOT24A0031", "LOT24A0060", "LOT-NOPE"], START, END, 2,
                                                         extra_filter=ds("factory")))
record("quoted_service_name", lambda: grafana.search_errors(L, ['a"b\\c'], START, END, 5, extra_filter=ds("factory")))
record("bad_filter", lambda: grafana.search_errors(L, SERVICES, START, END, 5, extra_filter="{dataset="))
(HERE / "victoria_logs.json").write_text(json.dumps({
    "versions": {"grafana": admin.get("/api/health").json()["version"], "victorialogs": "v1.17.0",
                 "plugin": "victoriametrics-logs-datasource 0.34.0"},
    "token_role": "Editor", "inserted": DATASETS, "exchanges": exchanges}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")

exchanges = {}
T = uids["traces"]
record("search_error_traces", lambda: grafana.search_error_traces(T, ["web-portal", "mes-api", "mes-db"], START, END, 20))
record("search_no_such_service", lambda: grafana.search_error_traces(T, ["nope"], START, END, 20))
record("get_trace", lambda: grafana.get_trace(T, "11" * 16))
record("get_trace_ok", lambda: grafana.get_trace(T, "33" * 16))
record("get_trace_missing", lambda: grafana.get_trace(T, "ff" * 16))
record("get_trace_bad_id", lambda: grafana.get_trace(T, "not-a-trace-id"))
(HERE / "victoria_traces.json").write_text(json.dumps({
    "versions": {"grafana": admin.get("/api/health").json()["version"], "victoriatraces": "v0.5.0"},
    "token_role": "Editor", "exchanges": exchanges}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
