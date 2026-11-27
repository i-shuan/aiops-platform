"""錄製 service_graph.json 的腳本（留著是為了交代來源，測試不會執行它）。

前提：本機拋棄式的 Grafana 13.0.7（127.0.0.1:23000，admin/admin）、Tempo 2.9.0（開了 metrics_generator 的 service-graphs 處理器，
remote_write 到 VictoriaMetrics v1.110.0 的 /api/v1/write；直方圖的桶用 Tempo 的預設值）、VictoriaTraces v0.5.0（啟動時加
-servicegraph.enableTask=true）。Grafana 上要有 victoria-metrics（Prometheus 型別）與 victoria-traces（Jaeger 型別）兩個資料源。

做的事：
  1. 用 OTLP 送兩段流量，同樣的 Trace 送進 Tempo 與 VictoriaTraces（寫入是這支腳本唯一直連的地方）：
       基線     20 條，全部成功而且快：web-portal → mes-api → mes-db（mes-db 沒有 span，只有 peer.service），
                mes-api → 訊息佇列 → report-worker（父子接法）。每個請求查 mes-db 1 次、約 20 毫秒
       等 6 分半（Tempo 的指標照收到的時間記，基線要落在「事故前 30 分鐘」裡、事故時間窗之外）
       事故     20 條：mes-db 變慢（1.5～2.5 秒），每個請求查 mes-db 5 次（N+1），4 條失敗
  2. **用 Editor 服務帳號的 Token**，呼叫 src/graph.py 的 service_graph（含延遲與基線）與 dependencies；
     它實際送給 Grafana 資料源代理的請求與回應原樣記下來。

用法：python record_service_graph.py <Editor Token 的檔案>
"""
import json
import pathlib
import secrets
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from src import graph  # noqa: E402
from src.grafana import Grafana, GrafanaError  # noqa: E402

HERE = pathlib.Path(__file__).parent
G = "http://127.0.0.1:23000"
OTLP = ("http://127.0.0.1:24318/v1/traces", "http://127.0.0.1:29429/insert/opentelemetry/v1/traces")   # Tempo、VictoriaTraces
token = pathlib.Path(sys.argv[1]).read_text().strip()
admin = httpx.Client(base_url=G, auth=("admin", "admin"), timeout=30)
uid, traces_uid = (admin.get(f"/api/datasources/name/{n}").json()["uid"] for n in ("victoria-metrics", "victoria-traces"))
attr = lambda k, v: {"key": k, "value": {"stringValue": v}}


def send(count: int, failing: int, db_ms: tuple[int, int], db_calls: int) -> None:
    for i in range(count):
        now, tid, fail = time.time_ns(), secrets.token_hex(16), i < failing
        db = db_ms[0] + (db_ms[1] - db_ms[0]) * i // max(1, count - 1)
        api_ms = db * db_calls + 15

        def span(sid, parent, name, start_ms, dur_ms, kind, error=None, attrs=()):
            s = {"traceId": tid, "spanId": sid, "name": name, "kind": kind,
                 "startTimeUnixNano": str(now - (api_ms + 2000) * 1_000_000 + start_ms * 1_000_000),
                 "endTimeUnixNano": str(now - (api_ms + 2000) * 1_000_000 + (start_ms + dur_ms) * 1_000_000), "attributes": list(attrs),
                 "status": {"code": 2, "message": error} if error else {"code": 1}}
            return {**s, "parentSpanId": parent} if parent else s
        portal = [span("a1" * 8, "", "POST /lots", 0, api_ms + 10, 2, "503" if fail else None),
                  span("a2" * 8, "a1" * 8, "HTTP POST mes-api", 3, api_ms + 4, 3, "503" if fail else None)]
        api = [span("b1" * 8, "a2" * 8, "POST /api", 5, api_ms, 2, "db timeout" if fail else None)]
        api += [span(f"b{n + 2}" * 8, "b1" * 8, "SELECT lot_state", 8 + n * db, db, 3, "timeout" if fail and n == db_calls - 1 else None,
                     [attr("db.system", "postgresql"), attr("peer.service", "mes-db")]) for n in range(db_calls)]
        api.append(span("c1" * 8, "b1" * 8, "publish lot.events", api_ms - 5, 2, 4, None, [attr("messaging.system", "rabbitmq")]))
        worker = [span("d1" * 8, "c1" * 8, "consume lot.events", api_ms, 5, 5, None, [attr("messaging.system", "rabbitmq")])]
        body = {"resourceSpans": [{"resource": {"attributes": [attr("service.name", n)]}, "scopeSpans": [{"scope": {"name": "sg"}, "spans": sp}]}
                                  for n, sp in (("web-portal", portal), ("mes-api", api), ("report-worker", worker))]}
        for url in OTLP:
            r = httpx.post(url, json=body, timeout=30)
            assert r.status_code < 300, r.text


send(20, 0, (15, 25), 1)
print("基線送完，等 6 分半…")
time.sleep(390)
incident_start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=30)
send(20, 4, (1500, 2500), 5)
print("事故送完，等 Tempo 把指標寫出去（60 秒）…")
time.sleep(60)
now = datetime.now(timezone.utc).replace(microsecond=0)

seen: list[dict] = []


def recording(request: httpx.Request) -> httpx.Response:
    with httpx.Client(timeout=30) as http:
        r = http.request(request.method, str(request.url), headers={k: v for k, v in request.headers.items() if k != "host"})
    seen.append({"request": {"method": request.method, "path": request.url.path.replace(uid, "<uid>").replace(traces_uid, "<uid>"),
                             "params": dict(request.url.params)},
                 "status": r.status_code, "response": r.json() if "json" in r.headers.get("content-type", "") else r.text})
    return httpx.Response(r.status_code, content=r.content, headers={"Content-Type": r.headers.get("content-type", "")})


grafana = Grafana(G, token, transport=httpx.MockTransport(recording), retries=0)
exchanges, results = {}, {}
for name, call in (("window_with_traffic", lambda: graph.service_graph(grafana, uid, now - timedelta(minutes=15), now)),
                   ("with_latency", lambda: graph.service_graph(grafana, uid, incident_start, now, baseline_minutes=30)),
                   ("window_before_any_traffic", lambda: graph.service_graph(grafana, uid, now - timedelta(hours=5), now - timedelta(hours=4))),
                   ("window_before_any_traffic_with_latency",
                    lambda: graph.service_graph(grafana, uid, now - timedelta(hours=5), now - timedelta(hours=4), baseline_minutes=30)),
                   ("bad_query", lambda: grafana.query_instant(uid, "sum by (client (increase(x[1m)", now)),
                   ("victoriatraces_dependencies", lambda: graph.dependencies(grafana, traces_uid, now - timedelta(minutes=15), now)),
                   ("victoriatraces_dependencies_before_any_traffic",
                    lambda: graph.dependencies(grafana, traces_uid, now - timedelta(days=3), now - timedelta(days=2, hours=23)))):
    try:
        results[name] = call()
    except GrafanaError as e:
        results[name] = f"GrafanaError: {str(e)[:150]}"
    exchanges[name] = seen[:]
    seen.clear()
    print(name, f"（{len(exchanges[name])} 個請求）->", json.dumps(results[name], ensure_ascii=False)[:900])
(HERE / "service_graph.json").write_text(json.dumps({
    "versions": {"grafana": "13.0.7", "tempo": "2.9.0（metrics_generator：service-graphs，直方圖的桶用預設值）", "victoriametrics": "v1.110.0",
                 "victoriatraces": "v0.5.0（-servicegraph.enableTask=true）"},
    "recorded_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "incident_start": incident_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "traffic": {"baseline": "20 條全部成功；每個請求查 mes-db 1 次、15～25 毫秒", "incident": "20 條、4 條失敗；每個請求查 mes-db 5 次、每次 1.5～2.5 秒"},
    "token_role": "Editor", "exchanges": exchanges, "results": results}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
