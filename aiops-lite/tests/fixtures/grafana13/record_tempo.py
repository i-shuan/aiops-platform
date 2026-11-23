"""錄製 tempo_*.json 的腳本（留著是為了交代來源，測試不會執行它）。

前提：本機拋棄式的 Grafana 13.0.7（127.0.0.1:23000，admin/admin）與 Tempo 2.9.0（HTTP :23200、OTLP/HTTP :24318）。
做的事：用 OTLP/HTTP 送三條 Trace 進 Tempo（兩條失敗、一條成功；web-portal → mes-api → mes-db），在 Grafana 建 Tempo 資料源，
然後**用 Editor 服務帳號的 Token 經 Grafana 的資料源代理**查 Tempo，回應原樣寫檔。
"""
import json
import os
import pathlib
import sys
import time

import httpx

G, OTLP = "http://127.0.0.1:23000", "http://127.0.0.1:24318/v1/traces"
HERE = pathlib.Path(__file__).parent
admin = httpx.Client(base_url=G, auth=("admin", "admin"), timeout=30)
now_ns = time.time_ns()


def attr(key, value):
    return {"key": key, "value": {"stringValue": value}}


def span(trace, sid, parent, name, start_ms, dur_ms, kind, error=None, attrs=()):
    s = {"traceId": trace, "spanId": sid, "name": name, "kind": kind,
         "startTimeUnixNano": str(now_ns - 60_000_000_000 + start_ms * 1_000_000),
         "endTimeUnixNano": str(now_ns - 60_000_000_000 + (start_ms + dur_ms) * 1_000_000),
         "attributes": list(attrs), "status": {"code": 2, "message": error} if error else {"code": 1}}
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


for tid, failed, offset in (("11" * 16, True, 0), ("22" * 16, True, 2000), ("33" * 16, False, 4000)):
    r = httpx.post(OTLP, json=trace(tid, failed, offset), timeout=30)
    assert r.status_code < 300, r.text
time.sleep(20)  # Tempo 收到 trace 之後要過幾秒才搜尋得到（實測 8 秒還搜不到）；用 trace id 直接取則立刻取得到

r = admin.post("/api/datasources", json={"name": "tempo", "type": "tempo", "access": "proxy", "url": "http://127.0.0.1:23200"})
uid = r.json()["datasource"]["uid"] if r.status_code < 300 else admin.get("/api/datasources/name/tempo").json()["uid"]
token_file = pathlib.Path(sys.argv[1])  # 先前建好的 Editor 服務帳號 Token
editor = httpx.Client(base_url=G, headers={"Authorization": f"Bearer {token_file.read_text().strip()}"}, timeout=30)


def record(name, path, params=None):
    r = editor.get(path, params=params)
    body = r.json() if "json" in r.headers.get("content-type", "") else r.text
    (HERE / f"{name}.json").write_text(json.dumps({"request": {"method": "GET", "path": path.replace(uid, "<uid>"), "params": params},
                                                   "status": r.status_code, "response": body}, ensure_ascii=False, indent=1) + "\n")
    print(name, r.status_code, len(r.content))
    return body


start, end = int(time.time()) - 600, int(time.time()) + 60
q = '{ resource.service.name =~ "web-portal|mes-api|mes-db" && status = error }'
found = record("tempo_search", f"/api/datasources/proxy/uid/{uid}/api/search", {"q": q, "start": start, "end": end, "limit": 20})
record("tempo_trace", f"/api/datasources/proxy/uid/{uid}/api/traces/{'11' * 16}")
record("tempo_trace_missing", f"/api/datasources/proxy/uid/{uid}/api/traces/{'ff' * 16}")
record("tempo_search_bad_query", f"/api/datasources/proxy/uid/{uid}/api/search", {"q": "{ status = ", "start": start, "end": end})
print("tempo datasource uid:", uid)
