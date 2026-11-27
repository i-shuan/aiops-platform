"""錄製 service_graph.json 的腳本（留著是為了交代來源，測試不會執行它）。

前提：本機拋棄式的 Grafana 13.0.7（127.0.0.1:23000）、Tempo 2.9.0（開了 metrics_generator 的 service-graphs 處理器，
remote_write 到 VictoriaMetrics v1.110.0 的 /api/v1/write）。先用 OTLP 送幾條 Trace 進 Tempo：
web-portal → mes-api → mes-db（mes-db 沒有 span，只有 peer.service）、mes-api → 訊息佇列 → report-worker（父子接法），
6 條裡 4 條失敗，等四十秒讓 Tempo 把指標寫出去。
做的事：**用 Editor 服務帳號的 Token**，呼叫 src/graph.py 的 service_graph；它實際送給 Grafana 資料源代理的請求與回應原樣記下來。

用法：python record_service_graph.py <Editor Token 的檔案> <VictoriaMetrics 資料源的 uid>
"""
import json
import pathlib
import sys
from datetime import datetime, timedelta, timezone

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from src import graph  # noqa: E402
from src.grafana import Grafana, GrafanaError  # noqa: E402

HERE = pathlib.Path(__file__).parent
token, uid = pathlib.Path(sys.argv[1]).read_text().strip(), sys.argv[2]
seen: list[dict] = []


def recording(request: httpx.Request) -> httpx.Response:
    with httpx.Client(timeout=30) as http:
        r = http.request(request.method, str(request.url), headers={k: v for k, v in request.headers.items() if k != "host"})
    seen.append({"request": {"method": request.method, "path": request.url.path.replace(uid, "<uid>"), "params": dict(request.url.params)},
                 "status": r.status_code, "response": r.json() if "json" in r.headers.get("content-type", "") else r.text})
    return httpx.Response(r.status_code, content=r.content, headers={"Content-Type": r.headers.get("content-type", "")})


grafana = Grafana("http://127.0.0.1:23000", token, transport=httpx.MockTransport(recording), retries=0)
now = datetime.now(timezone.utc)
exchanges = {}
for name, call in (("window_with_traffic", lambda: graph.service_graph(grafana, uid, now - timedelta(minutes=15), now)),
                   ("window_before_any_traffic", lambda: graph.service_graph(grafana, uid, now - timedelta(hours=5), now - timedelta(hours=4))),
                   ("bad_query", lambda: grafana.query_instant(uid, "sum by (client (increase(x[1m)", now))):
    try:
        result = call()
    except GrafanaError as e:
        result = f"GrafanaError: {str(e)[:150]}"
    exchanges[name] = seen[:]
    seen.clear()
    print(name, "->", json.dumps(result, ensure_ascii=False)[:500])
(HERE / "service_graph.json").write_text(json.dumps({
    "versions": {"grafana": "13.0.7", "tempo": "2.9.0（metrics_generator：service-graphs）", "victoriametrics": "v1.110.0"},
    "recorded_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "token_role": "Editor", "exchanges": exchanges}, ensure_ascii=False, indent=1) + "\n",
    encoding="utf-8")
