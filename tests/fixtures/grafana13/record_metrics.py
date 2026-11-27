"""錄製 prom_query_range*.json 的腳本（留著是為了交代來源，測試不會執行它）。檔名的 prom 指的是 Prometheus 的查詢介面，
資料庫是 VictoriaMetrics（使用者的決定，2026-11-27：指標一律是 VictoriaMetrics）。

資料的來歷：RabbitMQ 3.12.1（內建的 rabbitmq_prometheus 外掛）的一個佇列 eap.events，先正常收送 180 秒，然後消費者停掉、
生產者繼續每秒約 10 筆送 110 秒（真的訊息、真的堆積）。2026-11-25 第一次錄的時候存在 Prometheus 3.5.0；
2026-11-27 改成 VictoriaMetrics：把那一次錄到的序列（每 10 秒一點）照原本的時間戳與標籤寫進 VictoriaMetrics，
再送跟當時一模一樣的請求，經 Grafana 查回來。寫入是這支腳本唯一直連的地方。

前提：本機拋棄式的 Grafana 13.0.7（127.0.0.1:23000，admin/admin）、VictoriaMetrics v1.110.0（127.0.0.1:28428），
Grafana 上有 victoria-metrics（Prometheus 型別）資料源。
做的事：**用 Editor 服務帳號的 Token**，照 src/grafana.py 的 query_range 實際送出的請求打 Grafana 的資料源代理，請求與回應原樣寫檔。
用法：python record_metrics.py <Editor Token 的檔案> <原本的序列（第一次錄的 prom_query_range_*.json 所在的資料夾）>
"""
import json
import pathlib
import sys
from datetime import datetime, timezone

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from src.grafana import Grafana, GrafanaError  # noqa: E402

G, VM = "http://127.0.0.1:23000", "http://127.0.0.1:28428"
HERE = pathlib.Path(__file__).parent
token, source = pathlib.Path(sys.argv[1]).read_text().strip(), pathlib.Path(sys.argv[2])
admin = httpx.Client(base_url=G, auth=("admin", "admin"), timeout=30)
uid = admin.get("/api/datasources/name/victoria-metrics").json()["uid"]

# 1. 同一批序列寫進 VictoriaMetrics（照原本的時間戳與標籤）
METRICS = {"prom_query_range_queue_ready": "rabbitmq_queue_messages_ready", "prom_query_range_consumers": "rabbitmq_queue_consumers"}
lines = []
for name, metric in METRICS.items():
    for series in json.loads((source / f"{name}.json").read_text(encoding="utf-8"))["response"]["data"]["result"]:
        lines.append(json.dumps({"metric": {"__name__": metric, "service": "mq-broker", **series["metric"]},
                                 "values": [float(v) for _t, v in series["values"]],
                                 "timestamps": [int(t) * 1000 for t, _v in series["values"]]}))
r = httpx.post(f"{VM}/api/v1/import", content="\n".join(lines) + "\n", timeout=30)
assert r.status_code == 204, r.text
httpx.get(f"{VM}/internal/force_flush", timeout=30)
was = json.loads((source / "prom_query_range_queue_ready.json").read_text(encoding="utf-8"))["request"]["params"]
start, end = (datetime.fromtimestamp(int(was[k]), timezone.utc) for k in ("start", "end"))

# 2. 經 Grafana 查回來，請求與回應原樣記下
seen: list[dict] = []


def recording(request: httpx.Request) -> httpx.Response:
    with httpx.Client(timeout=30) as http:
        r = http.request(request.method, str(request.url), headers={k: v for k, v in request.headers.items() if k != "host"})
    seen.append({"request": {"method": request.method, "path": request.url.path.replace(uid, "prom-uid"),
                             "params": dict(request.url.params)}, "status": r.status_code, "response": r.json()})
    return httpx.Response(r.status_code, content=r.content, headers={"Content-Type": "application/json"})


def save(name: str) -> None:
    exchange = seen.pop()
    exchange["recorded_with"] = {"grafana": "13.0.7", "victoriametrics": "v1.110.0"}
    (HERE / f"{name}.json").write_text(json.dumps(exchange, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


grafana = Grafana(G, token, transport=httpx.MockTransport(recording), retries=0)
for name, expr in (
        ("prom_query_range_queue_ready", 'sum by (queue) (rabbitmq_queue_messages_ready{service="mq-broker"})'),
        ("prom_query_range_consumers", 'sum by (queue) (rabbitmq_queue_consumers{service="mq-broker"})'),
        ("prom_query_range_empty", 'sum by (queue) (rabbitmq_queue_messages_ready{service="amhs-mcs"})')):
    print(name, [(s["labels"], len(s["points"]), s["points"][0][1] if s["points"] else None, s["points"][-1][1] if s["points"] else None)
                 for s in grafana.query_range(uid, expr, start, end, 10)])
    save(name)
try:
    grafana.query_range(uid, 'sum(rate(rabbitmq_queue_messages_ready{service="mq-broker"}[1m)', start, end, 10)
except GrafanaError as e:
    print("bad query:", str(e)[:200])
save("prom_query_range_bad_query")
