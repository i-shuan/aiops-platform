"""錄製 prom_query_range*.json 的腳本（留著是為了交代來源，測試不會執行它）。

前提：本機拋棄式的 Grafana 13.0.7（127.0.0.1:23000）、Prometheus 3.5.0（127.0.0.1:29090）、RabbitMQ 3.12.1（內建的
rabbitmq_prometheus 外掛，/metrics/per-object，每 5 秒抓一次，抓取設定加了 service="mq-broker" 標籤）。
資料：一個佇列 eap.events，先正常收送 180 秒，然後消費者停掉、生產者繼續每秒約 10 筆送 110 秒（真的訊息、真的堆積）。
做的事：**用 Editor 服務帳號的 Token**，照 src/grafana.py 的 query_range 實際送出的請求打 Grafana 的資料源代理，請求與回應原樣寫檔。
用法：python record_metrics.py <Editor Token> <Prometheus 資料源的 uid> <消費者停掉的時間 ISO> <結束時間 ISO>
"""
import json
import pathlib
import sys
from datetime import datetime, timedelta

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from src.grafana import Grafana, GrafanaError  # noqa: E402

G = "http://127.0.0.1:23000"
HERE = pathlib.Path(__file__).parent
token, uid = sys.argv[1], sys.argv[2]
stopped, finished = datetime.fromisoformat(sys.argv[3]), datetime.fromisoformat(sys.argv[4])
seen: list[dict] = []


def recording(request: httpx.Request) -> httpx.Response:
    with httpx.Client(timeout=30) as http:
        r = http.request(request.method, str(request.url), headers={k: v for k, v in request.headers.items() if k != "host"})
    seen.append({"request": {"method": request.method, "path": request.url.path.replace(uid, "prom-uid"),
                             "params": dict(request.url.params)}, "status": r.status_code, "response": r.json()})
    return httpx.Response(r.status_code, content=r.content, headers={"Content-Type": "application/json"})


def save(name: str) -> None:
    (HERE / f"{name}.json").write_text(json.dumps(seen.pop(), ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


grafana = Grafana(G, token, transport=httpx.MockTransport(recording), retries=0)
start = stopped - timedelta(minutes=5)
for name, expr in (
        ("prom_query_range_queue_ready", 'sum by (queue) (rabbitmq_queue_messages_ready{service="mq-broker"})'),
        ("prom_query_range_consumers", 'sum by (queue) (rabbitmq_queue_consumers{service="mq-broker"})'),
        ("prom_query_range_empty", 'sum by (queue) (rabbitmq_queue_messages_ready{service="amhs-mcs"})')):
    print(name, [(s["labels"], len(s["points"])) for s in grafana.query_range(uid, expr, start, finished, 10)])
    save(name)
try:
    grafana.query_range(uid, 'sum(rate(rabbitmq_queue_messages_ready{service="mq-broker"}[1m)', start, finished, 10)
except GrafanaError as e:
    print("bad query:", str(e)[:160])
save("prom_query_range_bad_query")
