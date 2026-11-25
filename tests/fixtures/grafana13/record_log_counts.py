"""錄製 log_counts_*.json 的腳本（留著是為了交代來源，測試不會執行它）。

前提：本機拋棄式的 Grafana 13.0.7（127.0.0.1:23000）與 Elasticsearch 8.19.23（127.0.0.1:29200）。
「錯誤日誌 0 筆」有三種完全不同的原因，各造一個索引：
  log_counts_index_empty      索引在時間窗內沒有任何資料
  log_counts_service_mismatch 有 4 筆日誌，但 service.name 寫的是別的名字（cim_eap、AMHS-MCS…），對不到告警的系統名稱
  log_counts_no_errors        這些系統有 5 筆日誌，但等級都是 INFO／WARN
做的事：**用 Editor 服務帳號的 Token**，照 src/grafana.py 的 count_logs 實際送出的請求打 Grafana 的 ES 資料源代理，請求與回應原樣寫檔。
用法：python record_log_counts.py <Editor Token>
"""
import json
import pathlib
import sys
from datetime import datetime, timedelta, timezone

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from src.grafana import Grafana  # noqa: E402

G, ES = "http://127.0.0.1:23000", "http://127.0.0.1:29200"
HERE = pathlib.Path(__file__).parent
token = sys.argv[1]
es = httpx.Client(base_url=ES, timeout=30)
admin = httpx.Client(base_url=G, auth=("admin", "admin"), timeout=30)
now = datetime.now(timezone.utc)
SERVICES = ["mq-broker", "cim-eap", "amhs-mcs"]
CASES = {
    "index_empty": [],
    "service_mismatch": [("cim_eap", "ERROR"), ("AMHS-MCS", "ERROR"), ("rabbitmq", "ERROR"), ("instana-agent", "INFO")],
    "no_errors": [("mq-broker", "INFO"), ("cim-eap", "WARN"), ("cim-eap", "INFO"), ("amhs-mcs", "INFO"), ("amhs-mcs", "WARN")],
}
seen: list[dict] = []


def recording(request: httpx.Request) -> httpx.Response:
    with httpx.Client(timeout=30) as http:
        r = http.request(request.method, str(request.url), headers={k: v for k, v in request.headers.items() if k != "host"},
                         content=request.content)
    seen.append({"request": {"method": request.method, "body": request.content.decode()}, "status": r.status_code, "response": r.json()})
    return httpx.Response(r.status_code, content=r.content, headers={"Content-Type": "application/json"})


old = admin.get("/api/datasources/name/count-es")
if old.status_code == 200:
    admin.delete(f"/api/datasources/uid/{old.json()['uid']}")
uid = admin.post("/api/datasources", json={"name": "count-es", "type": "elasticsearch", "access": "proxy", "url": ES,
                                           "jsonData": {"index": "count-*", "timeField": "@timestamp"}}).json()["datasource"]["uid"]
grafana = Grafana(G, token, transport=httpx.MockTransport(recording), retries=0)
for name, docs in CASES.items():
    index = f"count-{name.replace('_', '-')}"
    es.delete(f"/{index}")
    es.put(f"/{index}", json={"mappings": {"properties": {"@timestamp": {"type": "date"}}}})
    for i, (service, level) in enumerate(docs):
        es.post(f"/{index}/_doc", json={"@timestamp": (now - timedelta(seconds=60 + i)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                                        "service": {"name": service}, "log": {"level": level}, "message": "x"})
    es.post(f"/{index}/_refresh")
    errors = grafana.search_errors(uid, index, None, SERVICES, now - timedelta(minutes=10), now)
    seen.pop()
    counts = grafana.count_logs(uid, index, SERVICES, now - timedelta(minutes=10), now)
    print(f"{name}: 錯誤日誌 {len(errors['logs'])} 筆 | 計數 {counts}")
    (HERE / f"log_counts_{name}.json").write_text(json.dumps(seen.pop(), ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
admin.delete(f"/api/datasources/uid/{uid}")
