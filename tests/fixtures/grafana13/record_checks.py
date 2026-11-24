"""錄製 org.json、org_bad_token.json、probe_logs_*.json 的腳本（留著是為了交代來源，測試不會執行它）。

前提：本機拋棄式的 Grafana 13.0.7（127.0.0.1:23000）與 Elasticsearch 8.19.23（127.0.0.1:29200）。ES 的 fluentd-demo 索引裡是
11 筆沒拆欄位的 K8s 容器標準輸出（動態 mapping：message、kubernetes.container_name 都是 text + .keyword，沒有 service.name、
沒有 log.level），Grafana 上有一個指到它的 Elasticsearch 資料源。
做的事：**用 Editor 服務帳號的 Token**，照 src/grafana.py 實際送出的請求（whoami、probe_logs）打 Grafana，請求與回應原樣寫檔。
用法：python record_checks.py <Editor Token> <ES 資料源的 uid>
"""
import json
import pathlib
import sys
from datetime import datetime, timedelta, timezone

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from src.grafana import Grafana, GrafanaError, LogFields  # noqa: E402

G = "http://127.0.0.1:23000"
HERE = pathlib.Path(__file__).parent
token, uid = sys.argv[1], sys.argv[2]
seen: list[dict] = []


def recording(request: httpx.Request) -> httpx.Response:
    with httpx.Client(timeout=30) as http:
        r = http.request(request.method, str(request.url), headers={k: v for k, v in request.headers.items() if k != "host"},
                         content=request.content)
    seen.append({"request": {"method": request.method, "path": request.url.path, "body": request.content.decode()},
                 "status": r.status_code, "response": r.json()})
    return httpx.Response(r.status_code, content=r.content, headers={"Content-Type": "application/json"})


def save(name: str) -> None:
    (HERE / f"{name}.json").write_text(json.dumps(seen.pop(), ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


now = datetime.now(timezone.utc)
grafana = Grafana(G, token, transport=httpx.MockTransport(recording), retries=0)
print("org:", grafana.whoami())
save("org")

for name, fields in (("probe_logs_fields_missing", LogFields()),
                     ("probe_logs_container", LogFields(service="kubernetes.container_name", level=""))):
    print(name, grafana.probe_logs(uid, "fluentd-demo", now - timedelta(hours=6), now, log_fields=fields))
    seen[-1]["request"]["path"] = seen[-1]["request"]["path"].replace(uid, "es-uid")
    save(name)

try:
    Grafana(G, "glsa_not-a-real-token", transport=httpx.MockTransport(recording), retries=0).whoami()
except GrafanaError as e:
    print("bad token:", e)
save("org_bad_token")
