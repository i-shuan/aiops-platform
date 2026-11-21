"""錄製 webhook_*.json 的腳本（留著是為了交代來源，測試不會執行它）。

前提：本機拋棄式的 Grafana 13.0.7（127.0.0.1:23000，admin/admin，啟動時 GF_UNIFIED_ALERTING_MIN_INTERVAL=10s）
與 PostgreSQL（amhs.live 表：service、value 兩欄，Grafana 用唯讀帳號讀；record.py 已經建好資料源 amhs-pg）。

用法：
  python record_webhooks.py setup            # 建 3 條貼了 aiops 標籤的規則（每 10 秒評估）、Webhook Contact Point、通知路由
  python record_webhooks.py capture <目錄>   # 起一個只會把收到的 webhook 原樣存檔的 HTTP 服務（:28091）
然後手動改 live 表的值讓規則越線／恢復（UPDATE live SET value = 350 WHERE service = 'mq-broker' …），
Grafana 送出的內容就會存在 <目錄>。樣本是 2026-11-21 這樣錄的：mq-broker、cim-eap、amhs-mcs 各隔 20 秒越線，再一起恢復。
"""
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx


def setup() -> None:
    a = httpx.Client(base_url="http://127.0.0.1:23000", auth=("admin", "admin"), timeout=30)
    keep = {"X-Disable-Provenance": "true"}
    pg = next(d["uid"] for d in a.get("/api/datasources").json() if d["name"] == "amhs-pg")
    a.post("/api/folders", json={"uid": "factory", "title": "factory"})
    for r in a.get("/api/v1/provisioning/alert-rules").json():
        a.delete(f"/api/v1/provisioning/alert-rules/{r['uid']}", headers=keep)

    def rule(title, service, severity, threshold):
        return {"title": title, "ruleGroup": "factory", "folderUID": "factory", "orgID": 1, "condition": "C",
                "for": "0s", "noDataState": "OK", "execErrState": "OK", "annotations": {"summary": title},
                "labels": {"aiops_detect": "true", "aiops_severity": severity, "service": service},
                "data": [{"refId": "A", "relativeTimeRange": {"from": 600, "to": 0}, "datasourceUid": pg,
                          "model": {"refId": "A", "rawQuery": True, "format": "table",
                                    "rawSql": f"SELECT value FROM live WHERE service = '{service}'"}},
                         {"refId": "C", "datasourceUid": "__expr__", "relativeTimeRange": {"from": 0, "to": 0},
                          "model": {"refId": "C", "type": "threshold", "expression": "A",
                                    "conditions": [{"evaluator": {"type": "gt", "params": [threshold]}}]}}]}

    for body in (rule("MQ queue depth", "mq-broker", "P1", 200), rule("CIM T3 timeout", "cim-eap", "P2", 5),
                 rule("AMHS transfer duration", "amhs-mcs", "P2", 90)):
        assert a.post("/api/v1/provisioning/alert-rules", json=body, headers=keep).status_code < 300
    group = a.get("/api/v1/provisioning/folder/factory/rule-groups/factory").json()
    group["interval"] = 10
    assert a.put("/api/v1/provisioning/folder/factory/rule-groups/factory", json=group, headers=keep).status_code < 300
    contact = {"name": "aiops-lite", "type": "webhook", "settings": {
        "url": "http://127.0.0.1:28091/capture", "httpMethod": "POST",
        "authorization_scheme": "Bearer", "authorization_credentials": "hook-secret"}}
    assert a.post("/api/v1/provisioning/contact-points", json=contact, headers=keep).status_code < 300
    policy = a.get("/api/v1/provisioning/policies").json()
    policy["routes"] = [{"receiver": "aiops-lite", "object_matchers": [["aiops_detect", "=", "true"]],
                         "group_by": ["grafana_folder", "alertname"], "group_wait": "1s", "group_interval": "10s",
                         "repeat_interval": "4h"}]
    assert a.put("/api/v1/provisioning/policies", json=policy, headers=keep).status_code < 300


def capture(out: str) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            headers = {k: v for k, v in self.headers.items() if k.lower() in ("user-agent", "content-type")}
            name = f"{out}/hook_{time.strftime('%H%M%S')}_{int(time.time() * 1000) % 1000:03d}.json"
            with open(name, "w", encoding="utf-8") as f:
                json.dump({"headers": headers, "body": body}, f, ensure_ascii=False, indent=1)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{}")

    HTTPServer(("127.0.0.1", 28091), Handler).serve_forever()


if __name__ == "__main__":
    setup() if sys.argv[1] == "setup" else capture(sys.argv[2])
