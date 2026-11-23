"""在演練用的那一台 Grafana 上加 Elasticsearch 資料源（trace、日誌各一個），讓平台經同一台 Grafana 撈 RCA 的證據。
可重複執行（已經有就更新）。只給本機演練叢集用；正式環境的資料源由那邊的 Grafana 管理者設定。

  AIOPS_GRAFANA_URL=http://127.0.0.1:13001 AIOPS_GRAFANA_USER=admin AIOPS_GRAFANA_PASSWORD=... \\
  ES_URL=http://elasticsearch.es-test.svc:9200 ES_API_KEY=... python scripts/drill_grafana_es.py

ES_API_KEY 是「Grafana 用來讀 ES」的唯讀金鑰：存進 Grafana 的資料源（secureJsonData，存了就讀不回來），
平台自己不持有。帳密與金鑰只從環境變數讀，不印出來。
"""
from __future__ import annotations

import os

import httpx

# uid -> (Grafana 資料源預設的索引樣式)。平台查詢時會在請求裡自己帶索引（依資料模型），這個欄位只影響 Grafana 自己的 Explore。
DATASOURCES = {"es-traces": "traces-*", "es-logs": "logs-*"}


def body(uid: str, index: str) -> dict:
    return {"uid": uid, "name": uid, "type": "elasticsearch", "access": "proxy", "url": os.environ["ES_URL"],
            "jsonData": {"index": index, "timeField": "@timestamp", "httpHeaderName1": "Authorization"},
            "secureJsonData": {"httpHeaderValue1": f"ApiKey {os.environ['ES_API_KEY']}"}}


def main() -> None:
    client = httpx.Client(base_url=os.environ["AIOPS_GRAFANA_URL"].rstrip("/"), timeout=20,
                          auth=(os.environ["AIOPS_GRAFANA_USER"], os.environ["AIOPS_GRAFANA_PASSWORD"]))
    for uid, index in DATASOURCES.items():
        exists = client.get(f"/api/datasources/uid/{uid}").status_code == 200
        r = client.put(f"/api/datasources/uid/{uid}", json=body(uid, index)) if exists else \
            client.post("/api/datasources", json=body(uid, index))
        print("資料源", uid, "更新" if exists else "新增", r.status_code)
        r.raise_for_status()


if __name__ == "__main__":
    main()
