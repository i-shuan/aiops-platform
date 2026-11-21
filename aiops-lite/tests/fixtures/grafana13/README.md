# Grafana 13.0.7 的真實樣本

2026-11-21 從本機拋棄式環境錄的：Grafana 13.0.7（官方 tarball）、PostgreSQL 16、Elasticsearch 8.19.23。
資料是腳本灌的假資料，**不是產線資料**；真的是 Grafana 送出與回應的只有「格式」。內容原樣沒改。

| 檔案 | 內容 | 怎麼錄的 |
| --- | --- | --- |
| `webhook_firing_mq／cim／amhs.json` | Grafana 的 Webhook Contact Point 實際推出來的告警（firing）。三條規則各隔 20 秒越線，標籤 `aiops_detect`、`aiops_severity`、`service` | `record_webhooks.py` |
| `webhook_resolved_mq／cim／amhs.json` | 同三筆告警恢復時推出來的內容（同一個 fingerprint，有 `endsAt`） | `record_webhooks.py` |
| `es_proxy_logs_traces.json` | `POST /api/datasources/proxy/uid/<uid>/_msearch`：錯誤日誌 + 錯誤 span（Editor Token） | `record.py` |
| `annotation_create／patch／list.json` | 註記的建立、更新、依標籤查回（Editor Token） | `record.py` |
| `meta.json` | Grafana 版本、錄製時間、資料源 uid | `record.py` |

webhook 檔是 `{"headers", "body"}`（body 就是 Grafana 送的 JSON）；其他是 `{"request", "status", "response"}`。
測試不會執行這兩支腳本；要重錄就照腳本開頭寫的環境起服務再跑。
