# Grafana 13.0.7 的真實樣本

2026-11-21 從本機拋棄式環境錄的：Grafana 13.0.7（官方 tarball）、PostgreSQL 16、Elasticsearch 8.19.23。
資料是腳本灌的假資料，**不是產線資料**；真的是 Grafana 送出與回應的只有「格式」。內容原樣沒改。

| 檔案 | 內容 | 怎麼錄的 |
| --- | --- | --- |
| `webhook_firing_mq／cim／amhs.json` | Grafana 的 Webhook Contact Point 實際推出來的告警（firing）。三條規則各隔 20 秒越線，標籤 `aiops_detect`、`aiops_severity`、`service` | `record_webhooks.py` |
| `webhook_resolved_mq／cim／amhs.json` | 同三筆告警恢復時推出來的內容（同一個 fingerprint，有 `endsAt`） | `record_webhooks.py` |
| `annotation_create／patch／list.json` | 註記的建立、更新、依標籤查回（Editor Token） | `record.py` |
| `victoria_logs.json` | 經 Grafana 代理查 VictoriaLogs v1.17.0（外掛 victoriametrics-logs-datasource 0.34.0）：`src/grafana.py` 每一個日誌查詢方法實際送出的請求與回應，加上寫進去的原始資料。2026-11-26 錄 | `record_victoria.py` |
| `victoria_traces.json` | 經 Grafana 的 Jaeger 型別資料源查 VictoriaTraces v0.5.0：搜尋失敗的 Trace、用編號取、編號不存在（404）。2026-11-26 錄 | `record_victoria.py` |
| `elasticsearch.json` | 經 Grafana 代理查 Elasticsearch 8.19.23：`src/logs.py` 的 Elasticsearch 每一個查詢方法、加上一次完整的證據收集，實際送出的請求與回應。資料跟 `victoria_logs.json` 是同一批。2026-11-26 錄 | `record_elasticsearch.py` |
| `tempo_search.json`、`tempo_trace.json`、`tempo_trace_missing.json`、`tempo_search_bad_query.json` | 經 Grafana 代理查 Tempo 2.9.0：TraceQL 搜尋錯誤 Trace、用編號取、不存在（404、沒有內容）、TraceQL 寫錯（400）。送進去的三條 Trace 跟 VictoriaTraces 的是同一組。2026-11-23 錄 | `record_tempo.py` |
| `meta.json` | Grafana 版本、錄製時間、資料源 uid | `record.py` |

webhook 檔是 `{"headers", "body"}`（body 就是 Grafana 送的 JSON）；`victoria_*.json` 是 `{"exchanges": {名稱: [{"request", "status", "response"}]}}`，
回應是原樣的文字（VictoriaLogs 一行回一個 JSON 物件）；其他是 `{"request", "status", "response"}`。

日誌樣本的時間是 2026-11-21 00:26～00:31，告警 webhook 的樣本是同一天 02:41 發的（分開錄的）。測試裡假的 Grafana 會照查詢的時間範圍篩資料，
所以預設把日誌與 Trace 的時間整批往後移 2 小時 10 分 30 秒（`tests/conftest.py` 的 `SHIFT`），其他內容不動。
測試不會執行這兩支腳本；要重錄就照腳本開頭寫的環境起服務再跑。
