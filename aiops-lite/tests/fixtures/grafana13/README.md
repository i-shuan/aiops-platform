# Grafana 13.0.7 的真實回應樣本

2026-11-21 用 `record.py` 從本機拋棄式環境錄的：Grafana 13.0.7（官方 tarball）、Prometheus 3.5.0、PostgreSQL 16、
Elasticsearch 8.19.23。資料是腳本灌的假資料（MQ 佇列深度、CIM T3 逾時、AMHS 搬運耗時、幾筆錯誤日誌），**不是產線資料**；
真的是 Grafana 回的只有「格式」。所有被錄下來的請求都用一把 **Editor** 服務帳號 Token 發出（確認 Editor 權限夠用）。
每個檔案是 `{"request", "status", "response"}`，response 原樣沒改。

| 檔案 | 內容 | 測試用途 |
| --- | --- | --- |
| `alert_rules.json` | `GET /api/v1/provisioning/alert-rules`：3 條貼了 `aiops_detect` 的規則 + 1 條沒貼的 | 規則納管、標籤讀取 |
| `query_promql.json` | `/api/ds/query` 重放 PromQL | 標準時序 |
| `query_sql_single_value.json` | SQL：time + 單一數值欄 | 合法格式 |
| `query_sql_multi_value.json` | SQL：time + 兩個數值欄 | 必須拒絕（不默默取第一欄） |
| `query_sql_long_format.json` | SQL 長格式：time + 字串欄 + 數值欄 | 必須拒絕 |
| `query_sql_no_timefilter.json` | SQL 沒寫 `$__timeFilter`：請求 15 分鐘、回了 3 小時 | 必須拒絕 |
| `es_proxy_logs_traces.json` | `POST /api/datasources/proxy/uid/<uid>/_msearch`：錯誤日誌 + 錯誤 span | 證據擷取、識別碼統計 |
| `query_promql_bad.json` | PromQL 寫錯：HTTP 400，錯誤在 `results.A.error` | 查詢錯誤算規則的問題 |
| `annotation_create／patch／list.json` | 註記的建立、更新、依標籤查回 | 讀到的欄位存在 |
| `meta.json` | Grafana 版本、錄製時間、資料源 uid | |

測試不會執行 `record.py`；要重錄就照它開頭寫的環境起服務再跑一次。
