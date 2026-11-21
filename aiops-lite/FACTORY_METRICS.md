# 工廠端沒有 Metrics 時怎麼收值

老舊的 CIM、MES、AMHS、MQ 常常沒辦法直接掛 Prometheus exporter。這份文件說明三個系統各自怎麼把「日誌」或「資料庫紀錄」
變成 Grafana 告警規則能用的數字，再經 Webhook 進 aiops-lite。

**aiops-lite 不需要真正的 Prometheus metrics。** 它只收 Grafana 的告警 Webhook，不管 Grafana 背後查的是 metrics、日誌還是資料庫。
所以每個系統都有兩條路：

- **路線一：Grafana 直接查原始資料算數字**（ES 計數、SQL 查表）。不用多養元件，最快。
- **路線二：轉成真的 metrics 存進時序庫**（exporter、日誌轉指標）。規則查詢比較便宜，但多一段設定要維護。

## 驗證狀態（照實寫）

| 做法 | 狀態 |
| --- | --- |
| SQL 查表當 Grafana 規則 → Webhook → aiops-lite | 實測過（本機 Grafana 13.0.7 + PostgreSQL，假資料） |
| ES 計數當 Grafana 規則 | **沒有在這個 repo 實測過**；是 Grafana 支援的標準做法 |
| IBM MQ exporter、Fluent Bit `log_to_metrics`、`sql_exporter`、textfile collector | **沒有實測過**；是各工具的標準用法，版本與設定細節請以各工具的官方文件為準 |

下面的查詢與設定是範例，表名、欄位名、門檻都要換成現場的。

## 總覽

| 系統 | 建議做法 | 要算的數字 | 需要誰配合 |
| --- | --- | --- | --- |
| CIM-EAP | 日誌進 ES，Grafana 規則對 ES 算筆數 | T3 逾時次數、機台斷線次數、lot hold 次數（每 5 分鐘） | CIM 團隊：裝日誌收集器、拆欄位 |
| AMHS | 唯讀 View，Grafana 用 SQL 資料源定時查 | 搬運中止數、待執行任務數、平均搬運耗時 | AMHS 團隊：開 View 或 Read-Replica、給唯讀帳號 |
| MQ | 先找現成 exporter；沒有才寫定時腳本 | 佇列深度、消費者連線數、最舊訊息年齡 | MQ 管理員：開唯讀監控帳號 |

## 1. CIM-EAP：日誌轉計數

EAP 本來就會把 SECS/GEM、HSMS 的日誌寫到本機磁碟，不用改 EAP 程式。

**步驟**

1. **收集**：掛 Filebeat 或 Fluent Bit，只讀日誌檔，送進公司的 Elasticsearch。
2. **拆欄位（最重要）**：在收集端或 ES Ingest Pipeline 把純文字拆成結構化欄位。
   例如 `[ERROR] EQP-CVD-07 HSMS T3 Timeout on LOT-9921` 拆成：

   | 欄位 | 值 | 誰會用到 |
   | --- | --- | --- |
   | `log.level` | `ERROR` | aiops-lite 用它篩錯誤日誌 |
   | `service.name` | `cim-eap` | aiops-lite 用它篩這個系統的日誌 |
   | `message` | 原始訊息 | 給 LLM 看的樣本 |
   | `eqp_id` | `EQP-CVD-07` | 機台出現次數統計 |
   | `lot_id` | `LOT-9921` | 批號出現次數統計 |
   | `error_type` | `T3_TIMEOUT` | Grafana 規則用它算筆數 |

3. **規則**：Grafana 告警規則選 ES 資料源，查詢 `error_type:T3_TIMEOUT`，指標選 Count，時間範圍取最近 5 分鐘，大於門檻就觸發。

**想要真的 metrics**：Fluent Bit 的 `log_to_metrics` 過濾器可以把符合條件的日誌直接變成 Prometheus counter，
規則就改成查 PromQL（注意 `increase()` 會外插出小數）。

**短時間拆不了欄位**：規則可以先用全文檢索（搜 `"T3 Timeout"`）觸發告警；代價是 aiops-lite 的機台、批號統計會是空的，
只能靠 LLM 從原始訊息裡讀出機台編號。

## 2. AMHS：SQL 查表

搬運狀態（ABORT、BLOCKED、搬運耗時）通常都在 AMHS 的資料庫裡。

**步驟**

1. 請 AMHS 團隊開一張**唯讀 View**（或給 Read-Replica 的連線），並給一個只有 `SELECT` 權限的帳號。
2. 在 Grafana 新增 SQL 資料源，連這個唯讀帳號。
3. 告警規則直接寫 SQL，回傳一個數字，後面接門檻：

```sql
-- 最近 5 分鐘的搬運中止數
SELECT COUNT(*) AS value
FROM amhs_transfer_view
WHERE status = 'ABORT'
  AND updated_at > NOW() - INTERVAL '5 minutes'
```

```sql
-- 最近 5 分鐘的平均搬運耗時（秒）
SELECT AVG(transfer_seconds) AS value
FROM amhs_transfer_view
WHERE finished_at > NOW() - INTERVAL '5 minutes'
```

**注意**

- **不要查正式交易大表**：連 Read-Replica，或請對方開限定時間範圍的 View。
- **評估頻率不要太密**：建議 1～3 分鐘一次，避免拖慢產線資料庫。
- 時間條件要用得到索引的欄位（例如 `updated_at`），不然每次評估都是全表掃描。

**不想讓 Grafana 直連資料庫**：用 `sql_exporter` 定時查同一句 SQL，轉成 Prometheus metrics，規則改查 PromQL。

## 3. MQ：先找現成的 exporter

| MQ | 做法 |
| --- | --- |
| IBM MQ | IBM 有開源的 Prometheus exporter（`mq-metric-samples`）：用唯讀監控帳號連 queue manager，就有佇列深度、連線數等指標 |
| RabbitMQ、Kafka 等 | 多半內建或有成熟的 exporter，直接用 |
| Tibco EMS 等沒有 exporter 的 | 寫定時腳本，見下 |

**沒有 exporter 時的定時腳本**：每 30～60 秒呼叫 MQ 的管理指令或管理 API，拿到佇列深度與消費者連線數，用下面兩種方式之一輸出。

- **印成一行 JSON 日誌送進 ES**，Grafana 規則對數值欄位取 max 或 last：

  ```json
  {"@timestamp": "2026-11-21T02:41:10Z", "service": {"name": "mq-broker"}, "queue": "lot.dispatch", "depth": 4210, "consumers": 0}
  ```

- **寫成 Prometheus 文字格式**，讓 node_exporter 的 textfile collector 收走：

  ```
  mq_queue_depth{service="mq-broker",queue="lot.dispatch"} 4210
  mq_consumer_count{service="mq-broker",queue="lot.dispatch"} 0
  ```

腳本要用唯讀的監控帳號，而且本身要有逾時：MQ 卡住時腳本也跟著卡住，就沒有資料了（見下一節第 1 點）。

## 工廠端一定會踩的三件事

1. **收集器掛了，計數會是 0，看起來像「一切正常」。**
   每個來源都要加一條「資料還在進來」的規則，例如「這個系統最近 10 分鐘的日誌總筆數等於 0 就告警」、
   「MQ 腳本最後一筆資料超過 3 分鐘沒更新就告警」。aiops-lite 不知道資料斷了：沒有告警進來，它就什麼都不做。
2. **Grafana 規則的 No data 行為要明確設定。**
   查不到資料時，規則會進入 NoData 狀態，可能另外發一則通知。每條規則都要在「Configure no data and error handling」
   明確選擇當成正常還是當成告警，不要留預設值碰運氣。
3. **規則能觸發，不代表 RCA 有證據。**
   告警進得來只需要規則與標籤；RCA 的日誌證據是 aiops-lite 另外去 ES 查的，用的是 `config.yaml` 的 `es.log_fields`。
   欄位名稱對不上、或日誌等級的寫法不在 `es.error_levels` 裡，就撈不到任何日誌（註記的 LLM 摘要會只根據告警與關係邊）。

## 進 aiops-lite 前的檢查清單

- [ ] 每條規則都貼了 `aiops_detect=true`、`aiops_severity`（P0～P3）、`service`。
- [ ] `service` 的值在三個地方一致：規則標籤、`config.yaml` 的 `edges`、ES 日誌的服務欄位（預設 `service.name`）。
- [ ] 規則查詢回傳的是一個數字（SQL 聚合成一列、ES 用 Count 或對數值欄位聚合）。
- [ ] 規則的 No data 行為已經明確選定。
- [ ] 每個來源都有一條「資料還在進來」的規則。
- [ ] ES 日誌至少有時間、服務、等級、訊息四個欄位；名稱跟預設不同的話，已經寫進 `config.yaml` 的 `es.log_fields`。
- [ ] 日誌等級的寫法（例如 `E`、`SEVERE`）跟預設不同的話，已經寫進 `es.error_levels`。
- [ ] 要統計的識別碼欄位（預設 `eqp_id`、`lot_id`、`carrier_id`）已經拆出來；名稱不同的話已經寫進 `es.id_fields`。
- [ ] Grafana 的通知路由把 `aiops_detect = true` 的告警送到 aiops-lite 的 Contact Point，而且沒有勾 Disable resolved message。

相關文件：Grafana 端的設定步驟見 `README.md` 的「Grafana 要做的設定」；`config.yaml` 每一項的說明見 `CONFIG.md`。
