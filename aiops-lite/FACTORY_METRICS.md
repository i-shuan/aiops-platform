# 工廠端沒有 Metrics 時怎麼收值

老舊的 CIM、MES、AMHS、MQ 常常沒辦法直接掛 Prometheus exporter。這份文件說明三個系統各自怎麼把「日誌」或「資料庫紀錄」
變成 Grafana 告警規則能用的數字，再經 Webhook 進 aiops-lite。

**aiops-lite 不需要真正的 Prometheus metrics。** 它只收 Grafana 的告警 Webhook，不管 Grafana 背後查的是 metrics、日誌還是資料庫。
所以每個系統都有兩條路：

- **路線一：Grafana 直接查原始資料算數字**（ES 計數、SQL 查表）。不用多養元件，最快。
- **路線二：轉成真的 metrics 存進時序庫**（exporter、日誌轉指標）。規則查詢比較便宜，但多一段設定要維護。

**建議的做法是三個系統都照下面的「統一欄位規範」把日誌送進 ES**：告警與 RCA 證據共用同一份資料，aiops-lite 不用改程式。
SQL 查表與 exporter 仍然可以用來觸發告警，但那樣 RCA 不會有該系統的明細。

## 先看這一節：誰做什麼、兩條路的差別

### 分工

| 元件 | 做什麼 | 不做什麼 |
| --- | --- | --- |
| Fluent Bit（各產線伺服器） | **只做單行拆欄位**：讀一行日誌，切出 `eqp_id`、`error_type` 等欄位，送進 ES | 不算次數、不算平均、不判斷門檻 |
| Elasticsearch | 存結構化日誌；index template 規定每個欄位的型別 | — |
| Grafana | **跨機統計與門檻**：算最近幾分鐘的筆數、平均、最大值，超標發 Webhook；另外當代理讓 aiops-lite 查 ES | — |
| aiops-lite | 收告警、收斂、去 ES 撈錯誤日誌樣本並統計機台／批號出現次數、交給 LLM、寫註記 | **不洗資料、不拆欄位、不算告警用的數字** |

aiops-lite 這一側唯一要配合的是 `config.yaml`：欄位名稱（`es.log_fields`、`es.id_fields`）、錯誤等級的寫法（`es.error_levels`）、
關係邊（`edges`）要跟實際的資料對上。改的是設定，不是程式。

**為什麼統計不放在 Fluent Bit**

- aiops-lite 的 RCA 需要原始日誌在 ES 裡（錯誤訊息樣本、機台批號統計都從 ES 撈）。原始日誌無論如何都要送進 ES，
  再在 Fluent Bit 算一次次數是重複做工。
- 時間窗與門檻放在 Grafana 規則，要調整在網頁上改就生效，不用動產線伺服器。
- Fluent Bit 只做拆欄位，設定比較穩定；但日誌格式變了、多了新的錯誤類型，parser 還是要改。

Fluent Bit 的 `log_to_metrics` 可以在本機把日誌變成計數器，送到 Prometheus 之後用 `sum()` 加總成全廠數字，算得準，
只是要多養一套 Prometheus。ES 空間不足時，比較好的折衷是**只送 `ERROR`、`WARN` 等級的日誌進 ES**（`INFO` 不送或抽樣）：
aiops-lite 本來就只撈錯誤等級的日誌，證據不受影響。改成只送指標、不送日誌的話，告警照響，但 RCA 就沒有日誌證據。

### 兩條路

**路線 A：走日誌（進 ES）**——CIM-EAP、MQ 腳本、有日誌檔的 AMHS

```
日誌 ─▶ Fluent Bit（拆欄位）─▶ ES ─▶ Grafana（算次數、發告警）─▶ aiops-lite（回頭經 Grafana 去 ES 撈證據）
```

**路線 B：走資料庫（不進 ES）**——只能查唯讀 View 的系統

```
DB 唯讀 View ─▶ Grafana（直接寫 SQL 算次數、發告警）─▶ aiops-lite（只收到告警，ES 裡沒有它的日誌）
```

| | 路線 A（日誌進 ES） | 路線 B（直連 DB） |
| --- | --- | --- |
| 告警觸發、收斂、開事故、寫註記 | 正常 | 正常 |
| LLM 看得到的 | 告警（系統、規則、嚴重度、開始時間、觸發當下的數字、是否已恢復）、它在關係邊的位置、**錯誤日誌樣本、機台／批號／載具的出現次數** | 告警（同左）、它在關係邊的位置。**沒有日誌樣本、沒有識別碼統計** |
| LLM 講得出來的 | 哪個系統先出事、錯誤訊息是什麼、集中在哪台機台與哪個批號 | 這個系統有異常、比上下游早或晚響；講不出是哪個載具、哪個 Stocker、為什麼 |

**混用時的副作用**：機台與批號的統計只來自有日誌的系統。CIM 走路線 A、AMHS 走路線 B 的話，LLM 看到的錯誤集中點全是 CIM 的機台，
結論容易偏向 CIM。這是依程式行為推論的，沒有用真模型實測過偏差有多大。

**怎麼選**

1. 系統有文字日誌檔：走路線 A。三個系統的證據完整度才會一致。
2. 沒有日誌檔、資料只在資料庫：路線 B 是權宜之計。想補上明細，可以用同步工具（例如 Logstash 的 JDBC input）
   把資料庫裡的 ABORT、BLOCKED 紀錄定時寫成下面「統一欄位規範」的 ES 文件；告警可以繼續走 SQL。代價是多維護一個同步元件（沒有實測過）。

## 驗證狀態（照實寫）

| 做法 | 狀態 |
| --- | --- |
| 統一欄位規範的 index template、三種聚合（算筆數、取最大值、取平均）、aiops-lite 的證據查詢 | 用真的 ES 8.19.23 驗過（2026-11-21，本機，文件裡的範例文件）；aiops-lite 那段是直接打 ES，沒有經過 Grafana 代理 |
| 型別沒先宣告、數值寫成字串時聚合會失敗 | 用真的 ES 8.19.23 重現過（回 400） |
| SQL 查表當 Grafana 規則 → Webhook → aiops-lite | 實測過（本機 Grafana 13.0.7 + PostgreSQL，假資料） |
| ES 計數當 **Grafana 告警規則** | **沒有實測過**（只驗了 ES 這一側的聚合）；是 Grafana 支援的標準做法 |
| Fluent Bit 的 parser | **沒有用 Fluent Bit 跑過**；範例的正規表示式只用 Python 對範例那一行驗過 |
| IBM MQ exporter、Fluent Bit `log_to_metrics`、`sql_exporter`、textfile collector、Logstash JDBC | **沒有實測過**；細節以各工具的官方文件為準 |

下面的查詢與設定是範例，表名、欄位名、門檻都要換成現場的。

## 統一欄位規範（建議三個系統都照這個送進 ES）

**為什麼要統一**：aiops-lite 的欄位對照（`config.yaml` 的 `es.log_fields`）只有一組，不能每個系統各設一套。
三個系統的欄位名稱不一樣的話，只有對得上的那個系統撈得到 RCA 證據。統一之後，同一份 ES 資料 Grafana 拿來算告警、
aiops-lite 拿來取證，各系統只要拆一次欄位。

### 欄位

| 欄位 | ES 型別 | 必要性 | 內容 | 誰會用 |
| --- | --- | --- | --- | --- |
| `@timestamp` | `date` | **必要** | 事件發生的時間（UTC 或帶時區） | Grafana、aiops-lite |
| `service.name` | `keyword` | **必要** | 系統名稱：`cim-eap`、`amhs-mcs`、`mq-broker`。要跟告警規則的 `service` 標籤、`config.yaml` 的 `edges` **完全一樣（分大小寫）** | aiops-lite 用它篩這個系統的日誌 |
| `log.level` | `keyword` | **必要** | 一律大寫：`ERROR`、`WARN`、`INFO`（另外可用 `FATAL`、`CRITICAL`） | aiops-lite 只撈錯誤等級的當證據 |
| `message` | `text` | **必要** | 原始訊息（人看得懂的那一句） | 給 LLM 看的樣本 |
| `error_type` | `keyword` | 建議 | 錯誤分類，大寫加底線：`T3_TIMEOUT`、`DISCONNECT`、`LOT_HOLD`、`TRANSFER_ABORT`、`TRANSFER_BLOCKED`、`CONSUMER_LAG` | Grafana 規則用它算筆數 |
| `eqp_id` | `keyword` | 有就填 | 機台、Stocker、Port 的編號 | aiops-lite 統計出現次數 |
| `lot_id` | `keyword` | 有就填 | 批號 | 同上 |
| `carrier_id` | `keyword` | 有就填 | 載具（FOUP）編號 | 同上 |
| `queue` | `keyword` | MQ | 佇列名稱 | Grafana 分組 |
| `queue_depth`、`consumers` | `long` | MQ | 佇列深度、消費者連線數 | Grafana 取最大值／最後一筆 |
| `transfer_seconds` | `float` | AMHS | 單次搬運耗時（秒） | Grafana 取平均 |
| `pending_jobs` | `long` | AMHS | 待執行任務數 | Grafana 取最後一筆 |

index 名稱建議 `factory-logs-<系統>`（例如 `factory-logs-cim`），`config.yaml` 的 `es.log_index` 填 `factory-logs-*`。

### 規則

1. **四個必要欄位缺一不可。** 少了 `service.name` 或 `log.level`，aiops-lite 一筆日誌都撈不到（它用這兩個欄位篩）。
   `error_type` 與識別碼沒拆的話，告警還是可以用全文檢索觸發、LLM 還是讀得到原始訊息，只是機台與批號的統計會是空的。
2. **只有錯誤等級的日誌會成為 RCA 證據。** `INFO` 的狀態回報（例如 MQ 每 30 秒印一次佇列深度）可以給 Grafana 算告警，
   但不會出現在 LLM 的證據裡。要讓它進證據，超過門檻時另外印一行 `ERROR`（見下面 MQ 的範例）。
3. **數值欄位的型別要先宣告。** 沒有先建 index template、第一筆又寫成字串（`"850"`）的話，ES 會把它當文字，
   之後取最大值或平均會直接失敗（實測回 400）。
4. **識別碼的寫法要固定。** `EQP-CVD-07` 與 `eqp-cvd-07` 會被當成兩台機台分開統計。
5. **欄位名稱不想照這份規範也可以**，但三個系統要用同一套，然後把名稱寫進 `config.yaml`：
   時間、系統、等級、訊息對應 `es.log_fields`，識別碼對應 `es.id_fields`，等級的寫法對應 `es.error_levels`。

### 三個系統的文件範例

```json
{"@timestamp": "2026-11-21T02:41:07Z", "service": {"name": "cim-eap"}, "log": {"level": "ERROR"},
 "message": "HSMS T3 Timeout", "error_type": "T3_TIMEOUT",
 "eqp_id": "EQP-CVD-07", "lot_id": "LOT-9921", "carrier_id": "FOUP0912"}
```

```json
{"@timestamp": "2026-11-21T02:41:50Z", "service": {"name": "amhs-mcs"}, "log": {"level": "ERROR"},
 "message": "transfer aborted: destination port occupied", "error_type": "TRANSFER_ABORT",
 "eqp_id": "STK01", "carrier_id": "FOUP0912", "transfer_seconds": 412.5}
```

```json
{"@timestamp": "2026-11-21T02:41:10Z", "service": {"name": "mq-broker"}, "log": {"level": "INFO"},
 "message": "queue status", "queue": "lot.dispatch", "queue_depth": 120, "consumers": 4}
{"@timestamp": "2026-11-21T02:41:40Z", "service": {"name": "mq-broker"}, "log": {"level": "ERROR"},
 "message": "queue depth over limit, no consumer", "error_type": "CONSUMER_LAG",
 "queue": "lot.dispatch", "queue_depth": 4210, "consumers": 0}
```

把這幾筆寫進 ES 8.19.23 之後實測的結果：`error_type:T3_TIMEOUT` 算得出筆數、`queue_depth` 取得到最大值、`transfer_seconds` 取得到平均；
aiops-lite 撈到三個系統各自的 `ERROR` 日誌（`INFO` 的兩筆沒有進來），機台、批號、載具的出現次數也統計得出來。

### ES 的 index template（第 2 步：規定型別）

```json
PUT _index_template/factory-logs
{
  "index_patterns": ["factory-logs-*"],
  "template": {"mappings": {"properties": {
    "@timestamp": {"type": "date"},
    "service": {"properties": {"name": {"type": "keyword"}}},
    "log": {"properties": {"level": {"type": "keyword"}}},
    "message": {"type": "text"},
    "error_type": {"type": "keyword"},
    "eqp_id": {"type": "keyword"}, "lot_id": {"type": "keyword"}, "carrier_id": {"type": "keyword"},
    "queue": {"type": "keyword"}, "queue_depth": {"type": "long"}, "consumers": {"type": "long"},
    "transfer_seconds": {"type": "float"}, "pending_jobs": {"type": "long"}
  }}}
}
```

這份 template 在 ES 8.19.23 套用成功。要在第一筆資料寫進去**之前**建好；已經存在的 index 型別改不了，要重建。

### 收集端拆欄位的範例（第 1 步）

以 `2026-11-21 10:41:07 [ERROR] EQP-CVD-07 HSMS T3 Timeout on LOT-9921 carrier FOUP0912` 這種格式為例，
Fluent Bit 的 parser 大致長這樣（**沒有用 Fluent Bit 跑過**，正規表示式只對這一行驗過能拆出六個欄位；實際格式要照現場的日誌改）：

```ini
[PARSER]
    Name        eap_log
    Format      regex
    Regex       ^(?<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(?<level>[A-Z]+)\] (?<eqp_id>EQP-[A-Z0-9-]+) (?<message>.*?)(?: on (?<lot_id>LOT-?[A-Z0-9]+))?(?: carrier (?<carrier_id>FOUP[0-9]+))?$
    Time_Key    time
    Time_Format %Y-%m-%d %H:%M:%S
```

拆出來的 `level` 要再搬到 `log.level`、補上固定的 `service.name: cim-eap`、依訊息內容補 `error_type`，
這些用收集器的欄位改寫功能或 ES 的 Ingest Pipeline 做。日誌的時間如果是當地時間，收集端要設好時區，不然事故時間窗會對不上。

### 三個系統怎麼照規範接

| 系統 | 資料怎麼進 ES | Grafana 規則 |
| --- | --- | --- |
| CIM-EAP | Fluent Bit 讀日誌檔、拆欄位（下面第 1 節） | 對 `error_type` 算最近 5 分鐘的筆數 |
| AMHS | MCS 有日誌檔：同 CIM。資料只在資料庫：用同步工具（例如 Logstash 的 JDBC input）把 ABORT、BLOCKED 紀錄寫成上面格式的文件 | 對 `error_type:TRANSFER_ABORT` 算筆數；對 `transfer_seconds` 取平均 |
| MQ | 定時腳本每 30～60 秒印一行 JSON（上面的範例），超過門檻時多印一行 `ERROR` | 對 `queue_depth` 取最大值；對 `consumers` 取最後一筆 |

**跟下面「AMHS 走 SQL、MQ 走 exporter」的差別**：那兩條路告警一樣進得來，但 aiops-lite 的證據只從 ES 撈，
所以 RCA 不會有 AMHS、MQ 的明細（LLM 只知道哪條規則響了與觸發當下的數字）。要有明細，就要讓它們的錯誤紀錄也照規範進 ES。

## 各系統的收值方式（總覽）

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
- [ ] ES 的 index template 已經在第一筆資料進來之前建好（數值欄位是數字、識別碼是 keyword）。
- [ ] 三個系統用同一套欄位名稱；`log.level` 的寫法一致（建議一律大寫）。
- [ ] ES 日誌至少有時間、服務、等級、訊息四個欄位；名稱跟預設不同的話，已經寫進 `config.yaml` 的 `es.log_fields`。
- [ ] 走 SQL 或 exporter 觸發告警的系統，錯誤紀錄也有照規範進 ES（不然 RCA 沒有該系統的明細）。
- [ ] 日誌等級的寫法（例如 `E`、`SEVERE`）跟預設不同的話，已經寫進 `es.error_levels`。
- [ ] 要統計的識別碼欄位（預設 `eqp_id`、`lot_id`、`carrier_id`）已經拆出來；名稱不同的話已經寫進 `es.id_fields`。
- [ ] Grafana 的通知路由把 `aiops_detect = true` 的告警送到 aiops-lite 的 Contact Point，而且沒有勾 Disable resolved message。

相關文件：Grafana 端的設定步驟見 `README.md` 的「Grafana 要做的設定」；`config.yaml` 每一項的說明見 `CONFIG.md`。
