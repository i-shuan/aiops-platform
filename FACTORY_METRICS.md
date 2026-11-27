# 工廠端沒有 Metrics 時怎麼收值

老舊的 CIM、MES、AMHS、MQ 常常沒辦法直接掛 Prometheus exporter。這份文件說明三個系統各自怎麼把「日誌」或「資料庫紀錄」
變成 Grafana 告警規則能用的數字，再經 Webhook 進 aiops-lite。

**aiops-lite 不需要真正的 Prometheus metrics。** 它只收 Grafana 的告警 Webhook，不管 Grafana 背後查的是 metrics、日誌還是資料庫。
所以每個系統都有兩條路：

- **路線一：Grafana 直接查原始資料算數字**（VictoriaLogs 計數、SQL 查表）。不用多養元件，最快。
- **路線二：轉成真的 metrics 存進時序庫**（exporter、日誌轉指標）。規則查詢比較便宜，但多一段設定要維護。

**建議的做法是三個系統都照下面的「統一欄位規範」把日誌送進日誌資料庫**：告警與 RCA 證據共用同一份資料，aiops-lite 不用改程式。
SQL 查表與 exporter 仍然可以用來觸發告警，但那樣 RCA 不會有該系統的明細。

**日誌資料庫可以是 VictoriaLogs 或 Elasticsearch**（`config.yaml` 的 `logs.type`，預設 `victorialogs`）。欄位規範兩種都一樣；
這份文件的範例與指令以 VictoriaLogs 為主，用 Elasticsearch 時不一樣的地方集中寫在「改送 Elasticsearch」那一小節。
Grafana 規則用哪個資料源算次數，跟 aiops-lite 用哪個來源查證據，用同一個比較好懂，但不是必須。

## 先看這一節：誰做什麼、兩條路的差別

### 分工

| 元件 | 做什麼 | 不做什麼 |
| --- | --- | --- |
| Fluent Bit（各產線伺服器） | **只做單行拆欄位**：讀一行日誌，切出 `eqp_id`、`error_type` 等欄位，送進 VictoriaLogs | 不算次數、不算平均、不判斷門檻 |
| VictoriaLogs（或 Elasticsearch） | 存結構化日誌。VictoriaLogs 沒有 mapping、不用先宣告型別，寫入時指定哪個欄位是時間、訊息、stream；Elasticsearch 要注意 mapping（見「改送 Elasticsearch」） | — |
| Grafana | **跨機統計與門檻**：算最近幾分鐘的筆數、平均、最大值，超標發 Webhook；另外當代理讓 aiops-lite 查 VictoriaLogs | — |
| aiops-lite | 收告警、收斂、經 Grafana 去 VictoriaLogs 撈錯誤日誌樣本並統計機台／批號出現次數、交給 LLM、寫註記 | **不洗資料、不拆欄位、不算告警用的數字** |

aiops-lite 這一側唯一要配合的是 `config.yaml`：日誌存在哪一種（`logs.type`）、欄位名稱（`logs.fields`、`logs.id_fields`）、錯誤等級的寫法（`logs.error_levels`）、
系統之間的關係由程式從 Trace 與日誌學，不用手寫；要的是各處的系統名稱一致。

**為什麼統計不放在 Fluent Bit**

- aiops-lite 的 RCA 需要原始日誌在 VictoriaLogs 裡（錯誤訊息樣本、機台批號統計都從那裡撈）。原始日誌無論如何都要送進去，
  再在 Fluent Bit 算一次次數是重複做工。
- 時間窗與門檻放在 Grafana 規則，要調整在網頁上改就生效，不用動產線伺服器。
- Fluent Bit 只做拆欄位，設定比較穩定；但日誌格式變了、多了新的錯誤類型，parser 還是要改。

Fluent Bit 的 `log_to_metrics` 可以在本機把日誌變成計數器，送到 Prometheus 之後用 `sum()` 加總成全廠數字，算得準，
只是多一段設定要維護（指標可以直接送進 VictoriaMetrics）。儲存空間不足時，比較好的折衷是**只送 `ERROR`、`WARN` 等級的日誌**（`INFO` 不送或抽樣）：
aiops-lite 的證據主要來自錯誤與警告；代價是批號時間線上看不到正常的紀錄，「錯誤 0 筆」時也分不出是沒送還是沒錯。改成只送指標、不送日誌的話，告警照響，但 RCA 就沒有日誌證據。

### 兩條路

**路線 A：走日誌（進 VictoriaLogs）**——CIM-EAP、MQ 腳本、有日誌檔的 AMHS

```
日誌 ─▶ Fluent Bit（拆欄位）─▶ VictoriaLogs ─▶ Grafana（算次數、發告警）─▶ aiops-lite（回頭經 Grafana 去 VictoriaLogs 撈證據）
```

**路線 B：走資料庫（不進 VictoriaLogs）**——只能查唯讀 View 的系統

```
DB 唯讀 View ─▶ Grafana（直接寫 SQL 算次數、發告警）─▶ aiops-lite（只收到告警，VictoriaLogs 裡沒有它的日誌）
```

| | 路線 A（日誌進 VictoriaLogs） | 路線 B（直連 DB） |
| --- | --- | --- |
| 告警觸發、收斂、開事故、寫註記 | 正常 | 正常 |
| LLM 看得到的 | 告警（系統、規則、嚴重度、開始時間、觸發當下的數字、是否已恢復）、它在關係邊的位置、**錯誤日誌樣本、機台／批號／載具的出現次數** | 告警（同左）、它在關係邊的位置。**沒有日誌樣本、沒有識別碼統計** |
| LLM 講得出來的 | 哪個系統先出事、錯誤訊息是什麼、集中在哪台機台與哪個批號 | 這個系統有異常、比上下游早或晚響；講不出是哪個載具、哪個 Stocker、為什麼 |

**混用時的副作用**：機台與批號的統計只來自有日誌的系統。CIM 走路線 A、AMHS 走路線 B 的話，LLM 看到的錯誤集中點全是 CIM 的機台，
結論容易偏向 CIM。這是依程式行為推論的，沒有用真模型實測過偏差有多大。

**怎麼選**

1. 系統有文字日誌檔：走路線 A。三個系統的證據完整度才會一致。
2. 沒有日誌檔、資料只在資料庫：路線 B 是權宜之計。想補上明細，可以用同步工具（例如 Logstash 的 JDBC input）
   把資料庫裡的 ABORT、BLOCKED 紀錄定時寫成下面「統一欄位規範」的日誌送進 VictoriaLogs；告警可以繼續走 SQL。代價是多維護一個同步元件（沒有實測過）。

## 驗證狀態（照實寫）

| 做法 | 狀態 |
| --- | --- |
| aiops-lite 的證據查詢（錯誤日誌、各系統最早的錯誤、警告、事故前後的次數、批號時間線、0 筆時的計數）| 用真的 VictoriaLogs v1.17.0 經 Grafana 13.0.7 的資料源代理驗過（2026-11-26，本機，腳本寫的假資料）。資料照下面的欄位規範、用 `/insert/jsonline` 寫入 |
| 巢狀的 `service: {name: …}`、`log: {level: …}` 寫進去之後變成 `service.name`、`log.level` | 同上，實測過 |
| 同樣的證據查詢改從 **Elasticsearch** 撈（`logs.type: elasticsearch`） | 用真的 Elasticsearch 8.19.23 經 Grafana 13.0.7 的資料源代理驗過（2026-11-26，本機）：把跟 VictoriaLogs 同一批假資料寫進去，每一種查詢的結果、收集到的整包證據都一樣（`tests/test_backends.py`）。索引一個是明確的 mapping（keyword／text）、一個是動態 mapping |
| 用 LogsQL 算筆數（`stats count()`、`stats by (欄位) count()`） | 同上，實測過 |
| 對數值欄位取最大值、最小值（`stats max(queue_depth)`、`stats min(consumers)`） | 實測過（2026-11-26）：Fluent Bit 送進去的 MQ 探針資料，`dashboards/aiops-incident-metrics.json` 的四個面板在 Grafana 上畫得出來。`avg` 沒有測 |
| VictoriaLogs 的查詢當 **Grafana 告警規則** | 實測過一條（2026-11-26，Grafana 13.0.7 + 外掛 0.34.0）：`error_type:="T3_TIMEOUT" \| stats count() as value`、查詢型態 Stats、大於 3 就觸發，規則狀態變成 firing。沒有接著驗它推出來的 Webhook 內容（Webhook 的格式跟資料源無關，之前用別的資料源驗過） |
| SQL 查表當 Grafana 規則 → Webhook → aiops-lite | 實測過（本機 Grafana 13.0.7 + PostgreSQL，假資料） |
| Fluent Bit 設定（`deploy/fluent-bit/` 三份）拆欄位、`http` 輸出 → VictoriaLogs | 實測過（2026-11-26，Fluent Bit 5.2.0 + VictoriaLogs v1.17.0，本機，範例日誌約 30 行）：三份都回 HTTP 200，欄位攤平成 `service.name`、`log.level`、`eqp_id`…，訊息在 `_msg`，台灣時間轉成正確的 UTC，格式對不上的行整行進 `_msg` |
| Fluent Bit 的 `es` 輸出 → Elasticsearch | 2026-11-21 用 Fluent Bit 5.2.0 + Elasticsearch 8.19.23 跑過同樣三份設定的拆欄位部分（當時的輸出是 `name: es`、`suppress_type_name: on`）。現在 `deploy/fluent-bit/` 的範例輸出是 VictoriaLogs；換回 `es` 輸出之後沒有重跑 |
| Fluent Bit 的 TLS／帳密、日誌輪替、斷線補送 | **沒有實測過** |
| IBM MQ exporter、Fluent Bit `log_to_metrics`、`sql_exporter`、textfile collector、Logstash JDBC | **沒有實測過**；細節以各工具的官方文件為準 |

下面的查詢與設定是範例，表名、欄位名、門檻都要換成現場的。

## 統一欄位規範（建議三個系統都照這個送；VictoriaLogs 與 Elasticsearch 都一樣）

**為什麼要統一**：aiops-lite 的欄位對照（`config.yaml` 的 `logs.fields`）只有一組，不能每個系統各設一套。
三個系統的欄位名稱不一樣的話，只有對得上的那個系統撈得到 RCA 證據。統一之後，同一份日誌 Grafana 拿來算告警、
aiops-lite 拿來取證，各系統只要拆一次欄位。

### 欄位

| 欄位 | 值的樣子 | 必要性 | 內容 | 誰會用 |
| --- | --- | --- | --- | --- |
| `@timestamp` | RFC3339 時間 | **必要** | 事件發生的時間（UTC 或帶時區）。寫入時用 `_time_field=@timestamp` 指定，進去之後叫 `_time` | Grafana、aiops-lite |
| `service.name` | 文字 | **必要** | 系統名稱：`cim-eap`、`amhs-mcs`、`mq-broker`。要跟告警規則的 `service` 標籤、`config.yaml` 的 `edges` **完全一樣（分大小寫）** | aiops-lite 用它篩這個系統的日誌；建議當 stream 欄位 |
| `log.level` | 文字 | **必要** | 一律大寫：`ERROR`、`WARN`、`INFO`（另外可用 `FATAL`、`CRITICAL`） | aiops-lite 撈錯誤等級的當證據，警告另外撈 |
| `message` | 文字 | **必要** | 原始訊息（人看得懂的那一句）。寫入時用 `_msg_field=message` 指定，進去之後叫 `_msg` | 給 LLM 看的樣本；事故前後的次數也是比這一欄裡的詞 |
| `error_type` | 文字 | 建議 | 錯誤分類，大寫加底線：`T3_TIMEOUT`、`DISCONNECT`、`LOT_HOLD`、`TRANSFER_ABORT`、`TRANSFER_BLOCKED`、`CONSUMER_LAG` | Grafana 規則用它算筆數 |
| `eqp_id` | 文字 | 有就填 | 機台、Stocker、Port 的編號 | aiops-lite 統計出現次數 |
| `lot_id` | 文字 | 有就填 | 批號 | 同上；另外用來串跨系統的時間線 |
| `carrier_id` | 文字 | 有就填 | 載具（FOUP）編號 | 同上 |
| `trace_id` | 文字 | IT 服務有就填 | 這筆日誌所屬請求的 trace id | aiops-lite 用它直接取那一條呼叫鏈（`traces.trace_id_field`，預設就是 `trace_id`） |
| `queue` | 文字 | MQ | 佇列名稱 | Grafana 分組 |
| `queue_depth`、`consumers` | 數字 | MQ | 佇列深度、消費者連線數 | Grafana 取最大值／最後一筆 |
| `transfer_seconds` | 數字 | AMHS | 單次搬運耗時（秒） | Grafana 取平均 |
| `pending_jobs` | 數字 | AMHS | 待執行任務數 | Grafana 取最後一筆 |

VictoriaLogs 沒有索引名稱、也沒有 mapping：所有系統的日誌寫進同一個地方，靠欄位分。
要讓 aiops-lite 只看其中一部分（例如同一套 VictoriaLogs 還收了辦公室系統的日誌），在 `config.yaml` 的 `logs.extra_filter`
寫一段 LogsQL 條件，例如 `{namespace="factory"}`。Elasticsearch 則是用索引分：`logs.index` 填要查的索引（例如 `factory-logs-*`）。

### 規則

1. **四個必要欄位缺一不可。** 少了 `service.name` 或 `log.level`，aiops-lite 一筆日誌都撈不到（它用這兩個欄位篩）。
   暫時拆不出等級欄位時有過渡做法：`logs.fields.level` 設成 `""`，改用訊息文字挑錯誤日誌；會漏撈也會誤撈，見 `CONFIG.md`「日誌沒有等級欄位」。
   `error_type` 與識別碼沒拆的話，告警還是可以用訊息裡的詞觸發、LLM 還是讀得到原始訊息，只是機台與批號的統計會是空的。
2. **錯誤與警告等級的日誌才會成為 RCA 的主要證據。** `INFO` 的狀態回報（例如 MQ 每 30 秒印一次佇列深度）可以給 Grafana 算告警，
   但不會出現在錯誤日誌樣本裡（帶批號的話會出現在批號時間線上）。要讓它進證據，超過門檻時另外印一行 `ERROR`（見下面 MQ 的範例）。
3. **stream 欄位選值的種類少的。** 寫入時的 `_stream_fields` 用 `service.name` 這種只有幾個值的欄位；
   不要拿批號、載具、trace id 當 stream 欄位（值的種類太多會拖慢 VictoriaLogs）。數值欄位不用先宣告型別。
4. **識別碼的寫法要固定。** `EQP-CVD-07` 與 `eqp-cvd-07` 會被當成兩台機台分開統計。
5. **欄位名稱不想照這份規範也可以**，但三個系統要用同一套，然後把名稱寫進 `config.yaml`：
   系統、等級對應 `logs.fields`，識別碼對應 `logs.id_fields`，等級的寫法對應 `logs.error_levels`；
   時間與訊息：VictoriaLogs 用寫入時的 `_time_field`、`_msg_field` 對應，aiops-lite 這一側不用設定；
   Elasticsearch 用 `logs.fields.time`、`logs.fields.message` 對應（預設 `@timestamp`、`message`）。

### 三個系統的日誌範例

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

巢狀的寫法（`"service": {"name": …}`）與帶點的扁平鍵（`"service.name": …`）都可以：寫進 VictoriaLogs 之後都是 `service.name`、`log.level`（實測過）。

### 寫進 VictoriaLogs 與檢查（第 2 步）

寫入用 JSON 行的介面，在網址上指定三件事：哪個欄位是時間、哪個是訊息、哪些當 stream 欄位。

```
POST <VictoriaLogs>/insert/jsonline?_stream_fields=service.name&_msg_field=message&_time_field=@timestamp
```

一行一筆 JSON。不用先建任何東西；要注意的是日誌的時間不能比保留期限還舊，否則會被丟掉。

寫進去之後，用 aiops-lite 會走的同一條路（Grafana 的資料源代理、Editor Token）檢查：

```bash
# 各系統各有幾筆（看 service.name 的值跟告警的 service 標籤是不是一模一樣）
curl -s -H "Authorization: Bearer $TOKEN" "$GRAFANA/api/datasources/proxy/uid/$UID/select/logsql/query" \
  -d 'query=* | stats by ("service.name") count() as total' \
  -d "start=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)"

# 某個系統最新的三筆錯誤（看 log.level、_msg、eqp_id、lot_id 有沒有拆出來）
curl -s -H "Authorization: Bearer $TOKEN" "$GRAFANA/api/datasources/proxy/uid/$UID/select/logsql/query" \
  -d 'query="service.name":="cim-eap" "log.level":in("ERROR") | sort by (_time) desc | limit 3' \
  -d "start=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)"
```

`$UID` 是 Grafana 上 VictoriaLogs 資料源的 uid。回應是一行一個 JSON。第一條查不到東西代表日誌沒有進來或時間不對；
第二條查不到、第一條有，代表系統名稱或等級的寫法對不上。

### 改送 Elasticsearch

欄位規範與上面的日誌範例完全一樣，不一樣的只有四件事：

1. **aiops-lite 的設定**：`logs.type: elasticsearch`、`logs.datasource_uid` 填 Grafana 上 Elasticsearch 資料源的 uid、
   `logs.index` 填索引（例如 `factory-logs-*`，要對得到 Fluent Bit 寫進去的索引名稱）。`logs.extra_filter` 這時不能設。
2. **mapping**：`message` 要是 text 型別（挑錯誤字樣、事故前後的次數都是對它做全文比對）；`service.name`、`log.level`、識別碼建議 keyword。
   沒有先宣告、讓 Elasticsearch 動態建成 text 加 `.keyword` 也查得到。`@timestamp` 要是 date。要給 Grafana 取平均、最大值的數值欄位
   （`queue_depth`、`transfer_seconds`）要是數字型別，不能被建成文字。
3. **Fluent Bit 的輸出**：把範例裡的 `http` 輸出換成 `es` 輸出（`name: es`、`host`、`port`、`index`、`suppress_type_name: on`；
   Elasticsearch 8 一定要 `suppress_type_name`）。拆欄位的部分不用動。`tail` 的 `key: message` 一樣要設：預設的 `log` 會跟 `log.level`
   這個欄位衝突，Elasticsearch 會拒收。
4. **檢查指令**：改打 `_msearch`，或直接在 Grafana 的 Explore 用那個資料源查 `service.name:"cim-eap" AND log.level:ERROR`。

事故前後的次數比對在兩種資料庫上都是近似，而且比法不同（VictoriaLogs 分大小寫、Elasticsearch 不分），同一批資料數出來可能差一點。

### 收集端拆欄位的設定（第 1 步）：Fluent Bit 範例

三份可以直接改的設定在 `deploy/fluent-bit/`，一個系統一份，裝在該系統的伺服器上，只讀日誌檔：

| 檔案 | 來源 | 做的事 |
| --- | --- | --- |
| `cim-eap.yaml` | EAP 的文字日誌 | 拆出時間、等級、機台、訊息、批號、載具；依訊息補 `error_type`（T3_TIMEOUT、DISCONNECT、LOT_HOLD） |
| `amhs-mcs.yaml` | MCS 的文字日誌（key=value 風格） | 拆出載具、來源設備、搬運耗時（轉成數字）；依結果補 `error_type`（TRANSFER_ABORT、TRANSFER_BLOCKED） |
| `mq-broker.yaml` | MQ 探針腳本印的 JSON 行 | 直接讀 JSON；檔案開頭規定了腳本要印成什麼樣子 |

三份的共同做法：`tail` 讀檔 → parser 拆欄位 → `modify` 補上 `service.name`、把 `level` 改名成 `log.level` → 用 `http` 輸出送到 VictoriaLogs 的
`/insert/jsonline?_stream_fields=service.name&_msg_field=message&_time_field=@timestamp`。
不算次數、不判斷門檻。

**驗證狀態**

- 2026-11-26 用 Fluent Bit 5.2.0 把三份設定各跑一次（本機、範例日誌約 30 行），送進 VictoriaLogs v1.17.0：三份都能啟動、寫入回 HTTP 200；
  欄位拆得出來（`service.name`、`log.level`、`eqp_id`、`lot_id`、`carrier_id`、`error_type`、`queue_depth`…），訊息本文在 `_msg`；
  台灣時間的日誌（`time_offset: '+0800'`）轉成正確的 UTC；格式對不上的行整行進 `_msg`，不會丟掉。
- 沒有驗的：帳密與 TLS（範例裡註解掉的那三行）、日誌輪替、VictoriaLogs 斷線後的補送、Windows。

**改設定時要注意的四件事**（都是實測時踩到或確認過的）

1. **日誌格式是假設的**。parser 的 `regex` 要照現場的日誌改；改完先用幾行真實日誌在測試機跑過。
2. **`modify` 的 `condition` 裡不能有空白**（會被當成參數分隔，Fluent Bit 啟動失敗）。要比對空白用 `\s`，例如 `NOT\sSELECTED`。
3. **`tail` 要設 `key: message`**。預設的鍵名是 `log`，拆不開的行會變成 `log: "整行文字"`：訊息不在 `_msg_field` 指定的欄位裡，
   預期 VictoriaLogs 會收下來但訊息本文對不到（這個錯誤的設法沒有實測），aiops-lite 拿到的樣本就沒有內容。
4. **時區**：日誌寫的是當地時間就要設 `time_offset`，不然會被當成 UTC，事故時間窗差 8 小時，aiops-lite 會撈不到。

沒有驗的：TLS 與帳號密碼連 VictoriaLogs、日誌輪替（log rotation）、VictoriaLogs 長時間斷線後的補送、大流量下的效能。

### 三個系統怎麼照規範接

| 系統 | 資料怎麼進 VictoriaLogs | Grafana 規則 |
| --- | --- | --- |
| CIM-EAP | Fluent Bit 讀日誌檔、拆欄位（下面第 1 節） | 對 `error_type` 算最近 5 分鐘的筆數 |
| AMHS | MCS 有日誌檔：同 CIM。資料只在資料庫：用同步工具（例如 Logstash 的 JDBC input）把 ABORT、BLOCKED 紀錄寫成上面格式的日誌 | 對 `error_type:TRANSFER_ABORT` 算筆數；對 `transfer_seconds` 取平均 |
| MQ | 定時腳本每 30～60 秒印一行 JSON（上面的範例），超過門檻時多印一行 `ERROR` | 對 `queue_depth` 取最大值；對 `consumers` 取最後一筆 |

**跟下面「AMHS 走 SQL、MQ 走 exporter」的差別**：那兩條路告警一樣進得來，但 aiops-lite 的日誌證據只從 `logs.type` 指定的那個日誌資料庫撈，
所以 RCA 不會有 AMHS、MQ 的明細（LLM 只知道哪條規則響了與觸發當下的數字；MQ 的指標有進 VictoriaMetrics 的話，可以用 `metrics.queries` 補上走勢）。
要有明細，就要讓它們的錯誤紀錄也照規範進 VictoriaLogs。

## 各系統的收值方式（總覽）

| 系統 | 建議做法 | 要算的數字 | 需要誰配合 |
| --- | --- | --- | --- |
| CIM-EAP | 日誌進 VictoriaLogs，Grafana 規則對它算筆數 | T3 逾時次數、機台斷線次數、lot hold 次數（每 5 分鐘） | CIM 團隊：裝日誌收集器、拆欄位 |
| AMHS | 唯讀 View，Grafana 用 SQL 資料源定時查 | 搬運中止數、待執行任務數、平均搬運耗時 | AMHS 團隊：開 View 或 Read-Replica、給唯讀帳號 |
| MQ | 先找現成 exporter；沒有才寫定時腳本 | 佇列深度、消費者連線數、最舊訊息年齡 | MQ 管理員：開唯讀監控帳號 |

## 1. CIM-EAP：日誌轉計數

EAP 本來就會把 SECS/GEM、HSMS 的日誌寫到本機磁碟，不用改 EAP 程式。

**步驟**

1. **收集**：掛 Fluent Bit（或 Vector、Filebeat 這類收集器），只讀日誌檔，送進公司的 VictoriaLogs。
2. **拆欄位（最重要）**：在收集端把純文字拆成結構化欄位。
   例如 `[ERROR] EQP-CVD-07 HSMS T3 Timeout on LOT-9921` 拆成：

   | 欄位 | 值 | 誰會用到 |
   | --- | --- | --- |
   | `log.level` | `ERROR` | aiops-lite 用它篩錯誤日誌 |
   | `service.name` | `cim-eap` | aiops-lite 用它篩這個系統的日誌 |
   | `message` | 原始訊息 | 給 LLM 看的樣本 |
   | `eqp_id` | `EQP-CVD-07` | 機台出現次數統計 |
   | `lot_id` | `LOT-9921` | 批號出現次數統計 |
   | `error_type` | `T3_TIMEOUT` | Grafana 規則用它算筆數 |

3. **規則**：Grafana 告警規則選 VictoriaLogs 資料源，查詢 `"service.name":="cim-eap" error_type:="T3_TIMEOUT" | stats count() as value`，
   查詢型態選 Stats，時間範圍取最近 5 分鐘，大於門檻就觸發（同一種寫法的規則實測會觸發，見上面的驗證狀態）。

**想要真的 metrics**：Fluent Bit 的 `log_to_metrics` 過濾器可以把符合條件的日誌直接變成 Prometheus counter，
規則就改成查 PromQL（注意 `increase()` 會外插出小數）。

**短時間拆不了欄位**：規則可以先比訊息裡的詞（`_msg:seq("T3","Timeout")`）觸發告警；代價是 aiops-lite 的機台、批號統計會是空的，
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

**Grafana 開源版能連哪些資料庫**

開源版（免費版）就能連開源資料庫，不需要 Enterprise 授權。

| 資料庫 | 開源版 Grafana | 驗證狀態 |
| --- | --- | --- |
| PostgreSQL | 內建資料源外掛，不用另外安裝 | **實測過**（見下表） |
| MySQL／MariaDB | 內建資料源外掛 | 安裝檔裡有這個外掛；沒有實際連過 |
| Microsoft SQL Server | 內建資料源外掛 | 安裝檔裡有這個外掛；沒有實際連過 |
| Oracle | 開源版的安裝檔裡**沒有**這個外掛；就我們所知它是 Enterprise 版才有的 | 沒有驗證過；上線前請向 Grafana 官方文件確認 |

資料庫是 Oracle、又只有開源版 Grafana 時，可以用下面的 `sql_exporter`，或把統計值定時同步到 PostgreSQL。

實測（2026-11-24，本機：Grafana 13.0.7，API 回報 `edition = Open Source`；PostgreSQL 16.15；假資料 4320 筆）：

| 測試 | 結果 |
| --- | --- |
| 建立 PostgreSQL 資料源 | 成功（型別 `grafana-postgresql-datasource`） |
| Save & test | `Database Connection OK` |
| 用 Editor Token 下 SQL 統計（`group by`、`avg`、`sum`） | 成功 |
| 時序格式的查詢（圖表與告警規則用的格式） | 成功 |
| 用只有 `SELECT` 權限的帳號寫入 | 被資料庫擋下（`permission denied`） |

**不想讓 Grafana 直連資料庫**：用 `sql_exporter` 定時查同一句 SQL，轉成 Prometheus metrics，規則改查 PromQL。

## 3. MQ：先找現成的 exporter

| MQ | 做法 |
| --- | --- |
| IBM MQ | IBM 有開源的 Prometheus exporter（`mq-metric-samples`）：用唯讀監控帳號連 queue manager，就有佇列深度、連線數等指標 |
| RabbitMQ、Kafka 等 | 多半內建或有成熟的 exporter，直接用 |
| Tibco EMS 等沒有 exporter 的 | 寫定時腳本，見下 |

**沒有 exporter 時的定時腳本**：每 30～60 秒呼叫 MQ 的管理指令或管理 API，拿到佇列深度與消費者連線數，用下面兩種方式之一輸出。

- **印成一行 JSON 日誌送進 VictoriaLogs**，Grafana 規則對數值欄位取 max 或 last：

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
   告警進得來只需要規則與標籤；RCA 的日誌證據是 aiops-lite 另外經 Grafana 去日誌資料庫（VictoriaLogs 或 Elasticsearch）查的，用的是 `config.yaml` 的 `logs.type`、`logs.fields`。
   欄位名稱對不上、或日誌等級的寫法不在 `logs.error_levels` 裡，就撈不到任何日誌（註記的 LLM 摘要會只根據告警與關係邊；註記最後一行會寫出是哪一關沒過）。

## 進 aiops-lite 前的檢查清單

- [ ] 每條規則都貼了 `aiops_detect=true`、`aiops_severity`（P0～P3）、`service`。
- [ ] `service` 的值在三個地方一致：規則標籤、日誌的服務欄位（預設 `service.name`）、有 Trace 的話還有 Trace 的服務名稱。
- [ ] 規則查詢回傳的是一個數字（SQL 聚合成一列、LogsQL 用 `stats count()` 或對數值欄位聚合）。
- [ ] 規則的 No data 行為已經明確選定。
- [ ] 每個來源都有一條「資料還在進來」的規則。
- [ ] `config.yaml` 的 `logs.type` 跟日誌實際存的地方一致（VictoriaLogs 或 Elasticsearch）。
- [ ] VictoriaLogs：寫入時有指定 `_time_field`、`_msg_field`、`_stream_fields`；stream 欄位沒有用到批號這類值很多的欄位。
- [ ] Elasticsearch：`message` 是 text 型別、數值欄位是數字型別；`logs.index` 對得到實際的索引名稱。
- [ ] 三個系統用同一套欄位名稱；`log.level` 的寫法一致（建議一律大寫）。
- [ ] 日誌至少有時間、服務、等級、訊息四個欄位；服務與等級的名稱跟預設不同的話，已經寫進 `config.yaml` 的 `logs.fields`。
- [ ] 用上面「寫進 VictoriaLogs 與檢查」的兩條指令（Elasticsearch 用 Grafana 的 Explore），經 Grafana 查得到各系統的日誌與錯誤。
- [ ] 走 SQL 或 exporter 觸發告警的系統，錯誤紀錄也有照規範進日誌資料庫（不然 RCA 沒有該系統的明細）。
- [ ] 日誌等級的寫法（例如 `E`、`SEVERE`）跟預設不同的話，已經寫進 `logs.error_levels`。
- [ ] 要統計的識別碼欄位（預設 `eqp_id`、`lot_id`、`carrier_id`）已經拆出來；名稱不同的話已經寫進 `logs.id_fields`。
- [ ] Grafana 的通知路由把 `aiops_detect = true` 的告警送到 aiops-lite 的 Contact Point，而且沒有勾 Disable resolved message。

相關文件：Grafana 端的設定步驟見 `README.md` 的「Grafana 要做的設定」；`config.yaml` 每一項的說明見 `CONFIG.md`。
