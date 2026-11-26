# aiops-lite 的輸入與輸出：有哪些、最理想要有哪些

aiops-lite 自己不產生資料，也不直連任何外部系統。它能做到什麼程度，完全取決於**經 Grafana 拿得到什麼**。
這份文件列出全部的輸入、每一項的最低要求與最理想的狀態、少了它會怎樣，以及全部的輸出。

## 一張表看完

| # | 輸入 | 從哪來 | 用在哪 | 必要嗎 |
| --- | --- | --- | --- | --- |
| 1 | 告警 | 各系統在 Grafana 設的規則，經 Webhook 推進來 | 觸發一切；RCA 的第 1 包證據 | **必要**。沒有告警，平台什麼都不做 |
| 2 | 關係邊 | `config.yaml` 的 `edges`，人工宣告 | 跨系統收斂；第 2 包證據 | 建議。沒有的話只有同一個系統的告警會併 |
| 3 | 日誌 | VictoriaLogs 或 Elasticsearch（`logs.type`），經 Grafana 的資料源代理（VictoriaLogs 要在 Grafana 裝外掛 `victoriametrics-logs-datasource`） | 第 3 包（錯誤日誌）、第 4 包（機台與批號統計）、第 5 包（批號時間線） | 建議。沒有的話摘要多半是「證據不足」 |
| 4 | Trace | VictoriaTraces（經 Grafana 內建的 Jaeger 型別資料源，URL 指到它的 `/select/jaeger`）或 Tempo（`traces.type`） | 第 6 包（呼叫鏈） | 選用。有埋點的 IT 服務才有 |
| 5 | 指標 | VictoriaMetrics，經 Grafana 的 Prometheus 型別資料源（Prometheus、Mimir 也可以） | 第 7 包（指標走勢） | 選用。補上日誌與 Trace 看不到的情況 |
| 6 | LLM | OpenAI 相容的端點 | 把證據寫成一句摘要 | 選用。沒有的話註記第 2 行是 `[未經分析]` 的原始摘要 |
| 7 | Grafana Token | Editor 角色的服務帳號 | 以上 3～5 項的查詢、寫註記 | **必要** |

查資料只經 Grafana，另外只認得 LLM 與（有設定的話）兩個下游出口。第 3～5 項都是「Grafana 上有這個資料源、Token 看得到它」就能用，不需要另外開權限；
aiops-lite 不直連 VictoriaLogs、Elasticsearch、VictoriaTraces、Tempo、VictoriaMetrics 任何一個。日誌與 Trace 的來源各選各的；
兩種來源回的資料由程式轉成同一種形狀，分析的程式不分來源（`tests/test_backends.py` 把同一批資料寫進兩邊，比對整包證據一樣）。

## 對外的路：進來兩個、出去最多四個

這是 aiops-lite 全部的對外連線。進來的沒有變：告警只從 Grafana Webhook 進來（Alertmanager 的入口先不做）。
出去的原本只有 Grafana 與 LLM；2026-11-26 起多了兩個下游出口，沒設定就是佔位、不會連出去。

| 方向 | 路 | 用途 |
| --- | --- | --- |
| 進來 | `POST /webhook` | Grafana 推告警進來（要帶 Webhook 密鑰） |
| 進來 | `GET /healthz` | Kubernetes 探針 |
| 出去 | Grafana（一把 Editor Token） | 經資料源代理查日誌、Trace、指標；寫註記 |
| 出去 | LLM | 把證據寫成摘要 |
| 出去 | 事故 API（`outputs.incident_api`，選用） | 事故開單、更新、結案時通知下游（告警中心、派工系統） |
| 出去 | 自動修復（`outputs.remediation`，選用） | 請下游（Conductor）跑告警規則上指定的修復流程 |

另外只有自己的資料庫（SQLite 或 PostgreSQL），不算對外。輸出有三種：Grafana 上的註記、事故 API、修復請求，細節見最後的「輸出」一節。
對照架構圖：aiops-lite 是圖上的 Alert Bot，事故 API 對應告警中心／建單，自動修復對應 Conductor。

**每張事故會對 Grafana 做幾次查詢**（只在要分析時才查：新事故、有新告警併入或嚴重度升級、LLM 重試）

| 查詢 | VictoriaLogs／VictoriaTraces | Elasticsearch／Tempo | 什麼時候 |
| --- | --- | --- | --- |
| 錯誤日誌 | 1 | 1 | 有設定 `logs.datasource_uid` |
| 各系統最早的錯誤、警告 | 各 1 | 合成 1（每個系統一個子查詢，加警告一個） | 有撈到錯誤日誌；警告要日誌有等級欄位、而且沒有關掉 |
| 事故前後的筆數 | 1（一條查詢算完所有種類） | 1（每種錯誤兩個子查詢） | 有撈到錯誤日誌，而且有要比較的錯誤或警告 |
| 日誌筆數（分清楚為什麼是 0 筆） | 1 | 1 | 錯誤日誌是 0 筆（跟上面兩項不會同時發生） |
| 批號／載具的時間線 | 每種關聯碼欄位 2（內容、筆數） | 每種關聯碼欄位 1 | 錯誤日誌裡有批號或載具 |
| 呼叫鏈 | 日誌裡的 trace id 逐個取（預設最多 6 個）；不夠才搜尋，每個系統 1 次，回來就是整條 | trace id 一樣逐個取；不夠才搜尋 1 次（一次問完所有系統），只回編號，要用到的再逐條取 | 有設定 `traces.datasource_uid` |
| 指標走勢 | 查詢數 × 系統數 | 同左 | 有設定 `metrics` |
| 寫註記 | 1～3（查現有的、必要時刪掉重建） | 同左 | 每次內容有變 |

Elasticsearch 的查詢都是 `_msearch`（一個請求裡放多個子查詢）。

進行中的事故每 60 秒續跑一輪時**不會**重新查這些，只檢查能不能結案、補寫註記。

**要留意的**

- 事故前後的筆數比對的是訊息裡的詞，往前多看 30 分鐘（`logs.baseline_minutes`）。只在本機的小資料量上跑過，
  沒有在大量資料上量過耗時。查詢變慢的話，把 `llm.max_log_patterns` 與 `llm.max_warn_patterns` 都設成 0 可以關掉這一項。
- 錯誤日誌 0 筆時多問的那一次，會數時間窗裡**全部**的日誌（不限系統）。資料量大的時候要多久沒有量過；
  VictoriaLogs 用 `logs.extra_filter` 把範圍縮到相關的 stream、Elasticsearch 用 `logs.index` 指到相關的索引，可以減少要掃的量。
- 事故前後的筆數兩種來源的比法不同：VictoriaLogs 用 `seq`（分大小寫），Elasticsearch 用 intervals 查詢（不分大小寫、訊息欄位要是 text）。
  兩邊都是近似，斷詞規則也不同，同一批資料數出來可能差一點（測試樣本上兩邊數字相同）。
- Elasticsearch 的索引不存在不算錯誤，會被當成「一筆都沒有」，註記上的提示會寫「檢查 logs.index」。
- 更早的「日誌資料庫裡的 Trace 錯誤片段」那一包證據沒有回來；Trace 只有呼叫鏈（第 6 包）。

**例外**：`eval/` 的評估工具會直連 VictoriaLogs（或 Elasticsearch）寫測試資料。它是開發時手動跑的，不會被服務載入，也不進映像檔。

## 三個等級

| | 最低可用 | 建議 | 最理想 |
| --- | --- | --- | --- |
| 告警 | 每個系統至少一條規則，貼了三個標籤 | 每個系統有「壞了」與「快壞了」兩類規則 | 加上趨勢型規則（見下）與資料斷流的規則 |
| 關係邊 | 沒有 | 主要的上下游都有宣告 | 跟實際的接線一致，有人負責更新 |
| 日誌 | 沒有 | 四個必要欄位齊全 | 再加機台、批號、載具、trace id，各系統時鐘有對時 |
| Trace | 沒有 | IT 服務有埋點，錯誤有標成 error | MQ 兩端都有傳追蹤編號，錯誤日誌帶 trace id |
| 指標 | 沒有 | 每個系統有流量、錯誤率、延遲 | 再加佇列、資料庫連線、資源飽和度 |
| 結果 | 事故卡與收斂，摘要是原始告警清單 | 摘要能指出哪個系統先出錯、訊息是什麼 | 摘要能分辨「誰壞了」與「誰只是被拖累」 |

## 1. 告警（必要）

**格式**：Grafana 告警的 Webhook（Contact Point），實測版本是 Grafana 13.0.7。

**規則上要貼的標籤**（名稱可在 `labels.*` 改）：

| 標籤 | 值 | 沒貼會怎樣 |
| --- | --- | --- |
| `aiops_detect` | `true` | 這筆告警不納管 |
| `service` | 系統名稱 | 這筆告警被略過，並回報給送出端 |
| `aiops_severity` | `P0`～`P3` | 當成 `labels.default_severity`（預設 P3） |
| `aiops_remediation`（選用） | 修復流程的名稱 | 這條告警沒有自動修復（見「輸出」） |

**程式會讀的欄位**：`status`（firing／resolved）、`fingerprint`、`startsAt`、`endsAt`、`labels`、`ruleUID`、規則名稱、`values`。

**最理想**

- `service` 的值在告警、關係邊、日誌、Trace、指標五個地方**完全一樣**。這是把所有輸入接在一起的唯一鍵。
- 每個系統的規則涵蓋四類：錯誤（錯誤率、失敗筆數）、延遲、飽和（佇列堆積、連線池）、斷流（資料停止更新）。
- resolved 有送出來（Contact Point 不要勾 Disable resolved message），不然事故不會結案。
- 嚴重度照影響範圍貼，不要全部 P1。
- 規則的評估值（`values`）有意義：它會原樣交給 LLM。

### 趨勢型規則：抓「還沒超標但走勢不對」

固定門檻抓不到慢慢惡化的情況。下面這些寫法都只需要改 Grafana 規則，aiops-lite 不用動，告警進來的方式跟其他規則一樣。

以 RabbitMQ 的佇列為例，在真的資料上算出來的值（2026-11-25 實測，見「驗證狀態」）：

| 規則的想法 | 查詢 | 正常期 | 堆積期 |
| --- | --- | --- | --- |
| 固定門檻 | `sum by (queue) (rabbitmq_queue_messages_ready{service="mq-broker"})` | 0 | 1007 |
| 變化率：每秒增加幾筆 | `sum by (queue) (deriv(rabbitmq_queue_messages_ready{service="mq-broker"}[1m]))` | 0 | 10 |
| 照目前斜率，10 分鐘後會到多少 | `sum by (queue) (predict_linear(rabbitmq_queue_messages_ready{service="mq-broker"}[1m], 600))` | 0 | 7025 |
| 跟一段時間前比的倍數 | `X / (X offset 3m + 1)`，`X` 是第一列的查詢 | 沒有結果 | 1007 |
| 有訊息但沒有消費者 | `X > 0 unless sum by (queue) (rabbitmq_queue_consumers{service="mq-broker"}) > 0` | 沒有結果 | 1007 |

怎麼選：

- **變化率**與**預測**適合會累積的量（佇列、磁碟、連線數）。預測型的好處是在真的滿之前就告警。
- **跟之前比**適合有週期的量（流量、交易數）。實務上用 `offset 1d` 或 `offset 1w` 跟昨天、上週同時段比；上表用 3 分鐘只是因為測試資料只有幾分鐘。
- **組合條件**（有訊息但沒有消費者）最不容易誤報，因為它描述的是一個明確的故障狀態。
- 「沒有結果」代表條件不成立，Grafana 規則要把 No Data 設成 OK，不然會變成另一種告警。

## 2. 關係邊（建議）

`config.yaml` 的 `edges`，每條是 `[上游, 下游]`。

- 收斂時不分方向：兩個有告警的系統之間有邊就併成一張事故。
- 交給 LLM 時帶方向：上游的問題會影響下游。

**最理想**

- 名稱跟告警的 `service` 完全一樣。
- 涵蓋不會出現在 Trace 與日誌裡的基礎設施（MQ broker、資料庫），這些只能靠宣告。
- 有人負責在系統改接線時更新。

**限制**：邊沒有型別，同步呼叫與佇列的傳播方式不同，目前分不出來；不會自動學習。

## 3. 日誌（建議）

VictoriaLogs（LogsQL）或 Elasticsearch（Query DSL），用 `logs.type` 選；欄位規範兩種都一樣，完整說明在 `FACTORY_METRICS.md`。
下表的「預設名稱」是日誌送出時的欄位名稱。系統名稱與等級的欄位名稱可在 `logs.fields` 改。時間與訊息：進了 VictoriaLogs 之後固定叫
`_time`、`_msg`（寫入時用 `_time_field`、`_msg_field` 對應），不用也不能設定；Elasticsearch 可以用 `logs.fields.time`、`logs.fields.message` 改
（預設 `@timestamp`、`message`），要查的索引用 `logs.index`。

| 欄位 | 預設名稱 | 等級 | 少了會怎樣 |
| --- | --- | --- | --- |
| 時間 | `@timestamp` | 必要 | 查不到 |
| 系統名稱 | `service.name` | 必要 | 一筆都撈不到（用它篩） |
| 等級 | `log.level` | 必要 | 一筆都撈不到；可以把 `logs.fields.level` 留空改用訊息文字挑，會漏撈也會誤撈。留空時也不會撈警告 |
| 訊息 | `message` | 必要 | 樣本沒有內容 |
| 機台、批號、載具 | `eqp_id`、`lot_id`、`carrier_id` | 建議 | 沒有「集中在哪台機台」的統計，也沒有批號時間線 |
| trace id | `trace_id` | 建議（IT 服務） | 呼叫鏈只能靠搜尋：拿到的是這段時間這些系統的失敗請求，不保證就是日誌裡報錯的那幾條 |
| 訊息編號、關聯編號 | 自訂，加進 `logs.id_fields` | 選用 | MQ 兩端沒有埋點時，可以用它把生產者與消費者的紀錄排在一起 |

**最理想**

- 各系統用同一套欄位名稱（欄位對照只有一組）。
- VictoriaLogs：訊息本文有對到 `_msg`（寫入時的 `_msg_field`），挑錯誤字樣、事故前後的次數比較都是在 `_msg` 裡比對詞；
  stream 欄位選值的種類少的（例如 `service.name`），不要拿批號、trace id 當 stream 欄位。
- Elasticsearch：訊息欄位是 text 型別；系統名稱、等級、識別碼是 keyword（動態 mapping 的 text 加 `.keyword` 也查得到）。
- 警告（WARN）也有送進來，而且訊息寫得出具體狀況（「連線池 92%」比「slow」有用）：出錯前的警告常常就是原因。
- 事故前至少保留 30 分鐘以上的日誌：比較事故前後要用。
- 識別碼拆成獨立欄位，不要只寫在訊息文字裡。
- 各系統的時鐘有對時：時間線靠時間戳排序，差幾秒順序就可能排錯。
- 敏感資訊在寫日誌之前就去識別化：送給 LLM 的樣本沒有遮罩。

## 4. Trace（選用）

VictoriaTraces（實測 v0.5.0）或 Tempo（實測 2.9.0），用 `traces.type` 選。應用程式都是用 OTLP 送進去；aiops-lite 讀 VictoriaTraces 是經 Grafana 的
Jaeger 型別資料源、用 Jaeger 相容的查詢介面，讀 Tempo 是經 Grafana 的 Tempo 資料源、用 TraceQL 搜尋。Tempo 回的 OTLP 由程式轉成跟 VictoriaTraces 同一種形狀。

**最理想**

- `service.name` 跟告警的 `service` 一樣。
- 失敗的 span 有把狀態設成 error，並帶錯誤訊息。搜尋只找有錯誤的 Trace（OTLP 的 status=ERROR 進 VictoriaTraces 之後是 `error=true` 標籤，
  錯誤訊息在 `otel.status_description`；程式認的就是這兩個，另外也認 `otel.status_code=ERROR`。Tempo 用 TraceQL 的 `status = error` 搜尋）。
- 錯誤日誌帶 `trace_id`：有的話直接用編號取那一條，不必靠搜尋。用 Tempo 時特別重要：它收到 Trace 後 8～40 秒以上才搜尋得到
  （2.9.0 實測），用編號則立刻取得到；VictoriaTraces 三秒內就搜尋得到。
- 取樣不要把失敗的請求丟掉。
- 對外的呼叫帶 `peer.service`、`db.system` 這類屬性：這樣沒有埋點的資料庫也會出現在呼叫鏈上。
- **MQ 兩端都傳追蹤編號**（訊息標頭的 `traceparent`）。用真的 RabbitMQ 與自動埋點實測過的結果
  （當時 Trace 存在 Tempo 2.9.0；這四種情況**沒有在 VictoriaTraces 上重跑**）：

| 情況 | aiops-lite 撈得到什麼 |
| --- | --- |
| 消費端接在生產端底下（pika 自動埋點的預設） | 完整的鏈，並標出失敗點 |
| 消費端另開一條 Trace、用連結指回去 | 兩條各自獨立，看不到是誰送的（程式目前不讀連結） |
| 送了沒人收 | 什麼都沒有：生產端沒有錯誤可找 |
| 中途的轉發程式沒帶標頭 | 兩條各自獨立，補不回來 |

**限制**

- Jaeger 的查詢介面一次只能搜尋一個服務，所以 VictoriaTraces 是每個系統各問一次；Tempo 一次問完，但只回編號，整條要另外取。
- 鏈上會帶 `messaging.system`、`messaging.destination` 這兩個屬性（有的話），但沒有用真的 MQ Trace 在 VictoriaTraces 上確認過看得出哪一段是佇列；
  也沒有明講訊息在佇列裡等了多久。

## 5. 指標（選用）

VictoriaMetrics（Grafana 上建成 Prometheus 型別的資料源；MetricsQL 相容 PromQL）。事故發生時，對事故裡的每個系統跑 `metrics.queries` 設定的查詢，時間窗是事故開始前 `lookback_minutes`
（預設 60 分鐘）到現在，壓成這幾個數字交給 LLM：

- `baseline`：事故開始前的中位數。
- `during`：事故開始後的最小、最大、最後一個值，以及最大值出現的時間。
- `last_vs_baseline`、`max_vs_baseline`：相對基準的倍數（基準是 0 就不算）。
- `points`：整段時間平均抽樣的幾個點。

**這一包只做算術，不判斷算不算異常。** 沒有任何門檻；判斷仍然是 Grafana 規則與 LLM 的事。

設定範例：

```yaml
metrics:
  datasource_uid: "<Grafana 上指到 VictoriaMetrics 的 Prometheus 型別資料源的 uid>"
  queries:
    - {name: 請求量（每秒）, expr: 'sum(rate(http_requests_total{service="$service"}[5m]))'}
    - {name: 錯誤率, expr: 'sum(rate(http_requests_total{service="$service",code=~"5.."}[5m])) / sum(rate(http_requests_total{service="$service"}[5m]))'}
    - {name: 延遲 p95（秒）, expr: 'histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{service="$service"}[5m])))'}
    - {name: 佇列待處理訊息數, expr: 'sum by (queue) (rabbitmq_queue_messages_ready{service="$service"})', services: [mq-broker]}
    - {name: 佇列的消費者數, expr: 'sum by (queue) (rabbitmq_queue_consumers{service="$service"})', services: [mq-broker]}
```

`$service` 會換成系統名稱（做過跳脫）；每條查詢都要有它。指標名稱與標籤要照你們實際的資料改，上面只有 RabbitMQ 那兩條實測過（待處理訊息數那一條在 VictoriaMetrics 上也查過）。

**最理想：每類系統要有的指標**

| 系統類型 | 指標 | 回答什麼問題 |
| --- | --- | --- |
| 對外提供服務的 | 請求量、錯誤率、延遲 | 是流量暴增造成的，還是流量正常但變慢 |
| MQ | 各佇列的待處理訊息數、未確認訊息數、消費者數、發佈速率 | 是生產者送太多、消費者處理不及，還是消費者不見了 |
| 資料庫 | 連線數、慢查詢數、鎖等待 | 是資料庫本身慢，還是被上游打爆 |
| 主機與容器 | CPU、記憶體、磁碟用量、重啟次數 | 是資源不足，還是程式本身的問題 |
| 工廠系統（沒有原生指標） | 用日誌或資料庫紀錄算出來的計數（見 `FACTORY_METRICS.md`） | 目前只能當告警，**還不能當這一包的證據**（見下） |

**限制**

- 指標要有一個標籤能對到系統名稱。
- 只支援 Prometheus 相容的查詢（PromQL／MetricsQL）。工廠端用日誌筆數算出來的趨勢（每分鐘幾筆逾時）目前不會進這一包。
- 每張事故要查「查詢數 × 系統數」次，查詢最多 20 條。
- 基準用事故前的中位數：事故前就已經在慢慢爬升的話，基準會被拉高。

## 6. LLM（選用）

OpenAI 相容的 chat completions 端點。拿到上面七包證據，回一句摘要與它點名的系統、識別碼。

- 點名了證據裡沒有的系統、機台、批號：整份退回，註記標 `[未經分析]`。
- 連不上、逾時、格式不對：同樣退回，並指數退避重試。
- 送過去的內容**沒有遮罩**。機密環境請把 `llm.url` 指到公司內部的模型。

## 各種故障，靠哪個輸入看得到

| 故障 | 告警 | 日誌 | Trace | 指標 |
| --- | --- | --- | --- | --- |
| 服務回錯誤 | 看得到 | 看得到錯誤訊息 | 看得到失敗在哪一步 | 看得到錯誤率變化 |
| 下游的資料庫連不上 | 要有規則 | 要有寫進訊息 | 看得到，即使資料庫沒有告警 | 要有資料庫指標 |
| MQ 消費者不見了 | 要有規則 | 消費者通常沒有錯誤日誌 | **看不到** | 看得到：消費者數歸零、佇列堆積 |
| MQ 消費者處理不及 | 要有規則 | 不一定 | 看得到，但等待時間要自己算 | 看得到：佇列持續上升而消費者數正常 |
| 流量暴增 | 要有規則 | 看不出來 | 看不出來 | 看得到 |
| 工廠設備之間的批號流向異常 | 要有規則 | 看得到（要有批號欄位） | 沒有埋點就看不到 | 看不到 |
| 資料停止送進來 | 要有斷流規則 | 沒有日誌就是沒有 | 沒有 | 沒有 |

最後一列是所有輸入共同的盲點：資料本身斷了，每一種證據都會是空的。只能靠規則去抓「超過幾分鐘沒有新資料」。

## 輸出

| 輸出 | 什麼時候 | 內容 |
| --- | --- | --- |
| Grafana 註記 | 事故內容有變 | 事故卡、根因分析與 1～3 個可能原因、證據那一行；有修復流程時多一行 `自動修復｜…` |
| 事故 API（`outputs.incident_api.url`） | 事故開單（opened）、內容更新（updated）、結案（closed）；同樣的內容只送一次 | POST 一筆 JSON，`schema` 是 `aiops-lite.incident.v1` |
| 自動修復（`outputs.remediation.url`，要 `enabled: true`） | 規則上貼了 `aiops_remediation=<流程名稱>` 的告警在響、事故還沒結案；同一張事故、同一條告警、同一個流程只送一次 | POST 一筆 JSON，`schema` 是 `aiops-lite.remediation.v1` |

事故 API 的欄位：`event`；`incident`{`uid`、`status`、`severity`、`services`、`started_at`、`ended_at`、`alert_count`、
`alerts`[{`fingerprint`、`rule_uid`、`title`、`service`、`severity`、`status`、`starts_at`}]}；`root_cause`{`analyzed`、`summary`、`candidates`[]}；
`evidence`；`grafana`{`url`、`annotation_id`、`tag`}。

修復請求的欄位：`workflow`、`incident_uid`、`service`、`alert`{`fingerprint`、`rule_uid`、`title`、`severity`}、
`context`{`root_cause_analyzed`、`root_cause_summary`}。

- **跑哪個流程只看告警規則上的標籤；LLM 的推論不會觸發、也不會選流程。** `context` 只是給人看的說明。
- **佔位**：沒設 url（或修復沒有 `enabled`）就不送，把本來會送的內容記進資料庫的 `deliveries` 表。佔位記過的之後不補送。
- 有填 `token` 就帶 `Authorization: Bearer`；送失敗每一輪重試、最多 10 次；失敗不影響註記。設定見 `CONFIG.md`「註記以外的出口」。
- 註記上的 `自動修復｜流程名稱（系統：已送出）` 只代表請求有沒有送出去，不代表流程跑完或修好了。
- **格式是這個專案自己定的，沒有跟任何真的告警中心或 Conductor 對過。**

## 驗證狀態

**實測過的：兩種來源的組合**（2026-11-26，本機拋棄式環境，資料是腳本產生的，不是產線資料）

- 元件：Grafana 13.0.7（外掛 `victoriametrics-logs-datasource` 0.34.0）；(a) VictoriaLogs v1.17.0、VictoriaTraces v0.5.0、
  VictoriaMetrics v1.110.0，都是單機版的官方執行檔；(b) Elasticsearch 8.19.23、Tempo 2.9.0（指標仍是 VictoriaMetrics）。
- 每個資料源都用同一把 Editor Token 經 Grafana 的資料源代理查得到；aiops-lite 用到的每一種查詢都跑過，
  請求與回應原樣錄在 `tests/fixtures/grafana13/` 的 `victoria_logs.json`、`victoria_traces.json`、`elasticsearch.json`、`tempo_*.json`。
  同一批資料寫進 VictoriaLogs 與 Elasticsearch，收集到的整包證據一樣。
- 完整流程（起真的服務、送 Webhook、真的 LLM、寫回 Grafana 註記）兩種組合各跑一次，四種情況都寫出符合資料的註記：
  - 三個系統日誌齊全、MQ 另有佇列指標：排第一的是 mq-broker；證據那一行列出錯誤日誌、事故前後的次數比較、警告、
    識別碼、時間線，以及 VictoriaMetrics 查到的指標走勢 1 條。
  - 日誌的系統名稱對不上：註記寫出「這段時間有 N 筆日誌，但沒有一筆的 service.name 是這些系統」。
  - 有日誌但都是 INFO／WARN：註記寫出「這些系統有 3 筆日誌，但 log.level 沒有一筆是 ERROR／FATAL／CRITICAL」。
  - 有失敗請求的 Trace：取到 2 條呼叫鏈（VictoriaTraces、Tempo 各一次），排第一的是呼叫鏈最底層的 mes-db（它沒有告警）。
- 出口只對本機一個假的 HTTP 端點驗過：事故 API 收到 4 張事故的 opened；修復請求在 `enabled: true` 時送出 1 筆、註記顯示「已送出」；
  `enabled: false` 時不送、註記顯示「佔位，沒有送出」。
- RCA 的評估案例的數字見 `EVAL.md`。

**實測過的：更早的一輪**（2026-11-25，當時指標存在 Prometheus、Trace 存在 Tempo；同樣是本機、腳本產生的資料）

- 指標走勢：Grafana 13.0.7、Prometheus 3.5.0、RabbitMQ 3.12.1（內建的 `rabbitmq_prometheus`）。一個佇列先正常收送 180 秒，
  然後消費者停掉、生產者繼續每秒約 10 筆送 110 秒，佇列真的堆到 1007 筆。aiops-lite 用 Editor Token 經 Grafana 查到：
  待處理訊息數的基準 0、事故期間最大 1007；消費者數的基準 1、最後 0；發佈速率前後都是每秒約 10 筆。
  （測試用的指標回應樣本 `prom_query_range_*.json` 仍然是這一次從 Prometheus 錄的。）
- 同一份證據交給真的 LLM（`gpt-6-luna`）各一次：
  - 有指標走勢：「可能源頭是 mq-broker 的 eap.events 消費端失效，佇列堆積並影響 amhs-mcs；消費端失效原因證據不足」。
  - 沒有指標走勢：「證據不足以判定源頭」。
- 上面趨勢型規則表的五種查詢，在同一份資料的兩個時間點算出表中的值（Prometheus 上算的）。
- 語法寫錯的查詢：只有那一條記進補充說明，其他查詢與分析照常。系統名稱帶引號時查詢沒有被截斷。
- MQ 的四種 Trace 情況：RabbitMQ 3.12.1、pika 1.5.0、OpenTelemetry 的 pika 自動埋點、Tempo 2.9.0。

**沒有驗證的**

- 產線資料，以及公司的 Grafana 上能不能裝 VictoriaLogs 的資料源外掛（用 Elasticsearch 當日誌來源就不需要外掛）。
- 出口沒有跟任何真的告警中心或 Conductor 對過；下游要求 Bearer 以外的認證目前不支援；修復流程跑完的結果不會回來（只知道請求有沒有送出）。
- Victoria 的叢集版、vmauth、多租戶。程式不會帶 `AccountID`／`ProjectID` 標頭，多租戶目前不支援。
- 大量資料下的查詢時間。特別是「錯誤 0 筆時數整段時間的筆數」會掃時間窗裡全部的日誌；Elasticsearch 的 intervals 查詢在大量資料下的耗時也沒有量過。
- Fluent Bit 的認證（vmauth、TLS）、日誌輪替、斷線補送。三份範例設定送進 VictoriaLogs 跑過一次，欄位與時間都對，但沒有帳密。
- 趨勢型規則的五種查詢、MQ 的四種 Trace 情況沒有在 VictoriaMetrics／VictoriaTraces 上重跑。
- 趨勢型規則只算了查詢的值，沒有真的建成 Grafana 告警規則讓它觸發。`offset 1d`、`offset 1w` 沒有測（沒有那麼久的資料）。
- HTTP 請求量、錯誤率、延遲、資料庫、主機那幾條查詢範例沒有測，指標名稱要照實際的資料改。
- 指標走勢交給 LLM 的效果只測了少數情境、一個模型。
- 「最理想」那一欄是依功能需要什麼整理的，不是從現場量測出來的。
