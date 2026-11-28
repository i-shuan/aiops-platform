# aiops-lite

事件驅動的極簡 AIOps：公司 Grafana 的告警經 Webhook 推進來 → 等一段 debounce → 跨系統收斂成事故 → 經 Grafana 收集證據交給 LLM →
把根因分析寫回 Grafana 註記，並送給下游的事故 API（告警中心／派工）、對貼了修復流程標籤的告警送修復請求（Conductor）。

**平台不判斷什麼算異常。** 告警沒推進來就什麼都不做；程式只做查詢、歸類、計數、算術，判讀交給 LLM，程式再把關。
開發規範與不可違背的界線見 `CLAUDE.md`。這一頁是全部的設計、設定、上線與驗證狀態；更早的分頁文件（輸入、設定參考、評估紀錄、
上線清單、工廠端收值、Phase 2 設計）已經合進這一頁，原文在 Git 歷史。

```
Grafana 告警規則 ──Webhook──▶ aiops-lite ──Editor Token 經 Grafana 資料源代理──▶ 日誌／Trace／指標／service graph
                                │
                                ├──▶ Grafana 註記（一張事故一筆）
                                ├──▶ 事故 API（沒有下游＝佔位）
                                └──▶ 修復請求（預設關閉，只認規則上的標籤）
```

---

## 1. 進與出

| 方向 | 內容 | 狀態 |
| --- | --- | --- |
| 進 | Grafana 告警 Webhook：Bearer 驗證，先落地進資料庫再回 200（存不進去回 503，Grafana 會重送） | 唯一的入口 |
| 查資料 | 一把 Grafana Editor Token，全部經資料源代理 | 嚴禁直連日誌、Trace、指標資料庫與產線資料庫 |
| 出 1 | Grafana 註記：一張事故一筆，原地更新；標籤變了（結案、升級、新系統加入）刪掉重建 | 實作 |
| 出 2 | 事故 API：開單（`opened`）、更新（`updated`）、結案（`closed`）時 POST，`schema: aiops-lite.incident.v3` | 沒設網址＝佔位；**格式自訂、未對接** |
| 出 3 | 修復請求：規則上貼了 `aiops_remediation=<流程名稱>` 的告警在響時 POST，`schema: aiops-lite.remediation.v1` | 預設關閉；**格式自訂、未對接** |

除此之外不對外做任何事：不重啟、不改設定、不起子行程。

## 2. 支援的資料庫

| 類別 | 選項 | 設定 | 備註 |
| --- | --- | --- | --- |
| 日誌 | VictoriaLogs、Elasticsearch | `logs.type` | VictoriaLogs 要在公司 Grafana 裝外掛 `victoriametrics-logs-datasource`（唯一要動 Grafana 的地方） |
| Trace | VictoriaTraces（Grafana 的 Jaeger 型別資料源，URL 指到 `/select/jaeger`）、Tempo | `traces.type` | 兩種的回應由程式轉成同一種形狀 |
| 指標 | 一律 VictoriaMetrics（Grafana 的 Prometheus 型別資料源） | `metrics.datasource_uid` | 不另外對 Prometheus 驗證 |
| service graph | Tempo 的 metrics-generator 指標（存在 VictoriaMetrics）、VictoriaTraces 的相依關係 | `traces.service_graph_datasource_uid`（只有 Tempo 要設） | 只有 Tempo 的有延遲與失敗數 |
| 自己的帳本 | SQLite（本機）、PostgreSQL（Helm 一律用） | `database.url` | 啟動自動建表、補欄位 |

日誌與 Trace 各選各的。兩種來源給的整包證據一樣（同一批資料寫進兩邊比對過，`tests/test_backends.py`）。

## 3. 一輪的流程

1. **落地與等待**：Webhook 寫進 `inbox`，等 `debounce_seconds`（預設 60）讓上下游的告警到齊。等待期間重啟不掉告警。
2. **解析**：只收 `aiops_detect=true` 的告警；缺 `service` 的略過並記下來；嚴重度不是 P0～P3 的當 P3。
3. **學系統關係**（只在有新告警的輪次）：見第 5 節。
4. **收斂**：見第 4 節。
5. **收集證據**：見第 6 節。成員或嚴重度有變才重新分析，重複通知不重問。
6. **分析**：交給 LLM，程式檢查回答，見第 7 節。
7. **寫回**：先寫自己的資料庫，再寫註記，再送兩個出口。註記寫失敗下一輪補，不會重複建。
8. **續跑與結案**：有進行中事故時每 `followup_seconds`（60）續跑一輪；成員告警全部 resolved、再安靜 `close_after_seconds`（300）才結案，期間又響就同一張繼續。

實際看到註記的延遲 ≈ 規則的 `for` + Grafana 路由的 `group_wait` + `debounce_seconds` + LLM 回應時間。

## 4. 收斂（合併事故）

- 同一個 `service` 的告警併成一張；新告警碰到進行中的事故併進去。告警到達的順序不影響結果。
- **跨系統要同時滿足**：兩個系統都在告警，而且這段時間它們之間**有失敗的呼叫**（service graph 上有失敗數，或 Trace 裡那一段自己失敗），
  或一方的錯誤日誌指名另一方。
- **只是平常有呼叫路徑不併**：大家都連的元件不會把不相干的告警拉成一張。中間的系統沒有告警不會串過去。什麼都沒看到的各開一張。
- 這條規則不准放寬。

**整合的單位是 `service`**：告警的 `service` 標籤、日誌的服務欄位、Trace 的服務名稱三邊要完全一樣（分大小寫）。一個 aiops-lite 對一個
Grafana、只跑 1 個副本。多個 cluster 或廠區共用一個 Grafana 時，兩種做法：把 cluster 寫進 `service` 的值（例如 `prod-a/mes-api`），
或每個 cluster 各部署一份、各自用自己的告警路由。兩個 cluster 有同名的 `service` 會被當成同一個系統。

## 5. 系統關係

**沒有手寫的 edges。** 關係只從看到的資料來，存在 `observed_calls` 表並標來源，30 天沒再看到就丟掉（`traces.graph_retention_days`）：

| 來源 | 內容 |
| --- | --- |
| service graph | Tempo：誰呼叫誰、請求數、失敗數、延遲（沒埋點的資料庫、訊息佇列的消費者也在）。VictoriaTraces：只有誰呼叫誰 |
| 呼叫鏈 | 撈到的 Trace 裡的父子關係；訊息佇列的連結接法（消費端另開一條 Trace）也接得起來，一條最多追 3 條 |
| 錯誤日誌 | 某個系統的錯誤訊息原樣提到另一個已知系統的名稱（比前兩種弱，方向不一定是呼叫） |

- **方向**：一般呼叫是「被呼叫的出問題會影響呼叫它的」；經訊息佇列的反過來（發送端出問題會影響消費端）。
- **給 LLM 的範圍**：兩跳（直接連到告警系統的，加上那些鄰居自己連到的），最多 20 條。排序只是算術：一跳的在前；同一跳照延遲變成基線的
  幾倍由大到小，沒有倍數的照最近看到的。
- **延遲邊列表**（只有 Tempo 的 service graph 有）：分析時每條呼叫帶上事故期間與**事故前 30 分鐘**（基線，`logs.baseline_minutes`）的數字，
  **只給數字、不設門檻、不貼「暴增」「重試風暴」的標籤**：

  | 欄位 | 算法 |
  | --- | --- |
  | `rps`、`rps_baseline` | 請求數的增量 ÷ 秒數（事故時間窗的平均，事故只占一小段時會被攤薄） |
  | `error_rate_pct`、`error_rate_baseline_pct` | 失敗數 ÷ 請求數 × 100 |
  | `p95_ms`、`p95_baseline_ms` | `histogram_quantile(0.95, …_seconds_bucket)`，在直方圖的桶裡內插 |
  | `avg_ms`、`avg_baseline_ms` | 耗時總和 ÷ 次數（不受桶寬影響） |
  | `p95_ratio`、`avg_ratio` | 事故期間 ÷ 基線（用原始值算；基線是 0 或沒有就不算） |
  | `latency_from` | `server`：被呼叫的那一段自己量的；server 端沒資料時改用 `client`（呼叫端量的，含網路） |

  沒有直方圖、延遲查詢失敗、VictoriaTraces：只剩請求數與失敗數，不擋分析。每張事故分析時多 12 次指標查詢。
- **關係不算直接證據**：只因為在關係邊上被點名的系統，依據等級是「推測」（見第 7 節）。

## 6. 證據（送給 LLM 的各包）

事故時間窗是告警開始前 `logs.window_padding_minutes`（10）分鐘到現在；「事故前」是再往前 `logs.baseline_minutes`（30）分鐘。
日誌的次數比較與 service graph 的基線用同一個定義（`src/promql.py` 的 `Window`）。任何一包撈不到都不擋分析，原因寫進補充說明。

| 包 | 內容 | 怎麼來 |
| --- | --- | --- |
| 0 | 證據概況：已有什麼、缺什麼與原因、沒接什麼 | 程式統計；註記最後一行是它的精簡版 |
| 1 | 收斂的告警（依開始時間，帶觸發當下的數值） | Webhook |
| 2 | 系統關係（含延遲邊列表） | 第 5 節 |
| 3 | 錯誤日誌樣本：同類去重，留最早的一筆**原文**，附同類筆數，以及訊息裡會變的數字在同類樣本中的範圍（`numbers_in_sample`：最早那一筆的數字只是剛好的值）；另外每個系統各取最早的 3 筆，大量報錯的系統不會把別人擠掉 | 時間區間內**錯誤與警告一次查詢**，最多 `logs.max_hits`（100）筆，再照等級分開 |
| 3p | **時間窗之前最後一筆錯誤**：時間窗內沒有錯誤日誌的系統，往回（最多 24 小時）找它最後一筆錯誤，給原文與「比時間窗的起點早幾分鐘」；這段時間也沒有就寫沒有 | 每個這樣的系統一次查詢（最多 5 個）。「時間窗內查無資料」不等於沒送日誌、也不等於無關；程式不擴大時間窗、不判斷相不相關，也不拉高依據等級 |
| 3d | 主要的錯誤種類在事故期間與事故前各幾筆（`new=true`＝事故前一筆都沒有） | 一次計數查詢。只給數字，不貼「暴增」「雜訊」 |
| 3e | 警告：同樣歸類與計數 | 跟錯誤同一次撈回來的 |
| 4 | 機台、批號、載具的出現次數 | 錯誤日誌的 `logs.id_fields` |
| 5 | 批號／載具的跨系統時間線（不限等級、不限系統；只代表先後，不代表因果） | 出現最多次的關聯碼回頭撈 |
| 6 | 呼叫鏈：**只取失敗的 Trace**，標出最底層的失敗點；經訊息佇列的那一段標 `via=messaging` | 先用錯誤日誌裡的 trace id 直接取，不夠才搜尋 |
| 6b | 同一個請求的日誌，**不限等級**（INFO 只在這裡撈：量小、一定跟出事的請求有關） | 用呼叫鏈的 trace id 回頭撈；日誌沒記 trace id 就沒有這一包，註記不提 |
| 7 | 指標走勢：事故前的基準（中位數）、事故期間的最小／最大／最後、倍數、抽樣的點 | `metrics.queries` 設定的查詢 |

不挑「最慢的 Trace」充數：沒有對照組的單一樣本會把偶發的慢（GC、網路抖動）當成線索，延遲看第 2 包的統計。
識別碼的樣板（數字換成 `*`）只拿來歸類，不送給模型，模型看到的永遠是原文。

## 7. LLM 分析與把關

**模型回答的格式**：一句結論（為什麼會發生）、一條**因果鏈**（`chain`：由原因到現象，1～4 步，每一步是系統、`evidence` 或 `inference`、
一到兩句發生了什麼與依據）、最多 2 個別的解釋（`alternatives`）、1～3 條下一步建議、點名的系統與識別碼。**模型不給分數**。

為什麼是鏈不是排行榜（2026-11-28）：憑證與握手、名稱解析與被解析的對象、逾時與熔斷，這類問題是兩端共同構成的，
硬要模型選一個排第一，量到的是格式不是分析。兩端都寫進鏈裡，分不出誰對的另一種解釋放「另一種可能」。

| 規則 | 做法 |
| --- | --- |
| 依據等級由程式判定 | 鏈的每一步與每個別的解釋後面標「直接證據／間接證據／推測」，是程式照證據算的（`evidence.evidence_grade`），同一份證據永遠得到同一個等級；講的是指向這個系統的證據有多直接，不是機率、也不保證推理對。模型自評的分數已經拿掉（2026-11-28）：同一份證據它會給 75、38、35 |
| 直接證據 | 那個系統自己有事故前沒出現過的錯誤（`new=true`）；或呼叫鏈的失敗點停在它；或別的系統的錯誤日誌原樣指名它 |
| 間接證據 | 名稱出現在日誌、警告、時間線、呼叫鏈、請求的日誌、指標裡，但不符合上面任何一條：只有警告、平常就有或數不出次數的錯誤、指標變化 |
| 推測 | 只出現在告警與關係邊裡；或模型自己標成 `inference` 的（只會往下調，不會往上） |
| 順序 | 鏈照模型說的因果順序，程式不重排；別的解釋照等級排（直接證據在前） |
| 點名要有憑據 | 有告警、在關係邊或呼叫鏈裡，或名稱原樣（不分大小寫、至少 3 個字）出現在送給模型的證據文字裡。找不到的：鏈的第一步或結論提到它 → 整份不採用；排在後面的 → 只拿掉那一個 |
| 機台、批號 | 證據裡找不到 → 整份不採用；下一步建議裡出現找不到的識別碼 → 只拿掉那一條 |
| 下一步建議 | 只是給值班人員看的文字：程式不照著做任何事，也不准拿它觸發或選擇修復流程 |
| LLM 不通、格式不對、整份不採用 | 第 2 行寫 `[未經分析]` 加原因與什麼時候再試，底下直接列程式收集到的證據原文與筆數（最多 5 行），不加排名或判斷字眼。指數退避（60 秒起、每次加倍、最多 1800 秒，成功歸零），狀態存資料庫，重啟不重置 |

**註記怎麼讀**：

```
【P2・進行中】受影響系統：eap、service-b｜收斂告警 2 筆
根因分析｜[LLM 推論] service-b 處理 eap.events 時因 lot_id 缺失觸發 KeyError，handler 崩潰…
經過｜eap：送出的 eap.events 沒有 lot_id…（推測）
↳ service-b：handler 取不到 lot_id，KeyError 崩潰…（直接證據）
另一種可能｜service-b：欄位預期有誤…（推測）
下一步建議｜1) 檢查 eap.events 的訊息內容…；2) 核對 service-b 處理 eap.events 的欄位規格…
證據｜已有告警 2 筆、…；缺…（原因）；沒接…
自動修復｜restart-mq-consumer（mq-broker：佔位，沒有送出）      ← 規則有貼修復標籤才有
2026-11-27 19:51 ～ 進行中（台灣時間）｜調整優先權：<規則連結>
```

## 8. 出口

- **事故 API**（`outputs.incident_api.url`／`token`）：內容是事故、成員告警、根因分析（摘要、因果鏈 `chain`、別的解釋 `alternatives`、`suggestions`）、證據那一行、註記的 id 與標籤。
  同樣的內容只送一次；失敗每一輪再試、最多 10 次，不影響註記。只支援 `Authorization: Bearer`。
- **修復請求**（`outputs.remediation.url`／`token`／`enabled`）：跑哪個流程**只看規則上的 `aiops_remediation` 標籤**（人事先寫好的），
  LLM 的推論不觸發、也不選流程；只對還在響的告警、還沒結案的事故送；同一張事故、同一條告警、同一個流程只送一次；預設 `enabled: false`。
  只知道請求有沒有送出，不知道流程成功與否；結案時不會去取消流程。
- **佔位**：沒設網址（或修復沒有 enabled）就不送，只把本來會送的內容記進 `deliveries` 表（`status = placeholder`），註記寫「佔位，沒有送出」。
  佔位記過的之後不補送。看佔位記了什麼：`SELECT channel, status, payload FROM deliveries ORDER BY id DESC LIMIT 5;`

## 9. 各系統要送什麼

### 告警（必要）

每條要納管的 Grafana 規則貼三個標籤：`aiops_detect=true`、`aiops_severity=P0～P3`、`service=<系統名稱>`。要讓它能觸發修復才多貼
`aiops_remediation=<流程名稱>`（只給評估過的規則貼）。每條規則明確設定 No data 的行為；每個資料來源另外加一條「資料還在進來」的規則
（收集器掛了計數是 0，看起來像正常，aiops-lite 不會知道）。

**趨勢型規則**（還沒超標但走勢不對）只要改 Grafana 規則。以 RabbitMQ 佇列為例（2026-11-25 實測值）：

| 想法 | 查詢 | 正常期 | 堆積期 |
| --- | --- | --- | --- |
| 固定門檻 | `sum by (queue) (rabbitmq_queue_messages_ready{service="mq-broker"})` | 0 | 1007 |
| 變化率 | `sum by (queue) (deriv(rabbitmq_queue_messages_ready{service="mq-broker"}[1m]))` | 0 | 10 |
| 10 分鐘後會到多少 | `sum by (queue) (predict_linear(rabbitmq_queue_messages_ready{service="mq-broker"}[1m], 600))` | 0 | 7025 |
| 有訊息但沒有消費者 | `X > 0 unless sum by (queue) (rabbitmq_queue_consumers{service="mq-broker"}) > 0` | 沒有結果 | 1007 |

### 日誌（建議）：統一欄位規範

aiops-lite 的欄位對照只有一組，各系統要用同一套名稱。範例 Fluent Bit 設定在 `deploy/fluent-bit/`（parser 的正規表示式要照真實日誌改）。

| 欄位 | 必要性 | 內容 |
| --- | --- | --- |
| `@timestamp` | **必要** | 事件時間（RFC3339）。VictoriaLogs 寫入時用 `_time_field=@timestamp` 指定 |
| `service.name` | **必要** | 系統名稱，跟告警的 `service` 標籤**完全一樣**；建議當 VictoriaLogs 的 stream 欄位 |
| `log.level` | **必要** | 一律大寫：`ERROR`、`WARN`、`INFO`（另可用 `FATAL`、`CRITICAL`） |
| `message` | **必要** | 原始訊息。VictoriaLogs 寫入時用 `_msg_field=message`；Elasticsearch 要是 text 型別 |
| `eqp_id`、`lot_id`、`carrier_id` | 有就填 | 機台、批號、載具；寫法要固定（`EQP-CVD-07` 與 `eqp-cvd-07` 會被當成兩台） |
| `trace_id` | IT 服務有就填 | 小寫十六進位，跟 Trace 資料庫的編號一樣；兩個方向都用（日誌 → 呼叫鏈、呼叫鏈 → 同一個請求的日誌） |
| `error_type`、`queue`、`queue_depth`、`transfer_seconds`… | 選用 | 給 Grafana 規則算數用 |

- 名稱不同可以用 `logs.fields`、`logs.id_fields`、`logs.error_levels` 對照；三個系統要用同一套。
- 拆不出等級欄位時的過渡做法：`logs.fields.level: ""`，改在訊息文字裡找 error、fatal 這些完整的詞（會漏撈也會誤撈，不撈警告）。
- 不要拿批號、trace id 這種值很多的欄位當 VictoriaLogs 的 stream 欄位。只看一部分日誌：VictoriaLogs 用 `logs.extra_filter`（LogsQL 條件），Elasticsearch 用 `logs.index`。
- 系統沒有 Metrics（老舊的 CIM、AMHS、MQ）時：CIM 用日誌轉計數、AMHS 開唯讀 View 給 Grafana 的 SQL 資料源、MQ 先找現成的 exporter。
  走 SQL 或 exporter 觸發告警的系統，錯誤紀錄還是要照規範送進日誌資料庫，不然 RCA 沒有它的明細。

### Trace 與 service graph（選用）

- 對外的呼叫帶 `peer.service`、`db.system`：沒埋點的資料庫也會出現在呼叫鏈與 service graph 上。
- 訊息佇列有傳遞 Trace（父子接法或連結接法），消費端的 span 種類標成 consumer。
- Tempo 開 metrics-generator 的 service-graphs，把存指標的資料源 uid 填進 `traces.service_graph_datasource_uid`；
  延遲直方圖的桶最好比預設細（預設從 0.1 秒起跳，快的呼叫算不出準的 p95）。VictoriaTraces 啟動時加 `-servicegraph.enableTask=true`。

### 指標（選用）

`config.yaml` 的 `metrics.queries` 加查詢（`$service` 會換成系統名稱），最多 20 條；只做算術，沒有門檻。

## 10. 設定

**只有一份 `config.yaml`**，預設 `/etc/aiops/config.yaml`，唯一的環境變數是 `AIOPS_CONFIG_FILE`。鍵名打錯（任何一層）、型別不對、必填沒填、
設定跟來源對不上（例如 victorialogs 卻寫了 `logs.index`），啟動時直接報錯。完整清單與每一項的說明、範圍在 **`config.example.yaml`**
（用 `#` 註解掉的是進階調整）。檔案裡有 Token：`config.yaml` 已在 `.gitignore`。

| 鍵 | 預設值 | 用途 |
| --- | --- | --- |
| `grafana.url`、`grafana.token` | 必填 | 公司 Grafana 與 Editor 服務帳號的 Token |
| `webhook.token`、`webhook.port` | 必填、8080 | Contact Point 帶的 Bearer 密鑰；監聽的 port |
| `database.url` | `sqlite:///aiops-lite.db` | 正式環境用 `postgresql+psycopg://…` |
| `llm.url`、`llm.api_key`、`llm.model` | 空 | OpenAI 相容端點；url 與 model 沒設 = 不分析 |
| `logs.type`、`logs.datasource_uid` | `victorialogs`、空 | 空 uid = 沒有日誌證據 |
| `logs.fields`、`logs.id_fields`、`logs.error_levels`、`logs.warn_levels` | `service.name`、`log.level`…；`[eqp_id, lot_id, carrier_id]` | 欄位與等級的對照 |
| `logs.max_hits`、`logs.window_padding_minutes`、`logs.baseline_minutes` | 100、10、30 | 一次撈幾筆；事故時間窗往前多看幾分鐘；事故前看多久（service graph 的基線也用這個） |
| `traces.type`、`traces.datasource_uid` | `victoriatraces`、空 | 空 uid = 沒有呼叫鏈 |
| `traces.service_graph_datasource_uid` | 空 | 只有 Tempo：存 service graph 指標的資料源 uid |
| `traces.trace_id_field`、`traces.max_traces`、`traces.graph_retention_days` | `trace_id`、3、30 | 日誌裡 trace id 的欄位；最多幾條錯誤 Trace；關係保留幾天（0 = 不學也不併） |
| `metrics.datasource_uid`、`metrics.queries` | 空 | 指標走勢 |
| `outputs.incident_api.*`、`outputs.remediation.*` | 空、`enabled: false` | 兩個出口；沒設 = 佔位 |
| `debounce_seconds`、`followup_seconds`、`close_after_seconds` | 60、60、300 | 等多久開始分析、多久續跑、安靜多久結案 |
| `timezone`、`log_level` | `Asia/Taipei`、`INFO` | `DEBUG` 會記下送給 LLM 的內容與原始回應 |

刻意不開放設定的內部常數（都是版面或查詢量的上限，沒有判斷門檻，多數**未驗證泛化**）：因果鏈最多 4 步、別的解釋最多 2 個、依據等級的判定規則（三級，見第 7 節）、最後一筆錯誤往回找 24 小時且最多 5 個系統、一種錯誤最多列 5 個會變的數字、
名稱比對至少 3 個字、關係最多 20 條、合併時最多取回 5 條失敗的 Trace、連結最多追 3 條、下一步建議最多 3 條各 120 字、
LLM 不通時最多列 5 行證據、延遲取第 95 百分位、查詢區間最長一天。各自在程式裡的位置與理由寫在旁邊的註解。

## 11. 部署與上線

### 公司 Grafana 要做的（都在介面或 API 上做，不改映像檔）

| # | 項目 | 做法 |
| --- | --- | --- |
| 1 | 服務帳號 | Administration → Service accounts：角色 **Editor**，產生 Token |
| 2 | 外掛 | **只有日誌放 VictoriaLogs 時**：裝 `victoriametrics-logs-datasource`（實測 0.34.0） |
| 3 | 資料源 | 日誌（VictoriaLogs 或 Elasticsearch）、Trace（Jaeger 指到 VictoriaTraces 的 `/select/jaeger`，或 Tempo）、指標（Prometheus 型別指到 VictoriaMetrics）；uid 從網址 `/connections/datasources/edit/<uid>` 取 |
| 4 | Contact Point | Webhook；URL `http://<release>.<namespace>.svc:8080/webhook`；Authorization `Bearer` + `webhook.token`；**不要**勾 Disable resolved message |
| 5 | 通知路由 | Matcher `aiops_detect = true` → 上面的 Contact Point；`group_wait` 設短（0～10 秒）；要保留原本的通知就勾 Continue matching |
| 6 | 儀表板 | 匯入 `dashboards/aiops-overview.json`（事故清單）與 `dashboards/aiops-incident-metrics.json`（指標曲線） |
| 7 | 通知失敗告警 | Webhook 送不到時 aiops-lite 自己不會知道，要在 Grafana 那一側設 |

規則、Contact Point、路由若是用 provisioning（YAML、Terraform）管的，要加進原本那份檔案，在網頁上改會被下次部署蓋掉。
用同一把 Token 確認查得到日誌（VictoriaLogs 的寫法）：

```bash
curl -s -H "Authorization: Bearer $TOKEN" "$GRAFANA/api/datasources/proxy/uid/$UID/select/logsql/query" \
  -d 'query="service.name":="cim-eap" | sort by (_time) desc | limit 3' \
  -d "start=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)" -d "end=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

### Helm

整份 `config.yaml` 由 chart 用 values 產生，放進 Secret、唯讀掛載；Secret 變了自動重啟。`values.yaml` 只放空白範本；真的 Token 寫在
**不進 Git** 的 `secrets.values.yaml`：

```yaml
grafana:  {url: https://grafana.corp.example, token: glsa_xxx}
webhook:  {token: xxx}
postgres: {password: xxx}        # 只用英數字（會放進連線字串）
llm:      {url: https://llm.corp.example/v1, apiKey: xxx, model: xxx}
logs:     {type: victorialogs, datasourceUid: xxx}      # Elasticsearch：type: elasticsearch，加 index
traces:   {type: tempo, datasourceUid: xxx, serviceGraphDatasourceUid: xxx}
metrics:  {datasourceUid: xxx}
```

```
helm upgrade --install aiops-lite deploy/helm/aiops-lite -f secrets.values.yaml
```

必填沒填 `helm template` 直接報錯；鍵名打錯 Helm 不會報錯，要等程式啟動才失敗（Pod 會重啟，log 寫是哪個鍵）。進階設定寫在 `extraConfig`（結構跟 config.yaml 一樣）。
**本機**：`cp config.example.yaml config.yaml`，填好後 `AIOPS_CONFIG_FILE=config.yaml python -m src.server`；`python -m src.main` 立刻跑一輪（除錯用）。

### 健康檢查

`GET /healthz`（不用 Token）回 503 的情況：工作執行緒死了、同一輪跑超過 10 分鐘。一輪失敗（例如資料庫斷線）不算不健康，30 秒後自己重試。
探針只管 Pod 裡面：Pod 活著但 Webhook 送不到，要靠 Grafana 那一側發現。

### 上線後的第一次驗證

- [ ] `GET /healthz` 回 `{"status": "ok"}`
- [ ] 讓一條貼了標籤的規則觸發 → 約 1 分鐘後「AIOps 事件總覽」出現一張事故
- [ ] 註記第 2 行是 `[LLM 推論]`，點名的機台、批號是真的
- [ ] 註記的「證據｜」那一行沒有「缺錯誤日誌」或「查詢失敗」；有的話括號裡寫了是哪一關沒過
- [ ] 規則恢復 → 等 `close_after_seconds` 之後事故移到「已結案」

### 要先跟其他團隊講好的事

統一的日誌欄位規範；`service` 的值三處一致；多 cluster 的做法；每個系統收哪些值、門檻多少（各系統決定，平台不管）；
日誌裡的機台、批號能不能送給 LLM（接了事故 API 也會送給下游）；哪些告警可以自動修復、由誰核准；試行範圍與成功標準。

## 12. 評估

`eval/` 是開發工具（不進映像檔）：把人造案例的日誌真的寫進 VictoriaLogs 或 Elasticsearch，用 aiops-lite 的程式經 Grafana 收證據，
問真的 LLM 幾次再計分。一次算對要同時符合：通過幻覺檢查、答案的系統**出現在因果鏈的某一步**（大小寫與 `:port` 視為相同；只列在「另一種可能」不算）、有提到重點字；
答案是「還無法判定」的，鏈上不能有等級是「直接證據」的步驟。它是不是鏈的第一步另外統計，只當參考。
（2026-11-28 以前是「排第一的可能原因要是它」，數字不能直接比。）案例分**開發用 12 個**（s01～s12）與**留出 7 個**（h01～h07）。

```bash
export EVAL_VL_URL=http://127.0.0.1:9428 EVAL_GRAFANA_URL=http://127.0.0.1:3000
export EVAL_GRAFANA_TOKEN=<Editor Token> EVAL_GRAFANA_ADMIN=admin:<密碼> LLM_API_KEY=<金鑰> LLM_CHAT_MODEL=<模型>
python -m eval.run --set tuning --runs 5 && python -m eval.run --set heldout --runs 5
python -m eval.run --backend elasticsearch --set tuning --runs 5   # EVAL_VL_URL 換成 EVAL_ES_URL
python -m eval.run --only s02 --runs 1 --show-prompt               # 看實際送給模型的內容
```

**改提示詞或證據前處理時**：改之前、改之後各跑一次（同一時間先後跑），留出的變差就不合併；不准為了讓留出的過關去調程式；
新想法先加一個現在會錯的案例；前處理只能歸類與計數。

**最近幾輪**（VictoriaLogs，各 5 次）：

| 日期 | 改動 | 開發用 | 留出 |
| --- | --- | --- | --- |
| 2026-11-25 | 日誌同類去重、各系統最早的錯誤、事故前後次數、警告 | 18/30 → 48/50 | 3/18 → 27/30 |
| 2026-11-26 | 放寬幻覺檢查（日誌指名的元件可以當原因） | 49/50 → 50/50 | 27/30 → 29/30 |
| 2026-11-27 | 下一步建議、說明寫具體、訊息佇列方向 | 56/60 → 55/60 | 35/35 → 34/35（h01 另跑 10 次：9/10 對 8/10，分不出差別） |
| 2026-11-27 | 錯誤加警告一次撈、請求的日誌、拿掉最慢的 Trace、延遲邊列表 | 53/60 → 55/60 | 34/35 → 34/35 |
| 2026-11-28 | 提示詞不寫死延遲桶寬 | 52/60 → 54/60 | 33/35 → 35/35（同一份提示詞兩輪差 2 次，判斷是浮動） |
| 2026-11-28 | 依據等級改由程式判定、模型不給分數（「無法判定」的計分同時改成「不能有直接證據等級的」） | 56/60 → 55/60 | 32/35 → 34/35（改前那一輪 h01 是 3/5，同一份程式前幾輪是 4～5/5：浮動） |
| 2026-11-28 | 時間窗之前最後一筆錯誤、同類樣本裡數字的範圍 | 56/60 → 56/60 | 33/35 → 34/35。第一版 57→55、34→32（不能合）：查到時間戳被當成數字、找不到也佔一段、範圍重複放兩處，修掉後是這一列的數字。掉過分的 h07 另外各問 10 次：9/10 → 6/10；連同這一輪是 12/15 對 11/15，分不出差別，但不能說沒有變差 |
| 2026-11-28 | 輸出改成因果鏈＋別的解釋；計分改成「答案出現在鏈的某一步」 | 54/60 → 55/60（改前用新計分重算最寬是 55/60） | 34/35 → 35/35（改前用新計分重算是 34/35）。改後答案同時是鏈的第一步：開發用 40/45、留出 29/30 |

s11 的答案鍵 2026-11-28 改過（使用者決定）：原本只認 `MariaDb` 在鏈裡，因果鏈的版本是 0/5。只有 `instana-agent` 自己的日誌時分不出是感測器、網路還是資料庫的問題，
模型把 `instana-agent` 寫進鏈、`MariaDb` 放在「另一種可能」是誠實的寫法；現在兩個任何一個在鏈裡都算（說明裡仍要提到 MariaDb），重跑 5/5。上表的數字都是改答案鍵之前的。
以前常錯的 s12（句子裡沒寫 `vmselect`）、h01（憑證上的主機還是閘道）、h07（寫入端還是儲存端）這一輪都是 5/5：兩端都在鏈裡就算對。

**這些數字不能代表什麼**：案例是人造的，比真實日誌乾淨，不是產線準確率；案例少，差一兩次分不出是不是巧合；留出的也是同一個人寫的；
只測了一個模型；計分只看排第一的名稱與關鍵字，依據等級的規則合不合理、建議有沒有用沒有量；**案例裡的關係是寫死的、沒有 Trace，量不到延遲邊列表與請求的日誌**。

## 13. 驗證狀態

**實測過的**（本機拋棄式元件、腳本產生的假資料，Grafana 13.0.7、Editor Token）：

| 項目 | 元件 | 結果 |
| --- | --- | --- |
| 兩種日誌來源給一樣的證據 | VictoriaLogs v1.17.0、Elasticsearch 8.19.23 | 同一批資料整包證據一樣（時間的寫法不同：VictoriaLogs 去掉小數秒尾端的 0） |
| 兩種 Trace 來源 | VictoriaTraces v0.5.0、Tempo 2.9.0 | 呼叫鏈、訊息佇列的連結接法都接得起來 |
| 合併規則 | Tempo service graph、VictoriaTraces 相依關係 | 有失敗呼叫的兩個告警系統併成一張，只有呼叫路徑的各開各的 |
| 延遲邊列表 | Tempo 2.9.0 → VictoriaMetrics v1.110.0 | rps、失敗率、p95、平均、倍數與基線都對；N+1 的請求量放大看得出來（資料庫的 rps 是上游的 5 倍） |
| 只有警告的慢查詢事故 + 真的 LLM | 兩組堆疊 | 都指出 orders-db 沒用到索引 |
| 隔兩跳的資料庫變慢 + 真的 LLM | Tempo + VictoriaLogs | 有呼叫鏈：排第一 mes-db（85）。只有延遲邊列表：模型排第一也是 mes-db，但它只在關係邊上，等級是推測（當時還是分數：被壓到 30，第二名 mes-api 60） |
| 錯誤在告警前就停了、去重留下偶然的數字 + 真的 LLM | VictoriaLogs、Elasticsearch | 兩種來源都找到「TC-4 讀取逾時，比時間窗早 22 分鐘」，LLM 把它列為推測並建議先查那支感測器（之前註記寫的是「一筆日誌都沒有，檢查日誌有沒有送進來」）；逾時錯誤附上 active sessions 的範圍 0～24，LLM 不再照著「0」下建議 |
| 因果鏈的完整流程 + 真的 LLM | Grafana 13.0.7、Tempo、VictoriaLogs | Webhook 進來到註記寫回：eap 經訊息佇列發事件、service-b 處理失敗，註記是「經過｜service-b：…（直接證據）」「另一種可能｜eap：…（推測）」。另外各問 3 次：熔斷、金鑰被拒兩題每次都把兩端寫進鏈裡；互相指名那題有一次因為模型把兩個系統寫成一個名稱而被退回；在提示詞加了「一步只寫一個系統」之後單獨再問 10 次，沒有一次被退回 |
| LLM 不通的版面、出口的佔位與重送 | 假的 HTTP 端點 | 照設計 |
| 自動測試 | SQLite、PostgreSQL 16 | 410 個；Grafana、各資料庫的回應都是實際錄下來的（`tests/fixtures/grafana13/`） |

**沒測過的**：現場的資料與日誌；映像檔與實際部署到叢集；公司 Grafana 的權限與版本；叢集版或有認證的 Victoria、Elasticsearch、Tempo；
內部 LLM；真的告警中心與 Conductor（兩個出口的格式都是自訂的）；查詢在大量資料上的耗時；現場 Tempo 有沒有開 service graph、日誌有沒有帶 trace id。

## 14. 已知限制

- **送給 LLM 的日誌沒有遮罩**：敏感資訊要在產生端去識別化，或用公司內部的模型。
- 沒有 Trace、日誌也沒提到別人的系統，不會跟任何人併；剛上線、又沒有 service graph 時關係圖是空的；從日誌學只認得已經看過的系統名稱。
- **VictoriaMetrics 會丟掉 exemplar**（v1.110.0 實測）：沒辦法從告警指標直接拿慢請求的 trace id。
- **Tempo 2.9.0 對沒埋點的資料庫，server 端填的是呼叫端量的耗時**（兩端一模一樣），「server 端沒資料改用 client」在這個版本沒有觸發過，只有測試驗過。
- **依據等級評的是證據，不是推理**：兩個系統的錯誤日誌互相指名時兩邊都是直接證據，誰先誰後程式分不出來；「舊的錯誤變頻繁」不是新錯誤，只算間接證據。判定規則是這個專案自己定的，只在人造案例上跑過。
- 「時間窗之前最後一筆錯誤」只往回找 24 小時、只取一筆，而且只找時間窗內完全沒有錯誤的系統；數字的範圍只看獨立的數字（機台、批號、位址裡的不算）、只來自撈回來的樣本，每筆的數字個數不一樣就不給。
- p95 是桶內插的：預設的桶從 0.1 秒起跳，基線 20 毫秒的呼叫會算成 95 毫秒左右；快的呼叫看平均。
- VictoriaTraces 的 service graph 沒有延遲與失敗數，只能當線索、不能拿來合併；它回報的呼叫次數會重複計算，所以不用。
- 錯誤與警告共用 `logs.max_hits`：警告很多時最新的那一批可能大半是警告，較早的錯誤靠「各系統最早的幾筆」補。
- 時間線靠各系統的時間戳排序，沒有對時的話先後可能是錯的；指標走勢的基準是事故前的中位數，事故前就在爬升的序列倍數會偏小。
- 只認同一個 Grafana 組織、一個副本；Tempo 收到 Trace 後要 8～40 秒才搜得到（用編號取則立刻取得到）。

## 15. 檔案

| 檔案 | 行數 | 職責 |
| --- | --- | --- |
| `src/server.py` | 218 | 收 Webhook、debounce、單一工作執行緒、`/healthz` |
| `src/main.py` | 307 | 一輪的流程 |
| `src/config.py` | 394 | 讀 `config.yaml`（嚴格檢查） |
| `src/grafana.py` | 195 | 解析 Webhook、對 Grafana 的 HTTP、指標查詢、註記 |
| `src/logs.py` | 272 | 兩種日誌來源，同一組方法、同一種形狀 |
| `src/traces.py` | 151 | 兩種 Trace 來源，統一成 Jaeger 的形狀 |
| `src/graph.py` | 199 | service graph（含延遲邊列表）、日誌指名、子圖裁切 `prune`；只提供事實 |
| `src/promql.py` | 71 | 時間窗 `Window`、PromQL 組裝、捨入、倍數（純函式） |
| `src/grouping.py` | 181 | 收斂、結案、哪些連結可以合併（`failing_links`）（純函式） |
| `src/evidence.py` | 779 | 收集證據、證據概況、依據等級的判定 |
| `src/rca.py` | 278 | 組提示詞、呼叫 LLM、檢查回答、下一步建議（不排版） |
| `src/report.py` | 129 | 註記上每一行的寫法與整份註記的組裝 |
| `src/outputs.py` | 157 | 事故 API、修復請求（佔位、不重送、重試） |
| `src/db.py` | 286 | 建表與自動補欄位 |

另外：`config.example.yaml`（設定的完整說明）、`deploy/helm/aiops-lite`、`deploy/fluent-bit/`（三個系統的範例）、`dashboards/`、
`eval/`（評估）、`tests/`（`tests/fixtures/grafana13/README.md` 寫每份錄下來的樣本怎麼錄的）、`demo/README.md`（Grafana 實際畫面）。

測試：`python -m pytest tests -q`；`TEST_DATABASE_URL=postgresql+psycopg://… python -m pytest tests -q` 在 PostgreSQL 上跑同一批。

## 16. 下一步

**等決定的**：
1. 有延遲數字的邊要不要算間接或直接證據（現在只靠關係邊被點名的系統是「推測」；放寬要改 `CLAUDE.md` 的界線）。
2. 沒有 trace id 時用 service graph 補撈鄰居系統的日誌：2026-11-28 做過、實測有效（只有 API 告警、原因在沒告警的資料庫日誌裡時，LLM 說得出具體原因），使用者決定先不要，已移除；程式在 Git 歷史的 fc69d4d。評估案例裡沒有「鄰居有日誌」的情況，量不到它。

**Phase 2：歷史案例參考（只有設計，還沒有實作）**：工程師在註記上用標籤確認「準確」的已結案事故，分析新事故時檢索相似的舊案給 LLM 參考，
不重新訓練模型。兩個防呆：只收明確的標籤回饋、不做隱式回饋；舊案不能進幻覺檢查的範圍（舊案裡的系統名稱不算這次的憑據）。
等試行累積真實事故之後再做：現在沒有真實案例，做了也無法驗證有沒有幫助。
