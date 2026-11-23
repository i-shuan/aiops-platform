# aiops-lite

事件驅動的極簡 AIOps：Grafana 告警經 Webhook 推進來 → 等一段 debounce → 跨系統收斂 → 收集證據交給 LLM → 寫回 Grafana 註記。
只拿一把 Grafana Editor Token，不直連任何外部資料庫／ES，沒有前端。同一個 repo 裡原本的 `aiops/`（aiops-platform）已封存不再更動，
lite 不 import 它的任何程式碼。

```
MQ / CIM / AMHS ──▶ 公司 Grafana（資料源 + 貼標籤的告警規則）
                          │ 告警 firing／resolved
                          ▼ Webhook（Contact Point）
┌──────────────────── aiops-lite（常駐服務，1 個副本）────────────────────┐
│ 收 Webhook → 先存資料庫 → 等 debounce（預設 60 秒，YAML 可調）            │
│   → 1 收斂（同系統、關係邊）→ 2 收集 4 包證據 → 3 LLM → 4 寫註記          │
│ 有進行中事故時每 followup 秒續跑一輪，直到全部 resolved 並安靜夠久才結案   │
└──────────────────────────────┬──────────────────────────────────────────┘
                               ▼ config.yaml 的 grafana.url + grafana.token
            Grafana Annotations（三行摘要）+「AIOps 事件總覽」儀表板
```

部署物：1 個常駐 Deployment（+ Service）、1 個帶 PVC 的 PostgreSQL、1 個放整份 config.yaml 的 Secret（`deploy/helm/aiops-lite`）。

**什麼算異常由各系統自己的 Grafana 告警規則決定**：規則 firing 就是異常，平台不重放查詢、不自己判數值（原本的 5 個偵測器已拿掉）。

## 工廠與傳統可觀測性一起用

三種訊號各走各的路，最後合成同一份證據交給 LLM：

| 訊號 | 工廠（老舊系統、沒有埋點） | IT 服務（有埋點） | aiops-lite 怎麼用 |
| --- | --- | --- | --- |
| 指標 | 日誌轉指標（Grafana 規則對 ES 算次數） | 原生 metrics（Prometheus、VictoriaMetrics） | 兩者都是 Grafana 告警，Webhook 進來的格式一樣 |
| 關聯 | **批號／載具**把各系統的日誌串成時間線 | **trace id** 向 Tempo 取回呼叫鏈 | 證據第 5 包（時間線）、第 6 包（呼叫鏈） |
| 日誌 | ES | ES | 證據第 3、4 包（錯誤日誌樣本、識別碼出現次數） |

- **批號時間線**：從錯誤日誌裡挑出現最多次的批號與載具，回 ES 把它們在各系統的紀錄（不限等級、不限系統）照時間排出來。
  它只說明先後，不說明因果；靠各系統的時間戳排序，系統之間時鐘差幾秒，順序就可能是錯的。
- **Tempo 呼叫鏈**：經 Grafana 的 Tempo 資料源取回失敗請求的整條呼叫路徑，標出「底下沒有其他失敗呼叫」的那個失敗點。
  錯誤日誌帶 trace id（`tempo.trace_id_field`）時直接用編號取；沒有才用 TraceQL 搜尋。
- **同一張事故**：工廠與 IT 服務的告警只要關係邊連得起來（例如 `[mes-api, cim-eap]`）就併成一張。兩邊的日誌要用同一套欄位名稱。
- **呼叫鏈裡出現的系統算有憑據**：例如呼叫鏈指到一個沒有告警、也不在關係邊裡的資料庫，LLM 可以點名它；憑空編的系統照樣被擋。

設定：`es.timeline_fields`、`tempo.datasource_uid` 等，見 `CONFIG.md` 的「關聯證據」。Loki、Jaeger 沒有接。

## 整合的單位：以 `service` 為準，一個 Grafana 一份

**事故以 `service`（系統名稱）為單位收斂，不是以 cluster、namespace、pod 或機台。**
收斂只看三樣東西：告警上的 `service` 標籤、設定檔的 `edges`、目前有沒有進行中的事故。告警上其他的標籤收斂時都不看；
機台、批號、pod 這類識別碼只出現在日誌裡，用來統計與做時間線，不影響事故怎麼分。

| 情況 | 結果 |
| --- | --- |
| 兩個 cluster 都有叫 `mes-api` 的服務，各自告警 | **併成同一張事故**（`service` 的值一樣） |
| 同一個服務有幾十個 pod 各自告警 | 併成一張，pod 不會獨立成事故 |
| 同一個系統底下好幾台機台各自告警 | 併成一張，機台只出現在日誌統計裡 |

**部署單位**：一個 aiops-lite 對一個 Grafana，只用一把 Grafana Token、收該 Grafana 送來的 Webhook，而且只能跑 1 個副本。
不是一個 cluster 一份，也不是一個服務一份。有多個廠區或 cluster 各有自己的 Grafana 時，要各部署一份。

**多個 cluster 或廠區共用同一個 Grafana 時**，有三種做法：

1. **把 cluster 寫進 `service` 的值**（例如 `prod-a/mes-api`）：不用改程式；代價是 `edges` 也要照這個名稱寫，邊會變多，日誌的 `service.name` 也要一致。
2. **每個 cluster 各部署一份 aiops-lite**，各自用該 cluster 的告警路由（通知路由的條件要多一個 cluster 標籤）：隔離最乾淨，要多維護幾份。
3. **讓收斂把 cluster 標籤也算進去**：要改程式，**目前沒有做**。

要在上線前決定用哪一種，因為選了之後 `service` 的命名與 `edges` 都要照著寫。

**沒有驗證的**：多個 Grafana 組織（org）的情況。Token 是某個組織底下的服務帳號，推測它收不到其他組織的註記與資料源，
Webhook 內容裡雖然帶組織編號，但程式不看；這些都是依 Grafana 的一般行為推測，沒有在多組織環境實測過。
另外兩個 cluster 同名服務會併成一張，這點是依收斂邏輯與測試確認的，沒有用真實多 cluster 資料跑過。

### 改用別的單位分析（function、cluster）

分析單位其實是「告警上的某一個標籤」，程式讀進來之後一路叫它 `service`。綁定的位置只有這幾處：

| 位置 | 作用 |
| --- | --- |
| `config.yaml` 的 `labels.service` | 指定用哪個告警標籤當分析單位（預設 `service`） |
| `src/grafana.py` 的 `parse_webhook` | 從告警讀出這個標籤，放進 `Alert.service` |
| `src/grouping.py` 的 `group_alerts`、`converge` | 以這個值收斂（同值合併、有關係邊就併） |
| `src/grafana.py` 的 `search_errors`、`search_error_traces` | 用這個值去 ES、Tempo 撈證據 |

**只換收斂單位：改設定就可以，不用改程式。** 把 `labels.service` 改成 `function` 或 `cluster`，
告警貼上對應的標籤，`edges` 也用同一層的名稱寫。同一個 function 或 cluster 的告警就會併成一張事故。

**證據要跟著對得上，否則會撈不到：**

| 證據 | 換單位之後 | 怎麼處理 |
| --- | --- | --- |
| 錯誤日誌、識別碼統計（ES） | 程式拿分析單位的值去比對日誌的 `es.log_fields.service` 欄位（預設 `service.name`） | 日誌裡有 `function` 或 `cluster` 欄位的話，把 `es.log_fields.service` 改成那個欄位 |
| 批號／載具時間線（ES） | 用批號、載具編號查，不用分析單位篩選；但批號是從上一列的錯誤日誌裡挑的，而且每筆事件顯示的「系統」也讀 `es.log_fields.service` | 上一列對得上，這包就有；改了欄位後時間線顯示的會是 function 或 cluster 名稱 |
| ES 裡的 Trace 錯誤片段 | 同上，欄位是 `es.trace_fields.service` | 同上 |
| Tempo 呼叫鏈 | 查詢寫死 `resource.service.name`，設定改不了 | **目前做不到**：這包證據會是空的，其他證據與 RCA 照常 |

**限制：一次只能有一層。** 一張事故只記一個維度的名單，沒辦法「以 service 收斂、同時再往上看 function 或 cluster」。
要多層同時分析得改程式（告警多帶幾個維度、收斂鍵可選、事故記錄各層名單，會動到 `grouping.py`、`db.py`、`report.py`），**目前沒有做**。

**沒有驗證的**：以上是依程式的寫法整理的，改用 function 或 cluster 當單位的設定沒有實際跑過。

## 為什麼要等（debounce）

收到 Webhook 的第 0 秒就分析，LLM 拿到的是殘缺的證據：

- 上下游的告警有時間差：上游出事後下游可能幾十秒後才響，甚至門檻嚴的下游先響。太早分析會把受害者當成根因。
- ES 日誌有寫入延遲（收集器緩衝、建索引）：太早查，撈不到機台與批號。
- 一場事故會連發好幾個 Webhook：各跑各的會重複問 LLM、註記互相覆蓋。

所以收到**第一個** Webhook 後固定等 `debounce_seconds`，這段時間進來的告警全部收進同一批，時間到才一起收斂、撈證據、問一次 LLM。
視窗不會因為後面一直有告警進來而延後；跑的途中進來的告警排進下一個視窗，併入同一張事故並重新分析。

## 設定：全部在一份 config.yaml

沒有環境變數。程式只讀一份 `config.yaml`，預設路徑 `/etc/aiops/config.yaml`；要換位置只有一個環境變數 `AIOPS_CONFIG_FILE`。
完整範例與每一項的說明見 `config.example.yaml`：

```yaml
grafana: {url: https://grafana.corp.example, token: ""}   # 必填
webhook: {token: "", port: 8080}                          # token 必填
database: {url: postgresql+psycopg://aiops:密碼@主機:5432/aiops}   # 沒寫 = sqlite:///aiops-lite.db
llm: {url: "", api_key: "", model: "", retry_initial_seconds: 60, retry_max_seconds: 1800}   # 整段可省略 = 不分析；temperature 不寫 = 用模型預設值
es:                                                       # 整段可省略 = 沒有日誌證據
  datasource_uid: ""
  log_index: "*"
  trace_index: ""
  id_fields: [eqp_id, lot_id, carrier_id]
  log_fields:   {time: "@timestamp", service: service.name, level: log.level, message: message}
  trace_fields: {time: "@timestamp", service: service.name, span: span.name, trace_id: trace.id, status: status.code, error: error.message}
timezone: Asia/Taipei
log_level: INFO
debounce_seconds: 60      # 收到第一個 Webhook 後等多久才開始分析
followup_seconds: 60      # 有進行中事故時，沒有新 Webhook 也每隔多久再跑一輪
close_after_seconds: 300  # 成員告警全部 resolved 之後，再安靜多久才結案
edges:
  - [mq-broker, cim-eap]  # [上游, 下游]
```

- **進階調整**（逾時、重試、ES 筆數上限、錯誤等級的值、給 LLM 的樣本數、提示詞、幻覺檢查樣式、標籤名稱、健康檢查門檻…）
  也都在這份檔案，全部選填；`config.example.yaml` 裡用 `#` 註解掉的就是。完整清單、預設值、範圍，以及哪些東西刻意不開放設定，見 **`CONFIG.md`**。
- **啟動時嚴格檢查**：檔案不存在、鍵名打錯（任何一層）、型別不對、必填沒填，直接報錯、不啟動。數字開頭的密鑰要加引號。
- **日誌與 Trace 的欄位名稱可以改**（`es.log_fields`、`es.trace_fields`）：查詢條件、排序、讀回來的內容都照這裡的名稱；沒寫的用預設值。
- **檔案裡有 Token 與密碼，不要推上 Git**：`config.yaml` 已在 `.gitignore` 與 `.dockerignore`，專案裡只有 `config.example.yaml`。
  設定被印出來（log、例外訊息）時，Token、密鑰、連線字串不會出現。
- 實際看到註記的延遲 ≈ 規則的 `for` + Grafana 路由的 `group_wait` + `debounce_seconds` + LLM 回應時間。

## 檔案

| 檔案 | 行數 | 負責內容 |
| --- | --- | --- |
| `src/server.py` | 218 | 常駐服務：收 Webhook（Bearer 驗證、先落地再回 200）、debounce 排程、單一工作執行緒、續跑 |
| `src/main.py` | 447 | 讀 config.yaml（嚴格檢查）與「一輪」的流程 |
| `src/grafana.py` | 270 | 解析告警 Webhook、ES Proxy 查日誌與 Trace、寫註記 |
| `src/grouping.py` | 166 | 同實體合併、拓撲鏈收斂、併入進行中事故、嚴重度只升不降、全部 resolved 且安靜夠久才結案 |
| `src/rca.py` | 356 | 不寫推理演算法：收集證據（告警、關係邊、日誌、識別碼統計、批號時間線、Tempo 呼叫鏈）、組 Prompt、呼叫 LLM、幻覺檢查、失敗退回原始摘要 |
| `src/report.py` | 59 | 固定三行註記（台灣時間、HTML 跳脫、調整優先權連結） |
| `src/db.py` | 183 | SQLAlchemy Core（SQLite／PostgreSQL），啟動自動建表：待處理告警、事故、每輪執行紀錄 |

合計約 1,699 行。

## 各系統要做的事（平台零直連）

1. 把資料送進公司 Grafana 看得到的資料源：MQ 掛 Prometheus exporter；CIM 用日誌收集器做 Log 轉指標；AMHS 開唯讀 View 給
   Grafana 的 SQL 資料源。錯誤日誌進 ES，帶 `service.name`、`log.level`、`message`、`@timestamp`，以及有的話 `eqp_id`／`lot_id`／`carrier_id`。
2. 在 Grafana 建告警規則（閾值自己定）並貼標籤：`aiops_detect=true`、`aiops_severity=P0～P3`、`service=<系統名稱>`
   （要跟關係邊裡的名稱一樣）。這三個標籤名稱是預設值，可以在 config.yaml 的 `labels` 改。沒有 `service` 的告警會被略過並回報給送出端；`aiops_severity` 寫錯當成 P3。

系統沒有 Metrics（老舊的 CIM、AMHS、MQ）時怎麼把日誌或資料庫紀錄變成告警規則，見 **`FACTORY_METRICS.md`**。
Fluent Bit 的設定範例在 `deploy/fluent-bit/`。
Demo 的 Grafana 實際畫面（依「降噪、找源頭、鎖定機台批號、自動結案」四個重點排列，含說明）在 **`demo/README.md`**。

## Grafana 要做的設定

**都在公司 Grafana 的網頁介面（或它的 API）上做；不改 Grafana 映像檔、不裝外掛、不改規則的閾值。**
repo 裡的 `deploy/helm/aiops-grafana` 是舊平台自己的 Grafana，lite 不使用、也沒改它。

| # | 項目 | 在哪做 | 怎麼做 |
| --- | --- | --- | --- |
| 1 | 服務帳號與 Token | Administration → Users and access → Service accounts | 新增角色 **Editor** 的帳號 → Add token → 填進 config.yaml 的 `grafana.token` |
| 2 | 告警規則標籤 | Alerting → Alert rules → 編輯規則 → Labels | 每條要納管的規則加 `aiops_detect=true`、`aiops_severity=P0～P3`、`service=<系統名>` |
| 3 | **Contact Point**（新增 1 個） | Alerting → Contact points → Add | 類型 Webhook；URL `http://<release>.<namespace>.svc:8080/webhook`；Authorization scheme `Bearer`、credentials 填 config.yaml 的 `webhook.token`；**不要**勾 Disable resolved message |
| 4 | **Notification policy**（新增 1 條路由） | Alerting → Notification policies → New child policy | Matcher `aiops_detect = true` → 上面的 Contact Point；`group_wait` 設短（0～10 秒，平台自己會等 debounce）；要同時保留原本的通知就勾 Continue matching |
| 5 | 儀表板 | Dashboards → New → Import | 貼上或上傳 `dashboards/aiops-overview.json`（事故清單：進行中、P0、P1、已結案；只用內建註記清單面板，不需要資料源）。另有 `dashboards/aiops-incident-metrics.json`（四張指標曲線，事故區間自動疊成紅色；匯入時選 ES 資料源，查詢要照 `FACTORY_METRICS.md` 的欄位規範，面板的查詢可自行改成各系統的指標） |
| 6 | ES 資料源 uid（要日誌證據才需要） | Connections → Data sources → 點進該資料源 | 從網址 `/connections/datasources/edit/<uid>` 取得，填進 config.yaml 的 `es.datasource_uid`（Helm：`es.datasourceUid`） |

### 注意

1. **規則、Contact Point、路由若是用 provisioning（YAML、Terraform）管的**，在 UI 上改會被下次部署蓋掉，要加進原本那份檔案。
2. **Grafana 要連得到 aiops-lite 的 Service**（同叢集用 Service 名稱；不同叢集要自己開路）。
3. **RBAC 只在預設權限下驗過**：Editor 能走 ES 代理、寫註記，是在 Grafana 13.0.7 預設權限下確認的。
4. **Token 與 Webhook 密鑰只放 `secrets.values.yaml`（不進 Git）**，專案裡的 `values.yaml` 保持空白。

## 流程細節

- **收 Webhook**：驗 Bearer → 解析 → 存進資料庫的 inbox → 回 200。存不進去回 503，Grafana 會重送。等 debounce 的期間服務重啟，
  告警還在 inbox，啟動後自動排一輪。
- **收斂**：同 service、或兩個有告警的 service 之間有關係邊（不分方向）就併成一張；中間的系統沒有告警不會串過去。
  新告警碰到進行中事故就併進去。告警到達的順序不影響結果（下游先響也一樣）。
- **結案**：成員告警全部 resolved，再安靜 `close_after_seconds` 才結案；期間又響就同一張事故繼續。
  視窗內先響又恢復的告警照樣會留下事故。
- **RCA**：事故的成員或嚴重度有變時才問 LLM（重複通知不重問）；LLM 失敗的進行中事故下一輪再試。
  LLM 回 JSON（summary／systems／ids），程式檢查點名的系統在事故或關係邊裡、機台批號在識別碼統計裡、
  摘要文字裡夾數字的識別碼在證據裡找得到；不通過就用 `[未經分析] 原始摘要`。
- **LLM 失敗會指數退避**：分析失敗（連不上、逾時、回應格式不對、幻覺檢查沒過）後，等 `llm.retry_initial_seconds`（預設 60）才再試，
  之後每失敗一次加倍（60、120、240…），最多 `llm.retry_max_seconds`（預設 1800）；成功就歸零。退避期間**不查 ES、不打 LLM、不重寫註記**
  （續跑的輪次照常跑，只是跳過這一步）；期間有新告警併進來，註記的 `[未經分析]` 原始摘要會跟著更新，到時間再帶完整成員重試。
  退避狀態存在資料庫，服務重啟不會重置。沒設 LLM（`llm.url` 留空）不算失敗、不退避。**不需要為了止損去清空 `llm.url`**。
- **註記標籤變了就刪掉重建**：事故結案（open → closed）、嚴重度升級、有新系統加入時，註記的標籤會變。Grafana 13.0.7 實測：更新註記的標籤之後，用**舊標籤**查還是查得到它，儀表板的「進行中」清單會一直列著已結案的事故（用真的 Grafana 跑 demo 時發現的）。所以標籤變了就刪掉舊的、建一筆新的（註記 id 會換），只有文字或時間變才原地更新。
- **寫回**：先寫自己的資料庫，再寫註記；一個事故一筆註記（標籤 `aiops`、`aiops-incident:<uid>`、嚴重度、`open`／`closed`、
  各系統名稱），之後原地更新。註記寫失敗下一輪補，不會重複建。

## 健康檢查

`GET /healthz` 不用 Token，給 Kubernetes 探針用（chart 已設好 readiness 與 liveness）。回 503 的情況：

- 工作執行緒死了（`worker dead`）。
- 同一輪跑超過 10 分鐘（`round stuck`）：執行緒還活著但卡住，單看「活著沒」抓不到，所以另外看這一輪跑了多久。

HTTP 服務本身沒回應，或連續 3 次（45 秒）沒回 200，K8s 會重啟 Pod。重啟不掉告警：還沒處理的都在資料庫的 inbox，啟動後自動排一輪。
一輪失敗（例如資料庫斷線）不算不健康：工作執行緒會自己在 30 秒後重試。

探針只管 Pod 裡面。**Pod 活著但 Webhook 送不到**（路由設錯、Token 不一致、網路不通）要靠 Grafana 那一側發現：
看 Contact Point 的送達狀態，或對 Grafana 內建 Alertmanager 的通知失敗指標設告警（這一段沒有在這個 repo 驗證過）。

## 部署與執行

**Helm**：整份 `config.yaml` 由 chart 用 values 產生，放進一個 **Secret**，掛載成 `/etc/aiops/config.yaml` 給 Pod 讀（唯讀、`0440`）；
Pod 沒有任何環境變數設定。Secret 內容有變就會自動重啟服務。

`values.yaml` 只放空白範本，可以進 Git。真實的 Token 與密碼寫在另一份**不進 Git** 的檔案（`secrets.values.yaml`、`*.secret.yaml` 已在 `.gitignore`），部署時一起帶：

```yaml
# secrets.values.yaml（不進 Git）
grafana:  {url: https://grafana.corp.example, token: glsa_xxx}
webhook:  {token: xxx}
postgres: {password: xxx}        # 只用英數字（會放進連線字串）
llm:      {url: https://llm.corp.example/v1, apiKey: xxx, model: xxx}   # 可省略
es:       {datasourceUid: xxx}                                          # 可省略
```

```
helm upgrade --install aiops-lite deploy/helm/aiops-lite -f secrets.values.yaml
```

`grafana.url`、`grafana.token`、`webhook.token`、`postgres.password` 沒填，`helm template` 會直接報錯。
`database.url` 留空 = 連 chart 自帶的 PostgreSQL；要連外部資料庫才填。
進階的選填設定寫在 values 的 `extraConfig`（結構跟 config.yaml 一樣）；自訂提示詞填 `llm.systemPrompt`。

**本機**：`cp config.example.yaml config.yaml`，填好之後 `AIOPS_CONFIG_FILE=config.yaml python -m src.server`
（預設 SQLite、port 8080）。`python -m src.main` 會立刻處理 inbox 裡的告警一輪（不等 debounce，除錯用）。

**測試**：`python -m pytest tests -q`（預設用 SQLite）；`TEST_DATABASE_URL=postgresql+psycopg://… python -m pytest tests -q`
改在真的 PostgreSQL 上跑同一批測試（每個測試前後把表清掉重建，請用專門的測試資料庫）。

## 驗證狀態（照實寫）

做過的：
- 190 個測試通過，**SQLite 與 PostgreSQL 16 各跑一遍都過**。Webhook 解析、ES 代理、註記全部用 **Grafana 13.0.7 實際送出與回應的內容**（`tests/fixtures/grafana13/`）；
  常駐服務的測試真的起 HTTP 服務、連發 Webhook。
- 對真的 Grafana 13.0.7 + PostgreSQL + ES 8.19.23 端到端跑過（2026-11-21，本機拋棄式環境，**資料是腳本控制的**）：
  三條規則各隔 20 秒越線，Grafana 在 43 秒內陸續推了 3 個 Webhook；aiops-lite 在第一個 Webhook 後整整 60 秒才跑第一輪，
  一輪收了 3 筆告警、收斂成 1 張 P1 事故（三個系統經兩條關係邊串起來）、寫 1 筆註記；三筆 resolved 進來後，
  安靜滿設定的 60 秒結案，註記原地改成已結案，之後不再續跑。期間的錯誤日誌是在 debounce 期間才寫進 ES 的，分析時撈得到。
- 改成單一 config.yaml 之後，用 `env -i`（清空所有環境變數、只留 `AIOPS_CONFIG_FILE`）起服務，對真的 Grafana 13.0.7 + PostgreSQL 16
  + ES 8.19.23 再跑一次端到端：Webhook → debounce → 1 張事故 → 註記 → resolved → 結案都正常。那次 amhs-mcs 的規則沒有響
  （測試腳本在規則的 10 秒評估週期內就把值改回去），所以事故只有 2 個系統；三個系統串起來的情況是先前（SQLite、環境變數版）驗過的。
- **用真的 LLM（OpenAI，模型 `gpt-6-luna`，金鑰是你授權使用的環境金鑰）跑過端到端**（2026-11-21，本機 Grafana 13.0.7 + PostgreSQL 16 + ES 8.19.23，
  日誌與告警是腳本灌的假資料）：三個系統先後越線 → 收成 1 張 P1 事故 → 真模型回出
  「可能源頭為 mq-broker 的 lot.dispatch 消費延遲，影響下游 cim-eap 與 amhs-mcs，錯誤集中於 EQP-CVD-07、LOT24A0031 與 FOUP0912」，
  通過幻覺檢查，註記標 `[LLM 推論]`；全部恢復後結案。錯的金鑰（401）也實測過：退回 `[未經分析]`、錯誤訊息不含金鑰。
  這次實測抓到一個問題並已修掉：程式原本固定送 `temperature: 0`，這個模型只接受預設值、回 400；現在預設不送，要送請設 `llm.temperature`。
- **Grafana 規則改查 ES 的版本，對真的 Grafana 13.0.7 + ES 8.19.23 + PostgreSQL 16 + 真的 LLM 再跑一次端到端**（2026-11-22，本機，資料是腳本灌的、
  格式照 `FACTORY_METRICS.md` 的統一欄位規範）：3 條 Grafana 告警規則直接查 ES（MQ 佇列深度、CIM T3 逾時筆數、AMHS 中止筆數）真的觸發，
  收斂成 1 張 P1 事故，真 LLM 指出 mq-broker 無消費者為可能源頭、異常集中在 EQP-CVD-07 等；恢復後結案。截圖（Grafana 告警規則、規則細節、
  指標曲線疊事故區間、事件總覽）是這次實跑的真實畫面。這次抓到註記標籤更新後舊標籤仍查得到的問題（見上面「註記標籤變了就刪掉重建」），已修正並重跑確認。
  那次的第三個告警（AMHS）比第一輪分析晚了幾秒才到，併入同一張事故後重新分析：進行中畫面先是 2 個系統，結案時是 3 個。
- **關聯證據（批號時間線、Tempo 呼叫鏈）用真的元件實測**（2026-11-23，本機：Grafana 13.0.7、ES 8.19.23、Tempo 2.9.0、Editor Token，模擬資料）。
  混合情境：IT 的 `mes-db` 連不上 → `mes-api`、`web-portal` 失敗 → 工廠的 `cim-eap` 逾時 → `amhs-mcs` 搬運中止。
  - 四個告警經關係邊收成 1 張 P1 事故；批號 `LOT24A0031` 的時間線跨 `cim-eap`、`amhs-mcs`、`mes-api` 三個系統，含出事前的正常紀錄。
  - 呼叫鏈取回 `web-portal → mes-api →（連不上 mes-db）`，失敗點標在 `mes-api` 的資料庫查詢。
  - 真的 LLM（OpenAI `gpt-6-luna`）各問一次：日誌本身就寫了「connection refused mes-db」時，有沒有新證據結論都一樣（都指 mes-db）；
    **把日誌改成只寫「upstream error」、關係邊也拿掉 mes-db 之後，沒有呼叫鏈的回「證據不足」，有呼叫鏈的指出 mes-db**。
    也就是說，呼叫鏈在「日誌沒講清楚」時才有差別；每種情況只問了一次，沒有重複量測。
  - 實測發現並處理的兩件事：Tempo 的搜尋不保證先回最新的（改成多搜再挑最近的）；剛收到的 Trace 要 8～40 秒以上才搜尋得到
    （改成優先用日誌裡的 trace id 直接取，實測送出 3 秒後就取得到）。
- Helm chart 用 `helm lint` 與 `helm template` 檢查過；產生的 Secret 裡的 `config.yaml` 用程式的 `Settings.from_dict` 讀過；
  Deployment 沒有任何 `env`。

沒做過的：
- **沒有任何真實產線資料與真實的告警風暴**；60 秒夠不夠等到齊，要看現場各系統的告警時間差與 ES 延遲，請依實際情況調 YAML。
- **真模型只實測過一個情境、一個模型**：摘要品質、不同情境下多常被幻覺檢查擋掉、其他模型的相容性都不知道；答案是不是對的根因，只有現場人員能判斷。
- chart 沒有部署到叢集、映像檔沒有實際 build 過（這個環境沒有 Docker 與叢集）。
- 只測過 Grafana 13.0.7。

## 上線

上線前要填哪些設定、公司 Grafana 與 ES 要配合什麼、還有哪些事沒驗證過，照 **`GO_LIVE_CHECKLIST.md`** 由上往下做。

## 下一階段（只有設計，尚未實作）

輕量級 Learning Loop：工程師在 Grafana 替結案的註記加 `rca-ok`／`rca-wrong` 標籤，之後類似的事故把確認過的舊案給 LLM 參考；
不微調模型、不自動改 Grafana 規則、不對外開放新入口。設計、兩個防呆與量測方式見 **`PHASE2_LEARNING_LOOP.md`**。

## 已知限制

- **沒有 Webhook 就什麼都不會發生**。Contact Point 送不到、路由沒設對、Grafana 自己掛了，平台都不會知道（`/healthz` 只管 Pod 自己）；
  要靠 Grafana 那一側的送達狀態或通知失敗告警，以及資料庫的 `rounds` 表。
- **resolved 沒送到，事故就不會結案**（Contact Point 勾了 Disable resolved message、或那一次推送失敗且重送用盡）。
  平台不猜「應該已經好了」。
- **只能跑 1 個副本**：debounce 的計時在記憶體裡（告警本身有落地）。chart 固定 1 個副本、Recreate 升級。
- **送給 LLM 的日誌樣本沒有遮罩**。日誌裡的敏感資訊要在產生端去識別化，或把 `llm.url` 指到公司內部的模型。
- 關聯時間線靠各系統日誌的時間戳排序：系統之間沒有對時的話，先後順序可能是錯的，LLM 會被誤導。
- 呼叫鏈只看得到有埋點、沒被取樣掉的請求；日誌沒帶 trace id 時要靠 Tempo 搜尋，剛發生的 Trace 可能還搜不到。
- 時間線與呼叫鏈會讓送給 LLM 的內容變長（每個事故多 1～2 次 ES 查詢、最多 4 次 Tempo 查詢）；一樣沒有遮罩。
- 幻覺檢查只擋「名稱不存在」，擋不了「存在的系統被講錯因果」；所以第 2 行固定標 `[LLM 推論]`。
- 沒有 Trace 時「集中在哪台機台／批號」只是錯誤日誌的計數，不是因果證據。
- 上游沒有告警規則（或沒越線）時平台看不到它：只會看到下游的告警，不會替上游補洞。
- 進行中事故會吸收同系統或相鄰系統的新告警，不看時間間隔：一筆告警響很久沒恢復時，之後相鄰系統不相干的告警也會併進來。
- 規則的標籤改了，Grafana 會把它當成另一筆告警（fingerprint 不同）。
