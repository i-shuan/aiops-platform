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
llm: {url: "", api_key: "", model: "", retry_initial_seconds: 60, retry_max_seconds: 1800}   # 整段可省略 = 不分析
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

- **啟動時嚴格檢查**：檔案不存在、鍵名打錯（任何一層）、型別不對、必填沒填，直接報錯、不啟動。數字開頭的密鑰要加引號。
- **日誌與 Trace 的欄位名稱可以改**（`es.log_fields`、`es.trace_fields`）：查詢條件、排序、讀回來的內容都照這裡的名稱；沒寫的用預設值。
- **檔案裡有 Token 與密碼，不要推上 Git**：`config.yaml` 已在 `.gitignore` 與 `.dockerignore`，專案裡只有 `config.example.yaml`。
  設定被印出來（log、例外訊息）時，Token、密鑰、連線字串不會出現。
- 實際看到註記的延遲 ≈ 規則的 `for` + Grafana 路由的 `group_wait` + `debounce_seconds` + LLM 回應時間。

## 檔案

| 檔案 | 行數 | 負責內容 |
| --- | --- | --- |
| `src/server.py` | 217 | 常駐服務：收 Webhook（Bearer 驗證、先落地再回 200）、debounce 排程、單一工作執行緒、續跑 |
| `src/main.py` | 291 | 讀 config.yaml（嚴格檢查）與「一輪」的流程 |
| `src/grafana.py` | 186 | 解析告警 Webhook、ES Proxy 查日誌與 Trace、寫註記 |
| `src/grouping.py` | 164 | 同實體合併、拓撲鏈收斂、併入進行中事故、嚴重度只升不降、全部 resolved 且安靜夠久才結案 |
| `src/rca.py` | 173 | 不寫推理演算法：收集 4 包證據、組 Prompt、呼叫 LLM、幻覺檢查、失敗退回原始摘要 |
| `src/report.py` | 60 | 固定三行註記（台灣時間、HTML 跳脫、調整優先權連結） |
| `src/db.py` | 166 | SQLAlchemy Core（SQLite／PostgreSQL），啟動自動建表：待處理告警、事故、每輪執行紀錄 |

合計約 1,257 行。

## 各系統要做的事（平台零直連）

1. 把資料送進公司 Grafana 看得到的資料源：MQ 掛 Prometheus exporter；CIM 用日誌收集器做 Log 轉指標；AMHS 開唯讀 View 給
   Grafana 的 SQL 資料源。錯誤日誌進 ES，帶 `service.name`、`log.level`、`message`、`@timestamp`，以及有的話 `eqp_id`／`lot_id`／`carrier_id`。
2. 在 Grafana 建告警規則（閾值自己定）並貼標籤：`aiops_detect=true`、`aiops_severity=P0～P3`、`service=<系統名稱>`
   （要跟關係邊裡的名稱一樣）。沒有 `service` 的告警會被略過並回報給送出端；`aiops_severity` 寫錯當成 P3。

## Grafana 要做的設定

**都在公司 Grafana 的網頁介面（或它的 API）上做；不改 Grafana 映像檔、不裝外掛、不改規則的閾值。**
repo 裡的 `deploy/helm/aiops-grafana` 是舊平台自己的 Grafana，lite 不使用、也沒改它。

| # | 項目 | 在哪做 | 怎麼做 |
| --- | --- | --- | --- |
| 1 | 服務帳號與 Token | Administration → Users and access → Service accounts | 新增角色 **Editor** 的帳號 → Add token → 填進 config.yaml 的 `grafana.token` |
| 2 | 告警規則標籤 | Alerting → Alert rules → 編輯規則 → Labels | 每條要納管的規則加 `aiops_detect=true`、`aiops_severity=P0～P3`、`service=<系統名>` |
| 3 | **Contact Point**（新增 1 個） | Alerting → Contact points → Add | 類型 Webhook；URL `http://<release>.<namespace>.svc:8080/webhook`；Authorization scheme `Bearer`、credentials 填 config.yaml 的 `webhook.token`；**不要**勾 Disable resolved message |
| 4 | **Notification policy**（新增 1 條路由） | Alerting → Notification policies → New child policy | Matcher `aiops_detect = true` → 上面的 Contact Point；`group_wait` 設短（0～10 秒，平台自己會等 debounce）；要同時保留原本的通知就勾 Continue matching |
| 5 | 儀表板 | Dashboards → New → Import | 貼上或上傳 `dashboards/aiops-overview.json`（只用內建註記清單面板，不需要資料源） |
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

**本機**：`cp config.example.yaml config.yaml`，填好之後 `AIOPS_CONFIG_FILE=config.yaml python -m src.server`
（預設 SQLite、port 8080）。`python -m src.main` 會立刻處理 inbox 裡的告警一輪（不等 debounce，除錯用）。

**測試**：`python -m pytest tests -q`（預設用 SQLite）；`TEST_DATABASE_URL=postgresql+psycopg://… python -m pytest tests -q`
改在真的 PostgreSQL 上跑同一批測試（每個測試前後把表清掉重建，請用專門的測試資料庫）。

## 驗證狀態（照實寫）

做過的：
- 104 個測試通過，**SQLite 與 PostgreSQL 16 各跑一遍都過**。Webhook 解析、ES 代理、註記全部用 **Grafana 13.0.7 實際送出與回應的內容**（`tests/fixtures/grafana13/`）；
  常駐服務的測試真的起 HTTP 服務、連發 Webhook。
- 對真的 Grafana 13.0.7 + PostgreSQL + ES 8.19.23 端到端跑過（2026-11-21，本機拋棄式環境，**資料是腳本控制的**）：
  三條規則各隔 20 秒越線，Grafana 在 43 秒內陸續推了 3 個 Webhook；aiops-lite 在第一個 Webhook 後整整 60 秒才跑第一輪，
  一輪收了 3 筆告警、收斂成 1 張 P1 事故（三個系統經兩條關係邊串起來）、寫 1 筆註記；三筆 resolved 進來後，
  安靜滿設定的 60 秒結案，註記原地改成已結案，之後不再續跑。期間的錯誤日誌是在 debounce 期間才寫進 ES 的，分析時撈得到。
- 改成單一 config.yaml 之後，用 `env -i`（清空所有環境變數、只留 `AIOPS_CONFIG_FILE`）起服務，對真的 Grafana 13.0.7 + PostgreSQL 16
  + ES 8.19.23 再跑一次端到端：Webhook → debounce → 1 張事故 → 註記 → resolved → 結案都正常。那次 amhs-mcs 的規則沒有響
  （測試腳本在規則的 10 秒評估週期內就把值改回去），所以事故只有 2 個系統；三個系統串起來的情況是先前（SQLite、環境變數版）驗過的。
- Helm chart 用 `helm lint` 與 `helm template` 檢查過；產生的 Secret 裡的 `config.yaml` 用程式的 `Settings.from_dict` 讀過；
  Deployment 沒有任何 `env`。

沒做過的：
- **沒有任何真實產線資料與真實的告警風暴**；60 秒夠不夠等到齊，要看現場各系統的告警時間差與 ES 延遲，請依實際情況調 YAML。
- **LLM 沒有接過真的模型**：只對本機的 OpenAI 相容假端點驗過 HTTP 接線；摘要品質、真模型多常被幻覺檢查擋掉都不知道。
- chart 沒有部署到叢集、映像檔沒有實際 build 過（這個環境沒有 Docker 與叢集）。
- 只測過 Grafana 13.0.7。

## 已知限制

- **沒有 Webhook 就什麼都不會發生**。Contact Point 送不到、路由沒設對、Grafana 自己掛了，平台都不會知道（`/healthz` 只管 Pod 自己）；
  要靠 Grafana 那一側的送達狀態或通知失敗告警，以及資料庫的 `rounds` 表。
- **resolved 沒送到，事故就不會結案**（Contact Point 勾了 Disable resolved message、或那一次推送失敗且重送用盡）。
  平台不猜「應該已經好了」。
- **只能跑 1 個副本**：debounce 的計時在記憶體裡（告警本身有落地）。chart 固定 1 個副本、Recreate 升級。
- **送給 LLM 的日誌樣本沒有遮罩**。日誌裡的敏感資訊要在產生端去識別化，或把 `llm.url` 指到公司內部的模型。
- 幻覺檢查只擋「名稱不存在」，擋不了「存在的系統被講錯因果」；所以第 2 行固定標 `[LLM 推論]`。
- 沒有 Trace 時「集中在哪台機台／批號」只是錯誤日誌的計數，不是因果證據。
- 上游沒有告警規則（或沒越線）時平台看不到它：只會看到下游的告警，不會替上游補洞。
- 進行中事故會吸收同系統或相鄰系統的新告警，不看時間間隔：一筆告警響很久沒恢復時，之後相鄰系統不相干的告警也會併進來。
- 規則的標籤改了，Grafana 會把它當成另一筆告警（fingerprint 不同）。
