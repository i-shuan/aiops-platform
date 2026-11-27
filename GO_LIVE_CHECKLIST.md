# aiops-lite 上線前檢查清單

給接手維運的人：照這份由上往下做，就能把 aiops-lite 接到公司環境。
每一節開頭標了「誰做」；打勾用的方框可以直接在 PR 或工單裡勾。

## 0. 先搞清楚它碰什麼、不碰什麼

- **aiops-lite 不碰產線資料庫，也不直連日誌、Trace、指標的儲存元件**（VictoriaLogs、Elasticsearch、VictoriaTraces、Tempo、VictoriaMetrics）。
  查資料只跟公司的 Grafana 講話，用一把 Grafana Token；日誌、Trace、指標都是借道 Grafana 的資料源去查。
- **先決定日誌與 Trace 放在哪一種**：日誌是 VictoriaLogs 或 Elasticsearch，Trace 是 VictoriaTraces 或 Tempo，兩者各選各的。
  下面凡是分兩種填法的地方都標了「用 Victoria」與「用 Elasticsearch／Tempo」。
- **只有日誌放在 VictoriaLogs 時，公司的 Grafana 才要裝一個外掛。** Grafana 沒有內建 VictoriaLogs 的資料源，要請管理員裝
  `victoriametrics-logs-datasource`（第 3 節）。裝不了又沒有 Elasticsearch 可用的話就沒有日誌證據；程式不會改成直連。
  用 Elasticsearch／Tempo 不用裝任何外掛。
- **對外多了兩個出口，預設都不送**：事故 API 與自動修復（第 4 節之後的「出口」）。還沒有下游系統時什麼都不用填。
- **它自帶的 PostgreSQL 只是事故的小帳本**：記「收到哪些告警、開了哪些事故、每一輪跑得如何」三張表。
  用 Helm 部署時會自動建立這個 PostgreSQL，表也是程式啟動時自動建，不用任何人先建資料庫或跑腳本。
- **你要填的是「聯絡方式」，不是架設基礎設施**：Grafana 在哪、用哪把 Token、LLM 在哪、日誌在 Grafana 的哪個資料源。
- **一個 aiops-lite 對一個 Grafana**（用一個組織底下的一把 Token），只能跑 1 個副本；事故以 `service` 為單位收斂，
  不看 cluster、namespace、pod 的標籤。有多個 cluster 或廠區時怎麼做，見 `README.md` 的「整合的單位」。
- 本機測試可以不要 PostgreSQL，改用單一檔案的 SQLite（設定檔的 `database.url` 不寫就是）。
  **Helm chart 一律用 PostgreSQL**，沒有 SQLite 的選項，所以 `postgres.password` 一定要填。

## 1. 填 `secrets.values.yaml`（平台維運做）

這個檔案放 Token 與密碼，**不進 Git**（`.gitignore` 已排除）。在 repo 外面（或 repo 根目錄，檔名已被 `.gitignore` 排除）新建一個，內容照下面填：

```yaml
grafana:
  url: https://grafana.corp.example     # 公司 Grafana 的網址
  token: glsa_xxxxxxxx                  # 步驟 1-1
webhook:
  token: 自訂英數字                      # 步驟 1-4
postgres:
  password: 自訂英數字                   # 步驟 1-4
llm:
  url: https://llm.corp.example/v1      # 步驟 1-3
  apiKey: xxxxxxxx
  model: 模型名稱
logs:
  type: victorialogs                    # 用 Elasticsearch 就改成 elasticsearch，並加一行 index: "你的索引-*"
  datasourceUid: xxxxxxxx               # 步驟 1-2：日誌資料源的編號
traces:
  type: victoriatraces                  # 用 Tempo 就改成 tempo
  datasourceUid: xxxxxxxx               # 步驟 1-2：有 Trace 才填
metrics:
  datasourceUid: xxxxxxxx               # 步驟 1-2：要指標走勢才填（另外要在 values.yaml 寫 metrics.queries）
```

### 白話版：這些值其實只有 3 類

你不是在設定基礎設施，只是在填一張「通訊錄（去哪找 Grafana、去哪找 LLM）」加上「兩組自己訂的密碼」。

| 區塊 | 填什麼 | 白話用途 |
| --- | --- | --- |
| `grafana` ＋ `logs`／`traces`／`metrics`（2 個值，加上最多 3 個資料源編號） | Grafana 網址、Token、日誌／Trace／指標各自的資料源編號 | **去哪裡看監控與撈證據**：讓程式找得到公司 Grafana，並知道透過它去翻哪一個資料源 |
| `llm`（3 個值） | 內部 LLM 的網址、API Key、模型名稱 | **找哪個 AI 幫忙分析**：公司內部 AI 服務的連線位置 |
| `webhook` ＋ `postgres`（2 個值） | 兩組自己訂的英數字密碼 | **系統自己的門鎖**：一組防別人亂發假告警，一組給自帶的事故小帳本用 |

**為什麼要 `webhook.token`（門鎖暗號）**

aiops-lite 有一個收告警的網址（`http://<release>.<namespace>.svc:8080/webhook`）。沒有暗號的話，叢集內任何人或任何測試腳本打這個網址，
只要內容長得像告警，aiops-lite 就會開一張事故、去問 LLM。所以這個值是**必填**，沒填服務起不來。

- 自己產生一串**夠長、猜不到**的英數字（例如用 `openssl rand -hex 16` 產生），不要用好猜的字。
- 之後在公司 Grafana 設 Webhook 通知時，把**同一串**貼進 Authorization 的 credentials；兩邊對得上才收。

**資料源編號到底是什麼（只是指路，不是設定儲存元件）**

這個檔案裡**不用也不會去設定公司的 VictoriaLogs、Elasticsearch、VictoriaTraces、Tempo、VictoriaMetrics**。這幾個值只是告訴 aiops-lite「借道 Grafana 查的時候，要用哪一個資料源」：

| 值 | 用 Victoria | 用 Elasticsearch／Tempo |
| --- | --- | --- |
| `logs.type`、`logs.datasourceUid` | `victorialogs`；接到 VictoriaLogs 的資料源的編號 | `elasticsearch`；接到 Elasticsearch 的資料源的編號 |
| `traces.type`、`traces.datasourceUid`（沒有 Trace 就不寫） | `victoriatraces`；**Jaeger 型別**（URL 指到 VictoriaTraces 的 `/select/jaeger`）資料源的編號 | `tempo`；Tempo 資料源的編號 |
| `metrics.datasourceUid`（不要指標走勢就不寫） | **Prometheus 型別**（指到 VictoriaMetrics）資料源的編號 | 同左（Prometheus 相容的都可以） |

編號在 Grafana 網頁點開該資料源，網址列最後那一段。

只想看其中一部分日誌時：VictoriaLogs 沒有 index 的概念，在 `values.yaml` 設 `logs.extraFilter`（一段 LogsQL 條件，例如 `{namespace="factory"}`），
它會加在每一條查詢的最前面；Elasticsearch 則設 `logs.index`（例如 `fluentd-*`，不寫就是全部）。兩個設定各自只在對應的 `logs.type` 下能用，設錯組合服務起不來。

要注意：這只是 aiops-lite 這一側。日誌那一側仍然有別人要做的事（各系統把日誌照統一欄位送進 VictoriaLogs 或 Elasticsearch），在第 3 節。

**要花多久**：Token、LLM 的連線資訊都拿到手之後，填這個檔案只要幾分鐘；花時間的是去要這些東西（開單請 Grafana 管理員建帳號、問到內部 LLM 的端點）。

### 1-1 Grafana Token

- [ ] Grafana 左側選單 **Administration → Users and access → Service accounts**
- [ ] **Add service account**：名稱 `aiops-lite`，角色 **Editor**
- [ ] 點進這個帳號 → **Add service account token** → 產生後複製（`glsa_` 開頭，**只顯示一次**）
- [ ] 貼到 `grafana.token`

看不到這個選單、或沒有權限建立：開單請 Grafana 管理員建，並註明這把 Token 需要能做四件事：

| 需要的權限 | 用途 |
| --- | --- |
| 經資料源代理查日誌（VictoriaLogs 或 Elasticsearch）、Trace（Jaeger 型別或 Tempo）、指標（Prometheus 型別）的資料源 | 撈錯誤日誌、呼叫鏈、指標走勢當 RCA 證據 |
| 新增註記 | 開事故時寫一筆 |
| 修改註記 | 更新事故的摘要與時間 |
| **刪除註記** | 事故結案或升級時，舊註記要刪掉重建（Grafana 更新標籤後舊標籤仍查得到） |

### 1-2 資料源的 uid

- [ ] Grafana 左側選單 **Connections → Data sources**，點進放日誌的那個資料源（用 Victoria：型別是 VictoriaLogs，沒有的話先做第 3 節；
      用 Elasticsearch：型別是 Elasticsearch，並把 `logs.type` 設成 `elasticsearch`）
- [ ] 看瀏覽器網址列，最後一段就是 uid：`…/connections/datasources/edit/`**`這一段`** → 貼到 `logs.datasourceUid`
- [ ] 有 Trace 的話：同樣的方式找到 Trace 的資料源 → 貼到 `traces.datasourceUid`（用 Victoria：**Jaeger 型別**、URL 指到 VictoriaTraces 的
      `/select/jaeger`；用 Tempo：Tempo 資料源，並把 `traces.type` 設成 `tempo`）
- [ ] 要指標走勢的話：找到 **Prometheus 型別**（URL 指到 VictoriaMetrics）的資料源 → 貼到 `metrics.datasourceUid`
- [ ] 用正式的 Token 確認查得到日誌（把 `$UID` 換成上面的 uid、系統名稱換成現場真的有的）。**用 VictoriaLogs：**

  ```bash
  curl -s -H "Authorization: Bearer $TOKEN" "$GRAFANA/api/datasources/proxy/uid/$UID/select/logsql/query" \
    -d 'query="service.name":="cim-eap" | sort by (_time) desc | limit 3' \
    -d "start=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)" -d "end=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  ```

  回來的是一行一筆的 JSON。一行都沒有：這一小時沒有這個系統的日誌，或系統名稱的欄位不叫 `service.name`（第 2 節）。
  回 403：Token 沒有走資料源代理的權限。回 404 或「plugin not found」之類：外掛還沒裝或 uid 抄錯。

  **用 Elasticsearch**（`fluentd-*` 換成現場的索引）：

  ```bash
  curl -s -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/x-ndjson" \
    "$GRAFANA/api/datasources/proxy/uid/$UID/_msearch" --data-binary $'{"index":"fluentd-*","ignore_unavailable":true}\n{"size":3,"sort":[{"@timestamp":"desc"}],"query":{"match_phrase":{"service.name":"cim-eap"}}}\n'
  ```

  `responses[0].hits.hits` 是空的：沒有這個系統的日誌，或欄位名稱對不上（第 2 節）。

三個都不填也能跑，只是 RCA 沒有那一種證據；全部不填的話 LLM 只能看告警本身。

### 1-3 內部 LLM

- [ ] 填公司內部 LLM 的端點（OpenAI 相容，網址到 `/v1` 為止）、API Key、模型名稱
- [ ] 不要填外部服務：日誌樣本（含機台、批號）會原樣送給這個端點，目前沒有遮罩

整段 `llm` 不寫也能跑：事故照樣收斂、照樣寫註記，第 2 行顯示 `[未經分析]` 的原始摘要。

### 1-4 自訂的兩組密碼

- [ ] `webhook.token`：自己訂一組**純英數字**的字串。之後在 Grafana 的 Contact Point 要填同一組（第 3 節）
- [ ] `postgres.password`：自己訂一組**純英數字**的字串（會放進連線字串，有符號會壞）

## 2. 對齊 `values.yaml`（平台維運做）

`deploy/helm/aiops-lite/values.yaml` 可以進 Git，裡面不放密碼。要看的只有這幾項：

- [ ] **系統之間的關係不用寫**（`edges` 已經移除，設定檔裡還有的話要刪掉，不然啟動報錯）。程式自己學：
      Tempo 有開 service graph 的話，把它存指標的那個資料源 uid 填進 `traces.serviceGraphDatasourceUid`
      （Grafana 的 Explore 選 Tempo，有 Service Graph 分頁且畫得出圖就是有開；uid 看 Tempo 資料源設定頁連到哪個 Prometheus 型別的資料源）。
      用 VictoriaTraces 的話不用填：它啟動時有加 `-servicegraph.enableTask=true` 就會自動讀到。
      沒開也沒關係，會從事故的呼叫鏈與錯誤日誌學，只是學得慢。上線後看學到了什麼：
      `SELECT caller, callee, source, times, last_seen FROM observed_calls ORDER BY last_seen DESC;`

- [ ] **等待時間 `timing`**：先用預設值上線，看實際情況再調

  | 設定 | 預設 | 什麼時候要調 |
  | --- | --- | --- |
  | `debounceSeconds` | 60 | 下游告警常常晚到、被分成兩次分析 → 調大；嫌註記出現得太慢 → 調小 |
  | `followupSeconds` | 60 | 一般不用動 |
  | `closeAfterSeconds` | 300 | 告警常常剛恢復又響、事故一直開新的 → 調大 |

- [ ] **映像檔 `image.repository`、`image.tag`**：改成公司映像檔倉庫的路徑與版本
- [ ] **欄位名稱**：公司日誌的欄位如果照 `FACTORY_METRICS.md` 的統一欄位規範，就不用動。不一樣的話改這幾項：

  | 不一樣的地方 | 改哪裡 |
  | --- | --- |
  | 系統、等級的欄位名稱。時間與訊息：VictoriaLogs 固定是 `_time`、`_msg`（由送日誌的那一端在寫入時指定，這裡不能設）；Elasticsearch 可以另外設 `time`、`message`（預設 `@timestamp`、`message`，訊息欄位要是 text 型別） | `logs.fields` |
  | 機台、批號、載具的欄位名稱 | `logs.idFields` |
  | 錯誤等級的寫法（例如 `E`、`SEVERE`） | `extraConfig.logs.error_levels` |
  | 只看一部分日誌 | VictoriaLogs：`logs.extraFilter`；Elasticsearch：`logs.index` |
  | 日誌裡記 trace id 的欄位（預設 `trace_id`） | `extraConfig.traces.trace_id_field` |
  | 告警標籤的名稱 | `extraConfig.labels` |

- [ ] **從舊版升級的話**：`es`、`tempo` 兩個區段已經改名成 `logs`（加 `type: elasticsearch`）、`traces`（加 `type: tempo`）；
      `logIndex` 改叫 `logs.index`、`logFields` 改叫 `logs.fields`，`traceIndex`、`traceFields` 沒有了。
      舊的寫法留著，程式啟動時會直接報錯並說要改成什麼（Pod 會一直重啟，log 裡有寫）。
- [ ] **出口 `outputs`**：還沒有下游系統就整段不要動（預設是佔位）。要接時見「出口」一節

其他進階設定（逾時、重試、給 LLM 的樣本數等）都有預設值，平常不用寫；清單見 `CONFIG.md`。

## 3. 公司 Grafana 與日誌／Trace 儲存上要做的（Grafana 管理員、儲存的管理員、各系統團隊）

**Grafana 管理員**

- [ ] 第 1-1 節的服務帳號與 Token
- [ ] **只有日誌放在 VictoriaLogs 時**：裝資料源外掛 `victoriametrics-logs-datasource`（實測的版本是 0.34.0）。這是整套裡唯一要動到 Grafana 本體的地方；
      公司的 Grafana 不允許裝外掛的話，先停下來討論（改用 Elasticsearch，或接受沒有日誌證據）。用 Elasticsearch／Tempo 不用裝任何外掛
- [ ] **Connections → Data sources → Add**，建資料源（已經有的就不用），uid 給平台維運填第 1-2 節：
      - 用 Victoria：VictoriaLogs（URL 填 VictoriaLogs 的位址）、Jaeger（URL 填 VictoriaTraces 的位址加 `/select/jaeger`）
      - 用 Elasticsearch／Tempo：Grafana 內建的 Elasticsearch、Tempo 型別，多半公司已經有了
      - 指標：Prometheus（URL 填 VictoriaMetrics 的位址）
- [ ] **Alerting → Contact points → Add**：類型 Webhook；URL `http://<release>.<namespace>.svc:8080/webhook`；
      Authorization scheme 選 `Bearer`、credentials 填第 1-4 節的 `webhook.token`；**不要**勾 Disable resolved message
- [ ] **Alerting → Notification policies → New child policy**：Matcher `aiops_detect = true` → 上面的 Contact Point；
      `group_wait` 設短（0～10 秒）；要同時保留原本的通知就勾 Continue matching
- [ ] **Dashboards → New → Import**：匯入 `dashboards/aiops-overview.json`（事故清單）與
      `dashboards/aiops-incident-metrics.json`（指標曲線，匯入時選日誌的資料源）
- [ ] 確認 Grafana 連得到 aiops-lite 的 Service（同叢集用 Service 名稱；不同叢集要另外開路）
- [ ] 對通知失敗設一條告警：Webhook 送不到時，aiops-lite 自己不會知道

規則、Contact Point、路由如果是用 provisioning（YAML、Terraform）管的，要加進原本那份檔案，在網頁上改會被下次部署蓋掉。

**日誌／Trace 儲存的管理員**

- [ ] 確認保留期限與儲存空間（VictoriaLogs 是 `-retentionPeriod`）；空間不夠時只收 `ERROR`、`WARN` 等級的日誌
- [ ] 用 VictoriaLogs：不用先建 index 或 mapping，收到什麼欄位就存什麼欄位，巢狀的 `service: {name: …}` 會攤平成 `service.name`
- [ ] 用 Elasticsearch：訊息欄位要是 **text** 型別（事故前後的次數比較靠它），系統名稱與等級是 keyword 或動態 mapping 都可以
- [ ] 用 Tempo：收到 Trace 後要 8～40 秒以上才搜尋得到；請有埋點的系統在日誌裡帶 trace id（預設欄位 `trace_id`），aiops-lite 會先用編號直接取
- [ ] Grafana 連得到這些元件的查詢位址；有擋認證的話，帳密設在 Grafana 的資料源上，aiops-lite 不用知道

**各系統團隊（CIM、AMHS、MQ）**

- [ ] 部署 Fluent Bit（範例在 `deploy/fluent-bit/`）；**parser 的正規表示式要照真實日誌改**。範例的輸出是 http，打 VictoriaLogs 的
      `/insert/jsonline`，網址上要指定哪個欄位當時間、哪個當訊息（`_time_field=@timestamp`、`_msg_field=message`）；連線的帳密與加密要設。
      日誌是送 Elasticsearch 的話，輸出改用 Fluent Bit 的 `es`，欄位規範一樣（範例裡沒有這一種）
- [ ] 在 Grafana 設告警規則，每條貼三個標籤：`aiops_detect=true`、`aiops_severity=P0～P3`、`service=<系統名稱>`
- [ ] 要讓某條告警可以觸發修復流程才多貼 `aiops_remediation=<流程名稱>`；先讀「出口」一節，沒討論過不要貼
- [ ] 每條規則明確設定 No data 時的行為，不要留預設
- [ ] 每個資料來源加一條「資料還在進來」的規則（收集器掛掉時計數是 0，看起來像正常）

## 4. 進現場前還沒做的事（平台維運做）

這些在開發環境沒有做過或沒有驗證過，**第一次上線時要實際確認**：

| 項目 | 現況 | 要做什麼 |
| --- | --- | --- |
| 映像檔 | 沒有 build 過 | 在有 Docker 的環境 `docker build`，推到公司倉庫 |
| 部署到叢集 | 只過了 `helm lint` 與 `helm template` | 實際 `helm upgrade --install`，確認 Pod 起得來（唯讀檔案系統、Secret 掛載） |
| Grafana 權限 | 只在預設權限下驗過 | 用正式的 Token 確認第 1-1 節那四件事都做得到 |
| VictoriaLogs 資料源外掛（用 VictoriaLogs 才需要） | 只在本機的 Grafana 13.0.7 裝過 0.34.0 | 在公司的 Grafana 上裝，跑第 1-2 節的 curl |
| 儲存元件的版本與部署方式 | 只測過單機、沒有認證的：VictoriaLogs v1.17.0、VictoriaTraces v0.5.0、VictoriaMetrics v1.110.0、Elasticsearch 8.19.23、Tempo 2.9.0 | 公司是叢集版、有 vmauth 或多租戶、或版本差很多的話，重跑第 1-2 節的 curl，並看一次實際事故的證據有沒有撈到 |
| 查詢耗時 | 只在幾十到幾千筆的假資料上跑過 | 第一批真實事故看服務 log 裡每一輪花多久；慢的話先設 `logs.extraFilter`（Elasticsearch 是 `logs.index`）縮小範圍。「錯誤日誌 0 筆」時多問的那一次會數整個時間窗的筆數 |
| 兩個出口 | 只對本機一個假的 HTTP 端點驗過；格式是自己定的，沒有跟任何真的告警中心或 Conductor 對過 | 見「出口」一節。接真的系統時幾乎一定要改欄位對應（要改程式） |
| Fluent Bit 送進 VictoriaLogs | 三份範例設定用 Fluent Bit 5.2.0 各跑過一次（本機、範例日誌），欄位與時間都對。沒有帳密與 TLS，沒有驗日誌輪替與斷線補送 | 部署後用第 1-2 節的 curl 確認欄位名稱是 `service.name`、`log.level`；前面有 vmauth 的話確認帳密寫法 |
| Grafana 版本 | 只測過 13.0.7 | 公司版本不同的話，重驗 Webhook 格式與註記行為 |
| 內部 LLM | 只測過一個外部模型 | 確認回應是 JSON、`temperature` 的行為；慢的話調 `extraConfig.llm.timeout_seconds` |
| 日誌遮罩 | 沒有 | 用內部模型可以不做；要送外部模型就得先改程式 |
| PostgreSQL 備份 | 沒有 | 單一實例加 PVC；要不要備份自己決定（掉了只會遺失事故紀錄，不影響產線） |

部署指令：

```
helm upgrade --install aiops-lite deploy/helm/aiops-lite -f secrets.values.yaml
```

**部署後先看 Pod 有沒有起來。** 設定檔的鍵名打錯，Helm 不會報錯，要等程式啟動時嚴格檢查才失敗（Pod 會一直重啟，log 裡有寫是哪個鍵）。

上線後的第一次驗證：

- [ ] `GET /healthz` 回 `{"status": "ok"}`
- [ ] 讓一條貼了標籤的規則觸發 → 約 1 分鐘後 Grafana 的「AIOps 事件總覽」出現一張事故
- [ ] 註記第 2 行是 `[LLM 推論]`（不是 `[未經分析]`），而且點名的機台、批號是真的
- [ ] 註記最後的「證據｜」那一行沒有「缺錯誤日誌」或「日誌查詢失敗」；有的話括號裡會寫是哪一關沒過
- [ ] 規則恢復 → 等 `closeAfterSeconds` 之後，事故移到「已結案」、「進行中」清單變空

## 出口（可以先不接）

註記以外有兩個出口：**事故 API**（把事故送給告警中心／派工系統）與**自動修復**（請 Conductor 跑修復流程）。
**預設都是佔位**：不送任何東西，只把「本來會送什麼」記進資料庫的 `deliveries` 表。還沒有下游系統時這一節可以整個跳過。

- [ ] 看佔位記了什麼（連進 aiops-lite 自帶的 PostgreSQL）：

  ```sql
  SELECT channel, status, payload FROM deliveries ORDER BY id DESC LIMIT 5;
  ```

  `channel` 是 `incident_api` 或 `remediation`；`status` 是 `placeholder`（沒有送出）、`sent`、`failed`。`payload` 就是送出去（或本來會送）的 JSON。

**要接事故 API 時**（`outputs.incidentApi.url`、`token`，放 `secrets.values.yaml`），先跟下游確認：

- [ ] 網址，以及從 aiops-lite 的 Pod 連不連得到
- [ ] 認證方式：目前只支援 `Authorization: Bearer <token>`，下游要別的就得改程式
- [ ] 欄位對應：送的是這個專案自己定的格式（`aiops-lite.incident.v1`），拿一筆佔位的 `payload` 給對方看；對不上就要改程式
- [ ] 重複收到同一張事故怎麼辦：同一張事故會送 `opened`、之後每次內容變了送 `updated`、結案送 `closed`，下游要能用事故的 `uid` 認出是同一張。
      同樣的內容只送一次；送失敗每一輪再試、最多 10 次

**自動修復上線前**（`outputs.remediation.url`、`token`、`enabled`）：

- [ ] 先維持 `enabled: false`，給少數規則貼 `aiops_remediation=<流程名稱>`，觀察佔位紀錄一陣子：什麼時候會送、送幾次、對象對不對。
      註記上會多一行 `自動修復｜流程名稱（系統：佔位，沒有送出）`
- [ ] 只給**評估過**的規則貼標籤。跑哪個流程只看規則上的標籤，LLM 的推論不會觸發、也不會選流程；所以標籤貼錯就是跑錯
- [ ] 確認流程本身**可以重複執行**：同一張事故、同一條告警、同一個流程 aiops-lite 只送一次，但告警恢復後又響、開了新事故，就會再送一次
- [ ] 跟 Conductor 那邊確認請求的格式（`aiops-lite.remediation.v1` 是自己定的）與認證
- [ ] 知道它的限制：只知道請求有沒有送出，**不知道流程成功與否**，結果不會回寫到註記；事故結案時也不會去取消流程
- [ ] 佔位記過的不會補送：把 `enabled` 改成 `true` 之後，只對之後的事故生效

## 5. 要先跟其他團隊講好的事

- [ ] **統一欄位規範**（`FACTORY_METRICS.md`）：三個系統的日誌欄位名稱要一樣，aiops-lite 的欄位對照只有一組
- [ ] **`service` 的值三處一致**：告警規則的標籤、日誌的 `service.name`、Tempo service graph 上的名稱（有 Trace 的話還有 Trace 的服務名稱）
- [ ] **整合的單位與命名**：aiops-lite 以 `service` 為單位收斂，一個 Grafana 對一份、只能 1 個副本。
      多個 cluster 或廠區共用同一個 Grafana 的話，先決定做法（`service` 帶 cluster 名、或各部署一份），因為 `service` 的命名與 `edges` 都要照著寫；
      兩個 cluster 有同名的 `service` 會被併成同一張事故（說明見 `README.md` 的「整合的單位」）
- [ ] **每個系統收哪些值、門檻多少**：由各系統團隊決定，平台不管
- [ ] **日誌裡的機台、批號能不能送給 LLM**（接了事故 API 的話，根因分析的摘要也會送給下游）
- [ ] **哪些告警可以自動修復、由誰核准**：自動修復只看規則上的標籤，貼標籤的人就是決定的人
- [ ] **試行範圍與成功標準**：先選一條產線或一個系統，訂好看什麼數字算成功

## 6. Phase 2：輕量級學習迴圈（現在不動程式）

設計已經寫好（`PHASE2_LEARNING_LOOP.md`），**程式還沒有做**，等試行累積真實事故之後再實作。

做法：工程師在 Grafana 替結案的事故註記加標籤 `rca-ok`（推論對了）或 `rca-wrong`（推論錯了），
之後類似的事故，把確認過是對的舊案給 LLM 參考。不微調模型、不自動改 Grafana 規則、不對外開放新入口。

現在就可以先做的兩件事：

- [ ] **確認工程師能不能編輯服務帳號建立的註記**（取決於公司 Grafana 的權限）。不行的話回饋管道要換做法
- [ ] **請工程師從現在開始加 `rca-ok`／`rca-wrong`**。程式現在不會讀，但這樣 Phase 2 開始時就有第一批案例與基準的準確率。
      **標籤要在事故結案之後再加**：事故結案時註記會刪掉重建，進行中加的標籤會不見

## 相關文件

| 文件 | 內容 |
| --- | --- |
| `README.md` | 架構、流程、驗證狀態、已知限制 |
| `CONFIG.md` | 每一項設定的預設值與範圍；哪些東西刻意不開放設定 |
| `FACTORY_METRICS.md` | 統一欄位規範、三個系統怎麼收值 |
| `deploy/fluent-bit/` | Fluent Bit 設定範例 |
| `demo/README.md` | Grafana 實際畫面 |
| `PHASE2_LEARNING_LOOP.md` | Phase 2 的設計 |
