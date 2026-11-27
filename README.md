# aiops-lite

事件驅動的極簡 AIOps：Grafana 告警經 Webhook 推進來 → 等一段 debounce → 跨系統收斂 → 收集證據交給 LLM → 寫回 Grafana 註記，
另外可以把事故送給下游的事故 API、請下游跑修復流程（這兩個出口還沒有真的下游，預設是佔位）。
只拿一把 Grafana Editor Token 去查資料，不直連任何日誌、Trace、指標的儲存元件，也不直連產線資料庫，沒有前端。
日誌可以放在 VictoriaLogs 或 Elasticsearch，Trace 可以放在 VictoriaTraces 或 Tempo，指標是 Prometheus 相容的（VictoriaMetrics）；
全部經公司 Grafana 的資料源代理去查。

這個 repo 現在只有 aiops-lite。原本的 aiops-platform（管線加可插拔算子的那一版：`aiops/`、它的測試、腳本、Helm chart、
資料庫遷移）已經從這個分支移除；要找舊程式請看 Git 歷史。

```
MQ / CIM / AMHS ──▶ 公司 Grafana（資料源 + 貼標籤的告警規則）
                          │ 告警 firing／resolved
                          ▼ Webhook（Contact Point）
┌──────────────────── aiops-lite（常駐服務，1 個副本）────────────────────┐
│ 收 Webhook → 先存資料庫 → 等 debounce（預設 60 秒，YAML 可調）            │
│   → 1 收斂（同系統、關係邊）→ 2 收集證據（最多 7 包）→ 3 LLM → 4 寫註記          │
│ 有進行中事故時每 followup 秒續跑一輪，直到全部 resolved 並安靜夠久才結案   │
└──────────────────────────────┬──────────────────────────────────────────┘
                               ▼ config.yaml 的 grafana.url + grafana.token
            Grafana Annotations（摘要＋證據）+「AIOps 事件總覽」儀表板
                               ▼ outputs（選用；沒設 url = 佔位，不送）
            事故 API（告警中心／建單）、自動修復（Conductor）
```

**進來的只有一條、出去的有三條**（對照公司的架構圖，aiops-lite 是圖上的 Alert Bot；Alertmanager 入口沒有接）：

| 方向 | 管道 | 現況 |
| --- | --- | --- |
| 進 | Grafana 告警的 Webhook | 有 |
| 出 | Grafana 註記（事故卡與根因分析） | 有 |
| 出 | 事故 API：事故開單、內容更新、結案時 POST 一筆 JSON 給下游（圖上的 AlertAgent → Alert Center） | 通道做好了；沒設 `outputs.incident_api.url` 就是**佔位** |
| 出 | 自動修復：規則上貼了 `aiops_remediation=<流程名稱>` 的告警在響時，請下游跑那個流程（圖上的 Conductor Ops platform） | 通道做好了；預設 `enabled: false`，是**佔位** |

佔位的意思：不送任何東西，只把「本來會送什麼」記進資料庫的 `deliveries` 表。細節見下面「流程細節」的「出口」。

部署物：1 個常駐 Deployment（+ Service）、1 個帶 PVC 的 PostgreSQL、1 個放整份 config.yaml 的 Secret（`deploy/helm/aiops-lite`）。

**什麼算異常由各系統自己的 Grafana 告警規則決定**：規則 firing 就是異常，平台不重放查詢、不自己判數值（原本的 5 個偵測器已拿掉）。

**全部的輸入與輸出、每一項的最低要求與最理想的狀態，見 `INPUTS.md`。**

## 工廠與傳統可觀測性一起用

三種訊號各走各的路，最後合成同一份證據交給 LLM：

| 訊號 | 工廠（老舊系統、沒有埋點） | IT 服務（有埋點） | aiops-lite 怎麼用 |
| --- | --- | --- | --- |
| 指標 | 日誌轉指標（Grafana 規則對日誌算次數） | 原生 metrics（VictoriaMetrics） | 兩者都是 Grafana 告警，Webhook 進來的格式一樣 |
| 關聯 | **批號／載具**把各系統的日誌串成時間線 | **trace id** 向 VictoriaTraces 或 Tempo 取回呼叫鏈 | 證據第 5 包（時間線）、第 6 包（呼叫鏈） |
| 日誌 | VictoriaLogs 或 Elasticsearch | VictoriaLogs 或 Elasticsearch | 證據第 3、4 包（錯誤日誌樣本、識別碼出現次數） |

- **來源可以選**：`logs.type` 是 `victorialogs`（預設）或 `elasticsearch`；`traces.type` 是 `victoriatraces`（預設）或 `tempo`。
  兩者各選各的（例如 Elasticsearch 配 VictoriaTraces）。兩種來源回的資料由程式轉成同一種形狀，分析的程式不分來源；
  測試把同一批資料寫進兩邊，比對過整包證據是一樣的。
- **批號時間線**：從錯誤日誌裡挑出現最多次的批號與載具，回日誌的來源把它們在各系統的紀錄（不限等級、不限系統）照時間排出來。
  它只說明先後，不說明因果；靠各系統的時間戳排序，系統之間時鐘差幾秒，順序就可能是錯的。
- **呼叫鏈**：經 Grafana 的 Jaeger 型別資料源（後面是 VictoriaTraces）或 Tempo 資料源取回失敗請求的整條呼叫路徑，標出「底下沒有其他失敗呼叫」的那個失敗點。
  錯誤日誌帶 trace id（`traces.trace_id_field`，預設 `trace_id`）時先用編號取；不夠才搜尋。VictoriaTraces 是每個系統問一次
  「有 error 標籤的 Trace」，回來的就是完整的 Trace；Tempo 是一次 TraceQL 搜尋，只回編號，要用到的才逐條取。
  Tempo 收到 Trace 後要 8～40 秒以上才搜尋得到（用編號取則立刻取得到），所以用 Tempo 時日誌最好帶 trace id。
- **同一張事故**：工廠與 IT 服務的告警只要關係邊連得起來（例如 `[mes-api, cim-eap]`）就併成一張。兩邊的日誌要用同一套欄位名稱。
- **日誌或呼叫鏈裡出現的元件算有憑據**：例如日誌寫連不上某個資料庫、或呼叫鏈指到它，就算它沒有告警、也不在關係邊裡，LLM 可以點名它；憑空編的照樣被擋。

- **指標走勢**（第 7 包，選用）：事故時經 Grafana 的 Prometheus 型別資料源（指到 VictoriaMetrics）查各系統的數值（`metrics.queries` 設定的查詢），
  壓成「事故前的基準、事故後的最小／最大／最後值、相對基準的倍數」。只做算術、沒有門檻，不判斷算不算異常。
  它補的是日誌與 Trace 看不到的情況，例如 MQ 的消費者不見了、流量暴增。

設定：`logs.type`、`logs.timeline_fields`、`traces.type`、`traces.datasource_uid`、`metrics.*` 等，見 `CONFIG.md`。Loki 沒有接。
設定檔還留著最早的 `es`／`tempo` 區段的話，啟動時會報錯並說要改成 `logs`（加 `type: elasticsearch`）／`traces`（加 `type: tempo`）。
要抓「還沒超標但走勢不對」，在 Grafana 規則用變化率、預測、跟之前比這類查詢，寫法與實測值見 `INPUTS.md`；平台本身不做異常偵測。

## 整合的單位：以 `service` 為準，一個 Grafana 一份

**事故以 `service`（系統名稱）為單位收斂，不是以 cluster、namespace、pod 或機台。**
收斂只看三樣東西：告警上的 `service` 標籤、程式觀察到的系統關係（見「註記怎麼讀」裡的說明，沒有手寫的設定）、目前有沒有進行中的事故。告警上其他的標籤收斂時都不看；
機台、批號、pod 這類識別碼只出現在日誌裡，用來統計與做時間線，不影響事故怎麼分。

| 情況 | 結果 |
| --- | --- |
| 兩個 cluster 都有叫 `mes-api` 的服務，各自告警 | **併成同一張事故**（`service` 的值一樣） |
| 同一個服務有幾十個 pod 各自告警 | 併成一張，pod 不會獨立成事故 |
| 同一個系統底下好幾台機台各自告警 | 併成一張，機台只出現在日誌統計裡 |

**部署單位**：一個 aiops-lite 對一個 Grafana，只用一把 Grafana Token、收該 Grafana 送來的 Webhook，而且只能跑 1 個副本。
不是一個 cluster 一份，也不是一個服務一份。有多個廠區或 cluster 各有自己的 Grafana 時，要各部署一份。

**多個 cluster 或廠區共用同一個 Grafana 時**，有三種做法：

1. **把 cluster 寫進 `service` 的值**（例如 `prod-a/mes-api`）：不用改程式；代價是日誌的 `service.name` 與 Trace 的服務名稱也要照這個名稱，程式才對得起來。
2. **每個 cluster 各部署一份 aiops-lite**，各自用該 cluster 的告警路由（通知路由的條件要多一個 cluster 標籤）：隔離最乾淨，要多維護幾份。
3. **讓收斂把 cluster 標籤也算進去**：要改程式，**目前沒有做**。

要在上線前決定用哪一種，因為選了之後告警、日誌、Trace 的服務名稱都要照著寫。

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
| `src/grafana.py` 的 `search_errors`、`search_error_traces` | 用這個值去 VictoriaLogs、VictoriaTraces 撈證據 |

**只換收斂單位：改設定就可以，不用改程式。** 把 `labels.service` 改成 `function` 或 `cluster`，
告警貼上對應的標籤，日誌與 Trace 的服務名稱也用同一層的名稱。同一個 function 或 cluster 的告警就會併成一張事故。

**證據要跟著對得上，否則會撈不到：**

| 證據 | 換單位之後 | 怎麼處理 |
| --- | --- | --- |
| 錯誤日誌、識別碼統計（VictoriaLogs） | 程式拿分析單位的值去比對日誌的 `logs.fields.service` 欄位（預設 `service.name`） | 日誌裡有 `function` 或 `cluster` 欄位的話，把 `logs.fields.service` 改成那個欄位 |
| 批號／載具時間線（VictoriaLogs） | 用批號、載具編號查，不用分析單位篩選；但批號是從上一列的錯誤日誌裡挑的，而且每筆事件顯示的「系統」也讀 `logs.fields.service` | 上一列對得上，這包就有；改了欄位後時間線顯示的會是 function 或 cluster 名稱 |
| 呼叫鏈（VictoriaTraces） | 搜尋時拿分析單位的值當 Trace 的服務名稱（`service.name`），設定改不了 | **目前做不到**：這包證據會是空的，其他證據與 RCA 照常。日誌有 trace id 欄位的話，用編號直接取的那幾條不受影響 |

**限制：一次只能有一層。** 一張事故只記一個維度的名單，沒辦法「以 service 收斂、同時再往上看 function 或 cluster」。
要多層同時分析得改程式（告警多帶幾個維度、收斂鍵可選、事故記錄各層名單，會動到 `grouping.py`、`db.py`、`report.py`），**目前沒有做**。

**沒有驗證的**：以上是依程式的寫法整理的，改用 function 或 cluster 當單位的設定沒有實際跑過。

## 根因分析做得到什麼、做不到什麼

| | |
| --- | --- |
| **做得到** | 指出哪個系統出了什麼錯、錯誤訊息是什麼、哪些錯誤是這次才出現的、集中在哪台機台或批號、失敗的呼叫停在哪裡、數值怎麼變壞的；列出 1 到 3 個可能的原因，各給一個信心分數 |
| **做不到** | 證明因果。它是把證據整理好之後請模型推論，所以固定標 `[LLM 推論]`。日誌、Trace、指標裡都沒有線索的事，它推不出來 |
| **證據很少時** | 仍然會分析：只有告警時，根據告警的名稱、數值、先後與關係邊列出可能的原因，分數給在 39 以下，並說最該補哪一種證據 |

它比較接近「把散在各處的線索整理成一段話，並排出嫌疑順序」，不是自動找出根因的系統。

## 註記怎麼讀

主角是根因分析：一句結論，底下列出 1 到 3 個可能的原因，每個最後帶信心分數。證據的狀況只在最後用一行補充。

有證據時（本機實測的實際內容）：

```
【P1・進行中】受影響系統：amhs-mcs、cim-eap、mq-broker｜收斂告警 3 筆
根因分析｜[LLM 推論] 最可能是 mq-broker 的磁碟空間耗盡並阻塞佇列，連帶影響 cim-eap 發布及 amhs-mcs 搬送。
1. mq-broker：mq-broker 的新錯誤明言「no space left on device」和「producers blocked」，且同系統有佇列告警，直接指向磁碟空間耗盡。（信心 95）
2. cim-eap：cim-eap 也可能有發布端異常；新錯誤寫「publish to eap.events timed out」，但較像受 mq-broker 佇列阻塞影響。（信心 48）
3. amhs-mcs：amhs-mcs 的新錯誤寫「no dispatch command received」，可能有搬送端問題，但也可能是上游未送達指令所致。（信心 25）
證據｜已有告警 3 筆、這些系統之間的關係邊 2 條、錯誤日誌 4 種、事故前後的次數比較 4 種錯誤（4 種事故前沒出現過）、…；沒接呼叫鏈、指標走勢
2026-11-25 20:36 ～ 進行中（台灣時間）｜調整優先權：MQ queue depth（P1）、…
```

只有告警時，仍然會分析，只是分數低：

```
【P2・進行中】受影響系統：test-svc｜收斂告警 1 筆
根因分析｜[LLM 推論] 目前只能推測 test-svc 的錯誤率告警反映其自身異常；最該補錯誤日誌，以確認新錯誤及實際源頭。
1. test-svc：「High error rate」告警在 test-svc 觸發，僅能提示該服務可能有錯誤率升高；缺少錯誤日誌，無法確認原因。（信心 30）
證據｜已有告警 1 筆；缺錯誤日誌（這段時間有 13 筆日誌，但沒有一筆的 service.name 是這些系統。檢查欄位名稱，以及值是否跟告警的系統名稱完全一樣）；沒接呼叫鏈、指標走勢
```

（下面這張圖是換成 Victoria 之前截的，版面一樣；括號裡的用字當時是「索引有 10 筆」，現在是「這段時間有 N 筆日誌」。）

![Grafana 上的實際畫面](demo/images/5-evidence-lines.png)

| 行 | 誰寫的 | 內容 |
| --- | --- | --- |
| `根因分析｜[LLM 推論] …` | 模型 | 一句結論。標 `[未經分析]` 時沒有經過模型（見下面「LLM 不通的時候」） |
| `1.`～`3.` 系統：說明（信心 N） | 模型列、程式檢查 | 可能的原因，由最可能排到最不可能。說明寫它可能出了什麼事、依據是哪一種證據裡的什麼 |
| `下一步建議｜1) …；2) …` | 模型寫、程式檢查 | 1～3 條值班人員接著可以確認或處理的事（看哪個元件、查哪個欄位）。只有經過模型分析時才有；提到證據裡沒有的機台批號的那一條會被拿掉。**只是給人看的文字，程式不會照著做任何事**，也不會觸發自動修復 |
| `證據｜已有…；缺…（原因）；沒接…` | 程式 | 補充：這次分析手上有什麼。缺的附原因（要動手處理的），沒接的只列名稱 |

**信心分數是什麼**

| 分數 | 意思 |
| --- | --- |
| 70～100 | 有直接指向它的證據，而且沒有別的合理解釋（它自己這次才出現的錯誤、呼叫鏈停在它、它的指標先變壞、別的系統的錯誤訊息指名連不上它） |
| 40～69 | 有指向它的證據，但還有別的可能 |
| 0～39 | 只是從告警的名稱、數值、先後或關係邊推測，沒有直接證據。模型自己標成「推測」的一律在這一段 |

- 根因分析講的是**這個問題為什麼會發生**（哪裡出了什麼事、怎麼造成現在的現象），不是每次都在指哪個系統是源頭。有多少證據就分析多少。
- 每一個可能的原因，模型要標它是「證據直接寫的」還是「推測」。標成推測的，程式把分數壓在 39 以下，註記上寫 `（推測，信心 30）`。
- 沒寫關係邊、日誌沒有機台或批號欄位，都不算缺證據，註記不會提；有就拿來用。
- 分數是模型自己給的，代表「依據有多直接」。**它不是機率，也沒有校準過**：95 分不代表一百次裡對九十五次。
- 程式只把關一件事：那個系統的名稱沒有出現在日誌、警告、Trace、時間線、呼叫鏈或指標裡（只在告警與關係邊裡），
  分數最高只到 30。所以只有告警的事故，分數一定都在 30 以下。
- 模型沒給分數、或給的不是數字，當成 0，程式不替它猜。
- **系統之間的關係全部由程式學，沒有手寫的 `edges`。** 三個來源，看到才記，存在資料庫的 `observed_calls` 表，各自標來源：
  1. **Trace 資料庫自己的 service graph**（對方有開才有）：
     - Tempo：開了 metrics-generator 的話，把存指標的資料源 uid 填進 `traces.service_graph_datasource_uid`。有誰呼叫誰、這段時間幾次請求、幾次失敗；
       沒埋點的資料庫、經過訊息佇列的消費者也在上面。
     - VictoriaTraces：啟動時加了 `-servicegraph.enableTask=true` 的話自動讀（同一個 `traces.datasource_uid`，不用另外設）。
       只有誰呼叫誰：沒有失敗數，沒埋點的資料庫不在上面，它回報的呼叫次數會重複計算所以不用。
  2. **事故的呼叫鏈**：撈到的 Trace 裡的父子關係。
  3. **錯誤日誌**：某個系統的錯誤訊息原樣提到另一個已知系統的名稱（比前兩種弱，方向不一定是呼叫）。
- **什麼時候併成同一張事故**：兩個系統都在告警，而且這段時間它們之間**有失敗的呼叫**（service graph 上有失敗數、或 Trace 裡那一段自己失敗），
  或其中一個的錯誤日誌指名另一個。只是「平常有呼叫關係」不併，所以大家都連的元件不會把不相干的告警拉成一張。
  什麼都沒看到的系統各開一張。
- **訊息佇列**：消費端的那一段標成「經訊息佇列」，這種關係方向反過來交給 LLM（發送端出問題會影響消費端；一般呼叫是被呼叫的影響呼叫它的）。
  兩種接法都接得起來：父子接法（消費端接在同一條 Trace 上）直接看得到；**連結接法**（消費端另開一條 Trace、只用連結指回發送的那一段）
  會順著連結把發送端那一條取回來，一條 Trace 最多追 3 條。只追「失敗的那一條連回去的發送端」，不會從發送端往下找其他消費者。
  實測：Tempo 2.9.0 與 VictoriaTraces v0.5.0 的回應格式都錄了樣本；完整流程（告警到註記）只在 Tempo 跑過。
- **給 LLM 當線索**：跟事故的系統有關的關係附在證據第 2 包，帶來源與失敗數。只因為有呼叫路徑而被點名的系統，分數最高 30。
- 限制：沒有 Trace、日誌也沒提到別人的系統，不會跟任何人併；  從日誌學只認得已經看過的系統名稱；不從「常常一起告警」學。
學到的關係 30 天沒再看到就丟掉（`traces.graph_retention_days`）。
- **關係邊是加分的，不是白名單。** 模型可以點名三種東西：有告警的系統、關係邊或呼叫鏈裡的系統，以及**名稱原樣出現在證據文字裡的元件**（不分大小寫）。例如日誌寫 `Cannot connect to MariaDb`，MariaDb 沒有告警、圖上也沒有，照樣可以列為可能的原因。
- 證據裡完全找不到的名稱：它是排第一的可能原因、或結論的句子提到它，整份分析退回 `[未經分析]`；只是排在後面的可能原因，就只拿掉那一個，其餘照用。

**LLM 不通的時候**（連不上、逾時、回錯誤碼，或回答沒通過檢查）：流程照走，註記改成這樣，LLM 恢復後下一次重試就換回分析結果。

```
根因分析｜[未經分析] LLM 目前連不上（連續失敗 2 次，約 2 分鐘後重試）。以下是程式收集到的證據，沒有經過推論：
・告警：mq-broker：MQ queue depth；cim-eap：CIM T3 timeout
・mq-broker 新出現：disk usage 98% on /data, producers blocked（事故期間 1 筆，事故前 0 筆）
・呼叫鏈停在：mes-api → mes-db，connection refused: mes-db:5432
證據｜已有告警 2 筆、…
```

- 底下那幾行是程式直接貼的原文與筆數，最多 5 行：第一行是哪些告警響了，接著是錯誤種類（事故前沒出現過的排前面），有呼叫鏈的話最後一行是最底層的失敗點。
- 它們**沒有經過模型**，所以標的是 `[未經分析]`，不是 `[LLM 推論]`；順序不代表誰是源頭。
- 回答沒通過檢查時，第 2 行寫的是「LLM 的回答沒有通過檢查」。沒有設定 LLM 不算故障，第 2 行只有原始的告警清單，不列這幾行。

**證據那一行**

- 缺錯誤日誌時括號裡會寫是哪一關沒過：這段時間一筆日誌都沒有、有日誌但系統名稱對不上、系統有日誌但沒有一筆算錯誤。
- 「事故前後的次數比較 N 種錯誤（M 種事故前沒出現過）」：事故前就一直有的錯誤是平常的雜訊，不會被當成這次的原因。
- 每種證據少了會看不出什麼（關係邊：誰影響誰；錯誤日誌：誰出了什麼錯；呼叫鏈：失敗的請求停在哪；指標走勢：數值怎麼變壞），
  這份完整的說明只交給模型，讓它在結論裡講最該補哪一種；註記上不重複。
- 還沒收集過證據的事故（例如 LLM 退避中才成立的）沒有列點與證據那一行。

## 錯誤日誌交給 LLM 之前的處理

直接把撈到的錯誤日誌丟給模型，很容易被「平常就在噴的錯誤」洗版。程式先做四件事（都是歸類與計數，沒有任何「算不算異常」的門檻）：

| 處理 | 做法 | 為什麼 |
| --- | --- | --- |
| 同類去重 | 把訊息裡的數字、UUID 換成 `*` 當成樣板，同一種錯誤只留最早的一筆**原文**，附上同類的筆數 | 五十行重複的日誌變成一行；原文保留，機台與批號不會被洗掉 |
| 各系統分開取最早的幾筆 | 除了最新的那一批，每個系統各自再取最早的 3 筆 | 某個系統大量報錯時，別的系統較早、較少的錯誤不會被擠掉 |
| 事故前後的次數 | 主要的幾種錯誤，向 VictoriaLogs 問它在事故期間與事故前 30 分鐘各出現幾筆（一次查詢） | 分辨平常就有的與新出現的。只給數字，不貼「暴增」「雜訊」的標籤 |
| 警告 | 同一段時間裡等級是 WARN 的日誌，同樣歸類與計數 | 出錯前的警告常常寫出原因（資源快用完、重試變多） |

每張事故多三次日誌查詢（各系統最早的錯誤、警告、計數；都經 Grafana）。日誌沒有等級欄位時不撈警告。這些改動有沒有用是量出來的，見 **`EVAL.md`**。

## 為什麼要等（debounce）

收到 Webhook 的第 0 秒就分析，LLM 拿到的是殘缺的證據：

- 上下游的告警有時間差：上游出事後下游可能幾十秒後才響，甚至門檻嚴的下游先響。太早分析會把受害者當成根因。
- 日誌有寫入延遲（收集器緩衝；VictoriaLogs 收到後也要幾秒才查得到）：太早查，撈不到機台與批號。
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
logs:                                                     # 整段可省略 = 沒有日誌證據
  type: victorialogs                                      # victorialogs 或 elasticsearch
  datasource_uid: ""                                      # Grafana 上那個資料源的 uid
  id_fields: [eqp_id, lot_id, carrier_id]
  fields: {service: service.name, level: log.level}       # elasticsearch 另外可以設 time、message（預設 @timestamp、message）
  # extra_filter: '{namespace="factory"}'                 # 只有 victorialogs：只看一部分日誌時填 LogsQL 條件
  # index: "factory-logs-*"                               # 只有 elasticsearch：要查的索引；不寫 = "*"
traces: {type: victoriatraces, datasource_uid: ""}        # 整段可省略 = 沒有呼叫鏈證據；type 是 victoriatraces（Jaeger 型別資料源）或 tempo
outputs:                                                  # 整段可省略 = 兩個出口都是佔位（不送，只記進 deliveries 表）
  incident_api: {url: "", token: ""}                      # 事故 API
  remediation: {url: "", token: "", enabled: false}       # 自動修復；enabled 是 false 時就算有 url 也只記錄
metrics: {datasource_uid: "", queries: []}                # 整段可省略 = 沒有指標走勢；Prometheus 型別資料源（指到 VictoriaMetrics）的 uid
timezone: Asia/Taipei
log_level: INFO
debounce_seconds: 60      # 收到第一個 Webhook 後等多久才開始分析
followup_seconds: 60      # 有進行中事故時，沒有新 Webhook 也每隔多久再跑一輪
close_after_seconds: 300  # 成員告警全部 resolved 之後，再安靜多久才結案
```

沒有 `edges` 這個設定了：系統之間的關係由程式自己學（設定檔裡還寫著的話啟動會報錯）。

- **進階調整**（逾時、重試、日誌筆數上限、錯誤等級的值、給 LLM 的樣本數、提示詞、幻覺檢查樣式、標籤名稱、健康檢查門檻…）
  也都在這份檔案，全部選填；`config.example.yaml` 裡用 `#` 註解掉的就是。完整清單、預設值、範圍，以及哪些東西刻意不開放設定，見 **`CONFIG.md`**。
- **啟動時嚴格檢查**：檔案不存在、鍵名打錯（任何一層）、型別不對、必填沒填，直接報錯、不啟動。數字開頭的密鑰要加引號。
- **日誌的系統與等級欄位名稱可以改**（`logs.fields`）：查詢條件、讀回來的內容都照這裡的名稱；沒寫的用預設值。
  時間與訊息在 VictoriaLogs 固定是 `_time`、`_msg`（寫入時指定哪個欄位當時間、哪個當訊息），不用也不能在這裡設；
  Elasticsearch 才能設 `logs.fields.time`、`logs.fields.message`（預設 `@timestamp`、`message`）。
  日誌沒有等級欄位時，`logs.fields.level` 可以設成 `""`，改成在訊息文字裡找 error、fatal 這些完整的詞（過渡做法：
  INFO 的訊息寫到 error 這個詞也會被挑進來，見 `CONFIG.md`）。
- **只看一部分日誌**：VictoriaLogs 沒有索引的概念，用 `logs.extra_filter` 填一段 LogsQL 條件，它會加在每一條日誌查詢的最前面；
  Elasticsearch 用 `logs.index` 指定索引。兩個設定各自只在對應的 `logs.type` 下能用，設錯組合啟動時報錯。
- **最早的 `es`／`tempo` 區段會讓服務啟動失敗**，錯誤訊息會說改成 `logs`（加 `type: elasticsearch`）／`traces`（加 `type: tempo`）。
  `log_index` 改叫 `logs.index`、`log_fields` 改叫 `logs.fields`；`trace_index`、`trace_fields`、`error_statuses`、
  `llm.max_trace_samples` 已經沒有了。
- **檔案裡有 Token 與密碼，不要推上 Git**：`config.yaml` 已在 `.gitignore` 與 `.dockerignore`，專案裡只有 `config.example.yaml`。
  設定被印出來（log、例外訊息）時，Token、密鑰、連線字串不會出現。
- 實際看到註記的延遲 ≈ 規則的 `for` + Grafana 路由的 `group_wait` + `debounce_seconds` + LLM 回應時間。

## 檔案

| 檔案 | 行數 | 負責內容 |
| --- | --- | --- |
| `src/server.py` | 218 | 常駐服務：收 Webhook（Bearer 驗證、先落地再回 200）、debounce 排程、單一工作執行緒、續跑 |
| `src/main.py` | 298 | 「一輪」的流程：照設定建來源物件、學系統關係、收斂、叫 rca、寫註記、叫 outputs |
| `src/config.py` | 393 | 讀 config.yaml（嚴格檢查：鍵名、型別、範圍、必填） |
| `src/grafana.py` | 195 | 解析告警 Webhook、對 Grafana 的 HTTP 呼叫（重試、錯誤訊息）、指標查詢、寫註記 |
| `src/logs.py` | 272 | 日誌的兩種來源（VictoriaLogs、Elasticsearch）：同一組查詢方法，回同一種形狀 |
| `src/traces.py` | 151 | Trace 的兩種來源（VictoriaTraces、Tempo）：Tempo 的 OTLP 轉成跟 VictoriaTraces 一樣的形狀 |
| `src/outputs.py` | 157 | 註記以外的兩個出口：事故 API、自動修復；佔位、不重送、失敗重試 |
| `src/graph.py` | 100 | 系統之間的關係：讀 service graph（Tempo 指標、VictoriaTraces 相依關係）、從日誌找提到的系統、判斷哪些連結可以合併事故 |
| `src/grouping.py` | 172 | 同實體合併、拓撲鏈收斂、併入進行中事故、嚴重度只升不降、全部 resolved 且安靜夠久才結案 |
| `src/evidence.py` | 654 | 收集證據（告警、系統關係、日誌、識別碼統計、批號時間線、呼叫鏈含訊息佇列的連結、指標走勢）與證據概況 |
| `src/rca.py` | 378 | 不寫推理演算法：組 Prompt、呼叫 LLM、檢查回答（幻覺檢查、信心上限、下一步建議）、失敗退回原始摘要 |
| `src/report.py` | 65 | 註記的固定格式（標題、摘要、證據的幾行、時間與調整優先權連結；台灣時間、HTML 跳脫） |
| `src/db.py` | 286 | SQLAlchemy Core（SQLite／PostgreSQL），啟動自動建表：待處理告警、事故、每輪執行紀錄、出口的處理紀錄 |

合計約 3,339 行。

## 各系統要做的事（平台零直連）

1. 把資料送進公司 Grafana 看得到的資料源：MQ 掛 Prometheus exporter；CIM 用日誌收集器做 Log 轉指標；AMHS 開唯讀 View 給
   Grafana 的 SQL 資料源。錯誤日誌進 VictoriaLogs（Fluent Bit 的 http 輸出打 `/insert/jsonline`；巢狀的 `service: {name: …}` 進去後會攤平成 `service.name`）或公司既有的 Elasticsearch，帶 `service.name`、`log.level`、`message`、`@timestamp`，以及有的話 `eqp_id`／`lot_id`／`carrier_id`。
2. 在 Grafana 建告警規則（閾值自己定）並貼標籤：`aiops_detect=true`、`aiops_severity=P0～P3`、`service=<系統名稱>`
   （要跟關係邊裡的名稱一樣）。要讓這條告警可以觸發修復流程，再加 `aiops_remediation=<流程名稱>`（選用，見「流程細節」的「出口」）。這幾個標籤名稱是預設值，可以在 config.yaml 的 `labels` 改。沒有 `service` 的告警會被略過並回報給送出端；`aiops_severity` 寫錯當成 P3。

系統沒有 Metrics（老舊的 CIM、AMHS、MQ）時怎麼把日誌或資料庫紀錄變成告警規則，見 **`FACTORY_METRICS.md`**。
Fluent Bit 的設定範例在 `deploy/fluent-bit/`。
Demo 的 Grafana 實際畫面（依「降噪、找源頭、鎖定機台批號、自動結案」四個重點排列，含說明）在 **`demo/README.md`**。

## Grafana 要做的設定

**都在公司 Grafana 的網頁介面（或它的 API）上做；不改 Grafana 映像檔、不改規則的閾值。**
**唯一的例外是一個外掛，而且只有日誌放在 VictoriaLogs 時才需要**：Grafana 沒有內建 VictoriaLogs 的資料源，要請 Grafana 管理員裝
`victoriametrics-logs-datasource`（實測 0.34.0）。日誌放在 Elasticsearch 的話不用裝任何東西；Trace（Jaeger、Tempo）與指標用的也都是 Grafana 內建的資料源型別。
這個 repo 不部署 Grafana，用的是公司既有的那一套。

| # | 項目 | 在哪做 | 怎麼做 |
| --- | --- | --- | --- |
| 1 | 服務帳號與 Token | Administration → Users and access → Service accounts | 新增角色 **Editor** 的帳號 → Add token → 填進 config.yaml 的 `grafana.token` |
| 2 | 告警規則標籤 | Alerting → Alert rules → 編輯規則 → Labels | 每條要納管的規則加 `aiops_detect=true`、`aiops_severity=P0～P3`、`service=<系統名>` |
| 3 | **Contact Point**（新增 1 個） | Alerting → Contact points → Add | 類型 Webhook；URL `http://<release>.<namespace>.svc:8080/webhook`；Authorization scheme `Bearer`、credentials 填 config.yaml 的 `webhook.token`；**不要**勾 Disable resolved message |
| 4 | **Notification policy**（新增 1 條路由） | Alerting → Notification policies → New child policy | Matcher `aiops_detect = true` → 上面的 Contact Point；`group_wait` 設短（0～10 秒，平台自己會等 debounce）；要同時保留原本的通知就勾 Continue matching |
| 5 | 儀表板 | Dashboards → New → Import | 貼上或上傳 `dashboards/aiops-overview.json`（事故清單：進行中、P0、P1、已結案；只用內建註記清單面板，不需要資料源）。另有 `dashboards/aiops-incident-metrics.json`（四張指標曲線，事故區間自動疊成紅色；匯入時選日誌的資料源，查詢要照 `FACTORY_METRICS.md` 的欄位規範，面板的查詢可自行改成各系統的指標） |
| 6 | **資料源**（要哪一種證據才需要哪一個） | Connections → Data sources → Add | 日誌：型別 **VictoriaLogs**（要先裝外掛），URL 填 VictoriaLogs 的位址；或用既有的 **Elasticsearch** 資料源。Trace：型別 **Jaeger**，URL 填 VictoriaTraces 的位址加 `/select/jaeger`；或用既有的 **Tempo** 資料源。指標：型別 **Prometheus**，URL 填 VictoriaMetrics 的位址。已經有的就不用再建 |
| 7 | 資料源的 uid | Connections → Data sources → 點進該資料源 | 從網址 `/connections/datasources/edit/<uid>` 取得，填進 config.yaml 的 `logs.datasource_uid`、`traces.datasource_uid`、`metrics.datasource_uid`；日誌不是 VictoriaLogs 就設 `logs.type: elasticsearch`，Trace 不是 VictoriaTraces 就設 `traces.type: tempo` |

建好之後用同一把 Token 確認查得到日誌（下面是 VictoriaLogs 的寫法；回來的是一行一筆的 JSON；一行都沒有就是時間窗內沒有這個系統的日誌，或欄位名稱對不上）：

```bash
curl -s -H "Authorization: Bearer $TOKEN" "$GRAFANA/api/datasources/proxy/uid/$UID/select/logsql/query" \
  -d 'query="service.name":="cim-eap" | sort by (_time) desc | limit 3' \
  -d "start=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)" -d "end=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

### 注意

1. **規則、Contact Point、路由若是用 provisioning（YAML、Terraform）管的**，在 UI 上改會被下次部署蓋掉，要加進原本那份檔案。
2. **Grafana 要連得到 aiops-lite 的 Service**（同叢集用 Service 名稱；不同叢集要自己開路）。
3. **RBAC 只在預設權限下驗過**：Editor 能走三個資料源的代理、寫註記，是在 Grafana 13.0.7 預設權限下確認的。
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
  摘要文字裡夾數字的識別碼在證據裡找得到；不通過就標 `[未經分析]`，並直接列出收集到的證據。
- **LLM 失敗會指數退避**：分析失敗（連不上、逾時、回應格式不對、幻覺檢查沒過）後，等 `llm.retry_initial_seconds`（預設 60）才再試，
  之後每失敗一次加倍（60、120、240…），最多 `llm.retry_max_seconds`（預設 1800）；成功就歸零。退避期間**不查日誌、不打 LLM、不重寫註記**
  （續跑的輪次照常跑，只是跳過這一步）；期間有新告警併進來，註記上「告警」那一行會跟著更新，到時間再帶完整成員重試。
  退避狀態存在資料庫，服務重啟不會重置。沒設 LLM（`llm.url` 留空）不算失敗、不退避。**不需要為了止損去清空 `llm.url`**。
- **註記標籤變了就刪掉重建**：事故結案（open → closed）、嚴重度升級、有新系統加入時，註記的標籤會變。Grafana 13.0.7 實測：更新註記的標籤之後，用**舊標籤**查還是查得到它，儀表板的「進行中」清單會一直列著已結案的事故（用真的 Grafana 跑 demo 時發現的）。所以標籤變了就刪掉舊的、建一筆新的（註記 id 會換），只有文字或時間變才原地更新。
- **寫回**：先寫自己的資料庫，再寫註記；一個事故一筆註記（標籤 `aiops`、`aiops-incident:<uid>`、嚴重度、`open`／`closed`、
  各系統名稱），之後原地更新。註記寫失敗下一輪補，不會重複建。
- **出口**（`outputs`，註記以外的兩個；處理紀錄都在資料庫的 `deliveries` 表，跨重啟保留）：
  - **事故 API**（`outputs.incident_api.url`／`token`）：事故開單（`opened`）、內容更新（`updated`）、結案（`closed`）時 POST 一筆 JSON
    （`schema: aiops-lite.incident.v2`：事故、成員告警、根因分析的摘要、可能原因與下一步建議（`root_cause.suggestions`，v2 新增）、證據那一行、Grafana 註記的 id 與標籤）。
    同樣的內容只送一次；失敗每一輪再試、最多 10 次，不影響註記。
  - **自動修復**（`outputs.remediation.url`／`token`／`enabled`）：規則上貼了 `aiops_remediation=<流程名稱>` 的告警在響時 POST 一筆請求
    （`schema: aiops-lite.remediation.v1`：流程名稱、系統、那條告警、事故 uid，根因摘要只當說明附上）。
    同一張事故、同一條告警、同一個流程只送一次；只對還在響的告警、還沒結案的事故送；預設 `enabled: false`。
    **跑哪個流程只看規則上的標籤，是人事先寫好的；LLM 的推論不會觸發、也不會選流程。**
    規則有貼標籤時，註記多一行 `自動修復｜restart-mq-consumer（mq-broker：已送出）`，狀態是「已送出／佔位，沒有送出／送出失敗，會再試」其中之一。
  - **佔位**：`url` 留空（或自動修復沒有 `enabled`）就不送，只把本來會送的內容記進 `deliveries` 表（`status = placeholder`）。
    佔位記過的之後不補送：之後才把出口打開，只對打開之後的事故與內容生效。目前還沒有真的下游系統，所以這是預設。

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
logs:     {datasourceUid: xxx}                                          # 可省略；traces、metrics 同樣寫法
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

全部是本機拋棄式環境、腳本產生的資料。**沒有任何一項用過產線資料。**

| 項目 | 怎麼驗的 | 結果 |
| --- | --- | --- |
| 自動測試 | 345 個，SQLite 與 PostgreSQL 16 各跑一遍。Grafana、VictoriaLogs、VictoriaTraces、Elasticsearch、Tempo、Prometheus 的回應是實際錄下來的（`tests/fixtures/grafana13/`）；假的 Grafana 照查詢條件篩資料，篩出來的結果逐條跟真的 VictoriaLogs 比過 | 全過 |
| 兩種日誌來源給的證據一樣 | 同一批資料寫進 VictoriaLogs v1.17.0 與 Elasticsearch 8.19.23，經 Grafana 用實際的查詢程式各收集一次 | 每一種查詢的結果、整包證據都一樣（只差時間的小數寫法）。Tempo 2.9.0 與 VictoriaTraces 收到的同一條 Trace，轉出來的呼叫鏈也一樣 |
| 三個 Victoria 元件經 Grafana 查得到 | 2026-11-26。Grafana 13.0.7 + VictoriaLogs v1.17.0 + VictoriaTraces v0.5.0 + VictoriaMetrics v1.110.0（單機版官方執行檔），Editor Token 經資料源代理 | 三者都通 |
| 完整流程：三系統連鎖 | 真的服務、Webhook、三個 Victoria 元件、真的 LLM（各問一次）。日誌 + VictoriaMetrics 的佇列指標 | 收成一張事故；排第一是 mq-broker（信心 95）；證據那一行列出指標走勢 1 條 |
| 完整流程：系統名稱對不上 | 同上，日誌的 `service.name` 寫成別的名字 | 註記寫出「這段時間有 13 筆日誌，但沒有一筆的 service.name 是這些系統」 |
| 完整流程：只有 INFO／WARN | 同上 | 註記寫出「這些系統有 3 筆日誌，但 log.level 沒有一筆是 ERROR／FATAL／CRITICAL」 |
| 完整流程：有 Trace 的 IT 服務 | 同上，兩條失敗的 Trace 用 OTLP 送進 VictoriaTraces | 呼叫鏈 2 條；排第一是沒有告警的 mes-db（信心 85） |
| 完整流程：Elasticsearch + Tempo | 2026-11-26。同樣四種情況改用 Elasticsearch 8.19.23 與 Tempo 2.9.0（指標仍是 VictoriaMetrics），真的 LLM 各問一次 | 四種情況的註記都對；呼叫鏈 2 條，排第一是沒有告警的 mes-db（信心 85） |
| 兩個出口 | 只對**本機一個假的 HTTP 端點**驗過 | 事故 API 收到 4 張事故的 `opened`（帶 Bearer token）；自動修復在 `enabled: true` 時送出 1 筆（`restart-mq-consumer`），註記顯示「已送出」；`enabled: false` 時沒有送，註記顯示「佔位，沒有送出」 |
| 日誌前處理（去重、事故前後的次數、警告） | 16 個有標準答案的案例，真的 VictoriaLogs、Grafana、LLM | 見 `EVAL.md` |
| 沒有等級欄位的容器日誌 | VictoriaLogs v1.17.0，4 筆沒拆欄位的日誌；另有一個留出案例 | 撈得到，但 INFO 訊息寫到 error 的那一筆也被挑進來（`CONFIG.md`） |
| Webhook → debounce → 收斂 → 註記 → 結案 | **換成 Victoria 之前**驗的：Grafana 13.0.7 的告警規則真的觸發、真的推 Webhook；PostgreSQL 16。這段流程不碰資料來源 | 三個系統收成一張事故，恢復後結案 |
| LLM 摘要與幻覺檢查 | 真的 LLM（`gpt-6-luna`）。錯的金鑰、模型不接受的參數也測過 | 摘要寫進註記；失敗時退回 `[未經分析]`，錯誤訊息不含金鑰 |
| MQ 的呼叫鏈 | **換成 Victoria 之前**驗的：RabbitMQ 3.12.1、pika 的 OpenTelemetry 自動埋點，當時 Trace 存在 Tempo 2.9.0 | 父子接法串得起來；連結、掉標頭、沒人收串不起來（`INPUTS.md`）。沒有在 VictoriaTraces 上重跑 |
| 指標走勢（RabbitMQ 真的堆積） | **換成 Victoria 之前**驗的：Prometheus 3.5.0、RabbitMQ 內建的指標，佇列真的堆到 1007 筆 | 有指標走勢時模型指出消費端失效；沒有時說無法判定（各問一次）。VictoriaMetrics 上只用匯入的假數值跑過（上面第三列） |
| 趨勢型告警規則的寫法 | 同一份 RabbitMQ 資料上算五種查詢的值（Prometheus） | 正常期與堆積期的值見 `INPUTS.md`；沒有建成規則讓它觸發 |
| Grafana 開源版連資料庫 | Grafana 13.0.7（Open Source）連 PostgreSQL 16 | 可以（`FACTORY_METRICS.md`） |
| Helm chart | `helm lint`、`helm template`，產生的 `config.yaml` 用程式實際載入 | 通過 |

**沒有驗證的**

- **產線資料與真實的告警風暴。** 60 秒的 debounce 夠不夠、日誌的延遲多長、摘要對不對，都要現場才知道。
- **映像檔沒有在這裡 build 過，chart 沒有在這裡部署過。**
- **叢集版的 Victoria、vmauth、多租戶。** 只測過單機版、沒有認證的元件。
- **在公司的 Grafana 上裝 VictoriaLogs 資料源外掛。** 只在本機的 Grafana 13.0.7 裝過 0.34.0。
- **真的下游系統。** 兩個出口沒有跟任何真的告警中心或 Conductor 對過；送的格式是這個專案自己定的，接真的系統時幾乎一定要改欄位對應。
  認證只支援 Bearer token。修復流程跑完的結果不會回寫：只知道請求有沒有送出，不知道流程成功與否。
- **大量資料下的查詢時間。** 只在幾十筆到幾千筆的資料上跑過。特別是「錯誤日誌 0 筆時數整段時間有幾筆」那一次查詢，
  會掃整個時間窗的全部日誌（有設 `logs.extra_filter` 的話只掃那一部分）。
- **Fluent Bit 的認證、日誌輪替、斷線補送。** 三份範例設定用真的 Fluent Bit 5.2.0 送進 VictoriaLogs 跑過（欄位、時間都對），但只跑了一次、沒有帳密、沒有 TLS。
- LLM 只測過一個模型；多數情境只問了一到五次。
- 只測過 Grafana 13.0.7、Elasticsearch 8.19.23、Tempo 2.9.0；多個 Grafana 組織、Loki（沒有接）、Windows 上的 Fluent Bit 都沒有測。
- 日誌遮罩沒有做：送給 LLM 的內容是原文。

## 上線

上線前要填哪些設定、公司 Grafana 與日誌／Trace 的儲存那一側要配合什麼、還有哪些事沒驗證過，照 **`GO_LIVE_CHECKLIST.md`** 由上往下做。

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
- 呼叫鏈只看得到有埋點、沒被取樣掉的請求；日誌沒帶 trace id 時要靠搜尋，只找得到失敗有被標成 error 的 Trace；VictoriaTraces 收到後幾秒才搜尋得到（實測 3 秒），Tempo 要 8～40 秒以上。
- 時間線與呼叫鏈會讓送給 LLM 的內容變長（每個事故：每種關聯碼多 2 次日誌查詢；Trace 是每個系統搜尋 1 次，加上日誌裡的 trace id 各取 1 次）；一樣沒有遮罩。
- 指標走勢只支援 Prometheus 相容的查詢，工廠端用日誌筆數算的趨勢不會進這一包；基準是事故前的中位數，
  事故前就在慢慢爬升的話基準會被拉高。每張事故多「查詢數 × 系統數」次指標查詢。VictoriaMetrics 只用匯入的假數值實測過一次。
- 事故前後的次數是用「樣板裡固定不變的詞照順序出現」去比對的近似值（VictoriaLogs 用 LogsQL 的 `seq`，分大小寫；
  Elasticsearch 用 intervals 查詢，不分大小寫，而且訊息欄位要是 text 型別）：
  別種錯誤剛好有同樣的詞與順序時會被算在一起；比不到的就不給次數。只有一個固定詞的訊息（例如只寫 `timeout`）不比較。
- 日誌沒有等級欄位時（`logs.fields.level: ""`），錯誤日誌是用「完整的詞、不分大小寫」的比對挑的（VictoriaLogs 是 `_msg:i("error")`，Elasticsearch 是訊息欄位的全文比對）：
  INFO 的訊息寫到 error 這個詞也會中；訊息裡完全沒有等級字樣的錯誤則挑不到。
- 日誌放在 VictoriaLogs 時要裝資料源外掛；公司的 Grafana 不能裝外掛、又沒有 Elasticsearch 可用的話，就沒有日誌證據（程式不會改成直連）。
- 兩個出口的格式是自己定的，沒有跟真的下游對過；自動修復只送出請求，不追蹤流程的結果，也不會在事故結案時取消。
- 幻覺檢查比對的是「名稱有沒有原樣出現在證據文字裡」：模型把名稱改寫了（縮寫、翻成中文、補上日誌沒寫的版本號）就對不上；兩個字以下的名稱不用文字比對。結論的句子裡提到、但沒有列進 `systems` 的名稱檢查不到（機台批號樣式的除外）。
- 幻覺檢查只擋「名稱不存在」，擋不了「存在的系統被講錯因果」；所以第 2 行固定標 `[LLM 推論]`。
- 沒有 Trace 時「集中在哪台機台／批號」只是錯誤日誌的計數，不是因果證據。
- 上游沒有告警規則（或沒越線）時平台看不到它：只會看到下游的告警，不會替上游補洞。
- 進行中事故會吸收同系統或相鄰系統的新告警，不看時間間隔：一筆告警響很久沒恢復時，之後相鄰系統不相干的告警也會併進來。
- 規則的標籤改了，Grafana 會把它當成另一筆告警（fingerprint 不同）。
