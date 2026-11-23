# 端到端演練紀錄（本機單節點叢集）

演練中遇到的問題集中記在這裡，作為上線前微調的依據。只記事實與量到的數字；沒驗證的寫明沒驗證。
環境是 Rancher Desktop 的單節點 k3s：部署與封包是真的，流量來自兩個模擬服務，不代表正式環境的負載。

> **2026-11-20 註**：這份是歷史紀錄，當時平台用 `AIOPS_ES_URL`／`AIOPS_ES_API_KEY` 直連 Elasticsearch。之後平台改成只經 Grafana 讀
> trace／日誌（`aiops/sources/grafana.py`，環境變數 `AIOPS_GRAFANA_URL`／`AIOPS_GRAFANA_TOKEN`／`AIOPS_GRAFANA_TRACES_UID`／`AIOPS_GRAFANA_LOGS_UID`），
> 直連 ES 的資料源、ES 指標查詢、`values-drill.yaml` 裡的 ES 設定都不再適用。下面的內容沒有改寫，數字是直連時量的。

## 環境（2026-11-16）

- 平台：`aiops-platform:drill`（分支 `chore/e2e-drill`，從 main `ff6e545` 開出），chart `deploy/helm/aiops-platform`
  加 `values-drill.yaml`。排程跑 `it_otel`，看門狗跑 `platform_watchdog`，各 5 分鐘一輪。
- 資料庫：chart 內建的開發用 PostgreSQL（沒有持久儲存，pod 重建資料就沒了）。
- 演練服務：`rca-mes-api`（it-mes-prod）→ `rca-wip-worker`（imc-wip-prod），來源是 `aiops-data-foundation` 的
  `e2e/k8s/rca-apps.yaml`；資料經 OTel Collector 寫進 es-test。
- 機密：Secret `aiops-platform-env`（用 kubectl 建，不在 chart 裡）。

## 已驗證

| 項目 | 結果 |
| :-- | :-- |
| migration 0001～0012 在平台自動建表後的資料庫上依序跑 | 12 支都沒有錯誤 |
| 同一批 migration 再跑第二次 | 12 支都沒有錯誤（可重複執行） |
| API 與兩個 CronJob 啟動 | 第一輪 `it_otel`、`platform_watchdog` 都成功，各 5 秒，`pipeline_runs` 各一筆 `ok` |
| 平台自己的心跳 | Alertmanager 收到 `AIOpsWatchdog`（兩條管線各一筆） |

沒驗證：在「完全空、沒有自動建表」的資料庫上只靠 migration 建出全部的表（migration 檔本來就假設表已存在）。

## 問題

### 1. 看門狗在部署後第一輪就誤報兩張卡（已重現，未修）

平台剛裝好、第一輪看門狗（02:50 UTC）就發出兩張 P3 卡：

- `it_otel 異常：已持續 170 分鐘，累計 34 筆`（節點 `stopped_rounds`）
- `drill-team` 的心跳斷了（節點 `stopped_heartbeats`，從 00:35 起算）

原因：看門狗往回看 180 分鐘，平台還不存在的那段時間每一格都是 0，被當成「排程停了」「心跳斷了」。
同樣的情況會發生在每一次新接一個團隊（加了心跳 target、對方還沒開始送）與每一次全新部署。
這不是門檻的問題，是「從來沒有紀錄」與「有紀錄之後停了」沒有分開。

### 2. 平台的唯讀 ES key 讀不到 Collector 寫的索引（已解決）

舊 key 是 `tests/dev_es_key.py` 建的，那支腳本只給 APM 的索引權限，演練服務經 Collector 寫的是 OTel 索引。
腳本加上 OTel 的兩個索引（`traces-*.otel-*`、`logs-*.otel-*`）後重建一把（id `ekhf4qEBVeEBYhoR5bMx`），放進 Secret。
另外：叢集 Secret `es-test/es-elastic` 的密碼跟 ES 實際的密碼對不上（401），腳本是用 `secrets.local.yaml` 的 `admin_password` 建成的。

### 3. 接一個新團隊要改出貨的看門狗 YAML，改了測試就紅

依文件「每接一個團隊就在看門狗 YAML 的 targets 加一行」做了之後，`tests/test_watchdog.py` 有 3 個測試失敗
（測試直接讀出貨的 YAML，寫死被監控的管線是 `metrics_only`）。同樣地，把排程管線從 `metrics_only` 換成別條也要改這個 YAML。
也就是說接團隊、換排程管線都要重建映像檔，而且目前沒辦法在不動測試的情況下做。演練分支沒有改測試，這個分支不能合併。

### 5. repeat interval 剛好 15 分鐘時，故障還沒結束事故卡就結案（已重現，已修，見「第二次演練」）

演練（2026-11-16，UTC）：03:02:19 讓 `rca-wip-worker` 查資料庫變 1.5 秒並噴連線池逾時，03:26:07 移除。

| 時間 | 發生什麼 |
| :-- | :-- |
| 03:04:52～57 | 團隊 Grafana 3 條告警送達（錯誤率 critical、延遲 high、探測慢 high），全部收下 |
| 03:05:02 | 併成 1 張 P0 卡（`same_entity` + `topology_chain`），送到 Alertmanager。故障開始後 2 分 43 秒 |
| 03:20:01 | 這張卡結案（安靜滿 `resolve_after_minutes: 15`）。**故障還在** |
| 03:20:52～57 | Grafana 照 repeat interval 15 分鐘重送同樣 3 條。晚了 51 秒 |
| 03:25:02 | 開了第 2 張卡（新的 `incident_uid`），被冷卻抑制、沒有通知 |

結果：同一場故障變兩張卡，值班的人在 03:20 看到「已結案」，其實故障還在。
原因：文件寫「repeat interval 不能超過 15 分鐘」，但重送會晚 group wait 加送達時間（這次 51 秒），剛好 15 分鐘就一定踩線。
文件允許的上限本身就會失敗。

修正：晚到的 51 秒不是網路延遲。第一次送達 03:04:52、重送 03:20:52，剛好隔 16 分鐘 = repeat interval 15 + group interval 1：
Grafana 每個 group interval 才檢查一次要不要重送，所以兩次送達最多隔 repeat interval + group interval。
四條偵測管線的 `resolve_after_minutes`（`alert.absorb_open` 與 `alert.heartbeat`）從 15 改成 22 =
15（文件允許的 repeat interval 上限）+ 5（Grafana 預設的 group interval）+ 1（告警點取整到分）+ 1（送達與重試）。
最後那 1 分鐘是留的餘裕，不是量出來的（演練裡送達不到 1 秒）。文件 `docs/EXTERNAL_ALERTS.md` 改寫成這條式子。
代價：告警真的結束後，事故卡要多等 7 分鐘才結案。測試：`tests/test_e2e_drill_fixes.py`。

### 6. 外部告警的事故卡，RCA 的「事件期間」沒有任何 span（已重現，已修，見「第二次演練」）

拿到能讀 OTel 索引的 key 之後重跑，撈到 7,758 筆 span、兩個服務都有，但 `rca_report` 仍然回「無法判定：入口請求少於 20 筆」。
原因：外部告警的時間是「平台收到的那一分鐘」，3 筆都是 03:04，事故卡的開始＝結束＝03:04。
`ingestion.incident_traces` 撈到事故結束為止（`after_minutes: 0`，撈到 03:04:00），根因演算法的「事件期間」卻是 [03:04, 03:05)，
這一分鐘的 span 根本沒撈，所以事件期間是 0 筆。偵測器時代的事故跨好幾分鐘，所以以前沒碰到。
另外「事件前」的 15 分鐘（02:49～03:04）裡已經有將近 2 分鐘是故障中（故障 03:02 開始，告警 03:04 才到），基準會被污染一些。

修正：這是撈取窗少一分鐘的錯，不只影響外部告警。事件的時間點是分鐘桶的起點（03:04 代表 03:04～03:05），根因演算法的
事件期間一直是 [開始, 結束 + 1 分鐘)，`ingestion.incident_traces` 卻只撈到最後一點的起點；跨好幾分鐘的事故只少最後一分鐘，
所以沒被發現（`tests/test_rca_evidence.py` 自己切 span 時用的是 +1 分鐘，跟真的撈取不一樣，也就測不到）。
現在撈到最後一點那一分鐘的結尾。5 個把舊結尾寫死的測試斷言跟著改成 +1 分鐘。
沒修的部分：基準被故障前段污染（上面那 2 分鐘）沒有處理；事件期間只有 1 分鐘，每分鐘入口請求不到 20 筆的服務仍然會是「證據不足」。

### 7. 平台讀不到演練服務的日誌

trace 讀得到之後，日誌仍然是 0 筆（事件期間與基準都是 0）。

原因（已查到，未修）：日誌不在平台讀的索引裡。
- 服務有寫：`rca-wip-worker` 故障期間在標準輸出寫了 922 行 ERROR（`Connection pool timeout…`，帶 trace_id）。
- Collector 有收：節點 Collector 讀容器日誌檔，先寫進 `logs-generic.otel-<group>`。
- 但 Collector 的設定檔註解寫明，中央 ES 的 `k8s-logs-otel` pipeline 會把它整理形狀後轉到 `k8s-<group>-<cluster>`。
  平台的 OTel 資料模型讀 `logs-*.otel-*`，用唯讀 key 查 02:30～03:40 這個索引是 0 筆（任何服務都是 0）。
- 沒驗證：`k8s-<group>-<cluster>` 裡實際的文件長相。唯讀 key 沒有那個索引的權限，我沒有去讀。

要修需要三樣：唯讀 key 加上 `k8s-*` 的讀取權限（要再用一次 ES 管理員密碼）、平台的日誌索引改指過去（`AIOPS_ES_LOG_INDEX`）、
以及那個形狀的欄位對照（服務名、等級、訊息、trace_id 的欄位名跟 OTel 原生的不同）。還沒做，等決定。

### 8. 關係圖的「trace 聚合」那一半在叢集上是空的：chart 沒有排程去沉澱拓撲（2026-11-17 查到，已修）

兩次演練的收斂（`topology_chain`）只靠我手動匯入的那一條宣告邊（`declared_edges` 1 列，origin `drill-manual`）。
`service_edges`（從 trace 沉澱的呼叫邊）是 0 列：`admin_topology_refresh` 要有人或排程去跑，而 `deploy/helm/aiops-platform`
只排了偵測管線與看門狗兩個 CronJob，沒有它。照 chart 部署、又沒有人宣告依賴的環境，關係圖會是空的，連鎖告警不會併成一張卡。

手動跑一次（`POST /api/v1/ingestion/topology/refresh`，回看 60 分鐘）：撈到 2,616 筆 span，沉澱出 1 條邊
`rca-mes-api → rca-wip-worker`（495 次呼叫），跟宣告的那一條一致；非同步邊 0 條（演練服務沒有訊息佇列）。
所以功能本身在真實 trace 上是對的，缺的是排程。

另外對照架構圖「邊來源／最後觀測時間／圖資版本」：資料表有來源與時間（宣告邊的 `origin`、`updated_at`；trace 邊的 `last_seen`），
但組進關係圖（`engine/graph.py` 的 `Edge`）之後只剩型別與權重，RCA 報告說不出某條邊是宣告的還是觀測到的、多久前看到的；
「圖資版本」目前不存在。

修正（分支 `feat/graph-provenance`）：
- chart 多一個 CronJob `topology`，每小時跑一次 `admin_topology_refresh`（回看 60 分鐘，窗口相接）。
- 關係圖的每條邊帶出處（來源、細節、最後觀測時間），事件群快照一起存；RCA 報告多一段「關係圖依據」，
  列出這張卡用到的每條邊是人宣告的、平台從 trace 沉澱的、還是這次事件的 trace 直接看到的，以及多久前。
- 圖資版本沒有做：報告最後一行寫每種來源最新的時間代替。
- 修正前建立的事故卡，快照沒有記出處，報告會寫「來源未記錄」。

## 第二次演練（2026-11-16，修正後，映像檔 `aiops-platform:drill2`）

同一場故障（`rca-wip-worker` 查資料庫變 1.5 秒並噴連線池逾時）。演練團隊的 Grafana 改用文件允許的最壞情況：
repeat interval 15 分鐘、group interval 5 分鐘（第一次是 1 分鐘）。時間都是 UTC。

| 時間 | 發生什麼 |
| :-- | :-- |
| 06:44:27 | 注入故障 |
| 06:46:42～47 | 3 條告警送達（注入後 2 分 15 秒） |
| 06:50:02 | 併成 1 張 P0 卡（事故 5）。注入後 5 分 35 秒 |
| 07:05:01 | 這一輪沒有結案（安靜 19 分鐘 < 22）。第一次演練就是在這個位置提前結案 |
| 07:06:42～47 | 第一次重送，跟第一次送達剛好隔 20 分鐘（15 + 5） |
| 07:10:01 | 3 筆併進同一張卡（共 6 筆），沒有開第二張 |
| 07:26:42～47 | 第二次重送，又隔 20 分鐘 |
| 07:29:30 前後 | 移除故障 |
| 07:30:01 | 第二次重送併進同一張卡（共 9 筆） |
| 07:50:01 | 結案（最後一點 07:26 起安靜滿 22 分鐘）。移除故障後 20 分 31 秒 |

結果：一場 45 分鐘的故障從頭到尾一張卡（1 個 `incident_uid`、9 筆告警點），故障期間沒有結案，故障結束後才結案。
這段時間兩條管線共 28 輪，全部成功；看門狗沒有出卡。

RCA（06:50 開卡後立刻跑，事故 5）：

| 管線 | 結果 |
| :-- | :-- |
| `rca_report` | **confirmed**，第一名 `rca-wip-worker`（分數 1.00），等待 `10.0.1.5`（db/oracle）86.3→1501.3 ms；`rca-mes-api` 分數 0.00。撈到 7,897 筆 span，事件期間入口請求 39 筆與 30 筆 |
| `rca_report_llm`（`it_otel` 的預設） | undetermined：`llm_not_configured`（沒有設定 LLM，照預期） |

第一次演練的事故 3 用新版重跑 `rca_report`：同樣是 confirmed、`rca-wip-worker`（等待資料庫 91.4→1508.2 ms）。

這次演練沒有涵蓋的：
- 兩個服務的流量都夠（每分鐘 30 筆以上）。低流量服務在 1 分鐘的事件期間過不了 20 筆的門檻，沒有測。
- 基準被污染：「事件前 15 分鐘」含告警送達前約 2 分鐘的故障，資料庫等待的基準是 86 ms（故障前實際約 30 ms）。這次差距大，結論沒受影響。
- 看門狗封頂 P2 只有測試驗證（`tests/test_e2e_drill_fixes.py`）；真實紀錄裡沒有出現失敗暴增，沒有實際觸發過。
- 日誌仍是 0 筆（問題 7）。

## 第三次驗證（2026-11-17，關係圖出處，映像檔 `aiops-platform:drill3`）

- 排程：`helm upgrade` 後多一個 CronJob `aiops-topology`（`7 * * * *`）。用它手動開一個 Job 跑完成功，
  `service_edges` 有 `rca-mes-api → rca-wip-worker`（`last_seen` 06:00:38 UTC）。排程自己到點觸發還沒等到，沒驗證。
- 新事故卡（事故 8，06:01:37 注入、06:05:03 開卡、P0）跑 `rca_report`：confirmed、`rca-wip-worker`，「關係圖依據」段落：

  ```
  rca-mes-api 呼叫 rca-wip-worker（call）
    - 這次事件的 trace 直接觀測到（1539 次呼叫；最後一筆 2026-11-17 06:03 UTC，事件開始後 2 分）
    - 人工宣告（匯入）（drill-manual；最後更新 2026-11-16 02:56 UTC，事件開始前 1 天 3 小時）
  圖資時間（整張圖每種來源最新的一筆）：人工宣告（匯入） 2026-11-16 02:56 UTC；這次事件的 trace 直接觀測到 2026-11-17 06:03 UTC
  ```
- 修正前建立的事故 5 重跑：宣告的那條邊寫「來源未記錄（事件群建立時的版本沒有記邊的出處）」，照預期。
- 已知的呈現取捨：同一條呼叫邊在資料表（平台從 trace 沉澱）與這次事件的 span 都看得到時，報告只寫後者
  （這次事件直接看到的是較強的依據），資料表那一筆的「最後觀測」不會另外列出。沒有 trace 的報告才會看到「平台從 trace 沉澱」。
- 沒驗證：訊息佇列邊與 Lot 流向的出處（演練服務沒有這兩種）；純拓撲推測（topology_inference）的報告只有測試驗證，
  演練裡有 trace，結論都是 confirmed。

順帶觀察到的真實看門狗行為：叢集昨晚關機、今天 05:20 重開後，看門狗第一輪開了兩張 P3 卡（`it_otel` 排程停了、`drill-team` 心跳斷了），
05:30 結案。這次是真的停過（不是問題 1 的誤報），等級是 P3。

## 第四次驗證（2026-11-17，真實 LLM）

設定來自本機的 `secrets.local.yaml`（provider `openai`、model `gpt-6-luna`），放進叢集 Secret。那個檔的 `baseUrl` 是空的，
平台不接受沒寫目的地，所以明確設成 OpenAI 官方端點 `https://api.openai.com`（目前是佔位，之後換公司的閘道）。
控制方式：只跑 4 次 RCA，其中 1 次平台自己判斷不呼叫 LLM。

| 事故卡 | 結果 | 整份報告耗時 |
| :-- | :-- | :-- |
| 事故 8（第 1 次） | confirmed，`rca-wip-worker`，自評信心 97%，引用 F15、F11、F2 | 16 秒 |
| 事故 8（第 2 次） | confirmed，`rca-wip-worker`，自評信心 94%，引用 F2、F4、F11、F15 | 14 秒 |
| 事故 5（昨天的卡） | confirmed，`rca-wip-worker`，等待 10.0.1.5 從 86 ms 到 1502 ms，引用 F15、F11 | 14 秒 |
| 事故 6（看門狗的卡，沒有 trace） | undetermined：`no_evidence`，平台沒有呼叫 LLM 判根因 | 6 秒 |

- 根因與不用 LLM 的 `rca_report` 一致（都是注入故障的 `rca-wip-worker` 等資料庫）。同一張卡跑兩次根因相同，信心與引用的事實略有不同。
- LLM 引用的事實編號都通過平台的驗證（`rejected` 是空的），白話摘要也是 LLM 寫的，開頭照實寫結論等級。
- 沒有證據的卡：照實標「無法判定」，沒有硬猜。

沒驗證、要知道的：
- **遮罩沒有在真實資料上被考驗到**：日誌是 0 筆（問題 7），送給 LLM 的只有數字型的事實與服務名，裡面沒有帳號、金鑰、個資可以遮。
- **資料庫位址 `10.0.1.5` 是原樣送出去的**：這是設計（RCA 要指認的依賴名稱不遮），不是漏遮。接真實環境時，內部 IP 會出現在送給 LLM 的內容裡，
  公司閘道那一層要知道這件事。
- **token 用量與花費沒有量到**：平台沒有記 LLM 回應的用量欄位。每份報告是 3 次以內的 LLM 呼叫，輸入是 15 筆事實加呼叫圖。
- 「證據互相矛盾、LLM 該不該保守」這種情況演練裡沒有，只測了「有明確證據」與「完全沒有證據」兩端。

### 4. 演練服務的共用腳本在 Windows 簽出後是 CRLF

`rca-lib.sh` 直接做成 ConfigMap 會讓 `rca-mes-api` 一直重啟（`syntax error: unexpected end of file`）。
去掉 `\r` 之後正常。這是另一個 repo 的檔案，這裡只記錄。
