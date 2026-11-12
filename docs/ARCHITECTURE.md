# AIOps 平台：架構、測試結果、模組手冊

三部分：**一、架構與各階段功能**　**二、過往測試結果**　**三、模組使用手冊**
標記：【新】= 新 RCA 引擎（rca_report_llm）的元件；【舊】= 確定性的舊判斷層（rca_report，當備援與對照）；無標記 = 共用。
原本的第三個引擎（認知型 `rca_report_cognitive`／`rca.root_cause.bayes`）已移除：它前面必須靠 LLM 抽證據，當不了離線備援
（程式在 git 歷史與 tag 1.1）。

---

## 一、架構與各階段功能

### 1.1 總覽

```
偵測管線 it_otel（不依賴 LLM）
  ingestion.fetch
    → ┌─────────────────────── 偵測（R+SNR）────────────────────────┐
      │ 三票軟投票 detector.soft_vote（每票 = 分數 / 門檻，加總 >= 2）  │
      │   週期票 hourly_baseline        該 週幾×小時 的平均與標準差      │
      │   突變票 residual_shift         殘差（原值 − 時段基準）的相鄰差  │
      │   漂移票 residual_chronic       殘差的 30 分鐘滾動平均           │
      │   三票都開 snr：門檻 = z_{1−p_fa/2}（p_fa 0.001 -> 3.29）× 門檻倍率 │
      │ 旁路（不投票、單獨成立）                                         │
      │   致命 hard_limit               單點踩到 YAML 界線 -> 直接 P0     │
      │   漂移 detect_drift_solo        殘差漂移連續 10 分鐘 -> 單獨告警  │
      └──────────────────────────────────────────────────────────────┘
    → new_only → grouping → scoring → absorb_open → suppression → notify_payload → heartbeat → notifier.* → persist
                                  │ 只交出「哪個實體、哪段時間」（事件群）
                                  ▼
RCA 管線
  主力 rca_report_llm【新】：fetch_traces / build_topology / fetch_logs
        → rca.facts（事實）→ enrichment.extract_evidence（LLM 抽證據）
        → rca.root_cause.llm（呼叫圖＋事實＋證據交給 LLM 判斷）→ rca.report → enrichment.rca_narrator（LLM 寫摘要）
        LLM 失效 -> 報告照出、rca_status = undetermined、根因寫「無法判定」
  備援 rca_report【舊】：fetch_traces / build_topology / fetch_logs
        → rca.root_cause.evidence（確定性，用 span 證據；沒有 trace 時退回 rca.root_cause.topological 依拓撲／Lot 流向排序）
        → rca.report（含 Runbook 比對矩陣）→ llm_summary / llm_briefing（可 noop）
```

圖上偵測畫成一個方塊，**YAML 裡仍是分開的節點**：每一票各自存基準線（以節點名稱）、各自能被回饋閉環調參
（`threshold_scale`，分 偵測器×管線×實體），不包成複合算子。`topological` 也仍是獨立算子（`evidence` 的 fallback 參數），沒有併進 `evidence`。

### 1.1.1 主線每一層實際使用的算子

實際在跑的主線是 `it_otel`（偵測告警）加 `rca_report_llm`（根因）。第 1–7 層每分鐘跑一輪，第 8–10 層只在有事故時跑，第 11 層是人送出回饋才跑。

| 階層 | 實際使用的算子 |
| :--- | :--- |
| 1. 抓資料 | `ingestion.fetch` |
| 2. 偵測 | `hourly_baseline`、`residual_shift`、`residual_chronic`、`hard_limit` |
| 3. 裁決 | `soft_vote`（SNR 門檻）、`new_only` |
| 4. 收斂 | `same_entity`、`topology_chain`、`messaging_link` |
| 5. 計分 | `golden_signals`、`p0_boost`、`fatal_floor` |
| 6. 吸收與抑制 | `absorb_open`、`suppress_window`、`suppress_same_root_cause`、`suppress_dedup` |
| 7. 通知 | `notify_payload`、`heartbeat`、`notifier.auto`、`notifier.watchdog` |
| 8. 根因：撈證據 | `incident_traces`、`incident_logs`、`build_dynamic_topology`、`rca.facts`、`extract_evidence` |
| 9. 根因：判斷 | `rca.root_cause.llm` |
| 10. 根因：報告 | `rca.report`、`rca_narrator`、`llm_briefing` |
| 11. 學習回饋 | `feedback_submit`、`feedback_propose`、`proposal_backtest`、`proposal_approve` |

不在 IT 主線的：`rca.root_cause.evidence`（`rca_report`，不連外）與 `rca.root_cause.topological`（連 trace 都沒有時，只看服務依賴圖／Lot 流向與先後時間）。

**RCA 走哪一條由偵測管線決定**：每條偵測管線 YAML 寫 `rca_workflow`，`POST /api/v1/rca` 沒帶 `workflow` 時照它走。

| 偵測管線 | 有沒有 trace | 預設 RCA 管線 |
| :--- | :--- | :--- |
| `it_otel`、`it_apm` | 有 | `rca_report_llm` |
| `factory`、`metrics_only` | 沒有 | `rca_report`（沒有 span 時退回 `topological`） |

新引擎不會自動退回舊引擎：LLM 沒設定或不可用時，報告照出但標「無法判定」。要舊引擎的結果，請求帶 `workflow: rca_report`。

**各算子簡介**

第 1 層　抓資料
- `ingestion.fetch`：從 Elasticsearch／Prometheus 抓每條序列這一輪的時序；基準線還沒有或到期時才多抓訓練窗。

第 2 層　偵測（每個算子各自給分數，不互相依賴）
- `detector.hourly_baseline`：週期票。拿「週幾 × 幾點」的歷史平均與標準差當基準，看現在偏離多少。
- `detector.residual_shift`：突變票。先扣掉週期基準得到殘差，再看殘差有沒有突然跳一階。
- `detector.residual_chronic`：慢性票。看殘差是不是連續偏向同一邊。管線裡掛兩次：一次投票，一次（連續 10 點）單獨成立、不經表決。
- `detector.hard_limit`：致命界線。物理或業務上踩到就是出事的上下限，單點成立、不投票、不接受後台調參。

第 3 層　裁決
- `detector.soft_vote`：把三張統計票合成一個判定。門檻由誤報機率 `p_fa` 換算（預設 3.29 個雜訊標準差）；列在 `bypass` 的節點原樣放行。
- `detector.new_only`：只留比資料庫裡最後一筆更新的異常，重跑同一段時間不會重複告警。

第 4 層　收斂（多筆異常併成一件事故）
- `alert.same_entity`：同一個實體、時間相近的異常併成一群。
- `alert.topology_chain`：服務依賴圖上相鄰（呼叫關係）且時間相近的群併在一起。
- `alert.messaging_link`：訊息佇列的生產者先出事、消費者稍後出事，併成同一件。

第 5 層　計分（決定嚴重度 P0–P3）
- `alert.golden_signals`：事故含黃金訊號（延遲、錯誤、流量、飽和度）時加權。
- `alert.p0_boost`：事故牽涉到列為 P0 的實體時加分。
- `alert.fatal_floor`：事故裡有致命界線事件時，分數至少拉到 P0 門檻並打上 `fatal:<節點>` 標籤。

第 6 層　吸收與抑制
- `alert.absorb_open`：新異常併入還在進行中的事故，一場事故從頭到尾只有一個編號。
- `alert.suppress_window`：落在靜音或維護窗口內的不通知。
- `alert.suppress_same_root_cause`：只是被下游拖累的上游症狀，下游已在同批告警就不另發。
- `alert.suppress_dedup`：同一組實體在冷卻時間（30 分鐘）內通知過就不重發；嚴重度升級時例外。

第 7 層　通知
- `alert.notify_payload`：替事故定嚴重度，產生通知內容（含回饋頁連結）。
- `alert.heartbeat`：進行中的事故每輪續送；安靜超過 15 分鐘送結案。
- `notifier.auto`：依環境變數決定出口（只寫日誌，或送 Alertmanager）。資料庫寫入成功後才送。
- `notifier.watchdog`：整輪每一步都成功才送「我還活著」的心跳；排程或資料源掛了，心跳過期就會告警。

第 8 層　根因：撈證據
- `ingestion.incident_traces`／`ingestion.incident_logs`：撈事故時段相關服務的 trace 與日誌。資料源連不上只記錯誤，報告照出。
- `rca.build_dynamic_topology`：用這次事故的 trace 重建當下的服務依賴圖。
- `rca.facts`：把 trace、日誌算成一條條有數字的事實（自身耗時變化、錯誤率、等待資源、流量來源、佇列延遲等）。純計算，不用 LLM。
- `enrichment.extract_evidence`：LLM 從日誌抽出證據；引文必須逐字出現在該服務自己的日誌裡，否則拒收。

第 9 層　根因：判斷
- `rca.root_cause.llm`：把呼叫圖、事實、日誌證據交給 LLM 排出根因候選。每個候選都要引用事實編號，數字要對得上，驗證不過就拒收；LLM 不可用或全數被拒時標「無法判定」，不硬猜。

第 10 層　根因：報告
- `rca.report`：依段落清單組出報告（摘要、時間軸、受影響實體、黃金訊號、根因候選、傳播路徑、trace 與日誌證據、建議處置等）。
- `enrichment.rca_narrator`：LLM 把報告寫成一段人看得懂的敘述，每句話要對應到報告段落。
- `enrichment.llm_briefing`：產生給值班人員的簡短戰情摘要。

第 11 層　學習回饋（手動動作）
- `admin.feedback_submit`：收下值班人員的回饋（誤報、漏報、正確）。
- `admin.feedback_propose`：把累積的誤報回饋轉成調參提案。
- `admin.proposal_backtest`：拿歷史資料回測提案；會讓已確認的真實事故漏掉的提案直接擋下。
- `admin.proposal_approve`：人工核准後才生效；YAML 寫死的參數不會被覆蓋。

### 1.1.2 關係圖（`ServiceGraph`，`aiops/engine/graph.py`）

實體之間的關係收在一個物件裡，算子用 `ctx.graph` 查詢，不再各自從清單拼圖。

| 邊的型別 | 宣告方向 | 異常傳播方向 | 來源 | 進傳播圖 |
| :--- | :--- | :--- | :--- | :--- |
| `CALL` 同步呼叫 | 呼叫者 → 被呼叫者 | 被呼叫者 → 呼叫者（反向） | `service_edges` 表（依管線的 `environment`）、YAML `topology:`、RCA 現場從 trace 還原 | 是 |
| `ASYNC` 訊息佇列 | 生產者 → 消費者 | 生產者 → 消費者 | `service_async_edges` 表 | **否**（現況照舊，只有 `alert.messaging_link` 用） |
| `FLOW` Lot 流向 | 上游機台 → 下游機台 | 上游 → 下游 | `lot_paths` 表、YAML `lot_edges:` | 是 |

| 查詢 | 回傳 | 誰在用 |
| :--- | :--- | :--- |
| `adjacent(kind, min_weight)` | 無向相鄰的實體對 | `alert.topology_chain`、`alert.lot_path_chain` |
| `pairs(kind)` | 有向（宣告方向）的實體對 | `alert.suppress_same_root_cause`、LLM 判斷的呼叫圖 |
| `weights(kind)` | 有向邊 → 權重（佇列延遲 P95） | `alert.messaging_link` |
| `propagation(min_lots)` | 傳播方向的有向邊 | `rca.root_cause.topological`、依賴圖段落、LLM 輸出的實體檢查 |
| `nodes(kind)` | 邊上出現過的實體 | 證據抽取的已知實體清單 |
| `declared(kind)` | 照原順序的清單 | `rca.build_dynamic_topology`（合併現場還原的邊） |

- 這是**不改行為**的重構：每個查詢都跟重構前各算子的寫法在 200 張隨機圖上逐一比對（`tests/test_service_graph.py`）。
- `ctx.topology`、`ctx.lot_edges`、`ctx.async_edges` 留著當載入與存檔的格式；`ctx.graph` 每次現算、不快取。
- 圖可以有環，不是 DAG。
- **圖分環境**（`feat/environment-scope`）：workflow YAML 選填 `environment`（`[A-Za-z0-9_.-]`，最長 64；預設空字串 = 不分環境，跟以前一模一樣）。`service_edges`、`service_async_edges`、`lot_paths` 都有 `environment` 欄位，唯一鍵含它（`uq_service_edges_env_pair`、`uq_service_async_edges_env_pair`、`uq_lot_paths_env_lot`，`migrations/postgres/0009`）。偵測時 `storage/hooks` 只載入這條管線環境的邊；事件群快照記下 `environment`，RCA 照事件群的環境載圖（快照沒存拓撲時才從表載；舊快照沒有這個鍵就沿用 RCA 管線自己的，等於舊行為）。`admin.topology_refresh`、`admin.merge_traces`、`admin.merge_lot_paths` 的請求都接受 `environment`，寫入與 prune 只碰那個環境。`''` 是「不分環境」的那一份，**不是**「全部環境」。
- **實體命名規則**：實體維持 `service.name`（工廠是機台編號），**不把環境或命名空間塞進名稱**。塞進去的話改環境名就斷基準線（基準線、調參、異常血緣都以實體名稱為鍵），跨環境比較也對不起來。環境只寫在管線的 `environment`；一個環境開一條管線（異常點、基準線、事件群、調參本來就以管線名稱分開）。載入期檢查：管線設了 `environment` 時，target 的 `entity` 以非英數字元切段後有一段等於環境名（`cart-prod`、`prod.cart`）就 `WorkflowError`；沒寫 `entity` 的 target 名稱是查詢字串（本來就可能含 `env="prod"`），不檢查。
- **環境的缺口**：撈 span 還沒依 `deployment.environment` 過濾（`aiops/sources` 另案重寫中），所以 `admin.topology_refresh` 與 RCA 現場還原用的 trace 來源要只含那個環境，否則別環境的呼叫會被記進來（`tests/test_environment_scope.py` 的 `xfail(strict=True)` 標著）。`trace_series`（trace 合成時序）、通知去重的指紋與最近通知仍不分環境，見 `docs/CROSS_DOMAIN_CONSISTENCY_AUDIT.md` M4。
- **還沒做的（都是行為變更，要等混沌回歸）**：佇列邊進傳播圖、`SUPPLY`（物理供應）與 `HOSTED_ON`（服務部署在哪台主機）兩種邊、`lot_paths` 的清理。

### 1.2 各階段功能與新舊差異

| 階段 | 舊做什麼 | 新做什麼 |
| :-- | :-- | :-- |
| 偵測（detector.*） | it_otel：hourly_baseline＋mad＋quantile_drift 布林多數決（tag 1.1） | it_otel 改成 R+SNR（見 1.1 與 `docs/RESIDUAL_SNR_ENSEMBLE.md`）；factory 仍是三票布林多數決，it_apm／metrics_only 仍是 zscore |
| 誤報回饋 | 分位數反推單一偵測器的 threshold | R+SNR 節點改調 `threshold_scale`（只能 >= 1）：整段偵測重跑（軟投票＋旁路）回測、黃金事故防護罩、人核准 |
| 收斂／計分／抑制／通知 | 事件群、靜音、冷卻、Alertmanager | 不動；仍**嚴禁依賴 LLM** |
| 證據撈取 | `ingestion.incident_traces/logs`、拓撲沉澱（`incident_series` 保留但目前沒掛在管線上） | 不動 |
| 事實（證據層） | 解讀混在 `spans.py`／`evidence.py`（子 span 晚結束直接記成時鐘偏移） | 【新】`rca.facts` 只陳述事實（如 `child_outlives_parent=13000 ms`），確定性、不呼叫 LLM |
| 日誌理解 | `logs.py` 樣板與暴增排序，只當旁證 | 【新】`enrichment.extract_evidence`：LLM 把日誌讀成封閉詞表證據，`quote` 必須逐字出現在原日誌 |
| 根因排名 | `rca.root_cause.evidence`：規則與圖演算法疊加分數；傳導上限 `MAX_HOPS=10`；沒有 trace 時退回 `topological` | 【新】`rca.root_cause.llm`：呼叫圖＋事實＋證據交給 LLM，候選要過實體白名單、引用、數字比對等驗證 |
| Runbook／處置 | `sections/actions.py` 比對矩陣（約 353 行） | 沿用 |
| 戰情摘要 | `llm_summary`／`llm_briefing`，可 `noop` 退模板 | 【新】`enrichment.rca_narrator`，只吃已判好的候選與事實；驗證不過退模板 |
| LLM 角色 | 可選潤飾 | RCA 路徑**必要**：抽證據、判斷根因、寫摘要；每一步都有驗證，失效標 undetermined |
| LLM 失效 | 退模板，結論照出 | 報告照出，`rca_status="undetermined"`、寫原因碼，根因欄「無法判定」，附事實與原始 trace／日誌入口；不退回舊規則、不無限重試 |
| 證據不足 | 回空清單＋`rca_root_note` | 同 |
| 刪舊條件 | — | 混沌 1–13 不輸 199／200，且 16／17／21 轉綠或歸因正確才刪舊判斷層與其專屬測試 |

### 1.3 接口型別（`aiops/engine/rca_types.py`）

```
Fact(kind, entity, value, unit, window, detail, source)          證據層輸出（frozen）
EvidenceItem(kind, entity, state, confidence, quote, log_ts)     LLM 抽取輸出（frozen，kind 在 EVIDENCE_KINDS 內）
EVIDENCE_KINDS = circuit_breaker_open, gc_pause, oom, connection_pool_exhausted, deadlock,
                 lock_wait, client_timeout, dependency_unreachable, deployment_change, broker_unavailable
ctx.rca_facts / rca_evidence / rca_status("ok"|"undetermined") / rca_undetermined_reason
根因輸出沿用 RootCandidate：rca.root_cause.llm 的 score = LLM 自評信心（驗證通過的候選才留下）
```

### 1.4 事實 kind（`rca.facts` 產出）

| kind | 掛在 | 值 |
| :-- | :-- | :-- |
| `entry_latency` | 服務 | 入口 span 平均總耗時（ms） |
| `entry_failure_rate` | 服務 | 入口失敗比例 |
| `failed_without_failed_child` | 服務 | 入口失敗且整條同步鏈沒有失敗 span 的比例 |
| `self_time_delta` | 服務 | 每請求自身耗時（區間聯集）事件 − 基準（ms） |
| `wait_delta` | 服務 | 每請求等某資源葉節點的時間 事件 − 基準（ms） |
| `inbound_rate` | 服務 | 每分鐘入口數 事件 ÷ 基準；`by_caller` 拆來源 |
| `traffic_drop` | 呼叫端 | 對 callee 每分鐘呼叫數 事件 ÷ 基準 |
| `fanout_ratio` | 呼叫端 | 每個入口請求打 callee 幾次 事件 ÷ 基準 |
| `child_outlives_parent` / `child_precedes_parent` | 父服務 | 子 span 晚於父結束／早於父開始最多幾 ms（只陳述，不判斷原因） |
| `log_template_surge` | 服務 | 錯誤日誌樣板頻率 事件 ÷ 基準 |
| `mq_lag` | 消費者 | 事件期間訊息延遲 P95（ms） |
| `metric_shift` | 實體 | 事件群原始時序 事件平均 − 事前平均 |

樣本不到 `min_requests`（預設 20）就不產出，原因記進 `ctx.log`；值為 0 或負照樣產出（證據層不判斷算不算異常）。

---

## 二、過往測試結果

### 2.1 全套測試

- 分支 `feat/adopt-r-snr`（合併 main `b0a8044` 之後、移除 bayes 之後）：pytest **1482 passed / 1 skipped / 18 xfailed**。
- 歷史：main `4e71f13` 1233 passed / 1 skipped / 19 xfailed。

### 2.2 混沌場景（`scripts/chaos/`，世界模擬器＋記憶體 ES 替身＋真實 PostgreSQL 16＋平台真實管線）

以下逐場景數字是 **main（偵測還是 hourly＋mad＋quantile_drift 多數決）** 量的：**1–13 共 21 列，199／200 斷言**（20 列全過；12e 為已知死區）。
it_otel 改成 R+SNR 之後，在 `e0ccc5e` 上實跑過一次 1–13＋24：**1–12 不變、13c 從 7／7 掉到 0／2**——ingress-gw 的延遲序列
在 11:43（注入 12:00 前）軟投票三票各自部分越線而告警，拓撲收斂把 payment-svc 併進這張提早開的卡，注入的突波被吸收進去，
場景找不到「注入之後開的事件群」，後面 RCA 的 5 條斷言沒跑到（不是漏報：突波有告警，在提早那張卡裡）。
原因是 R+SNR 的常態門檻＋軟投票只在每分鐘請求數上驗過，重尾的延遲序列會多誤報。之後的改動（刪 7 個算子、修 ES 補桶、
誤報回饋閉環、移除 bayes）**都還沒有重跑混沌**（混沌資料庫暫停中）。逐場景（main）：

- **1 隱性延遲陷阱**　測資：ord-svc 呼叫 payment-gw 20→3000 ms，應用吞例外回 200，0 筆 500、1220 個客戶端 span 逾時　結果：6／6，歸因 `payment-gw:443`（external/http）佔比 1.0，RB-01
- **2 離峰超低流量**　測資：bill-svc 每 5 分鐘 2～3 筆，注入 billing-db 8→5000 ms 且 100% 失敗　結果：修前 8／9（注入前誤報：低流量把「當分鐘總耗時」當延遲）→ 修後 9／9（查詢加 `divide_by`）；基準窗自動 15→60 分，RB-02
- **3 兩下游各 45%**　測資：cart-api 總耗時 +1000 ms，redis +450、postgres +450、自身 +100　結果：修前 6／7（`attribution` 只記 redis）→ 修後 7／7（`others` 列出 postgres 0.448）
- **4 連鎖雪崩**　測資：gateway→order-api→inventory-svc→oracle，oracle 12→4000 ms 且 50% 失敗　結果：4／4，三服務歸同一事件群，根因第 1 名 inventory-svc，RB-02
- **5 沒有子 span 的崩潰**　測資：純程式 Exception，事故窗無資源葉節點　結果：修前 7／8（`attribution` 殘留 redis share 0.0）→ 修後 8／8（空 attribution）；RB-03
- **6 LLM 幻覺注入**　測資：假模型回「重啟 k8s-node-99、查 Kafka」、虛構節點、虛構服務名、合法回文各一組　結果：未通過 3 條——原文被攔（靠數字 99，不是白名單）；`gpu-farm-prod`（無數字）與 `payment-gateway` 穿過白名單；無 `HallucinationBlocked` 日誌。**已知死區**，之後以封閉詞表守門處理
- **7 單點毛刺的分位數反推**　測資：1 分鐘與 3 分鐘毛刺（mad 分數約 80）　結果：修前 8／10（1 分鐘毛刺退回 ×1.2）→ 修後 10／10（`fetch_span` 含兩端）
- **8 黃金事故防護罩**　測資：zscore 3.0→10.0 的破壞性調參（25.0 在建立階段被範圍擋下 HTTP 400）　結果：9／9，事故窗異常 8→0、`safety.passed=false`、approve 回 409；溫和提案 3.0→3.5 通過
- **9 假日前夕流量腰斬（ACF）**　測資：含 3 天連假的週期資料；`sum` 聚合只有 7.5 天　結果：修前 5／6（空桶被當 0，算出 r(168h)=−0.221 宣稱有週期）→ 修後 6／6（覆蓋率不足一律「資料不足」，不提案）
- **10 JSONB 併發更新**　測資：真實 PostgreSQL，10 worker 各追加 5 次、並行首次寫同一事故　結果：修前 5／7（50 鍵只留 5～7、4～6／10 撞唯一鍵）→ 修後 8／8（50／50 鍵、0 IntegrityError）
- **11 並行子 span**　測資：asyncio.gather 同時打 DB 與外部金流　結果：通過（區間聯集歸因）
- **12 訊息佇列（12a～12g）**　測資：生產者壞版本、消費者自身變慢、舊版 agent 缺 link、3 小時延遲、偵測端併卡　結果：通過；**12e（生產者毫無症狀的毒訊息）為已知死區**，`xfail(strict=True)` 留存
- **13 流量放大者（13a～13c）**　測資：order-svc 對 payment-svc 呼叫放大 25 倍；放大者無告警；外部流量突波對照　結果：通過

誠實披露：ES 是記憶體替身；世界資料由自寫模擬器產生；每場景單一隨機種子；`divide_by` 全面套用在場景 1／8／12g／13 會出現新誤報（成因未查），故預設查詢未動；真實資料對照（11/3、5 服務）原查詢 75 異常點／3 事件群，換 `divide_by` 為 157／4，多出來的是真事故還是誤報未驗證。

### 2.3 RCA 引擎混沌並排（`run.py --compare`：legacy 對 llm）

- llm 引擎的歷史結果：`docs/RCA_LLM_JUDGE_CHAOS_RESULTS.md`（三方對照時代量的，含已移除的 cognitive 欄）。
- 場景 24（LLM 全掛）24a down／24b slow／24c garbage／24d hallucinate／24e faithful 對照：改成對 `rca_report_llm` 驗，
  斷言條件不變；**改寫後未實跑**（混沌資料庫暫停中）。改寫前在 cognitive 管線上的最後一次結果是 48／51（24b 逾時 60 秒、
  24e 熔斷證據沒進 ctx.rca_evidence，兩個都是改寫前就存在的失敗）。
- 離線版場景 24：`tests/test_llm_outage_pipeline.py` 用出貨的 rca_report_llm.yaml（rca.facts 之後的節點、參數原樣、LLM 節點照場景 24 接）
  驗 down／garbage／hallucinate／slow（逾時）／沒設定 都是報告照出、undetermined＋原因代碼、沒有候選、不重試，faithful 對照組 ok；
  三段撈取與 rca.facts 不在離線版裡（資料直接放進 ctx）。判斷節點的假 LLM 模式（`judge_faithful`／`judge_hallucinate`）用平台真的 `rca_judge` 驗證離線測過
  （`tests/test_scripted_llm_judge.py`），沒有在場景裡跑過。
- 假 LLM（`scripted_llm.py`）只測平台怎麼處理 LLM 輸出，**不代表真模型抽取與判斷的品質**；真模型品質尚無人驗。

---

## 三、模組使用手冊

寫法：第一層是檔案，其下是內容，`#` 右邊是解釋。

```
aiops/
├─ engine/                           管線骨架與型別（不放業務演算法）
│  ├─ context.py                     # PipelineContext：整條管線的強型別資料載體，每輪新建
│  │   ├─ rca_facts / rca_evidence   # 【新】證據層事實、LLM 抽出的證據
│  │   └─ rca_status                 # 【新】ok／undetermined；rca_undetermined_reason 寫原因碼
│  ├─ rca_types.py                   # 【新】Fact、EvidenceItem、EVIDENCE_KINDS、RcaStatus
│  ├─ pipeline.py                    # 讀 YAML、按序實例化算子並 execute；scope（target／batch／after_persist／after_finalize）、序列隔離
│  ├─ registry.py                    # OperatorRegistry：@register 登記、動態建立
│  ├─ operator.py                    # BaseOperator 合約
│  ├─ params.py                      # number／positive／whole 參數檢查、deep_merge
│  ├─ errors.py                      # InfrastructureError、NoDataError 等
│  ├─ single_flight.py               # 同一事件群併發請求合併成一次 RCA（行程內）
│  └─ types.py／topology.py／mq.py／requests.py／signature.py   # 事件、拓撲、訊息佇列、請求與簽章型別
│
├─ sources/                          外部資料源適配（只輸出 DataFrame）
│  ├─ elasticsearch.py／prometheus.py／mock.py   # 各資料源；base.py 為共同介面，_http.py 重試 2 次
│  └─ query/                         # 查詢語法解析：elasticsearch.py（含 divide_by）、promql.py、trace.py、window.py
│
├─ stages/                           管線算子，依執行順序分段
│  ├─ ingestion/fetch.py             # ingestion.fetch／incident_series／incident_traces／incident_logs
│  ├─ detectors/                     # 異常偵測
│  │  ├─ base.py                     # DetectorOperator：載基準線、調參、warning、訓練排除；演算法只寫 fit／detect
│  │  │                              #   ＋時序白化、自適應尺度、序列 SNR／p_fa 門檻（節點參數 snr）、門檻倍率 threshold_scale
│  │  ├─ hourly_baseline.py／residual_shift.py／residual_chronic.py   # it_otel 的三票（R+SNR）
│  │  ├─ mad.py／zscore.py／quantile_drift.py                          # factory／it_apm／metrics_only 用
│  │  ├─ rules.py                    # 規則偵測，不需訓練
│  │  ├─ hard_limit.py               # 致命軌：單點踩線成立、不投票、界線只認 YAML
│  │  └─ new_only.py                 # 只報新出現的實體
│  ├─ ensemble/ensemble.py           # detector.ensemble 布林多數決；bypass 的節點事件原樣留下
│  ├─ ensemble/soft_vote.py          # detector.soft_vote 軟投票（it_otel）；投票節點必須開 snr、bypass 同 ensemble
│  ├─ grouping/                      # group.py 收斂；absorb.py 吸收進行中事件群
│  ├─ scoring/score.py               # 計分、alert.fatal_floor
│  ├─ suppression/suppress.py        # 靜音窗、冷卻、同根因
│  ├─ notification/                  # payload.py、heartbeat.py（補送 endsAt）、notifiers.py、transport.py、links.py
│  ├─ rca/                           # 根因分析
│  │  ├─ base.py                     # RootCauseOperator.rank、RcaSection（applies／build）
│  │  ├─ report.py                   # rca.report 段落組裝（不 import 具體段落或演算法）
│  │  ├─ spans.py／logs.py／mq.py／topology_builder.py   # 證據層純函式：span 統計、日誌樣板、佇列、拓撲沉澱
│  │  ├─ facts/                      # 【新】rca.facts：spans.py／logs.py／mq.py／series.py／windows.py／operator.py
│  │  ├─ algorithms/                 # 【舊】evidence.py（約 520 行）、topological.py（evidence 沒有 trace 時的 fallback）
│  │  └─ sections/                   # overview／evidence／graph／log_patterns／similar_cases；【舊】actions.py（Runbook 矩陣）
│  └─ enrichment/                    # LLM（唯一可連外的 stages 目錄）
│     ├─ base.py／registry.py／providers.py   # BaseLLM.complete、LLMError、@register 供應商
│     ├─ narrative.py／summarizer.py # 舊：llm_summary／llm_briefing（可 noop），詞表守門
│     ├─ evidence_extractor.py       # 【新】enrichment.extract_evidence，無 noop；失敗 → undetermined
│     ├─ rca_judge.py                # 【新】rca.root_cause.llm，候選要過驗證；失敗 → undetermined
│     └─ rca_narrator.py             # 【新】enrichment.rca_narrator，驗證不過退模板（旁路）
│
├─ control_plane/                    # 手動動作 admin.*（唯一可 import storage 的算子層）
│  ├─ learning/                      # golden.py 黃金事故、learning.py 回饋、profile.py ACF 體檢、replay.py 回放、
│  │                                 # threshold.py 分位數門檻、tuning.py／tuning_rules.py 調參與規則升級、
│  │                                 # segment.py 整段偵測重跑、snr_feedback.py R+SNR 的誤報提案／回測／防護罩
│  └─ operations/                    # incidents.py、ingest.py、mappings.py 欄位對照、silences.py、sources.py、topology.py
│
├─ storage/                          # 持久化（不含業務判斷）
│  ├─ models.py／db.py／crud.py／repository.py／hooks.py   # 表定義、交易、upsert、Repository、StorageHooks
│  └─ （DB 只記最終結果：Incident、Audit、基準線；節點之間不經 DB 傳資料）
│
├─ pipelines/                        # 流程只在 YAML 宣告，名稱全域唯一
│  ├─ detection/                     # factory／it_apm／it_otel／metrics_only
│  ├─ rca/                           # rca_report.yaml（備援）；【新】rca_report_llm.yaml（主力）
│  └─ control/                       # admin_*.yaml（核准、回測、靜音、拓撲沉澱…共 20 支）
│
└─ interfaces/                       # 入口（零業務邏輯，嚴禁 if／for／try）
   ├─ app.py／runner.py              # 建立 app；runner 是組裝點、workflow 只接名稱
   ├─ api/                           # alerts.py、ingestion.py、runs.py、tuning.py
   ├─ bootstrap/                     # auth.py（Token 驗證）、fallback.py（錯誤處理）
   └─ cli/__main__.py                # 排程入口（K8s CronJob 或 Loop）

scripts/chaos/                       # 混沌驗收（只讀平台、不改平台）
├─ run.py                            # 跑場景：`run.py 1 2 3 --out f.md`；`--rca {legacy,llm}`、`--compare`、`--llm <模式>`
├─ world.py／es.py／harness.py       # 世界模擬器、ES 替身、PG 與斷言 harness（`CHAOS_PG_URL` 可換資料庫）
├─ scen_detect_rca.py／scen_learning.py／scen_mq.py／scen_traffic_amp.py   # 場景 2–5,11／6–10／12／13
├─ scen_llm_outage.py                # 場景 24：LLM 全掛（對 rca_report_llm）
└─ scripted_llm.py                   # 假 LLM：faithful／down／slow／garbage／hallucinate＋判斷節點 judge_faithful／judge_hallucinate

scripts/backtest/                    # 偵測回測（ES 唯讀或離線合成）
├─ residual_ensemble.py／variants.py／es_series.py   # R+SNR 與對照組（R、A、A+SNR）回放
└─ feedback_e2e.py                   # 誤報回饋閉環端到端（--synthetic 離線；留出驗證）

tests/                               # 驗證
├─ test_architecture.py              # 24 條架構防線；【新】+2 條：告警路徑不依賴 LLM、RCA 路徑 LLM 失敗降級
├─ test_load_time_validation.py      # 寫錯 YAML 在載入期報錯
├─ test_rca_facts.py／test_rca_extract_evidence.py／test_rca_llm_judge.py   # 【新】事實、證據抽取、LLM 判斷
├─ test_residual_ensemble.py／test_snr_soft_vote.py／test_feedback_snr.py   # 偵測 R+SNR 與誤報回饋閉環
└─ test_chaos_*.py／test_dual_track.py／test_stability_guards.py…       # 既有行為測試
```

### 常用指令

```bash
.venv/Scripts/python -m pytest tests/ -q                          # 全套（約 90 秒）
.venv/Scripts/python -m pytest tests/test_architecture.py -q      # 架構防線
.venv/Scripts/python scripts/chaos/run.py 1 2 3 --out out.md      # 混沌；需 PostgreSQL（預設 127.0.0.1:55433）
```
