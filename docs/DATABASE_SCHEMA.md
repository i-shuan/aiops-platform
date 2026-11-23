# 資料庫欄位與格式

資料來源：`aiops/storage/models.py`（SQLAlchemy 模型）、`migrations/postgres/`、`aiops/engine/requests.py`，以及開發資料庫（PostgreSQL，混沌測試用）裡實際存的資料。
共 **15 張表**。型別欄是 PostgreSQL 的實際型別（`DateTime` 對應 `TIMESTAMP WITHOUT TIME ZONE`，`JSON` 對應 `json`）。

## 0. 先看這幾條通用規則

| 規則 | 內容 |
|---|---|
| **時間** | 所有時間欄都是 `TIMESTAMP WITHOUT TIME ZONE`，**存的是 UTC**（程式用 `utc_now()` 寫入，去掉時區資訊）。讀出來要自己當 UTC。JSON 裡的時間是 ISO 8601 字串，帶 `+00:00`。 |
| **主鍵** | 每張表都有自增整數 `id`。整數 `id` 會因資料庫重建從 1 重來，**不能拿到外部使用**；對外的識別是 `incidents.uid`。 |
| **沒有外鍵** | 沒有任何 `FOREIGN KEY` 約束。表與表之間的關聯（例如 `notifications.incident_id`、`feedback.incident_uid`）由程式維護，資料庫不會擋孤兒資料。 |
| **節點之間不經資料庫** | 這些表只在管線前面載入輸入、後面寫入最終結果，以及手動動作管理自己的狀態（`CLAUDE.md` 第 0 節）。 |
| **升級** | 沒有 Alembic。新資料庫由 `models.reset_all()` 建表；既有 PostgreSQL 靠 `migrations/postgres/0001…0008_*.sql` 補欄位，每支可重複執行。 |
| **開放型欄位** | `JSON` 欄位內容是約定的形狀，資料庫不檢查；形狀見各表與第 17 節。 |
| **併發** | `detector_models.version`、`active_detector_configs.version` 是樂觀鎖：寫入必須帶讀取當時的版本，版本已被推進就不覆蓋。 |

## 1. `incidents`：事件群

一場事故從初發到結案只有一筆。唯一鍵 `uid`；索引 `status`、`pipeline`。

| 欄位 | 型別 | 可空 | 預設 | 說明 |
|---|---|---|---|---|
| id | INTEGER | 否 | 自增 | 內部使用（RCA 查詢、通知紀錄） |
| uid | VARCHAR(40) | 否 | 自動產生 | **對外識別**，格式 `inc-` + 32 位十六進位，例：`inc-4ed6cffd68534dabb58bd5dee7e1c8e4`；通知標籤 `incident_uid` 用它 |
| preset | VARCHAR(64) | 否 | | 來源管線名稱，例：`it_otel` |
| incident | JSON | 否 | | 事件群本體，格式見第 17.1 節 |
| severity | VARCHAR(8) | 否 | | `P0`／`P1`／`P2`／`P3`／`warning` |
| suppressed_by | VARCHAR(512) | 是 | | 被抑制的原因，例：`silence_window：mute#3`；沒被抑制是 NULL |
| created_at | DateTime | 否 | 現在（UTC） | |
| pipeline | VARCHAR(128) | 是 | | 來自哪條排程管線；NULL = 直接呼叫 `/alerts/process`，不會自動心跳與結案 |
| status | VARCHAR(16) | 否 | `open` | `open`（已通知、進行中）、`resolved`（已結案）、`suppressed`（被抑制、沒通知） |
| resolved_at | DateTime | 是 | | 結案時間 |
| context | JSON | 否 | `{}` | 建立事件群當下的拓撲與 Lot 流向（只留跟這件事有關的邊），格式見第 17.2 節 |

## 2. `anomalies`：偵測結果（只增不改）

由 `hooks.persist_batch` 跟事件群在**同一筆交易**寫入。唯一鍵 `(pipeline, entity, metric, ts, detector_node)`，同一個點不會寫兩份。索引 `pipeline`。

| 欄位 | 型別 | 可空 | 預設 | 說明 |
|---|---|---|---|---|
| id | INTEGER | 否 | 自增 | |
| pipeline | VARCHAR(128) | 否 | | |
| entity | VARCHAR(256) | 否 | | 實體（服務、機台）名稱 |
| metric | VARCHAR(512) | 否 | | 指標查詢字串。以前 ES 的指標是整段查詢 JSON 文字（2026-11-20 起平台不直連 Elasticsearch，舊資料列還是這種形狀），所以這欄可能很長 |
| ts | DateTime | 否 | | 異常點的時間（UTC） |
| value | FLOAT | 否 | | 當時的值 |
| score | FLOAT | 否 | | 異常分數 |
| detector_name | VARCHAR(64) | 否 | | 算子名，例：`detector.hourly_baseline`。外部匯入的異常不經這裡 |
| detector_node | VARCHAR(128) | 否 | `''` | 管線 YAML 的節點名稱（同一個算法可掛多次，各自有自己的節點名） |
| level | VARCHAR(16) | 否 | `anomaly` | `anomaly`（越過門檻）或 `warning`（接近門檻的預警） |
| attrs | JSON | 否 | `{}` | target 的屬性，例：`{"dept": "payments", "signal": "latency"}` |

## 3. `detector_models`：偵測器基準線（批次訓練後的狀態）

一條管線 × 實體 × 指標 × 偵測器 × 節點一筆。唯一鍵 `(pipeline, entity, metric, detector_name, node)`。

| 欄位 | 型別 | 可空 | 預設 | 說明 |
|---|---|---|---|---|
| id | INTEGER | 否 | 自增 | |
| pipeline | VARCHAR(128) | 否 | | |
| entity | VARCHAR(256) | 否 | | |
| metric | VARCHAR(512) | 否 | | |
| detector_name | VARCHAR(64) | 否 | | 算子名 |
| node | VARCHAR(128) | 否 | `''` | YAML 節點名稱；基準線以節點名稱為準 |
| state | JSON | 否 | | 算子 `get_state()` 的輸出，**形狀依算子而異**（第 17.3 節） |
| train_start | DateTime | 否 | | 訓練窗起點 |
| train_end | DateTime | 否 | | 訓練窗終點 |
| points | INTEGER | 否 | | 訓練點數（演算法真正用得到的點；冷啟動門檻 `min_train_points` 已於 2026-11-20 移除，這個數現在只是紀錄） |
| trained_at | DateTime | 否 | 現在 | |
| version | INTEGER | 否 | `1` | 樂觀鎖 |
| artifact_format | VARCHAR(16) | 否 | `''` | 非 JSON 的模型本體格式；**只允許 `npz`**（禁止 pickle）；空字串 = 沒有 |
| artifact | BYTEA | 是 | | 模型本體的位元組 |

## 4. `silences`：靜音、維護窗、人工排除

| 欄位 | 型別 | 可空 | 說明 |
|---|---|---|---|
| id | INTEGER | 否 | |
| kind | VARCHAR(16) | 否 | `mute`（靜音）、`maintenance`（維護窗）、`exclusion`（人工排除，另外會排除在訓練之外） |
| start | DateTime | 否 | 開始（UTC），必須早於 `end` |
| end | DateTime | 否 | 結束（UTC） |
| entity | VARCHAR(256) | 是 | NULL = 不限實體 |
| dept | VARCHAR(128) | 是 | NULL = 不限部門 |
| reason | VARCHAR(1024) | 否 | 原因 |
| created_by | VARCHAR(128) | 否 | 建立者 |

**沒有循環設定**：只有一次性的開始與結束時間。

## 5. `notifications`：通知紀錄（去重用）

| 欄位 | 型別 | 可空 | 說明 |
|---|---|---|---|
| id | INTEGER | 否 | |
| incident_id | INTEGER | 否 | 對應 `incidents.id`（沒有外鍵） |
| fingerprint | VARCHAR(1024) | 否 | 事件群指紋；有索引。`suppress_dedup` 用它判斷 30 分鐘內是否發過 |
| severity | VARCHAR(8) | 否 | 發出時的嚴重度 |
| last_event_ts | DateTime | 否 | 發出時最後一筆異常的時間 |
| count | INTEGER | 否 | 當時累計的異常筆數 |

## 6. `active_detector_configs`：偵測器現行參數（核准後生效）

分層：(偵測器, 管線, 實體)，`*` = 不限定。套用時取最具體的一層：管線×實體 > 管線 > 實體 > 全域。唯一鍵 `(detector_name, pipeline, entity)`。

| 欄位 | 型別 | 可空 | 預設 | 說明 |
|---|---|---|---|---|
| id | INTEGER | 否 | 自增 | |
| detector_name | VARCHAR(64) | 否 | | 算子名 |
| pipeline | VARCHAR(128) | 否 | `*` | |
| entity | VARCHAR(256) | 否 | `*` | |
| version | INTEGER | 否 | | 樂觀鎖；核准時版本已被推進則回 409，提案標成 `stale` |
| params | JSON | 否 | | 參數名 → 值，例：`{"threshold": 4.5}`。YAML 寫了的鍵鎖定，不會被這裡蓋掉 |
| source | VARCHAR(16) | 否 | | 來源提案的 `source`（見第 7 節） |
| proposal_id | INTEGER | 否 | | 來源提案 `tuning_proposals.id` |

## 7. `tuning_proposals`：調參提案

| 欄位 | 型別 | 可空 | 預設 | 說明 |
|---|---|---|---|---|
| id | INTEGER | 否 | 自增 | |
| source | VARCHAR(16) | 否 | | 建立 API 只收 `auto`、`llm`、`manual`；由回饋產生的提案與序列剖析建議程式裡另外寫入 `feedback`、`profile` |
| detector_name | VARCHAR(64) | 否 | | |
| pipeline | VARCHAR(128) | 否 | `*` | 核准時寫進同一層 |
| entity | VARCHAR(256) | 否 | `*` | |
| params | JSON | 否 | | 提案要改的參數（只能是該偵測器 `tunable` 範圍內的鍵） |
| reason | VARCHAR(2048) | 否 | `''` | |
| proposer | VARCHAR(128) | 否 | | |
| status | VARCHAR(16) | 否 | | `pending` → 回測後 `backtested` 或 `rejected`（沒通過黃金事故防護罩）→ 核准後 `approved`；核准時版本衝突 `stale` |
| base_version | INTEGER | 否 | | 提案建立時所依據的現行參數版本 |
| backtest | JSON | 是 | | 回測結果，格式見第 17.5 節 |
| approved_by | VARCHAR(128) | 是 | | |
| approved_at | DateTime | 是 | | 調參成效報表用來切前後 |
| created_at | DateTime | 否 | 現在 | |

## 8. `feedback`：維運回饋

| 欄位 | 型別 | 可空 | 說明 |
|---|---|---|---|
| id | INTEGER | 否 | |
| incident_uid | VARCHAR(40) | 是 | 針對某個事件群時填；有索引。**真正的漏報（平台根本沒告警）時為 NULL** |
| pipeline | VARCHAR(128) | 是 | 漏報補錄時必填 |
| entity | VARCHAR(256) | 是 | 同上 |
| time_start | DateTime | 是 | 漏報補錄：事情實際發生的時段 |
| time_end | DateTime | 是 | |
| verdict | VARCHAR(16) | 否 | `false_positive`（誤報）、`true_positive`（確認為真）、`missed`（漏報，或告警太晚／太輕） |
| detector_name | VARCHAR(64) | 是 | 指出哪個偵測器誤報 |
| text | VARCHAR(4000) | 否 | 說明，至少 1 字 |
| author | VARCHAR(128) | 否 | |
| proposal_id | INTEGER | 是 | 由這筆回饋產生的提案 |
| created_at | DateTime | 否 | |
| review | JSON（PostgreSQL 跑過 0013 是 JSONB） | 否 | 分項覆核，預設 `{}`。有填的項目：`grouping`（`ok`／`merged_wrong`／`split_wrong`）、`rca`（`ok`／`wrong_root`／`missed_root`）、`resolve`（`ok`／`early`）、`severity`（`ok`／`too_high`／`too_low`），細節 `unrelated_entities`、`same_as`、`actual_root_entity`；另有 `platform`：送出當下平台對這張卡的說法（`severity`、`status`、`entities`、`rca_conclusion_type`、`rca_root_entity`）。沒看的項目不存鍵 |

寫入規則（`FeedbackSubmit` 驗證）：沒有 `incident_uid` 時只能是 `missed`，且要給 `pipeline`、`entity`、`time_range`；`resolution` 只能寫在 `true_positive`；寫了 `root_cause` 就要寫 `resolution`；`context` 只能寫在針對事件群、確認為真的回饋上；`review` 只能寫在針對事件群的回饋上，至少填一項，細節欄位只能跟對應的判定一起給（`ReviewIn`）。`incidents.rca_root_entity`（VARCHAR(256)，預設 `''`）記最近一次 RCA 的第一候選，覆核時抄進 `review.platform`。

## 9. `incident_knowledge`：歷史事故經驗庫（黃金事故）

`true_positive` 一律寫入（處置可以之後再補）。唯一鍵 `incident_uid`；索引 `(verdict, window_start)`、`(pipeline, window_start)`、`pipeline`。

| 欄位 | 型別 | 可空 | 預設 | 說明 |
|---|---|---|---|---|
| id | INTEGER | 否 | 自增 | |
| incident_uid | VARCHAR(40) | 否 | | 唯一 |
| pipeline | VARCHAR(128) | 否 | `''` | |
| entities | JSON | 否 | | 實體名稱陣列 |
| metrics | JSON | 否 | | 指標陣列 |
| signals | JSON | 否 | | 訊號類別陣列（latency／errors／traffic／saturation／lag） |
| resolution | VARCHAR(4000) | 否 | `''` | **空字串 = 還沒寫處置**；有寫處置才會出現在 RCA「歷史相似案例」 |
| root_cause | VARCHAR(1000) | 否 | `''` | |
| author | VARCHAR(128) | 否 | | |
| created_at | DateTime | 否 | 現在 | |
| verdict | VARCHAR(16) | 否 | `true_positive` | `true_positive` = 黃金事故；後來被改判 `false_positive` 就降級（不刪，留紀錄） |
| severity | VARCHAR(8) | 否 | `''` | |
| window_start | DateTime | 是 | | 事件群時窗（UTC）；黃金事故回測就重跑這一段 |
| window_end | DateTime | 是 | | |
| context | JSON | 否 | | 擴充資訊（根因下鑽、範例 trace id、tags、序列清單）；追加要用 `merge_context`，**不要整包覆蓋** |
| updated_at | DateTime | 否 | 現在 | |

## 10. `field_mappings`：欄位對照

| 欄位 | 型別 | 可空 | 說明 |
|---|---|---|---|
| id | INTEGER | 否 | |
| source_type | VARCHAR(64) | 否 | 資料源類型；有索引。trace／日誌的對照（`span.*`／`log.*`）要掛在 `grafana`；2026-11-20 之前掛在 `elasticsearch` 的已核准對照不會再被讀到，要重新提交 |
| fields | JSON | 否 | 物件，**來源欄位名 → 標準欄位名**（字串對字串），至少一組 |
| submitter | VARCHAR(128) | 否 | |
| status | VARCHAR(16) | 否 | `pending`、`approved`；每個 `source_type` 取 `approved` 的那筆 |
| approved_by | VARCHAR(128) | 是 | |

## 11. `external_anomalies`：外部系統推來的異常

只增不改；唯一鍵 `(source, external_id)`，同一來源的外部編號只收一次。索引 `status`、`pipeline`。

| 欄位 | 型別 | 可空 | 預設 | 說明 |
|---|---|---|---|---|
| id | INTEGER | 否 | 自增 | |
| pipeline | VARCHAR(128) | 否 | | |
| source | VARCHAR(64) | 否 | | 來源系統名稱（例如工廠 FDC） |
| external_id | VARCHAR(256) | 否 | | 來源系統的異常編號 |
| entity | VARCHAR(256) | 否 | | |
| metric | VARCHAR(512) | 否 | | |
| ts | DateTime | 否 | | |
| value | FLOAT | 是 | | |
| score | FLOAT | 否 | | |
| attrs | JSON | 否 | `{}` | |
| received_at | DateTime | 否 | 現在 | |
| status | VARCHAR(16) | 否 | `pending` | `pending`（還沒進過告警流程）、`processed`（已被某一輪處理） |

## 12. `trace_series`：由 trace 合成的每服務時序

唯一鍵 `(service, ts)`；索引 `service`。

| 欄位 | 型別 | 可空 | 說明 |
|---|---|---|---|
| id | INTEGER | 否 | |
| service | VARCHAR(256) | 否 | |
| ts | DateTime | 否 | 分鐘時間點（UTC） |
| count | INTEGER | 否 | 請求數 |
| error_rate | FLOAT | 否 | 0～1 |
| p95_ms | FLOAT | 否 | 毫秒 |

## 13. `service_edges`：服務呼叫圖（同步邊，DAG 的長期沉澱）

唯一鍵 `(caller, callee)`；索引 `last_seen`。由 `admin.topology_refresh` 從事故前的 trace 沉澱。

| 欄位 | 型別 | 可空 | 預設 | 說明 |
|---|---|---|---|---|
| id | INTEGER | 否 | 自增 | |
| caller | VARCHAR(256) | 否 | | 呼叫端 |
| callee | VARCHAR(256) | 否 | | 被呼叫端 |
| calls | INTEGER | 否 | `0` | 累計呼叫次數（資料庫端原子累加） |
| last_seen | DateTime | 是 | | 最近一次看到；NULL = 不知道多久沒出現，prune 不刪它 |

## 14. `service_async_edges`：非同步邊（訊息佇列）

**跟 `service_edges` 分開存**：方向相反，不能混進同步拓撲。唯一鍵 `(producer, consumer)`；索引 `last_seen`。

| 欄位 | 型別 | 可空 | 預設 | 說明 |
|---|---|---|---|---|
| id | INTEGER | 否 | 自增 | |
| producer | VARCHAR(256) | 否 | | 生產者 |
| consumer | VARCHAR(256) | 否 | | 消費者 |
| messages | INTEGER | 否 | `0` | 累計訊息數（原子累加） |
| lag_ms_p95 | FLOAT | 否 | `0.0` | **最近一次**看到的延遲 P95（毫秒），不是累計值，每次覆蓋 |
| last_seen | DateTime | 是 | | |

## 15. `lot_paths`：Lot 機台路徑（工廠場景）

| 欄位 | 型別 | 可空 | 說明 |
|---|---|---|---|
| id | INTEGER | 否 | |
| lot_id | VARCHAR(128) | 否 | **唯一** |
| path | JSON | 否 | 依時間排好的機台名稱**陣列**，例：`["EQP-A", "EQP-B", "EQP-C"]`；機台間流向（`lot_edges`）由它推導 |
| start | DateTime | 否 | |
| end | DateTime | 否 | |

## 16. 資料庫裡沒有的東西
- **RCA 報告不落庫**：報告由 `/rca` 現算現回，不存進任何表（`rca_report` 的結果只在記憶體與 API 回應中）。事件群只存 `context`（拓撲快照），讓報告可重現。
- **資源邊**（服務 → 資料庫、外部 API）不存：只在 RCA 當下從事實層的等待資料推出。
- **LLM 的輸入輸出**不存。

## 17. JSON 欄位的形狀

### 17.1 `incidents.incident`
```json
{
  "score": 439.6611,
  "severity": "P0",
  "tags": ["same_entity", "topology_chain", "absorbed"],
  "entities": ["ingress-gw", "order-svc", "payment-svc"],
  "start": "2026-11-09T12:00:00+00:00",
  "end": "2026-11-09T12:24:00+00:00",
  "events": [
    {"entity": "ingress-gw", "metric": "<指標查詢字串>", "ts": "2026-11-09T12:00:00+00:00",
     "score": 133.75, "value": 8962000.0, "attrs": {"dept": "payments", "signal": "latency"},
     "detector": "detector.hourly_baseline", "detector_node": "detect", "source_type": "prometheus",
     "voted_by": [], "level": "anomaly"}
  ]
}
```
- `tags`：收斂方式與標記，例：`same_entity`、`topology_chain`、`absorbed`（被進行中事件群吸收）、`escalated:P2>P0`（升級紀錄）。2026-11-20 之前的舊資料列可能還有 `fatal:<節點>`（致命硬上限；`detector.hard_limit`／`alert.fatal_floor` 已移除，平台內已經沒有算子會打這個標籤）。
- `events[].detector` 與 `detector_node`：抓到這筆異常的偵測算子與節點名稱（每個偵測節點的事件各自成立）。`voted_by` 現在一律是空陣列：表決節點 `detector.soft_vote` 已於 2026-11-20 移除，欄位留著只為了讀舊資料。2026-11-20 之前的舊資料列可能是 `detector.soft_vote`／`vote`（`voted_by` 列出當時有貢獻的節點）、`detector.ensemble`（布林多數決）或其他已移除的算子名。
- `events[].level`：`anomaly` 或 `warning`。

### 17.2 `incidents.context`
```json
{"topology": [["ingress-gw", "order-svc"], ["order-svc", "payment-svc"]], "lot_edges": []}
```
`topology` 是 `[caller, callee]` 的陣列；`lot_edges` 是 `[上游機台, 下游機台]` 的陣列。只留跟這件事有關的邊。

### 17.3 `detector_models.state`
**形狀由算子的 `get_state()` 決定**，不同算子不同；基準線不到期不重訓。主線現有的：

| 算子 | state 的鍵 |
|---|---|
| `detector.hourly_baseline` | `buckets`（時段編號字串 → `[平均, 標準差, 點數, 週數]`，共 168 格）、`global`、`tz`、`buckets_ready`、`min_weeks` |
| `detector.residual_shift` | `profile`（見下）、`noise`（`noise` = σ、`points`） |
| `detector.residual_chronic` | `profile`、`noise`、`window` |
| `detector.sparse_count` | `window`、`points`、`events`、`zero_fraction`、`dispersion`、`countable` |

`profile`（時段基準，`stages/detectors/base.fit_seasonal_profile`）：`tz`、`min_slot_points`、`slots`（週幾×小時編號字串 → `[平均, 點數]`）、
`hours`（一天中的小時 → `[平均, 點數]`）、`global`、`slots_ready`、`hours_ready`、`lowo_share`。

**節點開了 `snr`（節點參數）時**，不管哪個算子，`state` 另外多一個頂層鍵 `"_snr"`（基底寫入，演算法自己的 `load_state` 看不到這層）：
`signal_var`、`noise_sigma`、`noise_floored`、`slots_ready`、`lowo_share`、`points`、`status`（`ok`／`insufficient`／`flat`）、
`snr_lin`、`snr_db`（資料不足時是 null）、`p_fa`、`threshold`、`threshold_source`（`gaussian`／`empirical`）。
沒開 snr 的節點沒有這個鍵；舊基準線沒有這個鍵時照節點原本的 `threshold` 判，下次重訓才補上。

2026-11-20 以前訓練的基準線還會多帶 `integer`、`level` 與 `noise.tail_q`／`noise.fpr`（分母底噪與尾巴校正用的，已移除）：載入時不讀、不報錯，下次重訓就不再寫。

`detector.zscore`、`mad`、`quantile_drift`、`rules`、`hard_limit` 已於 2026-11-20 移除，資料庫裡它們留下的舊基準線列沒有算子會再讀。`state` 內只能是 JSON；非 JSON 的模型本體放 `artifact`。

### 17.4 `tuning_proposals.params`、`active_detector_configs.params`
參數名 → 值的物件，鍵只能是該偵測器 `tunable` 範圍內的參數，且在建立提案時檢查範圍。例：`{"threshold": 4.5, "window": 45}`。`include_entities`、`exclude_entities` 等路由參數會被拒絕。
2026-11-20 以前核准的列可能帶著已移除的鍵：`threshold_scale`（門檻倍率）、`min_scale`、`min_scale_ratio`、`fpr`、`max_tail_factor`。沒有補 migration：偵測載入調參時把不認得的鍵略過並記一行 `params:<節點>:ignored-unknown:<鍵>`（`stages/detectors/base.tuning_keys`／`layer_params`），其餘的鍵照用；新提案帶這些鍵會回 400。

### 17.5 `tuning_proposals.backtest`
```json
{"metric": {"source_type": "...", "query": "..."}, "points": 0, "train_points": 0, "test_points": 0, "warmup_points": 0,
 "split_at": "ISO 時間", "current_params": {}, "proposed_params": {}, "workflow": {"locked": ["..."]},
 "current_anomalies": 0, "proposed_anomalies": 0, "missed_windows": [],
 "safety": {"passed": true, "missed_golden": [], "skipped_oldest": 0}}
```
`safety.passed` 為 false（會漏抓已確認的真異常）時，提案狀態是 `rejected`，不能核准。`workflow.locked` 列出被 YAML 鎖住、提案改了也不會生效的鍵。`safety` 內部欄位以程式為準（`control_plane/learning/golden.py`）。
管線裡這個偵測器的節點開了 snr 時，提案是**整段回測**（`control_plane/learning/segment.py`；2026-11-20 以前在 `snr_feedback.py`），形狀不同：
多 `"mode": "segment"`、`entity`、`test_days`、`current_events`／`proposed_events`、`current_minutes_per_week`／`proposed_minutes_per_week`；
沒有 `warmup_points`、`current_params`、`missed_windows`；`safety` 也多 `"mode": "segment"`，每場黃金事故記的是整段偵測的告警分鐘。

### 17.6 `incident_knowledge.context`
擴充資訊的物件，頂層鍵由寫入者決定，追加用 `merge_context`（資料庫端合併頂層鍵）。確認真異常時固定會寫入 `series`（事件群的序列清單，每項含實體、資料源、查詢），另外可放根因下鑽、範例 trace id、tags、處方註記。

### 17.7 `anomalies.attrs`、`external_anomalies.attrs`
target 的 `attrs` 原樣複製：`dept`、`signal`（`latency`／`errors`／`traffic`／`saturation`／`lag`）等；沒有固定鍵集合。

## 18. 沒驗證的
- JSON 欄位的鍵集合，我只核對了開發資料庫裡有資料的表（`incidents`、`anomalies`、`detector_models`）和程式碼；`tuning_proposals`、`incident_knowledge`、`field_mappings`、`external_anomalies`、`lot_paths` 的開發資料庫是空的，它們的形狀來自程式碼與請求驗證，沒有對照實際資料。
- 第 17.3 節只列了四個算子的 `state`，其餘算子的鍵沒逐一核對。
- `tuning_proposals.source` 的 `feedback`、`profile` 值，是從程式裡寫入的位置看到的，建立 API 的驗證只收 `auto`、`llm`、`manual`。
- 這份文件對應 main（tag 1.1）的模型，加上分支 `feat/adopt-r-snr` 的 JSON 形狀補充（residual 兩個偵測器的 `state`、`"_snr"`、
  `threshold_scale`、整段回測）；2026-11-20 移除 `threshold_scale` 等鍵後改了第 17.1、17.3、17.4、17.5 節的說明。表與欄位沒有改；這些補充只對照了程式碼與單元測試，沒有對照實際資料庫裡的資料。
