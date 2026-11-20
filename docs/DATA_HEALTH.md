# 資料健康度：資料有沒有進來、是誰的問題

平台每一輪排程都會留底，事後查得到「那個時間點資料有沒有進來、平台有沒有在跑」。
三張表，都放在平台現有的同一個 PostgreSQL（不另開資料庫）：

| 資料表 | 一列是什麼 | 用途 |
| :-- | :-- | :-- |
| `data_stream_health` | 一條資料流（管線 × 實體 × 查詢）的一段連續相同狀態 | 那個時間點這條資料流有沒有資料 |
| `pipeline_runs` | 一條管線的一輪 | 那一輪有沒有跑、成功還是失敗、失敗類型 |
| `data_stream_first_seen` | 一條資料流第一次收到資料的時間 | 分「從未有過資料」與「有過之後斷流」 |

目的是劃清權責：資料沒送進來、查詢對不到東西、欄位不存在，是資料源端或設定的缺口，平台照實記下來，不在引擎裡猜或補（CLAUDE.md §1.1 第 4 點）。
之後改成「各團隊自己的告警推進來、平台只做收斂與 RCA 建議」時，這套紀錄用來回答：是對方沒送，還是平台沒收到／沒在跑。

## 資料表與欄位

**`data_stream_health`**（狀態區間：狀態沒變只把 `last_checked_at` 往後推，變了才開新的一列）

| 欄位 | 說明 |
| :-- | :-- |
| `pipeline`、`entity`、`query` | 哪一條資料流 |
| `source_type` | 資料源種類 |
| `status` | 見下面的狀態表 |
| `message` | 這一段最近一次的訊息（資料源的錯誤原文，最長 1000 字） |
| `started_at`、`last_checked_at` | 這一段從什麼時候開始、最後一次確認是什麼時候 |
| `checks` | 這一段涵蓋幾輪檢查 |
| `is_current` | 是不是這條資料流最新的一段 |

**`pipeline_runs`**（一輪一列）

| 欄位 | 說明 |
| :-- | :-- |
| `pipeline`、`run_at` | 哪條管線的哪一輪（唯一；同一輪重跑覆蓋） |
| `status` | `ok` 成功／`degraded` 跑完但有序列出錯／`failed` 整輪失敗 |
| `failure_type` | 只有 `failed` 才有：`infrastructure_error`／`workflow_error`／`error` |
| `message` | 失敗原因或出錯的實體 |
| `targets`、`failed_targets` | 這一輪的序列數、出錯的實體數 |

**`data_stream_first_seen`**：`pipeline`、`entity`、`query`、`first_data_at`（一條資料流一列，只增不改）

## 狀態

資料流（`aiops/engine/health.py` 的 `STREAM_STATUSES`）——只看「資料有沒有抓回來」，跟後面的偵測節點有沒有出錯無關：

| 狀態 | 意思 | 通常是誰的事 |
| :-- | :-- | :-- |
| `ok` | 這一輪抓回了資料 | — |
| `no_data` | 查詢合法但沒有資料（資料源說沒資料、或回 200 但零筆），而且這條資料流**以前有過資料** = 斷流 | 資料源端（服務停了、埋點拿掉、索引滿了） |
| `never_seen` | 同上，但這條資料流**從來沒有過資料** | 還沒埋點、或查詢對不到任何東西 |
| `config_error` | 資料源說查詢不合法、欄位不存在、回應格式不對 | 管線設定（查詢、欄位對照） |
| `unreachable` | 抓這條資料流時資料源連不上（整輪也會失敗） | 資料源或網路 |
| `error` | 抓到資料之前出了其他錯（資料源程式的非預期例外等） | 看訊息 |

「以前有過資料」的判斷：`data_stream_first_seen` 有這條資料流，或已經有偵測基準線，或存過異常點（後兩者是為了這個功能上線之前就在跑的資料流）。

管線每一輪的失敗類型（`FAILURE_TYPES`）：

| 失敗類型 | 意思 |
| :-- | :-- |
| `infrastructure_error` | 資料庫、資料源、Alertmanager 連不上，或資料斷流比例達到 `max_failed_target_ratio`（心跳不續命） |
| `workflow_error` | 管線定義載入失敗（YAML 寫錯、算子參數不合法）：**這一輪根本沒跑**，但會有一列 |
| `error` | 其他例外（訊息開頭是例外的型別） |

## 讀法

- 資料流：某個時間點落在某一列的 `started_at` ～ `last_checked_at` 之間 → 那個時間點就是那個狀態。
  **沒有被任何區間蓋到的時段 = 平台那段時間沒有檢查到**，不是「正常」。
- 管線：那個時間有一列 → 那一輪的結果；**沒有列 = 那一輪沒有跑**（排程沒觸發、或紀錄寫不進資料庫）。
  資料流的區間看不出中間少跑了幾輪，要對照 `pipeline_runs`。

## 索引

Grafana 是用 SQL 主動查（每次開儀表板、每次自動重新整理），所以查詢用到的欄位都有索引，跟建表放在同一支 migration：

| 資料表 | 索引 | 給誰用 |
| :-- | :-- | :-- |
| `data_stream_health` | `ix_data_stream_health_stream`（pipeline, entity, query, started_at） | 某條資料流的歷史；寫入時找它目前的那一列 |
| | `ix_data_stream_health_current`（is_current, status） | 目前有問題的資料流 |
| | `ix_data_stream_health_last_checked`（last_checked_at） | 時間範圍、保留期限的清除 |
| `pipeline_runs` | `uq_pipeline_runs_pipeline_run_at`（pipeline, run_at，唯一） | 某條管線的時間範圍；同一輪重跑覆蓋 |
| | `ix_pipeline_runs_run_at`（run_at） | 不分管線的時間範圍、保留期限的清除 |
| `data_stream_first_seen` | `uq_data_stream_first_seen_stream`（pipeline, entity, query，唯一） | 每輪每條資料流查一次「以前有沒有過資料」 |

## 保留期限

管線 YAML 的 `health_retention_days`（預設 **180** 天，載入期檢查 1～3650）。做法跟拓撲刷新一樣：**每一輪寫入時順手清掉這條管線超過期限的舊紀錄**，不用另外排程。

| 資料表 | 清什麼 | 不清什麼 |
| :-- | :-- | :-- |
| `data_stream_health` | 已經結束的區間（`is_current = false`）而且 `last_checked_at` 超過期限 | 每條資料流目前的那一列 |
| `pipeline_runs` | `run_at` 超過期限的輪次 | — |
| `data_stream_first_seen` | 不清 | 全部（一條資料流一列） |

量的估計（180 天）：
- `pipeline_runs` 跟輪數成正比：每 5 分鐘一輪的一條管線約 5.2 萬列；每分鐘一輪約 26 萬列。
- `data_stream_health` 跟狀態變化次數成正比，不是輪數。

## 在哪裡記

| 情況 | 誰判 | 誰寫 |
| :-- | :-- | :-- |
| 每條序列跑完（成功或出錯） | 引擎 `engine/pipeline.py` 呼叫 `engine/health.stream_status`，結果放在這一輪的 `ctx.stream_checks` | `StorageHooks.record_health` |
| 整輪結束或失敗 | 引擎呼叫 `engine/health.run_status`，放在 `ctx.run_check` | 同上（跟 `persist_batch` 分開的交易：整輪失敗時最終結果沒有寫，但「失敗了」要留下來） |
| 排程的 YAML 載入失敗（管線沒跑） | 排程入口用的 `interfaces/runner.run_scheduled` | 同上 |

只記**排程的輪次**（`strict=False`、不是針對某個事件群、也不是帶請求的手動動作）。手動動作（`admin_*`、手動回測）與 RCA 不記：它們的失敗直接回給呼叫的人。
只吃外部告警的使用者管線（不抓資料、沒有資料流）也記：每一輪一列 `pipeline_runs`（`targets` 是 0），證明平台那時候有在跑——告警沒來時，配合外部異常的接收時間（`external_anomalies.received_at`，依 `source` 分），分得出是對方沒推還是平台沒跑。
`stages/` 的算子完全不知道這件事，也碰不到資料庫；三張表只能透過 `storage/repository.py`（`StreamHealth`、`PipelineRuns`、`StreamFirstSeen`）讀寫。

## 怎麼查

API（要 token）：

```
GET /api/v1/data-health/streams?current=true                    目前每條資料流的狀態
GET /api/v1/data-health/streams?status=config_error             設定或查詢錯誤的區間
GET /api/v1/data-health/streams?entity=cart-api&since=<時間>&until=<時間>   跟這段時間有重疊的區間（查某個時間點：since = until）
GET /api/v1/data-health/runs?pipeline=it_otel&since=<時間>&until=<時間>     那段時間的每一輪
GET /api/v1/data-health/runs?status=failed&failure_type=workflow_error      定義載入失敗的輪次
GET /api/v1/data-health/first-seen?pipeline=it_otel             每條資料流第一次收到資料的時間
```

Grafana：儀表板「資料健康度」（`deploy/helm/aiops-grafana/files/dashboards/data-health.json`）。每個查詢（含兩個下拉選單）都只查畫面的時間範圍：

| 畫面上的東西 | 讀哪張表 | 時間條件 |
| :-- | :-- | :-- |
| 下拉選單「管線」 | `pipeline_runs` | `$__timeFilter(run_at)` |
| 下拉選單「服務」 | `data_stream_health` | 跟時間範圍有重疊的區間 |
| 1 目前有問題的資料流 | `data_stream_health` | `is_current` 而且 `$__timeFilter(last_checked_at)` |
| 2 管線最近一輪的執行結果（含範圍內輪數、失敗輪數） | `pipeline_runs` | `$__timeFilter(run_at)` |
| 3 每條資料流的狀態時間軸 | `data_stream_health` | 跟時間範圍有重疊的區間 |
| 4 管線每輪的執行結果（時間軸） | `pipeline_runs` | `$__timeFilter(run_at)`，依 `$__interval` 分格取最嚴重的 |
| 5 沒有成功的輪次 | `pipeline_runs` | `$__timeFilter(run_at)`，最多 500 列 |
| 6 設定錯誤清單 | `data_stream_health` ＋ `pipeline_runs` | 兩邊各自的時間條件，最多 500 列 |
| 7 資料流出問題的紀錄 | `data_stream_health` | 跟時間範圍有重疊的區間，最多 500 列 |

狀態區間的時間條件寫成「`last_checked_at >= $__timeFrom()` 而且 `started_at <= $__timeTo()`」而不是 `$__timeFilter`：
一段區間可能從範圍之前開始、延續到範圍之後，只篩單一欄位會漏掉它。

唯讀帳號（`aiops_grafana_ro`）只有 `data_stream_health`、`pipeline_runs` 的 `SELECT`；建帳號與授權的 SQL 在
`migrations/postgres/0011_data_health.sql` 的註解，密碼只放進 Grafana 的 Secret。設定方式見 `deploy/helm/aiops-grafana/README.md`。

## 已知限制

1. **資料庫連不上時記不了。** 紀錄就存在平台資料庫裡；資料庫斷線的那段時間不會有任何紀錄（只有一行 log：`資料健康度寫入失敗`）。
   那段時間在歷史上是空白。要知道平台當時是不是活著，看 `notifier.watchdog` 的心跳（Alertmanager 端）與排程的備援通報。
2. **整輪中途失敗時，還沒輪到的資料流沒有紀錄。** 資料源連不上（`unreachable`）會讓整輪立刻失敗：連不上的那一條與它之前的有紀錄，之後的沒有。
3. **同儕查詢失敗會算在這條資料流頭上。** 一條序列的同儕（`peers`／`peer_group`）或同比環比（`lags_minutes`）抓不到時，整條序列算沒抓到，
   紀錄的鍵是 target 自己的查詢；實際是哪一個查詢出問題要看訊息。
4. **「從未有過資料」靠平台自己的紀錄判斷。** 這個功能上線之前就在跑、但既沒有基準線也沒存過異常點的資料流（例如偵測節點訓練點數一直不夠、沒存過基準線，也從沒異常過），
   上線後第一次斷流會被記成 `never_seen` 而不是 `no_data`。
5. **狀態來回跳的資料流會退化成每輪一列。** 沒有做防抖動：把短暫的斷流合併掉，紀錄就不再照實反映那個時間點有沒有資料。靠保留期限限制總量。
6. **管線定義載入失敗只有排程會記，而且用的是呼叫時給的名稱。** API 或手動觸發時 YAML 寫錯，錯誤直接回給呼叫的人、不進健康度歷史
   （維持「設定錯誤在碰資料庫之前就擋下」，`tests/test_fail_fast.py`）。載入失敗時讀不到 YAML 裡的 `name` 與保留期限：
   名稱用檔名（內建管線兩者相同），那一輪不清舊紀錄。
7. **時間是那一輪的邏輯時間（`now`）。** 用 `--now` 回放比現有紀錄更早的時間：資料流的狀態區間不改寫，那一輪本身照記一列。
   保留期限的基準也是這個時間：用很久以後的 `--now` 回放會把現在的紀錄當成過期清掉。
8. **同一條管線同時只能有一輪在跑。** 兩輪重疊執行可能各開一段區間；下一次寫入會把多出來的關掉，但那兩段的起訖時間可能重疊。
9. **已經不再跑的管線沒有人替它清。** 清除是那條管線自己每輪寫入時順手做的；管線下線後它的紀錄要手動刪（SQL 在 migration 的註解）。
10. **時間軸不畫中間的缺口。** 兩次紀錄之間畫成前一個狀態延續；少跑的輪次要看「管線最近一輪」那張表的範圍內輪數，或直接查 `pipeline_runs`。
11. **每輪每條資料流都會更新一次 `last_checked_at`**，而這個欄位有索引（時間範圍與清除要用）：寫入量跟「資料流數 × 輪數」成正比，
    靠 PostgreSQL 的 autovacuum 回收。沒有在真實負載下量過。
12. **RCA 隨需撈的 trace／日誌失敗不在這裡。** 那些記在該次 RCA 回傳的 `errors`，不是排程資料流。

## 驗證狀態

- 平台端（狀態判斷、三張表的寫入、保留期限、整輪失敗與載入失敗的紀錄、API）：`tests/test_data_health.py`，資料源是 mock、資料庫是 SQLite。
- 儀表板：JSON 能載入；每一條 SQL 對 ORM 建出來的表實際執行並檢查結果；每一條都有時間條件、時間範圍外不回任何列
  （SQLite；只把三處 PostgreSQL 的時間運算換成 SQLite 寫法）。
- 2026-11-15 在本機叢集的臨時 namespace（拋棄式 `postgres:16-alpine` 與 `grafana/grafana:13.0.7`，驗完已刪）實際跑過：
  - 兩支 migration（0010、0011）各連跑兩次都成功；只用 migration 建出來的三張表與索引，平台照常寫入。
  - 出貨的 `metrics_only` 管線（mock 資料源，模擬斷流、查詢改壞、回 200 零筆、全部斷流導致整輪失敗、兩輪故意不跑）跑 35 輪：
    狀態區間 17 列、`pipeline_runs` 35 列（32 輪有序列出錯、3 輪整輪失敗）、`first_seen` 4 列（從未有資料的那條沒有）。
  - 保留期限設 2 天：10 天前寫入的輪次與已結束的區間在下一輪被清掉；每條資料流目前的那一列與 `first_seen` 都留著。
  - 唯讀帳號：儀表板 9 條 SQL 原樣（只展開 Grafana 的巨集與變數）都能執行；讀 `incidents`、`feedback`、`data_stream_first_seen` 與寫入都被拒絕。
  - 灌量後看查詢計畫（`pipeline_runs` 518 萬列／774 MB = 20 條管線 × 每分鐘一輪 × 180 天；`data_stream_health` 43 萬列／90 MB
    = 300 條資料流 × 每條每天 8 段 × 180 天），用唯讀帳號、All 管線、All 服務：
    - 時間範圍 24 小時：9 條查詢全部走索引，最慢 138 ms（資料流時間軸，回 5,100 個點）。
    - 時間範圍 30 天：全部走索引；資料流時間軸 1.8 秒（回 14 萬個點，這份灌的資料每條資料流都出過問題）、管線時間軸 0.9 秒，其餘 140 ms 以內。
      長時間範圍請用「服務」下拉選單縮小。
  - 用這個 chart 部署的 Grafana 開「資料健康度」：7 個面板都有畫出來，下拉選單（SQL 帶時間條件）有值，狀態的中文與顏色正確。
    第一版在這一步抓到一個只靠測試看不到的問題：多條序列合併時別的序列的時間點會變成空值，把時間軸的色塊切斷、整片灰色；
    改成面板開 `spanNulls`、「之後沒有紀錄」用 -1 表示之後正確（測試已補上這兩點）。
- **沒有驗過的**：真實資料源（Elasticsearch／Prometheus）的錯誤訊息實際長什麼樣子、真實負載下的寫入與 autovacuum、
  幾百條資料流同時畫在瀏覽器裡的流暢度、正式環境的 PostgreSQL（TLS、權限）、Grafana 13.0.7 以外的版本。
