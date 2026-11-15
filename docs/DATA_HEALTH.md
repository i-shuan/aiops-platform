# 資料健康度：資料有沒有進來、是誰的問題

平台每一輪排程都會記下兩件事，事後查得到「那個時間點資料有沒有進來」：

1. **每條資料流**（管線 × 實體 × 查詢）這一輪有沒有抓到資料 → 資料表 `data_stream_health`
2. **整條管線**這一輪的執行結果 → 資料表 `pipeline_run_health`

目的是劃清權責：資料沒送進來、查詢對不到東西、欄位不存在，是資料源端或設定的缺口，平台照實記下來，不在引擎裡猜或補（CLAUDE.md §1.1 第 4 點）。

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

「以前有過資料」的判斷：這條資料流有過 `ok` 的健康度紀錄，或已經有偵測基準線，或存過異常點（後兩者是為了這個功能上線之前就在跑的資料流）。

管線（`RUN_STATUSES`）：

| 狀態 | 意思 |
| :-- | :-- |
| `ok` | 整輪成功、沒有任何序列出錯 |
| `degraded` | 整輪完成，但有序列（或旁路節點）記了錯誤 |
| `infrastructure_error` | 整輪失敗：資料庫、資料源、Alertmanager 連不上，或資料斷流比例達到 `max_failed_target_ratio`（心跳不續命） |
| `workflow_error` | 管線定義載入失敗（YAML 寫錯、算子參數不合法）：**這一輪根本沒跑** |
| `error` | 整輪因為其他例外失敗 |

## 怎麼存：狀態區間，不是每輪一列

每一列是一段連續相同的狀態：`status`、`started_at`、`last_checked_at`、`message`（區間內最近一次的訊息）、
`checks`／`runs`（這段涵蓋幾輪）、`is_current`（是不是最新的一段）。狀態沒變只把 `last_checked_at` 往後推；狀態變了才開新的一列。

讀法：

- 某個時間點落在某一列的 `started_at` ～ `last_checked_at` 之間 → 那個時間點就是那個狀態。
- **沒有被任何區間蓋到的時段 = 平台那段時間沒有檢查到**（管線沒跑、整輪在輪到這條資料流之前就失敗、或紀錄寫不進資料庫），**不是「正常」**。

## 在哪裡記

| 情況 | 誰判 | 誰寫 |
| :-- | :-- | :-- |
| 每條序列跑完（成功或出錯） | 引擎 `engine/pipeline.py` 呼叫 `engine/health.stream_status`，結果放在這一輪的 `ctx.stream_checks` | `StorageHooks.record_health` |
| 整輪結束或失敗 | 引擎呼叫 `engine/health.run_status`，放在 `ctx.run_check` | 同上（跟 `persist_batch` 分開的交易：整輪失敗時最終結果沒有寫，但「失敗了」要留下來） |
| 排程的 YAML 載入失敗（管線沒跑） | 排程入口用的 `interfaces/runner.run_scheduled` | 同上 |

只記**排程模式**（`strict=False`）而且有逐序列節點的管線。手動動作（`admin_*`、手動回測）與 RCA 不記：它們的失敗直接回給呼叫的人。
`stages/` 的算子完全不知道這件事，也碰不到資料庫。

## 怎麼查

API（要 token）：

```
GET /api/v1/data-health/streams?current=true                    目前每條資料流的狀態
GET /api/v1/data-health/streams?status=config_error             設定或查詢錯誤的區間
GET /api/v1/data-health/streams?entity=cart-api&since=<時間>&until=<時間>   跟這段時間有重疊的區間（查某個時間點：since = until）
GET /api/v1/data-health/runs?pipeline=it_otel                   管線執行結果的區間
```

Grafana：儀表板「資料健康度」（`deploy/helm/aiops-grafana/files/dashboards/data-health.json`），用唯讀帳號直接讀這兩張表。
設定方式見 `deploy/helm/aiops-grafana/README.md`；建唯讀帳號的 SQL 在 `migrations/postgres/0011_data_health.sql`。

## 已知限制

1. **資料庫連不上時記不了。** 健康度紀錄就存在平台資料庫裡；資料庫斷線的那段時間不會有任何紀錄（只有一行 log：`資料健康度寫入失敗`）。
   那段時間在歷史上是空白。要知道平台當時是不是活著，看 `notifier.watchdog` 的心跳（Alertmanager 端）與排程的備援通報。
2. **整輪中途失敗時，還沒輪到的資料流沒有紀錄。** 資料源連不上（`unreachable`）會讓整輪立刻失敗：連不上的那一條與它之前的有紀錄，之後的沒有。
3. **同儕查詢失敗會算在這條資料流頭上。** 一條序列的同儕（`peers`／`peer_group`）或同比環比（`lags_minutes`）抓不到時，整條序列算沒抓到，
   紀錄的鍵是 target 自己的查詢；實際是哪一個查詢出問題要看訊息。
4. **「從未有過資料」靠平台自己的紀錄判斷。** 這個功能上線之前就在跑、但既沒有基準線也沒存過異常點的資料流（例如只掛 `detector.rules`、從沒異常過），
   上線後第一次斷流會被記成 `never_seen` 而不是 `no_data`。
5. **區間看不出中間少跑了幾輪。** 狀態沒變時只推 `last_checked_at`；排程少跑一輪不會留下缺口，只能從 `checks`／`runs` 與區間長度對不上看出來。
6. **管線定義載入失敗只有排程會記，而且用的是呼叫時給的名稱。** API 或手動觸發時 YAML 寫錯，錯誤直接回給呼叫的人、不進健康度歷史（維持「設定錯誤在碰資料庫之前就擋下」，`tests/test_fail_fast.py`）。 讀不到 YAML 裡的 `name`；內建管線兩者相同，自己的 YAML 檔用檔名（去掉副檔名）。
7. **時間是那一輪的邏輯時間（`now`）。** 用 `--now` 回放比現有紀錄更早的時間不會寫（不改寫歷史）；回放較晚的時間會照寫。
8. **同一條管線同時只能有一輪在跑。** 兩輪重疊執行可能各開一列；下一次寫入會把多出來的關掉，但那兩列的起訖時間可能重疊。
9. **RCA 隨需撈的 trace／日誌失敗不在這裡。** 那些記在該次 RCA 回傳的 `errors`，不是排程資料流。
10. **保留期沒有自動清理。** 資料量 = 狀態變化次數（不是輪數）；清理的 SQL 在 `migrations/postgres/0011_data_health.sql`。

## 驗證狀態

- 平台端（狀態判斷、狀態區間、整輪失敗與載入失敗的紀錄、API）：`tests/test_data_health.py`，資料源是 mock、資料庫是 SQLite。
- 儀表板：JSON 能載入、每一條 SQL 對 ORM 建出來的表實際執行並檢查結果（SQLite；只把 PostgreSQL 的時間加減換成 SQLite 寫法）。
- 2026-11-15 在本機叢集的臨時 namespace（拋棄式 `postgres:16-alpine` 與 `grafana/grafana:13.0.7`，驗完已刪）實際跑過一次：
  - 兩支 migration（0010、0011）各連跑兩次都成功；只用 migration 建出來的表，平台照常寫入。
  - 出貨的 `metrics_only` 管線（mock 資料源，模擬斷流、查詢改壞、回 200 零筆、全部斷流導致整輪失敗）跑 37 輪：
    185 次資料流檢查只存 17 列，狀態區間與模擬的時間一致。
  - migration 註解裡的唯讀帳號：儀表板 9 條 SQL 原樣（只展開 Grafana 的巨集與變數）都能執行；讀 `incidents`、`feedback` 與寫入都被拒絕。
  - 用這個 chart 部署的 Grafana 開「資料健康度」：7 個面板都有畫出來，狀態的中文與顏色正確，下拉選單可以打字篩選、選單一服務。
    這一步抓到一個只靠測試看不到的問題：多條序列合併時別的序列的時間點會變成空值，把時間軸的色塊切斷、整片灰色；
    改成面板開 `spanNulls`、「之後沒有紀錄」用 -1 表示之後正確（測試已補上這兩點）。
- **沒有驗過的**：真實資料源（Elasticsearch／Prometheus）的錯誤訊息實際長什麼樣子、幾百條資料流時畫面與查詢的速度、
  正式環境的 PostgreSQL（TLS、權限、既有資料量）、Grafana 13.0.7 以外的版本。
