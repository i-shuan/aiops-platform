# OTel 端到端驗證結果（2026-11-12）

環境：本機拋棄式 Elasticsearch 8.19.23 ＋ otel-collector-contrib 0.165.0（elasticsearch exporter 預設設定＝otel mapping），
`AIOPS_ES_DATA_MODEL=otel`、暫存 SQLite、`AIOPS_NOTIFIER=log`、LLM `noop`。沒有碰叢集的 es-test。

## 資料（`gen_incident.py`，經真的 Collector 寫入）

- 90 分鐘、每 10 秒一條 checkout 請求（540 條），head sampling 50%（`ot=th:8`，整條 trace 一起丟）→ 實際寫入 279 條、2232 個 span
- 呼叫：checkout → cart-api → pg-main（PostgreSQL）、redis-cache；checkout → tax-gw（外部 HTTPS）；checkout → Kafka `orders` → ord-svc
- 事故：最後 20 分鐘 pg-main 40 → 400 ms、30% 失敗 → cart-api 500 → checkout 502。錯誤日誌 35 筆（21 筆事故、14 筆平常的雜訊）
- **標準答案：cart-api（等 pg-main）**

## 結果（`run_e2e.py`）

| 階段 | 結果 |
| --- | --- |
| 拓撲（`admin.topology_refresh`，90 分鐘） | 2 次 ES 查詢、2200 個 span、未截斷；同步邊 checkout → cart-api（275 次）、非同步邊 checkout → ord-svc（Kafka，275 則、延遲 P95 306 ms） |
| 偵測（`avg(duration)` of Server／Consumer span，MAD） | 6 次 ES 查詢（3 個服務 × 當輪＋訓練），8～34 ms／次；cart-api、checkout 各 21 個異常點 12:52–13:12Z（事故 12:52:12 開始）；ord-svc 0 點（沒被影響）；事故窗外 0 點 |
| 事件群 | 1 個（cart-api＋checkout，P0），`topology_chain` 依沉澱的拓撲併成一個 |
| `rca_report` | 0.5 秒、6 次 ES 查詢（trace 2、日誌 4），最慢 47 ms；864 個 span、108 條 trace、未截斷；日誌 26＋基準 7 筆 |
| **根因第 1 名** | **cart-api，分數 1.00**：自身耗時 77.6 → 439.0 ms／請求，其中等待 `pg-main:5432`（db/postgresql）39.7 → 400.9 ms、資源失敗 0 → 21 筆；第 2 名 checkout 0.0008（未達顯著門檻） |
| `trace_evidence` | 範例 trace「入口 502，cart-api、pg-main:5432 失敗」，呼叫樹 checkout → cart-api → pg-main 459 ms |
| `log_patterns` | `[cart-api] PSQLException: checkout failed: FATAL: remaining connection slots are reserved (pool <NUM>)` 事件期間 21 筆、新出現（約 42 倍） |
| errors | 無；略過段落 `golden_signals`（沒有黃金訊號序列）、`similar_cases`（暫存 DB 沒有歷史事故） |

直接對 ES 計數核對：cart-api Server span 事故前 15 分鐘 55 筆、平均 77.6 ms；事故期間 53 筆、平均 439.0 ms、`status.code=Error` 21 筆——跟報告一致。

## 沒驗到的

- 真實生產環境的 OTel 資料（這裡是自己組的 OTLP，經真的 Collector／ES，但呼叫型態是劇本）
- 偵測只用 MAD：90 分鐘的資料不夠 `hourly_baseline`（要以週計）；`it_otel` 的多數決沒跑
- 取樣權重在這份資料裡全部是 2（均勻取樣），比例不受影響；非均勻取樣（尾端取樣）的效果只有單元測試（`tests/test_es_data_models.py`）
- 已核准欄位對照（DB 裡的 `span.*`／`log.*`）經整條 `rca_report` 的路徑：只有單元測試
- Internal span、gRPC、HTTP 以外的 RPC、Collector 的 ecs mapping（死區，見上一層 README）
