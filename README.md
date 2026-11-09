# aiops-platform

AIOps 平台：從監控指標抓異常、收斂成事件群、抑制與分送通知、產出根因報告，並讓維運人員的回饋回頭調整門檻。

架構參考 Apache Superset 後端：**Command 是固定流程，Plugin 是可抽換零件，Preset 決定這個場景用哪些零件**。換場景（IT／工廠／純指標）不另寫一套流程，換演算法也不用改流程。

> 開發守則（分層、禁令、檔案該放哪）都在 [CLAUDE.md](CLAUDE.md)，而且大多由 `tests/test_architecture.py` 機械式檢查，違規就測試紅燈。

## ⚠️ 上正式環境前必須更換的憑證

目前是**開發階段**，下列憑證是為了方便驗證隨意設定的，或曾以明文出現在開發紀錄與指令輸出中。**上正式環境前全部要換**，新的值一律放 Kubernetes Secret，不要寫進 repo 或 values 檔。

### 本平台（aiops-platform）會讀的

| 環境變數 | 用途 | 要怎麼換 |
|---|---|---|
| `AIOPS_API_TOKEN` | 呼叫本平台 API 的 Bearer token | 產生隨機長字串（例如 `openssl rand -hex 32`）。可用逗號同時放新舊兩組，等呼叫端都換完再拿掉舊的 |
| `AIOPS_DATABASE_URL` | 資料庫連線（含帳密） | 正式 PostgreSQL 開專用帳號與密碼，只給本平台的資料庫權限 |
| `AIOPS_ES_API_KEY` | 讀 Elasticsearch 的 key | 在 ES 建**唯讀**的 API key，只給需要的索引 pattern 讀取權限。建法見下面「建立平台的唯讀 ES key」。**目前還沒建**，所以平台連不上叢集的 ES（回 401）；真實資料驗收（見「目前狀態」）卡在這裡 |
| `AIOPS_PROMETHEUS_TOKEN` | 讀 Prometheus 的 token（選填） | 依公司 Prometheus 的驗證方式核發 |
| `AIOPS_VICTORIAMETRICS_TOKEN` | 讀 VictoriaMetrics 的 token（選填） | 同上 |
| `AIOPS_ALERTMANAGER_TOKEN` | 推送通知到 Alertmanager 的 token（選填） | Alertmanager 前面有加驗證才需要 |
| `AIOPS_FALLBACK_WEBHOOK` | 資料庫掛掉時直接打的備援 webhook | webhook URL 本身就是憑證，正式值放 Secret |

#### 建立平台的唯讀 ES key

**本機開發的金鑰統一放在 `secrets.local.yaml`**（repo 根目錄，已被 `.gitignore` 擋住，不會進 git）。格式見 [secrets.local.example.yaml](secrets.local.example.yaml)，複製一份改名填值即可。換電腦要自己把這個檔搬過去；repo 裡永遠看不到值。這個檔不給 Helm 用，部署到叢集仍然走 `existingSecret`。

ES 的 `elastic` 超級帳號密碼存在叢集 Secret `es-test/es-elastic`，**不進 repo**。叢集上沒有 Kibana，所以用一支腳本直接呼叫 ES 的 API 建唯讀 key（只給 `metrics-apm.transaction.1m-*`、`traces-apm-*`、`logs-apm.*` 的 `read` 與 `view_index_metadata`）：

```bash
.venv\Scripts\python tests\dev_es_key.py
```

它會自己開 port-forward、讀 `es-elastic` 密碼、建 key，再把 key 寫進 `secrets.local.yaml` 的 `es.AIOPS_ES_API_KEY`（只換那一行，其他內容不動；`secrets.local.yaml` 沒被 `.gitignore` 擋住就拒絕寫）。畫面上只印 key 的 id，不印密碼或 key。**要建 key 得用管理員密碼，所以這支要由有叢集權限的人自己跑**；每跑一次多一把新 key，舊的不會自動撤銷。

這把 key 是開發階段用的，跟上表其他憑證一樣，**上正式環境前要撤銷重發**。

### 本平台的部署設定

| 位置 | 內容 | 要怎麼換 |
|---|---|---|
| `deploy/helm/aiops-alertmanager` 的 `receivers` | Slack／Teams／PagerDuty 的 webhook URL | 目前驗證用的 receiver 沒有設任何通道。正式設定時用 `-f` 帶入不進 repo 的 values 檔，chart 會把整份設定存成 Secret |

### 開發叢集上的其他系統（不是本平台的程式在用，但開發時曾曝光）

| 位置 | 內容 | 要怎麼換 |
|---|---|---|
| `values-local.yaml` 的 `llm.apiKey` | OpenAI API key（舊 aiops-engine 的本機部署設定，已被 `.gitignore` 擋住、沒有進 git） | 到 OpenAI 後台撤銷並重發。本平台目前還沒有 LLM 功能，不讀這個檔 |
| rancher-desktop 叢集 `grafana-new` 的 helm release `grafana` | Grafana admin 密碼、Grafana 讀 ES 的 key、讀 PostgreSQL 的密碼 | 由舊 repo `aiops-data-foundation` 的部署流程更換 |

## 架構一覽

```
Pipeline（固定骨架）：資料由記憶體內的 PipelineContext 依序流過 YAML 宣告的算子

  抓取(fetch) → 偵測(detector.*) → 只留新異常 → 心跳／恢復 → 收斂 → 計分 → 抑制 → 通知內容 → 通知(notifier.*) → 寫入
       ▲                                                                                              │
  sources                                                                    只有「最終結果」進資料庫（Incident、通知紀錄、模型狀態）
  (Prometheus／VM／ES)                                    節點之間不透過資料庫傳資料；管線前後由 storage/hooks.py 一次載入／一次寫入
                                                                             通知 ──► aiops-alertmanager ──► Slack／Teams／PagerDuty
                                                                                  └──► Grafana（只顯示，不發通知）
```

| 目錄 | 是什麼 |
|---|---|
| `aiops/engine/` | 底座：`PipelineContext`（強型別）、Pipeline 引擎、算子註冊中心、`BaseOperator`、錯誤型別、請求模型 |
| `aiops/pipelines/` | 管線 YAML，依業務主線分：`detection/`（`it_otel`、`it_apm`、`factory`、`metrics_only`）、`rca/`（`rca_report`、`rca_report_correlation`）、`control/`（各 `admin_*` 手動動作）。調整流程只改這裡；用名稱載入，名稱在整個 `pipelines/` 內唯一 |
| `aiops/stages/` | 管線算子，依執行順序分段：`ingestion` → `detectors` → `ensemble` → `grouping` → `scoring` → `suppression` → `rca` → `enrichment`（LLM，含 `noop`）→ `notification` |
| `aiops/control_plane/` | 控制面手動動作（`admin.*`）：`learning/`（調參、回饋、成效報表、規則升級）、`operations/`（靜音、欄位對照、事件群、匯入、資料源檢查） |
| `aiops/sources/` | 外部資料源適配：`mock`、`prometheus`、`victoriametrics`、`elasticsearch`；`sources/query/` 是查詢語法解析與時間窗對齊（不連線） |
| `aiops/storage/` | SQLAlchemy 資料表、repository、`hooks.py`（管線前後的載入／寫入） |
| `aiops/interfaces/` | 對外介面：`api/`（薄薄一層 HTTP）、`app.py`（FastAPI 入口）、`cli/`（排程，`python -m aiops.interfaces.cli`）、`bootstrap/`（啟動初始化、token 驗證、故障看守）、`runner.py`（唯一組裝點） |
| `scripts/synth_generator/` | 合成資料產生器（不在 `aiops/` 裡，平台不 import）：由服務拓撲與故障劇本產生有因果關係的 metrics、traces、logs 與標準答案，見「合成資料產生器」 |
| `tests/dev_es_key.py` | 開發輔助腳本（不是測試）：建本機用的唯讀 ES key 並寫進 `secrets.local.yaml`；檔名不是 `test_` 開頭，pytest 不會執行它 |
| `secrets.local.example.yaml` | 本機開發金鑰範本；複製成 `secrets.local.yaml`（已被 `.gitignore` 擋住）再填值 |
| `migrations/postgres/` | 既有 PostgreSQL 的手動升級 SQL（沒有 Alembic，可重複執行） |
| `deploy/helm/aiops-alertmanager/` | 接收本平台通知的 Alertmanager chart |
| `deploy/helm/aiops-grafana/` | 只顯示的 Grafana chart：APM（讀 ES）與平台告警（讀 Alertmanager），不發通知 |
| `deploy/helm/aiops-platform/` | 平台本身：API（Deployment）與排程（CronJob） |
| `deploy/elasticsearch/` | ES 說明與去機密的快照（chart 原始碼不在本機，見該資料夾 README） |

### `aiops/` 每個檔案在做什麼

依賴方向：`interfaces`（API、CLI）→ `interfaces/runner.py` → `engine`（引擎）← `stages`、`control_plane`（算子）；`storage` 只在管線前後被 `storage/hooks.py` 呼叫。`engine` 不 import 平台任何其他層。算子裡只有 `stages/ingestion/fetch.py` 碰資料源（`sources`），只有 `control_plane/` 碰資料庫。

三條業務主線各自對應：偵測告警管線 = `pipelines/detection/` + `stages/`（ingestion → notification）；RCA 診斷管線 = `pipelines/rca/` + `stages/rca/`、`stages/enrichment/`；學習閉環 = `pipelines/control/` + `control_plane/learning/`。

```
aiops/
├── engine/                          底座：引擎與共用型別（不 import 平台任何其他層）
│   ├── context.py                   強型別 PipelineContext：載入的輸入、流動的資料、RCA 欄位；to_result() 組 API 回傳
│   ├── pipeline.py                  讀 Workflow YAML、建立算子、驗證節點順序、依序執行；optional 節點與單條序列失敗隔離
│   ├── registry.py                  OperatorRegistry：@register 登記、依名稱建立
│   ├── operator.py                  BaseOperator 基底（scope: target／batch、node_name）
│   ├── types.py                     AnomalyEvent（不可變）、Incident（事件群）、Notification、merge_incidents
│   ├── requests.py                  API／手動動作的請求模型（Pydantic）
│   ├── errors.py                    平台錯誤型別，API 層統一轉成 HTTP 狀態碼
│   ├── params.py                    參數深度合併、調參分層挑選（管線×實體 > 管線 > 實體 > 全域）
│   ├── topology.py                  IT 呼叫關係與工廠 Lot 流向，統一成「異常會從 A 傳到 B」的有向邊
│   └── signature.py                 事件群特徵簽章與 Jaccard 相似度（歷史相似案例用）
├── pipelines/                       管線 YAML（沒有 Python，調整流程只改這裡；以名稱載入，名稱不准重複）
│   ├── detection/                   偵測告警管線
│   │   ├── it_otel.yaml             IT：抓指標 → 168 時段偵測 → 拓撲收斂 → 黃金訊號計分 → 抑制 → 通知
│   │   ├── it_apm.yaml              IT：APM 產品資料，有服務拓撲與黃金訊號（偵測用 zscore，不是 168 時段）
│   │   ├── factory.yaml             工廠：Lot 履歷收斂，沒有黃金訊號與同根因抑制
│   │   └── metrics_only.yaml        只有時序指標，沒有 trace 與拓撲
│   ├── rca/                         RCA 診斷管線
│   │   ├── rca_report.yaml          針對一個事件群產生 RCA 報告（根因用拓撲）
│   │   └── rca_report_correlation.yaml  RCA 報告（根因用時序互相關）
│   └── control/                     控制面手動動作
│       └── admin_*.yaml             18 個，每個手動動作一個，API 呼叫時載入
├── stages/                          管線算子，依執行順序分段（套件名稱不能以數字開頭，順序寫在 stages/__init__.py）
│   ├── _common.py                   共用小工具（還沒有事件群時每筆異常先自成一群）
│   ├── ingestion/                   1. 抓資料
│   │   └── fetch.py                 唯一碰資料源的算子：ingestion.fetch、ingestion.incident_series
│   ├── detectors/                   2. 偵測器（繼承 DetectorOperator，彼此不互相 import）
│   │   ├── base.py                  共同骨架：參數合併、基準線載入／保存、異常血緣、模型檔（npz）打包
│   │   ├── zscore.py                detector.zscore：偏離訓練期平均幾個標準差
│   │   ├── mad.py                   detector.mad：中位數與 MAD，比 zscore 耐尖峰
│   │   ├── hourly_baseline.py       detector.hourly_baseline：一週 168 時段各自一條基準線，可設時區
│   │   ├── compare.py               detector.dod／wow：跟昨天／上週同一時刻比
│   │   ├── quantile_drift.py        detector.quantile_drift：滾動 p95 漂移，抓尾端變慢
│   │   ├── chronic.py               detector.chronic：滾動平均跟基準比，抓小幅持續偏移
│   │   ├── shift.py                 detector.shift：相鄰兩點跳變
│   │   ├── peer.py                  detector.peer：跟同儕中位數的殘差比（同儕可寫死或動態分群）
│   │   ├── rules.py                 detector.rules：人訂上下限加連續 N 點，不學習
│   │   └── new_only.py              detector.new_only：只留比最後一筆已存異常更新的，重跑不重複告警
│   ├── ensemble/                    3. 多偵測器裁決
│   │   └── ensemble.py              detector.ensemble：多個偵測節點投票（聯集／交集／多數決／至少 N 票）
│   ├── grouping/                    4. 事件收斂
│   │   └── group.py                 same_entity／topology_chain／lot_path_chain
│   ├── scoring/                     5. 計分
│   │   └── score.py                 golden_signals（含黃金訊號加權）、p0_boost（牽涉 P0 加分）
│   ├── suppression/                 6. 抑制
│   │   └── suppress.py              suppress_window（靜音）／same_root_cause（同根因）／dedup（冷卻）
│   ├── rca/                         7. 根因分析（RCA 診斷管線）
│   │   ├── base.py                  兩種契約：RootCauseOperator（寫 rank()）、RcaSection（寫 applies()／build()）
│   │   ├── report.py                rca.report：依 sections 清單組裝報告，資料不足的段落略過
│   │   ├── algorithms/              根因演算法（換節點就換演算法）
│   │   │   ├── topological.py       topological（預設）：傳播圖上沒有異常上游的實體，依最早出現排序
│   │   │   ├── pagerank.py          pagerank：反向傳播圖上的 Personalized PageRank
│   │   │   └── causal.py            cross_correlation：事件時段原始時序的時間位移互相關，看誰領先
│   │   └── sections/                報告段落
│   │       ├── overview.py          摘要、時間軸、受影響實體、黃金訊號
│   │       ├── graph.py             可能的源頭、傳播路徑
│   │       ├── evidence.py          Trace 佐證（錯誤率、p95）
│   │       ├── similar_cases.py     歷史相似案例與當時的處置
│   │       └── actions.py           規則式建議處置
│   ├── enrichment/                  8. 可選的 AI 能力（noop 永遠不連外）
│   │   ├── base.py                  BaseLLM 介面、LLMError、解析模型回覆成 JSON
│   │   ├── registry.py              供應商註冊表：放一個檔案、用 @register 登記，啟動時自動掃描
│   │   ├── providers.py             內建供應商 noop／openai（含相容端點）／anthropic，build_llm()
│   │   ├── summarizer.py            enrichment.llm_summary：RCA 總結敘述，失敗或驗證不過就退回模板
│   │   └── narrative.py             敘述的提示詞、模板，以及依據驗證
│   └── notification/                9. 通知
│       ├── payload.py               alert.notify_payload：定嚴重度、產生通知內容（一個部門一則，路由標籤由平台決定）
│       ├── heartbeat.py             alert.heartbeat：進行中事件群的心跳重送與結案（恢復通知）
│       ├── notifiers.py             notifier.log／alertmanager／auto（依 AIOPS_NOTIFIER 選）、notifier.watchdog（自我健康心跳）
│       └── transport.py             實際傳輸：AlertmanagerTransport（v2 API）、LogTransport（只寫日誌）
├── control_plane/                   控制面手動動作（唯一可以用 storage.repository 的算子）
│   ├── learning/                    學習閉環
│   │   ├── tuning.py                調參唯一收口：提案 → 回測 → 核准；回饋、漏報補錄、寫入經驗庫
│   │   ├── tuning_rules.py          從回饋文字抽調參規則（LLM 提議、平台驗證，不過就退回固定規則）
│   │   └── learning.py              調參成效報表、頻繁調參轉靜態規則提案
│   └── operations/                  日常維運
│       ├── incidents.py             事件群查詢與手動結案
│       ├── silences.py              建立靜音／維護窗口
│       ├── mappings.py              欄位對照審核（提交 → 核准，舊版自動作廢）
│       ├── ingest.py                外部異常接收、trace 合成（含服務呼叫關係）、Lot 履歷合成
│       └── sources.py               檢查每個資料源有沒有設定、連不連得上
├── sources/                         外部資料源適配（輸出標準 DataFrame；不 import storage 與算子）
│   ├── __init__.py                  依名稱取得資料源、逐一自我檢查
│   ├── base.py                      資料源介面 fetch_data／fetch_group／check，以及「查詢寫錯」「沒資料」兩種錯誤
│   ├── prometheus.py                Prometheus 與 VictoriaMetrics（query_range；支援依標籤分組的動態同儕）
│   ├── elasticsearch.py             ES 降採樣查詢（date_histogram）轉成標準序列
│   ├── mock.py                      開發用假資料（含 seasonal: 日週期資料與動態同儕群）
│   ├── _http.py                     共用 HTTP：連線／認證／伺服器錯誤轉成 InfrastructureError
│   └── query/                       查詢語法解析與時間窗對齊（不連線）
│       ├── promql.py                PromQL 基本檢查（只擋明顯寫錯的）
│       ├── elasticsearch.py         ES 查詢的 JSON 字串 → date_histogram search body
│       ├── trace.py                 內部 trace 來源的「服務/欄位」語法
│       └── window.py                時間窗對齊到 step 的整數倍（UTC）
├── storage/                         資料庫（節點之間不經過它，只在管線前後被 hooks.py 呼叫）
│   ├── hooks.py                     引擎的載入與寫入點：管線前一次載入輸入、管線後一次寫入最終結果
│   ├── models.py                    14 張表：incidents、silences、notifications、anomalies、detector_models、field_mappings、external_anomalies、trace_series、service_edges、lot_paths、tuning_proposals、feedback、active_detector_configs、incident_knowledge
│   ├── repository.py                各表讀寫，沒有業務判斷；基準線與調參用 version 做樂觀鎖
│   ├── crud.py                      共用存取函式（建立、更新、條件式更新、單次 IN 查詢…）
│   └── db.py                        連線與 session（SQLite 開發／測試、PostgreSQL 正式），連不上轉成 InfrastructureError
└── interfaces/                      對外介面
    ├── app.py                       建立 FastAPI 應用（create_app），提供 /health
    ├── runner.py                    組裝點：load_all() 掃描所有算子；用名稱在 pipelines/ 找 workflow（拒絕路徑、軟連結、重複名稱）、接上 StorageHooks，提供 run() / run_context()
    ├── api/                         HTTP，零業務邏輯，每個 router 都要 token
    │   ├── runs.py                  POST /api/v1/runs、POST /api/v1/rca、GET /api/v1/operators
    │   ├── alerts.py                靜音（建立、列出）、事件群查詢與結案
    │   ├── ingestion.py             欄位對照、trace／Lot 合成、外部異常接收、資料源自我檢查
    │   └── tuning.py                提案、回測、核准、回饋、成效報表（/metrics）、規則升級（/rules/promote）
    ├── cli/                         排程入口
    │   └── __main__.py              CLI：run-once、loop、source-check（只呼叫 Pipeline，不做運算）
    └── bootstrap/                   啟動組裝
        ├── __init__.py              登記算子、自動掃描 api/ 掛 router 與 token、錯誤轉 HTTP、等資料庫就緒
        ├── auth.py                  Bearer token 驗證；沒設 token 一律拒絕；AIOPS_AUTH_DISABLED 只限本機開發
        └── fallback.py              故障看守：資料庫掛掉時寫本機日誌、打備援 webhook
```

## 本機開發

開發與測試只在 Python 3.14 上跑過；程式用到的語法需要 3.10 以上，其他版本沒有驗證。

```bash
python -m venv .venv
```

```bash
.venv/Scripts/python -m pip install -r requirements-dev.txt
```

`requirements.txt` 是執行時需要的（映像檔只裝這個），`requirements-dev.txt` 再加上測試工具。

macOS／Linux 把 `.venv/Scripts/python` 換成 `.venv/bin/python`。

### 跑測試

```bash
.venv/Scripts/python -m pytest tests/ -q
```

測試用記憶體 SQLite，不需要任何外部服務。有裝 `helm` 時會多跑 Alertmanager chart 的檢查，沒裝就略過。

### 啟動 API

API 一律要 token。本機開發可以先關掉驗證：

```bash
AIOPS_AUTH_DISABLED=true .venv/Scripts/python -m uvicorn aiops.interfaces.app:app --reload
```

或設定一組開發用 token：

```bash
AIOPS_API_TOKEN=dev-only-token .venv/Scripts/python -m uvicorn aiops.interfaces.app:app --reload
```

PowerShell 先執行 `$env:AIOPS_AUTH_DISABLED = "true"` 再啟動。啟動後到 http://127.0.0.1:8000/docs 看所有端點。資料預設存在 `./aiops-local.db`。

**`source_type` 一律必填（API 合約變更）**：`POST /api/v1/runs` 的 `targets[].source_type`、`targets[].peers[].source_type`，以及 `POST /api/v1/tuning/proposals/backtest` 的 `metric.source_type`、`peers[].source_type`，都沒有預設值。以前漏寫會默默變成 `mock`（假資料），現在漏寫或只給空白回 **422**，`detail[].loc` 指出是哪一個 `source_type`；workflow YAML 的 target 漏寫則在載入時就失敗。要用模擬資料請明寫 `"source_type": "mock"`。`peer_group.source_type` 不受影響：空字串仍代表「跟 target 同一個資料源」。

```bash
curl -X POST http://127.0.0.1:8000/api/v1/runs -H "Authorization: Bearer $AIOPS_API_TOKEN" -H "Content-Type: application/json" -d '{"workflow": "it_otel", "targets": [{"source_type": "prometheus", "query": "up{job=\"cart\"}", "entity": "cart"}]}'
```

### 跑排程

```bash
.venv/Scripts/python -m aiops.interfaces.cli run-once -w it_otel
```

| 指令 | 用途 |
|---|---|
| `run-once -w <workflow>` | 跑一輪就結束（適合 K8s CronJob）。`-w` 可以是 `aiops/pipelines/` 底下任一 YAML 的名稱（不含副檔名與子資料夾）或自己的 YAML 路徑；有任何一條序列失敗則結束碼非 0 |
| `loop -w <workflow> --interval 60` | 常駐，每隔 N 秒跑一輪（只適合單一副本） |
| `source-check` | 檢查每個資料源有沒有設定、連不連得上 |

每一輪就是跑一次 Pipeline：模型過期才重訓 → 偵測並只留新異常 → 收斂、計分、抑制 → 通知 → 寫入結果。同一段時間重跑不會重複開單或重複通知（第二輪只會多一則「恢復」通知）。

- **旁路節點**：workflow 的節點可加 `optional: true`（例如 `rca_report` 的 LLM 摘要）。它失敗時只記進結果的 `errors`（`node:<節點名>`），後面的節點與寫入照常；資料庫或資料源連不上仍會讓整輪失敗。
- **並發**：偵測器基準線與現行參數用 `version` 做樂觀鎖。兩個 worker 同時訓練同一條序列時，先寫的保留，後寫的不覆蓋。

- **只能用名稱跑 workflow**：API（`/runs`、`/rca`）的 `workflow` 只接受 `[A-Za-z0-9_-]+` 的內建名稱，路徑、`..`、軟連結一律拒絕。只有排程 CLI（`-w ./my.yaml`）可以指定自己的檔案。
- **偵測節點各自獨立**：基準線與異常都綁 Workflow 節點名稱。同一個演算法可以在同一條管線掛兩次（例如 zscore 短窗、長窗），各自有自己的基準線；`anomalies.detector_name` 是算子名（`detector.zscore`），`detector_node` 是節點名。
- **參數合併**：演算法預設值 ← 後台核准的調參 ← YAML 的 `params`，逐個鍵合併。**YAML 寫了的鍵是鎖定的**，不會被後台調參蓋掉；沒寫的鍵才吃調參。想讓某個參數交給後台調，就不要寫在 YAML。
- **調參分層**：核准的參數有 (偵測器, 管線, 實體) 三個維度，`*` 代表不限定。套用時取最具體的一層：管線×實體 > 管線 > 實體 > 全域。回饋轉提案可用 `scope: entity | pipeline | global` 選層級（預設 `pipeline`）。不同層的提案互不使對方過期。
- **回測跟偵測同一組參數**：提案有指定管線時，回測會讀那條管線的 YAML，疊上該偵測器節點的 `params`（鎖定鍵）與該實體 target 的 `overrides`；被 YAML 鎖住的鍵提案改了也不會生效，結果的 `workflow.locked` 會列出來、`current_anomalies` 與 `proposed_anomalies` 會一樣。
- **回測切段**：回測資料前 `train_ratio`（預設 0.7）拿來訓練、後面拿來測試，只計算測試段的異常，避免用同一段資料訓練又評分。滾動窗的偵測器（如 `chronic`）會往前多帶 `warmup_points()` 個點讓窗口湊滿，這些點不計分。
- **LLM 供應商**：放一個檔案到 `aiops/stages/enrichment/`，用 `@register("名稱")` 登記即可，啟動時自動掃描載入，不用改其他地方。

### 偵測演算法一覽

| 算子 | 比什麼 | 什麼時候用 | 要注意 |
|---|---|---|---|
| `detector.zscore`、`mad` | 整段訓練期一組平均（中位數） | 沒有週期的指標 | 有日週期的指標會在高峰／低谷誤報或漏報 |
| `detector.hourly_baseline` | 一週 168 個時段各自一組平均與標準差 | 流量、交易量這類有日週期／週末效應的指標（`it_otel`、`factory` 預設，與 `mad`、`quantile_drift` 一起多數決） | **冷啟動：流量類指標建議累積 3~4 週再信任它**。每個「週幾幾點」要看過至少 `min_weeks`（預設 2）週才會啟用，不夠的時段**自動退回全段統計（等同 `zscore`）**，所以 7 天訓練時整條等於 `zscore`，不會更吵但也還沒有週期效果。啟用：`train_lookback_minutes: 20160` 以上並把 `step_seconds` 調大（Prometheus 單次查詢約 11000 點上限）。時區用 `tz` 設，台灣設 `Asia/Taipei` |
| `detector.dod`、`wow` | 跟昨天／上週同一時刻的差值 | 要「跟昨天比」「跟上週比」 | `fetch` 要設 `lags_minutes`；`dod` 跨週末會變遲鈍，有週末效應用 `wow` |
| `detector.quantile_drift` | 滾動 p95（可調）跟平常比 | 延遲：平均沒變、最慢那批越來越慢 | 單一尖峰不會觸發（刻意的） |
| `detector.chronic` | 滾動平均跟基準比 | 小幅但持續的偏移 | 單一尖峰不會觸發（刻意的） |
| `detector.shift` | 相鄰兩點跳變 | 突然換水位 | 跳上去之後的新水位不再報 |
| `detector.peer` | 同時間同儕的中位數 | 同服務多個 Pod、同型號多台機台 | 同儕用 `peers` 寫死，或 `peer_group` 依標籤動態撈（Prometheus／VictoriaMetrics） |
| `detector.rules` | 人訂的上下限、連續 N 點 | 有明確規格的指標 | `require_rule: false` 時只用核准的規則提案，沒有規則的序列不判 |
| `detector.ensemble` | 前面幾個偵測節點的投票 | 想用交集／多數決壓誤報 | 接在偵測節點後面、`new_only` 前面；分母只算這條序列真的評估過資料的節點（見下方） |

#### 每個服務跑哪些偵測器（target 白名單、overrides、止血）

一條 workflow 的偵測節點是「菜單」，每個 target 自己點菜。三個機制，判斷順序是 **節點的 `exclude_entities` → 節點的 `include_entities` → target 的 `detectors`**，任何一關沒過，這個節點對這條序列就不跑（不載基準線、不載調參、不算投票）。

```yaml
nodes:
  - {name: fetch, operator: ingestion.fetch, params: {lookback_minutes: 60, train_lookback_minutes: 10080}}
  - name: detect
    operator: detector.hourly_baseline
    params:
      tz: Asia/Taipei
      min_weeks: 1
      exclude_entities: [inv-svc]          # 緊急止血：這個節點對 inv-svc 先關掉（優先於一切）
  - name: detect_mad
    operator: detector.mad
  - name: detect_qdrift
    operator: detector.quantile_drift
    params: {include_entities: [cart-api, ord-svc]}   # 灰度：只對這兩個服務開
  - name: vote
    operator: detector.ensemble
    params: {strategy: majority, tie_breaker: strict}
  - {name: new_only, operator: detector.new_only}

targets:
  - {source_type: prometheus, query: '...', entity: cart-api}       # 沒寫 detectors = 全部節點都跑（跟以前一樣）
  - source_type: prometheus
    query: '...'
    entity: pay-api
    detectors: [detect_mad, detector.quantile_drift]   # 節點名稱，或算子名稱（= 這個算子的所有節點）
  - source_type: prometheus
    query: '...'
    entity: ord-svc
    overrides:                                         # 這個服務自己的參數，key 是節點名稱
      detect_mad: {threshold: 5.0}
```

- **`detectors`**：同一個算子掛兩個節點（例如 `z_short`、`z_long`）時寫節點名稱只跑那一個；寫算子名稱（`detector.zscore`）兩個都跑。不寫就是全部都跑。
- **`overrides`**：參數優先序 `演算法預設值 ← 後台核准的調參 ← 節點 YAML params ← target overrides`（逐鍵合併，overrides 最優先），一樣要通過演算法的範圍檢查。不要把演算法參數直接攤在 target 根層級。
- **`include_entities`／`exclude_entities`**：寫在任何偵測節點的 `params`，不會傳給演算法、後台調參也改不到（提案帶這兩個鍵會被拒絕）。
- **投票分母**：`detector.ensemble` 只算「真的評估過這段資料」的節點。被上面三個機制跳過的、這一輪沒資料的，都算棄權、不進分母；有評估但沒抓到的算反對票。只剩 1 個參與者時它抓到就通過；參與者是偶數（最常見：白名單只留 2 個）時看 `tie_breaker`：`strict`（預設）平手不算，2 個要 2 票；`loose` 平手就算，2 個裡 1 票即通過。`at_least` 的 `min_votes` 比參與者多時降為「全部參與者都抓到」。`detector.hourly_baseline` 時段資料不足時是退回全段統計繼續判斷，**不是跳過**，所以仍然算參與者。
- **載入期就報錯**（`WorkflowError`，API 回 400）：`detectors` 寫了對不到的節點／算子名稱（訊息會列出可用節點）、`overrides` 的 key 不是偵測節點或這個 target 沒選它、overrides 參數拼錯或超出範圍、`include/exclude_entities` 寫了 workflow targets 裡沒有的實體（止血名單打錯字＝以為停了其實沒停，所以直接擋；workflow 沒宣告 targets、全由 API 傳入時無從比對，不檢查）、target 欄位拼錯（例如 `detector:`）。API 呼叫帶進來的 `targets` 也走同一套檢查。

真實資料回放（5 個 scn 服務分鐘級延遲，訓練 7 天、回放到 11/3 16:20Z；事件區間 11/3 14:00–16:30Z，數字是「區間內／區間外」的異常點）：

| 設定 | 區間內 | 區間外 |
|---|---|---|
| 預設：hourly（`min_weeks: 1`）+ mad + qdrift 多數決，target 沒寫 `detectors` | 158 | 59 |
| `detect` 加 `exclude_entities: [inv-svc, pay-api]` | 157 | 41 |
| `detect` 加 `include_entities: [cart-api, ord-svc]` | 156 | 35 |
| 每個 target `detectors: [detect_mad, detect_qdrift]`，`tie_breaker: strict` | 154 | 21 |
| 同上，`tie_breaker: loose` | 216 | 428 |
| 每個 target `detectors: [detect]`（只剩 1 個參與者） | 161 | 128 |

`loose` 等於兩個偵測器取聯集，非常吵，只在「寧可多報」的服務用。

動態同儕的寫法（自己是 `attrs[by]`，沒有就用 `entity`；`${屬性}` 會用 target 的 attrs 代入）：

```yaml
targets:
  - source_type: prometheus
    query: 'sum(rate(container_cpu_usage_seconds_total{pod="cart-7d9f"}[5m]))'
    entity: cart-7d9f
    attrs: {service: cart, pod: cart-7d9f}
    peer_group: {query: 'sum by (pod) (rate(container_cpu_usage_seconds_total{service="${service}"}[5m]))', by: pod}
```

偵測器需要存非 JSON 的模型本體時（樹模型、矩陣）用 `get_artifact()`／`load_artifact()`，格式只能是 `npz`（`pack_arrays`／`unpack_arrays`，載入時禁止 pickle）。目前內建偵測器都沒有用到它，是給之後的 ML 模型用的。

### RCA：根因演算法與報告段落

**拓撲從哪來（隨需計算）：**日常偵測完全不撈 trace（trace 量通常是指標的 10 倍以上）。只有跑 RCA 報告時，`ingestion.incident_traces` 才去 ES 撈事件開始前 15 分鐘到結束的 span，`rca.build_dynamic_topology` 用 `parent.id` → `span.id` 還原當下服務之間的呼叫邊，跟既有拓撲（事件快照、批次匯入的 trace）取聯集後交給根因演算法。ES 沒設定就略過、連不上就記進 `errors` 並略過，報告照出。trace 索引預設 `traces-apm-*`，用環境變數 `AIOPS_ES_TRACE_INDEX` 改；上限 `max_spans`（預設 20000）。區間內不超過上限就全部撈；超過時**不是取最早的 N 筆**（尖峰時那只是窗口開頭幾秒），而是把窗口切成最多 30 個時間桶、每桶用固定 seed 隨機挑 trace，輪流把**整條 trace** 撈回來直到湊滿上限（抽 span 會把父子拆斷），結果的 `trace_fetch.truncated` 為 true。記憶體：100 萬個 span 約 266 MB、聚合邊 0.7 秒，上限 2 萬時約 5 MB。

> 真實 ES（11/3 13:50–16:20Z，共 11,283 span／3,761 條 trace）：上限 2000 時舊做法只涵蓋 13:50–14:16（10 分鐘格 3／15），新做法涵蓋整段（15／15，每 10 分鐘 129–138 個 span），抽到的 666 條 trace 全部完整；上限 5000 時舊做法 7／15、新做法 15／15。抽樣多花 0.7–1.1 秒（多了每桶一次候選查詢）。這批 trace 沒有跨服務呼叫，所以兩種做法的邊都是 0，邊的準確度要等有串 trace context 的資料才量得到。

> 真實資料（11/3 事件，14:05–16:20Z）：撈到 10,167 個 span、3,389 條 trace，**跨服務呼叫 0 條**——這批 trace 每條都只在一個服務裡（下游只有 redis、postgresql、oracle、tax-gw），所以拓撲與 PageRank 仍排不出根因。機制本身是通的，缺的是有串接 trace context 的資料。

`rca_report.yaml` 的 `root_cause` 節點決定根因怎麼排序，報告段落只負責呈現：

| 根因算子 | 做法 | 需要 |
|---|---|---|
| `rca.root_cause.evidence`（`rca_report` 預設） | 這一輪撈到的 span：各服務**自身耗時**（span 耗時扣掉等下游的時間）與自身錯誤率的增量；被上游放大流量的服務，增量按流量倍數轉給放大者；前幾名拉不開或資料不足就回「證據不足」不指認。沒有 trace 的場景（工廠）退回 topological 並寫明；trace 來源連不上（例如 ES 重啟）則不退回、直接證據不足。候選的自身耗時增量裡，單一下游資源（`span.type` 是 db／external 的葉節點：Oracle、Redis、第三方 API）的等待佔 `min_dependency_share`（0.6）以上，就填 `dependency` 下鑽（例如 `tax-gw:443（external/http）`）：候選仍是服務、排名不變，報告與建議處置會說「去查它等的東西，不是它的程式」 | `ingestion.incident_traces`（已接好） |
| `rca.root_cause.topological` | 傳播圖上沒有異常上游的實體，依最早出現排序 | 拓撲或 Lot 流向 |
| `rca.root_cause.pagerank` | 反向傳播圖上的 Personalized PageRank：拖垮越多下游、越異常的上游排越前 | 拓撲或 Lot 流向 |
| `rca.root_cause.cross_correlation` | 事件時段原始時序的時間位移互相關：誰的波動領先別人。前處理 `preprocess` 可選 `diff`（預設，一階差分）或 `hybrid`（只在異常時段保留穩健 z 分數、其他歸零） | 前面要有 `ingestion.incident_series`（`rca_report_correlation.yaml` 已經接好）；這是相關不是因果 |

**互相關的 `diff` vs `hybrid`（`min_corr` 固定 0.5，沒有降低）：**一階差分在「整段水位抬高」（高原）的事件只剩兩個邊緣，相關被雜訊淹沒；`hybrid` 把異常時段的波形完整保留、平常時段一律歸零（日夜週期不會變成假相關），遮罩來源是事件群裡同實體的異常點，前後擴 `mask_expand_minutes`（預設 5）分鐘；遮罩內少於 `min_mask_points`（預設 20）個點的實體不參與。

| 資料 | 真實根因 | `diff`（max_lag 10/20/30） | `hybrid`（max_lag 10） | `hybrid`（max_lag 20/30） |
|---|---|---|---|---|
| 產生器 `load_amplification`（cart 14:05 → ord 14:10 → bill 14:20） | cart-api | 無候選 | api-gw ✗（領先 cart-api 1 點、ord-svc 6 點、bill-svc 10 點，r 0.98–0.99） | 同左（bill-svc 11 點） |
| 產生器 `callee_slowdown`（各服務同一分鐘變慢） | bill-svc | 無候選 | ord-svc ✗（領先 cart-api 1 點，r 0.99） | 同左 |
| ES 真實資料 11/3 事件 | 無標準答案（資料本身最早起點是 cart-api 14:46） | ord-svc 領先 bill-svc 10 點，r 0.75 | ord-svc 領先 bill-svc 10 點，r 0.98 | cart-api 領先 bill-svc、ord-svc（剛好撞到搜尋上限 20／30 點，r 0.52–0.57） |
| ES 真實資料 40 個無事件時段 | 應該全部沒有候選 | 誤判 0／0／1 次 | 誤判 0 次 | 誤判 0 次 |

**再加 `use_topology: true`（只比拓撲相鄰的服務，`min_corr` 仍是 0.5，`max_lag` 10 與 20 結果相同）：**

| 資料 | 真實根因 | `diff` | `hybrid` | `hybrid` + `use_topology: true` |
|---|---|---|---|---|
| 產生器 `callee_slowdown` | bill-svc | 無候選 | ord-svc ✗ | ord-svc ✗（領先 cart-api 1 點，r 0.99） |
| 產生器 `load_amplification` | cart-api | 無候選 | api-gw ✗ | api-gw ✗（領先 cart-api 1 點，r 0.99；cart-api 排第 2） |

拓撲限制確實去掉了不相干的配對（`load_amplification` 的 api-gw 累計分數從 2.95 降到 0.99），但沒有改變第一名：根因跟下游同一分鐘變慢時，相鄰服務之間 ±1 點的先後是雜訊。**兩套標準答案 0／2 命中，所以預設不切。** 沒有任何拓撲邊時，`use_topology: true` 不會丟例外，結果是沒有候選、段落略過。

**`hybrid` 的已知限制**（都有對應的 `xfail(strict=True)` 測試，見 `tests/test_rca_hybrid_limits.py`）：

- **短事故／毛刺**：遮罩內不到 `min_mask_points`（20）個點的實體不參與。4 分鐘的毛刺遮罩內只有 14 點，直接排除；12 分鐘的階梯有 22 點，可以。
- **仍在持續的高原被遮罩切斷**：事件標在第 20～59 分、高原維持到第 79 分時，遮罩（前後各擴 5 分）在第 64 分把高原從中間切斷，兩個實體共用的切口台階主導相關性，最佳 lag 變 0（r≈0.98），lag 0 的配對不列入，真實的 3 點領先被蓋掉。同一組波形若事件涵蓋到高原結束就正常。
- **偵測延遲造成假領先者**：兩個波形其實同步（真實領先 0 點），只因偵測器標出事件的時間差超過 `mask_expand_minutes`（5），hybrid 就報出「a 領先 b」（時間差 8 分鐘、r=0.94；≤5 分鐘沒有候選）。`diff` 不吃遮罩，沒有這個問題。換句話說 hybrid 的領先點數可能夾帶偵測器的反應時間差，不只是波形本身。

`hybrid` 救回了高原事件的相關性，也排對了「誰比誰晚」（ord 晚 6 點、bill 晚 10–11 點），但**兩個標準答案的第一候選都錯**：同一分鐘一起變慢的服務（例如 api-gw 直接呼叫 cart-api，延遲當下就加進去）只差 ±1 點，它照樣挑一個當「領先者」。錯的根因比沒有根因更糟，所以預設維持 `diff`，`hybrid` 要自己在 YAML 開。沒有 `min_mask_points` 防線時，`hybrid` 在無事件對照組是 40／40 誤判（只有零星異常點的服務等於拿十來個雜訊點互比）。

**`rca.root_cause.evidence` 驗收（產生器正式劇本，2 週資料、seed 42，走完整 `rca_report`；trace 與日誌經真的 `ElasticsearchSpec` 撈、含抽樣）：**

| 特徵組合 | callee_slowdown（真兇 bill-svc）基準 15／60 分鐘 | load_amplification（真兇 cart-api）基準 15／60 分鐘 |
|---|---|---|
| 純自身耗時 | ✓／✓ | ✓／✓ |
| ＋自身錯誤率（預設） | ✓／✓ | ✓／✓（cart-api 1.50、ord-svc 0.40、bill-svc 0.28） |
| ＋日誌權重 0.5／1.0 | ✓／✓ | ✓／✓（1.0 時 cart-api 1.89 對 ord-svc 1.39，差距縮小） |
| 只看自身錯誤率 | ✓／✓ | ✓／✓ |
| 對照：topological／PageRank | ✓／✓ | ✗ bill-svc／✗ ord-svc |

- **沒有流量放大歸因時**，load_amplification 是 cart-api +184 ms、ord-svc +171 ms，差 8%，只能回「證據不足」；加上歸因（ord-svc、bill-svc 流入流量 2.09 倍，由 cart-api 放大）後才排對。歸因是近似：被灌流量的服務增量中 (1 − 1/流量倍數) 算給放大者，理由文字會寫出轉了多少毫秒。
- **測試套件**（`tests/test_rca_evidence.py`）：正式劇本縮短到事故當天、3 個 seed（42、7、2026）× 兩種機制，第 1 名全部命中；把流量放大歸因關掉，load_amplification 會紅。
- **不會無中生有**：同一套產生器、事故效果全設 0 的對照劇本，40 個時段 40 次「證據不足（沒有明顯上升）」；每個服務同幅度變慢（共同外部因素）回「證據不足（拉不開）」。
- **真實 ES（scn 資料）**：40 個安靜時段全部「證據不足（trace 不足）」——每個服務 15 分鐘只有約 5 筆請求，低於 `min_requests` 20，**評分邏輯沒有被這批資料考驗到**。11/3 事故排出 bill-svc（自身耗時 +31 ms）、ord-svc（+20）、cart-api（+6）；這批 trace 沒有跨服務呼叫，自身耗時就是各服務總耗時，也沒有標準答案可以對。
- **日誌**：超過上限時改成依時間分桶抽樣並加權（6,147 筆估成 6,151），倍率改成新出現與既有樣板同一把尺。但 cascade 的真正成因訊息（cart-api `HikariPool ... Connection is not available`）仍排第 10：前幾名是下游的「交易太慢」警告，平常完全沒出現所以倍率最高。日誌權重預設 0，不影響根因排名；要把成因訊息排前面需要分辨「症狀日誌」與「成因日誌」，還沒做。

**建議處置是確定性 Runbook Matrix**（`rca.section.suggested_actions`，`stages/rca/sections/actions.py`）：不看 metric 名稱的子字串（以前 `transaction.duration` 含 `io` 會叫人查磁碟），而是用已算好的證據比對八條故障特徵，命中的全列、全部不命中才保底並寫明「沒有命中任何故障特徵」：RB-01 下游外部服務（候選的 `attribution.kind == external` 且等待佔比 ≥ 0.6）、RB-02 資料庫／快取（db／cache 且佔比 ≥ 0.5 或資源失敗增加）、RB-03 應用邏輯錯誤（`errors` 訊號＋範例 trace 失敗＋沒有主導資源）、RB-04 流量飽和（`traffic`＋`latency` 且範例 trace 沒失敗）、RB-05 磁碟 I/O（metric 整字 token 有 disk／io／iops）、RB-06 共同外部因素（根因算子回證據不足）、RB-F1 製程機台、RB-00 保底。命中的那條放在 `ctx.rca_runbook`（API 的 `result.runbook`）。**訊號類別**（latency／errors／traffic／saturation）的判定順序：target 的 `attrs.signal` → metric 字串切成整字 token（`duration`、`latency` → latency；`io`、`disk` → saturation）→ 判不出。真實 ES target 的 metric 是整段查詢 JSON、`attrs` 預設沒有 signal，所以 **targets 請補 `attrs: {signal: latency}`**：`alert.golden_signals` 加權與 RB-03／RB-04 都靠它，不填就不會觸發（第一步真實回放裡 `golden_signals` 一直被略過就是這個原因）。

**三行式止血摘要**（`enrichment.llm_briefing`，放在 `rca.report` 之後）：context 只放已算好的東西（事件實體與時長、根因候選的 `attribution`、範例 trace、前 3 個日誌樣板、命中的 Runbook），`provider: noop`（預設）直接用模板拼三行（【一句話根因】【立即止血動作】【驗證與排除項】，數字一定精確）；設了供應商就讓模型寫、平台驗：三個鍵都要有且以規定的【】開頭、≤ 200 字；拓撲裡有但不在白名單（事件實體、歸因資源、範例 trace 端點與 id、Runbook id）的服務名不准出現；每個數字要能在 context 裡找到 ±10% 以內的；kafka／dns／redis 這類常見幻覺詞只有 context 裡有才放行。不過就退回模板並把原因寫在 `narrative.briefing_rejected`。OpenAI 相容供應商預設帶 `response_format: json_object`（`json_mode`，Anthropic 沒有對應參數、靠 prompt）。原本的 `enrichment.llm_summary`（summary／claims 總結）不變，兩個節點各自獨立。

報告段落是註冊的 `rca.section.<代號>`，新增段落 = 在 `aiops/stages/rca/sections/` 放一個檔案。`trace_evidence` 段落優先用這一輪撈到的 span（事件前 vs 事件期間的請求、總耗時、自身耗時、錯誤率；資料帶 `span.type` 時再拆成「程式自身」與「等待每個下游資源」各多少、資源失敗幾筆），加一條代表性 trace 的呼叫鏈：**失敗優先、耗時次之**（入口 `event.outcome=failure` → HTTP 狀態碼 ≥ 500 → 子 span 有失敗 → 最慢），以前只挑最慢會挑到 244 ms／200 的正常慢請求。`ingestion.incident_traces` 設了 `max_before_minutes`（內建 RCA YAML 設 60）時，事件裡的服務在事件前入口請求不到 `min_entry_spans`（20）就把基準窗 15 → 30 → 60 分鐘往前擴（只撈多出來的前段、窗口連續、不跨週），段落第一行會標出「已由 15 分鐘擴展至 N 分鐘」。沒有才退回資料庫裡的 trace 時序。根因算子沒給候選時，「可能的源頭」段落會寫明證據不足的原因，不再整段消失。`similar_cases` 段落會列出經驗庫裡相似的歷史事故與當時的處置。

### 學習閉環

| 功能 | 怎麼用 |
|---|---|
| 漏報補錄 | `POST /api/v1/tuning/feedback` 不給 `incident_uid`，改給 `pipeline`、`entity`、`time_range`、`verdict: missed`。轉提案後回測會檢查那段時間抓不抓得到（`missed_windows`） |
| 歷史事故經驗庫 | 回饋 `verdict: true_positive` 時寫 `resolution`（可加 `root_cause`），之後類似事故的 RCA 會列出來 |
| 調參成效報表 | `GET /api/v1/tuning/metrics?window_days=7`：每個核准的調參，前後各 N 天同一層的事件群數與誤報／確認／漏報數。依偵測器血緣歸屬，是前後對照不是因果證明 |
| 規則升級器 | `POST /api/v1/tuning/rules/promote`（或排程 `run-once -w admin_rule_promote`）：同一層（管線 × 實體）N 天內被調參太多次，就用它的基準線換算出 `detector.rules` 上下限提案。目前支援 `zscore`、`mad` |

### 訓練排除、兩級告警、日誌比對、自我健康心跳

**訓練時排除歷史事故窗（預設開）**：重訓基準線前，`StorageHooks` 會把這個實體近 35 天的事件群（扣掉被標成 `false_positive` 的）前後各擴 10 分鐘，加上 `kind: maintenance` 的靜音窗，放進 `ctx.training_exclusions`；偵測節點訓練時把這些點拿掉。拿掉後剩不到原本一半或不到 30 點，就退回用全部資料（audit 記 `exclusion-skipped`）。節點設 `exclude_incidents: false` 可關閉，這是節點參數，target `overrides` 不能改。

> 真實資料（scn 5 個服務，11/3 16:20Z 重訓 7 天、窗內含 11/3 事故）：在同一時段再發生一次時，事故 110 分鐘裡被抓到的分鐘數——
>
> | 服務 | 排除的點 | `hourly_baseline` 沒排除 → 有排除 | `mad` | `quantile_drift` | 多數決 |
> |---|---|---|---|---|---|
> | cart-api | 101 | 1 → 91 | 91 → 91 | 90 → 90 | 90 → 91 |
> | bill-svc | 51 | 0 → 32 | 31 → 31 | 40 → 40 | 30 → 31 |
> | ord-svc | 51 | 1 → 31 | 31 → 31 | 50 → 50 | 30 → 31 |
>
> 受害的是時段基準線：7 天訓練時那個時段只看過一次，那一次就是事故，所以下週同一時段再壞會完全抓不到。`mad`、`quantile_drift` 用全段穩健統計，100 點的污染影響不到。三選二多數決靠另外兩票撐住，最終結果幾乎一樣；單獨用 `hourly_baseline`（或 target 白名單只留它）時差別就是 1 分鐘 vs 91 分鐘。

**warning／anomaly 兩級（預設關）**：偵測節點設 `warning_ratio`（0～1，例如 0.8）後，分數超過「門檻 × 比例」但沒越線的點記成 `level: warning`。`detector.ensemble` 越線票夠 → anomaly；不夠但越線＋預警票加起來夠 → warning。全部只是 warning 的事件群嚴重度固定 `warning`、通知帶 `level=warning` 標籤（`alert.notify_payload` 的 `notify_warnings: false` 改成只記錄不通知）。上一則是 warning、這次是 anomaly 時，`suppress_dedup` 不擋（升級一定發）。

> 真實資料回放（10/31 13:20Z → 11/3 16:20Z，訓練前 7 天，it_otel 預設三個偵測器多數決）：
>
> | | 事故窗內 | 窗外（誤報） |
> |---|---|---|
> | anomaly（開不開 warning 都一樣） | 153 分鐘 | 64 分鐘（49 段） |
> | warning，`warning_ratio: 0.8` | 2 分鐘 | 241 分鐘（167 段） |
> | warning，`warning_ratio: 0.9` | 1 分鐘 | 74 分鐘（66 段） |
>
> 提前量只有 bill-svc 一個服務：15:18 先預警、15:40 才越線（早 22 分鐘），升級路徑也實際走過（warning 通知 → 22 分鐘後 anomaly 不被冷卻擋、發 P3）。其他服務在窗內一開始就直接越線，沒有「接近門檻」的階段。**在這批資料上預警的雜訊是正式告警的 1～4 倍，所以預設不開**；要開的話建議 `notify_warnings: false` 或在 Alertmanager 把 `level="warning"` 分到低優先通道。

**Log 樣式比對（RCA 段落 `log_patterns`）**：`ingestion.incident_logs` 隨需撈事件期間（前後各 5 分鐘）與事件前 60 分鐘、事件裡那些服務的錯誤日誌（索引預設 `logs-apm.*`，用 `AIOPS_ES_LOG_INDEX` 改；只呼叫 `_search`）。段落把訊息裡的 UUID／長 id → `<ID>`、IP → `<IP>`、數字 → `<NUM>`，依（服務、例外類型、樣板）分組，列出新出現或頻率 ≥ 2 倍的前 3 名，附一筆 trace id 可以點進去看。沒撈到日誌就略過段落，不出空殼。

> - **ES 真實資料，11/3 事故**：事件期間 68 筆、事件前 8 筆，列出 3 個樣板——cart-api `Unhandled exception in handler` 14.7 倍、ord-svc `ORA-<NUM>: TNS:Connect timeout occurred` 4.2 倍、bill-svc `Downstream tax-gw:<NUM> returned <NUM>` 2.5 倍。cart-api 排第一，跟資料本身最早起點（cart-api 14:46）一致。對照的 10/30、10/31、11/2 同時段與 11/3 事故前，這 3 個服務完全沒有錯誤日誌，段落正確略過。
> - **產生器（有標準答案）**：`callee_slowdown` 根因 bill-svc 排第 1 ✓；`load_amplification` 根因 cart-api 排第 3 ✗（下游 bill-svc、ord-svc 的慢交易警告量更大）。安靜時段對照都回「沒有明顯暴增」。
>
> 結論：它回答「壞掉時噴了什麼錯、從哪條 trace 看」，**不是根因排名**——錯誤量最大的常常是被拖累的下游。

**自我健康心跳（Dead Man's Snitch）**：偵測 workflow 最後一個節點是 `notifier.watchdog`（scope `after_finalize`：引擎在資料庫寫入、通知送出、「已通知」與結案都寫成功之後才跑它）。每輪送一則 `alertname=AIOpsWatchdog`、`endsAt = 現在 + ttl_minutes`（預設 15，設排程間隔的 2～3 倍）的告警。排程停了、Pod 掛了、資料源或 DB 讓整輪失敗，就沒人續命，ttl 一到 Alertmanager 把它標成 resolved。在 Alertmanager 給它一條獨立路由並開 `send_resolved: true`，「心跳恢復」就等於「AIOps 停了」；或接 Healthchecks.io 這類 DMS 服務（收不到心跳才叫）：

```yaml
route:
  routes:
    - matchers: ['alertname="AIOpsWatchdog"']
      receiver: aiops-selfcheck
      group_wait: 0s
      repeat_interval: 5m
receivers:
  - name: aiops-selfcheck
    webhook_configs:
      - url: <值班通道或 DMS 的 webhook>
        send_resolved: true
```

> 實測（叢集裡的 Alertmanager，ttl 1 分鐘）：10:07:19 成功一輪 → `AIOpsWatchdog` active、endsAt 10:08:19；10:07:39 模擬 DB 寫入失敗的一輪 → 心跳沒送、endsAt 不變；10:08:20 從 active 清單消失（=resolved）。Alertmanager chart 已支援這條路由：values 設 `watchdogRoute.enabled: true` 與 `receiver`（預設關閉，範例見 `deploy/helm/aiops-alertmanager/values-channels.example.yaml`，做法 A／B 都有）。叢集上那台還沒套用。

### 告警生命週期：一場事故一張動態卡

偵測 workflow 的批次段順序是：收斂（`group_*`）→ 計分 → **`alert.absorb_open`** → 抑制（`suppress_*`）→ `alert.notify_payload` → **`alert.heartbeat`** → `notifier.auto`。

- **吸收（`alert.absorb_open`）**：這一輪新組出的事件群只要跟某張進行中的卡有共同實體、且距離它的最後一筆不到 `resolve_after_minutes`（預設 15，要跟 heartbeat 的一樣），就併進那張卡（異常點去重、分數取大），不另開 `incident_uid`。寫回資料庫的是整份快照；事件群的統計都從記憶體裡的事件清單算，不重新聚合資料庫。事件清單隨持續時間線性成長（每分鐘一筆的 2 小時事故約 120 筆）。
- **滾動摘要**：每輪重算 annotations——已持續分鐘數、累計筆數、起跳值 → 峰值（分數最高的那一點）→ 最新值、初發與最近時間。`startsAt` 鎖在初發。annotations 不影響 Alertmanager 辨識，labels 才影響。
- **升級**：嚴重度只升不降（降級在門檻附近會來回跳）。升級或吸收了新實體讓 labels 變了，心跳先對**舊 labels** 送 `endsAt = now`，同一輪再送新 labels（同一個 `incident_uid`），標題加 `[Escalated from P2]`，tags 記 `escalated:P2>P0`。warning 變 anomaly 走同一條路。
- **結案防抖動**：實體最後一個「接得上」的異常（跟前一筆相隔不到 15 分鐘）之後安靜滿 15 分鐘才送 `endsAt`。中間斷超過 15 分鐘就視為真的恢復，後來的異常是另一場。這是時間判斷，不是「連續 N 輪無異常」。
- **冷卻只擋新卡**：`suppress_dedup` 只作用在這一輪新組出的事件群；進行中的卡由 heartbeat 每輪續命，不受冷卻影響，也不阻止異常點寫入資料庫。
- **深度連結（metrics → trace → log）**：設定環境變數 `AIOPS_GRAFANA_URL` 後，`alert.notify_payload` 的 `links` 參數（`{dashboard: d/apm-overview, var: service, pad_minutes: 15, datasources: {traces: apm-traces, logs: apm-logs}}`）讓每張卡的 annotations 多出 `grafana_dashboard_url`（儀表板變數鎖在事故裡的服務、時間鎖在初發到最近一筆、前後各留 15 分鐘）與 `grafana_traces_url`／`grafana_logs_url`（Explore，Lucene `service.name:("a" OR "b")`，同一段時間）；儀表板連結同時是 Alertmanager 的 `generatorURL`（Grafana 卡片上的 See source）。心跳、升級、結案通知都帶同一組連結；labels 一個都沒多。RCA 報告的「Trace 佐證」段落（`section_params: {trace_evidence: {traces_datasource: apm-traces}}`）則在最慢的 trace 後附 Explore 連結（鎖 `trace.id`）。沒設 `AIOPS_GRAFANA_URL` 就一個連結都不產生，其他內容一字不差。在真實 Grafana 13.0.7 上打開儀表板連結：變數 = `cart-api`、時間 = 回放事故的 14:35–16:30Z；Explore 連結要登入後才看得到（匿名 Viewer 沒有 Explore 權限）。

> 真實資料回放（cart-api 11/3，多數決異常點 91 個、14:50~16:20Z，每 5 分鐘一輪）：改版前 Alertmanager 上出現 3 張卡（14:50、15:25、16:00 各開一張，資料庫 19 筆事件群、16 筆被冷卻擋下），描述從頭到尾是「1 筆」；改版後只有 1 張卡（14:50 開、16:35 結案，續命 20 次），資料庫 1 筆事件群，15:30 的卡寫著「已持續 40 分鐘，累計 41 筆，起跳 37.51 → 峰值 42.38 → 最新 42.62」。

### 穩定性防呆：斷流不送心跳、先寫再送、冷啟動不亂學

這三項是 [docs/PRODUCTION_READINESS.md](docs/PRODUCTION_READINESS.md) 盤點出來的生產問題，修法都在平台內部，沒有新架構。

- **資料斷流**：一條偵測 workflow 的序列「抓不到資料」（資料源丟 `NoDataError`、或抓回來是空的；ES 每桶都是 NaN／Inf 也算）的比例達到 `max_failed_target_ratio`（workflow 欄位，預設 `1.0` = 全部）時，整輪是基礎設施錯誤：`notifier.watchdog` 不續命、排程的備援通報會響。這是給「PVC 滿、APM 寫不進去、索引不見了」用的，那時查詢會回 HTTP 200 但零筆，以前會被當成「今天沒異常」照送心跳。查詢寫錯（`BadRequest`）只記錯誤，不算斷流；個別序列本來就稀疏的不會觸發（預設要全部）。
- **先寫資料庫、再送通知**：所有 `notifier.*` 改在 `after_persist` 階段。異常點、事件群、進行中事件群的更新在 `persist_batch` 的**單一交易**寫完（資料庫短暫斷線時整筆回復、重試 2 次），通知才送；「已通知」紀錄與結案要等通知真的送出去（`finalize_batch`）。
  - 修掉一個會**永久遺失告警**的問題：以前異常點每條序列先寫、通知後送，推送失敗後下一輪重新偵測到同一個點，`detector.new_only` 會當成處理過而丟掉，那次告警再也不會發。實測（固定 11:40 的異常，推送失敗後恢復）：改前連跑 3 輪都是 0 筆；改後下一輪補送。
  - 結案那一輪推送失敗：事件群不會被標成已結案，下一輪再送一次 `endsAt`。
- **冷啟動不亂學**：偵測節點參數 `min_train_points`（預設 60，每分鐘一點的 1 小時）。訓練點數不到就不學、不判、不投票，下一輪資料夠了再學；`detector.rules`（靜態上下限）不需要歷史資料，一個點也照判，剛上線的服務靠它兜底。**限制**：一上線就壞、而且壞超過 1 小時的服務，還是會把壞的狀態學成正常。

### 升級既有 PostgreSQL（沒有 Alembic，要手動做一次）

全新資料庫由程式自動建表。**已經在跑的 PostgreSQL 必須依序執行 [migrations/postgres/](migrations/postgres/) 裡的 SQL**，否則新版程式會因為缺欄位而失敗：

```bash
psql -h <host> -U <user> -d aiops -f migrations/postgres/0001_detector_models_version.sql
```

```bash
psql -h <host> -U <user> -d aiops -f migrations/postgres/0002_node_lineage_and_tuning_scope.sql
```

```bash
psql -h <host> -U <user> -d aiops -f migrations/postgres/0003_artifacts_missed_feedback_knowledge.sql
```

```bash
psql -h <host> -U <user> -d aiops -f migrations/postgres/0004_anomaly_level.sql
```

執行前先停掉排程與 API。`0004` 加上異常的等級欄位（`anomaly`／`warning`），既有資料一律是 `anomaly`。它帶 `lock_timeout = '5s'`：拿不到鎖就整個放棄（什麼都沒改），找空檔重跑；回滾 SQL 寫在檔案開頭的註解。

> 實測（PostgreSQL 16.15 容器，`anomalies` 200 萬筆／266 MB）：沒人擋時 33 ms 跑完、filenode 不變（沒有整表重寫），200 萬筆都讀成 `anomaly`；重複執行只出 NOTICE；回滾 SQL 正常拿掉欄位。另開一個連線開著交易讀這張表時，migration 等滿 5 秒後報 `lock timeout` 放棄、欄位沒加；**等鎖這段時間，後面進來的一般讀取被卡了約 4 秒**——這是剩下的代價，尖峰時段不要跑。PostgreSQL 10 以下會整表重寫，沒測過。`0002` 會把舊基準線掛到名為 `detect` 的節點（內建 workflow 的名稱），自己改過節點名稱的 workflow 第一次執行會重訓一次。`0003` 加上模型檔欄位、漏報補錄欄位、提案核准時間與經驗庫資料表；既有已核准的提案沒有核准時間，不會出現在調參成效報表。每個檔都可以重複執行。**這些 SQL 還沒有在真的 PostgreSQL 上跑過**（測試只用 SQLite），第一次請先在備份或測試庫上試。

`it_otel` 的偵測改成 `detector.hourly_baseline` 之後，舊的 `zscore` 基準線不會被沿用（演算法不同），第一次執行會抓 7 天資料重訓。

SQLite（本機、測試）刪掉舊的 `aiops-local.db` 重建即可。

### 合成資料產生器（`scripts/synth_generator/`）

叢集上現有的 `scn`／`synth` 資料有三個缺陷，驗不了 RCA：trace 全是單服務孤島（跨服務呼叫 0 條）、各服務的延遲波形各自獨立造、log 面板讀不到例外訊息。這支產生器改成**由服務拓撲與故障劇本因果驅動**，同一份系統狀態同時產出 metrics、traces、error logs，並另存標準答案 `ground_truth.json`。它放在 `aiops/` 外面，平台程式不 import 它。

四層：

| 層 | 檔案 | 做什麼 |
|---|---|---|
| 拓撲 | `topology.py` | 呼叫 DAG：`api-gw → [cart-api, auth-svc]`、`cart-api → [ord-svc 0.8, redis-cache]`、`ord-svc → [bill-svc, inventory-db]`、`bill-svc → [billing-db, tax-gw]`。redis、db、tax-gw 是外部依賴（只有 client span） |
| 故障劇本 | `faults.py`、`scenarios/*.yaml` | 何時開始、根因是誰、哪種機制、各服務的 lag 與振幅；共用一條包絡線（緩升 10 分、高原 90 分、指數衰減 20 分） |
| 系統狀態＋發射器 | `waveforms.py`、`metric_emitter.py`、`trace_emitter.py` | `waveforms.SystemState` 是**唯一**的故障狀態函數（每服務每分鐘的請求數、處理時間、錯誤率、重試倍數，以及往上游加總出來的整段耗時與失敗率）。metrics 是整段 2 週的解析模型；trace 只在事件前後 ±2 小時高密度（每分鐘 12 條），平常每小時 6 條健康樣本，逐筆走訪呼叫樹，取的是同一份陣列 |
| 寫入 | `generate.py`（本機檔案）、`es_writer.py`（載入 ES） | 預設只輸出 bulk NDJSON；寫 ES 要使用者自己跑 |

產生（只寫本機，不連網路；同一個 seed 輸出逐位元組相同）：

```bash
.venv/Scripts/python -m scripts.synth_generator --scenario scripts/synth_generator/scenarios/nov03_cascade.yaml --out synth-out/nov03_cascade
```

輸出資料夾（`synth-out/` 已被 `.gitignore` 擋住）有 `metrics.ndjson`、`traces.ndjson`、`logs.ndjson`（`_bulk` 的 create 格式）、`ground_truth.json`（根因、各服務起點；**不寫進 ES**）、`manifest.json`（筆數與 sha256）。一個 2 週劇本約 13 萬筆 metrics、9 萬筆 trace、數千筆 log，共約 200 MB，跑 15 秒。

離線驗證（直接 import 平台的程式吃這些檔案，印出拓撲邊、偵測器回放、三種根因演算法的排名）：

```bash
.venv/Scripts/python -m scripts.synth_generator.validate_offline synth-out/nov03_cascade
```

#### 兩種故障機制（`mechanism` 必填）

平台的拓撲是 (caller, callee)，傳播方向是 **callee → caller**（`engine/topology.propagation_edges`：下游出事拖垮上游）。劇本的方向必須講清楚，所以分兩種：

| mechanism | 根因在哪 | 劇本寫什麼 | 訊號長什麼樣 | 拓撲／PageRank 會怎樣 |
|---|---|---|---|---|
| `callee_slowdown`（`nov03_callee_slowdown.yaml`） | 最下游的服務（bill-svc 呼叫的 tax-gw 變慢） | 只寫根因自己的偏移；不准寫上游 | 上游的整段耗時本來就包含下游，**同一分鐘**一起變慢（trace 是加總出來的，不另外造） | 跟平台同向，應該指向最下游 |
| `load_amplification`（`nov03_cascade.yaml`） | 上游（cart-api 連線池耗盡、重試風暴） | 根因要有 `call_multiplier > 1`（對下游的呼叫次數放大）；下游各自的 lag 與偏移 | 下游請求量立刻上升，下游自己的處理時間在 lag 之後才上升；上游的整段耗時也包含下游的變慢 | 跟平台反向，**預期會指錯人**——這正是這份資料要量的 |

離線驗證的實際結果（seed 42，事件 11/3 14:05Z 起；拓撲由 trace 用平台的 `_span` 與 `service_edges` 還原，四條邊與劇本 DAG 完全一致）：

| 劇本（真實根因） | topological | pagerank | cross_correlation（預設 `min_corr: 0.5`） |
|---|---|---|---|
| `nov03_callee_slowdown`（bill-svc） | bill-svc ✓ | bill-svc ✓ | 排不出（所有服務同一分鐘變慢，互相關最佳位移是 0） |
| `nov03_cascade`（cart-api） | bill-svc ✗ | ord-svc ✗ | 排不出（差分後 \|r\| 最高 0.35）；降到 `min_corr: 0.2` 才排出 cart-api |

互相關排不出來的原因：平台先做一階差分再算相關，緩升 10 分鐘的波形差分之後幾乎只剩雜訊（cart-api 對 bill-svc 差分後 r=0.25）；不差分直接比水位是 r=0.98、cart-api 領先 11 點。這是演算法的前處理問題，不是資料沒有先後。

#### 指標的物理特性

- **請求數**：入口 api-gw 有強日夜週期（台北時間 04:00 最低、16:00 最高）、週末 × 0.4、Poisson 噪音；下游依呼叫機率一路乘下去。給 `detector.hourly_baseline`（14 天訓練，`min_weeks` 預設 2 剛好夠）。
- **延遲**：每筆請求對數常態（右偏長尾），沒有日夜波動。給 `detector.mad`、`detector.quantile_drift`。
- **錯誤率**：接近 0 的截斷常態，加偶發單點毛刺（每服務每分鐘 1/1500 機率），用來測 `detector.ensemble` 能不能濾掉單點雜訊。實測：mad＋quantile_drift＋hourly_baseline 三個多數決**濾不掉**（38 個毛刺過了 33 個，因為 mad 與 hourly 都會抓單點）；只用 mad＋quantile_drift 兩個（strict 平手不算）剩 4 個。另外 mad 對錯誤率幾乎無法使用：每分鐘失敗數大多是 0，MAD = 0，任何一筆失敗都算異常（8 天跳 1.1 萬點）。
- **CPU 沒有產生**：APM 的 CPU 在 `metrics-apm.app.*`，不在平台唯讀 key 能讀的 `metrics-apm.transaction.1m-*` 底下。

#### 文件格式（照 Elastic APM 資料模型）

- **trace** 發兩種文件：`processor.event: transaction`（`transaction.id`、`transaction.duration.us`；被別的服務呼叫時 `parent.id` = 上游那個 client span 的 `span.id`）與 `processor.event: span`（`span.id`、`span.duration.us`、`parent.id` = 所屬 transaction、`span.destination.service.resource`）。transaction 文件不帶 `span` 欄位，否則平台的 `_span` 會讀成耗時 0。結果欄位是 `event.outcome`（`success`／`failure`）。
- **metrics** 依 `event.outcome` 拆成 success／failure 兩份（有失敗才有 failure），`transaction.duration.histogram` 是 ES histogram 型別（Grafana p95 面板吃它）、`summary.sum`／`value_count`、`event.success_count`。
- **log**（`processor.event: error`）在 span 出錯（自己出錯，或呼叫下游失敗）或耗時超過平常 2.5 倍時寫，帶同一個 `trace.id`、`transaction.id`，`parent.id` 指向出事的 transaction 或 span。
- **`error.exception` 刻意寫成單一物件 `{"type","message","handled"}`，不是 Elastic APM 真實格式的陣列。** Grafana 的 log 面板（`logMessageField: error.exception.message`）沿 `_source` 逐層走，走不進陣列，所以舊資料只顯示文件 id；ES 索引上物件跟單元素陣列等價。另外寫頂層 `message` 當保險。用程式模擬 Grafana 的取值方式，6,266 筆 log 全部取得到字串（同樣文件改成陣列則全部取不到）。**實際在 Grafana 畫面上能不能顯示，要等資料載入後才確認，還沒驗證。**

#### 載入 ES（使用者自己跑）

`es_writer.py` 要 ES 管理員密碼，平台程式與 AI 助理都不跑。預設 dry-run，只讀本機檔案、印出會建什麼，不連網路：

```bash
.venv/Scripts/python -m scripts.synth_generator.es_writer synth-out/nov03_cascade
```

建議先用唯讀 key 抄既有 `scn` 資料串流的 mapping（`schema/*.json` 目前是**手寫的備用 mapping，不是從叢集抓的**；這一步會覆蓋它，`summary.sum` 這類欄位的型別才會跟現有資料一樣）：

```bash
.venv/Scripts/python -m scripts.synth_generator.es_writer --fetch-schema
```

真的寫入（讀 `secrets.local.yaml` 的 `es.url` 與 `es.admin_password`；叢集上的 ES 要先 `kubectl --context rancher-desktop -n es-test port-forward svc/elasticsearch 19200:9200`）：

```bash
.venv/Scripts/python -m scripts.synth_generator.es_writer synth-out/nov03_cascade --apply
```

- 每個劇本寫進自己的 namespace（`nov03_cascade` → `gencascade`、`nov03_callee_slowdown` → `genslowdown`）：`metrics-apm.transaction.1m-<ns>`、`traces-apm-<ns>`、`logs-apm.error-<ns>`，都在平台唯讀 key 與 Grafana 讀得到的 pattern 底下。兩個劇本是同一批服務、同一段時間，所以分開放；Grafana 會兩份一起看到，看其中一份要在查詢加 `data_stream.namespace`。
- namespace 只准 `gen` 開頭；檔案裡出現別的索引名稱就拒絕。**既有的 `scn`、`synth` 一筆都不碰**：不刪任何東西，只建／更新名為 `aiops-synth-<ns>-*` 的 index template（priority 500，只套用到那三個名稱）。
- 資料串流已存在就拒絕寫（避免重跑寫兩份），確定要追加才加 `--append`。
- 自己的 template 會蓋過 ES 內建的 APM template（只有 priority 最高的那個生效），所以內建的 APM ingest pipeline 不會跑，文件原樣寫入。
- **`--apply` 與 `--fetch-schema` 都還沒有對真的 ES 跑過**（開發時不准用管理員密碼）。

## 設定（環境變數）

| 變數 | 預設 | 說明 |
|---|---|---|
| `AIOPS_DATABASE_URL` | `sqlite:///./aiops-local.db` | 正式用 PostgreSQL，例如 `postgresql+psycopg://user:pass@host:5432/aiops`（需另裝 `psycopg`） |
| `AIOPS_API_TOKEN` | （無） | **必填**。沒設時所有 API 一律拒絕。可逗號分隔多組 |
| `AIOPS_AUTH_DISABLED` | `false` | `true` 時關閉驗證，只限本機開發 |
| `AIOPS_NOTIFIER` | `log` | `log`：只寫日誌；`alertmanager`：真的推送 |
| `AIOPS_ALERTMANAGER_URL` | （無） | 例如 `http://aiops-alertmanager.aiops-platform.svc:9093` |
| `AIOPS_ALERTMANAGER_TOKEN` | （無） | 選填，Bearer token |
| `AIOPS_GRAFANA_URL` | （無） | 選填，維運點得到的 Grafana 網址（例如 `https://grafana.example.com`）。有設才產生告警卡片與 RCA 報告的深度連結；儀表板、變數、資料源 uid 寫在管線 YAML |
| `AIOPS_PROMETHEUS_URL`／`AIOPS_PROMETHEUS_TOKEN` | （無） | Prometheus 位址與選填 token |
| `AIOPS_VICTORIAMETRICS_URL`／`AIOPS_VICTORIAMETRICS_TOKEN` | （無） | VictoriaMetrics 位址與選填 token |
| `AIOPS_ES_URL`／`AIOPS_ES_API_KEY` | （無） | Elasticsearch 位址與選填 API key |
| `AIOPS_ES_TRACE_INDEX`／`AIOPS_ES_LOG_INDEX` | `traces-apm-*`／`logs-apm.*` | RCA 隨需撈 trace／錯誤日誌的索引 |
| `AIOPS_FALLBACK_LOG` | （無，寫 stderr） | 備援日誌檔路徑 |
| `AIOPS_FALLBACK_WEBHOOK` | （無） | 資料庫或推送失敗時直接打的 webhook |

## 容器與部署

### 本機整套（docker compose）

```bash
docker compose up --build
```

Rancher Desktop 用 containerd 時改成 `nerdctl compose up --build`。會起 PostgreSQL、Alertmanager（只收不送）、API、排程（每 5 分鐘一輪）。帳密都是開發用的假值。

### Kubernetes：平台本身

```bash
helm upgrade --install aiops-platform deploy/helm/aiops-platform -n aiops-platform --create-namespace --set existingSecret=<你的 Secret 名稱> -f <放排程工作的 values 檔>
```

- API 是 Deployment，排程是 CronJob（每次跑一輪 `run-once`，`concurrencyPolicy: Forbid` 不會重疊）。
- **機密只能從 Secret 進來**：`existingSecret` 指向一個含 `AIOPS_API_TOKEN`、`AIOPS_DATABASE_URL` 等的 Secret。沒給 chart 會拒絕產生。
- `devSecretEnv`、`devPostgres` 只限開發（機密會留在 helm 紀錄、PostgreSQL 沒有持久儲存）。
- 排程工作寫在 values 的 `tasks.jobs`，格式同 `configs/jobs.example.yaml`。

### Kubernetes：Grafana 與 Elasticsearch

```bash
helm upgrade --install grafana deploy/helm/aiops-grafana -n grafana-new --create-namespace
```

Grafana 只顯示，不發通知；Secret `grafana-env` 要先建好（鍵見 `deploy/helm/aiops-grafana/values.yaml`）。**ES 還沒有 chart**：原始 chart 不在本機，`deploy/elasticsearch/` 只有說明與去機密的快照。舊引擎已從開發叢集移除，叢集上的 Grafana 已改由本 chart 部署（舊的告警規則與 `aiops-db` 資料源已一併移除）。

接手既有的 release 時要注意：Secret `grafana-env` 是舊 chart 建的，帶 `helm.sh/resource-policy: keep`，`helm upgrade` 不會刪它；新 chart 不管這個 Secret 的內容。裡面舊引擎留下的鍵（`AIOPS_PG_*`、`GRAFANA_PG_*`、`NOTIFY_*_URL`）已沒人用，可以刪：

```bash
kubectl -n grafana-new patch secret grafana-env --type=json -p '[{"op":"remove","path":"/data/AIOPS_PG_DATABASE"},{"op":"remove","path":"/data/AIOPS_PG_HOST"},{"op":"remove","path":"/data/AIOPS_PG_SSLMODE"},{"op":"remove","path":"/data/GRAFANA_PG_PASSWORD"},{"op":"remove","path":"/data/GRAFANA_PG_USER"},{"op":"remove","path":"/data/NOTIFY_IMC_URL"},{"op":"remove","path":"/data/NOTIFY_IT_URL"},{"op":"remove","path":"/data/NOTIFY_PLATFORM_URL"}]'
```

### Kubernetes：Alertmanager

```bash
helm upgrade --install aiops-alertmanager deploy/helm/aiops-alertmanager -n aiops-platform --create-namespace -f <你的 values 檔>
```

部門路由寫在 values 的 `deptRoutes`，通道寫在 `receivers`，Slack／Teams 寫法見 `deploy/helm/aiops-alertmanager/values-channels.example.yaml`。平台會送恢復通知，receiver 可以開 `send_resolved: true`。正式環境請開 `persistence.enabled`。**不要在 Alertmanager 設靜音或 inhibit_rules**，靜音只在平台設一份（`POST /api/v1/alerts/silences`）。Grafana 把它加成資料源只用來顯示告警。

## 目前狀態

已完成並有測試：持久化、偵測到告警的管線、批次訓練、排程、API 認證、Prometheus／VictoriaMetrics／ES 資料源、Lot 履歷合成、外部異常接收、11 個偵測算子（見上表，含 168 時段基準線、同比／環比、分位數漂移、動態同儕、多偵測器裁決）、工廠 Lot 收斂、RCA（可抽換的根因演算法：拓撲、PageRank、時序互相關；10 個段落含歷史相似案例與錯誤日誌樣式比對）、訓練排除歷史事故窗、warning／anomaly 兩級告警、自我健康心跳、LLM 敘述與回饋抽規則（含依據驗證與退回模板）、漏報補錄、調參成效報表、規則升級器、通知抑制、恢復通知（心跳與結案）、容器與 helm chart。

**演算法還沒在真實資料上驗證過**，還沒有任何誤報率或根因準確度的數字；先別把「測試全過」當成「演算法有用」。計畫中的真實資料驗收有三項：

1. 168 時段基準線 vs 舊 zscore：同一段真實時序整週回放，比較尖峰時段與離峰時段各跳幾次。
2. 多偵測器裁決：聯集 vs 多數決濾掉多少單一算法的毛刺，留下的是不是大尖峰。
3. RCA：拓撲 vs 時序互相關，對一次真實發生的延遲尖峰各自排出的第一候選，以及互相關的領先點數與相關係數有沒有意義（同時有流量不等於因果）。

這三項卡在上面的唯讀 ES key 還沒建。ES 裡實際有哪些服務、多少筆資料、涵蓋哪幾天，也還沒有確認過，以查到的為準。

演算法的效果只用合成資料（mock）驗證過：`seasonal:` 開頭的 mock query 會產生依時間戳決定、有日週期與週末效應的資料，其他 mock query 是一段雜訊加一個尖峰。在真實指標上的誤報率、互相關排出的根因準不準，都還沒驗證。

已在本機 rancher-desktop 叢集實測：平台（容器）跑在 K8s、資料存在 PostgreSQL、CronJob 排程、Alertmanager 分流與結案、Grafana 顯示、API 等待資料庫啟動。

還沒做或還沒驗證：

- 資料源只用模擬伺服器測過；還沒對真的 Prometheus、VictoriaMetrics、Elasticsearch 取過數。
- LLM 只用假的模型回應測過，沒有呼叫過真的模型。
- Slack／Teams 通道只驗過設定格式（`amtool check-config`），沒有真的送出訊息。
- 資料表結構還沒改用 Alembic migration（`create_all` 只會新增表、不會改欄位）。
- API token 沒有角色區分（有 token 就能做所有事）。
- 新平台自己的 Grafana 儀表板沒做：Grafana 的儀表板設定在舊 repo 的 chart 裡，要動舊 repo 才放得進去。
- 開發用 PostgreSQL（`devPostgres`）沒有持久儲存，pod 重建後資料表要等平台下次啟動才會重建。
