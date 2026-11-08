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
│       ├── notifiers.py             notifier.log／alertmanager／auto（依 AIOPS_NOTIFIER 選）
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

**拓撲從哪來（隨需計算）：**日常偵測完全不撈 trace（trace 量通常是指標的 10 倍以上）。只有跑 RCA 報告時，`ingestion.incident_traces` 才去 ES 撈事件開始前 15 分鐘到結束的 span，`rca.build_dynamic_topology` 用 `parent.id` → `span.id` 還原當下服務之間的呼叫邊，跟既有拓撲（事件快照、批次匯入的 trace）取聯集後交給根因演算法。ES 沒設定就略過、連不上就記進 `errors` 並略過，報告照出。trace 索引預設 `traces-apm-*`，用環境變數 `AIOPS_ES_TRACE_INDEX` 改；上限 `max_spans`（預設 20000），超過會截斷並在結果的 `trace_fetch.truncated` 註明。

> 真實資料（11/3 事件，14:05–16:20Z）：撈到 10,167 個 span、3,389 條 trace，**跨服務呼叫 0 條**——這批 trace 每條都只在一個服務裡（下游只有 redis、postgresql、oracle、tax-gw），所以拓撲與 PageRank 仍排不出根因。機制本身是通的，缺的是有串接 trace context 的資料。

`rca_report.yaml` 的 `root_cause` 節點決定根因怎麼排序，報告段落只負責呈現：

| 根因算子 | 做法 | 需要 |
|---|---|---|
| `rca.root_cause.topological`（預設） | 傳播圖上沒有異常上游的實體，依最早出現排序 | 拓撲或 Lot 流向 |
| `rca.root_cause.pagerank` | 反向傳播圖上的 Personalized PageRank：拖垮越多下游、越異常的上游排越前 | 拓撲或 Lot 流向 |
| `rca.root_cause.cross_correlation` | 事件時段原始時序的時間位移互相關：誰的波動領先別人 | 前面要有 `ingestion.incident_series`（`rca_report_correlation.yaml` 已經接好）；這是相關不是因果 |

報告段落是註冊的 `rca.section.<代號>`，新增段落 = 在 `aiops/stages/rca/sections/` 放一個檔案。`similar_cases` 段落會列出經驗庫裡相似的歷史事故與當時的處置。

### 學習閉環

| 功能 | 怎麼用 |
|---|---|
| 漏報補錄 | `POST /api/v1/tuning/feedback` 不給 `incident_uid`，改給 `pipeline`、`entity`、`time_range`、`verdict: missed`。轉提案後回測會檢查那段時間抓不抓得到（`missed_windows`） |
| 歷史事故經驗庫 | 回饋 `verdict: true_positive` 時寫 `resolution`（可加 `root_cause`），之後類似事故的 RCA 會列出來 |
| 調參成效報表 | `GET /api/v1/tuning/metrics?window_days=7`：每個核准的調參，前後各 N 天同一層的事件群數與誤報／確認／漏報數。依偵測器血緣歸屬，是前後對照不是因果證明 |
| 規則升級器 | `POST /api/v1/tuning/rules/promote`（或排程 `run-once -w admin_rule_promote`）：同一層（管線 × 實體）N 天內被調參太多次，就用它的基準線換算出 `detector.rules` 上下限提案。目前支援 `zscore`、`mad` |

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

執行前先停掉排程與 API。`0002` 會把舊基準線掛到名為 `detect` 的節點（內建 workflow 的名稱），自己改過節點名稱的 workflow 第一次執行會重訓一次。`0003` 加上模型檔欄位、漏報補錄欄位、提案核准時間與經驗庫資料表；既有已核准的提案沒有核准時間，不會出現在調參成效報表。三個檔都可以重複執行。**這些 SQL 還沒有在真的 PostgreSQL 上跑過**（測試只用 SQLite），第一次請先在備份或測試庫上試。

`it_otel` 的偵測改成 `detector.hourly_baseline` 之後，舊的 `zscore` 基準線不會被沿用（演算法不同），第一次執行會抓 7 天資料重訓。

SQLite（本機、測試）刪掉舊的 `aiops-local.db` 重建即可。

## 設定（環境變數）

| 變數 | 預設 | 說明 |
|---|---|---|
| `AIOPS_DATABASE_URL` | `sqlite:///./aiops-local.db` | 正式用 PostgreSQL，例如 `postgresql+psycopg://user:pass@host:5432/aiops`（需另裝 `psycopg`） |
| `AIOPS_API_TOKEN` | （無） | **必填**。沒設時所有 API 一律拒絕。可逗號分隔多組 |
| `AIOPS_AUTH_DISABLED` | `false` | `true` 時關閉驗證，只限本機開發 |
| `AIOPS_NOTIFIER` | `log` | `log`：只寫日誌；`alertmanager`：真的推送 |
| `AIOPS_ALERTMANAGER_URL` | （無） | 例如 `http://aiops-alertmanager.aiops-platform.svc:9093` |
| `AIOPS_ALERTMANAGER_TOKEN` | （無） | 選填，Bearer token |
| `AIOPS_PROMETHEUS_URL`／`AIOPS_PROMETHEUS_TOKEN` | （無） | Prometheus 位址與選填 token |
| `AIOPS_VICTORIAMETRICS_URL`／`AIOPS_VICTORIAMETRICS_TOKEN` | （無） | VictoriaMetrics 位址與選填 token |
| `AIOPS_ES_URL`／`AIOPS_ES_API_KEY` | （無） | Elasticsearch 位址與選填 API key |
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

已完成並有測試：持久化、偵測到告警的管線、批次訓練、排程、API 認證、Prometheus／VictoriaMetrics／ES 資料源、Lot 履歷合成、外部異常接收、11 個偵測算子（見上表，含 168 時段基準線、同比／環比、分位數漂移、動態同儕、多偵測器裁決）、工廠 Lot 收斂、RCA（可抽換的根因演算法：拓撲、PageRank、時序互相關；9 個段落含歷史相似案例）、LLM 敘述與回饋抽規則（含依據驗證與退回模板）、漏報補錄、調參成效報表、規則升級器、通知抑制、恢復通知（心跳與結案）、容器與 helm chart。

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
