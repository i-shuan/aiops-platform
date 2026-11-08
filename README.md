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
| `AIOPS_ES_API_KEY` | 讀 Elasticsearch 的 key | 在 ES 建**唯讀**的 API key，只給需要的索引 pattern 讀取權限 |
| `AIOPS_PROMETHEUS_TOKEN` | 讀 Prometheus 的 token（選填） | 依公司 Prometheus 的驗證方式核發 |
| `AIOPS_VICTORIAMETRICS_TOKEN` | 讀 VictoriaMetrics 的 token（選填） | 同上 |
| `AIOPS_ALERTMANAGER_TOKEN` | 推送通知到 Alertmanager 的 token（選填） | Alertmanager 前面有加驗證才需要 |
| `AIOPS_FALLBACK_WEBHOOK` | 資料庫掛掉時直接打的備援 webhook | webhook URL 本身就是憑證，正式值放 Secret |

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
  datasource_specs                                                           只有「最終結果」進資料庫（Incident、通知紀錄、模型狀態）
  (Prometheus／VM／ES)                                    節點之間不透過資料庫傳資料；管線前後由 storage/hooks.py 一次載入／一次寫入
                                                                             通知 ──► aiops-alertmanager ──► Slack／Teams／PagerDuty
                                                                                  └──► Grafana（只顯示，不發通知）
```

| 目錄 | 是什麼 |
|---|---|
| `aiops/core/` | `PipelineContext`（強型別）、Pipeline 引擎、算子註冊中心、錯誤型別、請求模型 |
| `aiops/operators/` | 可抽換算子：`detectors`、`alerts`（收斂／計分／抑制／恢復）、`rca`、`llm`（含 `noop`）、`notifiers`、`fetch`、`admin`（手動動作） |
| `aiops/workflows/` | 管線 YAML：`it_otel`、`it_apm`、`factory`、`metrics_only`、`rca_report` 與各 `admin_*` 手動動作。調整流程只改這裡 |
| `aiops/storage/` | SQLAlchemy 資料表、repository、`hooks.py`（管線前後的載入／寫入） |
| `aiops/api/` | 薄薄一層 HTTP：把請求交給 `aiops/runner.py`，不含業務邏輯 |
| `aiops/datasource_specs/` | 資料源適配：`mock`、`prometheus`、`victoriametrics`、`elasticsearch` |
| `aiops/metric_query/` | 查詢語法解析與時間窗對齊（不連線） |
| `aiops/tasks/` | 排程入口（`python -m aiops.tasks`），只負責呼叫 Pipeline |
| `aiops/security/` | API token 驗證 |
| `aiops/infrastructure/` | 故障看守（本機日誌、備援 webhook） |
| `deploy/helm/aiops-alertmanager/` | 接收本平台通知的 Alertmanager chart |
| `deploy/helm/aiops-grafana/` | 只顯示的 Grafana chart：APM（讀 ES）與平台告警（讀 Alertmanager），不發通知 |
| `deploy/helm/aiops-platform/` | 平台本身：API（Deployment）與排程（CronJob） |
| `deploy/elasticsearch/` | ES 說明與去機密的快照（chart 原始碼不在本機，見該資料夾 README） |

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
AIOPS_AUTH_DISABLED=true .venv/Scripts/python -m uvicorn aiops.app:app --reload
```

或設定一組開發用 token：

```bash
AIOPS_API_TOKEN=dev-only-token .venv/Scripts/python -m uvicorn aiops.app:app --reload
```

PowerShell 先執行 `$env:AIOPS_AUTH_DISABLED = "true"` 再啟動。啟動後到 http://127.0.0.1:8000/docs 看所有端點。資料預設存在 `./aiops-local.db`。

### 跑排程

```bash
.venv/Scripts/python -m aiops.tasks run-once -w it_otel
```

| 指令 | 用途 |
|---|---|
| `run-once -w <workflow>` | 跑一輪就結束（適合 K8s CronJob）。`-w` 可以是 `aiops/workflows/` 的名稱或自己的 YAML 路徑；有任何一條序列失敗則結束碼非 0 |
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
- **LLM 供應商**：放一個檔案到 `aiops/operators/llm/`，用 `@register("名稱")` 登記即可，啟動時自動掃描載入，不用改其他地方。

### 升級既有 PostgreSQL（沒有 Alembic，要手動做一次）

全新資料庫由程式自動建表。**已經在跑的 PostgreSQL 必須依序執行 [migrations/postgres/](migrations/postgres/) 裡的 SQL**，否則新版程式會因為缺欄位而失敗：

```bash
psql -h <host> -U <user> -d aiops -f migrations/postgres/0001_detector_models_version.sql
```

```bash
psql -h <host> -U <user> -d aiops -f migrations/postgres/0002_node_lineage_and_tuning_scope.sql
```

執行前先停掉排程與 API。`0002` 會把舊基準線掛到名為 `detect` 的節點（內建 workflow 的名稱），自己改過節點名稱的 workflow 第一次執行會重訓一次。兩個檔都可以重複執行。**這兩個 SQL 還沒有在真的 PostgreSQL 上跑過**（測試只用 SQLite），第一次請先在備份或測試庫上試。

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

已完成並有測試：持久化、偵測到告警的管線、批次訓練、排程、API 認證、Prometheus／VictoriaMetrics／ES 資料源、Lot 履歷合成、外部異常接收、5 種偵測器（zscore、shift、peer、rules、chronic）、工廠 Lot 收斂、8 段 RCA 與拓撲根因推論、LLM 敘述與回饋抽規則（含依據驗證與退回模板）、通知抑制、恢復通知（心跳與結案）、容器與 helm chart。

已在本機 rancher-desktop 叢集實測：平台（容器）跑在 K8s、資料存在 PostgreSQL、CronJob 排程、Alertmanager 分流與結案、Grafana 顯示、API 等待資料庫啟動。

還沒做或還沒驗證：

- 資料源只用模擬伺服器測過；還沒對真的 Prometheus、VictoriaMetrics、Elasticsearch 取過數。
- LLM 只用假的模型回應測過，沒有呼叫過真的模型。
- Slack／Teams 通道只驗過設定格式（`amtool check-config`），沒有真的送出訊息。
- 資料表結構還沒改用 Alembic migration（`create_all` 只會新增表、不會改欄位）。
- API token 沒有角色區分（有 token 就能做所有事）。
- 新平台自己的 Grafana 儀表板沒做：Grafana 的儀表板設定在舊 repo 的 chart 裡，要動舊 repo 才放得進去。
- 開發用 PostgreSQL（`devPostgres`）沒有持久儲存，pod 重建後資料表要等平台下次啟動才會重建。
