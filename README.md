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
                    ┌─────────────── tasks/（排程：python -m aiops.tasks）────────────────┐
                    │                                                                      │
監控資料源 ──► datasource_specs ──► detection ──► anomalies 表                                │
(Prometheus／VM／ES)                 (訓練、偵測)        │                                    │
外部系統 ──────────────────────► ingestion ──► external_anomalies 表                         │
(工廠 FDC 等)                                           │                                    │
                                                        ▼                                    │
                                          alerts（收斂 → 計分 → 抑制 → 產生通知）◄────────────┘
                                                        │
                                                        ▼
                                notifiers ──► aiops-alertmanager ──► Slack／Teams／PagerDuty
                                                        │
                                                        └──► Grafana（只顯示，不發通知）
rca：依 preset 組裝根因報告　　tuning：調參只能走「提案 → 回測 → 核准」
```

| 目錄 | 是什麼 |
|---|---|
| `aiops/domains/` | 業務領域：`detection`、`alerts`、`rca`、`ingestion`、`tuning`。每個領域有 `api.py`、`commands/`、`schemas.py`、`dao.py`，領域之間互不 import |
| `aiops/plugins/` | 可抽換零件：偵測器、收斂、計分、抑制、報告段落、LLM |
| `aiops/presets/` | 場景組態：`it_otel`、`it_apm`、`factory`、`metrics_only` |
| `aiops/datasource_specs/` | 資料源適配：`mock`、`prometheus`、`victoriametrics`、`elasticsearch` |
| `aiops/metric_query/` | 查詢語法解析與時間窗對齊（不連線） |
| `aiops/models/` | SQLAlchemy 資料表與存取 |
| `aiops/tasks/` | 排程與管線調度 |
| `aiops/notifiers/` | 通知出口：Alertmanager v2、log |
| `aiops/security/` | API token 驗證 |
| `aiops/infrastructure/` | 故障看守（本機日誌、備援 webhook） |
| `deploy/helm/aiops-alertmanager/` | 接收本平台通知的 Alertmanager chart |
| `configs/jobs.example.yaml` | 排程工作範例 |

## 本機開發

開發與測試只在 Python 3.14 上跑過；程式用到的語法需要 3.10 以上，其他版本沒有驗證。

```bash
python -m venv .venv
```

```bash
.venv/Scripts/python -m pip install -r requirements.txt
```

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
.venv/Scripts/python -m aiops.tasks run-once -c configs/jobs.example.yaml
```

| 指令 | 用途 |
|---|---|
| `run-once -c <jobs.yaml> [--job 名稱]` | 跑一輪就結束（適合 K8s CronJob）。任一 job 失敗則結束碼非 0 |
| `loop -c <jobs.yaml> --interval 60` | 常駐，每隔 N 秒跑一輪（只適合單一副本） |
| `source-check` | 檢查每個資料源有沒有設定、連不連得上 |

每一輪：模型過期才重訓 → 偵測並只寫入新異常 → 告警管線處理還沒處理過的異常。同一段時間重跑不會重複寫入或重複通知。

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

## 部署 Alertmanager

```bash
helm upgrade --install aiops-alertmanager deploy/helm/aiops-alertmanager -n aiops-platform --create-namespace -f <你的 values 檔>
```

部門路由寫在 values 的 `deptRoutes`，通道寫在 `receivers`。**不要在 Alertmanager 設靜音或 inhibit_rules**，靜音只在平台設一份（`POST /api/v1/alerts/silences`）。Grafana 把它加成資料源只用來顯示告警。

## 目前狀態

已完成並有測試：持久化、偵測到告警的管線、批次訓練、排程、API 認證、Prometheus／VictoriaMetrics／ES 資料源、Lot 履歷合成、外部異常接收、通知抑制與 Alertmanager 推送（已在本機叢集實測）、Grafana 顯示（已實測）。

還沒做或還沒驗證：

- 資料源只用模擬伺服器測過；還沒對真的 Prometheus、VictoriaMetrics、Elasticsearch 取過數。
- 只在 SQLite 測過，還沒對真的 PostgreSQL 跑；資料表結構還沒改用 Alembic migration。
- API token 沒有角色區分（有 token 就能做所有事）。
- 偵測器只有 `zscore`、`shift`；RCA 報告只有 4 段；還沒有 LLM。
- 恢復通知、新平台自己的儀表板、本平台的 Dockerfile 與 helm chart 都還沒做。
