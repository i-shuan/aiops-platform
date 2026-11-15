# aiops-grafana

只負責**顯示**的 Grafana：APM 的 metrics、trace、log（讀 Elasticsearch）、平台的告警（讀 `aiops-alertmanager`），以及平台的資料健康度（用唯讀帳號讀平台資料庫的兩張表）。

## 規則（見 CLAUDE.md 第 5 節；測試在 `tests/test_helm_grafana.py`）

- **不發通知。** 沒有告警規則、Contact Point、Notification Policy。通知由平台決定、經 Alertmanager 路由。
- **不放業務邏輯。** 只有資料源與儀表板。
- **只動 helm chart。** 要改資料源或儀表板，改 `files/`，不改 Grafana 本體、不自製外掛。
- **機密只從 Secret 進來。** values 與 `files/` 裡不得有金鑰、密碼。
- **匿名只能是 Viewer。**

## 內容

| 資料源 | 類型 | 用途 |
|---|---|---|
| `apm-metrics`、`apm-traces`、`apm-logs` | Elasticsearch | APM 資料（只讀） |
| `aiops-alertmanager` | Alertmanager | 顯示平台推送的告警 |
| `aiops-platform-db` | PostgreSQL（Grafana 內建，不用裝外掛） | 平台資料庫的**唯讀帳號**，只讀 `data_stream_health`、`pipeline_run_health` |

儀表板（都在 `files/dashboards/`）：

- **APM 總覽：metrics → trace → log**（`apm-overview`），6 個面板，只靠 ES。面板之間有連動：點 ① 的一條線（一支 API）→ ③ 只列這支 API 超過門檻的慢請求；點 ③ 的 `trace.id` → 下面列這條 trace 的每一段，④ 的 log 也只剩這條 trace 的。變數 `slow_ms`（慢請求門檻，預設 1000）、`trace`（`*` = 不篩）；右上「清除 API 與 trace 篩選」還原。
- **AIOps 事件群**（`aiops-incidents`）：說明頁，連到 Alerting → Active notifications（`/alerting/groups`，`aiops-alertmanager`）看平台推送的事件群。values 的 `featureToggles.alertingNavigationV2: false` 是為了這一頁：開著時匿名檢視者看到的標題是「Page not found」。Grafana 的 Alert list 面板讀不到外部 Alertmanager，所以不是面板列表。

- **資料健康度**（`data-health`）：資料有沒有進來、是誰的問題。說明見 `docs/DATA_HEALTH.md`。
  - 一打開就看得到「目前有問題的資料流」（狀態、訊息、從什麼時候開始）與「管線目前的執行結果」，不用先選服務。
  - 上方下拉選單：`管線`、`服務`（都是 SQL 查出來的、可多選、可打字篩選、預設 All），用來往下看單一服務的歷史；`時間軸範圍`預設只畫時間範圍內出過問題的資料流。
  - 下面依序是：每條資料流的狀態時間軸、管線每輪的執行結果（時間軸與表格）、設定錯誤清單、資料流出問題的紀錄（要證明「某個時間點沒有資料」看這張表的開始與最後確認）。
  - 時間軸上最後一次檢查之後是空白 = 平台沒有再檢查到，不是正常。

回饋表單不在 Grafana：告警卡片的 `feedback_url` 註解連到平台自己的回饋頁（平台設了 `AIOPS_PUBLIC_URL` 才有）。

## 部署

```bash
helm upgrade --install grafana deploy/helm/aiops-grafana -n grafana-new --create-namespace
```

Secret `grafana-env`（可用 `envSecret` 改名）要先建好，鍵見 `values.yaml`。

**資料健康度儀表板要多做三件事**（都不改映像檔、不裝外掛）：

1. 平台資料庫先跑 `migrations/postgres/0011_data_health.sql`（全新資料庫由平台啟動時自動建表，不用跑）。
2. 在平台資料庫建一個**唯讀帳號**，只給那兩張表的 `SELECT`（SQL 在同一個 migration 檔的註解裡）。不要用平台自己那組可寫的帳號。
3. 在 `grafana-env` Secret 補四個鍵：`AIOPS_DB_HOST`（主機:埠）、`AIOPS_DB_NAME`、`AIOPS_DB_RO_USER`、`AIOPS_DB_RO_PASSWORD`。
   資料庫沒有 TLS（叢集內的開發資料庫）時加 `--set platformDb.sslmode=disable`，預設是 `require`。

沒補這四個鍵時，其他儀表板不受影響，只有「資料健康度」的面板會顯示連線錯誤。release 名稱用 `grafana`、namespace 用 `grafana-new`，就能接手叢集上已在跑的那一個（資源名稱沿用舊 chart）。

## 從舊 repo 沒有搬過來的東西（以及原因）

舊 repo（`aiops-data-foundation`）的 Grafana chart 有下列東西，**刻意沒有搬**：

| 舊內容 | 沒搬的原因 |
|---|---|
| 告警規則、contact point、policy | 與新守則衝突：Grafana 不發通知，免得和平台的通知重複 |
| 儀表板「事故與根因」「學習迴圈」「回饋」 | 讀的是舊引擎的 PostgreSQL 與回饋 API，舊引擎已移除 |
| 總覽儀表板的 4 個面板與 `alert` 變數 | 同上（讀 `aiops-db`）。ES 的 5 個面板有留下，做成 `apm-overview` |
| 舊引擎的 PostgreSQL 資料源 | 舊引擎的資料庫已移除（現在的 `aiops-platform-db` 是新平台自己的資料庫，只開兩張健康度的表） |
| 回饋表單外掛（Business Forms）、內嵌回饋網頁（要關閉 HTML 消毒） | 依賴舊引擎的 API；且關閉消毒有安全代價 |

這些仍在舊 repo 與它的 git 歷史裡。新平台自己的儀表板目前有資料健康度；事件群列表、抑制、通知的還沒做。
