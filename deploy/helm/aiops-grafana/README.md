# aiops-grafana

只負責**顯示**的 Grafana：APM 的 metrics、trace、log（讀 Elasticsearch），以及平台的告警（讀 `aiops-alertmanager`）。

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

儀表板：**APM 總覽：metrics → trace → log**（`apm-overview`），5 個面板，只靠 ES。

## 部署

```bash
helm upgrade --install grafana deploy/helm/aiops-grafana -n grafana-new --create-namespace
```

Secret `grafana-env`（可用 `envSecret` 改名）要先建好，鍵見 `values.yaml`。release 名稱用 `grafana`、namespace 用 `grafana-new`，就能接手叢集上已在跑的那一個（資源名稱沿用舊 chart）。

## 從舊 repo 沒有搬過來的東西（以及原因）

舊 repo（`aiops-data-foundation`）的 Grafana chart 有下列東西，**刻意沒有搬**：

| 舊內容 | 沒搬的原因 |
|---|---|
| 告警規則、contact point、policy | 與新守則衝突：Grafana 不發通知，免得和平台的通知重複 |
| 儀表板「事故與根因」「學習迴圈」「回饋」 | 讀的是舊引擎的 PostgreSQL 與回饋 API，舊引擎已移除 |
| 總覽儀表板的 4 個面板與 `alert` 變數 | 同上（讀 `aiops-db`）。ES 的 5 個面板有留下，做成 `apm-overview` |
| `aiops-db`（PostgreSQL）資料源 | 舊引擎的資料庫已移除 |
| 回饋表單外掛（Business Forms）、內嵌回饋網頁（要關閉 HTML 消毒） | 依賴舊引擎的 API；且關閉消毒有安全代價 |

這些仍在舊 repo 與它的 git 歷史裡。新平台自己的儀表板（事件群、抑制、通知）還沒做。
