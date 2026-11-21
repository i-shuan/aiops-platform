# aiops-lite

只拿一把 Grafana Editor Token 的極簡 AIOps：讀規則 → 重放查詢 → 偵測 → 跨系統收斂 → 收集證據交給 LLM → 寫回 Grafana 註記。
不直連任何外部資料庫／ES，沒有 HTTP API，沒有前端。同一個 repo 裡原本的 `aiops/`（aiops-platform）已封存不再更動，
lite 不 import 它的任何程式碼。

```
MQ / CIM / AMHS ──▶ 公司 Grafana（PromQL / SQL / ES 資料源 + 貼標籤的告警規則）
                          │  只靠 GRAFANA_URL + GRAFANA_TOKEN
                          ▼
┌──────────────── aiops-lite（單一 CronJob，每 5 分鐘）────────────────┐
│ 1 讀規則與指標 → 2 異常偵測 → 3 跨系統收斂 → 4 收集 4 包證據 → LLM   │
└──────────────────────────────┬───────────────────────────────────────┘
                               ▼
            Grafana Annotations（三行摘要）+「AIOps 事件總覽」儀表板
```

部署物：1 個 CronJob、1 個帶 PVC 的 PostgreSQL、1 個關係邊 ConfigMap（`deploy/helm/aiops-lite`）。

## 檔案

| 檔案 | 行數 | 負責內容 |
| --- | --- | --- |
| `src/main.py` | 277 | 單一入口：讀環境變數與關係邊 YAML，依序執行主流程；規則之間互相隔離 |
| `src/grafana.py` | 245 | 讀規則標籤、重放 `/api/ds/query`（防呆、5,000 點分段）、ES Proxy 查日誌與 Trace、寫註記 |
| `src/detectors.py` | 312 | 季節基準 + hourly_baseline／residual_shift／residual_chronic／sparse_count + new_only，1% 相對底噪 |
| `src/grouping.py` | 150 | 同實體合併、拓撲鏈收斂、併入進行中事故、嚴重度只升不降、連續 N 輪無異常結案 |
| `src/rca.py` | 166 | 不寫推理演算法：收集 4 包證據、組 Prompt、呼叫 LLM、幻覺檢查、失敗退回原始摘要 |
| `src/report.py` | 54 | 固定三行註記（台灣時間、HTML 跳脫、調整優先權連結） |
| `src/db.py` | 187 | SQLAlchemy Core（SQLite／PostgreSQL），啟動自動建表：基準線、事故、每輪執行紀錄 |

合計約 1,390 行（原本預估 1,850）。

## 各系統要做的事（平台零直連）

1. 把資料送進公司 Grafana 看得到的資料源：MQ 掛 Prometheus exporter；CIM 用日誌收集器做 Log 轉指標；AMHS 開唯讀 View 給
   Grafana 的 SQL 資料源。錯誤日誌進 ES，帶 `service.name`、`log.level`、`message`、`@timestamp`，以及有的話 `eqp_id`／`lot_id`／`carrier_id`。
2. 在 Grafana 建告警規則並貼標籤：`aiops_detect=true`、`aiops_severity=P0～P3`、`service=<系統名稱>`
   （要跟關係邊裡的名稱一樣）。
3. 規則的查詢必須是**一條序列**：
   - PromQL：聚合成一條（`sum(...)`／`max(...)`）。回多條序列會被拒絕。
   - SQL：`time` + **一個**數值欄，並且寫 `$__timeFilter(時間欄)`。多個數值欄、長格式（帶字串欄）、沒有時間條件都會被拒絕，
     錯誤訊息會寫明是哪條規則、要怎麼改；不會默默取第一欄。
   - 稀疏的失敗計數要給**整數**：PromQL 的 `increase()` 會外插出小數，包一層 `round()`，不然 sparse_count 不適用。
   - 一條規則有多個資料源查詢時只重放第一個（會列在該輪的 errors）。

平台不接收 webhook：對方的規則有沒有 firing 不影響平台，平台自己重放查詢、自己判。

## Grafana 要做的設定

**都在公司 Grafana 的網頁介面（或它的 API）上做；不改 Grafana 映像檔、不裝外掛、不動 Contact Point 與通知路由、不改規則的閾值。**
repo 裡的 `deploy/helm/aiops-grafana` 是舊平台自己的 Grafana，lite 不使用、也沒改它。

| # | 項目 | 在哪做 | 怎麼做 |
| --- | --- | --- | --- |
| 1 | 服務帳號與 Token | Administration → Users and access → Service accounts | 新增角色 **Editor** 的帳號 → Add token → 複製後放進 K8s Secret 的 `GRAFANA_TOKEN` |
| 2 | 告警規則標籤 | Alerting → Alert rules → 編輯規則 → Labels | 每條要納管的規則加 `aiops_detect=true`、`aiops_severity=P0～P3`、`service=<系統名>`（要跟關係邊的名稱一致） |
| 3 | 規則的查詢（不符格式才改） | 同一頁的 Query 區 | SQL 加 `$__timeFilter(時間欄)`、只留一個時間欄加一個數值欄；PromQL 聚合成一條；計數用 `round(increase(...))` |
| 4 | 儀表板 | Dashboards → New → Import | 貼上或上傳 `dashboards/aiops-overview.json`（只用內建註記清單面板，不需要資料源） |
| 5 | ES 資料源 uid（要日誌證據才需要） | Connections → Data sources → 點進該資料源 | 從網址 `/connections/datasources/edit/<uid>` 取得，填進 Helm values 的 `logs.datasourceUid` |

不需要新增資料源：規則本來就在用的 Prometheus、SQL、ES 資料源直接沿用。

### 注意

1. **標籤沒有 YAML 檔可改。** 公司的告警規則若是用 provisioning（YAML、Terraform）管的，在 UI 上改會被下次部署蓋掉，
   標籤要加進原本管規則的那份檔案。這是各系統團隊的事。
2. **RBAC 只在預設權限下驗過。** Editor 能讀規則、重放 `/api/ds/query`、走 ES 代理、寫註記，是在 Grafana 13.0.7 預設權限下確認的；
   公司 Grafana 若改窄過 Editor，要再確認這四件事。
3. **Token 只放 Secret，不進 chart 或 values。**
4. 註記寫入需要 Token 有建立與更新註記的權限；儀表板用標籤 `aiops` 查詢註記，不綁特定儀表板。

## 流程細節

- **基準線**：每條規則第一次看到時抓 14 天訓練窗（每段 5,000 點），之後 24 小時重訓一次；查詢改了就重訓。
  訓練窗結束在觀察窗之前，並扣掉這個系統過去的事故時間。訓練點數不到 60 不判（列在 errors）。
- **偵測**：三張統計票（hourly_baseline、residual_shift、residual_chronic）同一個時間點過半才算；sparse_count 不投票、越線就成立。
  new_only 讓每一輪重看最近一小時也不會重複計數。
- **收斂**：同 service、或兩個異常的 service 之間有關係邊（不分方向）就併成一張；中間的系統沒有異常不會串過去。
  新異常碰到進行中事故就併進去；連續 3 輪沒有新異常結案。抓不到資料的系統那一輪不算「沒有異常」。
- **RCA**：事故的成員規則或嚴重度有變時才問 LLM（只是多幾個異常點不重問）；LLM 失敗的進行中事故下一輪再試。
  LLM 回 JSON（summary／systems／ids），程式檢查點名的系統在事故或關係邊裡、機台批號在識別碼統計裡、
  摘要文字裡夾數字的識別碼在證據裡找得到；不通過就用 `[未經分析] 原始摘要`。
- **寫回**：先寫自己的資料庫，再寫註記；一個事故一筆註記（標籤 `aiops`、`aiops-incident:<uid>`、嚴重度、`open`／`closed`、
  各系統名稱），之後原地更新。註記寫失敗下一輪補，不會重複建。

## 設定

環境變數見 `src/main.py` 開頭。必填只有 `GRAFANA_URL`、`GRAFANA_TOKEN`；`DATABASE_URL` 正式環境用
`postgresql+psycopg://…`。關係邊：

```yaml
edges:
  - [mq-broker, cim-eap]   # [上游, 下游]
  - [cim-eap, amhs-mcs]
```

部署：先建 Secret（`GRAFANA_TOKEN`、`POSTGRES_PASSWORD`，有 LLM 再加 `LLM_API_KEY`），然後
`helm upgrade --install aiops-lite deploy/helm/aiops-lite --set grafana.url=https://…`。
儀表板：在 Grafana 匯入 `dashboards/aiops-overview.json`（只用內建的註記清單面板，不需要資料源）。

本機跑一輪：`GRAFANA_URL=… GRAFANA_TOKEN=… python -m src.main`（預設用 SQLite）。測試：`python -m pytest tests -q`。

## 驗證狀態（照實寫）

做過的：
- 73 個測試通過、1 個 `xfail(strict)`（死區）。Grafana 回應的解析與防呆全部用 **Grafana 13.0.7 真實回應**
  （`tests/fixtures/grafana13/`，Editor Token 錄的）。
- 對真的 Grafana 13.0.7 + Prometheus + PostgreSQL + ES 8.19.23 端到端跑過（2026-11-21，本機拋棄式環境，**資料是腳本灌的**：
  3 天歷史、最後 10 分鐘 MQ 積壓 + CIM 逾時 + AMHS 搬運變慢）：3 條規則（PromQL×2、SQL×1）各 4,26x 個訓練點，
  偵測到 24 個異常點，收斂成 1 張 P1 事故（三個系統經兩條關係邊串起來），註記建立後下一輪原地更新；
  ES 證據的機台統計是 EQP-CVD-07×3；平台自己的資料庫用 SQLite 與 PostgreSQL 16 各跑過。儀表板匯入後截圖確認三行與連結正常顯示。
- 合成序列的留出驗證：訓練 3 週、第 4 週（沒有拿來學）10,080 點，誤報 0～1 點（3 組亂數種子）。
- Helm chart 用 `helm lint` 與 `helm template` 檢查過。

沒做過的：
- **沒有任何真實產線資料**。偵測門檻與常數移植自封存的 aiops-platform，只在合成資料上回放過（未驗證泛化）。
- **LLM 沒有接過真的模型**：只對本機的 OpenAI 相容假端點驗過 HTTP 接線；摘要品質、真模型多常被幻覺檢查擋掉都不知道。
- chart 沒有部署到叢集、映像檔沒有實際 build 過（這個環境沒有 Docker 與叢集）。
- 只測過 Grafana 13.0.7；Editor 能讀規則、查詢、走 ES 代理、寫註記是在預設權限下確認的，公司 Grafana 若改過 RBAC 要再確認。

## 已知限制

- **送給 LLM 的日誌樣本沒有遮罩**。日誌裡的敏感資訊要在產生端去識別化，或把 `LLM_API_URL` 指到公司內部的模型。
- 幻覺檢查只擋「名稱不存在」，擋不了「存在的系統被講錯因果」；所以第 2 行固定標 `[LLM 推論]`。
- 沒有 Trace 時「集中在哪台機台／批號」只是錯誤日誌的計數，不是因果證據。
- 多數決的代價：持續但小幅（約 1σ）的偏移只有 residual_chronic 一票，不會成立（`tests/test_detectors.py` 有標註）；
  約 2σ 的偏移只會零星報出。
- sparse_count 看「最近 30 點累積幾筆」：暴增結束後、離開窗口之前（最多 30 分鐘）仍會有越線點，
  事故會晚結案、收斂筆數也會多算這一段。水位跳升恢復的那一個點也可能被算成一筆異常。
- 基準線上線未滿 2 週時 hourly_baseline 退回全段統計，日夜週期明顯的指標在這段期間偏鈍。
- 結案數的是「輪」不是時間：手動多跑幾次會讓事故提早結案。
- 關係邊只在兩端都異常時才用來收斂；上游沒有規則、沒有資料，平台看不到它，不會替它補洞。
- 自己的健康度只有 `rounds` 表與 CronJob 的成功／失敗；沒有對外的心跳告警。
