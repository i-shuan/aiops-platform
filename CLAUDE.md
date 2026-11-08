# CLAUDE.md - AIOps 架構守則與開發規範

## 0. 一句話

**Command 是固定流程，Plugin 是可抽換零件，Preset 決定這個場景用哪些零件。**
換場景（IT／工廠／純指標）不另寫一套流程，只換 preset；換演算法不改流程，只換外掛名字。

## 1. 分層與硬性禁令

| 層 | 位置 | 職責 | 不准做的事 |
|---|---|---|---|
| 入口 | `domains/<d>/api.py` | 驗證請求 -> 呼叫 Command | 任何邏輯、import 自己領域 commands/schemas 以外的東西 |
| 流程 | `domains/<d>/commands/` | 固定業務流程（validate -> run） | import 具體外掛、直接碰 models |
| 存取 | `domains/<d>/dao.py` | 讀寫資料表 | 業務判斷 |
| 資料表 | `models/` | SQLAlchemy ORM（`core.py`）、連線與 session（`db.py`）、DAO 共用存取（`crud.py`） | 業務判斷 |
| 調度 | `tasks/` | 排程與管線：依序呼叫各領域 Command（訓練 -> 偵測 -> 告警） | 碰 DAO、資料表、外掛、資料源、通知出口 |
| 零件 | `plugins/<kind>/` | 純運算：偵測、收斂、計分、報告段落、LLM | 認識任何領域、碰 DB、碰外部服務 |
| 資料源 | `datasource_specs/` | 連線、查詢、轉成 `DataFrame(ts, value)` | 審核狀態、業務合成（trace 合成、Lot 履歷） |
| 場景 | `presets/` | 每個場景啟用哪些外掛（只寫名字）與場景參數（嚴重度門檻） | import 外掛或領域 |
| 出口 | `notifiers/` | 把平台決定要發的通知送出去（Alertmanager v2、log） | 判斷要不要發、碰資料表、認識領域 |
| 基礎設施 | `infrastructure/` | 故障看守（本機日誌、webhook）、基礎設施錯誤 | 業務邏輯 |

以下全部由 `tests/test_architecture.py` 機械式檢查，**違規 = 測試紅燈**：

1. `plugins/` 不得 import `domains`、`models`、`infrastructure`、`datasource_specs`、`presets`、`initialization`、`notifiers`。
2. `datasource_specs/` 不得 import `domains`、`models`、`plugins`。
3. `presets/` 不得 import `domains`、`plugins`、`models`、`datasource_specs`。
4. **領域之間不得互相 import。** 跨領域資料只能走共用的 `models/` 資料表，且遵守下面「跨領域資料讀取規範」。
5. 領域內只有 `dao.py` 能 import `aiops.models`。
6. `commands/` 只能從 `aiops.plugins.base`、`aiops.plugins.registry` 取用外掛；具體外掛一律 `XxxRegistry.get(name)`。
7. 領域層不得直接 import 具體資料源（如 `datasource_specs.mock`），一律 `get_datasource_spec()`。
8. `api.py` 只能 import `fastapi`、`typing`、`pydantic`、同領域的 `commands` 與 `schemas`；不得有 if／三元運算／for／while／try／with／推導式。
9. 不得出現 `*_service.py` 或 `services/` 目錄。
10. 每個 preset 引用的外掛名字都必須已註冊。
11. 偵測器現行參數（`active_detector_configs`）只有 `domains/tuning/dao.py` 能寫。
12. `aiops/` 底下不得有名稱含 `grafana` 的檔案或目錄（見第 5 節）。
13. 每張資料表都要在 `models/core.py` 的 `TABLE_OWNERS` 登記擁有者領域（直接比對 SQLAlchemy 的 `Base.metadata`）。
14. DAO 類別引用了別的領域擁有的 ORM 類別（如 `AnomalyRow`），類別名稱必須以 `ReadDAO` 結尾。
15. 任何 `*ReadDAO` 不得呼叫寫入函式（`crud.create／create_many／update／upsert`、`write_session`、`Session.add／delete／merge／flush／commit／execute`、`setattr`），也不得有寫入類方法名。
16. `notifiers/` 不得 import `domains`、`models`、`plugins`、`presets`、`datasource_specs`。
17. 領域層不得直接 import 具體通知出口（如 `notifiers.alertmanager`），一律 `get_notifier()`，型別只能用 `notifiers.base`。
18. `aiops/` 的 Python 不得出現 Alertmanager 設定語彙（路由、receiver、通道設定都只在 helm chart）。
19. `aiops.models` 只准 `models/` 自己、各領域 `dao.py`、`initialization/` 匯入。
20. `tasks/` 只能 import 各領域的 `commands` 與 `schemas`（加上 `aiops.schemas`、`initialization`、`infrastructure`），不得碰 DAO、`models`、`plugins`、`datasource_specs`、`notifiers`、`presets`。

### 跨領域資料讀取規範

- 領域**嚴禁寫入**其他領域擁有的資料表。寫入只有擁有者領域的 DAO 能做（擁有者見 `TABLE_OWNERS`）。
- 跨領域讀取必須在**自己領域的 `dao.py`** 獨立宣告 `*ReadDAO`（例：`rca/dao.py` 的 `IncidentReadDAO`、`detection/dao.py` 的 `DetectorConfigReadDAO`），內容只能是查詢。
- 這樣表結構要改或要拆庫時，全域搜尋 `ReadDAO` 就能找到所有跨領域的消費端。
- 檢查方式：在每個 `dao.py` 的類別裡，找出引用了哪些 ORM 類別（`XxxRow`）、呼叫了哪些函式。ORM 類別對資料表的對應直接取自 SQLAlchemy registry，新增資料表不用改測試。
- DAO 一律用 `models/crud.py` 存取，回傳 dict；Command 不會拿到 ORM 物件，也不開 session。寫入用 `write_session`（會 commit），讀取用 `read_session`（一律 rollback）。

### 持久化

- 連線：`AIOPS_DATABASE_URL`，預設 `sqlite:///./aiops-local.db`；正式用 PostgreSQL，例如 `postgresql+psycopg://user:pass@host:5432/aiops`（要另裝 `psycopg`，目前還沒加進 requirements）。
- 時間一律存成 UTC、不帶時區（`crud` 寫入時自動轉換）。讀出來的 naive datetime 就是 UTC。
- 資料表結構目前由啟動時的 `create_all()` 建立，**只會新增表、不會改既有欄位**。上正式環境前要改用 Alembic migration。
- 資料庫連不上會轉成 `InfrastructureError`（API 回 503 並走看守機制）。
- 一個 Command 裡的多次 DAO 呼叫**不在同一個交易**（每次 DAO 呼叫各自 commit）。目前接受；需要原子性時再引入 unit of work。

另外，`tests/test_plugin_contracts.py` 會自動對**每一個**已註冊外掛跑契約測試，新增外掛不必另寫契約測試。

## 2. 動作 vs 零件：不要把動作當外掛

| 是 Command（固定流程） | 是 Plugin（可抽換零件） |
|---|---|
| 收資料、訓練、偵測、調參、回測、核准 | zscore、shift、peer、rules、chronic…（偵測演算法） |
| trace 合成、Lot 履歷合成、外部異常匯入、欄位對照審核 | same_entity、topology_chain、lot_path_chain…（收斂 Grouper） |
| 告警收斂管線、產生 RCA 報告 | golden_signals、p0_boost…（計分 Scorer） |
| 建立靜音／維護窗口／排除時段、產生通知 payload、推送 | silence_window、same_root_cause、dedup…（抑制 Suppressor） |
| | summary、timeline、golden_signals…（報告段落 Section Builder） |

- **偵測器契約只有兩個方法：`fit(df) -> self`、`detect(df) -> AnomalyResult`。** 「train」「tune」「collect」不是外掛名字，是領域 Command。
- **trace 不另開一套 train／detect。** `ingestion` 的 `MergeTracesCommand` 把 span 轉成標準時序，之後跟一般指標走同一個偵測器。

### 各類外掛的單一契約（定義在 `plugins/base.py`）

所有外掛都是純運算：輸入進來、結果出去，不連 DB、不改傳入的物件、不認識領域。需要的場景資料（拓撲、P0 清單等）只能從 `context` 取得，由 Command 傳入。

| 外掛 | 契約 | 備註 |
|---|---|---|
| 偵測器 `BaseDetector` | `fit(df) -> self`；`detect(df) -> AnomalyResult`；`get_state() -> dict`；`load_state(state) -> self` | 先 fit（或 load_state）才能 detect；state 必須能 JSON 序列化，載回後偵測結果要一模一樣（契約測試檢查） |
| 收斂 `BaseGrouper` | `group(incidents, context) -> list[Incident]` | 不得丟掉任何事件；合併用 `merge_incidents()` |
| 計分 `BaseScorer` | `score(incident, context) -> float` | 回傳新分數，不得修改 incident |
| 抑制 `BaseSuppressor` | `suppress(incident, context) -> str \| None` | 回傳抑制原因；`None` 表示照發。靜音、近期通知、同批事件群由 Command 放進 `context` |
| 報告段落 `BaseSectionBuilder` | `applies(incident, context) -> bool`；`build(incident, context) -> ReportSection` | 資料不足時 `applies` 回 `False`，不輸出空殼 |
| LLM `BaseLLMProvider` | `complete(prompt) -> str` | |

**不要在外掛之外的地方偷渡這些邏輯**：收斂規則、計分規則、報告段落內容都不准寫進 Command 或 `api.py`，Command 只負責依 preset 依序呼叫外掛。

## 3. 檔案歸屬決策樹

- **新增 API 端點** → `domains/<domain>/api.py`；請求／回應規格放 `schemas.py`。跨領域共用的純資料型別（`MetricTarget`、`TimeRange`）放 `aiops/schemas.py`。
- **新增領域** → 建 `domains/<domain>/`，放 `api.py`（定義 `router`）。`init_routers` 會自動掛載，**不要**手動註冊。
- **業務流程** → 繼承 `BaseCommand`，放 `domains/<domain>/commands/<action>.py`。業務錯誤丟 `CommandError`／`CommandNotFound`（404）／`CommandConflict`（409）。
- **資料進來之後的合成、匯入、欄位對照審核** → `domains/ingestion/commands/`。
- **新監控資料源** → 繼承 `BaseDataSourceSpec`，放 `datasource_specs/<name>.py`，在 `datasource_specs/__init__.py` 的 `_SPECS` 加一行。欄位對照只讀「已核准」版本，由 Command 傳入，Spec 自己不查審核狀態。
- **新偵測演算法** → 繼承 `BaseDetector`，放 `plugins/detectors/<algo>.py`，加 `@DetectorRegistry.register`。
- **新收斂規則** → 繼承 `BaseGrouper`，放 `plugins/groupers/`，加 `@GrouperRegistry.register`。合併一律用 `merge_incidents()`。
- **新計分規則** → 繼承 `BaseScorer`，放 `plugins/scorers/`，加 `@ScorerRegistry.register`。回傳新分數，不得修改 incident。
- **新抑制規則** → 繼承 `BaseSuppressor`，放 `plugins/suppressors/`，加 `@SuppressorRegistry.register`，再加進 preset 的 `suppressors`（依序比對，第一個回傳原因的就抑制，所以 `silence_window` 排第一）。
- **靜音、維護窗口、人工排除時段** → 資料存在 alerts 的 `silences` 表，用 `POST /api/v1/alerts/silences` 建立。只有這一份，接收端不另設靜音。
- **通知內容（嚴重度、標題、路由標籤）** → `domains/alerts/commands/payload.py`。嚴重度門檻是 preset 的 `severity_thresholds`。一個部門一則通知，部門取自事件 `attrs["dept"]`，沒有就是 `unassigned`。
- **新通知出口（例如另一種接收端）** → 繼承 `BaseNotifier`，放 `notifiers/<name>.py`，在 `notifiers/__init__.py` 的 `get_notifier()` 加分支。只負責傳送，送不出去丟 `InfrastructureError`。
- **新 RCA 報告段落** → 繼承 `BaseSectionBuilder`，放 `plugins/rca_sections/`，加 `@SectionRegistry.register`。資料不足時讓 `applies()` 回 `False`，不要輸出空殼。
- **新場景或調整場景用的零件** → 改 `presets/__init__.py`，只寫外掛名字與場景參數。
- **任何改偵測參數的需求（自動調優、LLM 從回饋抽規則、人工調整）** → 一律 `CreateProposalCommand`（`source` 填 `auto`／`llm`／`manual`），再走 `BacktestProposalCommand` -> `ApproveTuningCommand`。**不准另開路徑直接寫參數。**
- **資料表** → `models/core.py`，並在 `TABLE_OWNERS` 登記擁有者；存取寫在各領域 `dao.py`。別的領域要讀就寫 `*ReadDAO`。
- **DB／ES 連不上** → DAO／Spec 丟 `InfrastructureError`。`initialization` 會統一回 503，並呼叫 `infrastructure.fallback.report_failure`（寫本機日誌，設了 `AIOPS_FALLBACK_WEBHOOK` 就直接打 webhook）。不要在業務 Command 或外掛裡自己做備援。
- **排程／背景任務** → `aiops/tasks/`，任務內只呼叫 Command。工作設定是 YAML（範例 `configs/jobs.example.yaml`），一個 job = 一條管線。
- **偵測結果要進告警** → 不准 detection 呼叫 alerts。`RunDetectionCommand` 帶 `pipeline` 就把新異常寫進 `anomalies`；`ProcessPendingAnomaliesCommand` 用 `AnomalyReadDAO` 讀、處理完把游標記在 alerts 自己的 `alert_watermarks`；兩者由 `tasks/pipeline.py` 串起來。
- **批次訓練、保存基準線** → `TrainDetectorCommand` 把 `get_state()` 存進 `detector_models`；`RunDetectionCommand` 帶 `pipeline` 時優先 `load_state()`。

### 偵測到告警的資料流

```
python -m aiops.tasks run-once -c jobs.yaml        （或 loop --interval 60）
  每個 job、每個 target：
    TrainDetectorCommand  模型超過 retrain_minutes 才重訓 -> detector_models
    RunDetectionCommand   載入模型偵測 -> 只寫比上次新的異常 -> anomalies（標上 pipeline）
  ProcessPendingAnomaliesCommand
    AnomalyReadDAO 讀游標之後的異常 -> 收斂、計分、抑制、推送 -> 推進 alert_watermarks
```

- 同一段時間重跑不會重複寫異常、不會重複處理（已有測試）。
- 推送失敗時游標不前進，下一輪會重處理同一批；但已存檔的事件群會多一筆（目前接受）。
- `loop` 只適合單一副本；多副本要改用 K8s CronJob 跑 `run-once`，或加分散式鎖。
- **utils/** → 只放無業務語意的純函式；含業務判斷的回到 Command。

## 4. 調參流程（防止多頭馬車）

```
自動 tune ─┐
LLM 回饋  ─┼─> 提案(pending) ─> 回測(backtested) ─> 核准(approved) ─> 寫入現行參數(version+1)
人工調整  ─┘                                      └─> 現行版本已變 -> stale（作廢，請重提）
```

提案建立時記下 `base_version`。核准時若現行參數版本已不同，代表期間有別的提案被核准（例如人工剛調好），舊提案自動作廢。這樣自動 tune 不會蓋掉人工設定，也不會有沒回測就套用的參數。

## 5. Grafana 與通知整合守則

**判斷與存放都在平台；Alertmanager 只做路由；Grafana 只做顯示（儀表板、告警清單）。兩者都不放業務邏輯。**

- **要改 Grafana 或 Alertmanager，只能改 helm chart**（Grafana 的 datasource、dashboards；Alertmanager 的路由與 receiver）。不改 Grafana 本體、不自製外掛、不在 Grafana 裡寫查詢以外的邏輯。
- **Alertmanager 的行為不准靠改程式碼調整。** 換通道、加部門、改分組時間、改重複提醒間隔，全部只改 `deploy/helm/aiops-alertmanager/values.yaml`（或部署時的 `-f` values），不碰 `aiops/` 底下的 Python。平台程式只能做兩件事：產生帶標籤的通知、用 `notifiers` 推送。
  - `aiops/` 裡不得出現 Alertmanager 的設定語彙（`receivers`、`inhibit_rules`、`slack_configs`、`webhook_configs`、`pagerduty_configs`、`matchers` 等）。路由與通道是 chart 的事。
  - 想讓接收端多一個可以路由的條件，是**平台多帶一個 label**（`payload.py`），再由 chart 的 `deptRoutes` 或路由規則去比對；不是在程式裡寫「這個部門送去哪」。
  - chart 本身的 templates 只放固定結構，不放業務判斷；部門與通道一律由 values 決定。
- **平台側不為 Grafana 開特例。** 不得有 `grafana` 領域、`*grafana*` 命名的模組或目錄、Grafana 專用外掛。Grafana 要讀的資料，就是各領域 `api.py` 的一般端點（例如 alerts 的事件群、rca 的報告），回傳固定的 JSON 形狀。
- **端點對 Grafana 唯讀。** Grafana 需要的是讀取與接收通知，不得為它開寫入端點。寫入一律走既有的 Command 流程（例如調參只能走提案 -> 回測 -> 核准）。
- **契約穩定優先。** Grafana 的面板與規則是照端點 JSON 欄位寫的。改 `schemas.py` 的回應欄位前，先確認 helm chart 裡引用該欄位的 provisioning 檔，要改就一起改，不要只改一邊。
- **通知的分工：平台決定「要不要發、發什麼」，Alertmanager 只決定「送去哪個通道」，Grafana 只顯示。**
  - 平台（`domains/alerts`）負責所有業務級判斷：何時算異常、怎麼收斂、分數與 P0 加權、同根因抑制、靜音與維護窗口、人工調整排除的時段，以及產出通知內容（payload）。
  - Alertmanager（`deploy/helm/aiops-alertmanager`）只做純路由：依 `dept` label 送到哪個 Slack channel、Teams、PagerDuty。
  - **不得在 Grafana 建立 alert rule、Contact Point 或 Notification Policy 來發這些通知，也不得在 Alertmanager 加 inhibit 或 silence。** 否則會出現「平台已因排除時段或抑制而不發，下游卻照樣呼叫」的不一致。靜音、夜間、排除時段的設定只有一份，在平台。
- **舊 repo `aiops-data-foundation/grafana/` 不動。** 新的 helm chart 在本 repo 的 `deploy/helm/` 下；舊檔只當參考。

### 已定案：平台主動推到獨立的 Alertmanager，Grafana 只顯示

```
POST /api/v1/alerts/process
  收斂 -> 計分 -> 抑制（silence_window -> same_root_cause -> dedup）-> 產生通知（一個部門一則）
  -> notifiers.get_notifier().send() -> POST {AIOPS_ALERTMANAGER_URL}/api/v2/alerts
  -> aiops-alertmanager（helm chart）只依 dept label 分流到 receiver（Slack／Teams／PagerDuty）
  -> Grafana 把 aiops-alertmanager 加成資料源，只「顯示」告警
```

- **不推到 Grafana 內建的 Alertmanager。** 實測 Grafana 13 要 `alert.instances.external:write` 權限才收得進外部告警；改用獨立的 Alertmanager，Grafana 就不需要任何寫入權限。
- **Chart 位置：`deploy/helm/aiops-alertmanager/`。** 要改路由、receiver、資源，只改它的 values。
  - 部門路由寫在 `deptRoutes`（`dept` -> `receiver`）；沒對到的進 `route.defaultReceiver`（預設 `unrouted`，不送任何地方）。
  - `receivers` 原樣放進 alertmanager.yml，並存在 Secret（webhook URL 是機密）。路由引用了沒定義的 receiver，render 會直接失敗。
  - **不得加 `inhibit_rules`、不得在 Alertmanager 設 silence。** 抑制與靜音只有平台一份（`tests/test_helm_alertmanager.py` 檢查）。
  - receiver 一律 `send_resolved: false`；`resolveTimeout` 預設 24h。原因是平台還不送 `endsAt`，見下方待辦。
- **Grafana 側只動 helm chart：** 把 aiops-alertmanager 加成 `alertmanager` 型資料源（`implementation: prometheus`、`handleGrafanaManagedAlerts: false`）。
  - **目前 `grafana-new` 的 Grafana 沒有 sidecar**，資料源只來自它自己 chart 的 `files/datasource.yaml`，所以本 chart 的 `displayDatasource`（帶 sidecar label 的 ConfigMap）在這台**不會生效**，預設保持關閉。
  - 已在舊 repo `aiops-data-foundation` 的 branch `feature/grafana-alertmanager-datasource` 加了 `aiops-alertmanager` 資料源（網址取自環境變數 `AIOPS_ALERTMANAGER_URL`，放在 Grafana 的 Secret `secretData`）。這是對「舊 repo 不動」的**一次例外**，經使用者同意，只限這一個資料源；其餘舊 repo 內容仍不動。
  - 部署：`helm upgrade grafana <舊 repo>/grafana/helmchart -n grafana-new --reuse-values --set secretData.AIOPS_ALERTMANAGER_URL=http://aiops-alertmanager.aiops-platform.svc:9093`。
- 平台設定：`AIOPS_NOTIFIER=alertmanager`、`AIOPS_ALERTMANAGER_URL=http://aiops-alertmanager.<namespace>.svc:9093`。

- 通知是 Alertmanager v2 格式：`labels`（`alertname=aiops_incident`、`incident_id`、`severity`、`preset`、`dept`、`entities`）、`annotations`（`summary`、`description`）、`startsAt`。
- **接收端的路由只准用 labels 比對**（例如 `dept`、`severity`），不得依 `annotations` 內容判斷。
- 設定：`AIOPS_NOTIFIER=alertmanager`、`AIOPS_ALERTMANAGER_URL`、選填 `AIOPS_ALERTMANAGER_TOKEN`（Bearer）。預設是 `log`，只寫日誌、不真的送。
- 推送失敗會丟 `InfrastructureError`：API 回 503，並走看守機制（本機日誌、`AIOPS_FALLBACK_WEBHOOK`）。

> 目前狀態（2026-11-08）：**平台 -> aiops-alertmanager 已在本機 rancher-desktop 端對端驗證成功**：三個部門各推一則，依 `dept` 分到正確的 receiver，沒帶部門的進 `unrouted`；同一批再跑一次全部被 `dedup` 擋下。
> 還沒做的：
> - **Grafana 顯示已接上並驗證**：Grafana 13 經資料源讀到 Alertmanager 的告警，標籤（`dept`、`severity`）與 receiver 都正確。Grafana 內建的資料源健康檢查端點對 alertmanager 型回 500（`plugin unavailable`），這是該資料源型別本來就不支援，不是連線失敗，實際讀取以 `/api/datasources/proxy/uid/aiops-alertmanager/api/v2/alerts` 為準。
> - **舊 Grafana 仍掛著自己的 alert rules／contact points／policies**（`grafana-alerting`），與「Grafana 只顯示、不發通知」相反，兩邊同時開可能重複通知。尚未處理，需要決定是否關掉。
> - **真的通道還沒設**：驗證時 receiver 都沒有 `*_configs`，收下但不送出。
> - **恢復通知**：平台只送 `startsAt`、不送 `endsAt`。之後由平台補送 `endsAt`，才能開恢復通知、把 `resolveTimeout` 改回合理值。
> - **平台本身的 helm chart** 還沒建（平台目前不在叢集裡跑）。

## 6. 標準樣板

### Command

```python
from aiops.commands.base import BaseCommand, CommandError
from aiops.datasource_specs import get_datasource_spec
from aiops.plugins.registry import DetectorRegistry


class ExampleActionCommand(BaseCommand):
    def __init__(self, detector_name: str, params: dict):
        self.detector_name = detector_name
        self.params = params

    def validate(self) -> None:
        if not DetectorRegistry.has(self.detector_name):
            raise CommandError(f"未知的檢測外掛: {self.detector_name}")

    def run(self) -> dict:
        self.validate()
        # 1. 抓資料：get_datasource_spec(...).fetch_data(...)
        # 2. 算結果：DetectorRegistry.get(...)(params).fit(train_df).detect(df)
        # 3. 寫 DB：透過本領域 dao.py
        return {"status": "success"}
```

### 偵測器外掛

```python
import pandas as pd

from aiops.plugins.base import AnomalyResult, BaseDetector
from aiops.plugins.registry import DetectorRegistry


@DetectorRegistry.register
class MyDetector(BaseDetector):
    name = "my_detector"

    def fit(self, df: pd.DataFrame) -> "MyDetector":
        # 從訓練資料學狀態，存在 self 上
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        # 輸入保證有 ts、value 兩欄；輸出必須是 AnomalyResult，長度與 df 相同
        ...
```

## 7. 送出修改前的自檢

```bash
.venv/Scripts/python -m pytest tests/ -q
```

必須全綠。新增架構規則時，先在 `tests/test_architecture.py` 補測試，再改本文件。
