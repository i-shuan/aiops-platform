# CLAUDE.md - AIOps Pipeline & Plugin 架構開發規範

## 0. 核心設計哲學 (Core Philosophy)
本平台採用 **「Pipeline 管線調度 + 可插拔算子 (Operator / Plugin)」** 架構：
- **Pipeline 是固定骨架**：資料由記憶體內的 `PipelineContext` 順序流動（Pull -> Detect -> Group/Suppress -> RCA -> Notify -> Persist）。
- **Operator 是可抽換零件**：演算法（MAD、Z-Score、Isolation Forest）與 LLM 供應商均為純算子，透過 `@OperatorRegistry.register` 登記，由 YAML 設定檔動態選用。
- **資料庫只記最終結果**：節點之間嚴禁透過資料庫傳遞資料（徹底廢除 `*ReadDAO` 模式），DB 只負責寫入最終生成的 Incident、Audit Log 與模型持久化狀態。

---

## 1. 架構分層與不可違背的禁令 (Architecture Invariants)

| 層次 | 所在位置 | 職責 | 嚴格禁止事項 |
| :--- | :--- | :--- | :--- |
| **資料流載體** | `engine/context.py` | 貫穿整條 Pipeline 的強型別 `PipelineContext` | 嚴禁在節點間傳遞鬆散無型別的 `dict` |
| **管線引擎** | `engine/pipeline.py` | 讀取工作定義，按序實例化算子並調用 `.execute(ctx)` | 嚴禁在引擎內 hardcode 具體算子邏輯 |
| **算子註冊** | `engine/registry.py` | 算子註冊中心 (`OperatorRegistry`)，負責動態發現與驗證 | — |
| **管線算子** | `stages/*` | 依執行順序分段：ingestion → detectors → ensemble → grouping → scoring → suppression → rca → enrichment（LLM）→ notification | 嚴禁 import 具體資料表 ORM 與 `storage`；嚴禁碰外網（`enrichment/`、`notification/notifiers.py`、`notification/transport.py` 除外；只有 `ingestion/fetch.py` 能用 `sources`） |
| **控制面** | `control_plane/*` | 手動動作（`admin.*`）：`learning/`（調參、回饋、成效報表、規則升級）、`operations/`（靜音、欄位對照、事件群、匯入、資料源檢查） | 唯一可以 import `storage` 的算子（目前都只透過 `storage/repository.py`）；也可以連外 |
| **外部源適配** | `sources/*`（查詢語法解析在 `sources/query/`） | 外部資料庫語法轉換與連線（Prometheus, ES, OTel），輸出 DataFrame | 嚴禁 import `models`、`stages`、`control_plane` |
| **持久化層** | `storage/*` | 資料庫模型定義（SQLAlchemy）與 Repository | 嚴禁業務判斷邏輯 |
| **管線 YAML** | `pipelines/*` | `detection/`（偵測告警）、`rca/`（RCA 診斷）、`control/`（`admin_*` 手動動作）；以名稱載入，名稱全域唯一 | 嚴禁在 YAML 以外的地方寫死流程 |
| **排程入口** | `interfaces/cli/*` | 呼叫 Pipeline 執行（支援 K8s CronJob 或定時 Loop） | 嚴禁直接在排程中操作底層運算 |
| **API 接口** | `interfaces/api/*`（`interfaces/app.py` 建立 app，`interfaces/bootstrap/` 掛驗證與錯誤處理，`interfaces/runner.py` 是組裝點） | 外部查詢與手動觸發，掛載 Token 驗證 | 零業務邏輯（嚴禁 `if / for / try`），純調度 Pipeline 或查詢 Storage |

### 🚫 架構自動化防禦（違規將導致 `test_architecture.py` 報錯）
1. **嚴禁內部跨節點偷渡**：任何算子內部嚴禁出現 `ReadDAO` 去翻其他模組的表。所有上游資訊必須由 `ctx` 取得。
2. **算子獨立性**：`stages/detectors/` 各演算法之間嚴禁互相 import。
3. **LLM 的位置（告警路徑零依賴、RCA 路徑必要）**：告警路徑（偵測 → 收斂 → 計分 → 抑制 → 通知，`pipelines/detection/`）**嚴禁依賴 LLM**——那些 YAML 不准掛 `enrichment.*` 節點，`stages/` 除了 `enrichment/` 本身都不准 import `aiops.stages.enrichment`。RCA 路徑的 LLM 是**必要元件**（`enrichment.extract_evidence` 抽證據、`enrichment.rca_narrator` 寫摘要，沒有 `noop`）：沒設定、連不上、逾時、格式不合、驗證全數被拒、或根本沒有事實與證據可判（`no_evidence`）時報告照出，`ctx.rca_status = "undetermined"` 並在 `rca_undetermined_reason` 寫代碼與原因，不整份失敗、不退回舊規則硬猜、不無限重試。舊引擎的 `enrichment.llm_summary`／`llm_briefing` 保留 `provider: noop`（預設），完全不連外、直接用模板。
4. **模型序列化安全**：整個 `aiops/` 嚴禁 import `pickle`／`joblib`／`dill`／`cloudpickle`／`shelve`／`marshal`，也不准出現 `allow_pickle=True`。非 JSON 的模型本體只能用 npz（`pack_arrays`／`unpack_arrays`）。
5. **RCA 不寫死**：報告段落是 `rca.section.*`、根因演算法是 `rca.root_cause.*`，都走 `OperatorRegistry`；`stages/rca/report.py` 不得 import 具體段落或演算法。
6. **關係圖只有一個查詢入口**：實體之間的關係（同步呼叫、訊息佇列、Lot 流向）一律問 `ctx.graph`（`engine/graph.py` 的 `ServiceGraph`）。`stages/`、`control_plane/` 不准讀 `ctx.topology`／`ctx.lot_edges`／`ctx.async_edges` 自己拼圖，也不准直接用 `propagation_edges`；那三個欄位只是載入與存檔的格式（寫入不擋：`rca.build_dynamic_topology`）。每條邊帶語意型別（`EdgeKind`），「異常往哪個方向傳」寫在 `KINDS` 一張表，新增邊的型別就是加一格，不准在算子裡自己翻方向。圖可以有環，不要假設它是 DAG。

### 1.1 權責邊界與架構隔離（平台管到哪裡為止）

**平台內部：單向依賴**

1. **引擎不認識控制面**：`engine/`、`stages/`、`sources/`、`storage/`（資料收容、偵測、`ServiceGraph` 收斂、RCA）是通用底層，不准 import `aiops.control_plane`。只有組裝點 `interfaces/` 同時認識兩邊。（`control_plane/` 在 `aiops/` 套件裡面，所以規則是「這四層不准反向依賴它」，不是「`aiops/` 不准」。）
2. **平台不依賴周邊**：整個 `aiops/` 不准 import repo 裡自己以外的東西（`scripts/`、`tests/`、`deploy/`）；不准用相對 import。
3. **控制面只能透過「提案 → 回測 → 核准 → 寫入資料庫」影響引擎**：調參、欄位對照、靜音、宣告的圖，都是寫進資料庫、下一輪由 `StorageHooks` 載進新的 `PipelineContext`。不准直接改執行中管線的記憶體狀態（模組層級、類別層級的可變狀態本來就不准，§100.3）。致命界線（`detector.hard_limit`，`accepts_tuning = False`）只認 YAML，控制面的提案一律拒絕。

**平台與外部團隊：防腐層**

4. **資料源缺口不在引擎裡補**：外部系統負責照標準格式（OTel、Prometheus、標準日誌欄位、宣告的圖）把資料送進來。沒埋點、沒送日誌、時間戳錯、CMDB 填錯，是資料源端的缺口——平台的做法是**照實說看不到**（序列錯誤、`rca_skipped`、`undetermined`、文件裡的「已知限制」），不准在引擎裡寫業務特判去猜或補洞（§100.5）。格式差異只能在 `sources/` 與欄位對照（`admin` 的欄位對照）吸收，不准漏進 `stages/`。
5. **遮罩的責任分界**：送往 LLM 的內容只有一個出口 `stages/enrichment/masking.send`，先遮罩再送，遮罩出錯就不送（`MaskError`）。它擋的是格式可辨識的敏感資訊（帳號、信箱、IP、憑證這類）；自由文字裡的業務機密沒有格式可認，由資料產生端在寫日誌之前負責去識別化（死區見 `masking.py` 與 `tests/test_llm_masking.py`）。**送往 Alertmanager 的通知內容不經過這個遮罩**，通知裡只放結構化欄位，不放日誌原文。
6. **輸出即終點**：平台的產出是收斂後的事故卡、標準通知 payload（交給 Alertmanager 派發）、RCA 證據報告（證據不足就是 `undetermined`）。派工、聯絡、重啟、退版、改設定是外部維運系統與業務團隊的事。`aiops/` 不准起子行程、不准帶叢集或遠端執行的函式庫、不准呼叫任何修復腳本；RCA 報告的「建議動作」只是文字。

自動檢查（`tests/test_architecture.py`）：`test_pipeline_layers_never_import_the_control_plane`、`test_platform_imports_nothing_from_outside_its_own_package`、`test_platform_never_executes_commands_or_remediation`，加上既有的 `test_algorithms_do_not_special_case_entities_or_lengths`、LLM 出口與 `hard_limit` 調參的測試。第 4 點「有沒有替外部補洞」與第 5 點「通知裡有沒有原文」機器只能擋一部分，靠 code review。

---

## 2. 算子合約規範 (Operator Protocols)

所有可插拔算子必須繼承 `BaseOperator` 並實作標準 `execute` 介面：

```python
from abc import ABC, abstractmethod
from aiops.engine.context import PipelineContext

class BaseOperator(ABC):
    @abstractmethod
    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """接收 Context，計算後原地或賦值修改 Context，回傳 Context"""
        pass
```

各類算子專屬契約：
 * **Detector（異常偵測）**：
   * 必須支援 `fit(df) -> self`（批次訓練）與 `detect(df) -> AnomalyResult`（即時推論）。
   * 必須支援 `get_state() -> dict` 與 `load_state(state: dict)` 以供模型基準線持久化。
   * 偵測用到滾動窗時覆寫 `warmup_points() -> int`（需要前面幾個點才算得出分數），回測切測試段時會多帶這些點讓窗口湊滿（不計分）。
   * 基準線與異常血緣以 **Workflow 節點名稱**（`self.state_key`）為準，同一個演算法可以在同一條管線掛多次。後台核准的調參目前只分 (演算法, 管線, 實體)、沒有節點這一層：`ctx.detector_params` 雖然以節點名稱為 key，同一條管線裡同一個演算法的多個節點拿到的是同一份調參（要各自不同就寫在節點 YAML `params` 或 target `overrides`）。
   * 需要「往前 N 分鐘的自己」（同比、環比）時覆寫 `required_lags() -> list[int]`；fetch 要在 YAML 設對應的 `lags_minutes`，df 會多 `lag_<分鐘>` 欄位，回測也照這個抓。
   * 模型有非 JSON 的本體時覆寫 `get_artifact() -> (格式, bytes)` 與 `load_artifact(格式, bytes)`，格式只能是 `npz`。
   * 繼承 `DetectorOperator` 的算子自動 `has_baseline = True`，引擎才會替它載入基準線與調參；`detector.new_only`、`detector.ensemble` 這類不學習的算子不繼承它。
   * `AnomalyEvent` 是 frozen：建立後不准改分數或偵測器名稱。要合併多個偵測器的判斷，用 `detector.ensemble` 產生新的裁決事件。
   * **每條 target 跑哪些偵測節點由引擎決定**（`engine/pipeline.py`），順序：節點 `params.exclude_entities` → `params.include_entities` → target 的 `detectors`（節點名稱或算子名稱，不寫 = 全部）。被跳過的節點不載基準線與調參、不進 `ctx.detector_runs`。引擎把真的會跑、會存基準線的節點名稱放進 `ctx.baseline_nodes`，`ingestion.fetch` 只在其中有節點還沒有基準線或到期時才抓訓練窗（全被跳過的序列不抓）。演算法裡不准自己判斷「這個實體要不要跑」。
   * `include_entities`／`exclude_entities` 是基底層的路由參數（`ROUTING_PARAMS`）：不進 `self.params`、不傳給演算法、調參提案帶它們會被拒絕。
   * 參數優先序：演算法預設值 ← 後台核准調參（`ctx.detector_params`）← 節點 YAML `params` ← target `overrides[節點名稱]`（`effective_params`，逐鍵 `deep_merge`）。overrides 在載入期用 `check_override` 檢查參數名與範圍。合併邏輯只有一份：`stages/detectors/base.layer_params`，偵測與回測（`admin.proposal_backtest`）都用它——回測會讀提案所屬管線的 YAML 疊上節點 `params` 與該實體 target 的 `overrides`，YAML 鎖住的鍵提案改了也不會生效，結果的 `workflow.locked` 會列出來。
   * `ctx.detector_runs` 只記「真的評估過資料」的節點（沒資料就 return 的不算）。`detector.ensemble` 的分母 = 參與者數；棄權不是反對票。`hourly_baseline` 資料不足時退回全段統計，仍算參與者——新偵測器若「這一輪算不出分數就不判」，覆寫 `can_evaluate(df) -> bool` 回 False（基底在加入 `detector_runs` 之前棄權；`rules` 沒規則、`quantile_drift`／`chronic` 滾動窗比資料長都這樣做），不要在 `detect` 裡回全 0。`requires_peers` 的算子在 `raw_df` 沒有 `peer_*` 欄位時（`peer_group` 只撈到自己）由基底棄權，`ingestion.fetch` 把它記成那條序列的錯誤。
   * 冷啟動門檻數的是 `training_points(train) -> int`（預設列數；`dod`／`wow` 只算 lag 欄位有值的列、`peer` 只算同儕有值的列），`DetectorState.points` 也記這個數。基準線到期重訓但訓練窗點數不足時，沿用資料庫裡的舊基準線照判（log `keep-old-baseline`），不留偵測盲區；完全沒有舊基準線才不判。
   * target 的 `detectors`／`overrides` 寫錯、節點的 include／exclude 寫了 workflow targets 裡沒有的實體，都在 `Pipeline.__init__`（API 傳入的 targets 在 `run()` 開頭）丟 `WorkflowError`，不准延到執行期。
   * `exclude_incidents`（預設 true）、`warning_ratio`（預設關）、`min_train_points`（預設 60）是基底層的節點行為參數（`NODE_PARAMS`）：同樣不進 `self.params`、target `overrides` 不能寫。訓練點數不到 `min_train_points` 的節點不學、不判、不進 `detector_runs`（冷啟動時幾十個點學到的基準線不可信）；不需要歷史資料的算子（`detector.rules`）設 `requires_training = False` 不受此限，剛上線的服務靠它兜底。訓練排除窗由 `StorageHooks` 放進 `ctx.training_exclusions`，基底 `_training_frame` 統一套用——演算法的 `fit` 不准自己去查事故或維護窗。
   * 事件等級只有 `ANOMALY`／`WARNING`（`engine/types.py`）。warning 由基底依 `threshold × warning_ratio` 產生，演算法只管回傳分數與越線旗標。
   * **雙軌制**（`docs/DUAL_TRACK_DESIGN.md`）：統計軌是 `detector.ensemble` 多數決；致命軌是 `detector.hard_limit`（`votes = False`、`accepts_tuning = False`、`requires_training = False`），單點踩線就成立。不投票的偵測節點必須列進 ensemble 的 `bypass`（事件原樣留下、不進分母），由 `alert.fatal_floor` 把分數拉到 payload 的 P0 門檻並打上 `fatal:<節點>` 標籤。致命界線只認 YAML（節點 `params` 或 target `overrides`）：後台調參、規則升級器的提案一律拒絕。新增「不交給多數決」的偵測器照這個做，不准在 ensemble 或引擎裡寫死算子名稱。
   * 需要看整條 workflow 才能驗的接線（引用的節點存不存在、前後順序）寫在算子的 `check_workflow(nodes: list[NodeInfo])`，引擎在 `Pipeline.__init__` 逐一呼叫，`TypeError`／`ValueError` 轉成 `WorkflowError`；不准延到 `execute` 才發現。
 * **節點 scope**：`target`（逐序列）→ `batch`（所有序列合併後）→ `after_persist`（資料庫寫入成功後才跑：所有 `notifier.*`）→ `after_finalize`（「已通知」與結案也寫成功後才跑：只有 `notifier.watchdog` 心跳，這一輪每一步都成功才續命）。順序錯在 `Pipeline.__init__` 報錯。
   * 寫入順序鐵律：異常點、事件群在 `hooks.persist_batch` 的**單一交易**落地（資料庫短暫斷線整筆回復後重試，`storage/db.run_in_transaction`），然後才送通知；通知都送出去了，引擎才呼叫 `hooks.finalize_batch` 記「已通知」與結案。推送失敗時事件群留在進行中，下一輪 `alert.heartbeat` 用同一組標籤補送（含 `endsAt`）。任何算子都不准在寫入前送出外部通知。
   * 資料斷流：這一輪抓不到資料（資料源丟 `NoDataError`、或抓回來是空的）的序列比例達到 workflow 的 `max_failed_target_ratio`（預設 1.0 = 全部）就是 `InfrastructureError`，`after_persist`／`after_finalize` 不執行、心跳不續命。只有 `engine/errors.NoDataError`（資料源與 `ingestion.fetch` 丟）算斷流：其他節點丟的 `NotFound`、查詢寫錯（`BadRequest`）、資料源回 200 但格式不對（`InvalidQueryError`）都只記錯誤。
   * 序列隔離：`strict=False`（排程）時一條序列的**任何**例外（含演算法 bug 的 `ValueError`／`KeyError`）只記進 `errors`（訊息帶型別），其他序列照跑；`InfrastructureError` 例外，一律整輪失敗。`strict=True`（手動動作）照丟；抓回來是空的序列在 strict 下也丟 `NoDataError`（404），跟資料源說沒資料同一種錯，不走斷流（503）。
 * **RCA 根因演算法（`rca.root_cause.*`）**：
   * 繼承 `stages/rca/base.RootCauseOperator`，只寫 `rank(incident, ctx) -> list[RootCandidate]`（由可能到不可能排好；資料不足回空清單，段落會略過）。
   * 需要原始時序時，在它前面放 `ingestion.incident_series`（讀 `ctx.rca_series`），不准自己連資料源。
   * 證據不夠就回空清單，並把原因寫進 `ctx.rca_root_note`（「可能的源頭」段落會照出），不准為了有答案而硬湊候選。`rca_report` 預設的 `rca.root_cause.evidence` 讀 `ctx.rca_spans`；`ctx.rca_traces_failed`（trace 來源連不上）時不准退回拓撲硬猜。
   * span 統計（自身耗時、入口請求、流量來源）與日誌樣板／暴增排序是共用純函式，放 `stages/rca/spans.py`、`stages/rca/logs.py`，算子與段落都用同一份。
 * **RCA 報告段落（`rca.section.*`）**：
   * 繼承 `stages/rca/base.RcaSection`，寫 `applies()` 與 `build()`；資料不夠就讓 `applies()` 回 False（記進 `rca_skipped`），不准輸出空殼。
 * **Grouper（事件收斂）**：
   * 接收 `ctx.anomalies`，輸出收斂後的 `ctx.incidents`。
   * 進行中事件群（`ctx.open_incidents`）由 `alert.absorb_open` 吸收新異常：一場事故從初發到結案只有一個 `incident_uid`。`ensure_incidents` 只建一次（`ctx.incidents_built`），清單被吸收光之後不准重建。
   * 順序鐵律：收斂 → 計分（`alert.fatal_floor` 在這一段，必須在 `absorb_open` 之前）→ `absorb_open` → 抑制 → `notify_payload` → `heartbeat` → `notifier.*`。升級（嚴重度只升不降）改了 labels 時，`heartbeat` 負責對舊 labels 送 `endsAt` 再送新的；Alertmanager 用整組 labels 辨識告警。
 * **Suppressor（告警抑制）**：
   * 檢查靜音窗口（維護期）、冷卻時間（Dedup）、同根因；被抑制事件必須打上 `ctx.is_suppressed = True` 與具體原因。
 * **LLM Provider（智慧診斷）**：
   * 介面為 `BaseLLM.complete(system: str, prompt: str) -> str`（`stages/enrichment/base.py`），失敗丟 `LLMError`；供應商用 `stages/enrichment/registry.register` 登記 factory（見範例二）。
   * 當設定為 `provider: noop` 時，直接跳過推論並填入結構化模板，不發出任何外部網路請求。

---

## 3. 標準算子實作範例

兩個範例都是可以直接執行的真實程式碼；`tests/test_claude_md_examples.py` 會檢查範例一跟 `aiops/stages/detectors/mad.py` 逐字相同、範例二能在乾淨的行程裡跑起來，範例過期測試就會紅。

### 範例一：偵測器（`stages/detectors/mad.py`，原檔全文）

偵測器繼承 `DetectorOperator`，只寫演算法的四個方法；`execute`（載入或訓練基準線、套用調參與 overrides、產生 `AnomalyEvent`、warning 兩級、訓練排除事故窗）全部由基底處理，演算法不要自己寫 `execute`。

```python
import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import SCALE_FLOOR_TUNABLE, DetectorOperator, is_integer_series, scale_floor

_CONSISTENCY = 0.6745  # 讓 MAD 在常態分布下跟標準差同尺度的常數


@OperatorRegistry.register("detector.mad")
class MADDetector(DetectorOperator):
    """Modified Z-Score（中位數與 MAD）：離群值不會把基準線拉歪，比 zscore 耐尖峰。
    params: threshold、min_scale／min_scale_ratio（分母底噪，見 base.scale_floor）。

    跟 zscore 一樣先 fit 再 detect：基準線來自訓練期，而不是這一輪資料自己（否則異常本身會被當成常態）。
    基準線存原始 MAD，底噪在 detect 時才套：後台調了 min_scale 不用等重訓就生效。
    """

    algorithm = "mad"
    tunable = {"threshold": (3.5, 1.0, 20.0), **SCALE_FLOOR_TUNABLE}

    def fit(self, df: pd.DataFrame) -> "MADDetector":
        values = df["value"]
        self.median_ = float(values.median())
        self.mad_ = float((values - self.median_).abs().median())
        self.integer_ = is_integer_series(values)  # 失敗數這類計數序列：分母至少 1 筆
        self.fitted = True
        return self

    def get_state(self) -> dict:
        self._require_fitted()
        return {"median": self.median_, "mad": self.mad_, "integer": self.integer_}

    def load_state(self, state: dict) -> "MADDetector":
        self.median_, self.mad_ = float(state["median"]), float(state["mad"])
        self.integer_ = bool(state.get("integer", False))  # 舊版基準線沒記：只套絕對與相對下限
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        scale = scale_floor(self.mad_, None, self.param("min_scale"), self.param("min_scale_ratio"),
                            center=self.median_, integer=self.integer_)
        scores = _CONSISTENCY * (df["value"] - self.median_).abs() / scale
        return self._result(df, scores, scores > self.param("threshold"))
```

### 範例二：新增一個 LLM 供應商（放在 `stages/enrichment/` 底下的新檔案）

不用改 `enrichment.llm_summary` 算子、也不用改 `providers.py`：用 `stages/enrichment/registry.register` 登記一個 factory，回傳實作 `complete(system, prompt) -> str` 的物件；`provider: noop` 永遠回傳 `None`、完全不連外。呼叫失敗丟 `LLMError`，算子會退回模板，不會讓 RCA 報告失敗。下面的 `echo` 供應商不連外，只示範介面：

```python
from aiops.stages.enrichment.base import BaseLLM, LLMError
from aiops.stages.enrichment.registry import build, register


class EchoLLM(BaseLLM):
    name = "echo"

    def __init__(self, model: str):
        self.model = model

    def complete(self, system: str, prompt: str) -> str:
        if not prompt:
            raise LLMError("prompt 是空的")  # 失敗一律丟 LLMError，呼叫端退回模板
        return '{"summary": "echo", "claims": []}'


@register("echo")
def _echo(model: str, base_url: str, api_key: str | None) -> BaseLLM:
    return EchoLLM(model)


llm = build("echo", "echo-1", "", None)
assert llm.complete("你是 SRE", "事件摘要").startswith("{")
assert build("noop", "", "", None) is None  # noop 完全不連外
```

---

## 4. 管線配置宣告 (Workflow YAML)

新增或調整場景流程時，**禁止修改 Python 流程代碼**，一律以 YAML 宣告節點流向：

```yaml
name: factory_lot_detection_pipeline
description: "工廠履歷時序檢測流程 (自由抽換 MAD 與關閉 LLM)"
nodes:
  # 1. 抓取指標
  - name: fetch_step
    operator: ingestion.elasticsearch
    params:
      index: "factory-metrics-*"

  # 2. 異常檢測 (隨時切換 detector.zscore / detector.mad / detector.isolation_forest)
  - name: detect_step
    operator: detector.mad
    params:
      threshold: 3.5

  # 3. 告警收斂
  - name: group_step
    operator: alert.lot_path_chain
    params:
      time_window_minutes: 15

  # 4. 告警抑制 (靜音/冷卻)
  - name: suppress_step
    operator: alert.suppress_window

  # 5. 可選 AI 摘要 (設為 noop 即刻關閉，不花 Token)
  - name: ai_step
    operator: enrichment.llm_summary
    optional: true   # 旁路節點：失敗只記進 errors 並跳過，不擋住後面的通知與落地
    params:
      provider: "noop"

  # 6. 發布與通知
  - name: notify_step
    operator: notifier.alertmanager
    params:
      endpoint: "http://alertmanager:9093"
```

**節點失敗怎麼處理**：預設任何節點出錯都讓這一輪（或這條序列）失敗。旁路裝飾節點（LLM 摘要等）加上 `optional: true`，失敗時只記進 `errors`（實體名為 `node:<節點名>`）並跳過，後面的節點與最終寫入照常執行。`InfrastructureError`（DB、資料源連不上）不受 `optional` 影響，一律讓整輪失敗。**不要把 fetch、detector、group、suppress、payload、notify 標成 optional。** 例外只有 RCA 的旁路證據撈取（`ingestion.incident_series`／`incident_traces`／`incident_logs`）：它們自己接住資料源連不上，寫進 `errors` 後略過，報告照出；偵測流程的 `ingestion.fetch` 不准這樣做。資料源的短暫抖動（連線失敗、逾時、429／502／503／504）由 `sources/_http.request` 重試 2 次。

**參數優先序**：演算法預設值 ← 後台核准的調參 ← YAML `params` ← target `overrides`，逐鍵合併（`engine/params.deep_merge`，收口在 `stages/detectors/base.layer_params`）；YAML 寫了的鍵鎖定、不被調參覆蓋。調參分層 (偵測器, 管線, 實體)，取最具體的一層（管線×實體 > 管線 > 實體 > 全域）。提案回測疊同樣的層，被 YAML 鎖住的鍵列在回測結果的 `workflow.locked`。

**RCA 依偵測管線分流**：偵測管線 YAML 的 `rca_workflow` 決定它的事件群預設用哪條 RCA 管線（`POST /api/v1/rca` 沒帶 `workflow` 時；`interfaces/runner.rca_workflow_for`）。有 trace 的寫 `rca_report_llm`，沒有 trace 的（工廠、純指標）寫 `rca_report`；新增偵測管線必須寫（`tests/test_rca_routing.py`），名稱寫錯在載入期報錯。

**workflow 只能用名稱**：API 的 `workflow` 欄位只接受 `[A-Za-z0-9_-]+` 的內建名稱（`interfaces/runner.workflow_path`，在 `pipelines/` 所有子資料夾依名稱找檔，名稱重複直接報錯），絕不接受路徑；只有排程 CLI 可以用 `allow_file=True` 載入自己的 YAML 檔。

**寫錯就在載入期報錯**（`tests/test_load_time_validation.py`）：
 * 值不合理（`threshold: -3`、`window_minutes: 0`、`cooldown_minutes: -5`）由算子的 `__init__` 檢查，用 `engine/params` 的 `number`／`positive`／`whole` 丟 `ValueError`；`OperatorRegistry.create` 轉成 `WorkflowParamError`（是 `WorkflowError` 也是 `ValueError`）。新算子的窗口、冷卻、門檻、權重參數一律在 `__init__` 檢查，不准拖到 `execute`。
 * 偵測器的 `tunable` 範圍由基底 `DetectorOperator.__init__` 的 `check_ranges()` 檢查（節點 YAML、target `overrides`、執行期合併後的參數都會經過）；需要額外檢查就覆寫 `check_ranges`，「合併後才知道齊不齊」的檢查（`detector.rules` 至少要有一條規則）才放 `validate_params`。
 * 欄位打錯（`param:`、`optionl:`、`windw_days:`、ES 查詢的 `fliter`）：`aiops/engine`、`aiops/sources` 的 Pydantic 模型一律繼承 `engine/pipeline.StrictModel`（或自己設 `extra="forbid"`），測試會掃。

**並發安全**：偵測器基準線（`detector_models`）與現行參數（`active_detector_configs`）用 `version` 欄位做樂觀鎖，寫入必須帶讀取當時的版本；版本已被別人推進就不覆蓋（基準線記在 audit，調參核准回 409 並把提案標成 stale）。

---

## 5. 檔案歸屬決策樹 (Where Does New Code Go?)

 * **要寫新的異常檢測演算法（如 Isolation Forest）？**
   👉 寫入 `aiops/stages/detectors/<name>.py`，掛上 `@OperatorRegistry.register("detector.<name>")`。
 * **要新增 LLM 供應商（如 Mistral、內部私有模型）？**
   👉 在 `aiops/stages/enrichment/` 放一個檔案，用 `@register("<name>")`（`stages/enrichment/registry.py`）登記 factory，啟動時自動掃描載入；不用改 `providers.py`、也不用改引擎。
 * **要新增監控指標或日誌連線源（如 InfluxDB）？**
   👉 寫入 `aiops/sources/<name>.py`，繼承 `BaseDataSourceSpec`，輸出轉成標準 DataFrame。
 * **要擴充新的收斂邏輯（如針對特定微服務呼叫鏈、同機櫃）？**
   👉 寫入 `aiops/stages/grouping/group.py`（或同目錄新檔案），實作 `BaseOperator`、用 `engine/types.merge_incidents` 給判斷函式；機櫃、網段這類資訊放在 target 的 `attrs`。
 * **要換 RCA 根因演算法（如 Random Walk、因果推論）？**
   👉 寫入 `aiops/stages/rca/algorithms/<name>.py`，繼承 `RootCauseOperator`、註冊成 `rca.root_cause.<name>`，在 `rca_report*.yaml` 換掉 `root_cause` 節點。
 * **要新增 RCA 報告段落？**
   👉 寫入 `aiops/stages/rca/sections/<name>.py`，繼承 `RcaSection`、註冊成 `rca.section.<key>`，加進 `rca.report` 的 `sections` 清單。
 * **要讓偵測器動態找同儕（同 service 的所有 pod）？**
   👉 不寫程式：target 加 `peer_group: {query, by}`。資料源要支援多序列分組就實作 `BaseDataSourceSpec.fetch_group`。
 * **要新增手動動作（核准、匯入、查詢這類 `admin.*`）？**
   👉 學習閉環相關寫入 `aiops/control_plane/learning/`，其他維運動作寫入 `aiops/control_plane/operations/`；註冊成 `admin.<name>`，在 `aiops/pipelines/control/admin_<name>.yaml` 宣告，API 端點在 `aiops/interfaces/api/` 只呼叫 `run("admin_<name>")`。
 * **要新增資料庫紀錄欄位？**
   👉 寫入 `aiops/storage/models.py`，只能透過 `aiops/storage/repository.py` 操作讀寫；並在 `migrations/postgres/` 補一支可重複執行的 SQL（沒有 Alembic，既有 PostgreSQL 靠它升級）。
 * **要調整執行步驟與算法超參數？**
   👉 寫入 `aiops/pipelines/<detection|rca|control>/<scene>.yaml`（名稱在 `pipelines/` 內必須唯一），完全不改動任何 Python 程式碼。
 * **要改 Grafana 的畫面（儀表板、面板連動、資料源）？**
   👉 只改 `deploy/helm/aiops-grafana/files/`（儀表板 JSON、`datasource.yaml`）與 chart 的 values，用 `helm upgrade` 部署。不改 Grafana 映像檔、不自製外掛。
 * **要讓人在畫面上填東西（回饋表單、核准）？**
   👉 表單頁寫在自研平台（`aiops/interfaces/`，靜態頁放 `aiops/interfaces/static/`，送出走既有的 `/api/` 端點與 token 驗證）；Grafana 只放「連過去」的連結（告警卡片的 `feedback_url` 註解，基底網址來自 `AIOPS_PUBLIC_URL`）。

### 周邊元件守則：只動 Helm chart，不動別人的映像檔

本 repo 部署的東西分兩種，改法不同：

| 元件 | 可以改什麼 | 嚴格禁止事項 |
| :--- | :--- | :--- |
| **自研平台**（`aiops/`，映像檔 `aiops-platform`） | 程式碼、映像檔、`deploy/helm/aiops-platform` | — |
| **Grafana**（`deploy/helm/aiops-grafana`） | chart 的 `files/`、templates、values | 嚴禁改官方映像檔、自製或 fork 外掛、進 pod 手動改設定；嚴禁放告警規則、Contact Point、業務邏輯（只顯示） |
| **Alertmanager**（`deploy/helm/aiops-alertmanager`） | chart 的 values（路由、receiver） | 嚴禁改官方映像檔 |
| **Elasticsearch**（開發叢集 `es-test`） | 這個 repo 沒有它的 chart；要調整就先把 chart 找回來或整理進 `deploy/helm/` 再改 | 嚴禁改映像檔；平台與 Grafana 對它**只讀**（`tests/test_architecture.py`），寫入合成資料只能由人跑 `scripts/synth_generator/es_writer` |

 1. **需求先問「chart 做不做得到」**：做得到就只改 chart；做不到（要新頁面、新 API、新資料）就做在自研平台，周邊元件只加設定或連結。不准為了一個功能去換周邊元件的映像檔。
 2. **所有改動都要回到 repo**：不准只在叢集上 `kubectl edit`／在 Grafana UI 手改而沒有對應的 chart 變更；部署一律 `helm upgrade`。
 3. **機密不進 chart**：values 與 `files/` 不得有金鑰、密碼，一律來自既有的 Secret。

---

## 6. 送出修改前的自動化驗證

```bash
# 1. 執行單元與行為測試
.venv/Scripts/python -m pytest tests/ -q

# 2. 執行架構防禦測試 (確認無 ReadDAO 殘留、算子無違規跨層依賴)
.venv/Scripts/python -m pytest tests/test_architecture.py -q
```

全部綠燈後方可 Commit。

---

## 99. 開發行為與驗收鐵律 (Anti-Stall & Reality-First Invariants)

為防止技術性拖延、形式主義與過度設計，所有對話與開發必須嚴格遵守以下禁令：

1. **嚴禁以「環境/安全問題」作為不驗證的藉口 (No Excuse Engineering)**：
   - 開發/驗證階段一律使用 `.gitignore` 排除的 `secrets.local.yaml` 或 `.env`。
   - **嚴禁在驗算演算法階段去修改 Helm Chart、CI 安全測試或部署設定**。違反此條視為過度設計（Over-engineering）。
   - 若缺測試資料或連線，必須主動提供最精簡的一行指令（如 `kubectl port-forward` 或密碼取得指令），不得要求使用者手動走繁複流程。

2. **驗證優先於單元測試 (Real Data Over Unit Tests)**：
   - 「合成資料測試全過」不等於演算法完成。
   - 任何演算法（基準線、RCA、同儕）實作完成後，**必須拿現存的真實資料（ES/Prometheus）進行回放對比驗收**。
   - 提交報告時，**禁止只回報「通過 N 個測試」**，必須直接貼出「真實指標回放數據、誤報數、檢出率、根因判斷結果」。

3. **禁止防衛性推拖 (Direct Action Only)**：
   - 當使用者要求跑數據驗收時，不得主動切換話題去重構周邊基礎設施（如 Secret 儲存方式、文檔格式、CI 白名單）。
   - 一律以「先取得結果、印出報表」為最高優先級，周邊清理工作一律後置。

---

## 100. 測試誠實性與生產真實性防禦準則 (Test Integrity Guidelines)

1. **嚴禁修改既有測試的邊界數值以迎合代碼 (No Test Downgrading)**：
   - 嚴禁透過調降測試輸入數值（例如將 9.0 改為 5.9）、放寬斷言（Assertions）或刪除邊界案例來讓測試通過。
   - 若代碼改動導致既有測試失敗，必須先判定「是原本測試假定與新架構衝突」，還是「新代碼引入了真實 Regression」。若需修改測試斷言，必須在 PR / Commit 訊息中明確陳述修改原因與業務語意變更。

2. **嚴禁生產代碼包含測試環境特化分支 (No Test-Specific Hacks)**：
   - 在 `stages/`、`storage/`、`pipelines/` 及演算法核心中，嚴禁出現 `if is_test:`、`if env == "test":`、`if "mock" in ...` 等繞道邏輯。
   - 核心邏輯必須對生產資料與測試資料一視同仁，測試的隔離性只能透過依賴注入（DI）或 Mock Fixture 完成，代碼本體不可感知測試環境。
   - 自動檢查：`tests/test_architecture.py::test_production_code_does_not_know_it_is_under_test`（掃 `aiops/` 的 `pytest`、`PYTEST_CURRENT_TEST`、`TESTING`、`is_test`、`test_env`）。

3. **旗標與狀態的常駐相容性 (State Leakage & Reset)**：
   - 在算子或 Engine 中引入任何旗標（如 `incidents_built`、快取、計數器）時，必須證明其生命週期在「常駐進程（Long-running Daemon / Loop）」中跨 Round 執行的重置機制，嚴禁依賴進程重啟來重置狀態。
   - 證明方式：旗標掛在每輪新建的 `PipelineContext` 上（`Pipeline.run` 每輪 `PipelineContext(...)`），並有跨輪次測試（`test_state_does_not_leak_between_rounds_of_a_long_running_loop`）。模組層級、類別層級的可變狀態一律不准。

4. **演算法死區誠實披露 (Algorithm Dead-zone Disclosure)**：
   - 當某個演算法（如門控遮罩、互相關、分位數）在特定波形（如階梯高原、單點毛刺、微弱斜率）下無法被激發時，嚴禁透過抽換測試波形（例如將階梯波形偷換成隨機漫步）來粉飾太平。必須將該波形死區以測試標註（`@pytest.mark.xfail(strict=True)` 或 docstring）明確留存。
   - 現有範例：`tests/test_rca_amplifiers.py`（透過訊息佇列放大時指不到生產者，`xfail(strict=True)`）。

5. **嚴禁用客製化 rule-based 補丁修測試失敗 (No Overfit Patches)**：
   - 測試或混沌場景失敗時，修的必須是**通用機制**（物理量、統計量、資料結構、錯誤處理），不准加「只為了讓這個測試、這個場景、這批資料過關」的規則。以下都算客製化補丁：
     - 針對特定服務名、資源名、佇列名、場景編號、資料長度、特定數值的分支或字面值比較（`if svc == "cart-api"`、`len(x) == 120`、`if scenario == 12`）。
     - 為單一場景新增的常數、門檻、權重，或調到「差 0.05 那個場景就會翻盤」的特調值。
     - 專為某個場景的形狀寫的特判（例如只在階梯波形、只在 25 倍扇出才成立的條件），而不是從現象的物理意義推導出來的判準。
   - 判斷是 bug 還是要加規則：先找「為什麼會失敗」。是程式錯誤（型別、邊界、例外、漏掉的輸入）就直接修，不需要新規則；是演算法對某類資料本來就看不到，照第 4 條記成死區，不准用特判蓋掉。
   - 新增或調整門檻／常數時，docstring 或 commit 訊息必須寫明：**來源**（通用依據或量測方式，不是「這樣才過」）、**驗證的資料**、**留出驗證**（沒有拿來調整的場景或序列上的結果）。沒有留出驗證就要寫明「未驗證泛化」，不准暗示已驗證。
   - 回報成績時，要分開寫「用來調整的資料」與「沒用來調整的資料」；兩者差距大就是過度擬合的訊號，必須在報告中說明，不准只報前者。
   - 自動檢查只能涵蓋一部分：`tests/test_architecture.py::test_algorithms_do_not_special_case_entities_or_lengths` 擋服務名與資料長度的字面值比較；常數是不是調出來的、規則是不是專病專藥，只能靠 code review 與留出驗證，沒有機器證明。
