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
3. **可選能力無害化（LLM Degradation）**：`stages/enrichment/` 必須提供 `noop` 實作。未配置金鑰時管線必須平滑降級至模板，嚴禁阻斷 Pipeline。
4. **模型序列化安全**：整個 `aiops/` 嚴禁 import `pickle`／`joblib`／`dill`／`cloudpickle`／`shelve`／`marshal`，也不准出現 `allow_pickle=True`。非 JSON 的模型本體只能用 npz（`pack_arrays`／`unpack_arrays`）。
5. **RCA 不寫死**：報告段落是 `rca.section.*`、根因演算法是 `rca.root_cause.*`，都走 `OperatorRegistry`；`stages/rca/report.py` 不得 import 具體段落或演算法。

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
   * 基準線、調參、異常血緣都以 **Workflow 節點名稱**（`self.state_key`）為準，同一個演算法可以在同一條管線掛多次。
   * 需要「往前 N 分鐘的自己」（同比、環比）時覆寫 `required_lags() -> list[int]`；fetch 要在 YAML 設對應的 `lags_minutes`，df 會多 `lag_<分鐘>` 欄位，回測也照這個抓。
   * 模型有非 JSON 的本體時覆寫 `get_artifact() -> (格式, bytes)` 與 `load_artifact(格式, bytes)`，格式只能是 `npz`。
   * 繼承 `DetectorOperator` 的算子自動 `has_baseline = True`，引擎才會替它載入基準線與調參；`detector.new_only`、`detector.ensemble` 這類不學習的算子不繼承它。
   * `AnomalyEvent` 是 frozen：建立後不准改分數或偵測器名稱。要合併多個偵測器的判斷，用 `detector.ensemble` 產生新的裁決事件。
   * **每條 target 跑哪些偵測節點由引擎決定**（`engine/pipeline.py`），順序：節點 `params.exclude_entities` → `params.include_entities` → target 的 `detectors`（節點名稱或算子名稱，不寫 = 全部）。被跳過的節點不載基準線與調參、不進 `ctx.detector_runs`。演算法裡不准自己判斷「這個實體要不要跑」。
   * `include_entities`／`exclude_entities` 是基底層的路由參數（`ROUTING_PARAMS`）：不進 `self.params`、不傳給演算法、調參提案帶它們會被拒絕。
   * 參數優先序：演算法預設值 ← 後台核准調參（`ctx.detector_params`）← 節點 YAML `params` ← target `overrides[節點名稱]`（`effective_params`，逐鍵 `deep_merge`）。overrides 在載入期用 `check_override` 檢查參數名與範圍。
   * `ctx.detector_runs` 只記「真的評估過資料」的節點（沒資料就 return 的不算）。`detector.ensemble` 的分母 = 參與者數；棄權不是反對票。`hourly_baseline` 資料不足時退回全段統計，仍算參與者——新偵測器若「資料不足就不判」，要在加入 `detector_runs` 之前 return。
   * target 的 `detectors`／`overrides` 寫錯、節點的 include／exclude 寫了 workflow targets 裡沒有的實體，都在 `Pipeline.__init__`（API 傳入的 targets 在 `run()` 開頭）丟 `WorkflowError`，不准延到執行期。
   * `exclude_incidents`（預設 true）、`warning_ratio`（預設關）是基底層的節點行為參數（`NODE_PARAMS`）：同樣不進 `self.params`、target `overrides` 不能寫。訓練排除窗由 `StorageHooks` 放進 `ctx.training_exclusions`，基底 `_training_frame` 統一套用——演算法的 `fit` 不准自己去查事故或維護窗。
   * 事件等級只有 `ANOMALY`／`WARNING`（`engine/types.py`）。warning 由基底依 `threshold × warning_ratio` 產生，演算法只管回傳分數與越線旗標。
 * **節點 scope**：`target`（逐序列）→ `batch`（所有序列合併後）→ `after_persist`（資料庫寫入成功後才跑，例如 `notifier.watchdog` 心跳）。順序錯在 `Pipeline.__init__` 報錯。寫入失敗時 `after_persist` 節點不執行，所以心跳停了 = 整輪沒成功。
 * **RCA 根因演算法（`rca.root_cause.*`）**：
   * 繼承 `stages/rca/base.RootCauseOperator`，只寫 `rank(incident, ctx) -> list[RootCandidate]`（由可能到不可能排好；資料不足回空清單，段落會略過）。
   * 需要原始時序時，在它前面放 `ingestion.incident_series`（讀 `ctx.rca_series`），不准自己連資料源。
 * **RCA 報告段落（`rca.section.*`）**：
   * 繼承 `stages/rca/base.RcaSection`，寫 `applies()` 與 `build()`；資料不夠就讓 `applies()` 回 False（記進 `rca_skipped`），不准輸出空殼。
 * **Grouper（事件收斂）**：
   * 接收 `ctx.anomalies`，輸出收斂後的 `ctx.incidents`。
   * 進行中事件群（`ctx.open_incidents`）由 `alert.absorb_open` 吸收新異常：一場事故從初發到結案只有一個 `incident_uid`。`ensure_incidents` 只建一次（`ctx.incidents_built`），清單被吸收光之後不准重建。
   * 順序鐵律：收斂 → 計分 → `absorb_open` → 抑制 → `notify_payload` → `heartbeat` → `notifier.*`。升級（嚴重度只升不降）改了 labels 時，`heartbeat` 負責對舊 labels 送 `endsAt` 再送新的；Alertmanager 用整組 labels 辨識告警。
 * **Suppressor（告警抑制）**：
   * 檢查靜音窗口（維護期）、冷卻時間（Dedup）、同根因；被抑制事件必須打上 `ctx.is_suppressed = True` 與具體原因。
 * **LLM Provider（智慧診斷）**：
   * 介面為 `complete(prompt: str) -> str`。
   * 當設定為 `provider: noop` 時，直接跳過推論並填入結構化模板，不發出任何外部網路請求。

---

## 3. 標準算子實作範例

### 範例一：可自由抽換的 MAD 檢測器 (`stages/detectors/mad.py`)

```python
import numpy as np
import pandas as pd
from aiops.engine.registry import OperatorRegistry
from aiops.engine.operator import BaseOperator
from aiops.engine.context import PipelineContext, AnomalyResult

@OperatorRegistry.register("detector.mad")
class MADDetector(BaseOperator):
    name = "mad"

    def __init__(self, threshold: float = 3.5):
        self.threshold = threshold

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if ctx.raw_df is None or ctx.raw_df.empty:
            return ctx

        values = ctx.raw_df["value"].values
        median = np.median(values)
        mad = np.median(np.abs(values - median))
        mad = mad if mad != 0 else 1e-6

        # 計算 Modified Z-Score
        mod_z_scores = 0.6745 * np.abs(values - median) / mad
        is_anomaly = (mod_z_scores > self.threshold).tolist()

        ctx.anomaly_result = AnomalyResult(
            algorithm="mad",
            is_anomaly=is_anomaly,
            scores=mod_z_scores.tolist(),
            latest_is_anomaly=bool(is_anomaly[-1]) if is_anomaly else False
        )
        return ctx
```

### 範例二：可自由外接或中斷的 LLM 算子 (`stages/enrichment/summarizer.py`)

```python
from aiops.engine.registry import OperatorRegistry
from aiops.engine.operator import BaseOperator
from aiops.engine.context import PipelineContext

@OperatorRegistry.register("enrichment.llm_summary")
class LLMSummarizer(BaseOperator):
    def __init__(self, provider: str = "noop", **kwargs):
        self.provider = provider
        self.is_enabled = (provider != "noop")

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        # 若未啟用 LLM 或無異常事件，直接降級為預設範本
        if not self.is_enabled or not ctx.incidents:
            ctx.llm_summary = "【自動診斷】未啟用 LLM，依據統計規則輸出摘要。"
            return ctx

        # 呼叫實際 Provider (OpenAI/Anthropic)
        # 生成內容需符合依據驗證，若失敗亦退回安全範本
        ctx.llm_summary = f"【AI 診斷】實體 {ctx.target_metric} 發生異常偏移..."
        return ctx
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

**節點失敗怎麼處理**：預設任何節點出錯都讓這一輪（或這條序列）失敗。旁路裝飾節點（LLM 摘要等）加上 `optional: true`，失敗時只記進 `errors`（實體名為 `node:<節點名>`）並跳過，後面的節點與最終寫入照常執行。`InfrastructureError`（DB、資料源連不上）不受 `optional` 影響，一律讓整輪失敗。**不要把 fetch、detector、group、suppress、payload、notify 標成 optional。**

**參數優先序**：演算法預設值 ← 後台核准的調參 ← YAML `params`，逐鍵合併（`engine/params.deep_merge`）；YAML 寫了的鍵鎖定、不被調參覆蓋。調參分層 (偵測器, 管線, 實體)，取最具體的一層（管線×實體 > 管線 > 實體 > 全域）。

**workflow 只能用名稱**：API 的 `workflow` 欄位只接受 `[A-Za-z0-9_-]+` 的內建名稱（`interfaces/runner.workflow_path`，在 `pipelines/` 所有子資料夾依名稱找檔，名稱重複直接報錯），絕不接受路徑；只有排程 CLI 可以用 `allow_file=True` 載入自己的 YAML 檔。

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
