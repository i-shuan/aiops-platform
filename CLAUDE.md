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
| **資料流載體** | `core/context.py` | 貫穿整條 Pipeline 的強型別 `PipelineContext` | 嚴禁在節點間傳遞鬆散無型別的 `dict` |
| **管線引擎** | `core/pipeline.py` | 讀取工作定義，按序實例化算子並調用 `.execute(ctx)` | 嚴禁在引擎內 hardcode 具體算子邏輯 |
| **算子註冊** | `core/registry.py` | 算子註冊中心 (`OperatorRegistry`)，負責動態發現與驗證 | — |
| **可抽換算子** | `operators/*` | 業務零件（Detector、Grouper、Suppressor、RCA、LLM、Notifier） | 嚴禁 import 具體資料表 ORM；嚴禁碰外網（LLM 與 Notifier 除外） |
| **外部源適配** | `datasource_specs/*` | 外部資料庫語法轉換與連線（Prometheus, ES, OTel），輸出 DataFrame | 嚴禁 import `models` 或 `operators` |
| **持久化層** | `storage/*` | 資料庫模型定義（SQLAlchemy）與 Repository | 嚴禁業務判斷邏輯 |
| **排程入口** | `tasks/*` | 呼叫 Pipeline 執行（支援 K8s CronJob 或定時 Loop） | 嚴禁直接在排程中操作底層運算 |
| **API 接口** | `api/*` | 外部查詢與手動觸發，掛載 Token 驗證 | 零業務邏輯（嚴禁 `if / for / try`），純調度 Pipeline 或查詢 Storage |

### 🚫 架構自動化防禦（違規將導致 `test_architecture.py` 報錯）
1. **嚴禁內部跨節點偷渡**：任何算子內部嚴禁出現 `ReadDAO` 去翻其他模組的表。所有上游資訊必須由 `ctx` 取得。
2. **算子獨立性**：`operators/detectors/` 各演算法之間嚴禁互相 import。
3. **可選能力無害化（LLM Degradation）**：`operators/llm/` 必須提供 `noop` 實作。未配置金鑰時管線必須平滑降級至模板，嚴禁阻斷 Pipeline。

---

## 2. 算子合約規範 (Operator Protocols)

所有可插拔算子必須繼承 `BaseOperator` 並實作標準 `execute` 介面：

```python
from abc import ABC, abstractmethod
from aiops.core.context import PipelineContext

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
 * **Grouper（事件收斂）**：
   * 接收 `ctx.anomalies`，輸出收斂後的 `ctx.incidents`。
 * **Suppressor（告警抑制）**：
   * 檢查靜音窗口（維護期）、冷卻時間（Dedup）、同根因；被抑制事件必須打上 `ctx.is_suppressed = True` 與具體原因。
 * **LLM Provider（智慧診斷）**：
   * 介面為 `complete(prompt: str) -> str`。
   * 當設定為 `provider: noop` 時，直接跳過推論並填入結構化模板，不發出任何外部網路請求。

---

## 3. 標準算子實作範例

### 範例一：可自由抽換的 MAD 檢測器 (`operators/detectors/mad.py`)

```python
import numpy as np
import pandas as pd
from aiops.core.registry import OperatorRegistry
from aiops.operators.base import BaseOperator
from aiops.core.context import PipelineContext, AnomalyResult

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

### 範例二：可自由外接或中斷的 LLM 算子 (`operators/llm/summarizer.py`)

```python
from aiops.core.registry import OperatorRegistry
from aiops.operators.base import BaseOperator
from aiops.core.context import PipelineContext

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

**參數優先序**：演算法預設值 ← 後台核准的調參 ← YAML `params`，逐鍵合併（`core/params.deep_merge`）；YAML 寫了的鍵鎖定、不被調參覆蓋。調參分層 (偵測器, 管線, 實體)，取最具體的一層（管線×實體 > 管線 > 實體 > 全域）。

**workflow 只能用名稱**：API 的 `workflow` 欄位只接受 `[A-Za-z0-9_-]+` 的內建名稱（`runner.workflow_path`），絕不接受路徑；只有排程 CLI 可以用 `allow_file=True` 載入自己的 YAML 檔。

**並發安全**：偵測器基準線（`detector_models`）與現行參數（`active_detector_configs`）用 `version` 欄位做樂觀鎖，寫入必須帶讀取當時的版本；版本已被別人推進就不覆蓋（基準線記在 audit，調參核准回 409 並把提案標成 stale）。

---

## 5. 檔案歸屬決策樹 (Where Does New Code Go?)

 * **要寫新的異常檢測演算法（如 Isolation Forest）？**
   👉 寫入 `aiops/operators/detectors/<name>.py`，掛上 `@OperatorRegistry.register("detector.<name>")`。
 * **要新增 LLM 供應商（如 Mistral、內部私有模型）？**
   👉 在 `aiops/operators/llm/` 放一個檔案，用 `@register("<name>")`（`operators/llm/registry.py`）登記 factory，啟動時自動掃描載入；不用改 `providers.py`、也不用改引擎。
 * **要新增監控指標或日誌連線源（如 InfluxDB）？**
   👉 寫入 `aiops/datasource_specs/<name>.py`，繼承 `BaseDataSourceSpec`，輸出轉成標準 DataFrame。
 * **要擴充新的收斂邏輯（如針對特定微服務呼叫鏈）？**
   👉 寫入 `aiops/operators/alerts/groupers.py`，實作標準 `BaseOperator`。
 * **要新增資料庫紀錄欄位？**
   👉 寫入 `aiops/storage/models.py`，只能透過 `aiops/storage/repository.py` 操作讀寫；並在 `migrations/postgres/` 補一支可重複執行的 SQL（沒有 Alembic，既有 PostgreSQL 靠它升級）。
 * **要調整執行步驟與算法超參數？**
   👉 寫入 `aiops/workflows/<scene>.yaml`，完全不改動任何 Python 程式碼。

---

## 6. 送出修改前的自動化驗證

```bash
# 1. 執行單元與行為測試
.venv/Scripts/python -m pytest tests/ -q

# 2. 執行架構防禦測試 (確認無 ReadDAO 殘留、算子無違規跨層依賴)
.venv/Scripts/python -m pytest tests/test_architecture.py -q
```

全部綠燈後方可 Commit。
