# CLAUDE.md - AIOps 架構守則與開發規範

## 1. 核心架構哲學

本專案採用「DDD 垂直領域（domains）+ 可插拔能力（plugins）+ 資料源適配（datasource_specs）」架構。
所有新功能、Bug 修復與重構，必須服從下列硬性禁令。**這些禁令由 `tests/test_architecture.py` 機械式檢查，違規 = 測試紅燈。**

### 絕對禁止事項

1. **嚴禁反向與跨層依賴**
   - `aiops/plugins/` 不得 import 任何 `aiops.domains`（外掛是純零件，不認識業務）。
   - `aiops/domains/*/api.py` 只能 import：`fastapi`、`typing`、`pydantic`、同領域的 `commands/` 與 `schemas`。不得 import `plugins`、`datasource_specs`、`models`。
   - `aiops/domains/*/commands/` 只能從 `aiops.plugins.base` 與 `aiops.plugins.registry` 取用外掛抽象；具體演算法（`plugins/detectors/*`、`plugins/llm/*`）一律 `DetectorRegistry.get()` 動態取得。
   - 領域層不得直接 import 具體資料源實作（如 `datasource_specs.mock`），一律走 `get_datasource_spec()`。
2. **嚴禁建立 Service 雜物箱**：不得出現 `*_service.py` 或 `services/` 目錄。業務邏輯一律寫在對應領域的 `commands/*.py`。
3. **api.py 零邏輯**：只做 (1) 用 Schema 驗證請求、(2) 實例化 Command 並呼叫 `.run()`。不得有 if / for / while / try / with / 推導式。連「列出外掛」這種小事也要包成 Command。
4. **utils/ 不是雜物箱**：只放無業務語意的純函式（時間戳轉換、矩陣工具）。含業務判斷的程式碼一律回到 Command。

## 2. 檔案歸屬決策樹

- **新增 API 端點** → `aiops/domains/<domain>/api.py`；請求/回應規格放 `schemas.py`。
- **新增領域** → 建 `aiops/domains/<domain>/`，放 `api.py`（需定義 `router`）。`initialization.init_routers` 會自動掛載，**不要**手動去 import 註冊。
- **系統業務流程**（檢測後是否發警報、審核調優參數…）→ 繼承 `BaseCommand`，放 `aiops/domains/<domain>/commands/<action>.py`。
- **串接新監控資料源**（Prometheus、VictoriaMetrics、ES、ClickHouse…）→ 繼承 `BaseDataSourceSpec`，放 `aiops/datasource_specs/<name>.py`，並在 `datasource_specs/__init__.py` 的 `_SPECS` 加一行。輸出固定為 `DataFrame(ts, value)`。
- **新增異常檢測演算法** → 繼承 `BaseDetector`，放 `aiops/plugins/detectors/<algo>.py`，用 `@DetectorRegistry.register` 註冊（啟動時自動掃描，不需改其他檔案）。
- **新增 LLM 供應商** → 繼承 `BaseLLMProvider`，放 `aiops/plugins/llm/`。
- **資料庫存取（CRUD）** → `aiops/domains/<domain>/dao.py`（尚未建立，第一個需要落庫的領域建立時一併定義）。Command 內不得散落原生 SQL 或 ORM session 操作。
- **ORM 模型** → `aiops/models/`。
- **排程 / 背景任務** → `aiops/tasks/`，任務內只呼叫 Command，不寫邏輯。

## 3. 標準樣板

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
        # 2. 算結果：DetectorRegistry.get(...)(params).detect(df)
        # 3. 寫 DB：透過 DAO
        return {"status": "success"}
```

業務錯誤一律丟 `CommandError`（或子類別，如 `CommandNotFound`），由 `initialization` 統一轉成 HTTP 4xx，不要在 api.py 自己 try/except。

### Detector 外掛

```python
import pandas as pd
from aiops.plugins.base import AnomalyResult, BaseDetector
from aiops.plugins.registry import DetectorRegistry


@DetectorRegistry.register
class MyDetector(BaseDetector):
    name = "my_detector"

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        # 輸入保證有 ts 與 value 兩欄；輸出必須是 AnomalyResult
        ...
```

外掛必須是無副作用的純運算：不連 DB、不打外部服務、不讀全域設定。

## 4. 送出修改前的自檢

```bash
.venv/Scripts/python -m pytest tests/ -q
```

必須全綠，其中 `tests/test_architecture.py` 負責第 1 節的所有依賴禁令。新增一條架構規則時，先在該檔補測試，再改本文件。
