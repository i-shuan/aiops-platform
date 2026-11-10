# RCA 第二步：建議生成（Advisory）檔案級設計規格（未動工）

狀態：設計稿，尚未實作。目標：把「建議處置」從 6 條子字串比對換成確定性 Runbook Matrix，直接引用第一步產出的下游資源歸因、範例 trace 與日誌樣板；
LLM 總結從自由長文改成三行式止血摘要，受白名單與數字一致性約束，失敗退回 Runbook 模板。
第一步（`docs/RCA_STEP1_DESIGN.md`）已合進 main（PR #3，`8d5c3e4`），本文所有「現況」都以 main 為準。

## 0. 動工前對照程式碼與真實資料抓出的四個坑

這四點不先定好，照原規格寫完程式在真機上一定失效。每一點都查過程式碼或剛才驗收用的 SQLite。

### 坑一（最大）：真實事件沒有「指標類別」

查第一步驗收產生的三個真實事件群（cart-api、ord-svc、bill-svc），每個 `AnomalyEvent`：

```
metric = {"index": "metrics-apm.transaction.1m-*", "agg": "avg", "value_field": "transaction.duration.summary.sum", "filter": {...}}
attrs  = {"dept": "payments"}
```

`metric` 是整段 ES 查詢 JSON（`TargetSpec` 沒有 `metric` 欄位，`AnomalyEvent.metric` 就是 target 的 `query`），`attrs` 沒有 `signal`。後果：

- 原規格 RB-03 的 `"error_rate" in metric_types`、RB-04 的 `traffic`＋`latency`、RB-05 的 `disk_io_time` 在真實資料上**永遠不成立**。
- 既有的 `rca.section.suggested_actions` 用子字串比對 `metric + attrs.signal`，`transaction.duration` 含 `io` → 延遲事件多一條「檢查磁碟容量與 I/O 等待」（第一步驗收報告看到的那條）。
- `alert.golden_signals` 讀 `e.metric in signals or e.attrs.get("signal") in signals`，真實事件兩個都對不上，所以第一步三份報告 `skipped` 裡一直有 `golden_signals`：**這個加權在真機上從沒生效過**。

### 坑二：`RootCandidate.dependency` 是給人看的字串

第一步填的是 `"tax-gw:443（external/http）"`。Runbook 要判「是不是 external」「佔比有沒有 ≥ 0.6」「失敗幾筆」，不能切中文字串。

### 坑三：檔名與資料位置

- 建議處置的檔案是 `aiops/stages/rca/sections/actions.py`（算子 `rca.section.suggested_actions`），不是 `suggested_actions.py`。
- LLM 節點是 `enrichment.llm_summary`（`stages/enrichment/summarizer.py`），prompt 與驗證在 `stages/enrichment/narrative.py`；沒有 `enrichment.narrative` 這個算子。
- 範例 trace 不在 ctx 上：`trace_evidence` 段落自己呼叫 `stages/rca/spans.sample_trace()`。建議處置與 LLM 的 context 也直接呼叫它，不新增 ctx 欄位。
- 日誌樣板排序是共用純函式 `stages/rca/logs.rank_patterns()`，`log_patterns` 段落與 `evidence` 算子都用它，Runbook 與 LLM context 也用它。

### 坑四：LLM 不打真模型就又是假驢收

現況 `rca_report*.yaml` 都是 `provider: noop`，測試用假供應商或 `monkeypatch`。`secrets.local.yaml` 已有開發用 OpenAI 金鑰（`llm.provider: openai`，上線前要換）。
第二步的驗收必須對 11/3 的 bill-svc 事件群真的叫一次模型，三行摘要過白名單與數字檢查才算過。

## 1. 決策一：訊號類別從 target 宣告（`attrs.signal`）

### 1.1 做法

- `TargetSpec` **不加欄位**：沿用 `attrs`，約定鍵 `signal`，值是四個黃金訊號之一 `latency`／`errors`／`traffic`／`saturation`（跟 `alert.golden_signals.GOLDEN_SIGNALS` 同一組字），工廠場景可以用自己的字（`rf_power`、`pressure`…）。
  `attrs` 本來就會原樣進 `AnomalyEvent.attrs`（`stages/detectors/base.py:177`），不用改引擎。
- `alert.golden_signals` **不改程式**，補上 signal 它就活了。
- Runbook 的「指標類別」判定順序：
  1. `attrs.signal`（有就用）
  2. 沒有時，把 `metric` 字串以非字母數字切成 **token**，整字相等才算（`duration`、`latency`、`p95` → latency；`error`、`errors`、`5xx` → errors；`qps`、`rps`、`requests` → traffic；`cpu`、`memory`、`disk`、`io`、`iops` → saturation）。`durat-io-n` 不再命中 `io`。
  3. 都沒有 → 類別未知，只靠歸因與 trace／日誌規則。
- 要補 signal 的地方：`pipelines/detection/it_otel.yaml`、`it_apm.yaml`、`factory.yaml`、`metrics_only.yaml` 的範例 targets；README「targets」一節；第一步驗收腳本 `rca_step1_accept.py` 的 5 個真實 target（`attrs: {dept: payments, signal: latency}`）。
- 載入期檢查：`Pipeline.__init__` 對 `attrs.signal` **不**強制（工廠場景不是黃金訊號），只在 README 寫明不填的後果：黃金訊號加權與 RB-03／RB-04／RB-05 不會觸發。

### 1.2 為什麼不加 `TargetSpec.metric`

API 合約（`POST /api/v1/runs` 的 targets）要跟著變、既有 YAML 與測試要全改；`attrs` 已經是「給收斂與計分看的標籤」的正式位置（CLAUDE.md 第 5 節：機櫃、網段放 `attrs`），訊號類別放這裡語意一致。

## 2. 決策二：`RootCandidate.attribution` 結構化欄位

```python
@dataclass
class RootCandidate:
    entity: str
    score: float
    reason: str
    dependency: str = ""                      # 不動：報告文字用
    attribution: dict[str, Any] = field(default_factory=dict)  # Runbook 與 LLM context 用；沒有資源葉節點就是 {}
```

`attribution` 的鍵（全部由 `rca.root_cause.evidence._dependency` 順手填，資料來源是 `ServiceWindowStats` 的 `app_ms_per_request`／`wait_ms_per_request`／`wait_failures`）：

| 鍵 | 型別 | 例（bill-svc） |
|---|---|---|
| `resource` | str | `tax-gw:443` |
| `kind` | str | `external` |
| `subtype` | str | `http` |
| `self_ms` | `{"before": float, "during": float}` | `{13.2, 12.6}`（程式自身） |
| `wait_ms` | `{"before": float, "during": float}` | `{13.3, 101.4}` |
| `failures` | `{"before": int, "during": int}` | `{1, 13}` |
| `share` | float | `1.0`（等待增量 ÷ 自身耗時增量，夾在 0～1） |
| `by` | `"latency"` 或 `"errors"` | 哪個條件讓 dependency 成立 |

`dependency` 非空 ⇔ `attribution` 非空；`share` 沒過門檻但有資源葉節點時，`attribution` 仍填（`by` 為空字串、`dependency` 為空），Runbook 才看得到「有資源但不夠主導」。
`PipelineContext.to_result()` 的 `candidates` 多輸出 `attribution`。

## 3. 決策三：Runbook Matrix 放哪、長什麼樣

### 3.1 位置

重構 `aiops/stages/rca/sections/actions.py`，**算子名與段落 key 維持 `suggested_actions`**（兩條 RCA YAML 的 `sections` 清單不用改）。
`_RULES` 子字串比對整個刪除。新增 ctx 欄位 `rca_runbook: dict | None`（`{"id", "name", "actions": [...]}`，命中的那條；沒命中為 None），給 LLM context 用。

### 3.2 評估特徵（`RunbookFacts`，純函式從 ctx 算，可單測）

| 特徵 | 來源 |
|---|---|
| `signals: set[str]` | 事件裡每個 `AnomalyEvent` 依第 1.1 節判定後的類別集合 |
| `metric_tokens: set[str]` | 同上第 2 步的 token（給 RB-05 這類精確名稱規則） |
| `top: RootCandidate \| None` | `ctx.rca_root_candidates[0]`；`top.attribution` 就是 `root_dependency` |
| `sample: SpanRecord \| None` | `spans.sample_trace(ctx.rca_spans, incident.start, incident.end + 1min)` |
| `sample_failed_resource: str` | 範例 trace 裡失敗的資源（`spans.sample_trace_reason` 同一套判定） |
| `log_patterns: list[dict]` | `logs.rank_patterns(...)` 前 3 筆（有 `template`、`error_type`、`ratio`、`during`） |
| `insufficient: bool` | 沒有候選且 `ctx.rca_root_note` 以「證據不足」開頭 |

### 3.3 規則表（依 priority 由高到低，**全部命中的都列**，不是只取第一條；每條的 actions 用 facts 做字串格式化）

| id | 條件（用 facts 寫） | 處方（3 條內） |
|---|---|---|
| RB-01 下游外部服務超時／激增 | `top.attribution.kind == "external"` 且 `share >= 0.6` | 檢查 `{resource}` 的健康狀態與網路延遲；確認本服務對 `{resource}` 的 HTTP client timeout 與連線池上限；持續惡化就評估降級或熔斷 |
| RB-02 資料庫／快取瓶頸 | `top.attribution.kind in ("db", "cache")` 且（`share >= 0.5` 或 `failures.during > failures.before`） | 排查 `{resource}` 的慢查詢、鎖等待與活躍連線數；近期是否有缺索引的查詢或批次搬移上線；應用端連線池是否打滿 |
| RB-03 應用程式邏輯錯誤 | `"errors" in signals` 且 `sample` 非 None 且（`sample.error` 或 `sample.status_code >= 500`）且（`top` 為 None 或 `share < 0.3`） | 點下方範例 trace 看 `{sample.name}` 的入參與堆疊；核對初發前 1 小時的部署、設定、feature flag；新版本導致就回滾 |
| RB-04 流量突波容量飽和 | `{"traffic", "latency"} <= signals` 且（`sample` 為 None 或 `sample.status_code < 500`） | 確認流量來源是活動還是爬蟲；HPA 或手動擴容；入口限流 |
| RB-05 磁碟 I/O | `metric_tokens ∩ {"disk", "io", "iops", "inode", "fs"}` 非空 或 `"saturation" in signals and "disk" in metric_tokens` | 檢查磁碟使用率與 IOPS；未滾動的 log 或 core dump |
| RB-06 共同外部因素 | `insufficient` | 不指認單一服務；查共用的資料源、網路、DNS、未被追蹤的依賴；比對同時間其他部門的告警 |
| RB-F1 製程機台 | `metric_tokens ∩ {"rf", "power", "pressure", "temp", "vacuum", "flow"}` 或 `signals` 含這些字 | 保留今天工廠那條：PM 紀錄與近期 recipe 變更 |
| RB-00 保底 | 一條都沒命中 | 比對事件前後的變更紀錄（部署、設定、配方）；再附「這次沒有命中任何故障特徵」一句，讓人知道是保底 |

**後續新增（日誌特徵處方，priority 介於 RB-07 與 RB-02 之間）**：`facts.log_patterns` 是嫌疑服務（第一名候選；沒有候選時是事件裡的實體）依暴增倍率排好的前 3 個錯誤日誌樣板。只在「問題在服務自己或它的資料庫／快取」（沒有主導的下游 `share < 0.3`，或歸因是 db／cache）且非 `insufficient` 時比對；樣板必須是突發的（正常期間沒出現，或頻率 ≥ 3 倍）。

| id | 日誌關鍵字（不分大小寫，比對例外類型＋樣板＋範例） | 處方 |
|---|---|---|
| RB-09 記憶體耗盡（93） | `OutOfMemory`、`OOMKilled`、`Java heap space`、`GC overhead limit exceeded`… | 確認 OOMKilled／重啟與 heap 曲線；heap dump 找佔用物件；重啟並暫時調高 limit |
| RB-10 連線池耗盡（92） | `ConnectionPoolExhausted`、HikariCP `Connection is not available, request timed out`、SQLAlchemy `QueuePool limit`、`too many connections／clients`… | 查池的 active／idle／pending 與上限；找佔著連線不還的呼叫；核對資料庫端 `max_connections` |
| RB-11 死結／鎖等待（91） | `Deadlock`、`LockAcquisitionException`、`Lock wait timeout exceeded` | 到資料庫查死結紀錄；核對更新順序與批次作業；暫停互鎖批次、有限重試 |

外部依賴主導（RB-01）時不看日誌：那時服務自己的連線池逾時是結果。11/3 真實資料的三個事件群（樣板分別是 `Unhandled exception in handler`、`ORA-<NUM>: TNS:Connect timeout occurred`、`Downstream tax-gw:<NUM> returned <NUM>`）都不命中，主處方不變。

第一條建議維持今天的「優先處理可能的源頭：…」；候選有 `dependency` 時第一步加的那句下鑽提醒保留，接著才是命中規則的處方。
11/3 預期：bill-svc → RB-01；ord-svc → RB-02；cart-api → `signals = {latency}`、沒有 dependency、`sample` 是 bill-svc 的 500（不是 cart-api 自己的）→ RB-03 不命中、RB-00 保底，報告寫明「沒有命中任何故障特徵」。

### 3.4 不做的

- 不做 Pod 層級「單節點壞點」規則：真實 target 沒有 pod 層級序列與 `peer_group`，寫了也是死條件（第一步同一個理由）。
- 不改 `rca.report` 與段落順序：`suggested_actions` 已是最後一段，前面所有證據都算好了。

## 4. 決策四：LLM 三行式止血摘要

### 4.1 `stages/enrichment/narrative.py`

- **context 壓縮**：新函式 `build_context(ctx, incident) -> dict`，只放已算好的數字，鍵固定：
  `incident_meta{entity|entities, duration_minutes, severity}`、`root_cause{candidate, score, attribution}`（直接放第 2 節的 dict）、
  `sample_trace{trace_id, endpoint, duration_ms, status_code, failed_resource}`、`top_error_logs[{service, error_type, template, during, ratio}]`（≤ 3）、
  `matched_runbook{id, name, actions}`、`root_note`（證據不足時的原因）。沒有的鍵給 `null`，不塞原始 metrics／span／log。
- **prompt**：system 固定英文（規格原文），加兩條：只能用 context 裡的名稱與數字、輸出只准 JSON。user prompt = `json.dumps(context, ensure_ascii=False)`。
- **schema**：`{"summary_line", "action_line", "verification_line"}` 三個非空字串，各 ≤ 200 字；多餘鍵忽略、少鍵視為格式錯。
- **驗證（全部要過）**：
  1. 實體白名單 = 事件實體 ∪ `attribution.resource` ∪ `sample_trace.endpoint` ∪ `matched_runbook.id`；文字裡出現任何 `ctx.topology`／`known_entities` 裡**不在白名單**的服務名 → 退回（沿用今天 `validate` 的做法，只是白名單從「事件實體」擴成上面四類）。
  2. 數字一致性：文字裡每個數字，要能在 context 的數值集合裡找到一個 **±10%** 以內的；找不到就退回。這比今天「原文逐字出現」寬，原因是模型會把 101.4 寫成 101 或「約 100」；10% 的代價寫在 7 節。
  3. 不准出現 context 以外的技術名詞黑名單（`kafka`、`rabbitmq`、`dns` 這類常見幻覺）除非它們在 context 裡。
- **模板退路** `template(context)`：三行直接由 Runbook 與 attribution 拼：
  `【一句話根因】bill-svc 等待 tax-gw:443（external/http）13.3→101.4 ms，程式自身 13.2→12.6 ms`／`【立即止血動作】<RB-01 第一條>`／`【驗證與排除項】自身錯誤率 0%→0%、流入流量 0.90 倍：排除程式本身與流量放大`。
  今天的 `template(sections)` 保留給舊欄位 `llm_summary`／`llm_claims`。
- ctx 新欄位 `llm_briefing: dict | None`（三行）；`llm_summary` 改為三行用換行接起來（舊消費者不壞），`llm_claims` 維持每行一條、`evidence` 指向對應段落 key（`root_cause_candidates`、`suggested_actions`、`trace_evidence`）。`to_result()` 的 `narrative` 多 `briefing`。

### 4.2 `stages/enrichment/summarizer.py`

流程不變：noop 或 `sections` 空 → 模板；有供應商 → `complete(SYSTEM, build_prompt(context))` → `parse_json_reply` → `validate_briefing` → 不過就 `ctx.llm_rejected = 原因` 並退回模板。`generated_by` 仍是 `llm`／`template`。
新 param `json_mode: bool = True`（只對 OpenAI 相容供應商有效）。

### 4.3 `stages/enrichment/providers.py`

`OpenAICompatibleLLM.__init__` 加 `json_mode: bool = False`，為 True 時 body 多 `"response_format": {"type": "json_object"}`。`BaseLLM.complete(system, prompt)` 介面不變（CLAUDE.md 範例二不受影響）。
Anthropic 沒有對應參數，維持靠 prompt＋`parse_json_reply`。`registry.register` 的 factory 簽名不變；`json_mode` 由 `build_llm(..., json_mode=)` 傳給 OpenAI factory，其他供應商忽略。

### 4.4 YAML

`rca_report*.yaml` 的 `summary` 節點**維持 `provider: noop`**（正式環境要不要花 token 是部署決定）。真實驗收腳本用 `Workflow.model_validate(base.model_dump() | {...})` 把 `summary` 節點的 params 換成 `{provider: openai, model: <secrets 的 model>}`，金鑰只從環境變數 `AIOPS_LLM_API_KEY` 進，不改 YAML、不印金鑰。

## 5. 檔案級改動清單

| 檔案 | 改什麼 |
|---|---|
| `aiops/engine/context.py` | `RootCandidate.attribution: dict`；`PipelineContext.rca_runbook`、`llm_briefing`；`to_result` 多 `attribution`、`briefing` |
| `aiops/stages/rca/algorithms/evidence.py` | `_dependency` 改回傳 `(dependency, attribution)`，兩個一起填 |
| `aiops/stages/rca/sections/actions.py` | 刪 `_RULES`；`signal_of(event)`、`RunbookFacts.from_ctx()`、`RUNBOOK_RULES`、`evaluate()`；寫 `ctx.rca_runbook` |
| `aiops/stages/enrichment/narrative.py` | `build_context`、`BRIEFING_SYSTEM`、`build_briefing_prompt`、`validate_briefing`（白名單、±10%、黑名單）、`briefing_template` |
| `aiops/stages/enrichment/summarizer.py` | 改走三行流程；`json_mode` param；填 `llm_briefing` |
| `aiops/stages/enrichment/providers.py` | OpenAI 相容 `json_mode` |
| `aiops/pipelines/detection/*.yaml` | 範例 targets 加 `attrs.signal` |
| `README.md` | targets 的 `signal`、建議處置規則表、三行摘要與退路、`json_mode` |
| `tests/test_runbook_matrix.py`（新） | 第 6.3 節 |
| `tests/test_narrative_guardrails.py`（新） | 第 6.4 節 |
| `scratchpad/rca_step2_accept.py`（驗收腳本，不進 repo） | 第 6.2 節 |

**不改**：`rca/report.py`、`rca/spans.py`、`rca/logs.py`、`sources/*`、`storage/*`、任何 detector、Helm／CI、既有測試斷言。
架構防線：`stages/rca/sections/actions.py` 不 import `enrichment`；`enrichment/*` 不 import 具體段落（只讀 ctx）；`narrative.py` 仍是純函式、不連外；連外只在 `providers.py`（白名單內）。

## 6. 硬性驗收

### 6.1 子字串誤判

- 手刻事件 `metric = "http_request_duration_seconds"`、無 `attrs.signal` → `signals == {"latency"}`，處方**沒有**任何「磁碟」字樣；`metric = "node_disk_io_time_seconds_total"` → 有。
- 真實 ES 回放的三個事件群，處方清單裡**沒有**「磁碟」。

### 6.2 11/3 真實 ES 回放（完整 `rca_report`，targets 補 `signal: latency`）

| 事件群 | 必須 |
|---|---|
| bill-svc | `rca_runbook.id == "RB-01"`；處方第一條含 `檢查下游依賴目標 tax-gw:443 的健康狀態與網路延遲`；`golden_signals` **不再**出現在 `skipped` |
| ord-svc | `rca_runbook.id == "RB-02"`；處方第一條含 `排查 oracle 當前慢查詢、鎖等待與活躍連線數` |
| cart-api | `rca_runbook.id == "RB-00"`，報告寫明沒有命中任何故障特徵 |
| 三個 | `errors == []`；`attribution` 出現在 `to_result()` 的 candidates；`provider: noop` 時 `narrative.briefing` 三行由模板填、`generated_by == "template"` |

### 6.3 `tests/test_runbook_matrix.py`

1. `signal_of`：有 `attrs.signal` 以它為準；沒有時 token 判定；`duration` 不給 `io`；ES JSON 查詢字串 → 只靠 `value_field` 的 token（`transaction.duration.summary.sum` → latency）。
2. RB-01～RB-06、RB-F1、RB-00 各一組手刻 facts 命中與不命中。
3. 多條同時命中依 priority 排列、保底只在全部不命中時出現。
4. 處方字串格式化帶入 `resource`、`sample.name`。
5. `ctx.rca_runbook` 有填；沒有候選、沒有 span（工廠）照樣跑出 RB-F1 或 RB-00，不丟例外。

### 6.4 `tests/test_narrative_guardrails.py`

1. `build_context` 對有／無 attribution、有／無 sample、有／無日誌都給固定鍵，缺的是 `null`。
2. 假供應商回合法三行 → `generated_by == "llm"`、`llm_briefing` 三鍵。
3. 回文提到拓撲裡不在白名單的服務 → 退回模板、`llm_rejected` 說明原因。
4. 數字 `101`（context 101.4）過；`150` 不過。
5. 回文含 `Kafka`（context 沒有）→ 退回。
6. 回文少一個鍵、不是 JSON、供應商丟 `LLMError` → 都退回模板且 `errors` 為空。
7. `json_mode` 為 True 時 OpenAI 請求 body 有 `response_format`；Anthropic 沒有。

### 6.5 真實 LLM（一次，bill-svc 事件群）

用 `secrets.local.yaml` 的 provider／model 與環境變數金鑰真的叫一次模型，貼出：原始回文、`validate_briefing` 結果、最終 `llm_briefing`、`generated_by`。
要過的是「格式正確且通過白名單與數字檢查」；若模型回文不過驗證、退回模板，**也算驗收資訊**，照實貼出退回原因，不重試到過為止。
金鑰只在本機環境變數，不進 log、不進報告；上線前換鑰的提醒不變。

## 7. 已知死區與取捨

- **數字 ±10%**：模型把 13 筆寫成 14 筆會過；代價是可解釋性稍降。退回模板的三行一定精確，所以報告永遠有一版 100% 正確的文字。
- **cart-api 這類「程式自己變慢但不到門檻」**：沒有規則命中，保底不會替它編理由；第三步的學習閉環（門檻）才是解法。
- **工廠場景**沒有 span、沒有 attribution，只有 RB-F1 與 RB-00 會動；跟今天的能力一樣，沒有退步。
- **Anthropic 沒有 json_mode**：靠 prompt 與 `parse_json_reply`，格式錯就退回模板。
- **模型非決定性**：temperature 0 但不保證逐字相同；驗收看「通過驗證」不看逐字。
- **白名單只擋「已知但不相干」的服務名**：模型編一個拓撲裡沒有的新名字（例如 `payment-gateway`）白名單擋不到，黑名單只擋常見幾個；這是 LLM 總結的固有風險，報告會標 `generated_by: llm` 讓人知道哪一段是模型寫的。

## 8. 實作紀錄與真實 ES 驗收結果（2026-11-09，分支 `feat/rca-step2-advisory`）

### 8.1 跟第 1～7 節不同的地方（實作時的修正，理由寫在這裡）

- **三行摘要是獨立節點 `enrichment.llm_briefing`，不是改 `llm_summary`。** 第 4.1 節原本寫「`llm_summary` 改為三行用換行接起來」。
  實作時發現 `tests/test_rca_llm.py` 有 9 個測試鎖著 summary／claims 的 schema 與退回原因字樣；把它們改掉等於為了新功能改既有斷言。
  改成兩個節點各自獨立：`llm_summary` 一字不動、新節點 `llm_briefing` 放在它後面（`rca_report*.yaml`），ctx 多 `llm_briefing`／`llm_briefing_generated_by`／`llm_briefing_rejected`，
  `to_result()` 的 `narrative` 多 `briefing`、`briefing_generated_by`、`briefing_rejected`。代價：設了供應商時每份報告叫兩次模型。
- **唯一改到的既有測試**：`tests/test_resilience.py::test_optional_summary_in_rca_report_workflow_is_marked_optional` 是節點盤點（列出 rca_report 每個節點的 optional 旗標），
  期望清單加上 `"briefing": True`。這是清單多一個節點，不是放寬斷言。
- **OpenAI 相容供應商的 temperature。** 第一次真實呼叫 `gpt-6-luna` 回 HTTP 400：`'temperature' does not support 0 with this model`（新一代推理型模型只接受預設值），
  兩個節點都退回模板、`errors` 為空（退路有效）。這是我們的請求參數問題，不是模型輸出問題：`OpenAICompatibleLLM.complete` 改成「400 訊息含 temperature 就拿掉 temperature 再送一次」，
  其他 400 照丟。既有測試 `test_openai_request_and_reply` 仍斷言預設帶 `temperature: 0`，沒改。
- **cart-api 命中 RB-06 而不是第 3.3 節預測的 RB-00**：根因算子對 cart-api 回「證據不足」（+3.4 ms 低於門檻），正好是 RB-06 的條件。預測錯、規則對。
- `RootCandidate.attribution` 在「有資源葉節點但不夠主導」時也填（`by` 為空、`dependency` 為空字串），跟第 2 節一致。

### 8.2 真實 ES 回放（實裝程式、完整 `rca_report` 管線）

跑法：真實 ES（唯讀 key）× `it_otel`，5 個 target 補 `attrs: {signal: latency}`，回放 11/3 14:00→16:20Z 每 5 分鐘一輪寫暫存 SQLite（notifier=log），
再對每個事件群跑完整 `rca_report`（provider noop）。三份報告 `errors` 全空。

| 事件群 | 分數（加權後） | `golden_signals` 段落 | 根因 #1 | `attribution` | Runbook |
|---|---|---|---|---|---|
| cart-api 14:56→16:15 | 15.35 | **有產出**（以前一直 skipped） | 證據不足（+3.4 ms） | — | **RB-06** 共同外部因素 |
| ord-svc 15:33→16:00 | 15.37 | 有產出 | ord-svc 1.0，dependency `oracle（db/oracle）` | kind db、share 1.0、wait 21.3→66.4、failures 0→17、self 22.5→21.1 | **RB-02**，第一條 `排查 oracle 當前慢查詢（slow log）、鎖等待（lock wait）與活躍連線數` |
| bill-svc 15:40→16:10 | 31.83 | 有產出 | bill-svc 1.0，dependency `tax-gw:443（external/http）` | kind external、share 1.0、wait 13.3→101.4、failures 1→13、self 13.2→12.6 | **RB-01**，第一條 `檢查下游依賴目標 tax-gw:443 的健康狀態與網路延遲（bill-svc 的增量幾乎全在等它）` |

三份報告的建議處置都**沒有**「磁碟」字樣（第 6.1 節）。三行模板（noop）例，bill-svc：

```
【一句話根因】bill-svc 等待 tax-gw:443（external/http）13.3→101.4 ms，程式自身 13.2→12.6 ms，資源失敗 1→13 筆
【立即止血動作】[RB-01] 檢查下游依賴目標 tax-gw:443 的健康狀態與網路延遲（bill-svc 的增量幾乎全在等它）
【驗證與排除項】bill-svc 程式自身耗時沒有惡化，排除它本身的程式與機器；範例 trace dd487b019660078efe3b34680e74e6ed 入口 500，失敗在 tax-gw:443
```

### 8.3 真實 LLM（OpenAI，`secrets.local.yaml` 的 provider／model，金鑰只在環境變數）

bill-svc 事件群，`summary` 與 `briefing` 兩個節點都換成 openai，各叫一次：

- **第一次**：兩個節點 HTTP 400（temperature 不支援 0）→ 都退回模板、`errors` 空。修 provider（8.1 節）後**再跑一次**（修的是請求參數，不是重擲模型輸出）。
- **第二次**：`briefing_generated_by = llm`、`briefing_rejected = None`（白名單、±10% 數字、黑名單全過）；`summary` 節點也是 `generated_by = llm`。模型原始回文（json_mode=True）：

```
【一句話根因】bill-svc 的 POST /invoice 失敗，根因指向 tax-gw:443；等待時間由 13.3 ms 升至 101.4 ms，失敗數由 1 升至 13。
【立即止血動作】檢查 tax-gw:443 健康狀態與網路延遲，確認 bill-svc 的 HTTP client timeout 與 Max Connections，若持續惡化則評估降級或熔斷。
【驗證與排除項】追查 trace dd487b019660078efe3b34680e74e6ed（POST /invoice，500），確認 tax-gw:443 等待時間及失敗數回落，並觀察 bill-svc 的 java.lang.IllegalStateException 錯誤。
```

第三行提到的 `java.lang.IllegalStateException` 來自 context 的 `top_error_logs`（bill-svc 事件期間暴增的日誌樣板之一），不是幻覺（見 8.4）。

### 8.4 測試

- 全套 `tests/`：見 commit 訊息的數字；既有斷言只有 8.1 節那一個節點清單加一項。
- 新增 `tests/test_runbook_matrix.py` 13 案（訊號判定、八條規則命中／不命中、優先序與保底、處方帶入資源名、ctx.rca_runbook、工廠無 span 不丟例外、duration 不給磁碟、`golden_signals` 有 signal 才加權）、
  `tests/test_narrative_guardrails.py` 18 案（context 固定鍵、合法三行、白名單、數字 ±10% 六組、黑名單、格式錯五組、供應商例外與 noop、證據不足模板、json_mode、temperature 退讓）。
