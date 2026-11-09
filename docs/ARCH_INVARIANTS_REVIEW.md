# 架構不變量與邊界安全審查（main `781ea3c`，2026-11-09）

唯讀審查：沒有改任何平台程式、沒有放寬任何測試。PR 只含這份報告、`scripts/probe/` 底下 14 支探測腳本、`tests/test_invariants_probe_rca_isolation.py`。
涉及未合併分支的題目同時看了分支程式：

| 代號 | 分支 | 怎麼取得 |
| :--- | :--- | :--- |
| PR A | `claude/hardening-a-noise-floor-edb621` | **還沒 commit**：2026-11-09 從那個 worktree 複製 `aiops/` 快照來測（`git diff` 見 `stages/rca/spans.py`、`stages/detectors/*`），之後若再改，數字可能不同 |
| PR B | `origin/claude/hardening-b-lexicon-2d1c7e`（`b1e056b`） | `git archive` |
| PR C | `origin/claude/hardening-c-topology-b504e2`（`8eefaf0`） | `git archive`（只用到 migration 0006） |

環境：Windows、`.venv` Python、PostgreSQL 16 容器（`postgres:16`，port 55437，拋棄式）。真 ES 這次沒用到：15 題都能用手刻資料或假 HTTP 回應撞出來，不需要真實流量。
所有結果都是 2026-11-09 實際跑出來的輸出原文（只刪掉重試的 WARNING log）。重跑方式見最後一節。

---

## 總表

| # | 題目 | 判定 | 一句話 |
| :-- | :--- | :---: | :--- |
| 1 | 分層隔離（RCA 純記憶體） | **部分** | RCA 核心在封鎖 storage／sources／API／SQLAlchemy／httpx 下跑完 10 段；唯一越界是 `trace_evidence` 經 `notification.links` 讀環境變數；`enrichment.summarizer` 硬依賴 httpx |
| 2 | 併發防禦 | **破防** | 樂觀鎖兩張表都正確；但經驗庫 upsert＋merge 是兩筆交易（讀得到半套）、verdict 沒 CAS、`rca_report` 完全沒有互斥（10 worker = 20 次 LLM 呼叫） |
| 3 | 時鐘偏移 | **破防（兩版各破一邊）** | main 完全不看時間戳；PR A 抓得到偏移，但「超出部分不扣」讓同一段時間被父子各算一次：服務自身 300 ms > 請求總長 200 ms |
| 4 | 數值合約 | **破防** | 節點 YAML 的參數範圍只在執行期檢查（`threshold: -3` 啟動成功、每條序列記錯、整輪仍算成功）；一個 Inf 分數讓整輪寫入失敗；`NodeSpec`／`Workflow` 沒有 `extra="forbid"` |
| 5 | 實體安全（PR B 詞表） | **部分** | 31 例 6 例不符：全形英文完全隱形、純數字只靠 ±10% 檢查、trace id 截半與 URL 編碼誤拒；實體白名單用子字串比對（main 也是）會誤拒 |
| 6 | ES 504／連線池爆滿 | **部分** | 一條序列 504 或 PoolTimeout → 整輪 `InfrastructureError`，健康序列的結果一起丟；`optional: true` 也擋不住（設計如此，但邊界比 CLAUDE.md 寫的「序列隔離」窄） |
| 7 | 換成純 OTel Collector 索引 | **合格** | 欄位映射集中在 `sources/elasticsearch.py`，`stages` 吃的是 dict 合約；必改 1 檔＋設定 2～3 檔 |
| 8 | 模組層級可變狀態 | **合格（有小洞）** | 算子沒有跨輪狀態；`db.get_engine` lazy 初始化無鎖：10 執行緒建出 10 個 Engine |
| 9 | 回溯窗含頭含尾 | **部分** | 回放統一 `[start, end]`；偵測與提案回測跟著資料源走：ES 60 點、Prometheus／mock 61 點；`replay.py:103` 註解說 Prometheus 不含尾是錯的 |
| 10 | 經驗庫 `context` 無版本 | **部分** | 缺鍵、多鍵都容忍；缺 `query`、`series` 不是 list、`context` 是 jsonb 陣列都直接丟例外；PG 的 `NULL \|\| patch` 默默變 NULL |
| 11 | migration 回滾 | **部分** | 兩支都沒有 CASCADE／DELETE；但 0005 回滾後再升級會把 `false_positive` 復活成黃金事故、context 全丟；0006 回滾把 NULL（不知道）改寫成「現在」 |
| 12 | `run_in_transaction` 重試污染 | **破防** | 平台現有的 `write()` 沒有污染輸入；但「COMMIT 已成功、回應途中斷線」時重跑 fn：只有異常點的那一輪寫成 6 筆（應 3）、有新事件群的那一輪整輪 `IntegrityError` |
| 13 | workflow 名稱路徑安全 | **合格** | 白名單 fullmatch 擋掉 `..`、`\n`、`\x00`、全形、`;`；Windows 檔案系統不分大小寫是唯一差異 |
| 14 | LLM 降級 | **合格（有前提）** | 內建供應商把所有錯誤包成 `LLMError`；外掛供應商丟 `RuntimeError`／`TimeoutError` 會漏出算子，靠 YAML 的 `optional: true` 兜住 |
| 15 | 常駐行程的跨輪／併發初始化 | **部分** | 每輪新建 `PipelineContext`、`StorageHooks._pool` 每輪清空；`get_engine` 無鎖（同第 8 題）、每次抓資料新建一個 `httpx.Client` 不關 |

合格 3、合格（有條件）2、部分 7、破防 4（第 3 題算破防）。

### 主幹上最該先補的三件事

1. **參數範圍搬到載入期（第 4 題）**。`Pipeline.__init__`（`engine/pipeline.py:160`）只透過 `OperatorRegistry.create` 檢查參數**名稱**；`validate_params`（`stages/detectors/base.py:79`）只在 `execute` 裡呼叫（`base.py:124-128`）。後果是節點 YAML 寫錯數值時服務照常啟動、心跳照常續命，**偵測其實全停**——這就是 CLAUDE.md「不准延到執行期」想擋的情況，而且比啟動失敗更難發現。`check_override` 已經示範了做法（`base.py:85-93` 在載入期把合併後的參數跑一次 `validate_params`），對節點自己的 `params` 也做一次就好。同一件事：`NodeSpec`、`Workflow` 補 `extra="forbid"`（`pipeline.py:73`、`:82`；`TargetSpec` 在 `:47` 已經有）。
2. **`run_in_transaction` 遇到 COMMIT 中斷要能判斷成敗（第 12 題）**。`anomalies` 沒有唯一鍵，重跑就是重複寫；有新事件群時又會被 `incidents.uid` 唯一鍵擋成整輪失敗。做法二選一：寫入前在同一筆交易插一筆「本輪 token」（唯一鍵），重試前先查它在不在；或幫 `anomalies` 加 (pipeline, entity, metric, ts, detector_node) 唯一鍵，改用 `ON CONFLICT DO NOTHING`。
3. **非有限數值在寫入邊界擋掉（第 4 題）**。ES／Prometheus 資料源已經濾掉 NaN／Inf（`sources/elasticsearch.py:61`、`sources/prometheus.py:64`），但 mock、trace 時序、未來的新資料源沒有；一旦漏進來，`json.dumps` 寫出 `Infinity`，PostgreSQL 的 json 拒收，`persist_batch` 整筆失敗（**所有健康序列一起不落地**）。在 `persist_batch` 前把非有限分數記成那條序列的錯誤並剔除，或在 `DetectorOperator.execute` 產生 `AnomalyEvent` 前就擋。

緊接著的第四件是第 2 題的 `rca_report` 互斥：一接上付費 LLM，重複點「產生報告」就是重複計費。

---

## 1. 分層隔離：`stages/rca` 能不能在沒有 storage／sources／FastAPI 下跑完

**結論**：能。RCA 核心演算法與 10 個報告段落在封鎖所有基礎設施 import 的行程裡用手刻物件跑完；唯一的越界是一條環境變數讀取。**部分合格**。

**證據**

探測：`scripts/probe/q1_rca_pure_memory.py`（pytest 包裝：`tests/test_invariants_probe_rca_isolation.py`）。在 `sys.meta_path` 最前面裝攔截器，`aiops.storage`／`aiops.sources`／`aiops.interfaces`／`aiops.control_plane`／`sqlalchemy`／`fastapi`／`starlette`／`httpx`／`psycopg`／`elasticsearch`／`requests` 一被 import 就 `ImportError`；清空全部 `AIOPS_*`；只 import `aiops.engine.*` 與 `aiops.stages.rca.*`；手刻 `SpanRecord`（front → cart-api → postgresql，事件前 15 分鐘 cart-api 自身 5 ms、事件中 200 ms，每分鐘 12 個請求）、`Incident`、`LogRecord`、`KnowledgeCase`、`PipelineContext`，依序跑 `rca.build_dynamic_topology` → `rca.root_cause.evidence` → `rca.report`（`rca_report.yaml` 的 10 段）。

```
candidates: [('cart-api', 1.0, ''), ('front', 0.0, '')]
root_note: (空)
sections built: ['summary', 'timeline', 'affected_entities', 'golden_signals', 'root_cause_candidates', 'propagation_path', 'trace_evidence', 'log_patterns', 'similar_cases', 'suggested_actions']
sections skipped: []
blocked import attempts: 無
aiops modules loaded: 27
  non-engine/non-rca: ['aiops.stages', 'aiops.stages.notification', 'aiops.stages.notification.links']
third-party loaded (subset): ['numpy', 'pandas', 'pydantic']
os.environ.get AIOPS_* reads during run: ['AIOPS_GRAFANA_URL']
RESULT: PASS
```
PR A 快照跑出一樣的結果（`--root <PR A>`）。

Import 矩陣：`scripts/probe/q1_import_matrix.py`，31 個模組各開一個乾淨子行程、同樣封鎖。

| 模組 | 單獨 import | 讀 `os.environ` | 跨出 engine／自己這層 |
| :--- | :---: | :---: | :--- |
| `stages/rca/` 全部 15 個（`base`、`spans`、`logs`、`report`、`topology_builder`、`algorithms/{causal,evidence,pagerank,topological}`、`sections/{actions,graph,log_patterns,overview,similar_cases}`） | OK | 無 | 無 |
| `stages/rca/sections/evidence.py` | OK | 無（間接有） | **`aiops.stages.notification.links`**（`sections/evidence.py:13`；`links.py:23` `os.environ.get("AIOPS_GRAFANA_URL")`） |
| `stages/detectors/` 全部 11 個 | OK | 無 | 無 |
| `stages/enrichment/{base,narrative,registry}` | OK | 無 | 無（`narrative.build_context` 在函式內 import `stages.rca.logs／spans`） |
| `stages/enrichment/providers.py` | **BROKEN**（被擋） | 有（`providers.py:109-111` `AIOPS_LLM_*`） | `httpx`（模組頂層） |
| `stages/enrichment/summarizer.py` | **BROKEN**（被擋） | 無 | 模組頂層 `from ...providers import build_llm`（`summarizer.py:10`）→ `httpx` |

沒有任何模組偷 import SQLAlchemy、storage、sources。

**風險與建議**
- `trace_evidence` 段落的輸出取決於部署環境變數：同一份 ctx 在兩台機器上會產出不同文字（有沒有 Explore 連結）。對可重現性與單測都是隱性輸入。建議 Grafana 基底網址跟 `GrafanaLinks` 一樣由引擎放進 ctx（`ctx.grafana_links` 已經存在，RCA 管線沒有填），段落只讀 ctx。
- `provider: noop` 執行期完全不連外（成立），但**載入期**仍需要 httpx 套件存在。沒有網路需求的離線部署也得裝 httpx。把 `build_llm` 的 import 移進 `execute` 的非 noop 分支即可。
- 純記憶體啟動有一個隱性前提：`rca.root_cause.evidence` 的 `__init__` 會 `OperatorRegistry.get(fallback)`（`algorithms/evidence.py` 建構子），呼叫端必須先 import `algorithms/topological.py`，否則 `WorkflowError`。正式環境由 `interfaces/runner.load_all` 掃整包，所以沒事；單獨使用演算法時是陷阱。

---

## 2. 併發防禦

**結論**：基準線與調參的樂觀鎖正確；經驗庫「兩步寫入」不是原子的；`rca_report` 沒有任何互斥或 CAS。**破防**。

**證據**（`scripts/probe/q2_concurrency_pg.py`，真 PostgreSQL 16，`threading.Barrier(10)` 同時放行）

```
[A] errors=0 context tag keys=10/10 series=[{'w': 3}] author=w3 resolution=res-3 root_cause=rc-3
[A] reader snapshots: {'no_row': 4, 'row_without_series': 8, 'complete': 22}
[A] rows for uid: 1
[B] 20 輪 TP×5 + FP×5 同時送，最後 verdict 分布：{'true_positive': 14, 'false_positive': 6}
[C] v0 insert winners=1 errors=0; v1 update winners=1 errors=0; final version=2 state={'w': 109}
[D] v0 winners=1 errors=0; v1 winners=1 errors=0; final version=2 params={'threshold': 19}
[E] 10 個 rca_report 同時跑同一個事件群：完成=10 errors=0 耗時=0.22s
[E] LLM complete() 總呼叫次數=20；generated_by={'template': 10} briefing_by={'template': 10}
[E] 資料表列數變化：無（RCA 報告不落地，只回給呼叫端）
[F] persist_batch 第 1 次嘗試寫完 3 筆異常點後斷線 -> 重試：Incidents.create 呼叫 2 次；anomalies=3（應 3）incidents=[2] ctx.incident_ids={'svc-f': 2}
[F2] fn 自己 pop 輸入清單：嘗試 2 次，最後寫入 1 筆（應 3）
```
（E 的 `template` 是因為假 LLM 回的 JSON 故意不合格式；重點是呼叫次數。A、B 的數字每次跑會變，形狀不變。）

逐項：

- **A. `Knowledge.upsert` + `merge_context`**（`control_plane/learning/tuning.py:263-264`，`FeedbackSubmitOp._golden`）。兩個呼叫各自開 `write_session` 各自 commit（`storage/crud.py:59` `upsert`、`:95` `merge_json`，都在 `db.write_session` 裡），**中間沒有包 `transaction()`**。
  - 好的部分：`ON CONFLICT DO UPDATE` 讓 10 個 worker 首次寫入 0 個 `IntegrityError`、只有 1 列；`context || patch` 讓 10 個 tag 鍵全部留下（chaos 報告場景 10 修過的就是這兩點，成立）。
  - 破的部分：讀者在 34 次讀取中 8 次看到「列已建好、`context` 還沒有 `series`」，4 次看不到列。黃金事故回測（`golden._series_of`，`golden.py:41`）在這個空檔讀到時會退回「從事件群快照找序列」；不會錯，但是不同的資料路徑。更大的洞是：`merge_context` 失敗（DB 斷線）時第一步已經 commit，留下沒有 `series` 的半套黃金事故，沒有重試也沒有回復。commit 訊息「atomic knowledge upsert+merge」只對**各自那一步**成立。
  - 純量欄位是最後 commit 的贏（`author=w3`、`resolution=res-3`），`series` 也是最後一個 merge 的贏（頂層鍵整個換掉）。兩個人同時確認同一事故、各寫不同處置時，其中一份處置默默消失。
- **B. verdict 沒有 CAS**。`true_positive` 路徑 `upsert(..., verdict="true_positive")`，`false_positive` 路徑 `upsert(verdict="false_positive")`（`tuning.py:248`），誰最後 commit 誰贏：20 輪裡 14 輪留下 TP、6 輪留下 FP。一筆被降級的事故可能因為同時有人按「確認」而又變回黃金事故，從此擋住調參提案。
- **C／D. 樂觀鎖**：`DetectorModels.save`（`storage/repository.py:157`）與 `ActiveConfigs.set`（`:192`）在 `expected_version=0` 走 `crud.insert_if_absent`（唯一鍵擋）、其他走 `crud.update_if_version`（`crud.py:135`，`WHERE version = :expected` 再 `version+1`）。10 搶 1，兩輪都恰好 1 個贏家、0 個例外。**合格**。
  - 小洞：`insert_if_absent`（`crud.py:127-132`）吞掉**所有** `IntegrityError`，不只唯一鍵衝突。`NOT NULL`／CHECK 違反也會變成 `None`，`persist_target` 把它記成 `conflict-kept-newer`（`storage/hooks.py:193-194`），真正的寫入 bug 會被當成正常的併發讓步。
- **E. `rca_report`**：`POST /api/v1/rca`（`interfaces/api/runs.py:15-17`）→ `runner.run` → 每次新建 `Pipeline`，沒有鎖、沒有 in-flight 表、沒有快取。RCA 報告**不落地**（10 次跑完資料表列數 0 變化；`persist_batch` 對載入進來的事件群直接略過，`hooks.py:203,211-212`），所以沒有「誰覆蓋誰」的問題。後果是成本與外部負載：每個 worker 各打一次 ES trace（最多 `max_spans: 20000`）、ES logs、`llm_summary` 一次、`llm_briefing` 一次。10 個重複請求 = **20 次 LLM 呼叫**、20 次 ES 大查詢；10 份報告也可能因為 ES 資料在跑的期間變動而內容不同。

**風險與建議**
- 經驗庫：把 `_golden` 的兩步包進 `db.run_in_transaction`（`write_session` 已經支援 ambient session），讀者就看不到半套；verdict 改成帶條件的 UPDATE（例如 `WHERE updated_at = :read_at`，或加 `version` 欄位比照 `detector_models`），衝突回 409。
- RCA：最便宜的做法是 `(incident_id, workflow)` 的 advisory lock（`pg_try_advisory_xact_lock`），拿不到就回 409「報告產生中」；或把報告結果存表並設短 TTL 快取。
- `insert_if_absent` 只接住唯一鍵衝突（檢查 `e.orig` 是不是 `UniqueViolation`），其他 `IntegrityError` 照丟。

另外看了 `storage/db.py:139` `run_in_transaction`：只在 `InfrastructureError` 且 `__cause__` 是 `OperationalError`／`InterfaceError` 時重試，間隔 0.5、1.5 秒，最多 2 次。F 證實 `persist_batch` 的 `write()`（`hooks.py:205-229`）重試時不會被第一次污染（`new_ids.clear()` 在 `:206`，`loaded` 在交易外先算好）；F2 證實這只是**呼叫端的紀律**：`fn` 一旦自己改輸入，重試不報錯、資料默默少了 2 筆。第 12 題接著看 COMMIT 中斷。

---

## 3. 時鐘偏移：`self_times`

**結論**：main 不看時間戳，偏移完全隱形；PR A 抓得到偏移，但「超出部分不扣」在純時鐘偏移時會**高估父 span 的自身耗時**，偏移幾毫秒就多算幾毫秒。**兩版各破一邊**。

**證據**（`scripts/probe/q3_clock_skew.py`，同一組手刻 span 跑兩個版本；`svc self/req` 是 `window_stats` 算出的「每請求自身耗時」，同一服務的父子 span 加總）

| 案例（父 span 時長） | main 父自身 | main 服務自身／請求 | PR A 父自身 | PR A 服務自身／請求 | PR A `clock_skew_spans` |
| :--- | ---: | ---: | ---: | ---: | :---: |
| 1 子早 100 ms 開始（−100..+50），父 200 | 50.0 | 200.0 | **150.0** | **300.0** | 1 |
| 2 子超出父結尾（100..400），父 200 | 0.0 | 300.0 | 100.0 | **400.0** | 1 |
| 3 子完全在父之後（300..350），父 200 | 150.0 | 200.0 | 200.0 | 250.0 | 1 |
| 4 子比父長、同起點（0..250），父 200 | 0.0 | 250.0 | 0.0 | 250.0 | 1 |
| 5 兩個並行 I/O 子（0..60 ×2），父 100 | **0.0** | 120.0 | 40.0 | 160.0 | 0 |
| 6 依序兩子（0..60、60..100），父 100 | 0.0 | 100.0 | 0.0 | 100.0 | 0 |
| 7 次毫秒偏移（−0.5..+49.5），父 200 | 150.0 | 200.0 | 150.0 | 200.0 | 0 |
| 8 早 100 ms ＋ 並行另一子（−100..+50、0..120），父 200 | 0.0 | 270.0 | 80.0 | 350.0 | 1 |
| 9 app 子與 I/O 子重疊（app 0..80、ext 50..150），父 200 | 20.0 | 200.0 | 20.0 | 200.0 | 0 |
| 10 子耗時是負的（−20 ms），父 200 | **220.0** | **220.0** | 200.0 | 200.0 | 0 |

- **main**（`stages/rca/spans.py:81-88`）：`自身 = 耗時 − Σ 子耗時`，再 `max(0)`。只看 `duration_ms`，不看 `ts`。所以時鐘偏移（案例 1、7）恰好算對，但並行（案例 5）把父扣成 0，負耗時（案例 10）讓父自身 220 ms 比父自己還長。偏移完全沒有任何訊號。
- **PR A**（快照 `stages/rca/spans.py:102-139` `self_times_detail`）：子區間先夾進父區間（`:121` `c_lo, c_hi = max(c_lo, 0.0), min(c_hi, hi)`），I/O 子取聯集，`app` 子加總（`:124`），超出 1 ms 容忍（`:94`）記成 `skewed`（`:120`）。並行（案例 5）修好了；負耗時被 `c_hi <= c_lo` 略過；`clock_skew_spans` 在案例 1、2、3、4、8 都正確計數，報告段落有數字才印（PR A 的 `sections/evidence.py` 改動）。
- **PR A 的副作用**：子 span 本身的自身耗時不受影響（葉節點 = 全部 `duration_ms`，`:112`），被夾掉的那段卻**沒有從父扣**。同一段時間被父、子各算一次：案例 1 服務自身／請求 300 ms，比入口請求總長 200 ms 還多 100 ms；案例 2 是 400 > 200。HARDENING_DESIGN 第 2 節說「超出的部分不算」，這是刻意的取捨，但它的後果是「**偏移越大，呼叫端看起來越像根因**」——跟 `evidence` 用自身耗時增量指認根因的邏輯正好衝突。

**風險與建議**
- 真實 11/3 資料子 span 都是依序的（HARDENING_DESIGN 第 1 節），所以兩版在正式資料上目前數字一樣；風險在跨主機時鐘不同步的環境（K8s 節點 NTP 漂移、瀏覽器 RUM span）。
- 建議 PR A 對「偏移」與「並行」分開處理：子區間超出父區間時，先**平移**到父區間內（保留長度、長度超過父就夾成父的長度），再取聯集；這樣案例 1 會回到 50 ms（跟 main 一樣），案例 5 維持 40 ms。`clock_skew_spans` 照記。
- 不論哪版，服務層 `self_ms_per_request` 都可能大於 `total_ms_mean`（案例 5 兩版都是）：子 span 的自身與父的自身是分開算的。報告若要並列這兩個數，要寫明。

---

## 4. 數值合約

**結論**：演算法層對 0／1／常數／NaN／Inf 大多不崩潰，但會產生不能落地的基準線與分數；參數範圍只在執行期檢查；YAML 只有 target 是嚴格模式。**破防**。

### 4a. 演算法層 `fit`／`get_state`／`detect`

`scripts/probe/q4_numeric_contract.py`：不經 `execute`，直接呼叫。偵測段 10 點，最後一點比訓練典型值 +1（小台階）；`json=NaN/Inf!` 代表 `get_state()` 不能嚴格 JSON 序列化（PostgreSQL 的 json／jsonb 收不下）。

| 偵測器 | 長度 0 | 長度 1 | 全常數 5（+1 台階） | 全 0（+1 台階） | 訓練含 NaN | 訓練含 +Inf |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `mad` main | state `nan/nan`，**json 不合法** | `mad=1e-06`，台階分數 **674500**，報 1 筆 | 同左 674500 | 同左 674500 | 正常（median 跳過 NaN）；偵測段 NaN 點分數 NaN、不報 | 正常；偵測段 Inf 點分數 inf、**報 1 筆** |
| `mad` PR A | 同 main | `mad=0` 存原值，`scale_floor` 整數底噪 1 → 分數 0.6745，不報 | 同左 | 同左 | 同 main | 同 main（Inf 點仍是 inf） |
| `zscore` main | `nan/nan`，json 不合法 | `std=0` → 分數**永遠 0**（死區） | 永遠 0 | 永遠 0 | 正常 | `mean=inf, std=nan`，**json 不合法**，偵測分數全 NaN |
| `zscore` PR A | 同 main | 底噪 1 → 分數 1.0（台階 ≥3 才報） | 同左 | 同左 | 正常 | 同 main |
| `hourly_baseline`（兩版） | **`fit` 丟 `ValueError: cannot convert float NaN to integer`**（`hourly_baseline.py:70` 的 `full_slot = max(int(...max()), 1)`，空 groupby 的 max 是 NaN） | main 分數 0（`g_std == 0` 時乘 0）／PR A 1.0 | 同左 | 同左 | 正常 | `global=[inf, nan]`，json 不合法，分數全 NaN |
| `quantile_drift`（window=5） | `nan/nan`，json 不合法 | main `scale=0.05`（`|center|×1%`）→ 台階 16 分、報／PR A `scale=1.0` → 0.8 不報 | 同左 | 兩版 `scale=1.0` | 正常 | 正常（rolling quantile 吸收單點 Inf） |
| `dod`（`compare`） | `fit` 丟 `BadRequest`（`compare.py` `d.empty`） | `scale=1.0` | 同左 | 同左 | 正常 | 正常（差值 inf−inf = NaN 被 dropna） |
| `peer` | `nan/nan`，json 不合法 | `mad=0` → `or 1.0` | 同左 | 同左 | 正常 | 正常 |
| `rules`（upper=5.5） | 不學習，`state={}` | 照判 | 照判 | 照判 | NaN 點不判 | Inf 點分數 **inf**、報 |

main 與 PR A 只在 mad／zscore／hourly／quantile_drift 的常數序列行為不同（PR A 的 `scale_floor`，快照 `stages/detectors/base.py:305-322`）：PR A 修掉了 main「MAD 遇到常數序列 1 個單位就 67 萬分」與「zscore 常數序列永遠 0 分」兩個極端。

### 4b. 這些輸入在正式路徑上會不會走到

- **長度 0／1**：`DetectorOperator.execute` 先擋 `ctx.raw_df.empty`（`base.py:116`），再擋 `training_points < min_train_points`（預設 60，`base.py:145-152`），所以正式偵測不會用 0、1 點訓練。**但控制面有兩條繞過**：
  - 提案回測 `ProposalBacktestOp`（`control_plane/learning/tuning.py:146-151`）只要求 `split >= 1`，然後 `_flags_in` 直接 `algo.fit(df.iloc[:begin])`（`tuning.py:83`）；漏報窗檢查只要求 `begin >= 2`（`tuning.py:176`）。main 的 MAD 用 1～2 點訓練時就是上表 674500 分的那一列：回測數字會嚴重失真。
  - 回放 `replay_window` 用的是常數 `DEFAULT_MIN_TRAIN_POINTS`（`replay.py:169`），不是節點 YAML 的 `min_train_points`。節點設了 200 的話，正式偵測與黃金事故回放的門檻不一致。
- **NaN／Inf**：ES 與 Prometheus 資料源在源頭濾掉（`sources/elasticsearch.py:61`、`sources/prometheus.py:64`），所以目前正式路徑碰不到。mock、`trace` 時序（`stages/ingestion/fetch.py:42` `_trace_frame`）、lag／peer 的 left join（產生 NaN，這是預期的）、未來的新資料源沒有這層。

一旦漏進來，`scripts/probe/q4_nonfinite_pg.py`（真 PostgreSQL）：
```
[1] DetectorModels.save(inf/nan state) RAISES sqlalchemy.exc.DataError: (psycopg.errors.InvalidTextRepresentation) invalid input syntax for type json
[2] persist_batch RAISES sqlalchemy.exc.DataError: (psycopg.errors.InvalidTextRepresentation) invalid input syntax for type json
    anomalies 寫入: []  incidents 寫入: 0
```
一條序列的 Inf 分數讓**同一輪所有序列**的異常點與事件群都寫不進去（`DataError` 不是 `OperationalError`，不重試；也不是 `PlatformError`，`Pipeline.run` 直接往上丟）。

### 4c. 參數寫錯是載入期還是執行期

`scripts/probe/q4_config_validation.py`：`ingestion.fetch`（mock）→ `detector.mad`，NullHooks、`strict=False`（排程模式）。

| 寫錯的地方 | main | PR A |
| :--- | :--- | :--- |
| 節點 `params.min_scale: -1` | 載入期 `WorkflowError`：main 沒有這個參數 | **執行期**：`errors=['svc-a: detector.mad.min_scale=-1 超出範圍 [0.0, 1e12]']`，`anomalies=0`，整輪「成功」 |
| 節點 `params.min_scale: 'abc'` | 載入期（參數不存在） | **執行期**：`could not convert string to float: 'abc'` |
| 節點 `params.min_scale: '5'`（字串數字） | 載入期（參數不存在） | 照跑，`float('5')` 當 5 用 |
| 節點 `params.min_scale_ratio: 2` | 載入期（參數不存在） | **執行期**：超出 `[0.0, 1.0]` |
| 節點 `params.threshold: -3` | **執行期**：`errors=['svc-a: detector.mad.threshold=-3 超出範圍 [1.0, 20.0]']` | 同左 |
| 節點 `params.threshold: 'abc'` | **執行期** | 同左 |
| target `overrides.mad.min_scale: -1` | 載入期（參數不存在） | **載入期** `WorkflowError`（`check_override`） |
| target `overrides.mad.threshold: -3` | **載入期** `WorkflowError` | 同左 |
| 節點拼錯 `optinal: true` | **默默忽略**，照跑（節點變成非 optional） | 同左 |
| workflow 拼錯 `max_failed_target_ration: 0.5` | **默默忽略**，照跑（斷流門檻維持 1.0） | 同左 |
| target 拼錯 `detector:`（少 s） | 載入期 `ValidationError`（`TargetSpec` 有 `extra="forbid"`） | 同左 |

原因：`Pipeline.__init__`（`engine/pipeline.py:160`）只做 `OperatorRegistry.create`，算子 `__init__` 只檢查參數**名稱**（`base.py:38-41`）；`validate_params`（`base.py:79-83`）只在 `execute` 第 126 行與 `check_override`（`base.py:93`，僅 target overrides）呼叫。`NodeSpec`（`pipeline.py:73`）、`Workflow`（`:82`）沒有 `model_config = ConfigDict(extra="forbid")`，只有 `TargetSpec`（`:47`）有。

**排程模式下「每條序列都記錯」不是斷流**：`BadRequest` 不是 `NoDataError`，不進 `starved`（`pipeline.py:283-286`），`max_failed_target_ratio` 不觸發，`after_persist`／`after_finalize`（含 `notifier.watchdog` 心跳）照常執行。服務看起來健康，偵測全停。

**風險與建議**：見「最該先補的三件事」第 1、3 點。另外 `hourly_baseline.fit` 在空資料丟的是 `ValueError` 而不是 `BadRequest`：正式路徑碰不到（前面已擋空資料），但回放、回測若哪天放寬門檻，會變成未分類的 500。

---

## 5. 實體安全：PR B 的 `unknown_identifier`／詞表 token 邊界

**結論**：token 邊界本身設計得對（中英夾雜、全形冒號、底線點、斜線端點、大小寫、連字號結尾都正確），破口在「ASCII 以外的字元」與「純數字」，誤拒在截斷與編碼。**部分合格**。

**證據**：PR B `stages/enrichment/narrative.py:87` `_IDENT = [A-Za-z0-9]+(?:[-_.:/@][A-Za-z0-9]+)*`，`:141` `unknown_identifier`（小寫比對，純數字交給 `_number_ok` `:214`，`101.4ms` 只查單位 `ms`），`:234` `validate_briefing`。合法詞 = context 鍵名＋值的所有 token 與「連接符切開的連續片段」（`tax-gw:443` → `tax`、`tax-gw`、`tax-gw:443`、`gw`、`gw:443`、`443`）∪ `GENERIC_LEXICON`。

`scripts/probe/q5_lexicon_tokens.py`：一份 cart 結帳鏈的 context（payment-gateway、`tax-gw:443`、`inventory_svc.v2`、`GET /api/v1/carts`、32 碼 trace id），拓撲已知實體只放白名單內的兩個（只測詞表）。

| 輸入（摘要第一行） | 抽到的 token | 結果 | 判定 |
| :--- | :--- | :--- | :--- |
| `請檢查payment-gateway的設定` | `payment-gateway` | pass | 對 |
| `請重啟kafka集群` | `kafka` | reject（kafka） | 對 |
| `payment-gateway 等 tax-gw：443 變慢`（全形冒號） | `tax-gw`、`443` | pass | 對（全形冒號切開，兩段都合法） |
| `… tax-gw：8443 …` | `8443` | reject（數字 8443 找不到） | 對 |
| `… tax-gw:480 …` | `tax-gw:480` | reject（識別字） | 對 |
| `inventory_svc.v2 大量 SocketTimeoutException` | `inventory_svc.v2`、`SocketTimeoutException` | pass | 對 |
| `inventory_svc.v3 …` | `inventory_svc.v3` | reject | 對 |
| `GET /api/v1/carts 504` | `GET`、`api/v1/carts`、`504` | pass | 對（開頭 `/` 不進 token） |
| `GET /api/v1/orders 504` | `api/v1/orders` | reject | 對 |
| `GET /api/v1/carts/../admin` | `api/v1/carts`、`admin` | reject（admin） | 對 |
| 完整 trace id | 32 碼 | pass | 對 |
| **trace id 截半（前 16 碼）** | `4bf92f3577b34da6` | **reject** | **誤拒**：片段只在連接符切，不收前綴 |
| `範例 trace 35773` | `35773`（純數字） | reject（數字找不到） | 對 |
| `k8s-node- 資源不足` | `k8s-node` | reject | 對（尾端 `-` 不進 token） |
| `Payment-Gateway 等 TAX-GW:443` | 原樣 | pass | 對（不分大小寫） |
| `Payment-Gateway 等 Kafka` | `Kafka` | reject | 對 |
| **`payment%2Dgateway`** | `payment`、`2Dgateway` | **reject** | **誤拒**（`%` 切斷） |
| **`GET %2Fapi%2Fv1%2Fcarts`** | `2Fapi`… | **reject** | **誤拒** |
| `請清 redis%2Ecache` | `redis`、`2Ecache` | reject | 對 |
| **`請重啟ｒｅｄｉｓ`（全形英文）** | **（無）** | **pass** | **逃逸**：`[A-Za-z]` 不含全形，整個字完全隱形 |
| `請重啟 rеdis`（西里爾 е） | `r`、`dis` | reject | 對（碎片碰巧不在詞表；換個碎片都在詞表的同形字就會過） |
| `連 10.0.0.5 逾時` | `10.0.0.5`（純數字） | reject（數字 10.0 找不到） | 對，但只靠數字檢查 |
| `回 ORA-12541` | `ORA-12541` | reject | 對 |
| `回錯誤碼 1045` | 純數字 | reject（數字找不到） | 對，但只靠數字檢查 |
| **`tax-gw 回錯誤碼 95`** | `95`（純數字） | **pass** | **逃逸**：95 恰好是 context 的 `wait_ms.during`，±10% 檢查不管語意 |
| `請重啟資料庫主機` | （無） | pass | 已知死區（PR B docstring 有寫） |
| `自身耗時 101.4ms` | `101.4ms` | pass | 對 |
| `101.4qx` | — | reject | 對 |
| `status_code 504` | `status_code` | pass | 對（鍵名入詞表） |
| `api_key 過期` | `api_key` | reject | 對 |

31 例 6 例不符（2 逃逸、3 誤拒、1 已知死區）。

**實體白名單用子字串比對（main 也有）**：`validate_briefing` 第 2 步 `if name and name in text`（PR B `narrative.py:252`；main `narrative.py:183`，`validate` 也一樣在 `:49`）。
```
FALSE-REJECT 拓撲有 tax-gw 服務、摘要寫白名單內的 tax-gw:443 -> 提到白名單以外的實體：tax-gw
FALSE-REJECT 拓撲有 cart 服務、摘要寫白名單內的 cart-api -> 提到白名單以外的實體：cart
```
服務名是別的服務名的前綴（`cart`／`cart-api`、`tax-gw`／`tax-gw:443`）就會誤拒。真實拓撲裡很常見（`order`／`order-svc`）。

**風險與建議**（不修）
- 全形英數：抽 token 前先做 NFKC 正規化（`unicodedata.normalize("NFKC", text)`），`ｒｅｄｉｓ` 會變成 `redis` 再被擋；同形字（西里爾、希臘）用「非 ASCII 字母一律視為可疑 token」處理。
- 純數字：目前「純數字 = 只做 ±10% 數值比對」對錯誤碼、port、PID 沒有語意保護。至少把「緊跟在 `碼`／`code`／`port`／`:` 後面的數字」當識別字比對，而不是數值。
- 誤拒成本低（退回模板，`briefing_generated_by` 照實標），逃逸成本高（人照著幻覺去重啟元件）。上面兩個逃逸應先處理；三個誤拒可以放著。
- 子字串白名單改成用同一個 `_IDENT` 抽 token 後整字比對。

---

## 6. ES 回 504／連線池爆滿時的錯誤隔離

**結論**：重試正確；但「一條序列的資料源 504」會讓整輪偵測失敗，連同健康序列的結果；這是刻意的設計，可是隔離邊界比 CLAUDE.md「序列隔離」那句讀起來窄。**部分合格**。

**證據**（`scripts/probe/q6_es_failure_isolation.py`：把 `httpx.Client.request` 換成假回應，走的仍是 `sources/_http.request` 的重試邏輯；targets = mock-a、es-bad、mock-c；`detector.rules` 每點都越線，有結果的序列一看就知道）

```
S1 一條 504×3                     整輪 RAISES InfrastructureError: elasticsearch 回應錯誤（HTTP 504）（已重試 2 次）：gateway timeout | ES requests=3 httpx.Client 建立=1
S2 一條 504×3、fetch 標 optional    整輪 RAISES InfrastructureError: elasticsearch 回應錯誤（HTTP 504）（已重試 2 次）：gateway timeout | ES requests=3
S3 一條 PoolTimeout               整輪 RAISES InfrastructureError: elasticsearch 連不上（已重試 2 次）：pool exhausted | ES requests=3
S4 一條 200 但格式不對              完成：有結果的序列=['mock-a', 'mock-c'] errors=[('es-bad', "elasticsearch 回應格式不對（index=metrics-x, value_field=v）：KeyError('key')")]
S5a 一條 200 但 0 筆（ratio=1.0）   完成：有結果的序列=['mock-a', 'mock-c'] errors=[('es-bad', 'elasticsearch 在這段時間沒有資料（或只有 NaN／Inf）：index=metrics-x')]
S5b 一條 200 但 0 筆（ratio=0.3）   整輪 RAISES InfrastructureError: 資料斷流：1/3 條序列抓不到資料（es-bad），達到 max_failed_target_ratio=0.3，這一輪不算成功
```

鏈路：
1. `sources/_http.py:29-53` `request`：`httpx.TransportError`（含 `PoolTimeout`、`ConnectTimeout`、`ReadTimeout`）與 429／502／503／504 重試 2 次（間隔 0.5、1.5 秒，`:19`）；401／403 直接 `InfrastructureError`；400／404／422 回給呼叫端；其他 5xx 不重試直接 `InfrastructureError`。
2. 偵測：`Pipeline.run` 的序列迴圈（`engine/pipeline.py:277-278`）`except InfrastructureError: raise`——**第一條碰到的序列就讓整輪失敗**，前面已經算好的序列結果不寫入（`persist_batch` 在迴圈之後）；但前面序列的**基準線已經寫了**（`persist_target` 在迴圈裡，`:276`）。
3. `optional: true`：`_run_node`（`pipeline.py:139-151`）對 `InfrastructureError` 一律往上丟，S2 證實。
4. `max_failed_target_ratio`（`pipeline.py:93`、`:301-306`）只計 `NoDataError` 與「200 但空」，S5b；504 不經過這裡。
5. RCA 旁路：`incident_series`／`incident_traces`／`incident_logs` 自己接住 `InfrastructureError` 寫進 errors（`stages/ingestion/fetch.py:192-197`、`:305`、`:372`），報告照出；`incident_traces` 失敗設 `ctx.rca_traces_failed`，`evidence` 不退回拓撲硬猜（`algorithms/evidence.py` `rank` 開頭）。這部分**合格**。

**風險與建議**
- 504 有兩種：ES 整體過載（真的是基礎設施）與「某條查詢太重、閘道逾時」（是那條序列的問題）。目前兩者無法區分，一條重查詢就能讓 50 條健康序列一起沒結果。真實逾時下的最壞耗時：ES client timeout 15 秒（`sources/elasticsearch.py:26`）× 3 次 + 2 秒 ≈ 47 秒，然後整輪丟掉。建議把「同一個資料源連續 N 條序列 `InfrastructureError`」才升級成整輪失敗，單條的記成序列錯誤（RCA 的 `incident_series` 已經是這個模式：同一資料源失敗一次後略過其餘）。
- 每次 `fetch_series` 都 `get_datasource_spec(...).from_env()`（`sources/__init__.py:30-33`）→ 新建一個 `httpx.Client`（`elasticsearch.py:29`、`prometheus.py:29`），從不 `close()`。每條序列 1～N 個 client（訓練窗、每個 lag、每個 peer 各一次）。不會出現 httpx 內部的連線池爆滿（每個 client 自己一個池），但沒有 keep-alive 重用（每次重新 TLS 握手）、socket 等 GC 才釋放。S1～S5 每輪都只看到 1 個 client，是因為只有 1 條 ES 序列。

---

## 7. 換成純 OTel Collector 索引要改幾個檔

**結論**：改動集中在資料源一層；`stages` 吃的是 dict 合約（`trace_id`、`span_id`、`parent_id`、`service`、`ts`、`duration_ms`、`error`、`kind`、`subtype`、`resource`、`status_code`、`name`），不用動。**合格**。

**必改（1 個檔）**：`aiops/sources/elasticsearch.py`
- `_span()`（`:276-296`）：APM 的 `span.id`／`transaction.id`／`parent.id`／`trace.id`／`service.name`／`span.duration.us`／`event.outcome`／`span.type`／`span.destination.service.resource`／`http.response.status_code`／`transaction.name` → OTel 的 `span_id`／`parent_span_id`／`trace_id`／`resource.attributes.service.name`／`duration`（奈秒）／`status.code`／`kind`（SERVER／CLIENT／INTERNAL）＋`attributes.db.system`、`attributes.server.address` 等。**要在這裡把 OTel 的 kind＋semantic attributes 換算成平台的 `kind`（`""`=入口、`app`、`db`、`external`、`messaging`…）**，`stages/rca/spans.py` 的 `_APP_KINDS`（`:32`）與 `is_resource_leaf`（`:66-71`）才不用改。
- `SPAN_FIELDS`、`SPAN_SORT`、`DEFAULT_TRACE_INDEX`（`:224-231`）、`_window_filters` 的 `service.name` term（`:249`）、`_sampled_spans` 的 `trace.id`（`:130`、`:152`）。
- 日誌：`fetch_logs`（`:168` 起）的 `processor.event = error` 過濾、`LOG_FIELDS`／`LOG_SORT`（`:233-235`）、`_log`（`:262-272`，OTel 是 `body`／`severity_text`／`attributes.exception.*`）、`DEFAULT_LOG_INDEX`（`:225`）。

建議做成新檔 `sources/otel_es.py`（繼承 `ElasticsearchSpec` 覆寫上面幾個函式）＋`sources/__init__.py:15` `_SPECS` 加一行，等於 2 個檔。

**設定（2～3 個檔）**：`pipelines/detection/it_*.yaml` 的 ES 指標查詢（`index`、`value_field`）、`pipelines/rca/rca_report*.yaml` 的 `source_type` 與 `section_params.trace_evidence.traces_datasource`、環境變數 `AIOPS_ES_TRACE_INDEX`／`AIOPS_ES_LOG_INDEX`。

**不用改**：`stages/ingestion/fetch.py`（`_record` `:245-250` 只讀 dict 合約）、`stages/rca/*`、`engine/*`、`storage/*`。

**周邊**：`scripts/synth_generator/*`、`scripts/chaos/es.py` 寫的是 APM 格式；`tests/` 有 16 個檔提到 `elasticsearch`／APM 欄位，fixture 要另做一份 OTel 版。

坑：OTel 的 `duration` 是奈秒、APM 是微秒；`status.code` 是 `"Error"`／`"Ok"`／`"Unset"` 字串；OTel 沒有 APM 的 transaction／span 文件區分，入口判斷要靠 `parent_span_id` 為空或 `kind = SERVER`。

---

## 8. 模組層級可變狀態

**結論**：沒有會跨輪殘留的模組或算子狀態；有一個 lazy 初始化沒上鎖。**合格（有小洞）**。

**證據**：`tests/test_architecture.py:266` 已經檢查「模組層級／類別層級的可變物件在執行期被原地修改」。我另外跑了兩件事：

1. grep：`global` 只有 `stages/enrichment/registry.py:47`（`_scanned`，import 期掃描一次）與 `storage/db.py:45`、`:56`（`_url`、`_engine`、`_factory`）；`_cache`／`lru_cache`／`functools.cache` 0 筆。模組層級的 dict／list：`sources/elasticsearch.py:226-235`（欄位清單常數）、`sources/__init__.py:15` `_SPECS`、`stages/enrichment/registry.py:29` `_ENTRIES`、`engine/registry.py:10` `OperatorRegistry._items`、`stages/rca/sections/actions.py:28` `SIGNAL_TOKENS`、`stages/notification/payload.py:22-24`、`sources/query/*` 常數；執行期都只讀（`_ENTRIES`、`_items` 只在 import 期註冊）。
2. AST 掃描（`scripts/probe/q8_mutable_state.py`）：架構測試沒有看「算子實例自己的屬性」。算子在 `Pipeline.__init__` 建一次，所以 `execute` 裡寫 `self.x` 就是跨輪狀態。結果：

```
A. 非 __init__／fit／load_state 方法裡改 self 屬性：
   aiops/engine/context.py:219 OpenIncident.remember -> self.previous
   aiops/engine/context.py:220 OpenIncident.remember -> self.changed
   aiops/engine/context.py:333 PipelineContext.log -> self.audit
   aiops/storage/hooks.py:166 StorageHooks._exclusion_pool -> self._pool / self._pool_key
   aiops/storage/hooks.py:64 StorageHooks.load_batch -> self._pool / self._pool_key
```
`OpenIncident`、`PipelineContext` 每輪新建；`StorageHooks._pool` 在每輪 `load_batch` 開頭清空（`hooks.py:64`）。**沒有任何 `stages/`／`control_plane/` 算子在 `execute` 裡改自己**。常駐 loop（`interfaces/cli/__main__.py:67`）每輪呼叫 `run_once` → `runner.build_pipeline` 重建 Pipeline。

小洞：`db.get_engine`（`storage/db.py:55-66`）先檢查 `_engine is None` 再建，沒有鎖。見第 15 題的實測。

---

## 9. 回溯窗含頭含尾的統一性

**結論**：回放一律 `[start, end]`；偵測與提案回測照資料源的原生語意，ES 少一點。**部分合格**。

**證據**（`scripts/probe/q9_window_bounds.py`：假 ES 照 `range gte／lt` 回、假 Prometheus 照 `query_range` 兩端都含回，mock 用真的 `MockSpec`）

```
── mock
   (a) 偵測 fetch(now=10:00:30, 60 分)  :  61 點 09:00:00 … 10:00:00
   (b) replay.fetch_span(09:00, 10:00) :  61 點 09:00:00 … 10:00:00
   (c) backtest fetch_target_frame     :  61 點 09:00:00 … 10:00:00
── elasticsearch
   (a) 偵測 fetch(now=10:00:30, 60 分)  :  60 點 09:00:00 … 09:59:00
   (b) replay.fetch_span(09:00, 10:00) :  61 點 09:00:00 … 10:00:00
   (c) backtest fetch_target_frame     :  60 點 09:00:00 … 09:59:00
   送出 es: ['gte 09:00:00 lt 10:00:00', 'gte 09:00:00 lt 10:01:00', 'gte 09:00:00 lt 10:00:00']
── prometheus
   (a) 偵測 fetch(now=10:00:30, 60 分)  :  61 點 09:00:00 … 10:00:00
   (b) replay.fetch_span(09:00, 10:00) :  61 點 09:00:00 … 10:00:00
   (c) backtest fetch_target_frame     :  61 點 09:00:00 … 10:00:00
   送出 prom: ['start 09:00:00 end 10:00:00（兩端都含）', 'start 09:00:00 end 10:01:00（兩端都含）', 'start 09:00:00 end 10:00:00（兩端都含）']
```

| 路徑 | 函式 | 語意 |
| :--- | :--- | :--- |
| 偵測 | `FetchOperator.execute`（`stages/ingestion/fetch.py:141-149`）→ `fetch_target_frame`（`:90`）→ 各 Spec | end 先對齊到 step（`:143`），再交給資料源：ES `[s, e)`（`sources/query/elasticsearch.py:71` `gte`／`lt`）、Prometheus `[s, e]`、mock `[start, end]` 不對齊（`sources/mock.py:51` `pd.date_range`） |
| 對齊 | `align_window`（`sources/query/window.py:9-18`） | 起訖都往下取整；mock 不呼叫它 |
| 回放 | `replay.fetch_span`（`control_plane/learning/replay.py:98-124`） | 多抓一個 step 再切 `ts <= end`（`:110`、`:123`）：**三個資料源一致 `[start, end]`** |
| 提案回測 | `ProposalBacktestOp.execute`（`tuning.py:145`）直接 `fetch_target_frame` | 跟偵測一樣照資料源走；**沒有切段**（回放有 `CHUNK_POINTS=10000`，`replay.py:35`） |

不一致的後果：
- 同一個黃金事故時窗，防護罩（走回放）看得到 end 那一點，回測主結果（走 `fetch_target_frame`）在 ES 上看不到。只差一點，但「最後一分鐘才越線」的事故會出現「防護罩說抓得到、回測說沒抓」。
- 偵測在 ES 上不含當下這一桶（未完成的桶，這是好事），Prometheus 上含。同一個指標換資料源，`lookback_minutes` 的實際點數差一。
- `replay.py:103` 的註解寫「Prometheus 對齊後也不含 end 那個桶」，與 Prometheus `query_range` 的語意（兩端都含）不符；因為後面有 `<= last` 切掉，結果剛好正確，但註解會誤導下一個人。
- 提案回測不切段：Prometheus 單次上限約 11000 點，`time_range` 拉超過 7.6 天（step 60 秒）就會被資料源回 400。

---

## 10. `incident_knowledge.context` 沒有版本欄位

**結論**：讀取端只有一個（`golden._series_of`），對缺鍵、多鍵容忍；對型別不對的資料直接丟例外。PostgreSQL 的 `||` 對非物件默默給出錯誤結果。**部分合格**。

**誰讀 context**：`control_plane/learning/golden.py:41-48` `_series_of` 讀 `context.series`。`similar_cases`（`stages/rca/sections/similar_cases.py`）讀的是 `KnowledgeCase` 的欄位（`entities`、`metrics`、`signals`、`resolution`、`root_cause`，由 `hooks.py:110-112` 從欄位組），**不讀 context**；`narrative.build_context` 完全不讀經驗庫。寫入只有 `tuning.py:264` `merge_context({**req.context, "series": [...]})`：使用者帶的 `context` 先展開、平台的 `series` 後寫，使用者蓋不掉 `series`（`engine/requests.py:208`，`dict[str, Any]`）。

**證據**（`scripts/probe/q10_knowledge_context.py`）
```
沒有 context 鍵                               -> []
context = None                             -> []
context = {}                               -> []
series 正常                                  -> [('cart', 'mock', 'cpu')]
series 項目多鍵（v2 加欄位）                        -> [('cart', 'mock', 'cpu')]
series 項目缺 query                           -> RAISES KeyError: 'query'
series 項目缺 entity                          -> []
series 是 dict（不是 list）                     -> RAISES AttributeError: 'str' object has no attribute 'get'
series 是 None                              -> RAISES TypeError: 'NoneType' object is not iterable
context 是 list（jsonb 陣列 || 物件 的結果）         -> RAISES AttributeError: 'list' object has no attribute 'get'
context 多了未知頂層鍵                            -> [('cart', 'mock', 'cpu')]
PG: '[1]'::jsonb || '{"series": []}'::jsonb = [1, {"series": []}]
PG: 'null'::jsonb || '{"series": []}'::jsonb = [null, {"series": []}]
PG: SQL NULL || '{"series": []}'::jsonb = None
```

- `_series_of` 用 `s.get("entity")` 過濾、再用 `s["source_type"]`、`s["query"]` 取值：缺 entity 會靜默略過，缺 query 會 `KeyError`。這個例外發生在 `admin.proposal_backtest` 的防護罩裡，**一筆壞掉的黃金事故會讓所有涉及那個實體的提案都回測不了**。
- `crud.merge_json` 在 PostgreSQL 用 `context || patch`（`crud.py:109`）。`||` 只有兩邊都是物件時才是合併：欄位內容若是 JSON `null` 或陣列，結果變成陣列；若是 SQL `NULL`，結果是 `NULL`，**`rowcount` 仍是 1，不報錯，patch 默默丟掉**。0005 把欄位設成 `NOT NULL DEFAULT '{}'`（`migrations/postgres/0005_knowledge_golden_incidents.sql:31`），所以 SQL NULL 只在程式自動建表、且有人顯式寫 `None` 時會發生；JSON `null` 可以經 `Knowledge.upsert(uid, context=None)` 寫進去（SQLAlchemy `JSON` 預設把 `None` 存成 JSON `null`）。目前沒有程式這樣呼叫，但 `upsert` 的 docstring 只說「不要整包覆蓋」，沒擋。

**建議**：`_series_of` 每個項目 `try` 取三個鍵，壞的記 log 略過；`merge_json` 的 SQL 改成 `COALESCE(CASE WHEN jsonb_typeof(context) = 'object' THEN context END, '{}'::jsonb) || patch`；context 加一個頂層 `"v": 1`，以後改格式時讀取端才有依據。

---

## 11. migration 回滾腳本會不會刪資料

**結論**：兩支回滾都沒有 `CASCADE`、沒有 `DELETE`；但 0005 回滾是**不可逆的資料銷毀**，而且「回滾後再升級」會把資料改錯。0006 回滾會造假時間戳。**部分合格**。

**證據**（`scripts/probe/q11_migration_rollback.py`：把檔頭註解裡的回滾 SQL 原樣取出，在真 PostgreSQL 上跑）
```
[0005] 回滾前: [('inc-fp', 'false_positive', '{"tags": ["deploy"], "series": [...]}'), ('inc-tp', 'true_positive', '{"trace_id": "abc"}')]
[0005] 回滾 SQL 有 CASCADE？ False ；有 DELETE？ False
[0005] 回滾後欄位: ['id', 'incident_uid', 'pipeline', 'entities', 'metrics', 'signals', 'resolution', 'root_cause', 'author', 'created_at']
[0005] 回滾後列: [('inc-fp', ''), ('inc-tp', '擴 pool')]
[0005] 再升級後: [('inc-fp', 'true_positive', '{}'), ('inc-tp', 'true_positive', '{}')]
[0006] 回滾 SQL 有 CASCADE？ False ；有 DELETE？ False
[0006] 回滾後: [('a', 'b', '2026-10-10 14:36:57'), ('c', 'd', '2026-11-09 14:36:57')] （c→d 原本是 NULL = 不知道）
```

- **0005**（`migrations/postgres/0005_knowledge_golden_incidents.sql:13-20`）：`DROP COLUMN verdict, severity, window_start, window_end, context, updated_at`。列不會少，但這六欄的資料永久消失：被降級的 `false_positive` 判定、所有 `context`（tags、trace id、黃金事故的 `series`）、時窗。
  - **回滾後再升級（換版失敗、退回、再試一次的標準流程）**：`verdict` 以 `DEFAULT 'true_positive'` 回填（`:27`），`inc-fp` 這筆被維運降級過的誤報**變回黃金事故**，此後擋住所有讓它「抓不到」的調參提案；`window_*` 從 `incidents` 快照回填（`:49-54`），事件群已刪的就空著。
  - `resolution` 的 `SET DEFAULT ''`（`:35`）沒有在回滾裡還原。舊版程式若依賴 `resolution NOT NULL` 且沒有預設值，會寫入失敗（舊版行為我沒驗證，只指出回滾不對稱）。
  - 有說「只在要退回舊版程式時才需要；舊版程式碰到多出來的欄位也能正常跑」——意思是**通常不該跑這段**。但註解沒有警告「跑了會毀掉 verdict 與 context，且再升級會把誤報復活」。
- **0006**（PR C `migrations/postgres/0006_service_edges_last_seen.sql` 檔頭）：`UPDATE service_edges SET last_seen = now() WHERE last_seen IS NULL` 再 `SET NOT NULL`。NULL 的語意是「不知道多久沒出現，prune 不刪」，回滾後變成「剛剛才看到」：退回舊版沒影響（舊版不用 `last_seen`），但**再升級後** `admin.topology_refresh` 的 prune（PR C `storage/repository.py:255`）會在 `prune_days` 之後才刪這些邊，等於把「不確定」重置成「新鮮」。不刪資料，影響輕。

**建議**：0005 回滾前先 `CREATE TABLE incident_knowledge_backup_0005 AS SELECT id, verdict, severity, window_start, window_end, context, updated_at FROM incident_knowledge`，再升級時從備份還原；註解明寫「跑了會銷毀什麼」。

---

## 12. `run_in_transaction` 重試時 fn 的輸入會不會被第一次污染

**結論**：平台現有的兩個 `fn` 都沒有污染輸入（第 2 題 F）；但重試的觸發條件包含「COMMIT 其實已經成功」的情況，重跑會重複寫。**破防**。

**證據**
- 呼叫端只有兩個：`StorageHooks.persist_batch`（`hooks.py:231`）與 `finalize_batch`（`:255`）。`persist_batch` 的 `write()` 每次開頭 `new_ids.clear()`（`:206`），`loaded` 在交易外先算（`:202`）；`finalize_batch` 的 `sent` 在交易外算（`:237-240`），`write()` 只讀。這兩個是對的。
- 第 2 題 F2：自己寫一個會 `pop` 輸入的 fn，第一次失敗、重試成功，最後只寫了 1 筆（應 3），**沒有任何錯誤**。`run_in_transaction` 的 docstring（`db.py:140-141`）有警告，沒有機制擋。
- **COMMIT 中斷**（`scripts/probe/q12_commit_ambiguity.py`，真 PostgreSQL；`Session.commit` 第一次先真的 commit 再丟 `OperationalError`，模擬「COMMIT 已送出、回應途中連線斷了」）：
```
[Q12] A 新事件群＋3 筆異常點：commit 呼叫 1 次；persist_batch RAISES IntegrityError: (psycopg.errors.UniqueViolation) duplicate key value violates unique constraint "incidents_uid_key"
[Q12]   資料庫實際：anomalies=3（應 3）incidents=1（應 1）ctx.incident_ids={}
[Q12] B 只有 3 筆異常點（被進行中事件群吸收，沒有新事件群）：commit 呼叫 2 次；persist_batch 回報成功
[Q12]   資料庫實際：anomalies=6（應 3）incidents=0（應 0）ctx.incident_ids={}
```
  - A：資料其實寫進去了，但重試時撞到 `incidents.uid` 唯一鍵，`IntegrityError` 不重試、往上丟，**整輪失敗、通知不送**。事件群已經是 open，下一輪 `alert.heartbeat` 會補送，所以最後會自癒，晚一輪。
  - B：`anomalies` 沒有唯一鍵（`storage/models.py:82` `AnomalyRow`），重試**成功**寫了第二份：6 筆。`detector.new_only` 之後用 `max(ts)` 判斷，不會再多，但成效報表、誤報分位數反推（`control_plane/learning/threshold.py`）、黃金事故回放都會把這段時間的異常算兩次。
- PostgreSQL 的 `SerializationFailure`／`DeadlockDetected` 在 psycopg 屬於 `OperationalError` 子類，同樣會重試；那種情況伺服器端確實回復了，重試是對的。問題只在 COMMIT 這一步。

**建議**：見「最該先補的三件事」第 2 點。

---

## 13. workflow 名稱的路徑安全（補充題）

**結論**：合格。

**證據**：`interfaces/runner.py:39-71` `workflow_path`：`[A-Za-z0-9_-]+` fullmatch（`:24-26`）→ `rglob` 唯一 → 拒絕軟連結 → `resolve().is_relative_to(WORKFLOW_DIR)`。`scripts/probe/q13_15_misc.py`：

```
'../rca/rca_report' / 'rca_report.yaml' / 'rca_report\n' / 'rca_report\x00' / '' / '*' / 'ｒｃａ_report' / 'C:\\Windows\\win' / 'rca_report;rm'
  -> 全部 BadRequest：不合法的 workflow 名稱
'RCA_REPORT' -> rca/RCA_REPORT.yaml   （Windows 檔案系統不分大小寫，rglob 找得到）
'a'×129      -> NotFound（API 端 RcaRequest.workflow 有 max_length=128，engine/requests.py:46）
```
唯一差異：Windows 上大小寫不同的名稱也能載入，Linux 容器上不行；`rglob` 在 Windows 上也看不到「`Foo.yaml` 與 `foo.yaml` 重複」。正式環境是 Linux，影響只在開發機。

---

## 14. LLM 降級（補充題）

**結論**：內建供應商合格；外掛供應商的「失敗丟 `LLMError`」只是約定，違反時靠 YAML 的 `optional: true` 兜住。**合格（有前提）**。

**證據**（`scripts/probe/q13_15_misc.py`，假供應商 `weird` 回各種東西）
```
reply='not json' / '[]' / 'null' / '{"summary": 5, "claims": "x"}' / '{"summary": "x", "claims": [{"text": 1}]}' /
      '{"summary_line": null}' / '{"summary_line": ["a"], ...}'
  -> LLMSummarizer: template（原因） | LLMBriefing: template（原因）   全部退回模板
reply=RAISE_RUNTIME  -> LLMSummarizer: 漏出 RuntimeError | LLMBriefing: 漏出 RuntimeError
reply=RAISE_TIMEOUT  -> LLMSummarizer: 漏出 TimeoutError | LLMBriefing: 漏出 TimeoutError
```
- `summarizer.py:38`、`:80` 只接 `LLMError`。內建 OpenAI 相容／Anthropic 把 `httpx.HTTPError`、非 2xx、非 JSON、缺欄位全部轉成 `LLMError`（`providers.py:12-22`、`:51-54`、`:76-78`），所以內建路徑不會漏。
- 外掛供應商（CLAUDE.md 第 3 節範例二的擴充方式）丟其他例外會漏出算子。`pipelines/rca/rca_report.yaml:35`、`:40` 與 `rca_report_correlation.yaml:37`、`:41` 都標了 `optional: true`，所以 RCA 報告照出（`_run_node` 接住，`pipeline.py:147-151`）；新寫的 YAML 若忘了標，就會讓整份 RCA 失敗。
- 漏出時 `ctx.llm_summary` 已經是模板（`summarizer.py:30-32` 在呼叫 LLM 前先填），所以結果仍然可用。

**建議**：兩個算子的 `except LLMError` 改成 `except Exception`，把型別寫進 `llm_rejected`。這跟架構測試「例外不准默默吞掉」（`tests/test_architecture.py:317`）不衝突：它有記錄原因。

---

## 15. 常駐行程的跨輪狀態與併發初始化（補充題）

**結論**：跨輪狀態合格（第 8 題）；併發初始化與 HTTP client 生命週期沒管。**部分合格**。

**證據**
- 跨輪：每輪 `Pipeline.run` 新建 `PipelineContext`（`pipeline.py:251`、`:263`），旗標（`incidents_built` 等）掛在 ctx 上；`tests/test_incident_lifecycle.py::test_state_does_not_leak_between_rounds_of_a_long_running_loop`（CLAUDE.md 第 100 節第 3 條）。
- `get_engine` 併發（`scripts/probe/q13_15_misc.py`，真 PostgreSQL，`create_engine` 包一層 sleep 50 ms 放大競態）：
```
create_engine 被呼叫 10 次；10 個執行緒拿到 10 個不同的 Engine；最後留在模組上的是其中 1 個，其他 9 個的連線池沒有人 dispose
```
  FastAPI 的同步端點跑在執行緒池裡，服務剛啟動時同時進來的請求就會撞上。每個 Engine 預設 pool_size 5＋overflow 10，9 個孤兒 Engine 最多佔住 135 條 PostgreSQL 連線，直到 GC。正式環境的 `init_infrastructure()`（`interfaces/cli/__main__.py:60`）若在開放請求前先呼叫過一次 `get_engine`（例如 `create_all`），這個競態就不會發生。我只確認了 CLI 路徑，API 啟動路徑沒驗證。
- HTTP client：見第 6 題，每次抓資料新建 `httpx.Client` 不關。

**建議**：`get_engine` 加一個 `threading.Lock`（double-checked）；資料源 Spec 改成「每輪建一次、輪末 `close()`」，或在 `get_datasource_spec` 快取一個 client（要注意 fork 後的行程不能共用）。

---

## 附錄：重跑方式

全部在 worktree 根目錄執行；`<PR A>`、`<PR B>`、`<PR C>` 是分支原始碼根目錄（PR A 目前要從 `hardening-a-noise-floor-edb621` worktree 複製 `aiops/`；B、C 用 `git archive <分支> aiops migrations | tar -x -C <目錄>`）。需要 PostgreSQL 的腳本會 `drop_all`，只能指向拋棄式容器。

```bash
nerdctl run -d --name pgarch -e POSTGRES_PASSWORD=x -e POSTGRES_DB=aiops -p 55437:5432 postgres:16
```
```bash
export PYTHONIOENCODING=utf-8 PROBE_PG_URL=postgresql+psycopg://postgres:x@127.0.0.1:55437/aiops
```

| 題 | 指令 |
| :-- | :--- |
| 1 | `python scripts/probe/q1_rca_pure_memory.py [--root <PR A>]`；`python scripts/probe/q1_import_matrix.py`；`pytest tests/test_invariants_probe_rca_isolation.py` |
| 2 | `python scripts/probe/q2_concurrency_pg.py`（PG） |
| 3 | `python scripts/probe/q3_clock_skew.py`；`python scripts/probe/q3_clock_skew.py --root <PR A>` |
| 4 | `python scripts/probe/q4_numeric_contract.py [--root <PR A>]`；`python scripts/probe/q4_config_validation.py [--root <PR A>]`；`python scripts/probe/q4_nonfinite_pg.py`（PG） |
| 5 | `python scripts/probe/q5_lexicon_tokens.py --root <PR B>` |
| 6 | `python scripts/probe/q6_es_failure_isolation.py` |
| 8 | `python scripts/probe/q8_mutable_state.py` |
| 9 | `python scripts/probe/q9_window_bounds.py` |
| 10 | `python scripts/probe/q10_knowledge_context.py`（PG 那段可選） |
| 11 | `python scripts/probe/q11_migration_rollback.py --prc <PR C>`（PG，會 `DROP SCHEMA public CASCADE`） |
| 12 | `python scripts/probe/q12_commit_ambiguity.py`（PG） |
| 13～15 | `python scripts/probe/q13_15_misc.py`（第 15 題需要 PG） |

```bash
nerdctl rm -f pgarch
```

第 7 題是讀碼題，沒有探測腳本。
