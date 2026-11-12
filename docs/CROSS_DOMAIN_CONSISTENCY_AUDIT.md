# 跨領域重疊與矛盾體檢（IT 微服務 × 工廠產線）

盤點日期 2026-11-12，對象 main `bbffc05`（tag 1.2）。只做診斷，沒有改程式。
每一條都對照過程式碼；「怎麼確認」欄寫的是讀到的位置，沒有實跑（資料庫目前起不來）。

## 處理進度（2026-11-12 定案後）

| 項目 | 狀態 |
| :--- | :--- |
| H1 預設引擎 | **已改**：依偵測管線分流。管線 YAML 的 `rca_workflow` 決定，`it_otel`／`it_apm` → `rca_report_llm`，`factory`／`metrics_only` → `rca_report`；呼叫端指定 `workflow` 時仍以指定的為準（`interfaces/runner.rca_workflow_for`，`tests/test_rca_routing.py`） |
| H2 工廠黑洞 | **已擋**：沒有 trace 的管線不會被分流到新引擎。新引擎沒有事實也沒有證據時改標 `undetermined`、原因代碼 `no_evidence`（以前是狀態 `ok`） |
| H2 餘項 | 未做：新引擎掛 `incident_series`、把 Lot 流向送進 LLM 的圖。做完之前工廠不開放 llm |
| H3 邊型別 | **部分完成**（2026-11-12，`refactor/service-graph-model`）：`ServiceGraph` 已建立，邊帶型別（CALL／ASYNC／FLOW）、方向由型別決定、算子統一用 `ctx.graph`，行為不變。尚未做：SUPPLY 型別、佇列邊進傳播圖（M3）、圖分管線（M4）。原規格：列為 `ServiceGraph` 重構（`refactor/service-graph-model`）的必備規格——每條邊帶語意型別（呼叫 CALL／佇列 ASYNC／Lot 流向 FLOW／物理供應 SUPPLY），傳播方向由型別決定，不靠人寫對順序 |
| M4 依賴圖分範圍 | **部分完成**（`feat/environment-scope`）：改成「依環境」而不是「依管線」分——workflow 選填 `environment`，三張圖表加 `environment` 欄位並納入唯一鍵（0009），hooks 只載同環境的邊，RCA 照事件群快照記的環境載圖。同一個環境的 IT 與工廠管線仍共用一張圖（名稱不撞就沒事，見下方 M4 原文）。尚未做：通知去重的指紋與最近通知不分管線／環境；`trace_series` 不分環境；撈 span 不依 `deployment.environment` 過濾（等 `sources` 重寫合流） |
| M1–M3、M5 | 未做：待辦，等資料庫恢復後量測或併入重構 |

注意：分流後 IT 事故預設走 LLM。部署環境沒有設定 LLM 供應商時，IT 的 RCA 報告會照出但標「無法判定（llm_not_configured）」，不會自動改用舊引擎；要舊引擎的結果得在請求帶 `workflow: rca_report`。以下是定案前的原始盤點。

## 結論先講

| 等級 | 數量 | 重點 |
| :--- | :--- | :--- |
| 確定存在的衝突（高風險） | 3 | 根因 API 預設仍是舊引擎；新引擎在工廠／沒有 trace 時一定「證據不足」，而且擋住了 `topological`；依賴圖沒有「這條邊是什麼語意」的欄位 |
| 功能重疊可簡化（中風險） | 5 | 同根因抑制只在 10–30 分鐘的縫隙裡起作用；`p0_boost` 在主線是空轉；佇列邊在無 trace 的根因推測裡看不到；依賴圖與通知去重不分管線；`incident_series` 沒有任何管線掛 |
| 目前安全無矛盾 | 6 | 方向翻轉只有一處；收斂不看方向；Lot 流向沒有混進呼叫圖；進行中事故依管線分開；trace 斷線不改用拓撲硬猜；狀態碼判斷只在有 span 時用 |

---

## 一、確定存在的衝突（高風險）

### H1　根因 API 的預設引擎是舊引擎，跟文件寫的「主力 llm」相反

- **現況**：`aiops/engine/requests.py:48` 的 `RcaRequest.workflow` 預設 `"rca_report"`（舊引擎 `evidence`）。呼叫端沒帶 `workflow` 就不會跑 `rca_report_llm`。全 repo 沒有其他地方指定 `rca_report_llm`。
- **矛盾**：`docs/ARCHITECTURE.md` 1.1.1 與 CLAUDE.md §1 都寫「RCA 主力是 llm、備援是 evidence」。實際上預設跑的是備援。
- **影響**：目前反而是「安全」的那一邊（舊引擎在工廠會退回 `topological`，見 H2），但文件、混沌成績（llm 193/200）跟線上實際跑的引擎不是同一個。
- **待決定**：預設改成 `rca_report_llm`，還是文件改寫成「預設 evidence，llm 需指定」。改預設前必須先處理 H2，否則工廠事故會全部變成「證據不足」。

### H2　新引擎在工廠／沒有 trace 時一定「證據不足」，而且 `topological` 接不上

- **路由實況**：

| 情境 | 舊引擎 `rca_report`（evidence） | 新引擎 `rca_report_llm`（llm） |
| :--- | :--- | :--- |
| 有 trace | 用 span 判自身耗時 | 事實＋日誌證據交給 LLM |
| trace 來源有設定但連不上（`rca_traces_failed`） | 證據不足，**不**用拓撲硬猜 | 只剩日誌事實；沒有日誌就證據不足 |
| 本來就沒有 trace（工廠、未設定） | 退回 `rca.root_cause.topological`（用 Lot 流向或呼叫圖推測） | 沒有事實 → **證據不足，不呼叫 LLM**，也不退回任何算子 |

- **原因**（三個疊在一起）：
  1. `rca.facts` 的時序事實（`metric_shift`）讀 `ctx.rca_series`，但**沒有任何 RCA 管線掛 `ingestion.incident_series`**，所以工廠只剩指標時，事實清單是空的。
  2. LLM 法官送出的圖（`rca_judge.graph_rows`）只有呼叫邊、佇列邊、資源邊，**沒有 Lot 流向**（`rca_judge.py` 完全沒讀 `ctx.lot_edges`）。
  3. `rca.root_cause.llm` 沒有 fallback 參數；CLAUDE.md §1 規定 LLM 路徑「不退回舊規則硬猜」。
- **狀態也不一致**：沒有日誌、沒有事實時，`extract_evidence` 讓 `rca_status` 維持 `ok`（`evidence_extractor.py:4`），法官回空清單只寫 `rca_root_note`（`rca_judge.py:512`）。報告顯示「狀態正常、沒有根因」，而不是 `undetermined` 加原因代碼，查詢 `rca_status` 的人會誤以為有判斷。
- **對應指令中的地雷**：「證據不足黑洞」確實存在，但只發生在新引擎；目前因為預設跑舊引擎（H1）所以沒觸發。
- **待決定**：工廠事故固定走 `rca_report`（在文件寫明分流規則）；或在 `rca_report_llm` 掛 `incident_series` 並把 Lot 流向送進法官的圖。後者會動 LLM 路徑，要先過混沌。

### H3　依賴圖沒有「邊的語意」欄位，寫反方向沒有任何東西會擋

- **現況**：方向規則只寫在 `engine/topology.propagation_edges` 的 docstring：呼叫邊 `(caller, callee)` 翻成「被呼叫端 → 呼叫端」，Lot 邊 `(upstream, downstream)` 不翻。workflow YAML 可以直接寫 `topology: [[a, b]]`（`engine/pipeline.py:94, 268`），一律當成「a 呼叫 b」。
- **地雷**：工廠的供電、供氣、冷卻這類「供應端 → 使用端」的關係**沒有對應的邊型別**。若有人照直覺把「變電站 → 機台」寫進 `topology`，程式會翻成「機台出事拖垮變電站」，根因直接指反；正確寫法是反過來寫成 `[機台, 變電站]`（機台「依賴」變電站），這只能靠人記得。寫進 `lot_edges` 則方向正確，但語意是「Lot 流經」，`lot_path_chain` 會把它當產線流向收斂。
- **目前沒有觸發**：出貨的 YAML 都沒有寫 `topology:` 或 `lot_edges:`。
- **修法方向**：這正是之前定案的 `ServiceGraph` 重構要解決的：邊帶型別（呼叫／佇列／流向／供應），對外只暴露「誰會拖累誰」。現在不動，照既定順序排在混沌回歸之後。

---

## 二、功能重疊可簡化（中風險）

### M1　同根因抑制（第 6 層）大多跟呼叫鏈收斂（第 4 層）重疊，只在 10–30 分鐘的縫隙裡起作用

- **先後**：`same_entity` → `topology_chain`（相鄰且間隔 ≤ 10 分鐘就併）→ `messaging_link` → 計分 → `absorb_open` → `suppress_window` → `suppress_same_root_cause`（間隔 ≤ 30 分鐘）→ `suppress_dedup`。
- **重疊**：兩件事故如果在呼叫圖上相鄰、間隔 ≤ 10 分鐘，第 4 層已經併成一件，第 6 層看不到兩件。第 6 層真正生效只剩「相鄰、間隔 10–30 分鐘、且被呼叫端那件分數較高」的情況。
- **誤殺風險**：這個縫隙正好是「上游晚了十幾分鐘出現的新故障」最可能落的地方。只要它的每個實體都呼叫了下游那件的某個實體、分數又沒有比較高，就會被當成症狀壓掉（`suppress.py:67-78`）。`exempt_tags: [fatal]` 只保護致命界線事件。
- **另外**：它不看下游那件有沒有被別的規則抑制（例如維護窗）；下游在維護、上游真的壞掉時，兩件都不會發。這可能是刻意的，但沒有文件寫明。
- **建議**：資料庫恢復後統計它在混沌與回放裡實際觸發幾次、其中幾次是真的症狀；觸發極少就考慮刪除，或把窗口與 `topology_chain` 對齊。不要現在改。

### M2　`p0_boost` 在主線是空轉

- `it_otel.yaml`、`it_apm.yaml`、`factory.yaml` 都是 `params: {entities: []}`，沒有任何實體會被加分。節點在跑但不做事；1.1.1 的分層表把它列成主線算子，容易讓人以為它有作用。

### M3　佇列關係存了，但沒有 trace 時的根因推測看不到

- `service_async_edges` 有存（`admin.topology_refresh`），每輪載入成 `ctx.async_edges`，但只有 `alert.messaging_link` 在用。
- `propagation_edges` 只收呼叫邊與 Lot 邊，所以 `rca.root_cause.topological` 與 `rca.section.propagation_path` 看不到佇列。沒有 trace 時，生產者與消費者都出事，`topological` 會把兩個都列成「不在拓撲中，無法判斷上下游」。
- 有 trace 時沒有這個問題：舊引擎與 LLM 法官都讀這一輪現撈的 `ctx.rca_mq`。

### M4　依賴圖與通知去重不分管線

- `service_edges`、`service_async_edges`、`lot_paths` 沒有 `pipeline` 欄位，每條管線每輪都把三者全部載入（`storage/hooks.py:72-77`）。工廠管線會帶著 IT 的呼叫圖、IT 管線會帶著 Lot 流向。
- 通知去重的指紋是實體名稱串起來（`engine/types.py:104-106`），最近通知也不分管線載入。
- **只有在名稱撞到時才出事**：例如機台和服務剛好同名，或同一個服務同時被 `it_otel` 與 `it_apm` 監控（後者會被前者的通知去重，這也許是想要的）。目前出貨的實體名稱沒有重疊。
- 實體名稱是單一字串空間，沒有「這是服務還是機台」的型別；進行中事故（`Incidents.list_open(pipeline)`）與異常紀錄有分管線，這兩處是安全的。

### M5　`ingestion.incident_series` 有算子、有事實產生器，但沒有任何管線掛

- `rca.facts` 的 `series_facts`（`metric_shift`）只吃 `ctx.rca_series`，而兩條出貨的 RCA 管線都沒掛 `incident_series`，這段在出貨設定下是死碼。它正好是 H2 工廠路徑缺的那塊。

---

## 三、目前安全無矛盾

| 項目 | 為什麼安全 | 位置 |
| :--- | :--- | :--- |
| 方向翻轉只有一處 | 所有「誰拖累誰」都經過 `propagation_edges`（`rca/base.graph_edges`、傳播路徑段落、`topological`、摘要的實體名單）；直接讀 `ctx.topology` 的地方只拿它當無向鄰接或名單 | `engine/topology.py:37-44` |
| Lot 流向沒有混進呼叫圖 | 兩者分開存（`lot_paths` vs `service_edges`）、分開載入，只在 `propagation_edges` 依各自規則合併；docstring 明寫「Lot 流向絕不能塞進 topology」 | `engine/topology.py`、`storage/hooks.py:72-77` |
| 佇列消費者不會被當成呼叫邊 | `service_edges()` 從 span 建邊時排除 `consumer`，避免被翻成「消費者拖垮生產者」 | `engine/topology.py:18-25` |
| 收斂不看方向 | `topology_chain` 與 `lot_path_chain` 用無向的 `frozenset`，`messaging_link` 兩個方向都試；方向寫反也不會影響併不併 | `stages/grouping/group.py` |
| trace 斷線不改用拓撲硬猜 | 舊引擎在 `rca_traces_failed` 時直接證據不足，只有「本來就沒有 trace」才退回 `topological` | `rca/algorithms/evidence.py:272-285` |
| HTTP 狀態碼的判斷只在有 span 時用 | `status_code >= 500` 只出現在 span 統計與處置建議，工廠沒有 span 不會走到；事實層的時序、日誌事實都不假設 HTTP | `rca/spans.py`、`rca/facts/` |

`tests/test_architecture.py` 對服務名稱字面值的檢查照常通過，這次沒有找到針對特定服務或機台名稱的分支。

---

## 建議的處理順序（只是建議，都還沒做）

1. **H1 先決定**：預設 RCA 引擎是哪一個。這是一行設定，但影響所有線上報告。
2. **H2 跟著 H1**：如果預設要改成 llm，工廠先固定走 `rca_report`，並把「沒有事實時狀態維持 ok」改成 `undetermined` 加原因代碼。
3. **M1、M2 等資料**：資料庫恢復後統計實際觸發次數再決定刪不刪。
4. **H3、M3、M4 併進 ServiceGraph 重構**：邊型別、佇列邊進傳播圖、依賴圖分管線，照既定順序排在混沌回歸之後。
