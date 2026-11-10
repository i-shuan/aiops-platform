# 第二階段設計：MQ 非同步跨時窗診斷（P0）＋ QPS 流量放大檢定（P1）

狀態：**設計草案，未動工**。基準：main `3028902`。本文件只寫 P0 的檔案級設計；P1 只列與現有 `_amplifiers` 的關係與缺口，P0 落地後再補細節。
CLAUDE.md §99：演算法完成要拿真實資料回放，本文第 6 節老實交代 MQ 這一塊真實 ES 驗不了什麼。

---

## 0. 先對齊的四個前提（對照你的草案，三處要修正）

| # | 草案的說法 | 程式／資料實況 | 設計怎麼處理 |
| :-- | :-- | :-- | :-- |
| 1 | 「讀 Span Link 或 Parent 的發布時間戳 T_pub」 | **span link 只有 `trace.id` + `span.id`，沒有時間**。parent 同理。時間得另外取。 | T_pub 兩個來源，依序：① 消費者 transaction 的 `transaction.message.age.ms`（T_pub = 消費時間 − age）；② 沒有 age 就再查一次被連結（或 parent）那個 span 的 `@timestamp`。兩個都沒有 → 不跨窗，note 寫明原因（降級，見 §3.5）。 |
| 2 | 「以 `[T_pub−5m, T_pub+5m]` 鎖定 Producer 時窗」 | 積壓時，事件窗內的訊息是**一段連續的發布時間**（積壓 40 分鐘 = 這批訊息的 T_pub 散在 40 分鐘上）。對單一 T_pub 取 ±5 分鐘會漏掉大半。 | 生產者時窗 = 事件窗內被採樣消費者 trace 的 **[p5(T_pub), p95(T_pub)]** 兩側各留 `pad`（預設 2 分鐘），上限 `max_lag_minutes`（預設 120）。基準窗 = 這個時窗之前、等長（沿用 `before_minutes` 的自適應規則）。 |
| 3 | 「只有當根因候選 Span 屬於 Messaging Consumer 時才觸發」 | 候選是 `rank()` 的**輸出**，撈 trace 在 `rank()` 之前，這樣是循環。 | 觸發條件改成「事件內任一實體在事件窗有 ≥ `min_consumer_spans`（預設 5）筆 messaging consumer 入口」。這是撈 trace 時就能數的，不依賴排名。一般 HTTP 事件完全不走這條路。 |
| 4 | 「`alert.messaging_link` 把時間差放寬到 1~2 小時就能合併」 | 偵測階段每一輪只看這一輪的異常；`alert.absorb_open` 只靠共同實體吸收，且 15 分鐘沒新異常事件群就結案。生產者的事件群 15 分鐘安靜就關了，40 分鐘後消費者才炸，**沒有東西可以合併**。 | 設計成兩層：**主戰場在 RCA**（消費者事件 → 反查生產者在它自己的時窗）。偵測端只做「同一輪／生產者事件群仍進行中」的合併（`alert.messaging_link`，§3.3）。不把窗口單純放寬到 2 小時：那會把無關的事件誤併。是否再讓 `absorb_open` 認非同步邊是決策點 D3。 |

### 0.1 現有程式碰到非同步會算歪（已實測，不是推測）

腳本 `probe_async_child.py`：生產者 `order-api` 50 ms、其中 send span 12 ms；消費者 `ship-worker` 20 分鐘後處理，parent = send span（Elastic APM 的 trace context 透過訊息 header 傳遞時就是這個結構）。在 main 上跑：

```
自身耗時: P 38.0 / S 12.0 / C 30.0
時鐘偏移被標記的 span: ['S']        ← 把 20 分鐘後的消費者當成「超出父區間」，假的時鐘偏移
send span 是資源葉節點嗎: False       ← 發布等待不再被算成等外部資源（kafka/orders）
ship-worker 呼叫者: {'order-api': 1}  ← 非同步消費被當成 order-api 同步呼叫
```

三個都是 P0 要先修的前置，不是順手：
1. `self_times_detail`：非同步子 span 不參與父的扣除與偏移判斷。
2. `is_resource_leaf`：只有非同步消費者當子節點的 messaging span 仍算資源葉節點。
3. `inbound_callers`：非同步呼叫者記成 `mq:<生產者>`，`_amplifiers` 往上游走時**不跨過**它（見 §5.2，不然消費者追趕積壓時的流量暴增會把責任推給生產者）。

---

## 1. 資料來源與欄位（Elastic APM）

假設（要用 §6 的一行指令在你的 ES 確認）：

| 角色 | 欄位 |
| :-- | :-- |
| 生產者 | span 文件：`span.type = messaging`、`span.subtype = kafka／rabbitmq…`、`span.action = send`、`span.destination.service.resource`（如 `kafka/orders`） |
| 消費者（逐則處理） | transaction 文件：`transaction.type = messaging`、`transaction.message.queue.name`、`transaction.message.age.ms`；`parent.id` 指到生產者 send span，**同一個 `trace.id`** |
| 消費者（批次 poll） | 新 `trace.id`、沒有 parent，改用 `span.links`（陣列，每筆 `{trace.id, span.id}`） |

**真實 ES 現況（已探測，唯讀）**：`traces-apm-*` 16,875 筆文件，`span.type` 只有 app／db／external，`transaction.type` 只有 request；`messaging` 0 筆、`span.links.*` 0 筆、`transaction.message.*` 0 筆，索引對映裡也沒有這些欄位。→ 見 §6。

---

## 2. 資料結構

`aiops/engine/context.py`
- `SpanRecord` 新增選填欄位（預設值 = 現有行為，mock 與既有測試不帶）：
  `consumer: bool = False`（transaction.type = messaging）、`queue: str = ""`、`age_ms: float | None = None`、`links: tuple[tuple[str, str], ...] = ()`（`(trace_id, span_id)`）。
- 新增 `MqEdge`（frozen dataclass）：`producer: str`、`consumer: str`、`queue: str`、`messages: int`（配到幾筆）、`lag_ms_p50 / lag_ms_p95: float`、`pub_start / pub_end: datetime`（生產者時窗）、`linked_by: "parent" | "link" | "age"`。
- `PipelineContext` 新增：`rca_mq: list[MqEdge]`、`rca_mq_note: str`（跨窗做不了的原因，報告照印）、`async_edges: dict[tuple[str, str], float]`（偵測用：邊 → lag p95 秒）。三個都掛在每輪新建的 ctx 上（§100.3，沒有模組層級狀態）。
- `RootCandidate.attribution` 在非同步時多一個鍵 `async`：`{queue, lag_ms: {before, during}, pub_window: [lo, hi], linked_by}`；既有鍵不動。

---

## 3. 檔案級變更

### 3.1 資料源 `aiops/sources/elasticsearch.py`（＋`sources/base.py`、`sources/mock.py`）
- `SPAN_FIELDS` 加 `transaction.type`、`span.action`、`transaction.message.queue.name`、`transaction.message.age.ms`、`span.links`；`_span` 對應填進新欄位（型別檢查同現有 `status_code` 的寫法，不用 try/except）。
- 新增 `fetch_linked_spans(trace_ids, span_ids, lo, hi, max_spans) -> (list[dict], bool)`：
  `terms trace.id`（同 trace 傳遞）或 `terms span.id`（span link，先撈被連結的 span 取 trace.id 與時間，再撈那條 trace）；時間限制 [lo, hi]，最多兩輪查詢；只呼叫 `_search`。`BaseDataSourceSpec` 預設實作回 `([], False)`，沒實作的資料源等於不支援。

### 3.2 撈取 `aiops/stages/ingestion/fetch.py::IncidentTracesFetch`
- 新參數：`follow_messaging: bool = False`（**預設關**：沒打開的 YAML 行為與 main 逐字相同）、`max_lag_minutes: int = 120`、`max_linked_traces: int = 200`、`min_consumer_spans: int = 5`、`mq_pad_minutes: int = 2`。載入期檢查範圍（負數、0 上限）。
- 主撈取與基準窗回溯做完後，跑 `_follow_messaging`：
  1. 數事件實體的 consumer 入口 ≥ `min_consumer_spans` 才繼續，否則記 `ctx.log` 後結束。
  2. 取至多 `max_linked_traces` 筆（固定順序，重跑可重現）的 `parent_id`／`links` 中不在已撈 span 內的，呼叫 `fetch_linked_spans`。
  3. 把回來的 span 併進 `ctx.rca_spans`（去重 `span_id`），並以 `stages/rca/mq.build_edges`（純函式）算 `ctx.rca_mq`；生產者基準窗再補撈 [pub_start − 基準, pub_start)。
  4. 任何一步連不上：記 `errors`、`ctx.rca_mq_note` 寫原因，**報告照出**（旁路證據，同現有 trace 撈取規矩）。
- 撈到的總 span 仍受 `max_spans` 約束，超過截斷並註記。

### 3.3 純函式 `aiops/stages/rca/mq.py`（新檔，沿用 spans.py／logs.py 的「共用純函式」慣例）
- `build_edges(spans, start, end, pad, max_lag) -> (list[MqEdge], note)`：消費者入口配對生產者 send span（parent → 同 trace；links → 被連結 span；都沒有且有 `age_ms` → 只給 lag、`linked_by="age"`、生產者未知則 `producer=""` 不猜）；算 lag 分位數與生產者時窗。
- `async_edges_from_spans(spans) -> dict[(producer, consumer), lag_p95_s]`：給 `admin.topology_refresh` 沉澱邊用。

### 3.4 前置修正 `aiops/stages/rca/spans.py`（0.1 的三條）
- `self_times_detail`：子 span `consumer=True` 時不計入扣除、不判偏移。
- `is_resource_leaf`：children 全是 consumer 入口時仍視為葉節點。
- `inbound_callers`：consumer 入口記成 `mq:<生產者服務>`（找不到父 → 原本的 `UNKNOWN_CALLER`）。
- **零回歸判準**：沒有 consumer 的資料，所有輸出與 main 逐點相同（11/3 快照 + 單元測試）。

### 3.5 根因 `aiops/stages/rca/algorithms/evidence.py`
- `_amplifiers`：往上游走遇到 `mq:` 開頭的呼叫者就停（不歸因給生產者）；邏輯只加一行判斷，其餘不動。
- `rank()` 在原本的 rows 算完後，新增「非同步生產者列」：對每條 `MqEdge`（producer 非空）用 `window_stats(spans, pub_start, pub_end)` 對 `window_stats(spans, pub_start − base, pub_start)` 算同一組自身耗時／自身錯誤增量，**同一個正規化與門檻**，生產者即使不在 `incident.entities` 也能成為候選。
- 「訊息夾帶的失敗」轉移（類比既有的流量放大轉移）：消費者自身錯誤增量，若失敗的 trace 能連回生產者、且該生產者在發布時窗有證據（`hit`），消費者那部分的錯誤增量轉記給生產者，理由寫明轉了多少；沒連結、沒證據就不轉。
- 降級（資料不夠就說不夠，不硬猜）：有 consumer 但沒有 link／parent／age → `ctx.rca_mq_note = "偵測到 messaging consumer，但沒有 span link、parent 或 message.age，無法跨時窗"`，排名行為與 main 相同。
- 新參數（皆在 `__init__` 載入期驗證）：`async_weight`（預設 1.0，生產者列直接用同一分數，不另設權重；此參數只為消融保留，與 `self_weight` 同規矩）。

### 3.6 報告 `aiops/stages/rca/sections/evidence.py`、`actions.py`
- 段落：消費者事件加一行「訊息延遲 X→Y 分鐘（queue、p95）；生產者時窗 hh:mm–hh:mm」；`ctx.rca_mq_note` 有值就印。
- `actions.py`：`RunbookFacts` 加 `async_info`（來自 `attribution.async`）；新增 **RB-07 非同步佇列積壓／訊息夾帶失敗**（priority 95，介於 RB-01 與 RB-02 之間：有明確跨窗證據時要贏過一般 trace 規則）。處方：查佇列積壓深度與消費者並行度、確認生產者在 `pub_window` 的變更／依賴、毒訊息（dead-letter）檢查。
  **編號調整**：你文中把 R_surge 掛 RB-07；P0 先做，RB-07 給 MQ，P1 的 QPS 放大改掛 **RB-08**。要對調告訴我（D2）。

### 3.7 收斂與拓撲
- `aiops/stages/grouping/group.py`：新增 `alert.messaging_link`。`linked(a, b)`：存在非同步邊 (p→c)，p 在較早的事件群、c 在較晚的，**且 a 的起點早於 b**（方向性：生產者先、消費者後），間隔 ≤ `clamp(lag_p95 × 1.5, min_window, max_window_minutes)`（`max_window_minutes` 預設 120）。沒有 `ctx.async_edges` 的邊不合併。
- `aiops/storage/models.py`：`ServiceEdgeRow` 加 `is_async bool default false`、`lag_ms_p95 float null`。`storage/repository.py::ServiceEdges.all()` 帶出；`storage/hooks.py` 載入 `ctx.async_edges`（在現有載入 `ctx.topology` 同處，沿用 `topology_explicit` 規則）。
- `migrations/postgres/0007_service_edges_async.sql`：可重複執行（`ADD COLUMN IF NOT EXISTS`），附 down。
- `aiops/control_plane/operations/topology.py`（`admin.topology_refresh`）：用 `async_edges_from_spans` 寫入欄位；prune 規則不變。
- `aiops/engine/topology.py::service_edges`：非同步邊仍會出現在同步邊裡（因為 parent.id 指得到），這是對的（同輪內仍可用既有 `alert.topology_chain` 以 10 分鐘窗合併），不改。

### 3.8 YAML（不改任何 Python 流程）
- `pipelines/rca/rca_report*.yaml`：`ingestion.incident_traces` 加 `follow_messaging: true` 與 `max_lag_minutes`。
- 偵測 YAML：`alert.topology_chain` 後加 `alert.messaging_link` 節點（順序鐵律不變：收斂 → 計分 → `absorb_open` → 抑制 → …）。
- 偵測消費延遲的指標（`transaction.message.age.ms` 的平均）是新增 target 的 YAML 設定（`attrs: {signal: lag}`），不是程式；`signal_of` 的 token 表要不要加 `lag` 見 D4。

### 3.9 順手防護（你要求的）
- 所有新參數在 `__init__` 載入期驗證範圍與型別（跟 `evidence.py` 現行寫法一致），新資料類別不收未知欄位。
- **不**動 `NodeSpec`／`Workflow` 的 `extra="forbid"`：它影響全部既有 YAML，是 PR #10 第 4 題的獨立修補，我會另外先掃一遍現有 YAML 有沒有靠多餘鍵活著，再決定要不要單獨做（D5）。

---

## 4. 混沌場景 12（非同步跨時窗）規範

`scripts/chaos/world.py`：`Call` 新增 `kind="messaging"` 非同步呼叫；欄位 `consumer`（消費服務）、`capacity`（消費者每分鐘處理量）、`poison`（隨訊息夾帶的失敗機率，陣列）、`link_mode`（`parent` | `links` | `none`）、`emit_age`（是否寫 `message.age.ms`）。延遲用流體佇列：`backlog[m] = max(0, backlog[m−1] + λ[m] − capacity)`，`delay = backlog / capacity`，所以延遲會爬升也會洩洪，不是固定值。`scripts/chaos/es.py` 補對應欄位。

| 子場景 | 設定 | 真值 | 判準（改前／改後都要印） |
| :-- | :-- | :-- | :-- |
| **12a** 消費者自己慢（對照組） | 12:00 起消費者的 DB 200→800 ms，容量不足，積壓 | 根因 = 消費者 + 它的 DB；生產者**不是**根因 | top-1 = 消費者；dependency 為該 DB；`attribution.async` 有 lag；生產者不得進前 2（防止新邏輯亂指） |
| **12b** 訊息夾帶的失敗（parent 傳遞） | 12:00 起生產者依賴 DB 變慢＋產出壞訊息；消費者約 12:20 才開始失敗（lag 20 分） | 根因 = 生產者 + DB；消費者是受害者 | 改前：top-1 = 消費者（自身錯誤）；改後：top-1 = 生產者，`pub_window` 涵蓋 12:00–12:20，消費者錯誤增量轉記 |
| **12c** 同 12b，批次消費用 `span.links`（新 trace id、無 parent） | `link_mode=links` | 同 12b | 同 12b；T_pub 走「查被連結 span」那條路 |
| **12d** 舊版 agent：無 link、無 age | `link_mode=none, emit_age=False` | 無法跨窗 | 不得亂指生產者；`rca_mq_note` 寫明原因；排名與 main 一致 |
| **12e** 生產者無自身症狀的毒訊息（死區） | 生產者所有指標正常，只是內容壞 | 根因 = 生產者的程式變更 | `@pytest.mark.xfail(strict=True)` 留存（§100.4）：現有證據只看延遲與錯誤，抓不到「內容」；note 要求人工查 dead-letter |
| **12f** 超過 `max_lag_minutes` | lag 3 小時 | 無法跨窗 | note 寫「延遲超過上限」，不撈 |

驗收印表（沿用場景 1–11 格式）：每個子場景的 top-1、生產者排名、`pub_window` 與真值的誤差、`attribution.async`、`Runbook`、撈到 span 數與兩輪查詢數。

---

## 5. P1：QPS 流量放大檢定（只列關係與缺口，P0 後再細化）

### 5.1 `_amplifiers` 現在涵蓋什麼（`algorithms/evidence.py:85`）
- 輸入：`spans.inbound_callers` 的「每服務流入呼叫者計數」，事件窗 vs 事件前每分鐘；`load = (nd/dur_min) / (nb/base_min)`，兩個窗都要 ≥ `min_requests`。
- 判斷：`load ≥ min_load_ratio（1.5）` 的服務往上游沿「主要呼叫者」走，到第一個自己 load 沒漲的呼叫者 = 放大者；走不到（來源是外部入口或孤兒）就**不指認**。
- 效果：被灌流量服務的自身耗時／錯誤增量的 `(1 − 1/load)` 轉給放大者；理由文字寫明。

### 5.2 缺口（P1 要補的，不重寫）
1. **外部入口突增沒有輸出**：來源是 `EXTERNAL_CALLER` 時 `break`，什麼都不記。R_surge 要把它變成明確結論（「入口流量 N 倍、自身耗時隨負載上升」），這是 RB-04（目前只看 metric signal，不看 span）的證據來源。
2. **沒有「每個父請求打幾次下游」**（fan-out／重試風暴）：現在只比 callee 計數，呼叫者自己 load 沒漲就算放大者，但不區分「呼叫次數／請求」變大（重試）與「呼叫者另有來源的流量」。
3. **沒有飽和判準**：負載升時自身耗時線性升（排隊）還是沒關係（資源壞了）。要加「增量與 load 的相關」才能支撐 `(1−1/load)` 這個近似。
4. **R_surge 沒有輸出欄位**：`attribution` 沒有 load／fan-out；報告與 Runbook 看不到。
5. 與 P0 的交集：非同步邊的「放大」（發布速率 vs 消費速率）不走 `_amplifiers`，P0 先讓它在 `mq:` 停下，P1 另用發布速率比判斷。

---

## 6. 驗證計畫（§99）與老實交代

**真實資料能驗什麼、不能驗什麼**
- 真實 ES（開發機）**沒有任何 messaging 資料**（§1 探測）。所以 MQ 演算法**無法用真實指標回放驗收**；我不會拿「合成資料全過」當完成。
- 能做的真實驗收：**零回歸**——11/3 真實事故快照（75 個異常、3 個事件群、attribution、Runbook）在新程式下與 main 逐點相同；證明新路徑在沒有 messaging 的真實資料上完全不動。
- MQ 本身的正確性靠場景 12 的模擬（照 Elastic APM 文件欄位，不是照我的演算法憑空造）。**請你確認一件事**：你們正式環境有沒有 Kafka／RabbitMQ 並已被 APM 接上？下面一行在正式 ES 查有沒有資料，有的話我用那份真資料回放，這才算數：

```bash
curl -s -H "Authorization: ApiKey $ES_KEY" -H "Content-Type: application/json" "$ES_URL/traces-apm-*/_count" -d '{"query":{"bool":{"should":[{"term":{"span.type":"messaging"}},{"term":{"transaction.type":"messaging"}}],"minimum_should_match":1}}}'
```

**動工後的驗收清單**
1. 11/3 零回歸：快照與 main 逐點相同（異常點、attribution、Runbook）。
2. 混沌場景 1–11：87／90 不變（含 8／11 全過）。
3. 場景 12a–f：逐子場景印改前／改後，12e xfail 標註。
4. 全套 pytest、`test_architecture.py`、`test_claude_md_examples`。
5. migration 0007：在 PostgreSQL 16 套兩次＋回滾（沿用 0006 的做法）。
6. 不加／不改既有測試斷言；若必須改，commit 訊息寫明業務語意。

---

## 7. 決策點（需要你定）

- **D1** P0 是否包含 `alert.messaging_link`（偵測端同輪合併）？建議包含；它小，但價值有限（§0 #4）。
- **D2** RB 編號：P0 的 MQ 用 RB-07、P1 的 QPS 放大用 RB-08（我的建議），或照你文中 QPS=RB-07、MQ=RB-08。
- **D3** 要不要讓 `alert.absorb_open` 也認非同步邊（生產者事件群進行中、消費者新異常併進去，且為此延長生產者事件群的結案等待）？建議 **P0 不做**：它動到結案時序與心跳，風險比其他項大，先看 RCA 跨窗的實際效果。
- **D4** `signal_of` 要不要新增 `lag` 類別（RB-07 的觸發除了 `attribution.async` 外，也看事件 signal）？建議加，一行 token。
- **D5** `NodeSpec`／`Workflow` 的 `extra="forbid"` 另開小 PR，不夾在這裡。


## 8. P0 實作結果與偏離設計（2026-11-10）

**狀態**：決策 D1～D5 已拍板（D1 做、D2 MQ=RB-07／QPS=RB-08、D3 P0 不做、D4 加 `lag`、D5 另開小 PR）。P0 已實作；P1（`_amplifiers` 四缺口、RB-08、場景 13）**未開始**。

### 8.1 驗收實測
| 項目 | 結果 |
|---|---|
| 全套 `pytest tests/` | 885 passed、1 skipped、13 xfailed（含架構測試、`test_claude_md_examples`） |
| 新增測試 | `test_rca_messaging.py` 70 passed＋1 xfailed（12e）；`test_messaging_pipeline.py` 51 passed＋1 xfailed（D3 邊界） |
| 既有測試檔 | 零改動（`git diff main -- tests/` 為空），沒有放寬任何斷言 |
| 混沌場景 12 | 76／77：12a 14／14、12b 15／15、12c 15／15、12d 9／9、12e 9／10（已知死區）、12f 7／7、12g 7／7 |
| 混沌場景 1～11 | 87／90，與改前逐列相同（場景 2、3、5 各差 1 項，是既有缺口，非本次引入） |
| 真實 11/3 ES 快照 | 異常點 75、事件群 3、errors 0，與改前 JSON 逐點相同（忽略 `program` 路徑欄位）；真實 ES 沒有任何 messaging 資料，所以這是「零回歸」證明，不是 MQ 功能證明 |
| 遷移 0007（PostgreSQL 16.15） | 升級 14 ms；重複執行 1 ms、已有資料時重複執行不動資料；UNIQUE(producer, consumer) 有效；回滾兩次、再升級、再回滾皆乾淨；SQL 版欄位與 ORM `create_all` 版完全一致 |

### 8.2 已知死區（`xfail(strict=True)` 留存）
- **12e** 生產者毫無症狀的毒訊息：trace 只有耗時與成敗，看不到訊息內容。第一名維持消費者，`attribution.async.suspect_payload=True`、建議處置多一行「抽查失敗訊息與 dead-letter queue，並核對生產者當時的變更」。
- **D3 邊界** 生產者這一輪已安靜、消費者才出事：`alert.absorb_open` 只認共同實體，會另開一張卡。
- 場景 12a 改前的 trace 撈取打到 20000 span 上限，「改前」數字偏低。

### 8.3 與本設計不同的地方
1. 核心純函式放 `aiops/engine/mq.py`（撈取、根因、拓撲沉澱三方都要用、彼此不能 import），`stages/rca/mq.py` 只留 RCA 專用部分。
2. 資料源介面是 `fetch_linked_spans(trace_ids, start, end, max_spans)`，沒有 `span_ids` 參數；`span.action` 不撈。
3. T_pub 來源優先序 parent → link → age（原設計 age 在前）：parent／link 有生產者身分，age 沒有。
4. T_pub 時窗用 P5～P95 加兩側 `mq_pad_minutes`（預設 2），不用 min～max；`max_lag_minutes`（預設 120）擋住過老的延遲，超過記 `skipped: over_max_lag`。
5. `MqEdge` 多了 `lag_ms_p95_before`、`failed` 兩欄；MQ 補撈有獨立 span 預算；`ctx.rca_main_start` 記錄補撈後主窗起點。
6. `service_edges` 不收消費者（避免把非同步誤當同步）；非同步邊另放新表 `service_async_edges`（遷移 0007），而不是設計的「在 `service_edges` 加欄位」：舊版程式與回滾後不會把方向顛倒的非同步邊讀成同步拓撲。`ctx.async_edges` 單位為毫秒，`topology_explicit` 時不載入。
7. 剪開非同步邊只改 `spans.py::_children_index`（不收 consumer）一處，自身耗時、時鐘偏移、資源葉節點、下游連帶失敗同時一致；消費者一律是入口，呼叫者記成 `mq:<生產者>`，流量放大回溯不會沿它往上走。`sample_trace` 的根也納入消費者。
8. 根因：生產者那一列只在「發布時窗內自己有症狀」且承接占比 ≥ 0.5（`MIN_MESSAGE_SHARE`）時才給分；只轉記「自身錯誤增量」，不轉記延遲。新參數 `async_weight`、`min_lag_seconds`。
9. RB-07 觸發條件：有 `attribution.async` 且沒有更強的依賴已主導（12a 消費者自己的 DB 慢 → 維持 RB-02，不誤報 RB-07）；RB-07 處方依角色排序（`Runbook.order`）。`signal_of` 加 `lag`（D4）。
10. 三行摘要詞表加非同步用語（`_async_clause`、`_async_verify`），驗證行不寫「排除它本身的程式」；`rca_mq_note` 同步出現在根因與拓撲段落；API 結果多 `root_cause.messaging`。
11. `alert.messaging_link`：窗長 = `clamp(lag × lag_factor, min_window_minutes, max_window_minutes)`，起點用 `<=`，無非同步邊時完全不動作；生產者「進行中但這輪安靜」不併（D3）。已在內建 `it_apm`／`it_otel`（`group_topology` 之後、`score_golden` 之前）與兩個 RCA YAML（`follow_messaging: true`、`max_lag_minutes: 120`）接上。
12. 儲存的 lag 取最近一次 `admin.topology_refresh` 的值；舊版把消費者當同步邊寫進 `service_edges` 的列，由既有 prune 自然過期。
13. 混沌：新增場景 12g（沉澱後同輪事件併卡）；模擬器沒有 lag 指標 target，`lag` 訊號只由單元測試涵蓋。
14. ~~已發現但不在範圍：RB-03 對訊息消費者的處方字面是「檢查 Kafka RECEIVE（HTTP 失敗）」，措辭不貼切。~~ 已修（`RunbookFacts._failure`）：範例 trace 是訊息消費者時寫「訊息消費失敗，佇列 <名稱>」並請人看「訊息內容（payload、headers）」；HTTP 請求有狀態碼才寫「HTTP <碼>」，沒有狀態碼只寫「失敗」。

---

## 9. P1 實作結果與偏離設計（2026-11-10，分支 `feat/traffic-amplifier-p1`）

範圍：5.2 的缺口 1、2、4（入口突波有輸出、扇出判準、attribution 欄位）＋ Runbook RB-08 與放寬的 RB-04。缺口 3（飽和判準）與缺口 5（非同步放大）沒有做。

### 9.1 做了什麼
- **扇出判準**（`algorithms/evidence.py`）：扇出 = 呼叫者→被呼叫者的呼叫數 ÷ 呼叫者自己的入口請求數。事件期間 ÷ 事件前 ≥ `min_fanout_ratio`（預設 2.0，YAML 可調，≤ 1 直接 `ValueError`），而且受害者自己的自身耗時或錯誤率增量過顯著門檻，才在放大者候選上標 `attribution["amplifier"]`（`type`、`victim`、`via`、`load_ratio`、`fanout{before,during}`、`fanout_ratio`、`moved_self_ms`、`moved_error`、`victims`、`in_incident`）。
- **不在事件裡的放大者補候選列**：扇出判準成立、它的入口請求也 ≥ `min_requests` 才補，`in_incident=False`，理由寫明「它不在事件裡（自己沒有告警）」。
- **入口突波**：往上游走到外部入口（沒有父 span 的請求不少於孤兒）時，在被灌流量的候選標 `attribution["ingress_surge"]`（`entry`、`load_ratio`、`entry_load_ratio`），不轉分數。
- **Runbook**（`sections/actions.py`）：RB-08「內部呼叫放大」優先序 85，條件是第一名帶 `amplifier.type == "traffic_amplifier"`；RB-04 條件變成「原條件 **或** 第一名帶 `ingress_surge`」。`RunbookFacts` 新增 `amplifier`、`ingress_surge` 兩個 property。
- **三行摘要**（`enrichment/narrative.py`）：模板新增放大者／入口突波句；封閉詞表加 `loop tight policy storm throttle throttling fanout fan-out fan amplifier amplification ingress gateway`；放大者的 `victim`／`via`／`victims` 與入口 `entry` 進實體白名單（13b 的放大者不在事件實體裡，沒進白名單會被當成編造）。
- **混沌場景 13**（`scripts/chaos/scen_traffic_amp.py`，`run.py 13`）：模擬器 `Call` 新增 `times`（每個請求重複呼叫幾次；預設 1 時亂數序列與輸出跟以前完全一樣）。

### 9.2 驗收實測
| 項目 | 結果 |
|---|---|
| 全套 `pytest tests/` | 913 passed、1 skipped、14 xfailed（改前 885／1／13；差額 = 新檔 28 passed＋1 xfailed），含架構測試與 `test_claude_md_examples` |
| 新增 `tests/test_rca_amplifiers.py` | 28 passed＋1 xfailed（非同步放大死區） |
| 既有測試檔 | 零改動，沒有放寬任何斷言 |
| 混沌 13a（放大者也在事件裡） | 13／13：候選 order-svc 1.50、payment-svc 0.07、ingress-gw 0.00；扇出 1.0→25.0（25 倍）、payment-svc 流入 22.87 倍、轉記自身耗時 95.1 ms、錯誤率 0.275；RB-08 |
| 混沌 13b（放大者完全靜默、不在事件裡） | 13／13：平台自己補列 order-svc 並排第一（1.50），`in_incident=False`；RB-08 |
| 混沌 13c（外部突波對照組） | 7／7：候選 payment-svc 1.50、其餘 0.00；沒有任何候選被標 amplifier；`ingress_surge` entry=ingress-gw、流入 6.01 倍；範例 trace 失敗仍命中 RB-04、不含 RB-08 |
| 混沌 1～11 | 87／90，與改前逐列相同（場景 2、3、5 各差 1 項是既有缺口） |
| 混沌 12 | 76／77，與改前逐列相同（12e 已知死區） |
| 真實 11/3 ES 快照 | 異常點 75、事件群 3、errors 0，JSON 與 P0 改後逐點相同（忽略 `program`）：ord-svc→oracle、bill-svc→tax-gw:443、cart-api 不指認。真實資料裡沒有扇出暴增的事故，所以這是「零回歸」證明，不是放大者功能在真實資料上的證明 |

### 9.3 已知死區與照現況寫死的行為
- **非同步放大**（`xfail(strict=True)`）：生產者把訊息量放大 5 倍，非同步邊不算流量倍數，指不到生產者；消費者照自己的症狀排第一，也不會亂標放大者。
- **新上線的呼叫路徑**：事件前這條邊不到 `min_requests` 筆，扇出倍數沒有分母，不標 amplifier（舊的流量轉記規則照做）。
- **放大倍數小**（例：扇出 1.6 倍、門檻調到 1.5）：只轉 `(1 − 1/1.6)` = 37.5% 的增量，受害者仍排第一；放大者排第二、有 `amplifier` 標記，但 RB-08 只看第一名所以不命中。
- **下游扛得住**：扇出暴增但受害者沒有顯著變化 → 不指名（沒有受害者就沒有放大者）。
- **模擬器限制**：沒有容量模型，payment-svc 被灌爆後的變慢與 500 是場景直接注入的，不是由流量推出來的；沒有 fire-and-forget 呼叫，13b 的「靜默」靠把 order-svc 排除在監控 target 外＋並行呼叫達成。

### 9.4 與核准規格不同的地方
1. **RB-04 的放寬是加一條路，不是拿掉 `not sample_failed`**：條件 =「（traffic＋latency 且範例沒失敗）或 有 `ingress_surge` 證據」。沒有 trace 證據、只有指標訊號時，範例失敗仍不開 RB-04——否則任何「流量漲＋變慢＋出錯」都會開容量處方，包含內部放大（該開 RB-08）與單純程式錯誤。
2. **待決**：入口突波＋`errors` 訊號＋失敗範例同時存在時，RB-03（80）仍排在 RB-04（70）前面，RB-04 只在 `also`。13c 的告警只有 latency 訊號所以主處方是 RB-04；測試 `test_open_decision_rb03_still_outranks_rb04_...` 把現況寫死。要改就是把 RB-04 在有 `ingress_surge` 時提到 80 以上，或讓 RB-03 在有 `ingress_surge` 時讓位。
3. **放大者自己也被灌流量**（外部流量漲＋它又重試）：只轉 `(1 − 1/min(load, 扇出倍數))`，其餘算外部流量的份；規格沒有寫這個情況。
4. 舊的 `(1 − 1/load)` 流量轉記規則沒有動；`amplifier` 標記是在它之上多加的判準。
