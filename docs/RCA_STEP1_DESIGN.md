# RCA 第一步強化：檔案級設計規格與驗收結果

狀態：**已實作**（分支 `feat/rca-step1-refinement`），第 8 節是真實 ES 走完整 `rca_report` 管線的實測輸出。
三項：範例 trace 錯誤優先、下游資源耗時剝離與歸因、事件前基準窗自適應回溯。第 1～7 節是動工前的設計與預演值。
所有數字來自 11/3 真實 ES 事故窗回放（基準 14:35–14:50Z、事件 14:50–16:15Z，5 個服務、2,130 個入口請求），
用平台自己的 `ElasticsearchSpec` 唯讀撈取，沒有改任何資料。

## 0. 為什麼要做：真實資料的證據

平台今天的「自身耗時」= span 耗時 − 直接子 span 耗時。打 redis／oracle／tax-gw 的 span 是葉節點、`service.name` 仍是呼叫它的服務，
所以等外部資源的時間全被算成「服務自己的時間」。11/3 拆開看（每請求 ms，事件前 15 分鐘 → 事件期間）：

| 服務 | 請求數 | 今天的「自身」 | 程式自身（app） | 等外部資源 | 資源 | 資源失敗筆數 |
|---|---|---|---|---|---|---|
| bill-svc | 81→427 | 24.7→57.3 | 12.7→12.7（+0.0） | 12.0→44.7（+32.7） | tax-gw:443（external/http） | 0→18 |
| ord-svc | 73→426 | 29.7→50.7 | 21.3→21.6（+0.3） | 8.4→29.1（+20.7） | oracle（db） | 1→21 |
| cart-api | 78→434 | 6.1→11.7 | 5.3→10.8（+5.5） | 0.9→0.8（−0.1） | redis（db） | 1→25 |
| inv-svc | 72→428 | 9.9→9.4 | 4.4→4.3 | 5.5→5.1 | oracle | 1→4 |
| pay-api | 78→415 | 20.6→20.8 | 14.3→14.4 | 6.3→6.4 | postgresql | 0→5 |

bill-svc 的增量 100% 是等 tax-gw；ord-svc 的增量 99% 是等 oracle；cart-api 是程式自己變慢，而且它 25 筆失敗全部是 redis 回錯。
今天的報告只會寫「bill-svc 自身耗時 +32.6 ms」，工程師會去查 bill-svc 的程式與機器，找不到。

ES 文件上這些欄位都有、平台一個都沒撈：`span.type`（app／db／external）、`span.subtype`（internal／redis／http）、
`span.destination.service.resource`（redis、tax-gw:443）、`http.response.status_code`（事件期間 500 × 73）、`transaction.name`（GET /cart、POST /invoice）。

## 1. 決策一：SpanRecord 與 SPAN_FIELDS 的擴充

### 1.1 `aiops/sources/elasticsearch.py`

`SPAN_FIELDS` 加 5 個欄位：

```python
SPAN_FIELDS = ["@timestamp", "trace.id", "span.id", "transaction.id", "parent.id", "service.name",
               "span.duration.us", "transaction.duration.us", "event.outcome",
               # 下游資源歸因與範例 trace 排序用（OTel／ECS 標準欄位；沒有就是空字串／0，行為退回今天的樣子）
               "span.type", "span.subtype", "span.destination.service.resource",
               "http.response.status_code", "transaction.name"]
```

`_span()` 回傳的 dict 多 5 個鍵，命名刻意避開 `type`（Python 關鍵字感、也跟 processor.event 混淆）：

| dict 鍵 | 來源欄位 | 型別／預設 | 備注 |
|---|---|---|---|
| `kind` | `span.type` | `str`，`""` | transaction 文件沒有 span.type → `""`（入口） |
| `subtype` | `span.subtype` | `str`，`""` | redis／http／postgresql |
| `resource` | `span.destination.service.resource` | `str`，`""` | tax-gw:443、redis；服務對服務時是 `ord-svc:8080` |
| `status_code` | `http.response.status_code` | `int`，`0` | 只有 transaction 有；0 = 不知道 |
| `name` | `transaction.name` | `str`，`""` | POST /invoice；只給報告文字用 |

`aiops/sources/base.py` 的 `fetch_spans` docstring 把這 5 個鍵列為**選填**：資料源可以不給。`_sampled_spans` 用同一個 `_span`，不另外改。

### 1.2 `aiops/engine/context.py` 的 `SpanRecord`

在既有 7 個欄位之後**全部加預設值**，只能用關鍵字新增、不插中間：

```python
@dataclass
class SpanRecord:
    trace_id: str
    span_id: str
    parent_id: str | None
    service: str
    ts: datetime
    duration_ms: float
    error: bool = False
    kind: str = ""          # span.type：""（入口 transaction）／app／db／external／messaging
    subtype: str = ""       # span.subtype：redis／http／postgresql
    resource: str = ""      # span.destination.service.resource：tax-gw:443、redis
    status_code: int = 0    # http.response.status_code；0 = 沒有
    name: str = ""          # transaction.name：POST /invoice
```

向後相容的證明：
- 既有 15 處 `SpanRecord(...)` 建構全是前 6～7 個位置參數（`tests/test_deep_links.py`、`test_arch_audit_gaps.py`、`test_rca_evidence.py`、
  `test_dynamic_topology.py`、`scripts/synth_generator/validate_offline.py`），一行都不用改。
- `SpanRecord(**_span(doc))` 這種用法（`test_rca_evidence.py:43`、`test_synth_generator.py:69`）靠 `_span` 與 `SpanRecord` **同一個 commit** 一起改，鍵名一致。
- `aiops/sources/mock.py` **不改**：它的 db 子 span 沒有 `kind`，進來是 `""`，走今天的算法，所有用 mock 的測試輸出逐字不變。
- `stages/ingestion/fetch.py:251` 的建構改成帶關鍵字 `kind=s.get("kind", "")…`，缺鍵就是預設。

### 1.3 `RootCandidate` 加一個選填欄位

```python
@dataclass
class RootCandidate:
    entity: str
    score: float
    reason: str
    dependency: str = ""   # 下鑽歸因：這個服務的增量主要花在哪個下游資源，例如 "tax-gw:443（external/http）"；空字串 = 沒有
```

`PipelineContext.to_dict()` 的 `candidates` 多輸出 `"dependency"`。API 回應多一個鍵，舊客戶端忽略即可。

### 1.4 `PipelineContext` 加一個欄位（回溯用）

```python
rca_trace_lookback: tuple[int, int] | None = None  # (設定的 before_minutes, 實際用的 before_minutes)；None = 沒撈 trace
```

每輪 `Pipeline.run` 新建 ctx，符合第 100 節第 3 條，不需要額外重置。

## 2. 決策二：自身耗時與外部資源耗時的切割、歸因的呈現

### 2.1 切割條件（`aiops/stages/rca/spans.py`）

```python
def is_resource_leaf(span, children) -> bool:
    """葉節點（沒有子 span）而且 span.type 不是 app／空：等外部資源（db、external、cache、messaging）。
    服務對服務的呼叫不會進來：下游 transaction 是它的子 span，所以它不是葉節點。
    上游沒裝 APM 的服務（external 葉節點、resource 像 ord-svc:8080）也會被當資源：這是誠實的——我們看不到它裡面。"""
    return not children.get(span.span_id) and span.kind not in ("", "app")
```

為什麼不用「有 `resource` 就算」：mock 與舊資料沒有 resource；而 OTel 的 `app`/`internal` span 也可能帶 destination。`span.type` 是語意最準的欄位。
為什麼不只看 `kind in ("db", "external")`：OTel 還有 `cache`、`messaging`、`storage`，白名單會漏；黑名單 `app`/空 才對。

`ServiceWindowStats` 加三個欄位（既有 6 個不動、算法不動）：

```python
app_ms_per_request: float                       # 自身耗時扣掉資源等待後，程式自己的時間
wait_ms_per_request: dict[str, float]           # 資源 -> 每請求等待 ms，key = resource 或（沒有 resource 時）subtype
wait_failures: dict[str, int]                   # 資源 -> 失敗的資源 span 筆數
```

`self_ms_per_request` **維持今天的定義**（含資源等待）。理由在 2.2。

### 2.2 歸因呈現：附在服務上，不當獨立候選

兩個選項的比較：

| | A. tax-gw:443 當獨立候選實體 | B. 候選仍是 bill-svc，附 `dependency` 下鑽 |
|---|---|---|
| 排名與分數 | 要改計分（資源沒有「事件前 vs 期間」的入口請求數，`min_requests` 門檻與流量放大歸因都對不上） | **不改計分**，`test_rca_evidence` 兩套標準答案（callee_slowdown 第 1 名 bill-svc、load_amplification 第 1 名 cart-api）保證照過 |
| 拓撲／傳播路徑段落 | tax-gw 不在 `ctx.topology`，`propagation_path`、`graph` 段落找不到節點 | 不受影響 |
| `suggested_actions`「優先處理可能的源頭」 | 會印出 tax-gw:443，但沒人能「處理」第三方 | 印 bill-svc 並加一條「檢查它對 tax-gw:443 的呼叫」 |
| LLM 總結驗證（`narrative.validate` 不准提事件外實體） | tax-gw 不在 incident.entities，可能被誤擋 | 實體仍是 bill-svc |
| 標準答案 YAML（`root_cause: bill-svc`） | 要改劇本定義 | 不改 |

**選 B。** 計分照舊用含等待的 `self_ms_per_request`（今天的語意「這個服務的請求在它自己身上變慢了，不是在等下游**服務**」仍然成立），
多做一層「這些時間花在哪」的拆解寫進 `reason` 與 `dependency`。

`aiops/stages/rca/algorithms/evidence.py` 的改動：
- 新 param `min_dependency_share: float = 0.6`。
- 候選的 `self_raw` 增量裡，單一資源的等待增量佔比 ≥ 0.6 → `dependency = f"{resource}（{kind}/{subtype}）"`；
  或入口失敗增量裡 ≥ 0.6 是「子樹裡只有該資源失敗」→ 同樣填 dependency（cart-api 25/25 是 redis 回錯）。
- `reason` 多一句：`其中 +32.7 ms 是等待 tax-gw:443（external/http，資源失敗 0→18 筆），程式自身 +0.0 ms`。
- 不改：`_amplifiers`、hit 門檻、dominance、排序鍵。

`aiops/stages/rca/sections/evidence.py`（trace_evidence）每服務那行改成：

```
＊bill-svc：請求 81→427；總耗時 24.7→57.3 ms；自身耗時 24.7→57.3 ms（+32.6）
    程式自身 12.7→12.7；等待 tax-gw:443 12.0→44.7（資源失敗 0→18 筆）；錯誤率 0%→4%
```

資源那一行只在該服務有資源葉節點時出現；mock 資料一行都不多。

`aiops/stages/rca/sections/actions.py`：第一名候選有 `dependency` 時，第一條建議從「優先處理可能的源頭：bill-svc」
改成「優先處理可能的源頭：bill-svc，主要耗時在等待 tax-gw:443（external/http）：檢查該依賴的延遲、錯誤與連線池，而不是 bill-svc 的程式」。

## 3. 決策三：範例 trace 的排序鍵

`aiops/stages/rca/spans.py` 新函式：

```python
def sample_trace(spans, start, end) -> SpanRecord | None:
    """事件期間最具代表性的入口 span：失敗優先、耗時次之。
    (1) 入口自己失敗（event.outcome=failure）  (2) HTTP 狀態碼 >= 500  (3) 子樹有失敗的 span  (4) 耗時由大到小"""
    below = _subtree_failed(spans)
    roots = [s for s in spans if s.parent_id is None and lo <= as_utc(s.ts) < hi]
    return max(roots, key=lambda s: (s.error, s.status_code >= 500, below.get(s.span_id, False), s.duration_ms), default=None)
```

為什麼是這個順序：`event.outcome` 是 APM 的權威失敗旗標，狀態碼補 outcome 缺漏的資料源，「子樹失敗但入口成功」是被吞掉的下游錯誤（有值得看的地方，但入口沒失敗所以排後面），最後才是耗時。

**11/3 真實資料套用結果**（事件期間入口 2,130 筆：入口失敗 73、狀態碼 ≥ 500 的 73、只有子樹失敗的 0）：

| 名次 | 服務 | transaction | 耗時 | 狀態碼 | 子 span |
|---|---|---|---|---|---|
| 新 #1 | bill-svc | POST /invoice | 237 ms | 500 | internal 10.4 ms；http→tax-gw:443 223.0 ms **失敗** |
| 新 #2 | ord-svc | POST /orders | 153 ms | 500 | internal 26.6 ms；oracle 120.2 ms 失敗 |
| 新 #3 | bill-svc | POST /invoice | 151 ms | 500 | internal 9.2 ms；http→tax-gw:443 138.5 ms 失敗 |
| 舊 #1 | bill-svc | POST /invoice | 244 ms | 200 | 沒有失敗 |

新 #1 的 trace id `dd487b019660078efe3b34680e74e6ed`。點開 Explore 連結看到的第一條就是 tax-gw 失敗的 500，跟根因歸因同一件事。
73 筆入口失敗的子樹失敗分佈：redis 25、oracle 25、tax-gw:443 18、postgresql 5。

報告文字的標題隨之改：`事件期間最具代表性的失敗 trace（入口 500，tax-gw:443 失敗；237 ms）`；
事件期間一筆失敗都沒有時退回今天的字樣 `事件期間最慢的一條 trace`。`call_chain` 每一行多印資源：`bill-svc http→tax-gw:443：223.0 ms（自身 223.0 ms），失敗`。

## 4. 決策四：自適應回溯（只擴連續窗口，不跨週）

### 4.1 參數（`aiops/stages/ingestion/fetch.py` 的 `IncidentTracesFetch`）

| param | 預設 | 意義 |
|---|---|---|
| `before_minutes` | 15 | 第一次撈的基準窗（不變） |
| `max_before_minutes` | 0 | 擴展上限；0 = 關閉，行為跟今天一字不差。`rca_report*.yaml` 設 60 |
| `min_entry_spans` | 20 | 擴展門檻；文件寫明「要跟 `rca.root_cause.evidence` 的 `min_requests` 一致」。不共用常數：ingestion 不該 import rca |

### 4.2 觸發條件與步進

1. 第一次撈 `[start − before, end + after]`。
2. 對**事件裡的每個實體**數基準窗 `[start − before, start)` 的入口 span（parent 為 None、或 parent 的服務不是自己；6 行，不 import `stages/rca`）。
3. 若有任何實體：基準窗入口數 < `min_entry_spans` **且** 事件期間入口數 ≥ `min_entry_spans`（事件期間也不夠的話擴基準窗沒用，直接留給 evidence 寫「證據不足」），
   就把基準窗加倍：15 → 30 → 60，直到 `max_before_minutes`。每一步**只撈多出來的前段** `[start − new, start − old)`，接在既有 spans 前面；窗口維持連續。
4. `max_spans` 是整次的總預算：追加撈取的上限 = `max_spans − len(spans)`；超過就走既有的 trace 抽樣，`truncated=True`。
5. 寫 `ctx.rca_trace_lookback = (before_minutes, 實際用的分鐘)`，`ctx.result["trace_fetch"]` 多 `before_minutes_used`、`extended_for`（哪些實體、原本幾筆）。

為什麼加倍而不是一步到 60：基準窗越長越鈍（事故前的緩慢劣化會被算進「正常」），只擴到剛好夠的長度。11/3 真實流量 ≈ 5 筆／分鐘，
10% 採樣時 15 分鐘約 8 筆 → 30 分鐘約 15 筆 → 60 分鐘約 30 筆，剛好在第二步停下。

### 4.3 報告誠實標註

`ctx.rca_trace_lookback[0] != [1]` 時：
- `trace_evidence` 第一行加：`（事件前基準窗因 cart-api 入口請求只有 8 筆，不足 20 筆，已由 15 分鐘擴展至 60 分鐘）`。
- `rca.root_cause.evidence` 的 `reason` 已經會印 `（事件前 N 分鐘）`，N 自然變 60；`rca_root_note` 證據不足時也一樣。
- `incident_series`、`incident_logs` 的窗口**不跟著動**：它們有自己的 before_minutes，耦合會讓三段資料的「事件前」長度不一致還不自知。

11/3 真實資料：事件前 15 分鐘入口 72～81 筆，全部 ≥ 20，**不觸發**，報告一字不變。能觸發的是把 trace 抽到 10% 的測試。

## 5. 檔案級改動清單

| 檔案 | 改什麼 | 層 |
|---|---|---|
| `aiops/sources/elasticsearch.py` | `SPAN_FIELDS` +5；`_span` 多回 5 個鍵 | sources |
| `aiops/sources/base.py` | `fetch_spans` docstring 列選填鍵 | sources |
| `aiops/engine/context.py` | `SpanRecord` +5 預設欄位；`RootCandidate.dependency`；`rca_trace_lookback`；`to_dict` 多 `dependency` | engine |
| `aiops/stages/ingestion/fetch.py` | `SpanRecord` 建構帶新鍵；`max_before_minutes`／`min_entry_spans` 與擴展迴圈；`trace_fetch` 結果欄位 | stages/ingestion |
| `aiops/stages/rca/spans.py` | `is_resource_leaf`、`ServiceWindowStats` +3 欄位、`window_stats` 填它們、`sample_trace`、`call_chain` 不動 | stages/rca |
| `aiops/stages/rca/algorithms/evidence.py` | `min_dependency_share`；填 `dependency`；`reason` 多下鑽句 | stages/rca |
| `aiops/stages/rca/sections/evidence.py` | 每服務多資源行；範例 trace 用 `sample_trace`；回溯標註；chain 行印資源 | stages/rca |
| `aiops/stages/rca/sections/actions.py` | 第一名有 `dependency` 時改第一條建議 | stages/rca |
| `aiops/pipelines/rca/rca_report.yaml`、`rca_report_correlation.yaml` | `fetch_traces` 加 `max_before_minutes: 60` | pipelines |
| `tests/test_rca_dependency.py`（新） | 見第 6 節 | tests |
| `README.md` RCA 一節 | 自身耗時定義、下鑽歸因、範例 trace 規則、回溯 | docs |

**不改**：`aiops/sources/mock.py`、`rca/topology_builder.py`（服務對服務邊的算法不受葉節點影響）、`rca/logs.py`、`enrichment/*`、
所有 `detector.*`、任何 Helm／CI／部署檔、任何既有測試的斷言。

架構防線核對：`stages/ingestion/fetch.py` 仍是唯一 import `sources` 的算子；`stages/rca/*` 不碰 `sources`、`storage`；
沒有新增模組層級可變狀態；`SpanRecord` 仍是 JSON 友善的 dataclass，沒有 pickle。

## 6. 驗收條件（硬數字，不含主觀百分比）

### 6.1 11/3 真實 ES 回放（`rca_report` 對 cart-api／bill-svc 事件群跑一次，比對 `result`）

| 項目 | 必須等於 |
|---|---|
| 範例 trace | `trace_id = dd487b019660078efe3b34680e74e6ed`，bill-svc POST /invoice，237 ms，status 500，子 span tax-gw:443 失敗 |
| 舊範例 | 244 ms／200 那條**不再**出現在範例位置 |
| 根因第 1 名 | bill-svc（跟今天一致），`dependency = "tax-gw:443（external/http）"` |
| bill-svc reason | 含 `程式自身 +0.0 ms`、`等待 tax-gw:443 12.0→44.7`、`資源失敗 0→18` |
| cart-api 那行 | `程式自身 5.3→10.8`；`等待 redis 0.9→0.8`；資源失敗 `1→25` |
| 回溯 | `trace_fetch.before_minutes_used = 15`，報告沒有「已擴展」字樣（基準窗入口 72～81 筆 ≥ 20） |
| 撈到的 span 數 | 7,608（跟今天一樣，欄位變多、筆數不變） |

### 6.2 既有測試

- `tests/` 全綠，**零斷言修改**：特別是 `test_rca_evidence.py` 的 callee_slowdown 第 1 名 bill-svc、load_amplification 第 1 名 cart-api、
  `test_quiet_control_scenario_never_names_a_suspect`、`test_insufficient_evidence_reasons`。
- `test_architecture.py` 全綠。
- `test_claude_md_examples.py` 全綠（沒碰 mad.py 與 enrichment 範例）。

### 6.3 新測試 `tests/test_rca_dependency.py`

1. 手刻 span：服務 A 有 external 葉節點 80 ms、app 子 span 20 ms → `app_ms_per_request = 20`、`wait_ms_per_request = {"gw:443": 80}`、`self_ms_per_request = 100`（不變）。
2. 同一組資料沒有 `kind`（mock 樣子）→ `app_ms_per_request == self_ms_per_request`、`wait_ms_per_request == {}`，報告無資源行。
3. 服務對服務：下游 transaction 是子 span → 上游不被當資源葉節點，`wait_ms_per_request == {}`。
4. `sample_trace`：四筆入口（慢且成功、快且 500、慢且 outcome=failure、成功但子樹失敗）→ 依序挑 outcome=failure 那筆；拿掉它挑 500；再拿掉挑子樹失敗；全成功挑最慢。
5. `evidence` 歸因：等待增量佔 self 增量 ≥ 0.6 → `dependency` 非空且 reason 含資源名；< 0.6 → `dependency == ""`。
6. 回溯：假資料源回 10% 抽樣的 span（基準 15 分鐘 8 筆入口）→ 第二次只撈 `[start−30, start−15)`、第三次 `[start−60, start−30)`，
   `rca_trace_lookback == (15, 60)`，報告含「已由 15 分鐘擴展至 60 分鐘」；`max_before_minutes: 0` 時只撈一次、欄位為 `(15, 15)`。
7. 回溯不觸發：事件期間入口也 < 20 → 不擴展（留給 evidence 寫證據不足）。
8. `to_dict()["root_cause"]["candidates"][0]` 有 `dependency` 鍵。

### 6.4 第 99 節：報告貼數字，不貼「通過 N 個測試」

交付時貼 6.1 那張表的實際輸出，以及 `rca_report` 完整 JSON 的 `root_cause` 與 `trace_evidence` 段落原文。

## 7. 已知死區（誠實披露）

- **上游沒裝 APM 的服務**會被當成「外部資源」（external 葉節點）。報告會寫「等待 ord-svc:8080」，語意沒錯但它其實是個服務。
- **資源變慢但沒失敗**（純延遲）：dependency 由等待增量佔比判定，可以抓到（bill-svc 就是這種）；**資源失敗但不變慢**（redis 快速回錯）：由失敗佔比判定。
  兩者都不顯著（例如等待 +3 ms 佔 55%）就不填 dependency，reason 仍印拆解數字。
- **同一服務同時等兩個資源各佔 45%**：沒有單一資源過 0.6，不填 dependency；reason 會列兩個。要不要改成「前兩名合計」留給實際案例決定。
- 回溯只擴連續窗口：事故前一小時若已是另一場事故，基準會被污染；`training_exclusions` 不套用在 RCA 基準窗（今天也沒有），先不處理。
- 11/3 真實資料驗證的是 Elastic APM 的欄位命名；純 OTel collector 寫入的索引欄位若是 `span.kind`／`db.system`，`_span` 要另外對映，這版不做。

## 8. 真實 ES 驗收結果（2026-11-09，實裝程式、完整管線）

跑法（腳本 `scratchpad/rca_step1_accept.py`）：真實 ES（唯讀 key）× `it_otel` 偵測管線回放 11/3 14:00→16:20Z 每 5 分鐘一輪，
事件群寫進暫存 SQLite（notifier=log，不推 Alertmanager）；再對每個事件群跑完整 `rca_report` workflow（`StorageHooks` 從 DB 載入事件群，
`incident_traces` → `build_dynamic_topology` → `incident_logs` → `rca.root_cause.evidence` → `rca.report` 10 段 → `llm_summary` noop）。
平台不寫 ES、不連 LLM。偵測 29 輪 29 秒、三份 RCA 各 0.5～0.7 秒、`errors` 全部為空。

偵測產生 3 個事件群（跟 11/9 上午的 e2e 一樣）：

| id | 實體 | 時間（UTC） | 嚴重度 |
|---|---|---|---|
| 1 | cart-api | 14:56→16:15 | P0 |
| 2 | ord-svc | 15:33→16:00 | P0 |
| 3 | bill-svc | 15:40→16:10 | P0 |

### 8.1 硬性驗收（對照第 6.1 節）

| 項目 | 規格要求 | 實測 | 結果 |
|---|---|---|---|
| 範例 trace | `dd487b019660078efe3b34680e74e6ed`，bill-svc POST /invoice，237 ms，500，tax-gw:443 失敗 | 事件群 1 與 3 都挑到這條：`事件期間最具代表性的失敗 trace（入口 500，tax-gw:443 失敗；dd487b01…，237 ms）`，呼叫鏈印出 `bill-svc external→tax-gw:443：223.0 ms，失敗` | ✓ |
| 舊範例 | 244 ms／200 那條不再出現 | 沒有出現 | ✓ |
| bill-svc 根因第 1 名 | bill-svc，`dependency = tax-gw:443（external/http）` | 事件群 3：`#1 bill-svc score=1.0 dependency='tax-gw:443（external/http）'` | ✓ |
| bill-svc reason | 含程式自身 ≈ 0、等待 tax-gw 增量、資源失敗筆數 | `程式自身 13.2→12.6 ms（-0.6）、等待 tax-gw:443，external/http 13.3→101.4 ms（+88.1，資源失敗 1→13 筆）` | ✓ |
| ord-svc（規格外，順帶） | — | 事件群 2：`#1 ord-svc dependency='oracle（db/oracle）'`，`程式自身 22.5→21.1（-1.4）、等待 oracle 21.3→66.4（+45.1，資源失敗 0→17 筆）`，日誌暴增 `ORA TNS:Connect timeout` 15 倍 | ✓ |
| cart-api 那行 | 程式自身、等待 redis、資源失敗 | 事件群 1：`程式自身 7.4→10.9 ms；等待 redis（db/redis） 0.9→0.8 ms（資源失敗 3→22 筆）`；根因「證據不足：cart-api +3.4 ms」（< 5 ms 門檻，跟改前一樣不指認） | ✓（數值見下註） |
| 回溯 | `before_minutes_used = 15`、報告無「已擴展」 | 三個事件群都是 `before_minutes_used: 15, extended_for: ""`（基準窗入口 72～82 筆 ≥ 20），報告沒有擴展字樣 | ✓ |
| span 數 | 7,608（14:35–16:16 探測窗） | 事件群 1 撈到 7,089（14:41–16:15）、2 撈到 3,120、3 撈到 3,369，`truncated: false`；欄位變多、筆數只跟窗口有關 | ✓（窗口不同，見下註） |
| 建議處置 | 第一名有 dependency 時多一條 | 事件群 3：`2. bill-svc 的增量主要在等待 tax-gw:443（external/http）：先查該依賴的延遲、錯誤與連線池／配額，而不是 bill-svc 自己的程式與機器` | ✓ |

**數值跟第 0 節預演表不同的原因**：預演用的是手設窗口（基準 14:35–14:50、事件 14:50–16:15、五個服務一起），正式管線的基準窗是**每個事件群自己的**
事件開始前 15 分鐘（bill-svc 事件 15:40 開始，基準 15:25–15:40，那時 tax-gw 已經開始變慢，所以基準 13.3 ms、事件期間 101.4 ms）。
結論一致：bill-svc 的增量 100% 是等 tax-gw，程式自身沒有變慢。

### 8.2 測試

- 全套 `tests/`：**633 通過、1 skip、10 xfail**，既有測試零斷言修改（含 `test_rca_evidence` 兩套標準答案 2/2、`test_claude_md_examples`）。
- `test_architecture.py` 全綠（過程中它抓到 `_span` 裡一個只設預設值的 `except`，改成型別判斷、不用 try）。
- 新增 `tests/test_rca_dependency.py` 9 個案例（規格要 8 個，失敗歸因多拆一個）。

### 8.3 順手看到、沒改的

- `rca.section.suggested_actions` 的關鍵字比對是子字串：`duration` 含 `io`，所以延遲事件會多一條「檢查磁碟容量與 I/O 等待」。這是既有 bug，不在這次範圍，留給第二步（建議生成）一起處理。
- 三個事件群沒有被收斂成一件（`topology_chain` 靠呼叫邊，這批 trace 跨服務呼叫 0 條），所以 cart-api 的事件群自己看不到 bill-svc／ord-svc 的根因；`trace_evidence` 段落仍列出所有服務的拆解，人看得到。
