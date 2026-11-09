# 核心防線四項：檔案級設計（未動工）

狀態：設計稿；第 1、2 節已實作（PR A，見各節「實測結果」）。基底 main `781ea3c`（PR #1～#6 全部合入）。每一項先寫「程式碼實況」再寫做法，實況跟提案前提不一致的地方直接標出來。
依「先驗證不動工」，這份只是設計；動工順序建議 1、2 一個 PR，3 一個 PR，4 一個 PR。

## 0. 四個前提的核對結果

| 提案前提 | 實況（grep 過的檔案與行） | 影響 |
|---|---|---|
| 「所有標準差與 MAD 計算的分母」都會除以零暴衝 | 只有 **`detector.mad`**（`mad.py:24`：`mad if mad != 0 else 1e-6`，就是場景 5 的 1,349,000 分）與 **`hourly_baseline`** 在全序列 std 為 0 時（`hourly_baseline.py:100`：`max(g_std*0.1, 1e-9)`）。`zscore` std 為 0 時分數全 0（不炸但全盲）；`quantile_drift`／`compare` 已有退路（std → 1% center → 1.0） | 做法要分兩種：炸的補底噪，盲的也補底噪才看得到 |
| 「拔掉多數決壓制 hack」 | 多數決（`detector.ensemble` majority）是 CLAUDE.md 第 2 節的設計，真實回放少 88% 單點誤報，不是補丁 | **不拔** |
| 並行子 span 讓自身耗時 < 0，`max(0, …)` 是掩耳盜鈴 | `spans.py:81-88`：自身 = 耗時 − Σ 子耗時，再夾 0。確認成立：asyncio／多執行緒並行的子 span 會重複扣。真實 11/3 資料與兩套合成劇本的子 span 都是**依序**的，所以沒有在數字上暴露 | 要做；但 `max(0)` 要留著當時鐘偏移的保護，不是拔掉 |
| 白名單只擋已知不相干實體、黑名單人工維護 | 屬實（`narrative.py:182-194`）。數字 ±10% 檢查是另一道獨立防線，場景 6 的 A、B、D 都是它擋下的 | 封閉詞表取代黑名單；**數字檢查保留**，不拔 |
| 拓撲「靜態寫死在 YAML」、`admin_merge_traces` 是「暫時腳本」 | **不符。** 偵測時 `ctx.topology` 來自資料庫 `service_edges`（`hooks.py:70-71`，YAML 沒寫 topology 才用）；`admin_merge_traces` 是正式 API `POST /api/v1/ingestion/traces/merge`，會 `ServiceEdges.add_calls` 持久化；RCA 已有 `rca.build_dynamic_topology` 從現抓 span 還原邊。**真正缺的是「沒有人定期呼叫它」**：沒有排程從 trace 來源自動沉澱邊 | 做法是加一個排程動作，不是重寫拓撲 |
| 「沿 DAG 出入度加權與反向傳播」打分 | `rca.root_cause.evidence` 刻意不用拓撲方向：README「RCA」一節，拓撲排序與 PageRank 在 load_amplification 劇本 0／2 命中（上游放大流量壓垮下游時方向相反） | **打分不接 DAG**；DAG 用在收斂（已有 `alert.topology_chain`）與保持邊新鮮 |
| 「純靠時間窗盲目湊對的弱關聯」可拔 | `alert.same_entity` 是「同一個服務、時間相近」合成一件，不是跨服務湊對；跨服務只有 `topology_chain`（要有邊） | **不拔** |

## 1. 稀疏流量底噪（Noise Floor）

### 1.1 做法

在 `stages/detectors/base.py` 加共用純函式：

```python
def scale_floor(scale: float, values: pd.Series, min_scale: float, min_scale_ratio: float) -> float:
    """分母的保底：max(算出來的 scale, 絕對下限 min_scale, |中心| × min_scale_ratio, 整數序列的 1 個單位)。
    整數序列（失敗數、請求數）MAD 常常是 0：1 筆就是最小有意義的變動，分母至少 1。"""
```

- `detector.mad`：`self.mad_ = scale_floor(mad, values, …)`，拔掉 `1e-6`。
- `detector.zscore`：`self.std_ = scale_floor(std, …)`，`detect` 不再「std 為 0 就全 0」。
- `detector.hourly_baseline`：每時段 std 的下限 `max(g_std*0.1, 1e-9)` 改成 `max(g_std*0.1, scale_floor(g_std, …))`。
- `quantile_drift`／`compare` 既有退路保留，最後一層改走 `scale_floor`（行為不變，只是統一入口）。
- 新 tunable（兩個都進 `tunable`，可被調參、YAML、target overrides 覆蓋）：

| 參數 | 預設 | 範圍 | 意義 |
|---|---|---|---|
| `min_scale` | 0.0 | 0～1e12 | 絕對下限，單位跟指標一樣（失敗數填 0.5、延遲 µs 填 5000）；0 = 不用 |
| `min_scale_ratio` | 0.01 | 0～1 | 相對下限：中心值的 1%（延遲 1,000,000 µs 的序列至少 10,000） |

為什麼不照「指標型態」給預設：平台不知道指標型態——`AnomalyEvent.metric` 是查詢字串、`attrs.signal` 可選填。單位由 target 自己知道，所以用 `overrides` 填（`it_otel.yaml` 範例 target 補上）；相對下限 1% 是沒填時的通用保底。整數序列的「至少 1」是資料本身的性質，不用設定。

### 1.2 驗收

- 單元：失敗數序列（90% 是 0、偶爾 1～2）→ mad 分數上限有界（2 筆失敗不再是百萬分）；常數序列 → zscore 不再全 0；真實 11/3 的 5 條延遲序列用預設參數重跑，異常點數與現行**一致**（底噪只在 scale 趨近 0 時起作用，正常序列不受影響，這是零迴歸的硬指標）。
- 混沌場景 5：注入前誤報事件群數 1 → 0；場景 2 的注入前誤報**不會**被這項修掉（那是 `summary.sum` 隨請求數變動，查詢設計問題），報表照實寫。
- 既有測試零斷言修改；`tunable` 多兩個鍵會讓 `test_proposal_validation` 這類列舉 tunable 的測試需要檢查——先跑再說，不預設要改。

### 1.3 實測結果（PR A，分支 `claude/hardening-a-noise-floor-edb621`）

**做法與設計稿的差異（都是實作時才看得到的）**

- 底噪在 **detect 時**才套：`mad`／`zscore`／`hourly_baseline` 的基準線存原始 MAD／標準差＋`integer` 旗標，`scale_floor(..., center=, integer=)` 在偵測時算。理由：基準線一天重訓一次，在 fit 時套的話後台改 `min_scale` 要等重訓才生效；舊版存的 `mad = 1e-6` 基準線載入後也立刻套相對下限，不用等重訓才不爆分。
- `hourly_baseline` 每時段下限是 `max(該時段 std, 全段 std × 0.1, scale_floor(0, 中心=該時段平均))`，不是設計稿寫的 `max(g_std*0.1, scale_floor(g_std, …))`——後者 ≥ 全段 std，會把每個時段的 std 都撐到全段 std，168 時段基準線等於失效。全段 std 為 0 時「分數全 0」的特例拔掉（現在是底噪當分母）。
- 三個下限都是 0（中心 0、非整數、兩個參數都關）時退回 1.0，跟 `quantile_drift`／`compare` 既有最後一層一致。
- 附帶修正 `control_plane/learning/threshold.infer`：`target` 改成無條件進位到小數 4 位、用回報出去的 `s_max` 算。底噪讓分數從 1 億級降到 201，`round(s_max × 1.05, 4)` 就會比 `round(s_max, 4) × 1.05` 小 0.00003，`tests/test_chaos_gaps.py::test_quantile_inference_now_covers_a_one_minute_false_positive` 的 `target >= s_max × 1.05` 因此不成立。改的是生產碼，測試沒動（那場單點誤報 S_max：main 101,052,392.782 → 本分支 201.3727）。

**硬驗收 1：真實 11/3 五條序列，預設參數**（`it_otel` 換 5 條 ES targets，2026-11-03 14:00Z→16:20Z 每 5 分鐘一輪，共 29 輪、0 errors）

| 序列 | hourly_baseline | mad | quantile_drift | 多數決 | main → 本分支 |
|---|---|---|---|---|---|
| bill-svc | 31 | 28 | 39 | 31 | 一致 |
| cart-api | 17 | 13 | 70 | 16 | 一致 |
| ord-svc | 29 | 10 | 47 | 28 | 一致 |
| pay-api | 4 | 0 | 0 | 0 | 一致 |
| inv-svc | 0 | 0 | 0 | 0 | 一致 |

逐點比對（實體, 節點, 時間, 等級, 分數到小數 4 位）373 筆**完全相同**；事件群同樣 3 個（cart-api 14:56→16:15、ord-svc 15:33→16:00、bill-svc 15:40→16:10）。真實延遲序列的 MAD／std 遠大於中心的 1%，底噪一次都沒起作用。

**硬驗收 3：混沌場景 5**（真實 PostgreSQL 16，`scripts/chaos/run.py`）

| 斷言 | main | 本分支 |
|---|---|---|
| 注入之前沒有誤報事件群（cart-api 失敗數序列） | 1 個（11:37）✗ | **0 個 ✓** |
| cart-api 事件群數 | 4 個（誤報 1 + 事故被切成 3 段） | 1 個 |
| 場景總計 | 6／8 | 7／8（剩下的 ✗ 是「attribution 應為空」，跟底噪無關，維持原缺口） |

場景 2 的注入前誤報**沒有被修掉**（main 與本分支都是 1 個，bill-svc 01:33）：那條是 `transaction.duration.summary.sum` 隨請求數變動，查詢設計問題，不是分母趨近 0。

常數序列單元測試（`tests/test_hardening_noise_floor.py`，17 案）：失敗數序列（90% 是 0）MAD 真的是 0，2 筆失敗 = 1.349 分（改版前 1,349,000 分）、40 筆爆量 26.98 分照樣抓；`zscore` 常數 100 → 150 是 50 分、異常（改版前 0 分）；相對下限 1%（中心 100 萬、std 0.5 → 分母 10,000）；整數序列至少 1；`hourly_baseline` 常數序列有分數；舊基準線（`mad = 1e-6`、沒有 `integer`）載入後不爆分；`min_scale` 由 target overrides 改了不用重訓就生效；五個偵測器的 tunable 範圍（負值、超過 1e12、ratio > 1 都擋）。

既有斷言修改 1 處：`tests/test_detectors.py::test_mad_constant_training_does_not_divide_by_zero` 原本斷言「常數 5 的序列出現 6 是異常」——那正是 1e-6 分母（674,500 分）的行為，跟本節「整數序列 1 筆是最小有意義的變動」直接衝突。改成更強的斷言：6 是 0.6745 分不是異常、15 是 6.745 分是異常、全部有限。

## 2. Span 區間聯集（Interval Merging）

### 2.1 做法

`stages/rca/spans.py` 的 `self_times`：

```python
# 子 span 的 [start, start+duration] 先夾進父 span 的區間，排序後合併重疊，自身 = 父耗時 − 聯集長度
```

- 用 `SpanRecord.ts`（開始）與 `duration_ms`，不需要新欄位。
- 子區間超出父區間的部分不算（時鐘偏移、APM 取樣誤差）。
- `max(0, …)` 保留：聯集後理論上不會負，剩下的負值只來自時鐘偏移，夾 0 並在 `ServiceWindowStats` 多記 `clock_skew_spans`（被夾的筆數），報告段落有數字才看得到。
- `call_chain` 印的「自身」同步改。`window_stats`、`resource_waits` 都吃 `self_times`，自動一致。

### 2.2 驗收

- 單元：父 100 ms，兩個子各 60 ms **並行**（同一起點）→ 自身 40（現行算出 0）；兩個子依序 → 自身 −20 夾 0（現行同）；子超出父範圍 → 超出部分不扣。
- `test_rca_evidence` 兩套標準答案 2／2 不變（產生器子 span 依序，聯集 = 加總）。
- 真實 11/3 回放：bill-svc／ord-svc／cart-api 的 attribution 數字**不變**（子 span 依序）；報表附 `clock_skew_spans` 數。
- 混沌加一個場景 11：`asyncio.gather` 式並行（ord-svc 同時打 inventory-db 與 payment-gw）→ 自身耗時正確、attribution share 不被灌水。

### 2.3 實測結果（PR A）

**做法與設計稿的差異：前提「真實 11/3 的子 span 都是依序的」不成立。** 撈 11/3 15:40–15:50 的 bill-svc trace，有 ≥ 2 個子 span 的父 span **49／49 個兄弟重疊**：`app` span 從 0 開始（9.2 ms），`external` tax-gw span 在 2～5 ms 處就開始。照設計稿字面「所有子 span 夾進父區間、排序、合併重疊」，bill-svc 程式自身 13.2→12.6 會變成 18.2→17.2、ord-svc 22.5→21.1 變成 29.6→28.7（每請求自身比總耗時還大），`tests/test_rca_dependency.py` 兩條既有斷言也會紅（它的 fixture 把 app 跟資源葉節點都放在父 span 的起點）。所以實作是：

- 自身 = 耗時 − `span.type = app` 子 span 的長度**加總** − 其他子 span（db／external／cache／下游服務）區間的**聯集**，下限 0。app span 是服務自己的程式碼、跑在請求自己的執行緒上，跟同一請求的 I/O 是先後關係；asyncio.gather／執行緒池的並行只發生在 I/O 子 span 之間。
- `@timestamp` 是毫秒精度：兄弟重疊不到 1 ms 當成依序；子 span 超出父區間不到 1 ms 照整段扣、不算時鐘偏移；超過 1 ms 才夾進父區間並記 `clock_skew_spans`。沒有這個容忍值，混沌場景 1、3 的依序子 span 會因截斷出現假重疊，程式自身漂 +0.2 ms。
- 相對父 span 起點的毫秒數用 `timedelta` 算（epoch 毫秒直接相減會掉到 1e-4 ms 精度）。
- 坑：並行時每個資源的等待各算各的，「程式自身＋各資源等待」可以大於總耗時（兩個並行 60 ms 等待 = 120 ms）。這是刻意的：每個資源真的各讓請求等了那麼久；README「RCA」一節寫明。

**硬驗收 2：真實 11/3 三個事件群的 attribution**（`rca_report`，`root_cause.candidates[0].attribution`）

| 事件群 | 候選 | resource | self_ms 前→中 | wait_ms 前→中 | failures 前→中 | share | main → 本分支 |
|---|---|---|---|---|---|---|---|
| cart-api 14:56→16:15 | （證據不足，無候選） | — | — | — | — | — | 一致（都沒有候選） |
| ord-svc 15:33→16:00 | ord-svc 1.0 | oracle（db/oracle） | 22.5→21.1 | 21.3→66.4 | 0→17 | 1.0 | 一致 |
| bill-svc 15:40→16:10 | bill-svc 1.0 | tax-gw:443（external/http） | 13.2→12.6 | 13.3→101.4 | 1→13 | 1.0 | 一致 |

`clock_skew_spans`：三次 RCA 撈到 7,089／3,120／3,369 個 span，時鐘偏移 **0**，報告沒有多那一行；`trace_evidence` 段落 16 行裡唯一不同的是最後一行自身耗時的定義文字。`tests/test_rca_evidence.py` 兩套合成標準答案（`nov03_callee_slowdown`、`nov03_cascade`）× 3 個 seed：**6／6 通過，跟 main 一樣**。

**硬驗收 4：混沌場景 11**（`scripts/chaos/scen_detect_rca.py::scenario_11`；`world.py` 的 `Svc(parallel=True)` 讓所有下游呼叫同一起點，請求耗時 = 自身 + 最慢的那個）

注入：ord-svc 自身 40 ms，並行打 inventory-db 200 ms 與 payment-gw:443 200 ms；12:00 起 payment-gw 200→800 ms、ord-svc 自身 40→240 ms。真值：程式自身 40→240、payment-gw 等待 200→800、佔比 600 ÷ 800 = 0.75。

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| 世界資料：兩個子 span 同一起點 | 全部 | 2236／2236 個請求 | ✓ |
| 偵測到 ord-svc 事件群 | 有 | 1 個（15 輪、異常 40 筆） | ✓ |
| 候選 ord-svc、下鑽 payment-gw:443 | ord-svc／payment-gw:443 | ord-svc／payment-gw:443 | ✓ |
| 事件前程式自身（不再被雙重扣成 0） | 40 ±6 ms | 40.2 | ✓ |
| 事件中程式自身 | 240 ±36 ms | 239.9 | ✓ |
| payment-gw 等待 | 200 ±30 → 800 ±120 | 202.7→803.1 | ✓ |
| 歸因佔比不被灌水 | 0.75 ±0.05，且低於改版前算法 | 0.752（改版前算法 0.933） | ✓ |
| clock_skew_spans | 全部 0 | ord-svc／cart-api／pay-api 前後都 0 | ✓ |
| 報告沒有時鐘偏移那一行 | 不含 | 不含 | ✓ |
| 變體：20 個請求的 payment-gw span 起點挪到父 span 前 30 ms、耗時蓋過父 span → 父自身 | 20 筆都是 0 | 20／20 | ✓ |
| 變體：clock_skew_spans | ord-svc 事件期間 20 | 20 | ✓ |
| 變體：報告印出偏移那一行 | 含「ord-svc 20 筆」 | 「時鐘偏移：…——ord-svc 20 筆」 | ✓ |

同一份 span 用改版前算法（耗時 − 子 span 加總、下限 0）：程式自身 **0.0→45.4 ms**（真值 40→240）、佔比 **0.933**（真值 0.75）——並行把程式自身吃掉，等待佔比被灌水。

**全套混沌 11 個場景**（本分支）：83／89 斷言、7／11 場景全過。跟 main 比只有場景 5（6／8 → 7／8）與新增的場景 11（12／12）不同；場景 1～4、6～10 每條斷言、每個數字都一樣（只有隨機產生的 incident uid 不同）。

單元（`tests/test_span_intervals.py`，12 案）：並行子 span（父 100、兩個 60 同起點 → 40）、部分重疊、依序（= 加總）、毫秒截斷的假重疊、依序但超出父（→ 0、記偏移）、子 span 跑出父尾（只扣父區間內）、子 span 早於父（→ 0、記偏移、`window_stats` 計數）、1 ms 內不算偏移、app span 加總不聯集（11/3 的形狀 → 3.5）、`call_chain` 印聯集後的自身、段落沒偏移時一字不多。

## 3. 封閉世界詞表（Closed-World Lexicon）

### 3.1 做法

`stages/enrichment/narrative.py`：

- 從三行文字抽「識別字 token」：含英數的連續片段，允許 `- _ . : / @` 連接（`payment-gw:443`、`POST /invoice`、`dd487b01…`、`k8s-node-99`、`gpu-farm-prod`、`java.lang.IllegalStateException`、`RB-01`）。中文敘述不抽（它不是實體）。
- 合法集合 `S_valid` = context JSON 裡出現過的所有識別字 token（事件實體、資源、端點、trace id、Runbook id、日誌樣板、Runbook 處方文字）∪ 一份**固定的通用技術詞表**（HTTP、HTTPS、ms、timeout、client、pool、rollback、trace、span、P0～P3、API、DB、SQL、latency…，放在 `narrative.py` 常數，不是人工維護的黑名單而是放行清單）。
- 文字裡任何識別字 token 不在 `S_valid` → 退回模板，原因寫出那個 token。
- **移除** `_HALLUCINATION_WORDS` 黑名單（封閉詞表涵蓋它）。
- **保留** 數字 ±10% 檢查與既有的「拓撲已知實體不在白名單」檢查（後者變成封閉詞表的特例，可以併掉但先留著對照）。

### 3.2 驗收

- 場景 6 全部變體：A（Kafka＋k8s-node-99）、A2（只有 Kafka）、B、B2（gpu-farm-prod）、C（payment-gateway）、D（99）全擋；E 合法放行。
- 回放兩次真實 OpenAI 的回文（步驟二驗收存的 bill-svc 三行）必須**仍然通過**：裡面的 `HTTP client timeout`、`Max Connections`、`POST /invoice`、`java.lang.IllegalStateException` 都在 context 的 Runbook 處方與日誌樣板裡。
- 誤拒率：對 10 條用模板產生的三行（它們只用 context 內容）跑驗證，0 條被拒。
- 死區照實寫：中文自由敘述裡的幻覺（「請重啟資料庫主機」）不含識別字，封閉詞表擋不到；這是語言模型總結的固有風險，報告標 `generated_by`。

## 4. 動態 DAG 拓撲：補的是「自動沉澱」，不是重寫

### 4.1 做法

- 新控制面動作 `admin.topology_refresh`（`control_plane/operations/topology.py`、`pipelines/control/admin_topology_refresh.yaml`、API `POST /api/v1/ingestion/topology/refresh`，排程用 `run-once -w admin_topology_refresh`）：
  1. 從 trace 來源撈最近 `lookback_minutes`（預設 60）的 span（走既有 `fetch_spans`，含抽樣上限）；
  2. 用既有 `engine/topology.service_edges` 算 (caller, callee, 次數)；
  3. `ServiceEdges.add_calls` 累加；`ServiceEdges.prune(older_than_days=7)`：超過 7 天沒再出現的邊移除（新增 `last_seen` 欄位＋migration 0006，可重複執行）；
  4. 結果回報新增／更新／移除的邊數。
- `alert.topology_chain` 不改（它已經沿邊收斂，場景 4 證明可傳遞到 3 層）。
- `rca.root_cause.evidence` **不接 DAG 權重**（理由在第 0 節）；`rca.build_dynamic_topology` 已做現場還原。
- YAML 的 `topology:` 保留為「明確覆寫」（`topology_explicit`），文件寫明：有排程刷新就不用寫。

### 4.2 驗收

- 混沌場景 4 改成**不**事先灌邊：偵測回放前先跑一次 `admin_topology_refresh`（從世界的 trace 來源），三個服務仍歸同一個事件群、根因仍 inventory-svc。
- 真實 ES：對 11/3 跑 `admin_topology_refresh` → 邊數 **0**（這批 trace 沒有跨服務呼叫），結果要如實回報 0 而不是報錯；`service_edges` 不被清空（prune 只刪 7 天沒見的）。
- 單元：累加、prune、空 trace、抽樣截斷都有。

## 5. 第二階段「可淘汰」清單的核對

| 提案說可拔 | 核對 | 結論 |
|---|---|---|
| 「分數爆到幾百萬」的外層特例與多數決 | 沒有針對爆分的特例程式（grep 不到）；多數決是設計 | 沒東西可拔；多數決保留 |
| `max(0, self_time)` | 時鐘偏移仍需要 | 保留並計數 |
| 數字比對規則 | 場景 6 實測它是擋下 3 個變體的主力 | 保留 |
| 人工黑名單 `_HALLUCINATION_WORDS` | 封閉詞表涵蓋 | **可拔**（第 3 項做完） |
| 靜態 YAML 拓撲 | 已是覆寫用途，預設走資料庫 | 保留為覆寫，文件改寫 |
| `admin_merge_traces` | 正式 API，Kafka／批次匯入也靠它 | 保留 |
| 時間窗湊對 | `same_entity` 是同服務合併，不是跨服務 | 保留 |
| 「單純看筆數的回溯放大器」 | 自適應回溯以入口請求數為門檻（`min_entry_spans`）是對的：trace 統計的樣本量就是請求數，不是方差 | 保留；要改的是 `min_requests` 一致性，不在這四項 |

## 6. 建議的動工順序與每個 PR 的硬驗收

1. **PR A：底噪＋區間聯集**（純演算法，兩個檔案為主）。驗收：真實 11/3 五條序列異常點數不變、attribution 數字不變、場景 5 注入前誤報 0、新場景 11 並行 span 自身耗時正確、全套綠、零斷言修改。
2. **PR B：封閉詞表**。驗收：場景 6 七個變體全對、兩次真實 OpenAI 回文仍過、模板三行誤拒 0。
3. **PR C：拓撲自動沉澱**。驗收：場景 4 不灌邊也能收斂、真實 ES 回報 0 邊不報錯、migration 0006 在 PostgreSQL 16 實跑。
