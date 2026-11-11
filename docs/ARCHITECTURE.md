# AIOps 平台：架構、測試結果、模組手冊

三部分：**一、架構與各階段功能（新舊差異）**　**二、過往測試結果**　**三、模組使用手冊**
標記：【新】= 認知型 RCA 新增（在分支 `claude/cog-*`，尚未合併進 `arch/cognitive-rca`）；【舊】= 舊判斷層，新贏了再刪；無標記 = 不動。

---

## 一、架構與各階段功能

### 1.1 總覽

```
偵測管線（不動，不依賴 LLM）
  ingestion.fetch → detector.* → detector.ensemble → grouping → scoring → absorb_open
    → suppression → notify_payload → heartbeat → notifier.*  → persist
                                  │ 只交出「哪個實體、哪段時間」（事件群）
                                  ▼
RCA 管線（新舊並排）
  舊：fetch_traces / build_topology / fetch_logs
        → rca.root_cause.evidence（規則＋拓撲 fallback）
        → rca.report（含 Runbook 比對矩陣）→ llm_summary / llm_briefing（可 noop）
  新：fetch_traces / build_topology / fetch_logs
        → rca.facts（事實）→ enrichment.extract_evidence（LLM 抽證據）
        → rca.root_cause.bayes（機率排名）→ rca.report → enrichment.rca_narrator（LLM 寫摘要）
```

### 1.2 各階段功能與新舊差異

| 階段 | 舊做什麼 | 新做什麼 |
| :-- | :-- | :-- |
| 偵測（detector.*） | MAD／Z-Score／分位數／168 時段等算子，基準線存 DB | 不動 |
| 收斂／計分／抑制／通知 | 事件群、靜音、冷卻、Alertmanager | 不動；仍**嚴禁依賴 LLM** |
| 證據撈取 | `ingestion.incident_traces/logs`、拓撲沉澱（`incident_series` 保留但目前沒掛在管線上） | 不動 |
| 事實（證據層） | 解讀混在 `spans.py`／`evidence.py`（子 span 晚結束直接記成時鐘偏移） | 【新】`rca.facts` 只陳述事實（如 `child_outlives_parent=13000 ms`），確定性、不呼叫 LLM |
| 日誌理解 | `logs.py` 樣板與暴增排序，只當旁證 | 【新】`enrichment.extract_evidence`：LLM 把日誌讀成封閉詞表證據，`quote` 必須逐字出現在原日誌 |
| 根因排名 | 規則與圖演算法疊加分數；傳導上限 `MAX_HOPS=10` | 【新】`rca.root_cause.bayes`：noisy-OR 後驗，環先縮點、不遞迴，同輸入同排名 |
| Runbook／處置 | `sections/actions.py` 比對矩陣（約 353 行） | 沿用；新引擎的假設與證據 kind 之後對應 Runbook |
| 戰情摘要 | `llm_summary`／`llm_briefing`，可 `noop` 退模板 | 【新】`enrichment.rca_narrator`，只吃已算好的機率與路徑；驗證不過退模板 |
| LLM 角色 | 可選潤飾 | RCA 路徑**必要**：抽證據＋寫摘要；**不排名、不下根因結論** |
| LLM 失效 | 退模板，結論照出 | 報告照出，`rca_status="undetermined"`、寫原因碼，根因欄「無法判定」，附事實與原始 trace／日誌入口；不退回舊規則、不無限重試 |
| 證據不足 | 回空清單＋`rca_root_note` | 同 |
| 刪舊條件 | — | 混沌 1–13 不輸 199／200，且 16／17／21 轉綠或歸因正確才刪舊判斷層與其專屬測試 |

### 1.3 接口型別（`aiops/engine/rca_types.py`）

```
Fact(kind, entity, value, unit, window, detail, source)          證據層輸出（frozen）
EvidenceItem(kind, entity, state, confidence, quote, log_ts)     LLM 抽取輸出（frozen，kind 在 EVIDENCE_KINDS 內）
EVIDENCE_KINDS = circuit_breaker_open, gc_pause, oom, connection_pool_exhausted, deadlock,
                 lock_wait, client_timeout, dependency_unreachable, deployment_change, broker_unavailable
ctx.rca_facts / rca_evidence / rca_status("ok"|"undetermined") / rca_undetermined_reason
根因輸出沿用 RootCandidate：score=後驗，attribution["posterior"]=機率拆解
```

### 1.4 事實 kind（`rca.facts` 產出）

| kind | 掛在 | 值 |
| :-- | :-- | :-- |
| `entry_latency` | 服務 | 入口 span 平均總耗時（ms） |
| `entry_failure_rate` | 服務 | 入口失敗比例 |
| `failed_without_failed_child` | 服務 | 入口失敗且整條同步鏈沒有失敗 span 的比例 |
| `self_time_delta` | 服務 | 每請求自身耗時（區間聯集）事件 − 基準（ms） |
| `wait_delta` | 服務 | 每請求等某資源葉節點的時間 事件 − 基準（ms） |
| `inbound_rate` | 服務 | 每分鐘入口數 事件 ÷ 基準；`by_caller` 拆來源 |
| `traffic_drop` | 呼叫端 | 對 callee 每分鐘呼叫數 事件 ÷ 基準 |
| `fanout_ratio` | 呼叫端 | 每個入口請求打 callee 幾次 事件 ÷ 基準 |
| `child_outlives_parent` / `child_precedes_parent` | 父服務 | 子 span 晚於父結束／早於父開始最多幾 ms（只陳述，不判斷原因） |
| `log_template_surge` | 服務 | 錯誤日誌樣板頻率 事件 ÷ 基準 |
| `mq_lag` | 消費者 | 事件期間訊息延遲 P95（ms） |
| `metric_shift` | 實體 | 事件群原始時序 事件平均 − 事前平均 |

樣本不到 `min_requests`（預設 20）就不產出，原因記進 `ctx.log`；值為 0 或負照樣產出（證據層不判斷算不算異常）。

---

## 二、過往測試結果

### 2.1 全套測試

- main `4e71f13`：pytest **1233 passed / 1 skipped / 19 xfailed**；架構測試 24 passed；快照回放 75 異常點／3 事件群／errors 0。
- 新引擎各分支（各自在 `9b86f35` 上獨立跑，未合併）：facts 1267 passed；llm 1269 passed／1 skipped／19 xfailed；bayes 1257 passed／20 xfailed。
- 合併後全套尚未跑。

### 2.2 混沌場景（`scripts/chaos/`，世界模擬器＋記憶體 ES 替身＋真實 PostgreSQL 16＋平台真實管線）

目前 main：**1–13 共 21 列，199／200 斷言**（20 列全過；12e 為已知死區）。逐場景：

- **1 隱性延遲陷阱**　測資：ord-svc 呼叫 payment-gw 20→3000 ms，應用吞例外回 200，0 筆 500、1220 個客戶端 span 逾時　結果：6／6，歸因 `payment-gw:443`（external/http）佔比 1.0，RB-01
- **2 離峰超低流量**　測資：bill-svc 每 5 分鐘 2～3 筆，注入 billing-db 8→5000 ms 且 100% 失敗　結果：修前 8／9（注入前誤報：低流量把「當分鐘總耗時」當延遲）→ 修後 9／9（查詢加 `divide_by`）；基準窗自動 15→60 分，RB-02
- **3 兩下游各 45%**　測資：cart-api 總耗時 +1000 ms，redis +450、postgres +450、自身 +100　結果：修前 6／7（`attribution` 只記 redis）→ 修後 7／7（`others` 列出 postgres 0.448）
- **4 連鎖雪崩**　測資：gateway→order-api→inventory-svc→oracle，oracle 12→4000 ms 且 50% 失敗　結果：4／4，三服務歸同一事件群，根因第 1 名 inventory-svc，RB-02
- **5 沒有子 span 的崩潰**　測資：純程式 Exception，事故窗無資源葉節點　結果：修前 7／8（`attribution` 殘留 redis share 0.0）→ 修後 8／8（空 attribution）；RB-03
- **6 LLM 幻覺注入**　測資：假模型回「重啟 k8s-node-99、查 Kafka」、虛構節點、虛構服務名、合法回文各一組　結果：未通過 3 條——原文被攔（靠數字 99，不是白名單）；`gpu-farm-prod`（無數字）與 `payment-gateway` 穿過白名單；無 `HallucinationBlocked` 日誌。**已知死區**，之後以封閉詞表守門處理
- **7 單點毛刺的分位數反推**　測資：1 分鐘與 3 分鐘毛刺（mad 分數約 80）　結果：修前 8／10（1 分鐘毛刺退回 ×1.2）→ 修後 10／10（`fetch_span` 含兩端）
- **8 黃金事故防護罩**　測資：zscore 3.0→10.0 的破壞性調參（25.0 在建立階段被範圍擋下 HTTP 400）　結果：9／9，事故窗異常 8→0、`safety.passed=false`、approve 回 409；溫和提案 3.0→3.5 通過
- **9 假日前夕流量腰斬（ACF）**　測資：含 3 天連假的週期資料；`sum` 聚合只有 7.5 天　結果：修前 5／6（空桶被當 0，算出 r(168h)=−0.221 宣稱有週期）→ 修後 6／6（覆蓋率不足一律「資料不足」，不提案）
- **10 JSONB 併發更新**　測資：真實 PostgreSQL，10 worker 各追加 5 次、並行首次寫同一事故　結果：修前 5／7（50 鍵只留 5～7、4～6／10 撞唯一鍵）→ 修後 8／8（50／50 鍵、0 IntegrityError）
- **11 並行子 span**　測資：asyncio.gather 同時打 DB 與外部金流　結果：通過（區間聯集歸因）
- **12 訊息佇列（12a～12g）**　測資：生產者壞版本、消費者自身變慢、舊版 agent 缺 link、3 小時延遲、偵測端併卡　結果：通過；**12e（生產者毫無症狀的毒訊息）為已知死區**，`xfail(strict=True)` 留存
- **13 流量放大者（13a～13c）**　測資：order-svc 對 payment-svc 呼叫放大 25 倍；放大者無告警；外部流量突波對照　結果：通過

誠實披露：ES 是記憶體替身；世界資料由自寫模擬器產生；每場景單一隨機種子；`divide_by` 全面套用在場景 1／8／12g／13 會出現新誤報（成因未查），故預設查詢未動；真實資料對照（11/3、5 服務）原查詢 75 異常點／3 事件群，換 `divide_by` 為 157／4，多出來的是真事故還是誤報未驗證。

### 2.3 認知型 RCA 混沌並排（`run.py --compare`）

- 舊引擎 1–13：輸出與改動前逐列相同，199／200（cog-chaos 分支實測）。
- 新引擎：**尚未跑**（`rca_report_cognitive.yaml` 尚未建立，`run.py --rca cognitive` 目前直接 exit 2）。
- 場景 24（LLM 全掛）24a down／24b slow／24c garbage／24d hallucinate／24e faithful 對照：已寫好，等新引擎接上才能跑。
- 假 LLM（`scripted_llm.py`）只測平台怎麼處理 LLM 輸出，**不代表真模型抽取品質**；真模型品質尚無人驗。
- 已知死區（bayes）：兩個獨立故障同時發生；熔斷但日誌沒有熔斷字樣。先驗全為手設，未經混沌或真實資料校準。

---

## 三、模組使用手冊

寫法：第一層是檔案，其下是內容，`#` 右邊是解釋。

```
aiops/
├─ engine/                           管線骨架與型別（不放業務演算法）
│  ├─ context.py                     # PipelineContext：整條管線的強型別資料載體，每輪新建
│  │   ├─ rca_facts / rca_evidence   # 【新】證據層事實、LLM 抽出的證據
│  │   └─ rca_status                 # 【新】ok／undetermined；rca_undetermined_reason 寫原因碼
│  ├─ rca_types.py                   # 【新】Fact、EvidenceItem、EVIDENCE_KINDS、RcaStatus
│  ├─ pipeline.py                    # 讀 YAML、按序實例化算子並 execute；scope（target／batch／after_persist／after_finalize）、序列隔離
│  ├─ registry.py                    # OperatorRegistry：@register 登記、動態建立
│  ├─ operator.py                    # BaseOperator 合約
│  ├─ params.py                      # number／positive／whole 參數檢查、deep_merge
│  ├─ errors.py                      # InfrastructureError、NoDataError 等
│  ├─ single_flight.py               # 同一事件群併發請求合併成一次 RCA（行程內）
│  └─ types.py／topology.py／mq.py／requests.py／signature.py   # 事件、拓撲、訊息佇列、請求與簽章型別
│
├─ sources/                          外部資料源適配（只輸出 DataFrame）
│  ├─ elasticsearch.py／prometheus.py／mock.py   # 各資料源；base.py 為共同介面，_http.py 重試 2 次
│  └─ query/                         # 查詢語法解析：elasticsearch.py（含 divide_by）、promql.py、trace.py、window.py
│
├─ stages/                           管線算子，依執行順序分段
│  ├─ ingestion/fetch.py             # ingestion.fetch／incident_series／incident_traces／incident_logs
│  ├─ detectors/                     # 異常偵測
│  │  ├─ base.py                     # DetectorOperator：載基準線、調參、warning、訓練排除；演算法只寫 fit／detect
│  │  ├─ mad.py／zscore.py／hourly_baseline.py／quantile_drift.py／residual_shift.py／residual_chronic.py
│  │  ├─ rules.py                    # 規則偵測，不需訓練
│  │  ├─ hard_limit.py               # 致命軌：單點踩線成立、不投票、界線只認 YAML
│  │  └─ new_only.py                 # 只報新出現的實體
│  ├─ ensemble/ensemble.py           # detector.ensemble 多數決；bypass 的節點事件原樣留下
│  ├─ grouping/                      # group.py 收斂；absorb.py 吸收進行中事件群
│  ├─ scoring/score.py               # 計分、alert.fatal_floor
│  ├─ suppression/suppress.py        # 靜音窗、冷卻、同根因
│  ├─ notification/                  # payload.py、heartbeat.py（補送 endsAt）、notifiers.py、transport.py、links.py
│  ├─ rca/                           # 根因分析
│  │  ├─ base.py                     # RootCauseOperator.rank、RcaSection（applies／build）
│  │  ├─ report.py                   # rca.report 段落組裝（不 import 具體段落或演算法）
│  │  ├─ spans.py／logs.py／mq.py／topology_builder.py   # 證據層純函式：span 統計、日誌樣板、佇列、拓撲沉澱
│  │  ├─ facts/                      # 【新】rca.facts：spans.py／logs.py／mq.py／series.py／windows.py／operator.py
│  │  ├─ bayes/                      # 【新】rca.root_cause.bayes
│  │  │   ├─ graph.py                #   呼叫圖、強連通分量縮點（迭代）
│  │  │   ├─ model.py                #   noisy-OR 推論
│  │  │   ├─ observe.py              #   事實／證據 → 觀測
│  │  │   ├─ priors.py               #   手設先驗（附出處註解、可調）
│  │  │   └─ operator.py             #   算子；證據不足回空清單＋rca_root_note
│  │  ├─ algorithms/                 # 【舊】evidence.py（約 520 行）、topological.py
│  │  └─ sections/                   # overview／evidence／graph／log_patterns／similar_cases；【舊】actions.py（Runbook 矩陣）
│  └─ enrichment/                    # LLM（唯一可連外的 stages 目錄）
│     ├─ base.py／registry.py／providers.py   # BaseLLM.complete、LLMError、@register 供應商
│     ├─ narrative.py／summarizer.py # 舊：llm_summary／llm_briefing（可 noop），詞表守門
│     ├─ evidence_extractor.py       # 【新】enrichment.extract_evidence，無 noop；失敗 → undetermined
│     └─ rca_narrator.py             # 【新】enrichment.rca_narrator，驗證不過退模板（旁路）
│
├─ control_plane/                    # 手動動作 admin.*（唯一可 import storage 的算子層）
│  ├─ learning/                      # golden.py 黃金事故、learning.py 回饋、profile.py ACF 體檢、replay.py 回放、
│  │                                 # threshold.py 分位數門檻、tuning.py／tuning_rules.py 調參與規則升級
│  └─ operations/                    # incidents.py、ingest.py、mappings.py 欄位對照、silences.py、sources.py、topology.py
│
├─ storage/                          # 持久化（不含業務判斷）
│  ├─ models.py／db.py／crud.py／repository.py／hooks.py   # 表定義、交易、upsert、Repository、StorageHooks
│  └─ （DB 只記最終結果：Incident、Audit、基準線；節點之間不經 DB 傳資料）
│
├─ pipelines/                        # 流程只在 YAML 宣告，名稱全域唯一
│  ├─ detection/                     # factory／it_apm／it_otel／metrics_only
│  ├─ rca/                           # rca_report.yaml；【新】rca_report_cognitive.yaml（待建）
│  └─ control/                       # admin_*.yaml（核准、回測、靜音、拓撲沉澱…共 20 支）
│
└─ interfaces/                       # 入口（零業務邏輯，嚴禁 if／for／try）
   ├─ app.py／runner.py              # 建立 app；runner 是組裝點、workflow 只接名稱
   ├─ api/                           # alerts.py、ingestion.py、runs.py、tuning.py
   ├─ bootstrap/                     # auth.py（Token 驗證）、fallback.py（錯誤處理）
   └─ cli/__main__.py                # 排程入口（K8s CronJob 或 Loop）

scripts/chaos/                       # 混沌驗收（只讀平台、不改平台）
├─ run.py                            # 跑場景：`run.py 1 2 3 --out f.md`；【新】`--rca {legacy,cognitive}`、`--compare`、`--llm <模式>`
├─ world.py／es.py／harness.py       # 世界模擬器、ES 替身、PG 與斷言 harness（`CHAOS_PG_URL` 可換資料庫）
├─ scen_detect_rca.py／scen_learning.py／scen_mq.py／scen_traffic_amp.py   # 場景 2–5,11／6–10／12／13
├─ scen_cognitive.py                 # 【新】場景 24：LLM 全掛
└─ scripted_llm.py                   # 【新】假 LLM：faithful／down／slow／garbage／hallucinate（只在腳本內登記，不進 aiops/）

tests/                               # 驗證
├─ test_architecture.py              # 24 條架構防線；【新】+2 條：告警路徑不依賴 LLM、RCA 路徑 LLM 失敗降級
├─ test_load_time_validation.py      # 寫錯 YAML 在載入期報錯
├─ test_rca_facts.py／test_rca_bayes.py／test_rca_extract_evidence.py   # 【新】三段各自測試
└─ test_chaos_*.py／test_dual_track.py／test_stability_guards.py…       # 既有行為測試
```

### 常用指令

```bash
.venv/Scripts/python -m pytest tests/ -q                          # 全套（約 90 秒）
.venv/Scripts/python -m pytest tests/test_architecture.py -q      # 架構防線
.venv/Scripts/python scripts/chaos/run.py 1 2 3 --out out.md      # 混沌；需 PostgreSQL（預設 127.0.0.1:55433）
```
