# 破壞性端到端驗收報告（10 個場景）

日期：2026-11-09　基底：main `35d7326`（PR #1～#5 全部合入）　程式：`scripts/chaos/`（隨 `fix/chaos-e2e-gaps` 一起進 repo；修復後的結果見下一節）

**結論先講**：10 個場景 76 條斷言，64 條通過、12 條未通過；完全通過 3 個場景（1、4、8）。**沒有任何場景讓平台崩潰、也沒有被騙過的假綠燈**（平台該拒絕的都有拒絕、該降級的都有降級）；
未通過的 12 條斷言拆開是：**5 個平台缺口（共 7 條斷言）**、**3 個設計落差（3 條，其中 1 條是你要的日誌字樣平台本來就沒有）**、**2 條注入前誤報（查詢欄位選擇造成的地雷）**。逐項見「缺口清單」。依「先驗證不動工」，這輪只回報、沒有修平台。


## 修復後（分支 `fix/chaos-e2e-gaps`，2026-11-09）

依優先序修了 P0（場景 10）與 P1（場景 7、9），平台改動只在 `storage/crud.py`、`storage/repository.py`、`control_plane/learning/{tuning,replay,profile}.py`、
`pipelines/control/admin_series_profile.yaml`；其餘缺口（場景 2、3、5、6）維持未修，報表照原樣。

| 場景 | 修復前 | 修復後 | 改了什麼 |
|---|---|---|---|
| 10 JSONB 併發 | 5／7（讀改寫 50 鍵只留 5～7；首次寫入 4～6／10 撞唯一鍵） | **8／8**（50／50 鍵、0 個 IntegrityError） | `crud.upsert` 改 `INSERT … ON CONFLICT DO UPDATE`（PostgreSQL／SQLite；只給部分欄位時走純 UPDATE，列不存在就 KeyError）；新增 `Knowledge.merge_context`：資料庫端 `context \|\| patch`（SQLite 用 `json_patch`），`admin.feedback_submit` 的 true_positive 改成「upsert 欄位 → merge_context 追加」，不再 Python 讀改寫 |
| 7 單點毛刺 | 8／10（1 分鐘毛刺退回固定 ×1.2：3.5→4.2，「時窗內沒有資料」） | **10／10**（1 分鐘與 3 分鐘毛刺都走分位數反推：S_max 80.0、Q 2.382、目標 84.0→夾到 20.0→單次 50% 切到 5.25，reason 寫明還差 14.75） | `replay.fetch_span` 一律回 `[start, end]` 含兩端：多抓一個 step 再把超過 end 的切掉（資料源含頭不含尾的 ES／Prometheus 與含尾的 mock 都一致）。黃金事故防護罩與分位數反推共用這一條 |
| 9 ACF 空桶 | 5／6（7.5 天資料用 sum 聚合算出 r(168h)=−0.221 並宣稱有週期） | **6／6**（sum 聚合也回報「資料不足以算 r(168h)」） | `admin.series_profile` 對 Elasticsearch 來源多發一次同查詢的 `count` 聚合，文件數 0 的桶當成沒資料（NaN）不參與配對；新增 `coverage`／`empty_buckets` 統計與 `min_coverage`（0.5）：覆蓋率不足一律「資料不足」、不算週期、不提案 |

總表：**70／77 斷言、6／10 場景全過**（修復前 64／76、3／10；場景 10 多了一條「併發追加沒有動到既有鍵」）。
全套 `tests/`：見 commit 訊息；新增 `tests/test_chaos_gaps.py` 11 案（ON CONFLICT 單一陳述式、10 執行緒同時首次寫入、merge_context 頂層追加與 KeyError、
10 執行緒追加 50 鍵零遺失、PG 分支產生的 SQL 是 `||`、同一事故兩次 true_positive 兩邊 context 都留、fetch_span 含終點與單點時窗、
單點誤報走分位數反推、profile 空桶遮罩、覆蓋率不足不提案、count 查詢是同一個 filter）。

限制：`merge_context` 在 SQLite 用 `json_patch`（RFC 7396，巢狀物件會往下合併），PostgreSQL 用 `||`（只合頂層）；呼叫端只能靠頂層鍵區分。
場景 10 的驗收用的是平台新 API `Knowledge.merge_context`；舊的「整包 `upsert(context=...)`」寫法仍然會覆蓋，README 已寫明不要那樣用。


## 核心防線 PR A／B／C 合併後重跑（main `3028902`，2026-11-09）

PR #9（底噪＋區間聯集）、#7（封閉詞表）、#8（拓撲自動沉澱）各自在獨立 worktree 複驗後依序 rebase 合入 main，再把全部 11 個場景重跑一次：

### 總表

| # | 場景 | 結果 | 通過／斷言 |
|---|---|---|---|
| 1 | 外部下游超時但應用拋 HTTP 200（隱性延遲陷阱） | 通過 | 6／6 |
| 2 | 離峰超低流量冷啟動（自適應回溯是否發動） | 未通過 | 8／9 |
| 3 | 兩個下游平分延遲（45% vs 45% 死區） | 未通過 | 6／7 |
| 4 | 跨服務連鎖雪崩（上游背黑鍋） | 通過 | 5／5 |
| 5 | 純程式 Exception，完全沒有子 Span（葉節點判斷邊界） | 未通過 | 7／8 |
| 6 | 惡意 LLM 幻覺注入（封閉詞表守門） | 通過 | 10／10 |
| 7 | 極端毛刺干擾下的分位數反推門檻 | 通過 | 10／10 |
| 8 | 黃金事故防護罩一票否決（破壞性調參回測） | 通過 | 9／9 |
| 9 | 假日前夕流量腰斬（ACF 週期檢定邊界） | 通過 | 6／6 |
| 10 | JSONB 上下文併發更新（真實 PostgreSQL） | 通過 | 8／8 |
| 11 | 並行子 span（asyncio.gather 同時打 DB 與外部金流） | 通過 | 12／12 |

11 個場景中完全通過 8 個；斷言合計 87／90。

**跟修復前（64／76、3／10）與 P0/P1 修復後（70／77、6／10）比**：場景 4 不再手動灌邊（`admin.topology_refresh` 從 9,228 個 span 沉澱 2 條邊）、場景 5 注入前誤報 1→0（mad 底噪，不再有 1,349,000 分）、場景 6 七個變體全擋（封閉詞表，含先前漏網的 gpu-farm-prod、payment-gateway）、新場景 11 並行子 span 的自身耗時 40.2／239.9 ms（真值 40／240）、歸因佔比 0.752（真值 0.75，舊算法灌水到 0.933）、時鐘偏移 20 筆被夾並計數。

**複驗時另外量到的**：11/3 真實 ES 五條序列 75 個異常點逐點（實體、時間、節點、分數、等級）與 main 相同，三個事件群的 attribution 數字相同；PR B 第一版把真實 OpenAI 回文裡的 `trace_id`（context 的鍵名）當未知識別字誤拒，修後再叫一次通過；migration 0006 在 PostgreSQL 16 兩條路徑各套兩次加回滾都成功。

**仍開著的 3 條**（都是設計落差，不是崩潰）：場景 2 注入前誤報來自 `summary.sum` 隨請求數變動的查詢設計；場景 3 `attribution` 只記 top-1 資源；場景 5 沒有資源葉節點時殘留基準窗的資源（`by` 為空、不影響 Runbook）。

**PR A 的兩個偏離**（寫在 `docs/HARDENING_DESIGN.md` 1.3／2.3）：(1) 區間聯集只對 I/O 子 span 做，`app` 子 span 仍加總——因為真實 11/3 的 app span 與 I/O span 本來就重疊（49／49），全部聯集會讓自身比總耗時大；代價是多執行緒並行的 app span 仍會重複扣。(2) 子 span 超出父區間的部分不扣、只計數：服務自身加總可能大於請求總長（架構審查 PR #10 第 3 題指出），這是「夾」與「扣」之間的取捨。

## 這份驗收怎麼跑、什麼是真的、什麼不是

| | 真實 | 不是真實（誠實披露） |
|---|---|---|
| 平台 | 真的 `ElasticsearchSpec`、`it_otel`／`it_apm` 偵測管線、`rca_report`（trace／日誌撈取、evidence 根因、Runbook、三行摘要）、PR #5 的回測／黃金防護罩／分位數／序列體檢、HTTP API（FastAPI） | — |
| 資料庫 | **真實 PostgreSQL 16**（容器），程式自動建表後套 **migration 0005**（`context` 轉 JSONB、GIN 索引）；每個場景一個全新資料庫 | — |
| Elasticsearch | 平台發出的查詢（date_histogram、avg／sum、search_after、random_score 抽樣…）原封不動 | **ES 是記憶體裡的 HTTP 層替身**（`httpx.MockTransport`）。原因：注入各種邊界條件必須寫資料，而平台與我都不該寫共用叢集；所以不是真的 ES 服務 |
| 資料 | trace 是完整 Elastic APM 格式的 transaction／span／error 文件（`http.response.status_code`、`span.type／subtype`、`destination.service.resource`、父子 id 串接）；指標用真實欄位 `transaction.duration.summary.sum`／`value_count`、依 `event.outcome` 拆兩份 | 由我寫的世界模擬器（`scripts/chaos/world.py`）產生，**不是專案原本的 `synth_generator`**：它沒有 HTTP 狀態碼、吞例外、一進 controller 就崩潰這些模式。指標是「解析期望值＋取樣噪音」向量化算出，trace 只在需要的時窗逐請求走訪，兩者共用同一組參數 |

每個場景的斷言寫成你要求的樣子，不遷就平台現況；平台做不到就記「未通過」。唯一例外是場景 8：你指定的 `zscore 3.0→25.0` 在**建立階段**就被參數範圍 [1.0, 10.0] 擋下（HTTP 400），到不了防護罩，所以我記錄這一點，再用範圍上限 10.0 去實測防護罩。

## 你問的三件事

**1. 非預期崩潰？** 平台端沒有。場景 8 第一次跑時崩潰（KeyError）是我的腳本沒處理 HTTP 400，已修。唯一接近的是場景 10：併發首次寫入同一筆事故時，`Knowledge.upsert` 丟出未處理的 `IntegrityError`（10 個 worker 裡 4～6 個失敗，三次重跑都出現）。

**2. 假綠燈？** 沒有發現「該擋沒擋卻回綠」的案例，但有一個**測試設計上的弱點**要老實說：場景 4 通過，可是三個服務的第一筆告警都在同一分鐘（12:00），「gateway 最早哀號」的前提沒有被製造出來；RCA 本來就不看告警先後，所以這個場景證明的是「歸因不受入口流量大影響」，不是「不受告警順序影響」。

**3. 規則穿透？** 有，在 LLM 白名單：**不含數字、也不在黑名單的虛構名稱可以穿過**（場景 6 的 B2、C）。你的原文（`Kafka`＋`k8s-node-99`）是被擋下了，但擋下它的是 `99` 這個數字，不是白名單。

## 缺口清單（依嚴重度）

| # | 場景 | 缺口 | 證據 | 性質 |
|---|---|---|---|---|
| 1 | 10 | **事故經驗庫 `context` 併發更新會丟資料**：`Knowledge.upsert` 整包覆蓋 `context`，平台沒有 append／merge API。10 個 worker 各追加 5 次，50 個鍵只留 5～7 個（三次重跑） | 對照：PostgreSQL 原生 `context \|\| patch` 同樣 10×5 是 50／50、無死鎖。**問題在平台的寫法，不在資料庫** | 平台缺口 |
| 2 | 10 | **同一筆新事故被同時首次寫入會撞唯一鍵**：`upsert` 是「先查再寫」，沒有處理競爭，4～6／10 個 worker 丟 `IntegrityError`（例：兩個人同時確認同一個事故） | 三次重跑 4、4、6 | 平台缺口 |
| 3 | 7 | **單點事故的分位數反推退回 ×1.2**：誤報事件群 start == end 時，反推對 ES 的查詢終點是事件群結束時間，而 ES 範圍查詢終點不含（`lt`），那一點被排除 → 「時窗內沒有資料」→ 退回固定規則（3.5→4.2）。3 分鐘毛刺則走完整路徑：S_max 80.0、Q 2.382、目標 84.0 夾到範圍上限 20.0、再受單次 50% 上限切到 5.25，reason 寫明還差 14.75 | 查詢日誌：反推時最大 `lt` = `12:10:00` = 事件群結束。推論（未另外驗證）：多點事件群也會少算最後一個點 | 平台缺口（單點最明顯） |
| 4 | 6 | **LLM 白名單擋不住虛構名稱**：`gpu-farm-prod`（無數字、不在黑名單）與 `payment-gateway`（虛構服務名）都被放行（`generated_by = llm`）。你的原文被擋是因為 `99`（數字檢查），不是白名單 | 這就是規格第 7 節預告的死區，現在有實測 | 平台缺口（已知死區） |
| 5 | 9 | **`sum` 聚合的「沒資料」被當成「流量為零」**：真實 ES 對沒資料的桶，`sum` 回 0.0、`avg` 回 null。同一份只有 7.5 天的資料，`avg` 正確回報「資料不足」，`sum` 卻補滿 4032 個點、算出 r(168h) = −0.221 並宣稱「有週期」。QPS／請求數類目標一定用 `sum`／`count` | 平台無法區分兩者 | 平台缺口（資料源語意） |
| 6 | 3 | **`attribution` 只記單一資源（top-1）**：兩個下游各佔 45% 時，`dependency` 字串與 `by` 正確留空、Runbook 正確退回 RB-00，但 `attribution` 結構體只有 redis，沒有 postgres（兩者都在 `reason` 文字裡） | 你的斷言要求結構體記兩者；規格寫的是單一資源 | 設計落差 |
| 7 | 5 | **沒有資源葉節點的事故，`attribution` 不是空的**：基準窗有 redis、事故窗崩潰後沒有，結果 `attribution = {resource: redis, by: "", share: 0.0, wait_ms: 2.0→0.0}`。`by` 為空所以不影響 Runbook，但報告裡會出現一個沒意義的資源 | PR #4 的設計是「任一視窗出現過資源葉節點就填」 | 設計落差 |
| 8 | 6 | **沒有 `HallucinationBlocked` 日誌**：攔截原因只寫在 `narrative.briefing_rejected`，平台不寫任何日誌 | 你要的字樣平台沒有；攔截本身有效 | 設計落差 |
| 9 | 2、5 | **注入之前就誤報**（兩個場景各 1 個事件群）：(2) 低流量下 `transaction.duration.summary.sum` 是當分鐘**總耗時**，會隨請求數成比例變動——01:33 那分鐘剛好 3 筆請求，值 112,530 ≈ 1 筆請求那分鐘（38,304）的 3 倍，被當成延遲暴增 3 倍；(5) 失敗數序列幾乎全是 0（平均 0.25 次／分），某分鐘 2 次失敗 → MAD = 0 → 分數 1,349,000，三個偵測器多數決通過 | 兩個都用先前真實 ES 回放同樣的查詢欄位 | 查詢設計地雷（用平均耗時或 histogram 就不會；舊專案文件也列了「稀少事件 z／MAD 失效」） |
| — | 8 | 你指定的 25.0 到不了防護罩（建立階段被 [1.0, 10.0] 擋下 HTTP 400） | 第一道防線有效；防護罩改用 10.0 實測 | 資訊 |

## 平台在攻擊下撐住的

- **場景 1（隱性延遲陷阱）6／6**：整段 0 筆 500、1,220 個客戶端 span 逾時失敗但交易全 200；RCA 仍把 ord-svc 歸因到 `payment-gw:443`（external／http，等待 20.3→3001.5 ms，程式自身 40.2→40.1 ms），佔比 1.0，Runbook RB-01。
- **場景 2（自適應回溯）**：事故前 15 分鐘只有 6 筆入口請求 < 20 → 基準窗自動 15→60 分鐘（「bill-svc 7 筆（門檻 20 筆）」），報告標出「擴展至 60 分鐘」；歸因 `billing-db`（db，share 1.0）、資源失敗 1→34、Runbook RB-02。
- **場景 4（連鎖雪崩）4／4**：三個服務歸為同一個事件群；根因第 1 名 inventory-svc（分數 1.0），order-api、gateway 都是 0.0；下鑽 oracle；RB-02。
- **場景 5（沒有子 span）**：RCA 無例外、`errors` 空；候選 cart-api（自身錯誤率 1%→100%）、Runbook RB-03、處方含「入參」與「回滾」。
- **場景 6**：使用者原文與只含 Kafka 的版本被攔截並降級為 Runbook 模板（三行含 RB-01），`errors` 為空；虛構數字被擋；合法回文放行（守門不是全擋）。
- **場景 8（防護罩）9／9**：zscore 偵測到 bill-svc P0 → 確認為真事故進黃金庫 → 提案 threshold 10.0 回測：事故窗異常 **8→0**、`safety.passed = false`、`missed_golden` 指名該事故 uid、approve 回 **HTTP 409**、狀態維持 rejected；對照溫和提案（3.0→3.5）通過並可核准。
- **場景 9**：含 3 天連假的窗口 r(24h) 0.8455、r(168h) 0.4656（不含連假 0.9423／0.9401，連假讓 168h 的 r 掉 0.47，有被如實回報）；仍判定有週期、**沒有任何 advisory 提案**；`avg` 聚合配對不足時明確「資料不足以算 r(168h)，不給建議」。
- **場景 10**：10 個 worker 併發沒有任何 `DeadlockDetected`；保留下來的鍵中文與巢狀結構 100% 完整；GIN `@>` 即時查得到。

## 這份驗收的限制

- 每個場景只跑一個隨機種子（結果確定性可重現，不是統計）；場景 10 的競爭結果每次略有不同（5～7、4～6），結論穩定。
- 場景 7 的毛刺是直接把那一分鐘的指標值換成「剛好讓 mad 分數 ~80」的值，沒有對應的 trace（這個場景用不到 RCA）。
- 場景 8 用的是 `it_apm`（有 zscore 節點）；事故是延遲約 3 倍，要 z 分數介於 3 與 10 之間才有意義，實測 8 個事故點在 10.0 下全部消失。
- ES 是替身（見上表）；沒有驗證真實 ES 的抽樣、分片、延遲與 429／503 重試。
- 沒有接進 CI；`scripts/chaos/` 需要一個 PostgreSQL（`CHAOS_PG_URL`）。

## 重現

```bash
nerdctl run -d --name pgchaos -e POSTGRES_PASSWORD=pgchaos -e POSTGRES_DB=chaos -p 55433:5432 postgres:16
.venv/Scripts/python scripts/chaos/run.py                 # 全部 10 個，約 1.5 分鐘
.venv/Scripts/python scripts/chaos/run.py 6 10            # 指定場景
```

---

# 逐場景實測明細（修復前，main 35d7326）

## 場景 1：外部下游超時但應用拋 HTTP 200（隱性延遲陷阱）　【通過】

**注入特徵**：ord-svc 呼叫 payment-gw：20→3000 ms（>95% 耗時在外部）；應用接住逾時、回 PENDING／HTTP 200，交易沒有任何 500

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| 世界資料：整段沒有任何 HTTP 500（陷阱成立） | 0 筆 500 | 0 筆 500；1220 個客戶端 span 逾時失敗 | ✓ |
| 偵測到 ord-svc 事件群 | 有 | True | ✓ |
| RCA 有候選且是 ord-svc | ord-svc | ord-svc | ✓ |
| attribution 指向 external/http 的 payment-gw | payment-gw:443、external、http | payment-gw:443、external、http | ✓ |
| 下鑽佔比 > 90% | > 0.90 | 1.0 | ✓ |
| Runbook 命中 RB-01（不是 RB-00） | RB-01 | RB-01 | ✓ |

> 偵測：19 輪、異常 60 筆、errors []、事件群 [(1, ['ord-svc'])]

> dependency='payment-gw:443（external/http）'；wait_ms={'before': 20.3, 'during': 3001.5}；self_ms={'before': 40.2, 'during': 40.1}；errors=[]

## 場景 2：離峰超低流量冷啟動（自適應回溯是否發動）　【未通過】

**注入特徵**：bill-svc 每 5 分鐘約 2~3 筆請求（事故前 15 分鐘約 8 筆 < 20）；03:00 注入 DB 鎖表：billing-db 8→5000 ms 且 100% 失敗

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| 世界資料：事故前 15 分鐘 bill-svc 入口請求 < 20 | < 20（約 8） | 6 筆 | ✓ |
| 注入之前沒有誤報事件群（稀疏流量不能自己亂叫） | 0 個 | 1 個（id, 開始時間）=[(1, '01:33')] | ✗ |
| 稀疏流量下仍偵測到注入的 bill-svc 事件群 | 有 | True | ✓ |
| RCA 沒有直接放棄（不是「證據不足／trace 不足」） | 有根因候選 | 候選 1 個；note='' | ✓ |
| 基準窗自適應擴展到 60 分鐘 | before_minutes 15 → before_minutes_used 60 | 15→60；'bill-svc 7 筆（門檻 20 筆）' | ✓ |
| 報告明確標出「擴展至 60 分鐘」 | trace 佐證段落含『擴展至 60 分鐘』 | True | ✓ |
| 耗時歸因產出（db） | billing-db、db | billing-db、db、share=1.0 | ✓ |
| 錯誤歸因產出（資源失敗增加） | failures.during > failures.before | {'before': 1, 'during': 34} | ✓ |
| Runbook 命中 RB-02 | RB-02 | RB-02 | ✓ |

> 偵測：19 輪、異常 23 筆、errors []、事件群 [(1, ['bill-svc']), (2, ['bill-svc'])]

> trace_fetch={"source_type": "elasticsearch", "spans": 4656, "truncated": false, "start": "2026-11-09T02:03:00+00:00", "end": "2026-11-09T03:59:00+00:00", "services": ["bill-svc", "pay-api"], "before_minutes": 15, "before_minutes_used": 60, "extended_for": "bill-svc 7 筆（門檻 20 筆）"}；errors=[]

## 場景 3：兩個下游平分延遲（45% vs 45% 死區）　【未通過】

**注入特徵**：cart-api 總耗時 +1000 ms：redis +450、postgres +450、自身 +100（各佔 45%／45%／10%，沒有單一資源過 60%）

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| 偵測到 cart-api 事件群 | 有 | True | ✓ |
| 候選是 cart-api（不是 redis 或 postgres） | cart-api | cart-api | ✓ |
| dependency 人類字串留空（沒有單一資源 > 60%） | '' | '' | ✓ |
| attribution.by 為空（沒有主導資源） | '' | '' | ✓ |
| attribution 結構體記錄『兩個』資源的數值 | redis 與 postgres 都在 attribution 裡 | attribution 只記單一 resource='redis' | ✗ |
| 理由文字（reason）列出兩個資源的等待 | reason 含 redis 與 postgres | True | ✓ |
| Runbook 為 RB-06 或優雅退回 RB-00，不得把罪名扣給單一資源 | RB-06／RB-00（不是 RB-01／RB-02） | RB-00 | ✓ |

> 偵測：16 輪、異常 45 筆、errors []、事件群 [(1, ['cart-api'])]

> attribution={"resource": "redis", "kind": "db", "subtype": "redis", "by": "", "self_ms": {"before": 30.0, "during": 130.6}, "wait_ms": {"before": 2.0, "during": 454.3}, "failures": {"before": 0, "during": 0}, "share": 0.452}；reason 摘要=自身耗時 37.0→1038.6 ms／請求（+1001.5）；自身錯誤率 0%→0%；入口請求 284→852（事件前 15 分鐘）；其中 程式自身 30.0→130.6 ms（+100.6）、等待 redis，db/redis 2.0→454.3 ms（+452.3，資源失敗 0→0 筆）、等待 postgres，db/postgresql 5.0→453.7 ms（+448.7，資源失敗 0→0 筆）；流入流量 1.00 倍；分數 1.00 = 自身耗時 1×1.00 + 錯誤 0.5×0.00；建議處置前 3 行=['1. 優先處理可能的源頭：cart-api', '2. [RB-00] 沒有命中任何故障特徵', '3. 比對事件時間前後的變更紀錄（部署、設定、配方）']

## 場景 4：跨服務連鎖雪崩（上游背黑鍋）　【通過】

**注入特徵**：gateway→order-api→inventory-svc→oracle；oracle 12→4000 ms 且 50% 逾時失敗，三個服務同時變慢，gateway 流量最大

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| 三個服務歸為同一個 Incident | 1 個事件群含 gateway、order-api、inventory-svc | [['gateway', 'inventory-svc', 'order-api']] | ✓ |
| 根因第 1 名是 inventory-svc（不是最早哀號的 gateway） | inventory-svc | ['inventory-svc', 'order-api', 'gateway'] | ✓ |
| 下鑽到 oracle（db） | oracle、db | oracle、db、share=1.0 | ✓ |
| Runbook RB-02 | RB-02 | RB-02 | ✓ |

> 拓撲：匯入 3000 個 span（事故前，經 admin_merge_traces 持久化）

> 偵測：異常 135 筆、errors []；最早告警順序 [('gateway', '12:00'), ('order-api', '12:00'), ('inventory-svc', '12:00')]；事件群 [(1, ['gateway', 'inventory-svc', 'order-api'])]

> 候選排序：[('inventory-svc', 1.0), ('order-api', 0.0), ('gateway', 0.0)]；傳播路徑段落是否產出：True

## 場景 5：純程式 Exception，完全沒有子 Span（葉節點判斷邊界）　【未通過】

**注入特徵**：cart-api 遇髒資料，controller 第一行拋 NullPointerException（HTTP 500）；整條 trace 只有 root transaction，沒有任何子 span

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| 世界資料：事故期間 cart-api 只有 root transaction | 0 個子 span、有 500 | 0 個子 span、878 筆 500 | ✓ |
| 注入之前沒有誤報事件群（零基準的失敗數序列不能自己亂叫） | 0 個 | 1 個（id, 開始時間）=[(1, '11:37')] | ✗ |
| 偵測到 cart-api 事件群（失敗數飆升） | 有 | True | ✓ |
| RCA 沒有因為沒有子 span 而崩潰 | errors 為空、報告有產出 | errors=[]；段落 ['affected_entities', 'golden_signals', 'log_patterns', 'root_cause_candidates', 'suggested_actions']… | ✓ |
| 候選是 cart-api | cart-api | cart-api | ✓ |
| attribution 為空（沒有任何外部資源） | {} | {'resource': 'redis', 'kind': 'db', 'subtype': 'redis', 'by': '', 'self_ms': {'before': 30.3, 'during': 2.0}, 'wait_ms': {'before': 2.0, 'during': 0.0}, 'failures': {'before': 0, 'during': 0}, 'share': 0.0} | ✗ |
| Runbook 命中 RB-03（應用邏輯錯誤） | RB-03 | RB-03 | ✓ |
| 處方含檢查入參與回滾（Rollback） | 含『入參』與『回滾』 | 入參=True、回滾=True | ✓ |

> 偵測：異常 46 筆、errors []、事件群 [(1, ['cart-api']), (2, ['cart-api']), (3, ['cart-api']), (4, ['cart-api'])]

> reason 摘要=自身耗時 32.3→2.0 ms／請求（-30.3）；自身錯誤率 1%→100%；入口請求 284→58（事件前 15 分鐘）；其中 程式自身 30.3→2.0 ms（-28.3）、等待 redis，db/redis 2.0→0.0 ms（-2.0，資源失敗 0→0 筆）；流入流量 0.61 倍；日誌暴增：Cannot invoke Cart.getItems() because cart is null (request  約 138 倍（權重 0）；分數 0.50 = 自身耗時 1×0.00 + 錯誤 0.5×1.00；trace 佐證前 3 行=['這一輪撈到 1194 個 span、626 條 trace；事件前 15 分鐘 vs 事件期間（每請求）：', '  pay-api：請求 228→56；總耗時 32.9→33.3 ms；自身耗時 32.9→33.3 ms（+0.4）；錯誤率 0%→0%', '    程式自身 24.8→25.4 ms；等待 postgres（db/postgresql） 8.0→7.9 ms（資源失敗 0→0 筆）']

## 場景 6：惡意 LLM 幻覺注入（白名單守門）　【未通過】

**注入特徵**：假模型回傳充滿幻覺的三行 JSON：『檢查 Kafka 佇列飽和度並重啟 k8s-node-99』（環境裡根本沒有）；另測只含虛構節點、虛構服務名、虛構數字、與合法回文各一組

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| 偵測到 ord-svc 事件群 | 有 | True | ✓ |
| A 使用者原文被攔截（降級為模板） | generated_by=template、有 rejected 原因 | template；數字 99 在證據中找不到（±10%） | ✓ |
| A2 只有 Kafka（無數字）被黑名單攔截 | template、原因含 kafka | template；提到證據裡沒有的 kafka | ✓ |
| A 降級後報告 errors 為空、三行仍由 Runbook 模板填好 | errors=[]、action 含 RB-01 | errors=[]；【立即止血動作】[RB-01] 檢查下游依賴目標 payment-gw:443 的健康狀態與網路延遲（ord-svc 的 | ✓ |
| A 日誌記錄 HallucinationBlocked | 日誌出現 HallucinationBlocked | 日誌命中 0 行（平台只寫 narrative.briefing_rejected，沒有日誌） | ✗ |
| B 虛構節點 k8s-node-99 被攔截 | template | template；數字 99 在證據中找不到（±10%） | ✓ |
| B2 虛構節點 gpu-farm-prod（無數字、不在黑名單）也必須攔截 | template | llm；None | ✗ |
| C 虛構服務名 payment-gateway 也必須攔截 | template | llm；None | ✗ |
| D 虛構數字被攔截（對照） | template、數字不在證據裡 | template；數字 99 在證據中找不到（±10%） | ✓ |
| E 合法回文被放行（守門不是全擋） | llm | llm；None | ✓ |

> A、B 被攔截的實際原因是『k8s-node-99』裡的數字 99 找不到證據（數字檢查），不是實體白名單；沒有數字、也不在黑名單的虛構名稱（B2、C）靠的是實體白名單，而白名單只擋『拓撲裡已知但不相干』的名字

## 場景 7：極端毛刺干擾下的分位數反推門檻　【未通過】

**注入特徵**：bill-svc 平時穩定；注入瞬間抖動讓 mad 分數每分鐘 ~80（極端離群值），維運標記該事件為 false_positive。分兩組：(a) 1 分鐘單點毛刺、(b) 3 分鐘毛刺

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| (a) 毛刺被偵測成事件群 | 有 | True | ✓ |
| (a) 門檻沒有被帶偏到 80 | < 80 | 4.2 | ✓ |
| (a) 單次調整 ≤ +50%（3.5 → ≤ 5.25）且方向向上 | 3.5 < threshold ≤ 5.25 | 4.2 | ✓ |
| (a) 走分位數反推（不是退回固定規則 ×1.2） | reason 不含『固定規則』 | 來自回饋 #1（false_positive）：固定規則：false_positive -> threshold 3.5 -> 4.2（沒用分位數反推：撈不到資料（{"index" | ✗ |
| (a) reason 註明目標過大、執行步進截斷 | reason 含截斷說明 | 來自回饋 #1（false_positive）：固定規則：false_positive -> threshold 3.5 -> 4.2（沒用分位數反推：撈不到資料（{"index" | ✗ |
| (b) 毛刺被偵測成事件群 | 有 | True | ✓ |
| (b) 門檻沒有被帶偏到 80 | < 80 | 5.25 | ✓ |
| (b) 單次調整 ≤ +50%（3.5 → ≤ 5.25）且方向向上 | 3.5 < threshold ≤ 5.25 | 5.25 | ✓ |
| (b) 走分位數反推（不是退回固定規則 ×1.2） | reason 不含『固定規則』 | 來自回饋 #1（false_positive）：分位數反推：誤報時窗最高分 S_max=80.0，過去 30 天（實際有分數 30 天、43200 點，排除 0 個事故／維護／黃金 | ✓ |
| (b) reason 註明目標過大、執行步進截斷 | reason 含截斷說明 | 來自回饋 #1（false_positive）：分位數反推：誤報時窗最高分 S_max=80.0，過去 30 天（實際有分數 30 天、43200 點，排除 0 個事故／維護／黃金 | ✓ |

> (a) 世界：mad 訓練窗 median=979803、MAD=328705（µs/分）；注入值 39966354（理論 mad 分數 80）；偵測 9 輪、異常 1、errors []；事件群 [(1, '12:10', '12:10')]

> (a) 提案 threshold=4.2；generated_by=template；事件群結束=2026-11-09T12:10:00+00:00；反推時對 ES 的查詢終點(lt)=['2026-11-09T10:50:00+00:00', '2026-11-09T12:10:00+00:00']；reason=來自回饋 #1（false_positive）：固定規則：false_positive -> threshold 3.5 -> 4.2（沒用分位數反推：撈不到資料（{"index": "metrics-apm.transaction.1m-*", "agg": "avg", "value_field": "transaction.duration.summary.sum", "filter": {"term": {"service.name": "bill-svc"}}}：時窗內沒有資料））

> (b) 世界：mad 訓練窗 median=979803、MAD=328705（µs/分）；注入值 39966354（理論 mad 分數 80）；偵測 9 輪、異常 3、errors []；事件群 [(1, '12:10', '12:12')]

> (b) 提案 threshold=5.25；generated_by=quantile；事件群結束=2026-11-09T12:12:00+00:00；反推時對 ES 的查詢終點(lt)=['2026-11-09T10:50:00+00:00', '2026-11-09T12:10:00+00:00', '2026-11-09T12:12:00+00:00']；reason=來自回饋 #1（false_positive）：分位數反推：誤報時窗最高分 S_max=80.0，過去 30 天（實際有分數 30 天、43200 點，排除 0 個事故／維護／黃金時窗）99.9% 分位數 Q=2.382；目標 max(S_max×1.05, Q)=84.0。目標 84.0 超出範圍，夾到 20.0。目標 20.0 超過單次上限 50%，這次先調到 5.25（還差 14.75；核准後若還誤報，再回饋一次會從 5.25 往上算）。

## 場景 8：黃金事故防護罩一票否決（破壞性調參回測）　【通過】

**注入特徵**：bill-svc 於 12:00 起延遲約 3 倍（P0）並確認為真事故；提案把 zscore 門檻從 3.0 調成 25.0

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| zscore 偵測到 bill-svc 事故 | 有 | True | ✓ |
| 確認為真事故，進黃金庫（HTTP 200） | 200 | 200 | ✓ |
| 使用者指定的 25.0：建立階段就被參數範圍擋下（第一道防線） | HTTP 400、超出範圍 | HTTP 400：{"message":"提案參數不合法：detector.zscore.threshold=25.0 超出範圍 [1.0, 10.0]"} | ✓ |
| 回測 HTTP 200 且 safety.passed 為 false | passed=false | HTTP 200；passed=False | ✓ |
| missed_golden 指名這場事故 | 含 inc-5e465a29c4f641b6843154b608d68be7 | ['inc-5e465a29c4f641b6843154b608d68be7'] | ✓ |
| 事故窗異常數歸零（現行 → 提案） | 提案 = 0 | 8 → 0 | ✓ |
| approve 強制回 HTTP 409 | 409 | 409：{'message': "提案回測沒通過黃金事故防護罩（會漏抓 ['inc-5e465a29c4f641b6843154b608d68be7']），不能核准；請重新提案"} | ✓ |
| 提案狀態維持 rejected | rejected | rejected | ✓ |
| 對照：溫和提案（3.0→3.5）通過防護罩並可核准 | passed=true、approve 200 | passed=True；approve 200 | ✓ |

> 偵測（it_apm／zscore）：11 輪、異常 9、errors []、事件群 [(1, '12:00', 'P0')]

> 25.0 到不了黃金事故防護罩：zscore.threshold 合法範圍是 [1.0, 10.0]，所以改用範圍上限 10.0 去實測防護罩（事故 z 分數須介於 3 與 10 之間才有意義）

> safety 明細：{"golden_total": 1, "checked": 1, "missed_golden": [{"incident_uid": "inc-5e465a29c4f641b6843154b608d68be7", "pipeline": "it_apm", "start": "2026-11-09T12:00:00+00:00", "end": "2026-11-09T12:29:00+00:00", "entity": "bill-svc", "current_anomalies": 8, "proposed_anomalies": 0, "series": [{"entity": "bill-svc", "source_type": "elasticsearch", "query": "{\"index\": \"metrics-apm.transaction.1m-*\", \"agg\": \"avg\", \"value_field\": \"transaction.duration.summary.sum\", \"filter\": {\"term\": {\"service.name\": \"bill-svc\"}}}", "train_points": 1430, "window_points": 29, "current_anomalies": 8, "p

## 場景 9：假日前夕流量腰斬（ACF 週期檢定邊界）　【未通過】

**注入特徵**：bill-svc 的 QPS：連續 3 天連假只有平日的 20%，仍維持日夜起伏；分別在含連假、不含連假、只有 7.5 天資料三種窗口做序列體檢

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| 含連假窗口：精確回報 r(24h)、r(168h) | 兩者都有數值 | r24h=0.8455、r168h=0.4656 | ✓ |
| 連假造成的 r 值變化有被回報（含 vs 不含） | 兩個窗口的 r 值都列出可比 | Δr24h=-0.0968、Δr168h=-0.4745 | ✓ |
| 含連假窗口：仍有日夜週期，不得提議拔除 hourly_baseline | proposal=None | proposal=False；有週期（r24h=0.8455、r168h=0.4656，至少一個 ≥ 0.3），時段基準線適用，不建議排除 | ✓ |
| 168h 配對不足（avg 聚合）：明確標記資料不足、維持既有算子 | r168h=None、advice 說資料不足、proposal=None | r168h=None；advice=資料不足以算 r(168h)（配對不到一天），不給建議；proposal=None | ✓ |
| 同一份只有 7.5 天的資料改用 sum 聚合：同樣要標記資料不足 | r168h=None、advice 說資料不足 | r168h=-0.221；advice=有週期（r24h=0.7206、r168h=-0.221，至少一個 ≥ 0.3），時段基準線適用，不建議排除 | ✗ |
| 三次體檢後資料庫沒有任何 advisory 提案 | 0 筆 | 0 筆 | ✓ |

> 含連假窗口：r24h=0.8455 r168h=0.4656（配對 3744／2016）；不含連假：r24h=0.9423 r168h=0.9401；只有 7.5 天資料（avg 聚合）：r24h=0.7768 r168h=None（168h 配對 144）；advice='資料不足以算 r(168h)（配對不到一天），不給建議'；同一份資料改用 sum 聚合：r24h=0.7206 r168h=-0.221（168h 配對 2016，points=4032／grid=4032）；advice='有週期（r24h=0.7206、r168h=-0.221，至少一個 ≥ 0.3），時段基準線適用，不建議排除'

## 場景 10：JSONB 上下文併發更新（真實 PostgreSQL）　【未通過】

**注入特徵**：10 個 worker 同時對同一筆事故記錄追加維運標籤、Trace ID 與處方註記（含大量中文與巢狀結構），每個 worker 追加 5 次

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| 沒有 DeadlockDetected／其他資料庫例外 | 0 個例外 | 0 個：[] | ✓ |
| 平台 API（讀改寫）併發追加：context 保留所有 50 個鍵 | 50 個 | 6 個（遺失 44，後寫覆蓋先寫） | ✗ |
| 保留下來的鍵：中文與巢狀結構完整無亂碼 | 每個保留的鍵都有維運標籤／處方註記 | 6/6 個完整 | ✓ |
| GIN 索引 @> 立即查得到剛寫入的屬性 | 命中 1 筆 | 1 | ✓ |
| 對照（PostgreSQL 原生 jsonb || 原子追加）：50 個鍵全保留、無死鎖 | 50 個、0 例外 | 50 個、0 例外 | ✓ |
| 對照組中文無亂碼（原生追加） | 處方註記=『原子追加 3-2』 | 原子追加 3-2 | ✓ |
| 同一個新事故被 10 個 worker 同時首次寫入：不得有人因唯一鍵衝突失敗 | 0 個失敗 | 4 個失敗：['IntegrityError'] | ✗ |

> 平台 API 路徑最終鍵數 6/50；原生原子追加 50/50；插入競爭失敗 4/10


# 修復後逐場景明細（場景 7、9、10）

### 場景 7（修復後）：極端毛刺干擾下的分位數反推門檻　【通過】

**注入特徵**：bill-svc 平時穩定；注入瞬間抖動讓 mad 分數每分鐘 ~80（極端離群值），維運標記該事件為 false_positive。分兩組：(a) 1 分鐘單點毛刺、(b) 3 分鐘毛刺

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| (a) 毛刺被偵測成事件群 | 有 | True | ✓ |
| (a) 門檻沒有被帶偏到 80 | < 80 | 5.25 | ✓ |
| (a) 單次調整 ≤ +50%（3.5 → ≤ 5.25）且方向向上 | 3.5 < threshold ≤ 5.25 | 5.25 | ✓ |
| (a) 走分位數反推（不是退回固定規則 ×1.2） | reason 不含『固定規則』 | 來自回饋 #1（false_positive）：分位數反推：誤報時窗最高分 S_max=80.0，過去 30 天（實際有分數 30 天、43200 點，排除 0 個事故／維護／黃金 | ✓ |
| (a) reason 註明目標過大、執行步進截斷 | reason 含截斷說明 | 來自回饋 #1（false_positive）：分位數反推：誤報時窗最高分 S_max=80.0，過去 30 天（實際有分數 30 天、43200 點，排除 0 個事故／維護／黃金 | ✓ |
| (b) 毛刺被偵測成事件群 | 有 | True | ✓ |
| (b) 門檻沒有被帶偏到 80 | < 80 | 5.25 | ✓ |
| (b) 單次調整 ≤ +50%（3.5 → ≤ 5.25）且方向向上 | 3.5 < threshold ≤ 5.25 | 5.25 | ✓ |
| (b) 走分位數反推（不是退回固定規則 ×1.2） | reason 不含『固定規則』 | 來自回饋 #1（false_positive）：分位數反推：誤報時窗最高分 S_max=80.0，過去 30 天（實際有分數 30 天、43200 點，排除 0 個事故／維護／黃金 | ✓ |
| (b) reason 註明目標過大、執行步進截斷 | reason 含截斷說明 | 來自回饋 #1（false_positive）：分位數反推：誤報時窗最高分 S_max=80.0，過去 30 天（實際有分數 30 天、43200 點，排除 0 個事故／維護／黃金 | ✓ |

> (a) 世界：mad 訓練窗 median=979803、MAD=328705（µs/分）；注入值 39966354（理論 mad 分數 80）；偵測 9 輪、異常 1、errors []；事件群 [(1, '12:10', '12:10')]

> (a) 提案 threshold=5.25；generated_by=quantile；事件群結束=2026-11-09T12:10:00+00:00；反推時對 ES 的查詢終點(lt)=['2026-11-09T10:50:00+00:00', '2026-11-09T12:11:00+00:00']；reason=來自回饋 #1（false_positive）：分位數反推：誤報時窗最高分 S_max=80.0，過去 30 天（實際有分數 30 天、43200 點，排除 0 個事故／維護／黃金時窗）99.9% 分位數 Q=2.382；目標 max(S_max×1.05, Q)=84.0。目標 84.0 超出範圍，夾到 20.0。目標 20.0 超過單次上限 50%，這次先調到 5.25（還差 14.75；核准後若還誤報，再回饋一次會從 5.25 往上算）。

> (b) 世界：mad 訓練窗 median=979803、MAD=328705（µs/分）；注入值 39966354（理論 mad 分數 80）；偵測 9 輪、異常 3、errors []；事件群 [(1, '12:10', '12:12')]

> (b) 提案 threshold=5.25；generated_by=quantile；事件群結束=2026-11-09T12:12:00+00:00；反推時對 ES 的查詢終點(lt)=['2026-11-09T10:50:00+00:00', '2026-11-09T12:11:00+00:00', '2026-11-09T12:13:00+00:00']；reason=來自回饋 #1（false_positive）：分位數反推：誤報時窗最高分 S_max=80.0，過去 30 天（實際有分數 30 天、43200 點，排除 0 個事故／維護／黃金時窗）99.9% 分位數 Q=2.382；目標 max(S_max×1.05, Q)=84.0。目標 84.0 超出範圍，夾到 20.0。目標 20.0 超過單次上限 50%，這次先調到 5.25（還差 14.75；核准後若還誤報，再回饋一次會從 5.25 往上算）。


### 場景 9（修復後）：假日前夕流量腰斬（ACF 週期檢定邊界）　【通過】

**注入特徵**：bill-svc 的 QPS：連續 3 天連假只有平日的 20%，仍維持日夜起伏；分別在含連假、不含連假、只有 7.5 天資料三種窗口做序列體檢

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| 含連假窗口：精確回報 r(24h)、r(168h) | 兩者都有數值 | r24h=0.8455、r168h=0.4656 | ✓ |
| 連假造成的 r 值變化有被回報（含 vs 不含） | 兩個窗口的 r 值都列出可比 | Δr24h=-0.0958、Δr168h=-0.4738 | ✓ |
| 含連假窗口：仍有日夜週期，不得提議拔除 hourly_baseline | proposal=None | proposal=False；有週期（r24h=0.8455、r168h=0.4656，至少一個 ≥ 0.3），時段基準線適用，不建議排除 | ✓ |
| 168h 配對不足（avg 聚合）：明確標記資料不足、維持既有算子 | r168h=None、advice 說資料不足、proposal=None | r168h=None；advice=資料不足以算 r(168h)（配對不到一天），不給建議；proposal=None | ✓ |
| 同一份只有 7.5 天的資料改用 sum 聚合：同樣要標記資料不足 | r168h=None、advice 說資料不足 | r168h=None；advice=資料不足以算 r(168h)（配對不到一天），不給建議 | ✓ |
| 三次體檢後資料庫沒有任何 advisory 提案 | 0 筆 | 0 筆 | ✓ |

> 含連假窗口：r24h=0.8455 r168h=0.4656（配對 3744／2016）；不含連假：r24h=0.9413 r168h=0.9394；只有 7.5 天資料（avg 聚合）：r24h=0.7768 r168h=None（168h 配對 144）；advice='資料不足以算 r(168h)（配對不到一天），不給建議'；同一份資料改用 sum 聚合：r24h=0.7772 r168h=None（168h 配對 144，points=2160／grid=4033）；advice='資料不足以算 r(168h)（配對不到一天），不給建議'


### 場景 10（修復後）：JSONB 上下文併發更新（真實 PostgreSQL）　【通過】

**注入特徵**：10 個 worker 同時對同一筆事故記錄追加維運標籤、Trace ID 與處方註記（含大量中文與巢狀結構），每個 worker 追加 5 次

| 斷言 | 預期 | 實測 | |
|---|---|---|---|
| 沒有 DeadlockDetected／其他資料庫例外 | 0 個例外 | 0 個：[] | ✓ |
| 平台 API（Knowledge.merge_context）併發追加：context 保留所有 50 個鍵 | 50 個 | 50 個（遺失 0） | ✓ |
| 併發追加沒有動到既有鍵 | base=起始 仍在 | 起始 | ✓ |
| 保留下來的鍵：中文與巢狀結構完整無亂碼 | 每個保留的鍵都有維運標籤／處方註記 | 50/50 個完整 | ✓ |
| GIN 索引 @> 立即查得到剛寫入的屬性 | 命中 1 筆 | 1 | ✓ |
| 對照（PostgreSQL 原生 jsonb || 原子追加）：50 個鍵全保留、無死鎖 | 50 個、0 例外 | 50 個、0 例外 | ✓ |
| 對照組中文無亂碼（原生追加） | 處方註記=『原子追加 3-2』 | 原子追加 3-2 | ✓ |
| 同一個新事故被 10 個 worker 同時首次寫入：不得有人因唯一鍵衝突失敗 | 0 個失敗 | 0 個失敗：[] | ✓ |

> 平台 API 路徑最終鍵數 50/50；原生原子追加 50/50；插入競爭失敗 0/10

