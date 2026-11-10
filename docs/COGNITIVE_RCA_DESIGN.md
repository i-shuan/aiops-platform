# 認知型 RCA 設計合約（分支 `arch/cognitive-rca`）

這份文件是並行施工的接口合約：三個以上的 session 各做一段，只要都遵守這裡的型別與檔案歸屬就不會撞。
改合約要回主 session，不要自己改別人的檔案。

## 1. 已拍板的決定

1. **LLM 在 RCA 路徑是必要元件**（偵測 → 收斂 → 通知的告警路徑仍然**不准**依賴 LLM）。
2. **LLM 的角色 = 必經的證據抽取者 + 戰情摘要作者**；**根因排名由機率模型算**，LLM 不下根因結論。
3. **LLM 連不上／逾時／回傳格式不合時**：報告照出，`ctx.rca_status = "undetermined"`，`rca_undetermined_reason` 寫明原因，
   根因欄位寫「無法判定」，並附證據層算好的事實與原始 trace、日誌入口。不整份失敗，不退回舊規則硬猜。逾時後不無限重試。
4. **新舊並排**：舊判斷層（`rca.root_cause.*` 現有四個、`sections/actions.py` 的 Runbook 比對矩陣）**先不刪**；
   新引擎在混沌 1–13 不輸 199／200、且修掉 16／17／21 之後才刪舊的。
5. 偵測、收斂、抑制、通知、控制面不動。

## 2. 管線

新 workflow：`aiops/pipelines/rca/rca_report_cognitive.yaml`（舊的 `rca_report.yaml` 原封不動，名稱全域唯一）。

```
fetch_traces / build_topology / fetch_logs       （沿用既有 ingestion.incident_*）
  → rca.facts                 證據層：原始資料 → list[Fact]，確定性、不帶結論、不呼叫 LLM
  → enrichment.extract_evidence   LLM：日誌 → list[EvidenceItem]，經驗證才進 ctx；失敗 → undetermined
  → rca.root_cause.bayes      機率模型：facts + evidence + 拓撲 → 候選根因（帶機率）；undetermined 時略過
  → rca.report                段落組裝（既有）
  → enrichment.rca_narrator   LLM：把已算好的機率與因果路徑寫成戰情摘要，驗證後才採用
```

## 3. 接口型別（已提交：`aiops/engine/rca_types.py`、`PipelineContext` 新欄位）

- `Fact(kind, entity, value, unit, window, detail, source)`：frozen。證據層的唯一輸出，放 `ctx.rca_facts`。
- `EvidenceItem(kind, entity, state, confidence, quote, log_ts)`：frozen。`kind` 必須在 `EVIDENCE_KINDS` 封閉詞表。放 `ctx.rca_evidence`。
- `ctx.rca_status: "ok" | "undetermined"`、`ctx.rca_undetermined_reason: str`。
- 根因輸出沿用 `RootCandidate`（`engine/context.py`）。機率放 `score`（0～1，全體候選加總 ≤ 1 的後驗），
  並在 `attribution["posterior"]` 放各假設的機率拆解；`reason` 寫人看得懂的一句話。需要新欄位就加在 `attribution`，不改 dataclass。

### 事實 kind 詞彙（證據層產出，機率模型消費）

`self_time_delta`、`wait_delta`（對某資源／下游的等待增量）、`entry_failure_rate`、`entry_latency`、
`fanout_ratio`（每筆入口請求的平均呼叫數，前後比）、`child_outlives_parent`（子 span 晚於父 span 結束多少 ms，**只陳述事實，
不判斷是逾時還是時鐘偏移**）、`failed_without_failed_child`（入口失敗且底下沒有失敗子 span 的比例）、`traffic_drop`（下游流量變化）、
`log_template_surge`、`mq_lag`。新增 kind 要在這份文件登記。

## 4. 檔案歸屬（每個 session 只動自己的）

| 段 | 檔案 | 規則 |
| :-- | :-- | :-- |
| 證據層 | `aiops/stages/rca/facts/`（新）、`rca.facts` 算子 | 可 import 既有 `spans.py`／`logs.py`／`mq.py` 當純函式；**不准 import `enrichment`**；沒有 LLM 也要能算 |
| LLM 抽取與摘要 | `aiops/stages/enrichment/evidence_extractor.py`、`rca_narrator.py`（新）、`CLAUDE.md` §1 第 3 條與 `tests/test_architecture.py` 對應測試 | 網路只能在 `enrichment/` |
| 機率模型 | `aiops/stages/rca/bayes/`（新）、`rca.root_cause.bayes` | 不 import `enrichment`、不連網；純函式、可重現 |
| 混沌並排 | `scripts/chaos/`、`run.py` 新增 `--rca cognitive` | 只讀平台；不改 `aiops/` |
| 整合（主 session） | `rca_report_cognitive.yaml`、`rca.report` 對 undetermined 的段落處理 | |

## 5. 各段規格重點

**證據層**：把 `spans.py`／`evidence.py` 裡「解讀」的部分拆掉，只留事實。例如子 span 比父 span 晚結束 13 秒，
只輸出 `child_outlives_parent=13000`，不再記成「時鐘偏移」。`_fanout` 搬進來當事實。舊檔案不刪、不改行為（舊引擎還要並排跑）。

**LLM 抽取**：輸入是事件期間與基準期間的日誌樣板（含代表性原文），不是整包原始日誌。輸出必須是 JSON、`kind` 在詞表內、
`entity` 是已知實體、`quote` 逐字出現在送入的日誌裡，否則該項丟棄；全數被拒或格式錯 → `undetermined`。
timeout、重試次數由節點 params 設，預設逾時後不重試。**沒有 `noop`**：未設定供應商就是 `undetermined`（原因：`llm_not_configured`）。
測試用的假供應商由測試或混沌腳本自己用 `registry.register` 登記，**不放進 `aiops/`**。

**機率模型**：假設空間 = 每個實體的「自身故障」＋「受上游/下游傳導」＋「被熔斷／逾時切斷（呼叫端行為）」。
用 noisy-OR 與寫在程式常數的先驗（附註解與出處，先驗要有一份可調的 YAML 或常數檔）。呼叫圖有環（A↔B）時先做強連通分量縮點，
縮成一個節點再算，結果再展開，不准遞迴走訪。證據不足時回空清單並寫 `rca_root_note`（沿用舊規矩）。同一組輸入必須得到同一個排名。

**摘要作者**：只吃已算好的機率、因果路徑、命中的 Runbook 與事實，驗證沿用 `narrative.validate` 的思路（數字、實體白名單）；
驗證不過就退回模板（這一步是旁路，失敗不影響根因結論）。

## 6. 驗收

- 單元測試：每段自己的測試，不改既有測試的斷言（CLAUDE.md §100.1）。
- 混沌：`run.py --rca cognitive` 與預設（舊引擎）逐列並排，貼實測數字。
- 新舊都跑 1–13：新引擎不得輸給 199／200；16、17、21 預期從紅轉綠或至少歸因正確。
- LLM 不可用：混沌加一個「LLM 全掛」場景，驗報告仍產出、狀態 `undetermined`、事實與原始證據都在。
