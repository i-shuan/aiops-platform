# 混沌新舊 RCA 並排（legacy ↔ cognitive）

合約：`docs/COGNITIVE_RCA_DESIGN.md`。這裡只動 `scripts/chaos/`，平台一行不改。

## 用法

```bash
# 舊引擎（預設，輸出跟加這些選項之前逐列相同；不含只對 cognitive 有意義的 24）
.venv/Scripts/python scripts/chaos/run.py --out legacy.md
# 新引擎（rca_report_cognitive 還沒建立就 exit 2，不退回 legacy）
.venv/Scripts/python scripts/chaos/run.py --rca cognitive --out cognitive.md
# 並排：每個場景先 legacy 再 cognitive，每條斷言兩欄＋每次 RCA 的根因排名、分數、Runbook、LLM 證據
.venv/Scripts/python scripts/chaos/run.py --compare 1 2 3 24 --out compare.md
# cognitive 時 LLM 換成哪種假 LLM 模式（預設 faithful；yaml = 照 YAML 的真供應商）
.venv/Scripts/python scripts/chaos/run.py --rca cognitive --llm down 1
```

## 接線

- 所有場景的 RCA 都走 `harness.rca()`（`rca_report` 原本寫死在這裡一處）；現在依 `harness.use_rca()` 選 workflow。
  cognitive 時把 operator 是 `enrichment.extract_evidence`／`enrichment.rca_narrator` 的節點 `provider` 換成 `chaos_scripted`、
  `model` 帶模式（`scripted_llm.wire`）。一個 LLM 節點都找不到就報錯，不讓「LLM 全掛」在 LLM 沒接上時假裝通過。
- 每次 `rca()` 的關鍵數字記進 `harness.RCA_LOG`，`--compare` 印成表。

## 腳本化假 LLM（`scripted_llm.py`）

**只測平台怎麼處理 LLM 的輸出，不代表真模型的抽取品質。** 真模型要另外用真實供應商與錄製回應驗，不能拿這個的通過當證據。
模式：`faithful`／`down`／`slow[:秒]`／`garbage`／`hallucinate`。`python -m scripts.chaos.scripted_llm` 印各模式對熔斷、GC、死結三行日誌的回傳。

## 場景 24：LLM 全掛（`scen_cognitive.py`）

24a down、24b slow、24c garbage、24d hallucinate：報告照出、`rca_status == "undetermined"`、原因代碼非空、
沒有根因候選、報告寫「無法判定」、`ctx.rca_facts` 有東西且出現在報告裡、`trace_evidence`／`log_patterns` 段落在；slow 另驗逾時生效。
24e faithful 對照組：熔斷證據通過驗證、quote 逐字在世界日誌裡、狀態 ok、有候選。
cognitive workflow 不存在時五列都是「未執行（尚無 cognitive 引擎）」。

## 需要跟其他段對齊的介面（猜的部分）

抽取算子還沒寫好，下面幾點是照合約猜的。整合時改這裡去對齊算子的實作，不要為了遷就假 LLM 去改平台。

1. **供應商參數**：`wire()` 設的是節點 `params.provider` 和 `params.model`（model 欄位放模式）。抽取／摘要算子要用
   `stages/enrichment/registry.build(provider, model, base_url, api_key)`（或經過它的 `providers`）建 LLM，外部登記的供應商才接得上。
2. **逾時參數名**：依序讀 `timeout_seconds`、`timeout_s`、`timeout`，都沒有就當 30 秒。slow 模式會睡「逾時＋5 秒」。
   假 LLM 是在同一個行程裡睡，不經過 HTTP，所以只靠 httpx 逾時的算子會一直等到它睡完，24b 會紅。這是真的缺口
   （非 HTTP 的供應商會卡住），不是假 LLM 的問題。
3. **prompt 格式**：假 LLM 會讀 JSON 物件（`entity`／`service`／`service.name` 鍵＋字串欄位），也會讀純文字行
   （`[svc]`、`service=svc`）。抽取算子最好每個日誌樣板送一個物件，帶 `entity` 和代表性原文。
4. **回覆形狀**：最上層鍵是 `{"evidence": [...]}`，每項有 `kind, entity, state, confidence, quote, log_ts`。
5. **quote 驗證**：假 LLM 的 quote 是規則命中的**子字串**（例如 `CircuitBreaker 'payment-gw' is OPEN`），不是整行。
   驗證要用「是某筆日誌的子字串」來判斷。
6. **evidence 為空**（faithful 但沒有任何規則命中）算 `ok` 還是 `undetermined`？合約沒寫，要定。
7. **摘要作者**：同一個假供應商也會接到摘要的呼叫，回的是 `{"evidence": [...]}`，預期驗證不過、退回模板。
   如果要測摘要的正常路徑，得另外寫摘要專用的腳本。
8. **原因代碼**：場景 24 只要求非空。最好列一份封閉清單（例如 `llm_unreachable`／`llm_timeout`／`llm_bad_json`／
   `evidence_all_rejected`／`llm_not_configured`），到時候再逐模式斷言對應的代碼。
9. **「無法判定」字樣**：判定不了時，報告段落或 `root_cause.note` 要寫「無法判定」。
10. **事實進報告**：現在 `to_result()` 沒有 facts。場景 24 認 `out["facts"]`，或 key 含 `fact` 的段落，主 session 決定用哪個。
    `rca_status` 也還沒進 `to_result()`，場景是直接讀 ctx。
11. **cognitive YAML 要保留**：`trace_evidence`、`log_patterns` 段落；節點名 `fetch_traces`（場景 12 的 mutate 用名稱找它）；
    `enrichment.llm_briefing` 節點（場景 6 在測它）。
12. **場景 1–13 的斷言讀的鍵**：`attribution` 的 `resource/kind/subtype/share/by/amplifier/suspect_payload/moved_error/ingress_surge/
    failures/async/wait_ms/self_ms`、`dependency`、`root_cause.messaging.{note,edges}`、`result.trace_fetch`、`runbook.id`。
    bayes 沒有填這些鍵，對應的斷言就會紅。這是新引擎的落差，不是斷言的問題（斷言不改）。
