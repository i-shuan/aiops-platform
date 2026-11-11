# 版本 1.0

# RCA 三引擎並排混沌測試結果（場景 1–13）

> **歷史紀錄**：「機率模型」欄的 `rca_report_cognitive`／`rca.root_cause.bayes` 之後已移除（它前面必須靠 LLM 抽證據，當不了離線備援；
> 程式在 git 歷史與 tag 1.1）。現在只剩舊引擎（`rca_report`，確定性備援）與 LLM 判斷（`rca_report_llm`，主力）；數字保留作對照，沒有重跑。

- 日期：2026-11-10
- 分支：`arch/cognitive-rca`（未 commit）
- 三個引擎：**舊引擎**（`rca_report`）、**機率模型**（`rca_report_cognitive`，bayes）、**LLM 判斷**（`rca_report_llm`，呼叫圖＋事實＋日誌證據一起交給 LLM）
- 資料：混沌測試的合成資料；LLM 為真實供應商。舊引擎、機率模型各跑 1 次，LLM 判斷每場景跑 3 次
- 分數格式：通過檢查項數／總檢查項數

## 1. 逐場景對照

| 場景 | 舊引擎 | 機率模型 | LLM 判斷（3 次） | LLM 耗時 |
|---|---|---|---|---|
| 1 外部 API 變慢 | 6/6 | 3/6 | 6/6、6/6、6/6 | 約 20 秒 |
| 2 | 9/9 | 6/9 | 9/9 ×3 | 約 19 秒 |
| 3 兩個下游平分延遲 | 7/7 | 4/7 | **3/7 ×3** | 約 22 秒 |
| 4 | 5/5 | 3/5 | 5/5 ×3 | 約 19 秒 |
| 5 純程式例外 | 8/8 | 7/8 | **7/8 ×3** | 約 16 秒 |
| 6 | 10/10 | 崩潰 | 10/10 ×3 | 約 125 秒 |
| 7 | 10/10 | 10/10 | 10/10 ×3 | 約 6 秒 |
| 8 | 9/9 | 9/9 | 9/9 ×3 | 約 1 秒 |
| 9 | 6/6 | 6/6 | 6/6 ×3 | 約 1 秒 |
| 10 | 8/8 | 8/8 | 8/8 ×3 | 約 1 秒 |
| 11 | 12/12 | 7/12 | 12/12 ×3 | 約 46 秒 |
| 12 訊息佇列（12a–12g） | 76/77 | 53/77 | **53/77、52/77、54/77** | 約 390 秒 |
| 13 流量放大（13a–13c） | 33/33 | 15/33 | **16/33、17/33、16/33** | 約 76 秒 |

結論：
- LLM 判斷明顯優於機率模型（場景 1、2、4、6、11 機率模型輸、LLM 全過；場景 6 機率模型崩潰）。
- 仍輸給舊引擎的是場景 3、5、12、13。場景 13（放大者指認）LLM 判斷 16～17/33，只比機率模型的 15/33 好一點：13a 7/13、13b 6/13、13c 3/7（舊引擎全過）。
- 同一場景連跑 3 次結果幾乎相同，沒有忽過忽不過；場景 12 差 1–2 項。
- 速度：LLM 判斷比舊引擎慢很多（場景 12 約 6 分鐘 vs 45 秒）。

## 2. 場景 3、5、12 分別是什麼

| 場景 | 情境 | 舊引擎 | LLM 判斷 | 沒過的是什麼 | 性質 |
|---|---|---|---|---|---|
| 3 | cart-api 總耗時 +1000 ms：redis +450、postgres +450、自身 +100，沒有單一資源過 60%（45%／45% 死區） | 7/7 | 3/7 | 4 項：人類字串要留空（實際填了 `redis`）；`attribution.by` 要為空；歸因要同時記兩個資源（只記了 redis）；理由要列出兩個資源 | LLM 只指了一個資源，不符「平分時不指單一元凶」的舊格式要求；沒有指認別的服務 |
| 5 | cart-api 遇髒資料，controller 第一行拋 NullPointerException（HTTP 500），trace 只有 root transaction，沒有子 span | 8/8 | 7/8 | 1 項：歸因欄位要求為空 `{}`，實際多了 `judge`、`cause`、`cites` | 根因判斷（cart-api 自己出錯）是對的；是我加的 LLM 專屬欄位與舊斷言格式衝突 |
| 12 | 訊息佇列一整組（12a–12g，共 77 項） | 76/77 | 53/77 | 見下表 | 見下表 |

### 場景 12 子場景（LLM 判斷第 1 次）

| 子場景 | 情境 | 沒過項數 | 沒過的內容 |
|---|---|---|---|
| 12a | 消費者自己的 DB 變慢造成積壓（對照組：不准怪生產者） | 3 | `attribution.async` 要標成消費者、判定為積壓（實際空）；平台算出的延遲 P95 對不上真值（0.00 vs 8.20 分）；理由要寫明回頭看過生產者 |
| 12b | 生產者 12:00 出事、消費者 12:20 才失敗（parent 傳遞） | 6 | 第一名要是生產者 ord-api（實際沒有候選）；消費者錯誤增量要轉記給生產者；生產者要有下鑽歸因 orders-db |
| 12c | 同 12b，但消費者只用 span.links 指回生產者 | 7 | 同 12b |
| 12d | 舊版 agent，沒有 link／parent／age，跨不了時窗 | 1 | 排名與分數要跟改前一模一樣（實際不同） |
| 12e | 生產者毫無症狀的毒訊息（已知死區） | 4 | 要說明回頭看過生產者；要求人工抽查 dead-letter；`attribution.async` 要標 `suspect_payload` |
| 12f | 訊息延遲 3 小時，超過 max_lag_minutes（120），不往回撈 | 1 | 排名與分數要跟改前一模一樣 |
| 12g | 生產者與消費者同一輪都異常時併成一張事件卡 | 1 | Runbook 要命中 RB-07（實際 RB-02） |

場景 12 的失分多數是檢查項要求舊引擎專有的結構（`attribution.async`、錯誤增量轉記、與改前完全一致的排名），LLM 判斷沒有產出這些。是否算判斷錯誤，要逐項人工看 LLM 的回覆才能分辨，目前尚未查。

## 3. 其他觀察

- **場景 1 修正前後**：第一次 LLM 判斷只有 2/6。原因是送給 LLM 的呼叫圖是空的（外部 API、資料庫不是服務實體，沒有畫進圖），LLM 回「payment-gw:443 不在已知實體中，無法判定」。修正：圖上補「服務 → 資源」的邊；提示詞寫明「時間花在等某資源時，候選填該服務並指名資源」；指名資源後，等待時間、失敗數、佔比由平台從事實抄進歸因欄位。修正後 6/6。
- **驗證有擋到內容**：場景 11 有一次 LLM 在理由裡寫了事實中找不到的數字（1.975），被擋掉，那一輪狀態為「無法判定」，沒有排名；其他輪正常。
- **全套 pytest**：1365 通過、1 跳過、20 預期失敗（已知死區）。

## 4. 已知限制與待辦

- LLM 的信心是自評，不是校準過的機率；同一輸入理論上可能給不同排名（本次 3 次結果相近，但樣本小）。
- 場景 3：已查明是我的去重規則丟掉第二個資源候選（見第 5 節），應改成同一實體可保留多個資源候選。場景 5：只是歸因欄位格式。場景 12：見第 5 節觀察；12b／12c 的 LLM 多半指到消費者而非生產者，是判斷與期望不同，不只是格式問題。
- 場景 6 每次約 125 秒、場景 12 約 390 秒，需評估實際可接受的延遲。
- 本次只用合成資料；把真實日誌或指標送到外部 LLM，需要另外取得使用者同意。
- 開發用的 OpenAI key、Grafana／ES 密碼、API token 在上線或合併 main 前要更換。
- 尚未做：用真實 ES／Prometheus 資料回放驗收（CLAUDE.md 第 99 條）。

## 5. LLM 原始回應（本次重跑記錄，非前述 3 次測試的同一批輸出）

說明：這是另外重跑一次場景 3、5、12 時，記下的每次 LLM 原始回覆（API 金鑰與送出的完整 prompt 未收錄；prompt 長度見各項）。LLM 每次輸出可能略有不同，所以分數與前表不一定完全相同（例如場景 12 這次是 54/77）。場景 12 這一輪共 16 次 LLM 呼叫，記錄檔沒有標註每次屬於 12a–12g 哪個子場景，下面依呼叫順序列出，不硬配對。

### 場景 3：兩個下游平分延遲

LLM 給了兩個候選：cart-api 等 redis、cart-api 等 postgres，信心都是 0.5，並在 note 說明無法分辨哪個資源先出問題。**但平台對同一個實體只保留第一個候選，postgres 那個被丟掉**，所以歸因只記了 redis，檢查失敗。這是程式的去重規則造成的，不是 LLM 判斷錯。

#### 場景 3 回覆

**第 1 次呼叫**（prompt 4264 字）｜平台最後採用的第一名：`[['cart-api', 0.5, 'redis']]`｜Runbook：`RB-00`

```json
{
  "candidates": [
    {
      "entity": "cart-api",
      "confidence": 0.5,
      "subtype": "dependency",
      "resource": "redis",
      "reason": "cart-api 入口延遲為 1039.0 ms，等待 redis 的增量為 452.3 ms；資料支持依賴等待是延遲來源之一，但不足以確認它是否是唯一根因。",
      "cites": [
        "F3",
        "F11"
      ]
    },
    {
      "entity": "cart-api",
      "confidence": 0.5,
      "subtype": "dependency",
      "resource": "postgres",
      "reason": "cart-api 入口延遲為 1039.0 ms，等待 postgres 的增量為 448.7 ms；資料支持依賴等待是延遲來源之一，但不足以確認它是否是唯一根因。",
      "cites": [
        "F3",
        "F12"
      ]
    }
  ],
  "note": "redis 與 postgres 的等待增量都明顯，現有資料無法分辨哪個資源先出問題，也無法排除兩者同時變慢；pay-api 的延遲與等待指標則沒有明顯變化。"
}
```

### 場景 5：純程式例外

回覆正確：cart-api 自己出錯，信心 0.99，引用了 NullPointerException 日誌。唯一失分是歸因欄位多了 `judge`、`cause`、`cites`。

#### 場景 5 回覆

**第 1 次呼叫**（prompt 4494 字）｜平台最後採用的第一名：`[['cart-api', 0.99, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "cart-api",
      "confidence": 0.99,
      "subtype": "error",
      "resource": "",
      "reason": "cart-api 事件期間入口失敗率為 1.0，且失敗請求中沒有下游失敗的比例也是 1.0，符合服務自身直接產生錯誤。錯誤日誌樣板顯示 Cart 為 null 時發生 NullPointerException；入口耗時降至 1.986 ms，也不支持依賴變慢是根因。",
      "cites": [
        "F1",
        "F3",
        "F5",
        "F9"
      ]
    }
  ],
  "note": ""
}
```

### 場景 12：訊息佇列（77 項，這次 54/77）

觀察：
- 16 次呼叫裡有 13 次第一名是消費者 ship-worker；只有 1 次（第 15 次）指到生產者 ord-api（等 orders-db）。要求「第一名是生產者」的 12b、12c 因此失分。
- 第 14、16 次回空候選，note 寫「無法判斷 payload 是 ord-api 產生還是 ship-worker 版本造成，需要 producer 端 payload 或部署版本證據」。這很可能對應 12e（毒訊息，已知死區），LLM 沒有硬猜，行為符合預期。
- 所有回覆都沒有 `attribution.async`、錯誤增量轉記這類舊引擎專有結構，這些檢查項必然失分，跟判斷對錯無關。
- 對照 12a（消費者自己的 DB 變慢）：第 1 次回覆指認 ship-worker 等 ship-db（0.97），方向正確，但「平台算出的延遲 P95」和「async 標成積壓」兩項仍失分。

#### 場景 12 回覆

**第 1 次呼叫**（prompt 6155 字）｜平台最後採用的第一名：`[['ship-worker', 0.97, 'ship-db']]`｜Runbook：`RB-02`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.97,
      "subtype": "dependency",
      "resource": "ship-db",
      "reason": "ship-worker 的平均耗時從 219.7 ms 升至 820.6 ms，而等待 ship-db 的時間從 199.8 ms 升至 800.7 ms；自身 app 時間則為 19.91 ms 與 19.94 ms，顯示主要延遲來自等待 ship-db。",
      "cites": [
        "F6",
        "F16",
        "F20"
      ]
    }
  ],
  "note": ""
}
```

**第 2 次呼叫**（prompt 6613 字）｜平台最後採用的第一名：`[['ship-worker', 0.96, 'ship-db']]`｜Runbook：`RB-02`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.96,
      "subtype": "dependency",
      "resource": "ship-db",
      "reason": "ship-worker 的入口耗時由事件前 219.7 ms 升至 820.6 ms，而自身 app 耗時為 19.91 ms 與 19.94 ms；等候 ship-db 的時間由 199.8 ms 升至 800.7 ms，指向它是主要延遲來源。",
      "cites": [
        "F6",
        "F17",
        "F21"
      ]
    }
  ],
  "note": ""
}
```

**第 3 次呼叫**（prompt 6177 字）｜平台最後採用的第一名：`[['ship-worker', 0.98, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.98,
      "subtype": "error",
      "resource": "",
      "reason": "F13 顯示新出現的錯誤樣板為「invalid message payload (schema mismatch)」，而 F9 顯示其失敗都沒有下游呼叫失敗，指向 ship-worker 自身直接產生錯誤。",
      "cites": [
        "F3",
        "F9",
        "F13",
        "F20"
      ]
    }
  ],
  "note": ""
}
```

**第 4 次呼叫**（prompt 6599 字）｜平台最後採用的第一名：`[['ship-worker', 0.78, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.78,
      "subtype": "error",
      "resource": "",
      "reason": "ship-worker 有入口失敗，且所有失敗都沒有失敗的下游呼叫；新出現的日誌樣板指出訊息 payload schema mismatch，較符合在消費端直接處理失敗，而非依賴變慢。",
      "cites": [
        "F3",
        "F9",
        "F13",
        "F14"
      ]
    }
  ],
  "note": "證據無法分辨 schema mismatch 是由 ord-api 發布了不相容的 payload，還是 ship-worker 的程式或 schema 有問題；F14 顯示訊息延遲事件前後相同，無法支持本次事故由積壓惡化引起。"
}
```

**第 5 次呼叫**（prompt 6177 字）｜平台最後採用的第一名：`[['ship-worker', 0.97, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.97,
      "subtype": "error",
      "resource": "",
      "reason": "ship-worker 的入口失敗率為 0.4933，且 0.4933 的失敗都沒有失敗的下游呼叫；同時出現新的「invalid message payload (schema mismatch)」錯誤樣板，指向服務自身因訊息格式不符而直接失敗。",
      "cites": [
        "F3",
        "F9",
        "F13"
      ]
    }
  ],
  "note": ""
}
```

**第 6 次呼叫**（prompt 6597 字）｜平台最後採用的第一名：`[['ship-worker', 0.98, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.98,
      "subtype": "error",
      "resource": "",
      "reason": "ship-worker 有 0.4933 的入口失敗率，且 F9 顯示這些失敗都沒有失敗的下游呼叫；同時出現新的「invalid message payload (schema mismatch)」錯誤樣板，指向服務自身處理訊息時出錯。",
      "cites": [
        "F3",
        "F9",
        "F13"
      ]
    }
  ],
  "note": ""
}
```

**第 7 次呼叫**（prompt 6133 字）｜平台最後採用的第一名：`[['ship-worker', 0.98, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.98,
      "subtype": "error",
      "resource": "",
      "reason": "ship-worker 有 220/446 個請求失敗，且所有失敗都沒有失敗的下游呼叫；同時出現新的「invalid message payload (schema mismatch)」錯誤樣板，指向服務自身直接產生錯誤。",
      "cites": [
        "F3",
        "F9",
        "F13"
      ]
    }
  ],
  "note": ""
}
```

**第 8 次呼叫**（prompt 6133 字）｜平台最後採用的第一名：`[['ship-worker', 0.98, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.98,
      "subtype": "error",
      "resource": "",
      "reason": "ship-worker 有 220/446 入口請求失敗，且這些失敗都沒有下游失敗；F13 顯示新出現的「invalid message payload (schema mismatch)」錯誤樣板，指向服務自身直接產生錯誤。",
      "cites": [
        "F3",
        "F9",
        "F13"
      ]
    }
  ],
  "note": ""
}
```

**第 9 次呼叫**（prompt 6178 字）｜平台最後採用的第一名：`[['ship-worker', 0.65, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.65,
      "subtype": "error",
      "resource": "",
      "reason": "ship-worker 的事件期失敗率為 0.4933（F3），且失敗中 1.0 的比例沒有失敗的下游呼叫（F9）；同時出現新的「invalid message payload (schema mismatch)」錯誤樣板（F13），因此它是最直接的異常候選。",
      "cites": [
        "F3",
        "F9",
        "F13"
      ]
    }
  ],
  "note": "現有資料無法判定 schema 不符是 ship-worker 自身處理錯誤，還是上游 ord-api 產生了不符合預期的訊息；缺少訊息內容及生產端證據。"
}
```

**第 10 次呼叫**（prompt 6600 字）｜平台最後採用的第一名：`[['ship-worker', 0.96, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.96,
      "subtype": "error",
      "resource": "",
      "reason": "ship-worker 的入口失敗率為 0.4933，且失敗全部沒有失敗的下游呼叫；同時出現新的「invalid message payload (schema mismatch)」錯誤樣板，指向服務直接因訊息格式不符而失敗。",
      "cites": [
        "F3",
        "F9",
        "F13"
      ]
    }
  ],
  "note": ""
}
```

**第 11 次呼叫**（prompt 6140 字）｜平台最後採用的第一名：`[['ship-worker', 0.93, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.93,
      "subtype": "error",
      "resource": "",
      "reason": "ship-worker 有 220/446 個請求失敗，且失敗請求都沒有失敗的下游呼叫；同時出現新的「invalid message payload (schema mismatch)」錯誤日誌樣板，指向服務自身因訊息格式不符而直接回錯。",
      "cites": [
        "F3",
        "F9",
        "F13"
      ]
    }
  ],
  "note": ""
}
```

**第 12 次呼叫**（prompt 6140 字）｜平台最後採用的第一名：`[['ship-worker', 0.85, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.85,
      "subtype": "error",
      "resource": "",
      "reason": "ship-worker 的入口失敗率為 0.4933，且失敗中 0.4933 沒有下游失敗；新出現的錯誤日誌樣板指出 invalid message payload（schema mismatch），較符合服務直接回錯，而非下游依賴故障。",
      "cites": [
        "F3",
        "F9",
        "F13"
      ]
    }
  ],
  "note": "呼叫圖沒有 async 生產者資訊，無法判斷不合法訊息是否由圖外來源產生；目前證據支持 ship-worker 是直接報錯的實體。"
}
```

**第 13 次呼叫**（prompt 6189 字）｜平台最後採用的第一名：`[['ship-worker', 0.82, None]]`｜Runbook：`RB-03`

```json
{
  "candidates": [
    {
      "entity": "ship-worker",
      "confidence": 0.82,
      "subtype": "error",
      "resource": "",
      "reason": "ship-worker 有 220 次失敗，且失敗請求全部沒有失敗的下游呼叫（F9）；同時出現新的「invalid message payload (schema mismatch)」錯誤樣板，事件期間記錄頻率為 228.0（F13），較符合自身處理訊息時直接出錯。",
      "cites": [
        "F9",
        "F13"
      ]
    }
  ],
  "note": "證據指出錯誤發生在 ship-worker，但無法確認 schema 不符是由消費端程式造成，還是由 ord-api 生產了不合規訊息；目前沒有足夠證據將 ord-api 判為根因。"
}
```

**第 14 次呼叫**（prompt 6609 字）｜平台最後採用的第一名：`[]`｜Runbook：`RB-03`

```json
{
  "candidates": [],
  "note": "ship-worker 有 220 次沒有失敗子呼叫的失敗，且出現新的 schema mismatch 日誌（F9、F13），但資料無法判斷不相容的 payload 是由 ord-api 產生，還是 ship-worker 的處理／版本造成。ord-api 等待 orders-db 的時間增加（F19），但沒有證據將此變化與 ship-worker 的 payload 錯誤連起來；需要 producer 端 payload 與 consumer schema／版本的驗證資訊才能分辨根因。"
}
```

**第 15 次呼叫**（prompt 6273 字）｜平台最後採用的第一名：`[['ord-api', 0.8, 'orders-db']]`｜Runbook：`RB-02`

```json
{
  "candidates": [
    {
      "entity": "ord-api",
      "confidence": 0.8,
      "subtype": "dependency",
      "resource": "orders-db",
      "reason": "ord-api 的入口耗時為 394.4 ms，且等待 orders-db 增加 342.3 ms；相比之下，其自身耗時增量僅 342.8 ms 的總延遲變化中有明確的 orders-db 等待證據，較符合依賴資源變慢。",
      "cites": [
        "F5",
        "F15",
        "F19"
      ]
    }
  ],
  "note": "ship-worker 有 220 次直接失敗，並出現新的 schema mismatch 日誌樣板；但資料不足以確認錯誤 payload 是否由 ord-api 產生，也無法證明這與 ord-api 等待 orders-db 有關。"
}
```

**第 16 次呼叫**（prompt 6693 字）｜平台最後採用的第一名：`[]`｜Runbook：`RB-03`

```json
{
  "candidates": [],
  "note": "F13 顯示 ship-worker 收到 schema mismatch 的訊息並記錄錯誤，但無法判斷是 ord-api 發出了不符合契約的 payload，還是 ship-worker 的 schema／版本不相容；需要 producer 端 payload 或部署版本證據。F20 證明 ord-api 等待 orders-db 時間增加，可解釋 ord-api 的延遲，但沒有證據將其與 ship-worker 的錯誤連起來。"
}
```

