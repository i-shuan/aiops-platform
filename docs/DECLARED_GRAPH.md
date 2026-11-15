# 宣告的關係圖（沒有 trace 的環境）

有 trace 的環境，服務之間的呼叫關係由 `admin.topology_refresh` 從 span 沉澱。沒有 trace 的環境（工廠、純指標、沒裝探針的系統）
沒有這個來源，收斂只能一個實體一張卡、RCA 分不出上下游。這份文件說明怎麼用宣告補上這張圖。

## 一條邊怎麼寫

```yaml
graph:
  - {from: press-1, to: chiller, kind: call}      # press-1 依賴 chiller
  - {from: chiller, to: press-2, kind: supply}    # chiller 供應 press-2（跟上一行是同一種關係，從另一頭寫）
  - {from: etch-3, to: litho-1, kind: flow, weight: 40}   # 物料從 etch-3 流到 litho-1，40 批
  - {from: order-api, to: ship-worker, kind: async, weight: 800}   # 訊息佇列，延遲 P95 800 毫秒
```

`kind` 必填、沒有預設值。方向照資料來源的說法寫，**不要自己翻**——異常往哪邊傳由型別決定（`engine/graph.KINDS`）：

| kind | from -> to 的意思 | 誰出事會拖垮誰 | 用在哪 |
| :-- | :-- | :-- | :-- |
| `call` | from 呼叫／依賴 to | to -> from | 收斂（`alert.topology_chain`）、同根因抑制、RCA |
| `supply` | from 供應 to（電、氣、冷卻水、共用主機） | from -> to | 同上（`call A -> B` 與 `supply B -> A` 結果完全一樣） |
| `flow` | from 是 to 的上游（物料、Lot） | from -> to | Lot 收斂（`alert.lot_path_chain`，`min_lots` 會篩 weight）、RCA |
| `async` | from 發訊息給 to | 不進傳播圖 | 只給訊息收斂（`alert.messaging_link`）；weight 是延遲 P95 毫秒 |

要新增一種關係：在 `EdgeKind` 加一個值、在 `KINDS` 填它的方向，不用改任何算子。

## 兩個入口

| 入口 | 適合 | 範圍 |
| :-- | :-- | :-- |
| 管線 YAML 的 `graph` | 少量、跟著管線版本走 | 只有這條管線 |
| `POST /api/v1/ingestion/graph/import`（`admin.graph_import`） | 大量、要跟 CMDB 同步 | 同一個 `environment` 的所有管線 |

匯入的請求：`{"mode": "replace" | "merge", "environment": "", "source": "cmdb", "edges": [...]}`。
`replace` 把這個環境的宣告邊整份換掉（單一交易，`edges` 給空清單 = 清空）；`merge` 加進去、同一條邊更新 weight。

宣告的邊跟觀測到的邊（trace 沉澱、Lot 履歷）**取聯集**，存在自己的表 `declared_edges`
（`migrations/postgres/0010_declared_edges.sql`），不會像 trace 沉澱的邊那樣因為太久沒被看到而汰除。
既有的 `topology`／`lot_edges` 欄位照舊（給了就整張取代觀測到的同類邊）。

## 防呆

| 寫錯的樣子 | 什麼時候擋 |
| :-- | :-- |
| 沒寫 `kind`、寫了不認得的 `kind`、多了欄位（`direction:`） | YAML 載入失敗；API 回 422 |
| 自己指自己 | YAML 載入失敗；匯入回 400 |
| 不同型別把同一對實體宣告成相反方向（`call A -> B` 又 `supply A -> B`） | YAML 載入失敗；匯入整批拒絕、什麼都不寫（merge 時連同資料庫裡既有的宣告一起檢查） |
| 宣告的 `call` 跟 trace 觀測到的呼叫方向相反 | 不擋（觀測是事實、宣告是人的說法，由人決定改哪一邊），結果的 `conflicts_with_observed` 列出來 |

擋不到的：**型別本身選錯**。`call chiller -> press`（chiller 依賴 press）在格式上完全合法，根因會指到 press
（`test_a_wrongly_declared_kind_points_the_other_way_which_is_why_kind_is_mandatory`）。這只能靠匯入後看結果的 `by_kind`
與抽查；程式不知道真實世界誰供應誰。

## 沒有 trace 時 RCA 用到什麼

`rca_report` 的根因算子是 `rca.root_cause.evidence`；沒有 span 時退回 `rca.root_cause.topological`：
在傳播圖上，「卡片裡沒有其他異常上游」的實體是源頭候選，依首次異常時間排序。它需要的輸入：

| 輸入 | 來源 | 沒有 trace 時有沒有 |
| :-- | :-- | :-- |
| 卡片裡有哪些實體、各自首次異常時間、最高分數 | 事件群裡的異常點（偵測或外部匯入） | 有 |
| 傳播方向的邊 | `ctx.graph`：觀測到的邊＋宣告的邊（RCA 用事件群建立當下的快照） | 宣告了就有 |

`ingestion.incident_series`（重抓事件時段的原始時序）**沒有掛**，拓撲推測也不需要它：它餵的是 `rca.facts` 的時序事實，
只有 LLM 版（`rca_report_llm`）會讀，而沒有 trace 的管線（`factory`、`metrics_only`）路由到的是 `rca_report`。
要讓沒有 trace 的環境也走 LLM 版，才需要把它掛進 `rca_report_llm`——那是另一個決定（每次出報告多打幾次資料源），這次沒做。

## 拓撲推測的限制（不是這次造成的，宣告圖之後才會遇到）

- 只看「卡片裡誰沒有異常的上游」：真根因自己沒有告警（不在卡片裡）時一定指錯；全部互為上下游時全部列出。
- 假設異常沿依賴方向傳：上游把流量放大壓垮下游時會排反（有 trace 時由 `evidence` 處理）。
- 不看時序形狀，同一張卡裡只用首次異常時間排先後，解析度是偵測的一輪（1～5 分鐘）。

## 驗證

- 單元與端到端：`tests/test_declared_graph.py`（方向表、載入期防呆、匯入、環境隔離、快照重現、沒有 trace 的收斂與根因）。
- 企業考卷 e1 的 D 軌（沒有 trace＋宣告 8 條同步呼叫邊）：見 `docs/ENTERPRISE_EXAM_RESULTS.md`（分支 `chaos/enterprise-exam`）。
- **真實資料沒有驗證過**：沒有任何真實的 CMDB 或工廠拓撲匯入過，考卷是合成的 IT 架構。
