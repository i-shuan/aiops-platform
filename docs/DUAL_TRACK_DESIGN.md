# 雙軌制偵測（致命靜態旁路＋統計多數決）：現況查證、實測與設計稿

> 狀態：**設計稿，等拍板**。這一輪沒改任何產品程式碼。回放與證明用的一次性腳本放在 session scratchpad，沒進 repo（路徑見附錄）。
> 基準：`main` @ `3028902`。

---

## 0. 結論先講

**現況是「半套，而且單點會被吃掉」。**

1. **軌道二（多數決）已經在跑**：`it_otel`、`factory` 都是三選二多數決（`aiops/pipelines/detection/it_otel.yaml:17-26`、`factory.yaml:13-22`）。不過三個偵測器是 **168 時段基準線＋MAD＋分位數漂移**，不是對方說的「MAD＋Quantile＋Z-Score」，基準線也不是「15～60 分鐘滾動」。
2. **軌道一（致命旁路）不存在**：四條偵測管線**都沒有掛 `detector.rules`**。就算照直覺把 rules 掛進去，實際跑管線的結果是：
   - rules 放在多數決**裡面**：單獨 rules 命中直接被否決（`voters=4:need=3:1->0`）。更糟的是**分母從 3 變 4、門檻從 2 票變 3 票**，連原本 2/3 會過的統計異常也會被否決，既有判決改變了（S6）。
   - rules 放在多數決**外面**（ensemble 的 `nodes` 只列三個統計節點；這現在就能用 YAML 做到）：事件活下來了，但**嚴重度只有 P3**（rules 分數是「1＋超出比例」，P0 門檻是 10），**30 分鐘冷卻會吃掉**、**維護窗會吃掉**，**併進進行中的事件群也不會升級**（S2b～S5）。
3. **對方的「88%」有出處，而且出處就在本 repo**：`it_otel.yaml:12` 的註解，來自 commit `be4a70a` 的回放。我用同一批真實資料重跑，**88% 準確重現**（scn 組延遲，窗外警報 512 → 59 分鐘），但它量的是**「警報分鐘數」**，大部分是分位數漂移那種一長段的越線被壓掉；**不是「單點毛刺」**。只看真正的單點毛刺（前後一分鐘都沒越線的孤立點），多數決相對三者聯集只壓掉 **25%～75%**（延遲 mes 組 25%、scn 組 75%），相對最好的單一點偵測器只多壓 **9%～63%**。註解寫「88% 的單點誤報」是把指標講錯了。
4. **真實資料裡沒有致命樣本**：9 條真實 APM 序列（回放段約 10.4 萬個分鐘點）沒有一點達到對方舉例的硬上限（錯誤率最高 46.9%，≥80% 的點：0；p95 最高 1.43 秒，離 60 秒很遠）。「單點極端值被多數決否決」的實例有 23 筆，**全部是錯誤率 7%～29% 的小流量雜訊**（每分鐘 11～49 筆請求），不是致命事件。致命場景只能用合成波形補測（第 5 節，明講是合成）。
5. **實測挖到一個對方沒提、但會直接打穿「5xx 比例 > 80%」的死區**：健康服務的錯誤率大多數分鐘是 0（真實資料 63%～77% 的分鐘 = 0），MAD 的分母保底（`scale_floor`）退回 1.0，**MAD 在 [0,1] 的比例序列上分數最高只有 0.67、永遠到不了 3.5**，卻照樣算參與者、照樣投反對票。實測 9 條錯誤率序列裡 7 條 MAD 一次都沒越線。結果：健康服務**單一分鐘 5xx = 100%，多數決 0 票通過**（W7）。

**建議走方案 B**（第 6 節）：獨立的 `detector.hard_limit` 算子＋ensemble 的 `bypass` 宣告（不進分母）＋計分層 `alert.fatal_floor`（把事件群分數墊到 P0）＋冷卻／同根因認得 `fatal` 標記。全部靠 YAML 宣告、預設關閉，既有管線行為不變；回滾 = 刪 YAML 節點。

---

## 1. 對方主張 vs 程式碼與實測

| 對方主張 | 實際 | 判定 |
| :--- | :--- | :--- |
| 「MAD＋Quantile＋Z-Score 同時跑、2 票成立」 | 現行是 `detector.hourly_baseline`（168 時段）＋`detector.mad`＋`detector.quantile_drift`，`strategy: majority`（3 個參與者要 2 票）。沒有 zscore。`it_apm`、`metrics_only` 只有單一 zscore，沒有多數決 | 偵測器組合不符 |
| 「用 15～60 分鐘滾動基線抓緩慢漂移」 | 現行 hourly／mad 的基準線是**訓練期 7 天學出來、每天重訓**的固定基準線；只有 quantile_drift 用 30 點滾動窗算尾端分位數，但它比對的也是訓練期學到的常態。**滾動 15～60 分鐘的基線恰好抓不到緩慢漂移**：漂移會被當成新常態吸收進基線 | 方向相反 |
| 「多數決可壓掉 88% 單點毛刺」 | 88% 是**窗外警報分鐘數**的降幅（真實重現）；**單點毛刺**的降幅是 25%～75%（見第 4 節） | 數字對、指標錯 |
| 「致命值單點踩線就一票成立、直接 P0」 | 現行沒有任何節點做這件事；就算加 rules，不是被否決就是 P3（第 3 節實跑） | 不存在 |
| 虛擬碼 `if latest_value >= hard_ceiling: return AnomalyResult(severity="CRITICAL_P0", …)` | 違反四條架構規則：①流程寫死在 Python（要 YAML 宣告）②演算法自己決定「要不要跑後面」③事件等級只有 `ANOMALY`／`WARNING`（`engine/types.py:24-25`），P0 是 `alert.notify_payload` 依分數算的（`payload.py:122-123`）④`AnomalyResult` 沒有 severity 欄位、`AnomalyEvent` 是 frozen | 不能照抄 |
| 「只看 latest_value」 | 管線每輪看的是整段 `lookback_minutes`（60 分鐘），不是最後一點；只看最後一點會漏掉兩輪之間的踩線 | 不能照抄 |

---

## 2. 現況查證（附檔案:行號）

### 2.1 `detector.rules` 能表達什麼

- 檔案：`aiops/stages/detectors/rules.py`。
- 能表達：**絕對值上限 `upper`、下限 `lower`**（可同時給）、**連續 N 點 `consecutive`**（預設 1，`rules.py:59-60`：違規段長度 ≥ N 才標）。`require_rule: false` 時規則只來自核准的規則提案，沒有就棄權（`rules.py:25-26`）。
- **不能表達**：比例（要比例就得資料源直接給比例序列）、「N 點中有 M 點」、資料品質門檻（壞值上限、流量下限）、多指標組合（例如「錯誤率 > 80% 且請求數 > 20」）。
- 分數：`1 + 超出比例`（`rules.py:61`）。踩線 5% 分數 1.05。**這個分數設計是給「越線了」用的，不是給嚴重度用的**，直接決定了它在計分層只能拿到 P3。
- 不需訓練：`requires_training = False`（`rules.py:19`），不受 `min_train_points` 限制（`base.py:145`）。
- 規則值寫在哪：節點 `params`（YAML）或 target `overrides[節點名稱]`（`other_params` 含 `upper/lower/consecutive`，`rules.py:18`；`check_override` 在 `Pipeline.__init__` 檢查，`base.py:85-93`、`pipeline.py:196-214`）。另一條路是規則升級器（`admin.rule_promote`，`control_plane/learning/learning.py:7-8`）把統計基準線換算成上下限提案、核准後由 `ctx.detector_params` 帶進來。

### 2.2 `detector.ensemble` 怎麼算票

- `aiops/stages/ensemble/ensemble.py`。
- 參與者 = `nodes`（有寫的話）或 `ctx.detector_runs` 中真的評估過的節點（`ensemble.py:65-66`）。**`detector.rules` 繼承 `DetectorOperator`，有規則就會進 `detector_runs`（`base.py:164`），所以它是參與者之一。**
- 門檻：majority = 超過一半（`ensemble.py:56-62`）。3 個參與者要 2 票；**4 個要 3 票**（strict）。
- 不是參與者的節點，事件原樣保留（`ensemble.py:79`、`99`）。**這是目前唯一能讓 rules 不被否決的路**：`nodes` 只列統計節點。但這是「列白名單排除」，新增統計節點忘了加進 `nodes` 就會變成聯集繞過投票；`nodes` 寫錯字也不會在載入期報錯（只在執行期記 `abstained`）。

**單獨一個 rules 命中、其他沒命中：會被否決。** 實跑證據見第 3 節 S2。

### 2.3 規則命中後，下游會不會降級、延後、吃掉

| 關卡 | 程式碼 | 對單獨 rules 命中的影響 | 實跑 |
| :--- | :--- | :--- | :--- |
| 收斂 `alert.same_entity` | `group.py:20-26`；事件群分數 = 成員最大值（`types.py:107`） | 跟同實體的統計事件併成一群，不會吃掉；分數取最大 | — |
| 計分 `alert.golden_signals`／`p0_boost` | `score.py:22-27`（×1.5）、`40-44`（列名實體 +10，**現行 `entities: []`**） | rules 分數 1.01 → ×1.5 = 1.52，**P3** | S2b |
| 嚴重度 `alert.notify_payload` | `payload.py:122-123`：`severity_for(score, {P0:10,P1:6,P2:3,P3:0})` | **P3（降級）** | S2b |
| 吸收 `alert.absorb_open` | `absorb.py:54`：分數取 `max(進行中, 新)` | 併進進行中的 P2 事件群，分數 1.5 < 4，**不升級**，卡片仍是 P2 | S4 |
| 冷卻 `alert.suppress_dedup` | `suppress.py:96-103`：只有 warning→anomaly 才放行 | 30 分鐘內同實體通知過（事件群已結案）就**吃掉** | S3 |
| 靜音 `alert.suppress_window` | `suppress.py:29-42` | 維護窗內**吃掉**（是否該吃見決策 D3） | S5 |
| 同根因 `alert.suppress_same_root_cause` | `suppress.py:58-78`：下游分數較高就抑制上游 | rules 分數小，**很容易被當成「被拖累的症狀」吃掉**（`it_otel`／`it_apm` 有這個節點） | 程式碼推論 |
| `detector.new_only` | `new_only.py` | 只濾掉已存過的時間點，不影響 | — |

### 2.4 現行管線怎麼接

| 管線 | 偵測節點 | 裁決 | rules | 計分 |
| :--- | :--- | :--- | :--- | :--- |
| `it_otel` | hourly_baseline（`min_weeks: 1`）、mad、quantile_drift | majority | **無** | golden_signals、p0_boost（空） |
| `factory` | hourly_baseline、mad、quantile_drift | majority | **無** | p0_boost（空）；收斂多 `lot_path_chain`；**沒有同根因抑制** |
| `it_apm` | zscore | 無（單一偵測器） | **無** | golden_signals、p0_boost（空） |
| `metrics_only` | zscore | 無 | **無** | 無 |

工廠與 IT 的差別只在收斂（Lot 履歷 vs 服務拓撲）與 IT 多了黃金訊號加權、同根因抑制；**偵測與裁決完全一樣**，兩邊都沒有硬上限。

### 2.5 另一個坑：同一演算法的調參跨節點共用

`storage/hooks.py:131-137`：調參以**算子名稱**查，同一條管線掛兩個 `detector.rules` 節點（一個放 SLA 硬上限、一個接規則升級器的統計規則）會拿到**同一份**核准規則。也就是說，**規則升級器從統計基準線推出來的上下限，會漏進「致命旁路」節點**，變成能直接拉 P0 的規則。這是方案 B 要獨立算子名稱的主因。

---

## 3. 執行證據：真的跑管線（不是讀程式碼推論）

腳本：`prove_bypass.py`（scratchpad）。直接 `Pipeline(workflow).run()`，節點跟 `it_otel.yaml` 一樣（hourly_baseline `min_weeks: 1`、mad、quantile_drift、majority、new_only、same_entity、golden_signals、p0_boost、absorb_open、suppress_window、suppress_dedup 30 分、payload、heartbeat），外加 `detector.rules {upper: 200}`。資料：訓練 7 天分鐘級、測試 60 分鐘、最後一分鐘放波形。

| 情境 | 投票 audit | 存活事件 | 嚴重度／結果 |
| :--- | :--- | :--- | :--- |
| S1 基準 50±3，最後一點 210（統計也會同意），rules 在票內 | `voters=4:need=3:1->1` | vote（detect, mad, rules） | P0（分數 95.5，來自統計，不是 rules） |
| **S2 基準 190±4 貼近上限，最後一點 203（只有 rules 會叫），rules 在票內** | **`voters=4:need=3:1->0`** | **無** | **被多數決否決，沒有任何通知** |
| S2b 同 S2，`vote.nodes` 只列三個統計節點（rules 在票外） | `voters=3:need=2:1->0` | rules 原樣保留 | **P3**（分數 1.01×1.5 = 1.52） |
| S3 = S2b ＋ 20 分鐘前通知過同實體（已結案） | 同上 | rules | **`dedup：30 分鐘內已通知過`，沒通知** |
| S4 = S2b ＋ 同實體有進行中的 P2 事件群 | 同上 | rules（被吸收） | 進行中事件群仍是 **P2**、沒升級 |
| S5 = S2b ＋ 維護窗 | 同上 | rules | **`silence_window：maintenance#9`，沒通知** |
| **S6 基準 50±3，最後一點 120（統計 2/3 同意、沒到上限），rules 在票內** | **`voters=4:need=3:1->0`** | **無** | **原本會發的 P0 被否決：加 rules 改變了既有判決** |
| S6b 同 S6，rules 在票外 | `voters=3:need=2:1->1` | vote（detect, mad） | P0 |

結論：**rules 放票內 = 單點致命被否決＋既有判決變嚴；放票外 = 活下來但 P3、會被冷卻／維護窗吃、不升級。** 兩種都不符合「不等、不降、不被冷卻吞掉」。

---

## 4. 真實資料回放

**資料**：dev ES（APM transaction）先前匯出的 9 條真實分鐘序列，跟 commit `be4a70a` 算 88% 用的是同一批。

- scn 組 5 條（cart-api、pay-api、bill-svc、inv-svc、ord-svc）：10/24 13:20 → 11/3 16:20Z，訓練前 7 天、回放約 3 天（每條 4,500 點），已知事故窗 11/3 14:00–16:30Z。
- mes 組 4 條（mes-api、order-svc、report-svc、wip-worker）：10/12 → 11/2，訓練前 7 天、回放約 14 天（每條 20,309 點），**沒有標註的事故窗**，所以 mes 組的「窗外」= 全部。
- 指標：平均延遲 `lat_ms`、`p95_us`、錯誤率 `err_rate`。
- 做法：真的跑 `Pipeline`，節點與 `it_otel` 一致，`detector.ensemble` 開 `keep_members: true` 保留各節點原始票。「孤立單點」= 越線的那一分鐘前後一分鐘都沒越線。

### 4.1 (a) 多數決壓掉多少：分鐘數 vs 單點毛刺

| 指標 | 組 | 窗外警報分鐘：hourly / mad / qdrift / **聯集 → 多數決** | 降幅 | 孤立單點：hourly / mad / qdrift / **聯集 → 多數決** | 單點降幅（vs 聯集 / vs hourly / vs mad） |
| :--- | :--- | :--- | :--- | :--- | :--- |
| lat_ms | scn | 128 / 144 / 305 / **512 → 59** | **88%** | 131 / 138 / 1 / **211 → 53** | 75% / 60% / 62% |
| lat_ms | mes | 475 / 607 / 3602 / **4153 → 481** | 88% | 449 / 529 / 8 / **545 → 409** | **25%** / 9% / 23% |
| p95_us | scn | 217 / 160 / 398 / **614 → 138** | 78% | 221 / 164 / 4 / **223 → 142** | 36% / 36% / 13% |
| p95_us | mes | 763 / 536 / 1844 / **2638 → 426** | 84% | 723 / 499 / 2 / **770 → 397** | 48% / 45% / 20% |
| err_rate | scn | 528 / **0** / 3491 / **3841 → 178** | 95% | 526 / **0** / 20 / **326 → 195** | 40% / 63% / — |
| err_rate | mes | 1386 / 956 / 1212 / **2983 → 521** | 83% | 1296 / 868 / 16 / **1673 → 478** | 71% / 63% / 45% |

讀法：

- **88% 重現了，但是是分鐘數**。聯集的窗外警報有六到八成來自 quantile_drift（它一越線就是一長段，孤立單點幾乎是 0）；多數決主要壓掉的是這些長段，不是單點。
- **多數決對單點毛刺的壓制力有限**：hourly 與 mad 都是「逐點跟基準線比」，對同一個尖峰高度相關，兩票常常一起投。mes 組延遲 14 天裡多數決仍留下 409 個孤立單點（約每條序列每天 7 個）。
- 代價：scn 組延遲事故窗內，聯集抓到 219 分鐘，多數決 158 分鐘（少 28%）；跟 hourly 單獨（161）幾乎一樣。

### 4.2 (b) 有沒有「單點極端值被多數決否決」

- 「數值超過訓練期 7 天最大值、卻被否決」：lat_ms **0 筆**、p95_us **0 筆**、err_rate **2 筆**（order-svc 10/22 09:39 err=0.28、10/25 01:53 err=0.29，都是只有 hourly 投票、分數 4.3／7.2）。
- 放寬到「某節點分數 ≥ 2 倍門檻卻被否決」的孤立單點：lat_ms 0、p95_us 0、**err_rate 23 筆**，全部是 hourly 單獨投票（分數 6.1～8.4），mad 與 qdrift 沒投：

| 實體 | 時間（UTC） | 錯誤率 | 該分鐘請求數 | hourly 分數 |
| :--- | :--- | :--- | :--- | :--- |
| ord-svc | 10/31 14:21 | 9.1% | 11 | 8.39 |
| bill-svc | 11/03 12:26 | 14.3% | 21 | 7.90 |
| report-svc | 10/27 12:09 | 23.5% | 17 | 7.90 |
| ord-svc | 11/01 17:56 | 10.5% | 19 | 7.29 |
| order-svc | 10/25 01:53 | 28.9% | 38 | 7.21 |
| report-svc | 10/28 20:36 | 25.0% | 16 | 7.05 |
| …其餘 17 筆 | | 6.7%～25% | 15～49 | 6.08～6.88 |

  **這 23 筆都是小流量下的比例雜訊（1～4 筆失敗就能讓比例跳到 10%～25%），被否決是對的**，不是致命事件被吃掉。
- 「被否決的長段」：lat_ms 58 分鐘、p95_us 82 分鐘，全部是 quantile_drift 單獨投票的一長段（例如 bill-svc 11/3 16:11 之後，事故尾巴），不是單點。

### 4.3 真實資料有沒有致命樣本：沒有

| 實體 | 平均延遲最大值 | p95 最大值 | 錯誤率最大值 | 錯誤率 ≥ 80% 的分鐘 | 每分鐘請求數中位數 |
| :--- | ---: | ---: | ---: | ---: | ---: |
| cart-api | 49 ms | 86 ms | 19.4% | 0 | 35 |
| pay-api | 143 ms | 250 ms | 28.6% | 0 | 23 |
| bill-svc | 225 ms | 431 ms | 18.8% | 0 | 23 |
| inv-svc | 18 ms | 31 ms | 25.0% | 0 | 23 |
| ord-svc | 315 ms | 563 ms | 29.2% | 0 | 23 |
| mes-api | 51 ms | 120 ms | 46.9% | 0 | 141 |
| order-svc | 29 ms | 71 ms | 28.9% | 0 | 41 |
| report-svc | 597 ms | 1,430 ms | 28.3% | 0 | 22 |
| wip-worker | 51 ms | 110 ms | 23.0% | 0 | 62 |

沒有 `active_threads`、機台微電弧這類指標的真實資料（dev ES 只有 APM）。**致命情境只能用合成資料驗證**，見下一節。

### 4.4 順帶挖到的死區：MAD 在錯誤率序列上永遠投反對票

健康服務的錯誤率訓練期 63%～77% 的分鐘是 0，中位數 0、MAD 0。`scale_floor`（`base.py:305-323`）三個下限都是 0 → 退回 1.0，MAD 分數 = 0.6745 × |x| / 1 ≤ **0.67**，永遠到不了門檻 3.5。它照樣「評估過資料」、進 `detector_runs`、算一票反對。**所以在錯誤率上，多數決實際上等於「hourly 且 qdrift」**；qdrift 要滾動窗湊滿才會動，第一分鐘幾乎不會投票。這直接打穿「5xx 比例瞬間 > 80%」的需求（W7）。這不是這次雙軌的範圍，列為後續（第 8 節 D8）。

---

## 5. 合成波形（**以下全部是合成資料**）

腳本：`synth_waves.py`（scratchpad），同一條 it_otel 管線，最後幾分鐘放波形。「票外」= `vote.nodes` 只列三個統計節點。

| 波形 | rules 在票內 | rules 在票外 |
| :--- | :--- | :--- |
| W1 貼近上限（190±4，上限 200），1 點 203 | 否決（`4:need=3:1->0`），無通知 | rules 存活，**P3** |
| W2 同上，連 2 點 203 | 2 點中過 1 點，P1 | P1（統計第 2 點才跟上） |
| W3 同上，高原 10 點 203 | 10 點過 9 點，P1 | P1 |
| W4 遠離上限（50±3），1 點 210 | P0（統計同意） | P0 |
| W5 感測器壞值，1 點 9999 | **P0（假致命）** | **P0（假致命）**：統計也被騙，跟 rules 無關 |
| W6 5xx 比例（0.02±0.01），1 點 0.85 | P0 | P0 |
| **W7 健康服務（70% 分鐘錯誤率 = 0），1 點 1.00，上限 0.8** | **否決（`3->0`），無通知** | rules 存活，**P3** |
| W8 同 W7，連 5 點 1.00 | 5 點過 4 點，P0（第 1 分鐘漏） | P0 |

重點：

- **貼近上限的序列（W1）與健康服務的錯誤率（W7）是真正的漏洞**：統計偵測器覺得「這在常態範圍內」或「數學上投不了票」，只有硬上限會叫。
- 高原與連續多點（W2、W3、W8）統計會慢一兩分鐘跟上，**多數決不是完全漏，是晚、而且嚴重度不由硬上限決定**。
- W5：壞值 9999 不管有沒有雙軌都會變 P0。**假致命的問題現在就存在**，雙軌只是讓它更明顯，見坑點 7.4。

---

## 6. 設計：兩條路

共同原則（兩案都遵守）：流程只在 YAML 宣告；演算法不判斷「要不要跑後面」；事件等級仍只有 `ANOMALY`／`WARNING`；P0 由計分層把分數墊高、`alert.notify_payload` 照既有門檻換算；`AnomalyEvent` 不改、不加欄位（用既有的 `detector`／`detector_node` 血緣辨識）；所有新參數預設關閉。

### 方案 A：ensemble 層宣告「否決不了的一票」

**做法**：`detector.ensemble` 新增參數 `bypass: [節點名稱]`。列進去的節點：

- 不算參與者、不進分母（分母只看統計節點，**既有判決不變**）；
- 它的事件原樣保留（跟現在「非參與者」的處理一樣，`ensemble.py:79`）。

**改的檔案**

| 檔案 | 改動 |
| :--- | :--- |
| `aiops/stages/ensemble/ensemble.py` | `__init__` 收 `bypass`；`execute` 的 participants 排除 `bypass`；audit 記 `bypass=<節點>` |
| `aiops/engine/pipeline.py` | 新增通用的載入期檢查掛點：算子可實作 `check_workflow(nodes)`（本 workflow 全部節點的名稱、算子名稱、params），`Pipeline.__init__` 對每個節點呼叫（引擎不認得 ensemble，只呼叫介面）。ensemble 用它檢查 `bypass`、`nodes` 的名稱都是本 workflow 的偵測節點（**順便補上 `nodes` 拼錯不報錯的現有缺口**） |
| `aiops/pipelines/detection/it_otel.yaml`、`factory.yaml` | 加 `- {name: fatal, operator: detector.rules, params: {require_rule: false}}`（放在 `detect_qdrift` 後、`vote` 前），`vote` 改成 `params: {strategy: majority, bypass: [fatal]}`；硬上限寫在 target `overrides.fatal` |

**能做到的**：不被否決、不改分母。**做不到的**（第 3 節實跑已證明）：P3、被冷卻吃、被同根因吃、不升級。另外 2.5 節的調參漏洞（規則升級器的統計規則漏進 `fatal` 節點）**沒解**。要補齊就得再改計分與抑制，那就是方案 B。

**回滾**：YAML 拿掉 `fatal` 節點與 `bypass`；程式碼的預設值是空清單，對既有管線零影響。

### 方案 B（建議）：獨立的硬上限算子＋貫穿計分與抑制的 fatal 標記

**做法**：四個零件，各自用 YAML 宣告。

1. **`detector.hard_limit`**（新算子，`aiops/stages/detectors/hard_limit.py`）
   - 跟 `detector.rules` 同樣的越線判斷，但**算子名稱獨立**：調參以算子名稱查（`hooks.py:134`），規則升級器產生的 `detector.rules` 提案漏不進來。
   - `tunable = {}`、不接受任何調參提案（提案建立時就拒絕：硬上限是 SLA／物理限制，不是學出來的）；`requires_training = False`；沒有上下限就棄權（`can_evaluate` 回 False）。
   - 參數：`upper`、`lower`、`consecutive`（預設 1）、`plausible_max`／`plausible_min`（選填：超出物理可能的值當成壞資料，不判致命，記 `hard_limit:<節點>:implausible:<值>` 到 audit 與 `ctx.errors`，見坑點 7.4）。
   - 分數：沿用 `1 + 超出比例`。**嚴重度不在這裡決定**。
   - 共用的越線邏輯（連續段長度）抽到 `stages/detectors/base.py` 的純函式 `limit_breach(values, upper, lower, consecutive)`，`rules.py` 與 `hard_limit.py` 都呼叫它（偵測器之間不准互相 import，`tests/test_architecture.py:130`）。`rules.py` 行為一行不變。
2. **ensemble `bypass`**：同方案 A 的第一點。另外加一條載入期檢查：**workflow 裡有 `detector.hard_limit` 節點、ensemble 沒有把它列進 `bypass` 也沒用 `nodes` 排除它 → `WorkflowError`**，防止有人加了硬上限卻默默把多數決門檻從 2 票拉到 3 票（S6 的回歸）。判斷「這個節點不投票」用算子類別屬性 `votes: ClassVar[bool]`（`DetectorOperator` 預設 True，`hard_limit` 為 False），引擎不認得具體算子。
3. **`alert.fatal_floor`**（新計分算子，`aiops/stages/scoring/score.py`）
   - params：`nodes: [fatal]`（必填，節點名稱）、`severity: P0`（預設）。載入期由 `check_workflow` 讀同一條 workflow 裡 `alert.notify_payload` 的 `severity_thresholds`，把 `severity` 換成分數下限（現行 P0 = 10）；workflow 沒有 payload 節點或沒有這個等級 → `WorkflowError`。這樣有人調高 P0 門檻時，致命事件不會悄悄掉成 P1。
   - 事件群裡只要有一筆事件的 `detector_node` 在 `nodes` 裡：`inc.score = max(inc.score, 該等級的分數下限)`，並在 `inc.tags` 加 `fatal:<節點名>`（tags 跟著事件群存檔，`payload.py:78-84` 會把它寫進卡片描述）。
   - 位置：`score_golden`／`score_p0` 之後、`absorb_open` 之前（順序鐵律：收斂 → 計分 → absorb）。這樣 `absorb_open` 的 `max(cur.score, inc.score)`（`absorb.py:54`）會把進行中的事件群墊到 10，`alert.notify_payload` 照既有的「只升不降」升到 P0（`payload.py:138`），`alert.heartbeat` 照既有流程先關舊標籤再送新的。**不用改 absorb、payload、heartbeat。**
   - 載入期檢查：`nodes` 必須是本 workflow 的 `detector.hard_limit` 節點（同樣走 `check_workflow` 掛點）。
4. **抑制認得 `fatal` 標記**
   - `alert.suppress_dedup` 新增 `escalate_tags: [fatal]`（預設空 = 行為不變）：帶這些 tag 的事件群，**只有上一次通知已經是 P0 才吃掉**（防同一個致命狀態每輪刷屏），否則照發。`RecentNotification.severity` 已經有上次的等級（`context.py:138`），不用改 context。
   - `alert.suppress_same_root_cause` 新增 `exempt_tags: [fatal]`（預設空）：致命事件群不被當成「被下游拖累的症狀」。
   - `alert.suppress_window`：**預設照舊會被靜音**；要不要讓致命事件打穿維護窗是決策 D3，若要就加 `pass_tags: {fatal: [mute, exclusion]}` 這類「哪些 kind 擋不住 fatal」的宣告。

**YAML 長相（`it_otel.yaml`，只列有變的行；`factory.yaml` 同理，沒有 `suppress_root_cause`）**

```yaml
  - name: detect_qdrift
    operator: detector.quantile_drift
  # 致命硬上限：單點踩線就成立，不進多數決分母；上限寫在 target overrides.fatal（沒寫的 target 棄權）
  - name: fatal
    operator: detector.hard_limit
  - name: vote
    operator: detector.ensemble
    params: {strategy: majority, bypass: [fatal]}
  ...
  - name: score_p0
    operator: alert.p0_boost
    params: {entities: []}
  - name: score_fatal
    operator: alert.fatal_floor
    params: {nodes: [fatal], severity: P0}
  - name: absorb_open
  ...
  - name: suppress_root_cause
    operator: alert.suppress_same_root_cause
    params: {exempt_tags: [fatal]}
  - name: suppress_dedup
    operator: alert.suppress_dedup
    params: {cooldown_minutes: 30, escalate_tags: [fatal]}

targets:
  - {source_type: prometheus, query: 'db_active_threads{db="orders"}', entity: orders-db,
     attrs: {dept: dba, signal: saturation}, overrides: {fatal: {upper: 200}}}
```

注意 `it_otel.yaml` 正被 `feat/mq-async-rca` 加節點（`alert.messaging_link`，在收斂層），兩邊改的是不同行，合併時照順序放就好；這份設計稿不動那個分支的檔案。

**改的檔案總表**

| 檔案 | 改動 | 既有行為 |
| :--- | :--- | :--- |
| `aiops/stages/detectors/hard_limit.py`（新） | `detector.hard_limit` | 新算子，沒人用就不存在 |
| `aiops/stages/detectors/base.py` | 抽出 `limit_breach` 純函式；`DetectorOperator.votes = True` | 不變 |
| `aiops/stages/detectors/rules.py` | 改呼叫 `limit_breach` | 不變（既有 rules 測試一行不改要全綠） |
| `aiops/stages/ensemble/ensemble.py` | `bypass` 參數＋`check_workflow` | 預設空清單，不變 |
| `aiops/engine/pipeline.py` | `Pipeline.__init__` 呼叫各算子的 `check_workflow(nodes)`（nodes = 本 workflow 全部節點的 名稱／算子名稱／params／是否投票） | 沒實作的算子不受影響 |
| `aiops/stages/scoring/score.py` | `alert.fatal_floor` | 新算子 |
| `aiops/stages/suppression/suppress.py` | `escalate_tags`、`exempt_tags`（預設空） | 不變 |
| `aiops/control_plane/learning/tuning.py`（或提案建立處） | `detector.hard_limit` 的調參提案一律拒絕 | 不變 |
| `aiops/pipelines/detection/it_otel.yaml`、`factory.yaml` | 加 `fatal`、`score_fatal`，`vote` 加 `bypass`，抑制加 tag 參數 | **只有 target 寫了 `overrides.fatal` 才有作用**；範例 targets 不寫，等同關閉 |
| `CLAUDE.md` 第 2 節 | 補「硬上限算子不投票、`fatal_floor` 位置」兩條 | 文件 |

**沒有** schema 變更、migration、`engine/context.py` 變更、`engine/types.py` 變更。

**回滾**：YAML 拿掉 `fatal`、`score_fatal` 節點與新參數。程式碼留著也無害（全部預設關）。

### A vs B 比較

| | A | B |
| :--- | :--- | :--- |
| 單點不被否決 | ✅ | ✅ |
| 不改既有分母 | ✅ | ✅（＋載入期強制） |
| 嚴重度 P0 | ❌ P3 | ✅ 計分墊底 |
| 進行中事件群升級 | ❌ | ✅（沿用既有升級路徑） |
| 不被冷卻吃 | ❌ | ✅（上次已 P0 才吃） |
| 不被同根因吃 | ❌ | ✅ |
| 規則升級器的統計規則不會漏進來 | ❌ | ✅ 獨立算子名稱 |
| 改動量 | 2 個 .py | 6 個 .py＋1 個新檔 |

---

## 7. 坑點逐條

### 7.1 硬上限的值從哪來、誰維護

- **寫在 target 的 `overrides.fatal`**，不寫在節點 `params`：同一條管線 50 個服務的上限各不相同，節點層只放共用行為（`consecutive` 預設、`plausible_*`）。`check_override` 在 `Pipeline.__init__` 就驗參數名與型別。
- 不放 `attrs`：`attrs` 是給收斂、RCA、路由讀的描述資訊，不參與偵測參數合併；放那裡等於另開一條參數來源，違反「合併邏輯只有一份（`layer_params`）」。
- **誰維護**：服務 owner（SLA）或製程工程師（機台規格），走 Git PR 改 YAML。**不走後台調參、不走規則升級器**（B 案在提案建立時就拒絕）。理由：硬上限不該被「誤報太多就自動調高」的閉環推走。
- 坑：YAML 寫錯單位（ms 寫成 s）會變成每分鐘都 P0 或永遠不叫。驗收時要求每條硬上限附一條真實資料回放：過去 7 天會叫幾次（第 9 節）。

### 7.2 致命事件的嚴重度怎麼表達

- 事件等級不加新值，仍是 `ANOMALY`。**P0 落在計分層**（`alert.fatal_floor` 把分數墊到 P0 門檻），由 `alert.notify_payload` 照既有門檻換算 → 通知標籤 `severity=P0`。Alertmanager 依 `severity` 路由，接收端不用改。
- 為什麼不加 `fatal="true"` 通知標籤：Alertmanager 用整組 labels 辨識告警，進行中事件群中途變成 fatal 會多一次「關舊開新」。標籤只用既有的 `severity`；fatal 資訊放 tags（會進卡片 description）。要不要另外加標籤是決策 D2。
- 坑：如果 `fatal_floor` 寫死分數 10，之後有人把 `severity_thresholds.P0` 調到 12，致命事件就悄悄變成 P1。所以 `fatal_floor` 收的是**等級名稱**（`severity: P0`），載入期從同一條 workflow 的 payload 節點換算成分數下限，對不上就 `WorkflowError`。

### 7.3 冷卻與靜音窗對致命事件適不適用

- **冷卻**：不該吃第一次致命，但要防刷屏。B 案：上一次通知已經是 P0 才吃（同一個致命狀態持續中，本來就由 `absorb_open`＋`heartbeat` 續命，不會開新卡）。
- **維護窗（`maintenance`）**：維護中重啟 DB、拔機台，硬上限一定會叫；照樣吵等於教人把告警靜音整個關掉。**建議維持靜音**（預設行為），但這是取捨：維護期間真的燒起來就沒人知道。
- **`mute`／`exclusion`**：mute 是有人主動說「這個先別吵」；exclusion 是事後標「這段不算」。建議同維護窗，一律尊重人工靜音。決策 D3。

### 7.4 假致命：感測器壞值、單位錯、重啟歸零

- 證據：W5（9999）不管有沒有雙軌都已經是 P0，**統計偵測器一樣被騙**。所以「雙軌會帶來假致命」只說對一半：它帶來的是**低於統計門檻的假致命**（例如 0.81 的比例是因為只有 1 筆請求）。
- 三個工具，按「單點就要叫」的衝突程度排序：
  1. `plausible_max`／`plausible_min`（不衝突）：物理上不可能的值（比例 > 1、溫度 9999）直接當壞資料，不判致命，記錯誤。
  2. 資料源層的流量門檻（不衝突，**比例型指標必備**）：比例在 query 裡就把「請求數 < N」的分鐘變成無值，例如 PromQL `rate(5xx[1m]) / rate(all[1m]) and on() rate(all[1m]) > 0.3`。真實資料每分鐘請求數中位數只有 22～141，小流量下一筆失敗就是 5%～10%；第 4.2 節那 23 筆就是這種。
  3. `consecutive: 2`（衝突）：擋得掉重啟歸零這種「一點 0」，代價是致命事件晚一個點（1 分鐘）。**建議：上限預設 1、下限預設 2**（下限踩線最常見的原因是重啟／漏報，上限踩線比較少是假的）。決策 D4。
- 單位錯沒有演算法解，只能靠第 7.1 節的「上線前回放」。

### 7.5 進行中事件群的吸收與升級

- B 案不改 `absorb_open`：`fatal_floor` 在它前面，把新事件群墊到 10，`absorb_open` 取最大值，`notify_payload` 只升不降升到 P0，`heartbeat` 先對 P2 舊標籤送 `endsAt` 再送 P0（S4 情境改完後要變成「P2 → P0，`[Escalated from P2]`」）。
- 坑：吸收後的 tags 是聯集（`absorb.py:53`），`fatal:` tag 會跟著進行中事件群一直到結案。結案前同實體再踩線不會開新卡（這是想要的）。
- 坑：致命狀態結束、統計異常仍在延燒時，嚴重度**不會降回 P2**（只升不降）。這是既有設計，不改。

### 7.6 `ctx.detector_runs` 與 ensemble 分母

- `hard_limit` 照樣進 `detector_runs`（它真的評估過資料，RCA 與除錯要看得到），但 ensemble 用 `bypass` 把它排除在參與者之外 → 分母只算統計節點，**既有判決逐點不變**。
- 載入期強制：有 `votes = False` 的節點卻沒被 `bypass`／`nodes` 排除 → `WorkflowError`（防 S6 那種「默默把門檻從 2 拉到 3」）。
- `require_rule` 這種「沒規則就棄權」：`hard_limit` 沒寫上限的 target 用 `can_evaluate` 回 False，不進 `detector_runs`，跟現在 rules 一樣。

### 7.7 「直接送 RCA」

RCA 目前是手動觸發（`POST` `rca_report`，`engine/requests.py:46`）；自動觸發是 `feat/mq-async-rca` 正在做的事。這份設計只保證致命事件群帶 `fatal:<節點>` tag，**MQ RCA 那邊可以把它當觸發條件**，不在這裡實作，避免兩邊改同一批檔案。

---

## 8. 待決策

| # | 問題 | 建議 | 代價 |
| :--- | :--- | :--- | :--- |
| D1 | 走 A 還是 B | **B** | 多改 4 個 .py；A 實測只解決「不被否決」，其他三項都沒過 |
| D2 | P0 怎麼表達：只墊分數，還是另加通知標籤 `fatal="true"` | **只墊分數＋tags**；`fatal_floor` 收 `severity: P0`、載入期跟 payload 門檻對齊 | 不加標籤，Alertmanager 無法把「硬上限 P0」跟「統計 P0」分流；要分流就加標籤，代價是中途變 fatal 會多一次關舊開新 |
| D3 | 維護窗／mute／exclusion 擋不擋致命事件 | **都擋**（維持現狀） | 維護期間真的燒起來沒人知道；要打穿就加 `pass_tags`，代價是維護期間告警噪音 |
| D4 | `consecutive` 預設值 | **上限 1、下限 2** | 下限致命晚 1 分鐘；要更嚴格就逐 target 覆寫 |
| D5 | 比例型硬上限（5xx > 80%）的流量門檻放哪 | **放資料源 query**（PromQL `and on() …`／ES 的 `min_doc_count`），平台不加新參數 | 每條比例 target 的 query 要寫兩段；平台側加 `min_count` 就得讓 fetch 多帶一欄，改動大 |
| D6 | 誰能改硬上限 | **只能 Git PR 改 YAML**；後台調參與規則升級器一律拒絕 `detector.hard_limit` | 改上限要走 PR，緊急時慢 |
| D7 | 第一批硬上限給哪些 target | 先只上「有真實資料可回放」的：IT 側選 1～2 條錯誤率／延遲；工廠機台**等有真實資料**再上 | 工廠的微電弧、`active_threads` 目前沒有真實資料，上線等於盲設 |
| D8 | MAD 在 0 為主的比例序列上永遠投反對票（4.4 節） | **另開一題**，不放這次：可能的修法是 `can_evaluate` 在中心與 MAD 都是 0 的比例序列上棄權 | 不修的話錯誤率的多數決一直是「hourly 且 qdrift」；修了會改變既有錯誤率序列的判決，要重新回放 |
| D9 | `it_otel.yaml:12` 註解「88% 的單點誤報」 | **改成「88% 的窗外警報分鐘數；單點毛刺 25%～75%」**，`PRODUCTION_READINESS.md:16`、`HARDENING_DESIGN.md:11` 同步 | 純文件 |

---

## 9. 驗收方式

### 9.1 既有測試必須原樣通過（一行斷言都不改）

- `tests/test_target_detectors.py`（ensemble 分母、棄權、tie_breaker、白名單）、`tests/test_detectors.py`、`tests/test_algorithms.py`（rules 與各偵測器）、`tests/test_stability_guards.py`、`tests/test_hardening.py`、`tests/test_architecture.py`（含偵測器互不 import）。
- `tests/test_claude_md_examples.py`（範例一要跟 `mad.py` 逐字相同；這次不動 mad）。

### 9.2 要新增的測試

| 測試 | 斷言 |
| :--- | :--- |
| 單點硬上限不被否決 | S2 波形＋`bypass: [fatal]` → 1 筆事件存活、嚴重度 P0 |
| 分母不變 | 同一份資料，有／沒有 `fatal` 節點，`vote` 的 audit `voters=3:need=2` 與存活事件**完全相同**（S6 波形） |
| 載入期：`hard_limit` 沒被 bypass | `WorkflowError` |
| 載入期：`bypass`／`nodes`／`fatal_floor.nodes` 寫錯名稱 | `WorkflowError` |
| 進行中事件群升級 | S4 → 進行中 P2 升 P0，heartbeat 送出舊標籤 `endsAt`＋新標籤，標題 `[Escalated from P2]` |
| 冷卻 | 上次 P3 → 致命照發；上次 P0 → 吃掉 |
| 同根因 | 帶 `fatal` 的上游不被抑制 |
| 維護窗 | 依 D3 決議 |
| 規則升級器不漏進來 | 核准一筆 `detector.rules` 調參後，`fatal` 節點的參數不變；建立 `detector.hard_limit` 調參提案被拒 |
| `plausible_max` | 9999 不判致命、`ctx.errors` 有記錄 |
| 長駐不漏狀態 | 兩輪連跑，第二輪沒有致命資料就沒有 `fatal:` tag（tag 掛在每輪新建的事件群上） |

### 9.3 死區（依第 100 節用 `xfail(strict=True)` 留存）

- **兩輪之間的踩線**：硬上限只看抓得到的點；資料源 step 60 秒、真實尖峰只有 10 秒，分鐘平均根本到不了上限。這是取樣問題，演算法做不到。
- **貼近上限的雜訊抖動**（例如 199↔201 來回）：`consecutive: 1` 會每輪踩線，靠 absorb＋冷卻收斂，不是演算法能判斷真假。
- **比例型指標沒有流量門檻**（D5 不做時）：1 筆請求失敗 = 100%，會叫 P0。
- **D8 不修時**：健康服務錯誤率單分鐘 100%，**多數決**（不含硬上限）仍抓不到——W7 留成 xfail，證明「沒設硬上限的序列就沒有這道防線」。

### 9.4 真實資料回放要比的數字

1. **既有判決不變**：9 條真實序列 × 3 個指標，加 `fatal`（上限設在訓練期 p99.9 × 1.5，確保會踩到幾次）前後，`vote` 的逐分鐘輸出差異 = **0**。
2. **硬上限會叫幾次**：每條擬上線的硬上限，過去 7 天（真實資料）會產生幾張 P0 卡；超過每週 1 張就退回重訂。
3. **致命事件端到端**：合成 W1／W7 灌進 dev ES（`scripts/synth_generator`，**平台本身不寫 ES**，用既有的產生器），跑 it_otel 管線到 Alertmanager，量從踩線到收到 P0 的延遲（分鐘）。
4. **第 4.1 節的表重跑一次**，數字要一樣（證明 bypass 沒改到多數決）。

---

## 附錄：本輪腳本與原始輸出（scratchpad，未進 repo）

- `prove_bypass.py`：第 3 節 S1～S6。
- `replay_dual.py`：第 4 節回放（參數 `lat_ms`／`err_rate`／`p95_us`），輸出 `replay_<指標>.json`、彙總 `summary.txt`。
- `synth_waves.py`：第 5 節 W1～W8，輸出 `synth.txt`。
- 資料：先前 session 從 dev ES 匯出的 `series/*.csv`（真實 APM 分鐘序列）。
