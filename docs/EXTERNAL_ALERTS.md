# 各團隊的告警怎麼送進平台（Grafana webhook）

## 權責：誰設規則誰負責

- **什麼算異常，由設告警規則的團隊決定。** 規則放在各團隊自己的監控系統／Grafana，門檻、靜默、誰會被叫都是他們的設定。
  平台的 `aiops-grafana` 只負責顯示，不放任何告警規則（CLAUDE.md 第 5 節）。
- **平台只做兩件事**：把推進來的告警用關係圖（ServiceGraph）收斂成一張事故卡、出 RCA 建議。
  RCA 報告帶結論等級（有直接證據／只知道症狀位置／拓撲推測／無法判定），**是建議，不是定論**。
- **沒設規則、沒推告警，平台就不會產生任何東西。** 告警太多、太少、門檻設錯，是規則的問題，不是平台判錯。

## 團隊要在自己的 Grafana 做的事

1. **建一個 Webhook 通知管道（Contact point）**
   - URL：`https://<平台位址>/api/v1/ingestion/grafana-alerts?pipeline=<偵測管線名稱>&source=<團隊代號>`
     - `pipeline`：告警要進哪條偵測管線（`aiops/pipelines/detection/` 底下的檔名，例如 `it_otel`）。寫錯會回 404，
       你的 Grafana 會顯示這個通知管道送達失敗。
     - `source`：團隊代號（英數、`_.-`），同一個團隊固定用同一個。平台用「代號 + 告警指紋」去重。
   - HTTP 方法：POST。
   - Authorization：Scheme 填 `Bearer`，Credentials 填平台給你的 token（平台的 `AIOPS_API_TOKEN` 可以放多組，用逗號分隔，每個團隊一組）。
   - 內容格式：用 Grafana 預設的 webhook 格式，不要改成自訂模板。
2. **通知政策（Notification policy）把要送給平台的告警路由到這個通知管道。**
   - 建議**只送給平台、不直接通知人**：平台收斂後送一張事故卡到 Alertmanager，人就不會同時收到原始告警與事故卡兩份。
   - **Repeat interval 設成 15 分鐘以下**（不能超過那條偵測管線 `alert.heartbeat` 的 `resolve_after_minutes`，內建管線都是 15）。
     平台的事故卡是靠「一直有新的告警點進來」維持開著；Grafana 預設 4 小時才重送一次，平台會在告警還沒結束時就把事故卡結案。
3. **告警規則的標籤照下面的規範加。**

## 標籤規範

| 標籤 | 必填 | 規範 | 不符合時 |
| :-- | :-- | :-- | :-- |
| `service` | **必填** | 服務名稱，**跟 trace 的 `service.name` 一字不差**（也就是平台關係圖裡的名字）。也可以用 `service_name`。不要放主機名、Pod 名、環境名 | 拒收這筆，回應的 `rejected` 寫原因；平台不拿別的標籤猜 |
| `alertname` | Grafana 自動帶 | 規則名稱，當作這筆異常的指標名 | 拒收 |
| `severity` | 建議 | `critical`／`p0` → 10、`high`／`error`／`p1` → 6、`warning`／`p2` → 3、`info`／`p3` → 1（不分大小寫） | 用預設分數 3（P2），紀錄上標 `score_source=default` |
| 其他標籤 | 選填 | 原樣存在異常的 `labels` 裡，RCA 與事故卡不使用 | — |

分數對到偵測管線 `alert.notify_payload` 的嚴重度門檻（P0 ≥ 10、P1 ≥ 6、P2 ≥ 3）。對照表與實體標籤的順序在
`aiops/pipelines/control/admin_ingest_grafana_alerts.yaml`，改那裡就好，不用改程式。

為什麼 `service` 要跟 trace 一致：收斂靠關係圖把「同一條呼叫鏈上的告警」併成一件事，RCA 也靠它找上下游。名字對不上，
告警還是會進來、會變成事故卡，但就只能各自一張、RCA 沒有上下游可看。

## 心跳告警（建議每個團隊都設）

告警推送天生是稀疏的：沒有東西壞的時候，本來就一筆都不會推。所以只看「有沒有告警進來」，**分不出「對方沒事」跟「對方的告警管道斷了」**。

做法：在你的 Grafana 設一條**一直觸發**的告警規則（例如條件寫成 `1 > 0`），標籤加 `aiops_heartbeat=true`，路由到同一個 webhook 通知管道，
repeat interval 5～15 分鐘。平台收到後：

- 存成心跳（`status = heartbeat`），**不進任何管線、不開事故卡**，不需要 `service` 標籤。
- 平台看門狗（`aiops/pipelines/watchdog/platform_watchdog.yaml`）數每個來源的心跳；連續 50 分鐘一筆都沒有（容許連續漏掉 2 筆 15 分鐘一筆的心跳，依據見 `docs/WATCHDOG.md`），平台自己會發一張卡
  （「team-a 的告警管道斷了」），而不是默默以為你那邊沒事。要讓看門狗看你的來源，平台那邊要在看門狗 YAML 的 targets 加一行
  （`alerts/<你的 source>/heartbeats`，寫法在 YAML 註解裡）。
- 心跳間隔不要超過 15 分鐘：門檻是照 15 分鐘算的，間隔更長會誤報。間隔更短、想更快被發現的團隊，平台那邊可以用同一個式子調小（沒有驗證過）。

沒有設心跳的來源，看門狗不會替它判斷任何事（見 `docs/WATCHDOG.md`）。

## 平台怎麼處理一筆告警

| Grafana 送來的 | 平台做什麼 |
| :-- | :-- |
| `status: firing` | 轉成一筆外部異常：實體 = `service`、指標 = `alertname`、分數照上表、時間 = **平台收到的那一分鐘** |
| 同一筆告警在同一分鐘內重送 | 只收一次 |
| 同一筆告警之後重送（repeat interval） | 記成新的一點，事故卡繼續開著 |
| `status: resolved` | 不轉。事故卡照平台的規則結案：安靜滿 `resolve_after_minutes` |
| 標籤 `aiops_heartbeat=true` | 存成心跳，只給平台看門狗數，不開事故卡（見上一節） |
| `annotations`（summary、description 這類自由文字） | 不收。平台只存結構化欄位 |
| 不認得的欄位（新版 Grafana 多的） | 忽略，不影響 |

下一輪那條偵測管線跑的時候，這些外部異常跟平常一樣走收斂、抑制、事故卡、心跳；事故卡可以按 RCA 出報告。

回應：大部分情況回 200，`result` 裡有 `accepted`（收了幾筆）、`duplicates`、`resolved_ignored`、`rejected`（每筆拒收的原因）。
部分告警格式不對時**仍回 200**：Grafana 收到非 2xx 會整批重送，一筆壞的不該讓好的一直重來。拒收也會寫進平台的 log。

## 已知限制

1. **Grafana 的畫面看不到 `rejected`。** 只有回 4xx（pipeline 寫錯）時你的通知管道會顯示失敗；標籤不符被拒收時回的是 200，
   要看平台的 log 或回應內容。之後會補一個查詢。
2. **時間是平台收到的時間，不是告警真正開始的時間。** 相差 Grafana 的 group wait（預設 30 秒）加網路延遲。告警的 `startsAt` 存在異常的 `attrs` 裡。
3. **事故卡的結案跟你的告警結案不同步。** 平台不看 `resolved`；你的告警結束後，事故卡要再安靜 `resolve_after_minutes` 才結案。
   反過來，repeat interval 設太長，事故卡會比告警早結案（見上面第 2 步）。
4. **token 是平台共用的那一套。** 拿到 token 就能呼叫平台所有 API，不只是這個 webhook。每個團隊給一組、可以單獨換掉，但權限不分。
5. **Grafana 只有在 alerts 陣列被截斷時才會少送**（`truncatedAlerts` 大於 0）：平台不補，照收到的處理。
6. **一次最多 5000 筆告警。** 超過整批回 400。

## 驗證狀態

- 測試用的是 Grafana 13.0.7 實際送出的內容（`tests/fixtures/grafana_webhook/`，怎麼抓的寫在那裡）：firing、resolved、沒有 `service` 標籤各一份。
- `tests/test_grafana_alerts.py`：轉換、去重、分數對照、拒收、API 驗證，以及端到端——三個有呼叫關係的服務各推一筆告警，
  下一輪收斂成一張 P0 事故卡、送出通知，RCA 報告標成拓撲推測；同一筆告警重送不會開新卡；沒有告警時平台不送任何告警。
- 沒驗過的：真的從團隊的 Grafana 打到真的平台（要平台部署到叢集）、HTTPS、大量告警同時推送時的效能、Grafana 13.0.7 以外的版本。
