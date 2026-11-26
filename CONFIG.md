# aiops-lite 設定參考

所有設定都在一份 `config.yaml`（範本：`config.example.yaml`）。本文件分兩部分：

- **A. 可以在 config.yaml 調的項目**：全部選填，沒寫就用預設值（預設值就是原本寫死在程式裡的數字）。
- **B. 留在程式裡的內部常數**：改了會破壞儀表板、資料庫或對外格式，所以不開放設定。

必填只有三個：`grafana.url`、`grafana.token`、`webhook.token`。鍵名打錯、型別不對、超出範圍，啟動時直接報錯。
Helm 部署時，常用設定是 `values.yaml` 上的欄位；下面標「進階」的項目寫在 `values.yaml` 的 `extraConfig`（結構跟 config.yaml 一樣）。

## A. 可以在 config.yaml 調的項目

### 常用設定

| 鍵 | 預設值 | 用途 |
| --- | --- | --- |
| `grafana.url`、`grafana.token` | 必填 | 公司 Grafana 的網址與 Editor 服務帳號 Token |
| `webhook.token` | 必填 | Grafana Contact Point 帶的 Bearer 密鑰 |
| `webhook.port` | 8080 | 服務監聽的 port |
| `database.url` | `sqlite:///aiops-lite.db` | 平台自己的資料庫；正式環境用 `postgresql+psycopg://…` |
| `llm.url`、`llm.api_key`、`llm.model` | 空 | OpenAI 相容端點；url 與 model 沒設 = 不分析 |
| `llm.temperature` | 不送 | 有寫才送給模型（0～2）；有些模型只接受預設值 |
| `llm.retry_initial_seconds` | 60 | LLM 分析失敗後第一次等多久再試 |
| `llm.retry_max_seconds` | 1800 | 退避上限（每失敗一次加倍，成功歸零） |
| `logs.type` | `victorialogs` | 日誌存在哪一種：`victorialogs` 或 `elasticsearch`。都只經 Grafana 的資料源代理查 |
| `logs.datasource_uid` | 空 | Grafana 上那個資料源的 uid（VictoriaLogs 要先在 Grafana 裝外掛 `victoriametrics-logs-datasource`）；空 = 沒有日誌證據 |
| `logs.extra_filter` | 空 | **只有 victorialogs**。加在每一條日誌查詢最前面的 LogsQL 條件，用來只看一部分日誌，例如 `{namespace="factory"}`。VictoriaLogs 沒有索引，要限縮範圍就用這個；空 = 看這個資料源的全部 |
| `logs.index` | `*` | **只有 elasticsearch**。要查的索引，例如 `factory-logs-*` |
| `logs.id_fields` | `[eqp_id, lot_id, carrier_id]` | 錯誤日誌裡要統計出現次數的識別碼欄位 |
| `logs.fields` | `service: service.name`、`level: log.level` | 日誌的欄位名稱。只有 `level` 可以設成 `""`（見下面「日誌沒有等級欄位」）。elasticsearch 另外可以設 `time`（預設 `@timestamp`）、`message`（預設 `message`）；VictoriaLogs 的時間與訊息固定是 `_time`、`_msg`，不能設（寫入時用 `_time_field`、`_msg_field` 對應） |
| `traces.type` | `victoriatraces` | Trace 存在哪一種：`victoriatraces` 或 `tempo`。跟 `logs.type` 各選各的 |
| `traces.datasource_uid` | 空 | victoriatraces：Grafana 上 Jaeger 型別資料源的 uid（URL 指到 VictoriaTraces 的 `/select/jaeger`）；tempo：Tempo 資料源的 uid。空 = 沒有呼叫鏈證據 |
| `outputs.incident_api.url`、`.token` | 空 | 事故 API 出口；url 空 = 佔位（見下面「註記以外的出口」） |
| `outputs.remediation.url`、`.token`、`.enabled` | 空、空、`false` | 自動修復出口；沒有 url 或 `enabled: false` = 佔位 |
| `timezone` | `Asia/Taipei` | 註記裡顯示的時間 |
| `log_level` | `INFO` | `DEBUG` 會記下送給 LLM 的內容與原始回應 |
| `debounce_seconds` | 60 | 收到第一個 Webhook 後等多久才開始分析 |
| `followup_seconds` | 60 | 有進行中事故時，沒有新 Webhook 也每隔多久再跑一輪 |
| `close_after_seconds` | 300 | 成員告警全部 resolved 之後，再安靜多久才結案 |
| `edges` | 空 | 系統層級的上下游關係 `[上游, 下游]` |

設定跟來源對不上（例如 `logs.type: victorialogs` 卻寫了 `logs.index`，或 elasticsearch 寫了 `extra_filter`）啟動時直接報錯。
更早的 `es`、`tempo` 兩個區段已經改名：設定檔裡還留著的話，啟動時報錯並指出要改成 `logs`（加上 `type: elasticsearch`）、
`traces`（加上 `type: tempo`）；`log_index` 改叫 `index`、`log_fields` 改叫 `fields`。
仍然沒有的設定：`trace_index`、`trace_fields`、`error_statuses`、`llm.max_trace_samples`
（「日誌資料庫裡的 Trace 錯誤片段」那包證據沒有回來，Trace 只有呼叫鏈）。

兩種來源回的資料由程式轉成同一種形狀（日誌是一層的字典，時間在 `_time`、訊息在 `_msg`；Trace 一律是 Jaeger 的形狀，
Tempo 的 OTLP 由程式轉），分析的程式不分來源。測試把同一批資料寫進兩邊，比對整包證據一樣（`tests/test_backends.py`）。

### 進階調整（原本寫死在程式裡，2026-11-21 起可調）

| # | 鍵 | 預設值 | 範圍 | 用途／什麼時候要調 |
| --- | --- | --- | --- | --- |
| 1 | `llm.timeout_seconds` | 60 | 1～3600 | 等 LLM 回應的逾時。推理型模型比較慢、常逾時就調大；`healthcheck.stuck_seconds` 要跟著調大 |
| 2 | `logs.error_levels` | ERROR、error、Error、FATAL、fatal、CRITICAL、critical | 非空清單 | 日誌等級是這些值才算錯誤日誌。公司的日誌寫 `E`、`SEVERE` 之類就要改（大小寫不同要分開列） |
| 3 | （已移除） | — | — | 原本是 `es.error_statuses`，隨「日誌資料庫裡的 Trace 錯誤片段」那包證據一起拿掉，沒有回來。編號留著，免得後面的項目跟舊文件對不上 |
| 4 | `logs.max_hits` | 100 | 1～10000 | 錯誤日誌最多撈幾筆（最新的在前）；識別碼的出現次數只統計這幾筆 |
| 5 | `logs.window_padding_minutes` | 10 | 0～1440 | 事故開始時間往前多看幾分鐘（錯誤日誌常比告警早出現） |
| 6 | `llm.max_log_samples` | 20 | 0 以上 | 給 LLM 看幾筆錯誤日誌樣本（控制 prompt 長度） |
| 7 | （已移除） | — | — | 原本是 `llm.max_trace_samples`，同第 3 項 |
| 8 | `llm.max_message_chars` | 300 | 20 以上 | 單筆日誌訊息截斷長度 |
| 9 | `llm.top_ids` | 10 | 1 以上 | 每種識別碼只列出現次數前幾名 |
| 10 | `llm.max_summary_chars` | 300 | 20 以上 | LLM 摘要超過就截斷（註記第 2 行） |
| 11 | `llm.system_prompt_file` | 空（用內建提示詞） | 檔案路徑 | 自訂系統提示詞。相對路徑相對於設定檔所在資料夾。**必須保留「只輸出含 summary、systems、ids 的 JSON」**，少了這三個欄位名稱啟動時報錯。Helm 用 `llm.systemPrompt` 填多行文字即可 |
| 12 | `llm.id_pattern` | `[A-Za-z][A-Za-z0-9_.\-]*\d[A-Za-z0-9_.\-]*` | 正規表示式；`""` = 關閉 | 幻覺檢查：摘要文字裡符合這個樣式的字必須在證據裡找得到。見下方「第 12 項的副作用」 |
| 13 | `grafana.timeout_seconds` | 30 | 1～600 | 呼叫 Grafana（含資料源代理、寫註記）的逾時 |
| 14 | `grafana.retries`、`grafana.retry_backoff_seconds` | 2、0.5 | 0～10、0～60 | 短暫失敗（連線失敗、逾時、429／502／503／504）重試幾次、每次多等幾秒（第 n 次等 n 倍） |
| 15 | `labels.detect`、`labels.severity`、`labels.service`、`labels.remediation` | `aiops_detect`、`aiops_severity`、`service`、`aiops_remediation` | 標籤名稱 | 告警規則上的標籤名稱。改了要同步通知各系統團隊，Grafana 通知路由的 matcher 也要跟著改。`remediation` 的值是修復流程的名稱（見「註記以外的出口」） |
| 16 | `labels.default_severity` | P3 | P0～P3 | 嚴重度標籤沒寫或寫錯時當成哪一級 |
| 17 | `webhook.max_body_bytes` | 1000000 | 1024 以上 | 單次 Webhook 內容大小上限，超過回 413 |
| 18 | `webhook.host` | `0.0.0.0` | 位址 | 服務綁定的位址 |
| 19 | `retry_failed_round_seconds` | 30 | 1 以上 | 一輪失敗（例如資料庫斷線）之後多久再試 |
| 20 | `healthcheck.stuck_seconds` | 600 | 30 以上 | 同一輪跑超過這麼久，`/healthz` 回 503（K8s 會重啟 Pod）。要大於「Grafana 逾時 + LLM 逾時」乘上同時進行中的事故數 |
| 21 | `rounds_retention_days` | 90 | 1 以上 | 每輪執行紀錄（`rounds` 表）保留幾天 |
| 22 | `annotation.max_rule_links` | 5 | 0 以上 | 註記最後一行最多列幾條「調整優先權」的規則連結 |

### 關聯證據（批號時間線、呼叫鏈、指標走勢）

| 鍵 | 預設值 | 範圍 | 用途／什麼時候要調 |
| --- | --- | --- | --- |
| `logs.timeline_fields` | `id_fields` 裡的 `lot_id`、`carrier_id` | `id_fields` 的子集；`[]` = 不做 | 哪些識別碼要把跨系統的日誌串成時間線。工廠沒有 trace id，批號與載具就是關聯碼 |
| `llm.max_timeline_ids` | 2 | 0～20 | 最多替幾個關聯碼做時間線（取錯誤日誌裡出現最多次的）；0 = 不做 |
| `llm.max_timeline_events` | 30 | 1～500 | 每條時間線最多幾筆紀錄（由舊到新；超過會標示被截斷） |
| `logs.baseline_minutes` | 30 | 5～1440 | 「事故前」看多久。同一種錯誤在這段時間出現幾筆，會跟事故期間的筆數一起交給 LLM |
| `llm.max_log_patterns` | 6 | 0～20 | 最多替幾種錯誤比較事故前後的筆數；0 = 不比較 |
| `logs.warn_levels` | WARN、warn、Warn、WARNING、warning、Warning | 文字清單，可以是空的 | 日誌等級是這些值算警告；空清單 = 不撈警告。日誌沒有等級欄位（`logs.fields.level` 是空的）時不撈 |
| `llm.max_warn_patterns` | 5 | 0～20 | 最多給 LLM 看幾種警告；0 = 不撈警告 |
| `traces.trace_id_field` | `trace_id` | 欄位名稱；`""` = 不用 | 錯誤日誌裡記 trace id 的欄位。有的話直接用編號取（最多試 `max_traces` 的兩倍個），不夠才用搜尋補。Tempo 收到 Trace 後 8～40 秒以上才搜尋得到（2.9.0 實測），日誌帶 trace id 特別有用；VictoriaTraces 三秒內搜尋得到 |
| `traces.max_traces` | 3 | 0～20 | 每個事故最多取回幾條錯誤 Trace 的呼叫鏈；0 = 不取（連搜尋都不做） |
| `traces.max_spans` | 30 | 1～500 | 每條呼叫鏈最多列幾個 span（失敗的與入口優先） |
| `metrics.datasource_uid` | `""` | uid；`""` = 不查 | Grafana 上 Prometheus 型別資料源的 uid（URL 指到 VictoriaMetrics；Prometheus、Mimir 也可以） |
| `metrics.queries` | `[]` | 最多 20 條 | 事故時要查的指標（PromQL／MetricsQL），每條 `{name, expr, services}`。`expr` 一定要有 `$service`（換成系統名稱，已跳脫）；`services` 選填，有寫就只對那幾個系統查。名稱不能重複 |
| `metrics.lookback_minutes` | 60 | 5～1440 | 事故開始前往回看多久當基準（取中位數）。太短會把事故前的爬升算進基準 |
| `metrics.step_seconds` | 60 | 5～3600 | 取樣間隔；不要小於指標的抓取間隔 |
| `metrics.max_points` | 12 | 2～60 | 每條序列給 LLM 看幾個點（整段平均抽樣，最後一個點一定留） |
| `metrics.max_series` | 5 | 1～20 | 一條查詢回很多條序列（例如每個佇列一條）時，只留事故期間數值最大的幾條 |

#### 第 12 項的副作用（已知限制）

預設樣式是「字母開頭、夾數字的字」，本意是抓 `EQP-CVD-07`、`LOT24A0031` 這種編號。但模型如果在摘要裡寫了 `HTTP2`、`v2`、`S3`
這類泛稱，而證據裡沒有這個字，整段摘要會被當成幻覺、退回 `[未經分析]` 並進入退避。兩種處理方式：

- 把樣式改成只認你們的編號格式，例如 `"(EQP|LOT|FOUP)[-A-Z0-9]+"`。
- 設成 `""` 關掉摘要文字的檢查。模型另外列出的 `systems`、`ids` 清單照樣會檢查（不存在的系統或機台還是會被擋）。

這個副作用在真實資料上多常發生沒有量過；用 OpenAI gpt-6-luna 實測的那一次沒有觸發。

#### 每個事故會查幾次

每一次都是經 Grafana 資料源代理的一個請求。

| 查什麼 | VictoriaLogs／VictoriaTraces | Elasticsearch／Tempo |
| --- | --- | --- |
| 錯誤日誌（最新的在前） | 1 | 1（`_msearch`） |
| 有錯誤日誌時：各系統最早的幾筆、警告 | 2（沒有等級欄位或關掉警告就少 1 次） | 1（每個系統一個子查詢，加警告一個） |
| 事故前後的次數 | 1（一條查詢算完所有種類） | 1（每種錯誤兩個子查詢） |
| 錯誤日誌 0 筆時：數這段時間的總筆數與這些系統的筆數 | 1 | 1 |
| 關聯時間線 | 每種關聯碼欄位 2（內容、筆數） | 每種關聯碼欄位 1 |
| 呼叫鏈：日誌裡的 trace id | 逐個取，最多 `max_traces` × 2 個 | 同左 |
| 呼叫鏈：不夠才搜尋 | 每個系統 1 次，回來就是完整的 Trace | 1 次（TraceQL，一次問完所有系統），只回編號；要用到的再逐條取 |
| 指標走勢 | 查詢數 × 系統數（有寫 `services` 的只算那幾個系統） | 同左（指標跟日誌、Trace 的來源無關） |

**Elasticsearch 要留意的**

- 系統名稱、等級、識別碼欄位是 keyword，或動態 mapping 的 text 加 `.keyword`，都查得到。訊息欄位要是 text 型別。
- 事故前後的次數用 intervals 查詢（固定的詞照順序出現、不分大小寫）；VictoriaLogs 用 `seq`（分大小寫）。兩邊都是近似，
  斷詞規則也不同，同一批資料數出來可能差一點（測試樣本上兩邊數字相同）。
- 索引不存在不算錯誤（`ignore_unavailable`），會被當成「這段時間一筆日誌都沒有」，註記上的提示會寫「檢查 logs.index」。

### 註記以外的出口（`outputs`）

進來的還是只有 Grafana Webhook。出去的除了 Grafana 註記，多兩個；兩個都可以只當佔位。

| 鍵 | 預設值 | 範圍 | 用途 |
| --- | --- | --- | --- |
| `outputs.incident_api.url` | 空 | `http://`／`https://` 開頭；空 = 佔位 | 事故開單（opened）、內容更新（updated）、結案（closed）時 POST 一筆 JSON 給下游（告警中心、派工系統）。同樣的內容只送一次 |
| `outputs.incident_api.token` | 空 | 文字 | 有填就帶 `Authorization: Bearer` |
| `outputs.remediation.url` | 空 | 同上 | 自動修復（Conductor）：告警規則上貼了 `aiops_remediation=<流程名稱>` 的告警在響、事故還沒結案時 POST 一筆。同一張事故、同一條告警、同一個流程只送一次 |
| `outputs.remediation.token` | 空 | 文字 | 有填就帶 `Authorization: Bearer` |
| `outputs.remediation.enabled` | `false` | `true`／`false` | `false` = 就算有 url 也只記錄、不送。`true` 但沒有 url 啟動時報錯 |
| `outputs.timeout_seconds` | 10 | 1～120 | 等下游回應的逾時 |

- **跑哪個修復流程只看告警規則上的標籤（人事先貼的）。LLM 的推論不會觸發、也不會選流程**；根因分析只當說明附在請求裡。
- **佔位**：沒設 url（或修復沒有 `enabled`）就不送，把本來會送的內容記進資料庫的 `deliveries` 表
  （`channel`、`status` = placeholder／sent／failed、`payload`、`detail`、`attempts`）。佔位記過的之後不補送：之後才把出口打開，只對打開之後的事故生效。
- **失敗**：每一輪重試，最多 10 次。出口失敗不影響註記；那一輪會標成 degraded 並續跑。
- **註記**：規則有貼修復標籤時多一行 `自動修復｜流程名稱（系統：已送出）`，狀態另有「佔位，沒有送出」「送出失敗，會再試」「送出失敗，已放棄」。
- **格式**是這個專案自己定的（`aiops-lite.incident.v1`、`aiops-lite.remediation.v1`，欄位見 `INPUTS.md`），
  **沒有跟任何真的告警中心或 Conductor 對過**；只支援 Bearer 認證。

### 日誌沒有等級欄位（`logs.fields.level: ""`）

給「日誌沒有拆欄位、等級還包在訊息文字裡」的環境用，例如直接收的 K8s 容器標準輸出。
不用改收日誌的那一端；代價是準確度比有等級欄位差。**能拆欄位就拆欄位，這個只是過渡做法。**

設定（K8s 容器日誌的例子，服務名稱在 `kubernetes.container_name`）：

```yaml
logs:
  extra_filter: '"kubernetes.namespace_name":="factory"'   # 選填：只看某個命名空間（一般欄位的寫法；stream 欄位可以寫成 {欄位="值"}）
  fields: {service: kubernetes.container_name, level: ""}
```

告警的 `service` 標籤值、`edges` 裡的名稱都要跟 `kubernetes.container_name` 的值一樣。

**怎麼挑**：在訊息本文裡找 `logs.error_levels` 的詞，任何一個出現就算錯誤日誌（不分大小寫，預設收成 error、fatal、critical 三個詞；
VictoriaLogs 的寫法是 `_msg:i("error") or _msg:i("fatal") or …`，Elasticsearch 是對訊息欄位的 match 查詢）。
Elasticsearch 的設定把 `extra_filter` 那一行換成 `type: elasticsearch` 與 `index`，其餘一樣。
比對的是「完整的詞」，不是等級。要把 WARN 也算進去就在 `logs.error_levels` 加上。
送給 LLM 的補充說明會寫明「樣本是依訊息文字挑的，可能混入不是錯誤的紀錄」。

**實測**（2026-11-26，VictoriaLogs v1.17.0，13 筆自己造的容器日誌，用實際的查詢程式經 Grafana 代理；不是產線資料）：

| 訊息 | 結果 |
| --- | --- |
| <code>… &#124; ERROR &#124; …</code>、<code>… &#124; FATAL &#124; …</code>、`[error] upstream timed out`、`level=error msg=…` | 撈到 |
| `java.lang.NullPointerException at …`（訊息裡沒有等級字樣） | **漏掉** |
| `IOError: Connection reset by peer`（error 黏在別的字裡） | **漏掉** |
| <code>… &#124; INFO &#124; … recovered after error from upstream</code> | **誤撈** |
| <code>… &#124; DEBUG &#124; … log.level=error is not set</code> | **誤撈** |
| `finished with 0 errors`、`ErrorHandler registered`、WARN、一般的 INFO | 沒撈（對） |
| 別的容器的 ERROR | 沒撈（對） |

6 筆真的錯誤撈到 4 筆、漏 2 筆；6 筆不是錯誤的誤撈 2 筆（更早在 Elasticsearch 8.19.23 上用同樣 13 筆測的結果一樣）。
這幾筆是為了涵蓋各種寫法挑的，比例不代表產線上的準確度。同一批資料用預設的 `log.level`（欄位不存在）是 0 筆。
測試樣本（`tests/fixtures/grafana13/victoria_logs.json` 的 `unparsed`）裡另有一筆 `health probe: 0 error(s) in last minute` 也被誤撈：
`error(s)` 的括號把 error 隔成完整的詞。同一批樣本寫進 Elasticsearch，挑到的筆數一樣，那一筆同樣被誤撈（`tests/test_backends.py`）。

**限制**：

- INFO、DEBUG 的訊息裡提到 error 這個詞會被當成錯誤（實測）。
- 沒有等級字樣的錯誤（例外堆疊的後續幾行、只寫例外名稱的訊息）撈不到（實測）。
- 等級字樣黏在別的字裡（`IOError`、`errors`）不算同一個詞，撈不到；`finished with 0 errors` 這種也因此不會誤撈（實測）。
- 多行的例外堆疊如果被收成一行一筆，只有帶等級字樣的那一行會進來。
- 分不出警告：沒有等級欄位時不撈警告那一包。
- 機台、批號沒有拆成欄位的話，識別碼統計與關聯時間線仍然是空的；這個設定只解決「挑錯誤日誌」。
- 沒有在真實的產線日誌上驗證過。

## B. 留在程式裡的內部常數（不開放設定）

| 項目 | 位置 | 為什麼不放進 config.yaml |
| --- | --- | --- |
| 嚴重度只有 P0～P3 與它們的排序 | `src/grafana.py` 的 `SEVERITIES`、`src/grouping.py` 的 `SEVERITY_RANK` | 儀表板用 `P0`、`P1` 標籤查註記；改了儀表板失效、舊事故對不上 |
| 註記標籤 `aiops`、`aiops-incident:<uid>`、`open`／`closed`、系統名稱 | `src/report.py` | 儀表板的查詢、「註記建過就不重複建」的查找都依賴這些標籤 |
| 註記的版面（標題、摘要、證據的幾行、時間）與「進行中／已結案」的文字 | `src/report.py` | 對外的呈現契約；`[LLM 推論]`／`[未經分析]` 的標示不能被設定拿掉 |
| 證據的判斷與用字：註記上一行（已有、缺的與原因、沒接的），交給模型的完整版另外寫每種證據少了會看不出什麼 | `src/rca.py` 的 `overview`、`overview_brief`、`overview_text`、`CAN_ANSWER` | 照實際撈到的結果產生，沒有可調的門檻；用字是給值班人員看的固定說法 |
| 規則編輯連結的路徑 `/alerting/<uid>/edit` | `src/report.py` | 由 Grafana 的版本決定，不是我們的設定 |
| 資料表與欄位名稱、成員告警的時間欄位 | `src/db.py` | 改了要搬資料；程式只會自動「補欄位」 |
| 事故 uid 的長度（12 碼） | `src/grouping.py` | 內部識別；改了舊註記的標籤對不上 |
| 要重試的 HTTP 狀態 429／502／503／504 | `src/grafana.py` 的 `RETRY_STATUS` | HTTP 的語意，不是調整項（要調的是次數與間隔：第 14 項） |
| 標籤值 `true／1／yes／on` 視為開啟 | `src/grafana.py` 的 `TRUE_VALUES` | 值的寫法慣例 |
| 退避的指數上限（2 的 30 次方）、錯誤訊息截斷長度（200、300 字） | `src/main.py` 的 `backoff_seconds`、`src/grafana.py`、`src/rca.py` | 防溢位與 log 長度的內部護欄 |
| 註記查找的 `limit: 1`、LogsQL 查詢的寫法（`in`、`seq`、`partition by`、`stats count() if`）、Elasticsearch 查詢的寫法（`_msearch`、`terms`、`intervals`）、每筆日誌的 `_time`／`_msg` | `src/grafana.py`、`src/logs.py` 的 `TIME_FIELD`、`MESSAGE_FIELD` 與各查詢方法 | 查詢的實作細節；`_time`、`_msg` 是 VictoriaLogs 固定的欄位名稱，Elasticsearch 的文件由程式轉成同一種形狀 |
| 設定檔路徑 `/etc/aiops/config.yaml` 與唯一的環境變數 `AIOPS_CONFIG_FILE` | `src/main.py` | 設定的唯一入口，本身不能再放進設定檔 |
| 各設定值的合法範圍、log 等級的可選值 | `src/main.py` | 驗證規則 |
| Webhook 的必要欄位（`status`、`fingerprint`、`startsAt`、`labels`、`ruleUID`、`values`） | `src/grafana.py` 的 `parse_webhook` | Grafana 的 webhook 格式，不是我們定的 |
| 搜尋 Trace 時一次要 20 條（VictoriaTraces 是每個系統 20 條）、再挑最近的 | `src/rca.py` 的 `TRACE_SEARCH_LIMIT` | 搜尋結果不保證先回最新的；要調的是最後取幾條（`traces.max_traces`） |
| Trace 搜尋用服務名稱（OTel 的 `service.name`）比對分析單位：VictoriaTraces 是 Jaeger 的 `service` 參數加 `error=true`，Tempo 是 TraceQL 的 `resource.service.name` 加 `status = error` | `src/traces.py` 的 `search_error_traces` | Jaeger 查詢介面一次只能指定一個服務。把 `labels.service` 換成 function 或 cluster 時呼叫鏈會撈不到（見 README「改用別的單位分析」） |
| 哪些 span 算失敗：`error` 標籤是 true，或 `otel.status_code` 是 ERROR；錯誤訊息取 `otel.status_description` | `src/rca.py` 的 `ERROR_TAG_VALUES`、`summarize_trace` | OTLP 的 status=ERROR 進 VictoriaTraces 之後就是這兩個標籤（實測 v0.5.0）；Tempo 回的 OTLP 由 `src/traces.py` 的 `_from_otlp` 轉成同樣的標籤。是格式，不是調整項 |
| 指標查詢最多 20 條、佔位字是 `$service` | `src/main.py` 的 `MAX_METRIC_QUERIES`、`src/rca.py` 的 `SERVICE_PLACEHOLDER` | 每張事故要查「查詢數 × 系統數」次，上限是保護指標資料庫；佔位字是設定檔的寫法約定 |
| 指標走勢的算法（基準取中位數、數值留四位有效數字） | `src/rca.py` 的 `summarize_series` | 只是把序列壓短的算術，沒有任何「算不算異常」的門檻可調 |
| 每個系統取最早的 3 筆錯誤、警告取樣 50 筆 | `src/rca.py` 的 `FIRST_ERRORS_PER_SYSTEM`、`WARN_SAMPLE` | 取樣量：保證被別的系統擠掉的錯誤至少有樣本。不是判斷門檻；只在 `EVAL.md` 的人造案例上跑過，未驗證泛化 |
| 錯誤樣板怎麼產生（UUID、長十六進位字串、數字換成 `*`）、怎麼變成計數用的條件（樣板裡固定不變的詞照順序出現：LogsQL 的 `seq`、Elasticsearch 的 intervals；固定的詞少於兩個就不數） | `src/rca.py` 的 `log_pattern`、`pattern_words` | 歸類與計數的實作。是近似：別種訊息剛好有同樣的詞、順序也一樣時會被算在一起；未驗證泛化 |
| 可能原因最多 3 個；信心分數 0～100，分段是 70／40；沒有直接證據指向的系統分數最高 30 | `src/rca.py` 的 `MAX_CANDIDATES`、`CONFIDENCE_HIGH`、`CONFIDENCE_MEDIUM`、`NO_DIRECT_EVIDENCE_CAP`、`DIRECT_EVIDENCE` | 3 個是註記版面的上限。分段是提示詞裡跟模型講的定義，上限 30 落在「只是推測」那一段；分數不是機率、沒有校準。「有沒有直接證據」是名稱的字串比對。都只在 `EVAL.md` 的人造案例跑過，未驗證泛化 |
| LLM 沒有分析時註記上直接列的證據：最多 5 行、每行引用的原文最多 120 字 | `src/rca.py` 的 `MAX_FACT_LINES`、`FACT_CHARS` | 版面的上限，不是判斷門檻；順序是固定規則（事故前沒出現過的先列），不代表誰是源頭 |
| 出口送失敗最多試 10 次（每一輪一次）；出口的 JSON 格式與 `schema` 名稱；`deliveries` 表的狀態用字 | `src/outputs.py` 的 `MAX_ATTEMPTS`、`incident_payload`、`remediation_payloads` | 10 是沒有依據的數字，未驗證泛化；調之前先看下游多久會恢復。格式是對下游的契約，改了要跟對方一起改 |
| 呼叫鏈帶哪些 span 屬性（`peer.service`、`db.system` 等） | `src/rca.py` 的 `SPAN_ATTRIBUTES` | 只取能指出「下一站是誰」的屬性，避免把請求內容送給 LLM |
| LLM 回答的格式（`summary`、`systems`、`ids` 的 JSON） | `src/rca.py` 的 `check_answer` | 幻覺檢查靠這個格式；自訂提示詞時啟動會檢查有沒有保留 |
| 嚴重度的判定方式 | — | 程式沒有「判定規則」：嚴重度直接讀告警的標籤，事故取成員裡最高的，只升不降 |

## 改預設值之前

A 表的預設值有測試鎖住（`tests/test_tunables.py::test_without_the_optional_settings_everything_keeps_its_old_value`）：
改預設值等於改掉所有沒寫該設定的部署的行為，要改請連測試一起改並在 commit 訊息說明原因。
