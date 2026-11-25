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
| `es.datasource_uid` | 空 | Grafana 上 ES 資料源的 uid；空 = 沒有日誌證據 |
| `es.log_index`、`es.trace_index` | `*`、空 | 日誌與 Trace 的 index；trace_index 空 = 不查 Trace |
| `tempo.datasource_uid` | 空 | Grafana 上 Tempo 資料源的 uid；空 = 沒有呼叫鏈證據 |
| `es.id_fields` | `[eqp_id, lot_id, carrier_id]` | 錯誤日誌裡要統計出現次數的識別碼欄位 |
| `es.log_fields` | `time: @timestamp`、`service: service.name`、`level: log.level`、`message: message` | 日誌文件的欄位名稱。只有 `level` 可以設成 `""`（見下面「日誌沒有等級欄位」） |
| `es.trace_fields` | `time: @timestamp`、`service: service.name`、`span: span.name`、`trace_id: trace.id`、`status: status.code`、`error: error.message` | Trace 文件的欄位名稱 |
| `timezone` | `Asia/Taipei` | 註記裡顯示的時間 |
| `log_level` | `INFO` | `DEBUG` 會記下送給 LLM 的內容與原始回應 |
| `debounce_seconds` | 60 | 收到第一個 Webhook 後等多久才開始分析 |
| `followup_seconds` | 60 | 有進行中事故時，沒有新 Webhook 也每隔多久再跑一輪 |
| `close_after_seconds` | 300 | 成員告警全部 resolved 之後，再安靜多久才結案 |
| `edges` | 空 | 系統層級的上下游關係 `[上游, 下游]` |

### 進階調整（原本寫死在程式裡，2026-11-21 起可調）

| # | 鍵 | 預設值 | 範圍 | 用途／什麼時候要調 |
| --- | --- | --- | --- | --- |
| 1 | `llm.timeout_seconds` | 60 | 1～3600 | 等 LLM 回應的逾時。推理型模型比較慢、常逾時就調大；`healthcheck.stuck_seconds` 要跟著調大 |
| 2 | `es.error_levels` | ERROR、error、Error、FATAL、fatal、CRITICAL、critical | 非空清單 | 日誌等級是這些值才算錯誤日誌。公司的日誌寫 `E`、`SEVERE` 之類就要改（大小寫不同要分開列） |
| 3 | `es.error_statuses` | error、ERROR、Error、STATUS_CODE_ERROR | 非空清單 | span 狀態是這些值才算錯誤 span |
| 4 | `es.max_hits` | 100 | 1～10000 | 每次 ES 查詢最多撈幾筆；識別碼的出現次數只統計這幾筆 |
| 5 | `es.window_padding_minutes` | 10 | 0～1440 | 事故開始時間往前多看幾分鐘（錯誤日誌常比告警早出現） |
| 6 | `llm.max_log_samples` | 20 | 0 以上 | 給 LLM 看幾筆錯誤日誌樣本（控制 prompt 長度） |
| 7 | `llm.max_trace_samples` | 10 | 0 以上 | 給 LLM 看幾筆 Trace 錯誤片段 |
| 8 | `llm.max_message_chars` | 300 | 20 以上 | 單筆日誌訊息截斷長度 |
| 9 | `llm.top_ids` | 10 | 1 以上 | 每種識別碼只列出現次數前幾名 |
| 10 | `llm.max_summary_chars` | 300 | 20 以上 | LLM 摘要超過就截斷（註記第 2 行） |
| 11 | `llm.system_prompt_file` | 空（用內建提示詞） | 檔案路徑 | 自訂系統提示詞。相對路徑相對於設定檔所在資料夾。**必須保留「只輸出含 summary、systems、ids 的 JSON」**，少了這三個欄位名稱啟動時報錯。Helm 用 `llm.systemPrompt` 填多行文字即可 |
| 12 | `llm.id_pattern` | `[A-Za-z][A-Za-z0-9_.\-]*\d[A-Za-z0-9_.\-]*` | 正規表示式；`""` = 關閉 | 幻覺檢查：摘要文字裡符合這個樣式的字必須在證據裡找得到。見下方「第 12 項的副作用」 |
| 13 | `grafana.timeout_seconds` | 30 | 1～600 | 呼叫 Grafana（含 ES 代理、寫註記）的逾時 |
| 14 | `grafana.retries`、`grafana.retry_backoff_seconds` | 2、0.5 | 0～10、0～60 | 短暫失敗（連線失敗、逾時、429／502／503／504）重試幾次、每次多等幾秒（第 n 次等 n 倍） |
| 15 | `labels.detect`、`labels.severity`、`labels.service` | `aiops_detect`、`aiops_severity`、`service` | 三個不同的名稱 | 告警規則上的標籤名稱。改了要同步通知各系統團隊，Grafana 通知路由的 matcher 也要跟著改 |
| 16 | `labels.default_severity` | P3 | P0～P3 | 嚴重度標籤沒寫或寫錯時當成哪一級 |
| 17 | `webhook.max_body_bytes` | 1000000 | 1024 以上 | 單次 Webhook 內容大小上限，超過回 413 |
| 18 | `webhook.host` | `0.0.0.0` | 位址 | 服務綁定的位址 |
| 19 | `retry_failed_round_seconds` | 30 | 1 以上 | 一輪失敗（例如資料庫斷線）之後多久再試 |
| 20 | `healthcheck.stuck_seconds` | 600 | 30 以上 | 同一輪跑超過這麼久，`/healthz` 回 503（K8s 會重啟 Pod）。要大於「ES 逾時 + LLM 逾時」乘上同時進行中的事故數 |
| 21 | `rounds_retention_days` | 90 | 1 以上 | 每輪執行紀錄（`rounds` 表）保留幾天 |
| 22 | `annotation.max_rule_links` | 5 | 0 以上 | 註記最後一行最多列幾條「調整優先權」的規則連結 |

### 關聯證據（批號時間線、Tempo 呼叫鏈）

| 鍵 | 預設值 | 範圍 | 用途／什麼時候要調 |
| --- | --- | --- | --- |
| `es.timeline_fields` | `id_fields` 裡的 `lot_id`、`carrier_id` | `id_fields` 的子集；`[]` = 不做 | 哪些識別碼要把跨系統的日誌串成時間線。工廠沒有 trace id，批號與載具就是關聯碼 |
| `llm.max_timeline_ids` | 2 | 0～20 | 最多替幾個關聯碼做時間線（取錯誤日誌裡出現最多次的）；0 = 不做 |
| `llm.max_timeline_events` | 30 | 1～500 | 每條時間線最多幾筆紀錄（由舊到新；超過會標示被截斷） |
| `tempo.trace_id_field` | `trace.id` | 欄位名稱；`""` = 不用 | 錯誤日誌裡記 trace id 的欄位。有的話直接用編號向 Tempo 取（立即取得到），不夠才用搜尋補 |
| `tempo.max_traces` | 3 | 0～20 | 每個事故最多取回幾條錯誤 Trace 的呼叫鏈；0 = 不取 |
| `tempo.max_spans` | 30 | 1～500 | 每條呼叫鏈最多列幾個 span（失敗的與入口優先） |
| `metrics.datasource_uid` | `""` | uid；`""` = 不查 | Grafana 上 Prometheus 相容資料源（Prometheus、VictoriaMetrics…）的 uid |
| `metrics.queries` | `[]` | 最多 20 條 | 事故時要查的指標，每條 `{name, expr, services}`。`expr` 一定要有 `$service`（換成系統名稱，已跳脫）；`services` 選填，有寫就只對那幾個系統查。名稱不能重複 |
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

### 日誌沒有等級欄位（`es.log_fields.level: ""`）

給「日誌沒有拆欄位、等級還包在訊息文字裡」的環境用，例如 Fluentd 直接收的 K8s 容器標準輸出。
不用改收日誌的那一端；代價是準確度比有等級欄位差。**能拆欄位就拆欄位，這個只是過渡做法。**

設定（K8s 容器日誌的例子，服務名稱在 `kubernetes.container_name`）：

```yaml
es:
  log_index: "fluentd-*"
  log_fields: {service: kubernetes.container_name, level: ""}
```

告警的 `service` 標籤值、`edges` 裡的名稱都要跟 `kubernetes.container_name` 的值一樣。

**怎麼挑**：在訊息欄位做全文比對，訊息裡有 `es.error_levels` 任何一個詞就算錯誤日誌（不分大小寫，預設是 error、fatal、critical）。
比對的是「完整的詞」，不是等級。要把 WARN 也算進去就在 `es.error_levels` 加上。
送給 LLM 的補充說明會寫明「樣本是依訊息文字挑的，可能混入不是錯誤的紀錄」。

**實測**（Elasticsearch 8.19.23、動態 mapping、13 筆自己造的容器日誌，用實際的查詢程式；不是產線資料）：

| 訊息 | 結果 |
| --- | --- |
| <code>… &#124; ERROR &#124; …</code>、<code>… &#124; FATAL &#124; …</code>、`[error] upstream timed out`、`level=error msg=…` | 撈到 |
| `java.lang.NullPointerException at …`（訊息裡沒有等級字樣） | **漏掉** |
| `IOError: Connection reset by peer`（error 黏在別的字裡） | **漏掉** |
| <code>… &#124; INFO &#124; … recovered after error from upstream</code> | **誤撈** |
| <code>… &#124; DEBUG &#124; … log.level=error is not set</code> | **誤撈** |
| `finished with 0 errors`、`ErrorHandler registered`、WARN、一般的 INFO | 沒撈（對） |
| 別的容器的 ERROR | 沒撈（對） |

6 筆真的錯誤撈到 4 筆、漏 2 筆；6 筆不是錯誤的誤撈 2 筆。這幾筆是為了涵蓋各種寫法挑的，比例不代表產線上的準確度。
同一批資料用預設的 `log.level`（欄位不存在）是 0 筆。

**限制**：

- 沒有等級字樣的錯誤（例外堆疊的後續幾行、只寫例外名稱的訊息）撈不到。
- INFO、DEBUG 的訊息裡提到 error 這個詞會被當成錯誤。
- 訊息欄位在 ES 必須是 `text` 型別；是 `keyword` 的話比不到，結果是 0 筆。
- 多行的例外堆疊如果被收成一行一筆，只有帶等級字樣的那一行會進來。
- 機台、批號沒有拆成欄位的話，識別碼統計與關聯時間線仍然是空的；這個設定只解決「挑錯誤日誌」。
- 沒有在真實的產線日誌上驗證過。

## B. 留在程式裡的內部常數（不開放設定）

| 項目 | 位置 | 為什麼不放進 config.yaml |
| --- | --- | --- |
| 嚴重度只有 P0～P3 與它們的排序 | `src/grafana.py` 的 `SEVERITIES`、`src/grouping.py` 的 `SEVERITY_RANK` | 儀表板用 `P0`、`P1` 標籤查註記；改了儀表板失效、舊事故對不上 |
| 註記標籤 `aiops`、`aiops-incident:<uid>`、`open`／`closed`、系統名稱 | `src/report.py` | 儀表板的查詢、「註記建過就不重複建」的查找都依賴這些標籤 |
| 註記的版面（標題、摘要、證據的幾行、時間）與「進行中／已結案」的文字 | `src/report.py` | 對外的呈現契約；`[LLM 推論]`／`[未經分析]` 的標示不能被設定拿掉 |
| 證據幾行的用字與判斷（有的、缺的與原因、查詢失敗、還沒接的） | `src/rca.py` 的 `overview`、`overview_text` | 照實際撈到的結果產生，沒有可調的門檻；用字是給值班人員看的固定說法 |
| 規則編輯連結的路徑 `/alerting/<uid>/edit` | `src/report.py` | 由 Grafana 的版本決定，不是我們的設定 |
| 資料表與欄位名稱、成員告警的時間欄位 | `src/db.py` | 改了要搬資料；程式只會自動「補欄位」 |
| 事故 uid 的長度（12 碼） | `src/grouping.py` | 內部識別；改了舊註記的標籤對不上 |
| 要重試的 HTTP 狀態 429／502／503／504 | `src/grafana.py` 的 `RETRY_STATUS` | HTTP 的語意，不是調整項（要調的是次數與間隔：第 14 項） |
| 標籤值 `true／1／yes／on` 視為開啟 | `src/grafana.py` 的 `TRUE_VALUES` | 值的寫法慣例 |
| 退避的指數上限（2 的 30 次方）、錯誤訊息截斷長度（200、300 字） | `src/main.py` 的 `backoff_seconds`、`src/grafana.py`、`src/rca.py` | 防溢位與 log 長度的內部護欄 |
| 註記查找的 `limit: 1`、ES 查詢的 `ignore_unavailable`、`.keyword` 備援 | `src/grafana.py` | 查詢的實作細節 |
| 設定檔路徑 `/etc/aiops/config.yaml` 與唯一的環境變數 `AIOPS_CONFIG_FILE` | `src/main.py` | 設定的唯一入口，本身不能再放進設定檔 |
| 各設定值的合法範圍、log 等級的可選值 | `src/main.py` | 驗證規則 |
| Webhook 的必要欄位（`status`、`fingerprint`、`startsAt`、`labels`、`ruleUID`、`values`） | `src/grafana.py` 的 `parse_webhook` | Grafana 的 webhook 格式，不是我們定的 |
| 向 Tempo 搜尋時一次要 20 條、再挑最近的 | `src/rca.py` 的 `TRACE_SEARCH_LIMIT` | Tempo 的搜尋不保證先回最新的；要調的是最後取幾條（`tempo.max_traces`） |
| Tempo 查詢用 `resource.service.name` 比對分析單位 | `src/grafana.py` 的 `search_error_traces` | OTel 的標準屬性。把 `labels.service` 換成 function 或 cluster 時呼叫鏈會撈不到（見 README「改用別的單位分析」） |
| 指標查詢最多 20 條、佔位字是 `$service` | `src/main.py` 的 `MAX_METRIC_QUERIES`、`src/rca.py` 的 `SERVICE_PLACEHOLDER` | 每張事故要查「查詢數 × 系統數」次，上限是保護指標資料庫；佔位字是設定檔的寫法約定 |
| 指標走勢的算法（基準取中位數、數值留四位有效數字） | `src/rca.py` 的 `summarize_series` | 只是把序列壓短的算術，沒有任何「算不算異常」的門檻可調 |
| 呼叫鏈帶哪些 span 屬性（`peer.service`、`db.system` 等） | `src/rca.py` 的 `SPAN_ATTRIBUTES` | 只取能指出「下一站是誰」的屬性，避免把請求內容送給 LLM |
| LLM 回答的格式（`summary`、`systems`、`ids` 的 JSON） | `src/rca.py` 的 `check_answer` | 幻覺檢查靠這個格式；自訂提示詞時啟動會檢查有沒有保留 |
| 嚴重度的判定方式 | — | 程式沒有「判定規則」：嚴重度直接讀告警的標籤，事故取成員裡最高的，只升不降 |

## 改預設值之前

A 表的預設值有測試鎖住（`tests/test_tunables.py::test_without_the_optional_settings_everything_keeps_its_old_value`）：
改預設值等於改掉所有沒寫該設定的部署的行為，要改請連測試一起改並在 commit 訊息說明原因。
