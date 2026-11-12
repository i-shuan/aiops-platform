# OTel Collector → Elasticsearch 真實文件樣本

來源：本機拋棄式環境（`make_samples.py` 開頭有完整指令），**不是**叢集裡的 es-test。

- Elasticsearch `8.19.23`（跟叢集同版），單節點、關掉安全性，只綁 `127.0.0.1:29200`
- `otel/opentelemetry-collector-contrib:0.165.0`，`elasticsearch` exporter 只設 `endpoint`（其他全用預設）
- 送件：`make_samples.py` 自己組 OTLP/HTTP JSON。服務有 checkout（入口）、cart-api、ord-svc（Kafka 消費者）；
  依賴有 PostgreSQL、Redis、外部 HTTPS（tax-gw）、Kafka 生產／單筆消費（parent）／批次消費（links）；
  另有錯誤 trace（status ERROR、exception 事件、HTTP 500/502）、取樣 tracestate（`ot=th:c`、`ot=th:8`、`ot=p:2`），
  以及 INFO／WARN（結構化 body）／ERROR 日誌
- 每份資料送兩次：scope 不帶 `elastic.mapping.mode`（= exporter 預設），以及帶 `elastic.mapping.mode=ecs`

| 檔案 | 筆數 | 是什麼 |
| --- | --- | --- |
| `traces-generic.otel-default.json` | 57 | 預設（otel）模式的 span |
| `logs-generic.otel-default.json` | 7 | 預設模式的日誌（5）＋ span 的 exception 事件（2） |
| `traces-generic-default.json` | 57 | ecs 模式的 span（見下方「ECS 模式」） |
| `logs-generic-default.json` | 10 | ecs 模式的日誌（送了兩次，見 meta.json） |
| `logs-apm.error-default.json` | 4 | ecs 模式的 exception 事件（送了兩次） |
| `*.mapping.json` | | 寫入後 ES 實際的 mapping |

## 實測結論（Collector 0.165.0 + ES 8.19.23）

**預設 mapping 模式是 `otel`。** README 文字自相矛盾（有一段標題寫 "Default (none) mapping mode"），
實測不設任何 mapping 時文件進 `traces-generic.otel-default`／`logs-generic.otel-default`，欄位是 OTel 原名。
0.165 已經拿掉 `mapping.mode` 設定鍵（設了 Collector 起不來：`'mapping' has invalid keys: mode`），
要指定模式只能用 scope 屬性 `elastic.mapping.mode` 或 client metadata `X-Elastic-Mapping-Mode`。

### otel 模式的 span（`traces-<dataset>.otel-<namespace>`）

| 意義 | 欄位 | 實測值／型別 |
| --- | --- | --- |
| 時間 | `@timestamp` | mapping 是 `date`；`_source` 是**字串**、毫秒＋小數，例如 `"1794487341966.212300"` |
| trace／span／父 | `trace_id`、`span_id`、`parent_span_id` | keyword，十六進位；根 span 沒有 `parent_span_id` |
| 種類 | `kind` | `"Server"`／`"Client"`／`"Producer"`／`"Consumer"`（首字大寫；Internal 沒測） |
| 耗時 | `duration` | long，**奈秒**（120 ms → `120000000`）；APM 是 `*.duration.us` 微秒，差 1000 倍 |
| 狀態 | `status.code`、`status.message` | `"Error"`；UNSET 時整個 `status` 是 `{}`（沒有 code） |
| 服務 | `resource.attributes.service.name` | `resource.attributes` 是 passthrough：直接查 `service.name` 也查得到 |
| 屬性 | `attributes.<原名>` | 平的點號鍵：`attributes["db.system.name"]`、`attributes["server.address"]` |
| 取樣 | `trace_state` | 原樣字串，例如 `"ot=th:c"`；沒取樣資訊時沒有這個欄位 |
| links | `links` | `[{"trace_id": ..., "span_id": ...}]`，沒有時是 `[]` |
| span 事件 | （不在 span 文件裡） | 拆成**另一筆日誌文件**進 `logs-<dataset>.otel-<namespace>`，`event_name: "exception"`，帶 `trace_id`／`span_id` |

沒有 `processor.event`、`transaction.*`、`event.outcome`、`representative_count`。
入口請求要看 `kind == "Server"`（或 `Consumer`），不能看「有沒有 span.type」。

### otel 模式的日誌（`logs-<dataset>.otel-<namespace>`）

- 訊息：`body.text`（字串 body）或 `body.structured`（map body，原樣保留成物件）
- 嚴重度：`severity_number`（ERROR = 17）、`severity_text`
- 例外：`attributes["exception.type"]`、`attributes["exception.message"]`
- 關聯：`trace_id`、`span_id`
- 跟 APM 不同：這個資料流**所有等級都在一起**（INFO 也在），錯誤要自己過濾；
  同一個錯誤若 span 有 exception 事件、程式又寫了 ERROR 日誌，會出現**兩筆**（一筆 `event_name: exception`、一筆 severity 17）

### ECS 模式（`elastic.mapping.mode=ecs`）——不支援，記成死區

- span 進 `traces-generic-<namespace>`，**裸 ES 沒有對應樣板，寫入直接失敗**（`index_not_found_exception`）；
  樣本是在拋棄式 ES 手動補最小樣板後才拿到的
- span 形狀接近 ECS：`trace.id`、`span.id`、`parent.id`、`span.kind`（`"SERVER"` 全大寫）、`span.name`、
  `service.name`、`event.outcome: failure`（只有錯誤時才有）、`span.db.type`、`peer.service`、`server.address`
- **完全沒有耗時欄位**、沒有 `trace_state`：自身耗時、延遲佐證、取樣還原全部做不到
- exception 事件進 `logs-apm.error-<namespace>`（`error.exception` 是單一物件），形狀跟 Elastic APM 的錯誤文件相容
- 一般日誌進 `logs-generic-<namespace>`，訊息在 `message`；結構化 body 的訊息**整個不見**
