"""破壞性端到端場景（docs/CHAOS_E2E_REPORT.md）：世界模擬器 -> 假 Grafana（後面是 ES 的 HTTP 層替身）-> 平台真實管線。

世界模擬器（world.py）用同一組參數同時產生兩種資料，兩者才會一致：
- 指標：每個服務每分鐘的 Elastic APM metrics 文件（transaction.duration.summary.sum／value_count，依 event.outcome 拆兩份）
- trace：逐請求走訪呼叫樹，輸出 Elastic APM 的 transaction／span 文件與錯誤日誌（含 http.response.status_code、span.type／subtype／
  destination.service.resource）
trace／日誌：假 Grafana 解開平台真的 GrafanaSpec 送的資料源 proxy 查詢，交給 es.py 回應；平台程式一行都不改、不知道對面是替身。
指標：平台不再直連 Elasticsearch（2026-11-20），由模擬器自己的資料源 chaos_metrics 直接讀世界（es.py、metric_query.py）。
"""
