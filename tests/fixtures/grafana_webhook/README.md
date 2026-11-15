# Grafana 告警 webhook 的真實樣本

三個檔都是 Grafana 13.0.7 實際送出的 webhook 內容（2026-11-15 抓的，原樣、沒有改過）：

- 本機叢集的臨時 namespace 起一個拋棄式 `grafana/grafana:13.0.7`，用 provisioning API 建一個 Webhook 通知管道
  （網址指向一個只會把請求印出來的 pod）與兩條一定會觸發的告警規則。
- `firing_CheckoutCpuHigh.json`：labels `service=checkout`、`severity=critical`、`team=a`。
- `firing_DiskAlmostFull.json`：labels `host=db-01`、`severity=warning`——**沒有 service 標籤**（實體對不上的例子）。
- `resolved_CheckoutCpuHigh.json`：把 CheckoutCpuHigh 那條規則刪掉之後 Grafana 送的結案通知。
- 送出的 HTTP 標頭是 `User-Agent: Grafana`、`Authorization: Bearer <通知管道設的 credentials>`、`Content-Type: application/json`。

`tests/test_grafana_alerts.py` 讀這三個檔。測試裡其他情況（嚴重度查不到、一次推好幾筆）都是從 `firing_CheckoutCpuHigh.json`
複製一份再改標籤，不是另外手寫的格式。
