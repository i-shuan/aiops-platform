"""管線算子，依執行順序分資料夾（套件名稱不能以數字開頭，順序寫在這裡）：

  ingestion     抓資料（ingestion.fetch、ingestion.incident_series）
  detectors     異常偵測（detector.*，每個演算法一個檔，彼此不 import）
  ensemble      多偵測器裁決（detector.ensemble）
  grouping      事件收斂（alert.same_entity、alert.topology_chain、alert.lot_path_chain）；
                進行中事件群吸收新異常（alert.absorb_open，排在計分之後、抑制之前）
  scoring       計分（alert.golden_signals、alert.p0_boost）
  suppression   抑制（alert.suppress_*）
  rca           根因診斷（rca.root_cause.*、rca.section.*、rca.report）
  enrichment    LLM 摘要與供應商（enrichment.llm_summary；noop 不連外）
  notification  通知內容、心跳結案與發送（alert.notify_payload、alert.heartbeat、notifier.*）

實際的執行順序由 pipelines/ 的 YAML 決定，這裡的分法只是讓人看得出每個檔案在管線的哪一段。
算子由 interfaces/runner.load_all() 掃描載入，載入即完成 @OperatorRegistry.register。
"""
