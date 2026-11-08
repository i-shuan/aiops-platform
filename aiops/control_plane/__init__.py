"""控制面：手動動作（admin.*），由 API 或 CLI 觸發，各自讀寫自己的狀態。只有這裡的算子可以碰 storage。

  learning     學習閉環：調參提案／回測／核准、回饋收集與轉提案（tuning.py）、LLM 抽規則（tuning_rules.py）、
               調參成效報表與規則升級器（learning.py）
  operations   日常維運：靜音（silences）、欄位對照（mappings）、事件群查詢與結案（incidents）、
               外部異常／trace／lot 路徑匯入（ingest）、資料源自我檢查（sources）
"""
