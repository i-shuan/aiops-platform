"""認知型 RCA 的證據層（rca.facts，docs/COGNITIVE_RCA_DESIGN.md §3、§5）：原始資料 -> list[Fact]。

只陳述「資料裡發生了什麼」，不下結論：不寫誰是兇手、不判斷是時鐘偏移還是逾時、不判斷是不是「自己的錯」、
不判斷佇列算不算積壓。結論交給機率模型（rca.root_cause.bayes）。

- spans.py   ：入口耗時／失敗率、自身耗時增量、資源等待增量、流入流量、呼叫邊的流量變化與扇出、子 span 超出父 span
- logs.py    ：錯誤日誌樣板的頻率變化
- mq.py      ：訊息佇列延遲
- series.py  ：事件群原始時序的前後位移
- operator.py：rca.facts 算子（組合上面四個、固定排序、記下資料不足而略過的項目）

數字的算法一律重用舊引擎的純函式（stages/rca/spans.py、logs.py），不另寫一份；舊檔案不改。
"""
