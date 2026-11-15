"""根因結論等級的共用判準（純函式）：第一名候選是不是「只有症狀位置」（engine/rca_types.ConclusionType 的 symptom_location）。

根因演算法（rca.root_cause.evidence、rca.root_cause.llm）排完名之後各自呼叫，回傳非空字串就把這一次的結論降成
symptom_location 並把字串寫進 ctx.rca_conclusion_reason。這裡不改排名、不改分數、不指認別人。

判準（全部成立才降級；任何一項算不出來就不降，維持演算法宣告的等級）：
  1. 候選自己沒有變慢：自身耗時增量 < max(min_self_ms, 事件前 × min_self_ratio)（跟 evidence 的顯著門檻同一條）
  2. 候選自己在出錯：自身錯誤率增量 >= min_error_increase（入口失敗、而且它底下沒有任何失敗的子 span）
  3. 事件期間它「自己失敗」的入口請求裡，過半（MIN_AFTER_DEPENDENCY_SHARE）在失敗之前已經同步呼叫過至少一個依賴
     （別的服務、或資料庫／快取／外部 API 這類資源），而且那些呼叫全部成功

依據：span 只記耗時與成功／失敗，不記回應內容。第 3 點成立時，「依賴回了 200 但內容是空的或錯的、呼叫端解析時自己丟例外」
與「依賴完全正常、呼叫端自己的程式有 bug」留下的 span 逐筆相同（fix/rca-silent-downstream 分支的
docs/RCA_SILENT_DOWNSTREAM.md、tests/test_rca_silent_downstream.py 有證明），只看 span 必然在其中一種指錯。
所以這一級的意思是「根因未確認，症狀在這裡」——不是「下游有問題」，也不把候選換成下游。
反過來，失敗的請求一個依賴都還沒呼叫到（在入口就崩潰、或它根本沒有依賴）時，沒有任何回應內容可以怪，維持 confirmed。

MIN_AFTER_DEPENDENCY_SHARE = 0.5 的來源：「過半」——不到一半代表它大部分的失敗跟依賴的回應無關。不是從考卷調出來的；
只在合成 span（tests/test_rca_conclusion_type.py）上驗過，沒有拿真實資料或留出考卷驗證，未驗證泛化。

死區（tests/test_rca_conclusion_type.py 留有測試）：
  a. 依賴回空、呼叫端除了出錯還因此變慢（重試、退避、走備援計算讓自身耗時過了顯著門檻）：第 1 點不成立，仍是 confirmed
  b. 呼叫端真的是自己的 bug、但出錯前呼叫過依賴：會被標成 symptom_location（保守：寧可說未確認）
  c. 訊息消費者處理訊息失敗：訊息內容一樣看不到，但這裡只看同步依賴；evidence 另外用它自己的 mq_suspect 旗標處理，llm 沒有
"""
from __future__ import annotations

from datetime import datetime

from aiops.engine.context import SpanRecord
from aiops.stages.rca.spans import ServiceWindowStats, own_failures_after_dependency

MIN_AFTER_DEPENDENCY_SHARE = 0.5


def symptom_only(entity: str, before: ServiceWindowStats | None, during: ServiceWindowStats | None,
                 spans: list[SpanRecord], start: datetime, end: datetime, *, min_self_ms: float = 5.0,
                 min_self_ratio: float = 0.2, min_error_increase: float = 0.05) -> str:
    """entity 在 [start, end) 是不是只有症狀位置。是就回說明文字（給 ctx.rca_conclusion_reason），不是或判不出來回空字串。
    before／during 是它在事件前、事件期間的 window_stats；門檻預設值跟 rca.root_cause.evidence 的預設相同。"""
    if before is None or during is None:
        return ""
    self_inc = during.self_ms_per_request - before.self_ms_per_request
    if self_inc >= max(min_self_ms, before.self_ms_per_request * min_self_ratio):
        return ""
    if during.own_error_rate - before.own_error_rate < min_error_increase:
        return ""
    failed, after = own_failures_after_dependency(spans, start, end).get(entity, (0.0, 0.0))
    if not failed or after / failed < MIN_AFTER_DEPENDENCY_SHARE:
        return ""
    return (f"{entity} 只有「自己出錯」這個症狀（自身錯誤率 {before.own_error_rate:.0%}→{during.own_error_rate:.0%}，"
            f"自身耗時沒有明顯變化），而且它自己失敗的請求有 {after / failed:.0%} 是在依賴已經成功回應之後才失敗。"
            "trace 不記回應內容：分不出是它自己的程式錯，還是依賴回了成功但內容不對。根因未確認，"
            "請同時檢查它的錯誤日誌與它呼叫的依賴當時回了什麼")
