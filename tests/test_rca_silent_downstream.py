"""下游靜默回錯內容（HTTP 200、耗時正常、內容是空的或錯的），呼叫端因此自己丟例外。

已知架構限制（docs/RCA_SILENT_DOWNSTREAM.md）：trace 與指標只記「誰花了多久、誰失敗」，不記回應內容。
這種故障在 span 上跟「呼叫端自己的程式有 bug」逐筆相同，rca.root_cause.evidence 只看得到呼叫端自身錯誤率上升，
會把呼叫端排第一。企業考卷 e1 的 E2 格 4/6 題因此「因果反指」，隨機考卷 v2 的 empty 家族 53%。
這裡不加任何規則去猜：兩種原因的 span 一樣，任何只看 span 的判準都必然在其中一種指錯。
"""
from datetime import datetime, timedelta, timezone

import pytest

from aiops.engine.context import PipelineContext, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident

T = datetime(2026, 11, 3, 14, 0, tzinfo=timezone.utc)
CALLER, CALLEE = "portal-web", "portal-api"


def world(cause: str, fail_share: int = 2, per_minute: int = 4, minutes=range(-15, 30)) -> list[SpanRecord]:
    """portal-web -> portal-api。事件期間每 fail_share 個請求有 1 個在 portal-web 失敗。
    cause 只是「真實世界發生了什麼」的標籤：
      downstream_empty  portal-api 回 200、耗時照舊、內容是空的，portal-web 解析時丟例外
      caller_bug        portal-api 回應完全正常，portal-web 自己新上的程式有 bug
    APM 不記回應內容，所以兩種原因產生的 span 完全一樣——這個函式刻意不讓 cause 影響任何一個欄位。"""
    assert cause in ("downstream_empty", "caller_bug")
    spans, n = [], 0
    for m in minutes:
        for k in range(per_minute):
            n += 1
            tid, ts = f"t{n}", T + timedelta(minutes=m, seconds=k * 13)
            failed = m >= 0 and n % fail_share == 0
            spans.append(SpanRecord(tid, f"{tid}w", None, CALLER, ts, 45.0, error=failed, status_code=500 if failed else 200))
            spans.append(SpanRecord(tid, f"{tid}a", f"{tid}w", CALLEE, ts + timedelta(milliseconds=5), 30.0, status_code=200))
    return spans


def rank(spans, entities):
    incident = Incident([AnomalyEvent(e, "errors", T + timedelta(minutes=m), 5.0) for e in entities for m in range(30)])
    ctx = PipelineContext(pipeline="rca", now=T + timedelta(minutes=30))
    ctx.incidents, ctx.incidents_built = [incident], True
    ctx.rca_spans = spans
    return OperatorRegistry.create("rca.root_cause.evidence", {}).rank(incident, ctx), ctx


def test_silent_bad_response_and_caller_bug_leave_identical_spans():
    """限制的根源：兩種原因在 trace 上不可分辨（不是門檻或演算法調得不好）。"""
    assert world("downstream_empty") == world("caller_bug")


@pytest.mark.xfail(strict=True, reason="已知架構限制：下游回 200 但內容錯。span 不記回應內容，跟呼叫端自己的 bug 逐筆相同，"
                                       "只看得到呼叫端自身錯誤率上升（docs/RCA_SILENT_DOWNSTREAM.md）。要解需要回應內容層的遙測"
                                       "（回應大小、回傳筆數）或多個互不相干的呼叫者同時出錯的旁證，現在都沒有")
def test_dead_zone_silent_bad_response_from_downstream_is_attributed_to_the_caller():
    ranked, _ = rank(world("downstream_empty"), (CALLER, CALLEE))
    assert ranked[0].entity == CALLEE


def test_silent_downstream_names_the_caller_by_its_own_errors_and_never_invents_evidence_against_the_callee():
    """死區的另一面：指不到下游，但也不准亂指。呼叫端照自己的症狀上榜；下游沒有任何症狀，分數是 0、沒有歸因。
    下游就算在事件裡（被收斂進同一張卡），也不會因為「它是被呼叫端」就被推上去。"""
    ranked, _ = rank(world("downstream_empty"), (CALLER, CALLEE))
    assert ranked[0].entity == CALLER and ranked[0].score > 0
    callee = next(c for c in ranked if c.entity == CALLEE)
    assert callee.score == 0 and not callee.dependency


def test_silent_downstream_with_only_the_caller_on_the_card_still_names_the_caller():
    """E2 實際的樣子：下游沒有症狀所以不在卡片上，候選只有呼叫端。"""
    ranked, _ = rank(world("downstream_empty"), (CALLER,))
    assert [c.entity for c in ranked] == [CALLER]
