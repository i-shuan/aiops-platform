"""訊息佇列（非同步邊）在 RCA 報告裡的共用文字與判準：根因算子、Trace 佐證段落、建議處置都用同一份。純函式。

發布時間（T_pub）與延遲怎麼從 span 還原在 engine/mq.py（撈取、根因、拓撲沉澱三邊共用）；這裡只管「怎麼講給人聽」。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from aiops.engine.context import MqEdge, SpanRecord
from aiops.stages.rca.spans import ServiceWindowStats, window_stats


def lag_text(ms: float | None) -> str:
    """延遲的人話：不到 1 秒印 ms，不到 2 分鐘印秒，其餘印分鐘。None（沒有樣本）印「—」。"""
    if ms is None:
        return "—"
    if ms < 1000:
        return f"{ms:.0f} ms"
    if ms < 120_000:
        return f"{ms / 1000:.1f} 秒"
    return f"{ms / 60000:.1f} 分鐘"


def window_text(start: datetime, end: datetime) -> str:
    return f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')}"


def is_backlog(edge: MqEdge, min_lag_seconds: float) -> bool:
    """這條邊算不算積壓：事件期間延遲 P95 >= min_lag_seconds，而且比事件前至少多一倍（事件前沒有樣本就只看絕對值）。
    本來就固定延遲 20 分鐘的佇列（排程批次、delay queue）不算積壓。"""
    if edge.lag_ms_p95 < min_lag_seconds * 1000:
        return False
    return edge.lag_ms_p95_before is None or edge.lag_ms_p95 >= 2 * edge.lag_ms_p95_before


def producer_windows(spans: list[SpanRecord], edge: MqEdge) -> tuple[ServiceWindowStats | None, ServiceWindowStats | None]:
    """生產者在 (發布時窗之前等長的一段, 發布時窗) 的統計：根因算子判斷它當時有沒有異常、佐證段落印數字，用同一份。
    不知道生產者是誰、或時窗長度是 0（只有一則訊息又沒留緩衝）回 (None, None)；那段時間沒有它的入口 span 則該項是 None。"""
    width = edge.pub_end - edge.pub_start
    if not edge.producer or width <= timedelta(0):
        return None, None
    return (window_stats(spans, edge.pub_start - width, edge.pub_start).get(edge.producer),
            window_stats(spans, edge.pub_start, edge.pub_end).get(edge.producer))


def async_attribution(edge: MqEdge, role: str, min_lag_seconds: float, moved_error: float = 0.0,
                      suspect_payload: bool = False) -> dict[str, Any]:
    """RootCandidate.attribution["async"]：role = consumer（候選是消費者）／producer（候選是發訊息的那一方）。
    moved_error：訊息夾帶失敗轉記的自身錯誤率（消費者 = 轉出去多少，生產者 = 承接多少）；沒有轉就不帶這個鍵。
    suspect_payload：消費者處理這批訊息失敗、回頭看生產者卻沒有任何症狀——trace 看不到訊息內容，分不出是消費者的程式
    還是生產者發的內容有問題（已知死區）；建議處置據此多開一條「抽查失敗訊息」。只有 True 才帶這個鍵。
    lag_text／pub_window_text 是同一組數字的人話版（「20.0 分鐘」「12:00–12:20」）：建議處置與三行摘要直接引用，
    三行摘要的數字守門（文字裡的數字要在 context 找得到）才對得上——lag_ms 是毫秒，沒有人會照著寫 1200000。"""
    before = edge.lag_ms_p95_before
    out: dict[str, Any] = {
        "role": role, "queue": edge.queue, "producer": edge.producer, "consumer": edge.consumer,
        "lag_ms": {"before": None if before is None else round(before, 1), "during": round(edge.lag_ms_p95, 1)},
        "lag_text": {"before": lag_text(before), "during": lag_text(edge.lag_ms_p95)},
        "pub_window": [edge.pub_start.isoformat(), edge.pub_end.isoformat()],
        "pub_window_text": window_text(edge.pub_start, edge.pub_end),
        "linked_by": edge.linked_by, "messages": edge.messages, "backlog": is_backlog(edge, min_lag_seconds),
    }
    if moved_error > 0:
        out["moved_error"] = round(moved_error, 4)
    if suspect_payload:
        out["suspect_payload"] = True
    return out


def edge_line(edge: MqEdge) -> str:
    """Trace 佐證段落的一行：誰發給誰、幾則、延遲怎麼變、這批訊息是什麼時候發的、發布時間怎麼來的。"""
    how = {"parent": "消費者的 parent 指到發送 span", "link": "span link", "age": "只有 message.age，不知道是誰發的"}[edge.linked_by]
    return (f"訊息佇列 {edge.queue or '（佇列名稱未知）'}：{edge.producer or '（生產者未知）'} → {edge.consumer}，{edge.messages} 則；"
            f"延遲 P95 {lag_text(edge.lag_ms_p95_before)}→{lag_text(edge.lag_ms_p95)}；"
            f"這批訊息的發布時窗 {window_text(edge.pub_start, edge.pub_end)}（{how}）")
