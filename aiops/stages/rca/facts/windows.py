"""證據層共用的時間窗：事件期間與事件前（基準期）。跟 rca.root_cause.evidence 用同一套切法，新舊引擎的數字才對得起來。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from aiops.engine.context import PipelineContext
from aiops.engine.types import Incident, as_utc


@dataclass(frozen=True)
class Windows:
    base_lo: datetime   # 基準期起點（主撈取窗的開頭）
    start: datetime     # 事件開始 = 基準期終點
    end: datetime       # 事件結束 + 1 分鐘（最後一個異常點那一分鐘也算進去）

    @property
    def has_baseline(self) -> bool:
        return self.base_lo < self.start

    @property
    def base_minutes(self) -> float:
        return max((self.start - self.base_lo).total_seconds() / 60, 0.0)

    @property
    def incident_minutes(self) -> float:
        return (self.end - self.start).total_seconds() / 60


def windows(incident: Incident, ctx: PipelineContext) -> Windows:
    """事件期間 = [事件開始, 事件結束 + 1 分鐘)；基準期 = [主撈取窗開頭, 事件開始)。
    主撈取窗開頭用 ctx.rca_main_start（補撈過訊息生產者時，手上最早的 span 是幾十分鐘前的，不能拿來當基準起點）；
    沒有就用手上最早的 span；連 span 都沒有就沒有基準期（base_lo = start）。"""
    start, end = as_utc(incident.start), as_utc(incident.end) + timedelta(minutes=1)
    if ctx.rca_main_start:
        base_lo = as_utc(ctx.rca_main_start)
    elif ctx.rca_spans:
        base_lo = min(as_utc(s.ts) for s in ctx.rca_spans)
    else:
        base_lo = start
    return Windows(min(base_lo, start), start, end)
