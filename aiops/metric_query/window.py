"""時間窗口對齊：起訖都對齊到 step 的整數倍（UTC），不同資料源取到的點才會落在同一組時間格上。"""
from datetime import datetime, timezone


def to_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def align_window(start: datetime, end: datetime, step_seconds: int) -> tuple[datetime, datetime]:
    """start 往下取整、end 往下取整；對齊後若重疊成空窗，至少保留一個 step。"""
    if step_seconds <= 0:
        raise ValueError("step_seconds 必須大於 0")
    s, e = int(to_utc(start).timestamp()), int(to_utc(end).timestamp())
    s -= s % step_seconds
    e -= e % step_seconds
    if e <= s:
        e = s + step_seconds
    return datetime.fromtimestamp(s, tz=timezone.utc), datetime.fromtimestamp(e, tz=timezone.utc)
