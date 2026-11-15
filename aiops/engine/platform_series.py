"""平台自己的紀錄當成時序（source_type = platform）：給看門狗管線（pipelines/watchdog/）監控 AIOps 平台自己。

資料從哪來：StorageHooks.load_target 把查詢對應的原始紀錄（時間、狀態）載進 ctx.platform_rows，ingestion.fetch 用這裡的
bucket_counts 換成「每格幾筆」的序列。跟 source_type = trace 同一種做法：sources/ 不准碰 storage，平台自己的資料不走外部資料源。
這個檔是純函式（storage 與 stages 都可以 import engine）。

查詢語法：<表>/<鍵>/<欄位>
  runs/<管線名稱>/rounds      這條管線每格跑了幾輪（pipeline_runs 的列數；排程停了就是 0）
  runs/<管線名稱>/failed      每格整輪失敗幾輪（status = failed）
  runs/<管線名稱>/not_ok      每格不是成功的輪數（degraded + failed）
  alerts/<來源>/alerts        每格收到幾筆外部告警（不含心跳）
  alerts/<來源>/heartbeats    每格收到幾筆心跳告警（團隊 Grafana 那條一直觸發的告警，見 docs/EXTERNAL_ALERTS.md）
  alerts/<來源>/pushes        每格收到幾筆（告警＋心跳）
  <來源> 寫 * = 所有來源加總。
計數是資料，0 也是資料：查得到的時間範圍內沒有任何一筆，序列就是一排 0，不是「沒有資料」。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

PLATFORM_SOURCE = "platform"
FIELDS: dict[str, frozenset[str]] = {
    "runs": frozenset({"rounds", "failed", "not_ok"}),
    "alerts": frozenset({"alerts", "heartbeats", "pushes"}),
}
ALL_SOURCES = "*"
HEARTBEAT_STATUS = "heartbeat"  # external_anomalies.status：心跳告警（不進任何管線，只給看門狗數）
_QUERY = re.compile(r"([^/\s]+)/([^/\s]+)/([^/\s]+)")  # <表>/<鍵>/<欄位>，每段不准空白與斜線


@dataclass(frozen=True)
class PlatformQuery:
    table: str
    key: str
    field: str


def split_platform_query(query: str) -> tuple[str, str, str] | None:
    """<表>/<鍵>/<欄位> 拆成三段；格式不對回 None（不檢查表與欄位名稱，那是 parse_platform_query 的事）。"""
    m = _QUERY.fullmatch(query.strip())
    return (m.group(1), m.group(2), m.group(3)) if m else None


def parse_platform_query(query: str) -> PlatformQuery:
    parts = split_platform_query(query)
    if parts is None:
        raise ValueError(f"platform 查詢要寫成 <表>/<鍵>/<欄位>（例如 runs/metrics_only/failed），不是 {query!r}")
    table, key, field = parts
    if table not in FIELDS:
        raise ValueError(f"platform 查詢的表只能是 {sorted(FIELDS)}，不是 {table!r}")
    if field not in FIELDS[table]:
        raise ValueError(f"{table} 的欄位只能是 {sorted(FIELDS[table])}，不是 {field!r}")
    if table == "runs" and key == ALL_SOURCES:
        raise ValueError("runs 要指定管線名稱（不支援 *）")
    return PlatformQuery(table, key, field)


def selects(q: PlatformQuery, status: str) -> bool:
    """這一筆紀錄（依它的狀態）算不算進這個欄位。"""
    if q.table == "runs":
        return q.field == "rounds" or (q.field == "failed" and status == "failed") or (q.field == "not_ok" and status != "ok")
    if q.field == "heartbeats":
        return status == HEARTBEAT_STATUS
    return q.field == "pushes" or status != HEARTBEAT_STATUS


def bucket_counts(times: list[datetime], start: datetime, end: datetime, step_seconds: int, now: datetime) -> pd.DataFrame:
    """[start, end) 切成 step 秒一格，數每格有幾筆 -> DataFrame(ts, value)，ts 是格子的開始（UTC）。
    只留已經完整過完的格子（格子結束 <= now）：還在進行中的那一格筆數不完整，留著會讓「降到 0」誤報。"""
    step = timedelta(seconds=step_seconds)
    epoch = int(start.timestamp())
    first = datetime.fromtimestamp(epoch - epoch % step_seconds, tz=start.tzinfo)
    grid = []
    t = first
    while t < end and t + step <= now:
        grid.append(t)
        t += step
    counts = dict.fromkeys(grid, 0)
    for ts in times:
        offset = int((ts - first).total_seconds()) // step_seconds
        if offset < 0:
            continue
        bucket = first + offset * step
        if bucket in counts:
            counts[bucket] += 1
    return pd.DataFrame({"ts": pd.to_datetime(list(counts), utc=True), "value": [float(v) for v in counts.values()]})
