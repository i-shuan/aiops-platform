"""RCA 用的 span 統計（純函式，不碰資料源與 DB）：自身耗時、入口請求數、錯誤。

輸入是 ingestion.incident_traces 這一輪撈回來的 ctx.rca_spans（以整條 trace 為單位抽樣，父子接得起來）。

- 自身耗時（self time）= span 耗時 − 它的直接子 span 耗時總和（下限 0）。子 span 平行時會多扣，所以下限設 0。
  「ord-svc 總共 2 秒、其中 1.9 秒在等 cart-api」-> ord-svc 自身 0.1 秒、cart-api 自身 1.9 秒。
- 入口 span（entry）：父 span 不在同一個服務（或沒有父 span）的 span，等於這個服務被呼叫一次。
  每個服務的「每請求自身耗時」= 這個服務所有 span 的自身耗時總和 ÷ 入口數。
- 呼叫者：沒有父 span 的是真正的外部入口（EXTERNAL_CALLER=""）；有父 span 但父不在樣本裡（撈取窗邊界、抽樣、
  上游沒裝 APM）的是孤兒（UNKNOWN_CALLER="?"），仍算一次請求，但不能說它「來自外部」。
- 自身錯誤：入口 span 失敗，而且它往下的子 span（同一條呼叫鏈）都沒有失敗，算這個服務自己的錯；
  下游失敗而連帶失敗的不算（那是被拖累）。
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from aiops.engine.context import SpanRecord
from aiops.engine.types import as_utc

EXTERNAL_CALLER = ""  # 沒有父 span：外部打進來的請求
UNKNOWN_CALLER = "?"  # 有父 span 但父不在樣本裡：不知道是誰叫的，不能當成外部入口


@dataclass(frozen=True)
class ServiceWindowStats:
    service: str
    requests: int  # 入口 span 數
    self_ms_per_request: float
    total_ms_mean: float  # 入口 span 的平均總耗時
    own_error_rate: float  # 自己造成的失敗比例（不含下游連帶）
    error_rate: float  # 入口 span 失敗比例（含下游連帶）


def self_times(spans: Iterable[SpanRecord]) -> dict[str, float]:
    """span_id -> 自身耗時（ms）。"""
    spans = list(spans)
    child_ms: dict[str, float] = defaultdict(float)
    for s in spans:
        if s.parent_id:
            child_ms[s.parent_id] += s.duration_ms
    return {s.span_id: max(0.0, s.duration_ms - child_ms.get(s.span_id, 0.0)) for s in spans}


def _subtree_failed(spans: list[SpanRecord]) -> dict[str, bool]:
    """span_id -> 它的子孫裡有沒有失敗的 span（不含自己）。"""
    children: dict[str, list[SpanRecord]] = defaultdict(list)
    for s in spans:
        if s.parent_id:
            children[s.parent_id].append(s)
    memo: dict[str, bool] = {}

    def failed_below(span_id: str) -> bool:
        if span_id not in memo:
            memo[span_id] = False  # 防環
            memo[span_id] = any(c.error or failed_below(c.span_id) for c in children.get(span_id, []))
        return memo[span_id]

    return {s.span_id: failed_below(s.span_id) for s in spans}


def window_stats(spans: list[SpanRecord], start: datetime, end: datetime) -> dict[str, ServiceWindowStats]:
    """[start, end) 內的 span，依服務彙總。自身耗時用全部 span 算（含視窗邊界外的子 span 也算進扣除）。"""
    selfs = self_times(spans)
    below = _subtree_failed(spans)
    service_of = {s.span_id: s.service for s in spans}
    lo, hi = as_utc(start), as_utc(end)
    acc: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for s in spans:
        if not (lo <= as_utc(s.ts) < hi):
            continue
        a = acc[s.service]
        a["self_ms"] += selfs.get(s.span_id, 0.0)
        if s.parent_id is None or service_of.get(s.parent_id) != s.service:  # 入口 span
            a["requests"] += 1
            a["total_ms"] += s.duration_ms
            a["errors"] += bool(s.error)
            a["own_errors"] += bool(s.error) and not below.get(s.span_id, False)
    out = {}
    for service, a in acc.items():
        n = a["requests"]
        if not n:
            continue
        out[service] = ServiceWindowStats(service, int(n), a["self_ms"] / n, a["total_ms"] / n,
                                          a["own_errors"] / n, a["errors"] / n)
    return out


def inbound_callers(spans: list[SpanRecord], start: datetime, end: datetime) -> dict[str, dict[str, int]]:
    """[start, end) 內每個服務的入口 span 是誰呼叫的：callee -> {caller: 次數}。
    沒有父 span 記成 EXTERNAL_CALLER；有父但父不在樣本裡記成 UNKNOWN_CALLER（孤兒，不是外部入口）。"""
    service_of = {s.span_id: s.service for s in spans}
    lo, hi = as_utc(start), as_utc(end)
    out: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for s in spans:
        if not (lo <= as_utc(s.ts) < hi):
            continue
        if s.parent_id is None:
            out[s.service][EXTERNAL_CALLER] += 1
        elif s.parent_id not in service_of:
            out[s.service][UNKNOWN_CALLER] += 1
        elif service_of[s.parent_id] != s.service:
            out[s.service][service_of[s.parent_id]] += 1
    return {k: dict(v) for k, v in out.items()}


def call_chain(spans: list[SpanRecord], trace_id: str) -> list[tuple[int, SpanRecord, float]]:
    """一條 trace 依實際 parent.id 還原的呼叫樹（深度優先）：[(深度, span, 自身耗時 ms)]。"""
    mine = [s for s in spans if s.trace_id == trace_id]
    selfs = self_times(mine)
    ids = {s.span_id for s in mine}
    children: dict[str | None, list[SpanRecord]] = defaultdict(list)
    for s in mine:
        children[s.parent_id if s.parent_id in ids else None].append(s)
    out: list[tuple[int, SpanRecord, float]] = []

    def walk(parent: str | None, depth: int, seen: frozenset) -> None:
        for s in sorted(children.get(parent, []), key=lambda x: as_utc(x.ts)):
            if s.span_id in seen:
                continue
            out.append((depth, s, selfs.get(s.span_id, 0.0)))
            walk(s.span_id, depth + 1, seen | {s.span_id})

    walk(None, 0, frozenset())
    return out
