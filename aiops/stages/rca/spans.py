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
- 資源等待：葉節點（沒有子 span）而且 span.type 不是 app／空的 span（db、external、cache、messaging…），是這個服務在等
  一個沒被追蹤成服務的東西（Oracle、Redis、第三方 API）。它的 service.name 仍是呼叫它的服務，所以自身耗時會把這段算進去
  ——真實資料裡 bill-svc 的「自身」有 78% 其實是等 tax-gw。self_ms_per_request 維持含等待（排名語意不變：
  「這個服務的請求在它自己身上變慢、不是在等下游服務」），另外拆出 app_ms_per_request（程式自己）與 wait_ms_per_request
  （每個資源各多少），讓報告說得出「該查的是它等的東西」。上游沒裝 APM 的服務會被當成資源（external 葉節點），
  報告寫「等待 ord-svc:8080」語意沒錯，但它其實是個服務。
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from aiops.engine.context import SpanRecord
from aiops.engine.types import as_utc

EXTERNAL_CALLER = ""  # 沒有父 span：外部打進來的請求
UNKNOWN_CALLER = "?"  # 有父 span 但父不在樣本裡：不知道是誰叫的，不能當成外部入口
_APP_KINDS = ("", "app")  # 空 = 入口 transaction；app = 服務自己的程式；其他（db、external、cache、messaging…）都是等外部資源


@dataclass(frozen=True)
class ServiceWindowStats:
    service: str
    requests: int  # 入口 span 數
    self_ms_per_request: float  # 自身耗時（含等外部資源）：排名用的，語意不變
    total_ms_mean: float  # 入口 span 的平均總耗時
    own_error_rate: float  # 自己造成的失敗比例（不含下游連帶）
    error_rate: float  # 入口 span 失敗比例（含下游連帶）
    app_ms_per_request: float = 0.0  # 自身耗時扣掉資源等待：程式自己的時間（沒有資源葉節點時 = self_ms_per_request）
    wait_ms_per_request: dict[str, float] = field(default_factory=dict)  # 資源 -> 每請求等待 ms
    wait_failures: dict[str, int] = field(default_factory=dict)  # 資源 -> 失敗的資源 span 筆數


def resource_label(span: SpanRecord) -> str:
    """資源名稱：destination.service.resource（tax-gw:443、redis），沒有就用 subtype（http、redis），再沒有就用 type。"""
    return span.resource or span.subtype or span.kind


def resource_kind(span: SpanRecord) -> str:
    """報告用的種類字樣：external/http、db/redis；只有一層就印那一層。"""
    return "/".join(p for p in (span.kind, span.subtype) if p)


def _children_index(spans: Iterable[SpanRecord]) -> dict[str, list[SpanRecord]]:
    children: dict[str, list[SpanRecord]] = defaultdict(list)
    for s in spans:
        if s.parent_id:
            children[s.parent_id].append(s)
    return children


def is_resource_leaf(span: SpanRecord, children: dict[str, list[SpanRecord]]) -> bool:
    """葉節點（沒有子 span）而且 span.type 不是 app／空：等外部資源（db、external、cache、messaging…）。
    服務對服務的呼叫不會進來：下游 transaction 是它的子 span，所以它不是葉節點。
    用黑名單（app／空）而不是白名單（db、external）：OTel 還有 cache、messaging、storage，白名單會漏。
    沒有 kind 的資料（mock、舊索引）一律當程式自己的時間，統計跟以前一字不差。"""
    return span.kind not in _APP_KINDS and not children.get(span.span_id)


def resource_kinds(spans: Iterable[SpanRecord]) -> dict[str, str]:
    """資源名稱 -> 種類字樣（tax-gw:443 -> external/http）。算子理由與報告段落共用。"""
    spans = list(spans)
    children = _children_index(spans)
    return {resource_label(s): resource_kind(s) for s in spans if is_resource_leaf(s, children)}


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
    children = _children_index(spans)
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
    children = _children_index(spans)
    service_of = {s.span_id: s.service for s in spans}
    lo, hi = as_utc(start), as_utc(end)
    acc: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    waits: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    wait_fail: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for s in spans:
        if not (lo <= as_utc(s.ts) < hi):
            continue
        a = acc[s.service]
        own = selfs.get(s.span_id, 0.0)
        a["self_ms"] += own
        if is_resource_leaf(s, children):
            label = resource_label(s)
            waits[s.service][label] += own
            wait_fail[s.service][label] += int(bool(s.error))
        else:
            a["app_ms"] += own
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
                                          a["own_errors"] / n, a["errors"] / n, a["app_ms"] / n,
                                          {k: v / n for k, v in waits[service].items()}, dict(wait_fail[service]))
    return out


def sample_trace(spans: list[SpanRecord], start: datetime, end: datetime) -> SpanRecord | None:
    """事件期間最具代表性的入口 span：失敗優先、耗時次之。
    排序鍵：(1) 入口自己失敗（event.outcome=failure） (2) HTTP 狀態碼 >= 500 (3) 子樹裡有失敗的 span (4) 耗時由大到小。
    outcome 是 APM 的權威失敗旗標；狀態碼補 outcome 缺漏的資料源；「子樹失敗但入口成功」是被吞掉的下游錯誤；
    最後才看耗時——以前只看耗時，真實資料挑到的是 244 ms／200 的正常慢請求，真正的 500 排第二。"""
    lo, hi = as_utc(start), as_utc(end)
    roots = [s for s in spans if s.parent_id is None and lo <= as_utc(s.ts) < hi]
    if not roots:
        return None
    below = _subtree_failed(spans)
    return max(roots, key=lambda s: (bool(s.error), s.status_code >= 500, below.get(s.span_id, False), s.duration_ms))


def sample_trace_reason(span: SpanRecord, spans: list[SpanRecord]) -> str:
    """範例 trace 為什麼被挑中（報告標題用）：「入口 500，tax-gw:443 失敗」；全成功時回空字串（標題退回「最慢」）。"""
    parts: list[str] = []
    if span.error or span.status_code >= 500:
        parts.append(f"入口 {span.status_code}" if span.status_code else "入口失敗")
    failed = sorted({resource_label(c) for c in spans if c.trace_id == span.trace_id and c is not span and c.error
                     and c.kind not in _APP_KINDS})
    if failed:
        parts.append(f"{'、'.join(failed)} 失敗")
    elif not span.error and span.status_code < 500 and _subtree_failed([s for s in spans if s.trace_id == span.trace_id]).get(span.span_id):
        parts.append("子 span 失敗")
    return "，".join(parts)


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
