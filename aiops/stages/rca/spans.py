"""RCA 用的 span 統計（純函式，不碰資料源與 DB）：自身耗時、入口請求數、錯誤。

輸入是 ingestion.incident_traces 這一輪撈回來的 ctx.rca_spans（以整條 trace 為單位抽樣，父子接得起來）。

- 自身耗時（self time）= span 耗時 − 直接子 span 區間的「聯集」長度。每個子 span 的 [開始, 開始+耗時] 先夾進父 span
  的區間，排序後合併重疊：asyncio.gather／多執行緒並行打兩個下游（同一起點、各 60 ms、父 100 ms）自身是 40 ms，
  不是以前「耗時減加總」的 −20 再夾成 0（並行的服務會被當成「全部時間都在等下游」，自身耗時被雙重扣光）。
  依序呼叫的子 span 聯集 = 加總，數字跟以前一樣。
  例外：span.type = app 的子 span（服務自己的程式碼，跑在請求自己的執行緒上）一律「加總」、不參與聯集——
  它跟同一個請求的 I/O 是先後關係，不是並行。真實 11/3 資料的 app span 從 0 開始、外部呼叫在 2～5 ms 處開始
  （時間戳是毫秒截斷、app span 包住了發出呼叫前後的程式時間），照字面聯集會把 4～7 ms 重疊算成「自身」，
  自身耗時反而比總耗時還大；並行（asyncio.gather、執行緒池）只發生在 I/O 子 span（db、external、cache…）之間。
  「ord-svc 總共 2 秒、其中 1.9 秒在等 cart-api」-> ord-svc 自身 0.1 秒、cart-api 自身 1.9 秒。
- 時鐘偏移：子 span 有一段落在父 span 區間外（開始早於父、或結束晚於父；不同主機的時鐘不同步、APM 取樣誤差），
  超出的部分不扣（超出不到 1 ms 是毫秒截斷，照扣）；這種父 span 記成「時鐘偏移」，彙總在 ServiceWindowStats.clock_skew_spans，報告段落有數字才印。
  max(0, …) 保留當最後的保護（聯集夾在父區間內，理論上不會負）。
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
- 非同步邊（訊息佇列）：消費者 transaction（SpanRecord.consumer = True）的 parent.id 指到生產者的 send span，
  但它是「之後某個時間才被另一個行程處理」，不是 send span 等它做完。所以它不算任何 span 的同步子 span
  （_children_index 只收同步子 span），三件事跟著對：
  (1) send span 仍是葉節點 -> 發布訊息花的時間算「等 kafka／rabbitmq」這個資源，不會因為底下掛了消費者就消失；
  (2) 消費者的區間不從 send span 扣、也不算時鐘偏移（晚 20 分鐘處理是佇列延遲，不是兩台主機的時鐘不同步）；
  (3) 消費失敗不算生產者「下游失敗被拖累」——生產者早就回應了。
  消費者一律是入口（即使生產者跟它同一個服務）。它的呼叫者記成 MQ_CALLER_PREFIX + 生產者服務（mq:order-api）：
  跟同步呼叫者分開，流量放大的回溯不會沿著它往上走（訊息量變多不等於此刻被上游打爆，要看的是發布時窗，見 engine/mq.py）。
  還原不出生產者（parent 與 links 都不在樣本裡）記成 UNKNOWN_CALLER。
- 取樣還原：每個 span 帶 weight（SpanRecord.weight：APM representative_count、OTel tracestate 的 adjusted count，
  沒有取樣資訊就是 1）。取樣後的 span 數不是實際請求數，而且常常不是均勻取樣（尾端取樣會把錯誤 trace 全留、正常的只留一成），
  所以「多少比例、每請求多少、流量多少」一律加權：每請求自身耗時、平均總耗時、錯誤率、資源等待用加權平均，
  inbound_callers（流入流量、呼叫邊、扇出）數加權後的請求數，ServiceWindowStats.weighted_requests 是估計的實際請求數。
  「樣本夠不夠」仍看實際撈到幾筆：requests（入口 span 數）、wait_failures（失敗的資源 span 筆數）不加權——
  看到 5 筆、每筆代表 4 個請求，統計上仍然只有 5 筆證據，不能因為權重而通過 min_requests 之類的門檻。
  全部權重是 1 時，每個數字跟加權之前一字不差。
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable

from aiops.engine.context import SpanRecord
from aiops.engine.types import as_utc

EXTERNAL_CALLER = ""  # 沒有父 span：外部打進來的請求
UNKNOWN_CALLER = "?"  # 有父 span 但父不在樣本裡：不知道是誰叫的，不能當成外部入口
MQ_CALLER_PREFIX = "mq:"  # 透過訊息佇列送來的：mq:<生產者服務>。不是同步呼叫者
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
    clock_skew_spans: int = 0  # 子 span 超出父 span 區間（時鐘偏移）、超出部分沒扣的 span 筆數
    weighted_requests: float = 0.0  # 取樣還原後估計的入口請求數（Σ weight）；沒取樣時 = requests


def resource_label(span: SpanRecord) -> str:
    """資源名稱：destination.service.resource（tax-gw:443、redis），沒有就用 subtype（http、redis），再沒有就用 type。"""
    return span.resource or span.subtype or span.kind


def resource_kind(span: SpanRecord) -> str:
    """報告用的種類字樣：external/http、db/redis；只有一層就印那一層。"""
    return "/".join(p for p in (span.kind, span.subtype) if p)


def _children_index(spans: Iterable[SpanRecord]) -> dict[str, list[SpanRecord]]:
    """parent span_id -> 它的「同步」子 span。訊息消費者（consumer）不收：它是非同步處理，父 span 沒有在等它。
    自身耗時的扣除、時鐘偏移、資源葉節點、下游連帶失敗都從這裡拿子 span，所以非同步邊在這一處剪開就全部一致。"""
    children: dict[str, list[SpanRecord]] = defaultdict(list)
    for s in spans:
        if s.parent_id and not s.consumer:
            children[s.parent_id].append(s)
    return children


def producer_service(span: SpanRecord, service_of: dict[str, str]) -> str | None:
    """消費者 transaction 的訊息是哪個服務發的：先看 parent.id（單筆消費），再看 span.links（批次消費）。
    都不在樣本裡回 None。"""
    if span.parent_id and span.parent_id in service_of:
        return service_of[span.parent_id]
    for _, linked in span.links:
        if linked in service_of:
            return service_of[linked]
    return None


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


# 子 span 超出父區間多少才算時鐘偏移：資料源給的開始時間是 @timestamp（毫秒精度，次毫秒被截掉），父子各自截斷就可能
# 差到 1 ms，那不是偏移。超出不到這麼多：照以前整段扣、不夾；兄弟 span 重疊不到這麼多：當成依序（不是並行）
CLOCK_SKEW_TOLERANCE_MS = 1.0


def _offset_ms(ts: datetime, origin: datetime) -> float:
    """相對父 span 起點的毫秒數（用 timedelta 算，微秒精確；直接用 epoch 毫秒相減會掉精度）。"""
    return (as_utc(ts) - origin) / timedelta(milliseconds=1)


def self_times_detail(spans: Iterable[SpanRecord]) -> tuple[dict[str, float], frozenset[str]]:
    """(span_id -> 自身耗時 ms, 有時鐘偏移的 span_id)。
    自身 = 耗時 − app 子 span（夾進父區間）的長度加總 − 其他子 span 區間（夾進父區間）的聯集長度，下限 0。"""
    spans = list(spans)
    children = _children_index(spans)
    out: dict[str, float] = {}
    skewed: set[str] = set()
    for s in spans:
        kids = children.get(s.span_id)
        if not kids:
            out[s.span_id] = max(0.0, s.duration_ms)
            continue
        origin, hi = as_utc(s.ts), s.duration_ms  # 父區間 = [0, 耗時]
        intervals, app_ms = [], 0.0
        for c in kids:
            c_lo = _offset_ms(c.ts, origin)
            c_hi = c_lo + c.duration_ms
            if c_lo < -CLOCK_SKEW_TOLERANCE_MS or c_hi > hi + CLOCK_SKEW_TOLERANCE_MS:
                skewed.add(s.span_id)  # 真的超出：超出的部分不扣
                c_lo, c_hi = max(c_lo, 0.0), min(c_hi, hi)
            if c_hi <= c_lo:
                continue
            if c.kind == "app":
                app_ms += c_hi - c_lo  # 程式自己的時間：跟 I/O 先後發生，加總
            else:
                intervals.append((c_lo, c_hi))
        covered, cur_lo, cur_hi = 0.0, 0.0, None
        for a, b in sorted(intervals):
            # 重疊不到容忍值（毫秒截斷造成的假重疊）當成依序：依序呼叫的子 span 聯集 = 加總，跟以前一字不差
            if cur_hi is None or a > cur_hi - CLOCK_SKEW_TOLERANCE_MS:
                if cur_hi is not None:
                    covered += cur_hi - cur_lo
                cur_lo, cur_hi = a, b
            else:
                cur_hi = max(cur_hi, b)
        if cur_hi is not None:
            covered += cur_hi - cur_lo
        out[s.span_id] = max(0.0, s.duration_ms - app_ms - covered)
    return out, frozenset(skewed)


def self_times(spans: Iterable[SpanRecord]) -> dict[str, float]:
    """span_id -> 自身耗時（ms）。見 self_times_detail。"""
    return self_times_detail(spans)[0]


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


def own_failures_after_dependency(spans: list[SpanRecord], start: datetime, end: datetime) -> dict[str, tuple[float, float]]:
    """[start, end) 內每個服務：(自己失敗的入口請求數, 其中「失敗之前已經同步呼叫過依賴、而且那些呼叫都成功」的請求數)。
    自己失敗 = 入口 span 失敗、子孫沒有任何失敗（跟 window_stats 的 own_error_rate 同一個定義，所以子孫裡的呼叫必然全部成功）。
    依賴 = 同步子孫裡屬於別的服務的 span，或資源葉節點（db、external、cache…）。兩個數字都是取樣加權後的請求數。
    給結論等級判準用（stages/rca/conclusion.py）：拿到成功回應之後才失敗的請求，trace 分不出是自己的錯還是回應內容不對。"""
    below = _subtree_failed(spans)
    children = _children_index(spans)
    service_of = {s.span_id: s.service for s in spans}
    lo, hi = as_utc(start), as_utc(end)

    def called_dependency(root: SpanRecord) -> bool:
        stack, seen = list(children.get(root.span_id, [])), {root.span_id}
        while stack:
            c = stack.pop()
            if c.span_id in seen:
                continue
            seen.add(c.span_id)
            if c.service != root.service or is_resource_leaf(c, children):
                return True
            stack.extend(children.get(c.span_id, []))
        return False

    out: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])
    for s in spans:
        if not (lo <= as_utc(s.ts) < hi) or not s.error or below.get(s.span_id, False):
            continue
        if not (s.parent_id is None or service_of.get(s.parent_id) != s.service or s.consumer):
            continue  # 不是入口
        out[s.service][0] += s.weight
        out[s.service][1] += s.weight * called_dependency(s)
    return {k: (v[0], v[1]) for k, v in out.items()}


def window_stats(spans: list[SpanRecord], start: datetime, end: datetime) -> dict[str, ServiceWindowStats]:
    """[start, end) 內的 span，依服務彙總。自身耗時用全部 span 算（含視窗邊界外的子 span 也算進扣除）。"""
    selfs, skewed = self_times_detail(spans)
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
        w = s.weight
        own = selfs.get(s.span_id, 0.0)
        a["self_ms"] += own * w
        a["clock_skew"] += s.span_id in skewed
        if is_resource_leaf(s, children):
            label = resource_label(s)
            waits[s.service][label] += own * w
            wait_fail[s.service][label] += int(bool(s.error))  # 筆數（證據量），不加權
        else:
            a["app_ms"] += own * w
        # 入口 span：沒有父、父在別的服務，或是訊息消費者（生產者跟它同一個服務也算一次獨立的處理）
        if s.parent_id is None or service_of.get(s.parent_id) != s.service or s.consumer:
            a["requests"] += 1
            a["weighted"] += w
            a["total_ms"] += s.duration_ms * w
            a["errors"] += bool(s.error) * w
            a["own_errors"] += (bool(s.error) and not below.get(s.span_id, False)) * w
    out = {}
    for service, a in acc.items():
        n, wn = a["requests"], a["weighted"]
        if not n:
            continue
        out[service] = ServiceWindowStats(service, int(n), a["self_ms"] / wn, a["total_ms"] / wn,
                                          a["own_errors"] / wn, a["errors"] / wn, a["app_ms"] / wn,
                                          {k: v / wn for k, v in waits[service].items()}, dict(wait_fail[service]),
                                          int(a["clock_skew"]), _count(wn))
    return out


def _count(v: float) -> int | float:
    """加權後的計數：整數就回 int（沒取樣、或權重都是整數時跟以前的型別一樣），不是整數才留小數。"""
    return int(v) if float(v).is_integer() else round(v, 3)


def sample_trace(spans: list[SpanRecord], start: datetime, end: datetime) -> SpanRecord | None:
    """事件期間最具代表性的入口 span：失敗優先、耗時次之。
    排序鍵：(1) 入口自己失敗（event.outcome=failure） (2) HTTP 狀態碼 >= 500 (3) 子樹裡有失敗的 span (4) 耗時由大到小。
    outcome 是 APM 的權威失敗旗標；狀態碼補 outcome 缺漏的資料源；「子樹失敗但入口成功」是被吞掉的下游錯誤；
    最後才看耗時——以前只看耗時，真實資料挑到的是 244 ms／200 的正常慢請求，真正的 500 排第二。"""
    lo, hi = as_utc(start), as_utc(end)
    # 訊息消費者也是入口：它的 parent 是幾分鐘前別的服務的 send span，不在事件時窗裡，只看「沒有父」會永遠挑不到它
    roots = [s for s in spans if (s.parent_id is None or s.consumer) and lo <= as_utc(s.ts) < hi]
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
    """[start, end) 內每個服務的入口 span 是誰呼叫的：callee -> {caller: 次數}。次數是取樣還原後的請求數（Σ weight），
    沒取樣時就是 span 數（int，跟以前一樣）。
    沒有父 span 記成 EXTERNAL_CALLER；有父但父不在樣本裡記成 UNKNOWN_CALLER（孤兒，不是外部入口）。
    訊息消費者記成 MQ_CALLER_PREFIX + 生產者服務；還原不出生產者記成 UNKNOWN_CALLER（它絕對不是外部打進來的同步請求）。"""
    service_of = {s.span_id: s.service for s in spans}
    lo, hi = as_utc(start), as_utc(end)
    out: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for s in spans:
        if not (lo <= as_utc(s.ts) < hi):
            continue
        if s.consumer:
            producer = producer_service(s, service_of)
            out[s.service][MQ_CALLER_PREFIX + producer if producer else UNKNOWN_CALLER] += s.weight
        elif s.parent_id is None:
            out[s.service][EXTERNAL_CALLER] += s.weight
        elif s.parent_id not in service_of:
            out[s.service][UNKNOWN_CALLER] += s.weight
        elif service_of[s.parent_id] != s.service:
            out[s.service][service_of[s.parent_id]] += s.weight
    return {k: {c: _count(n) for c, n in v.items()} for k, v in out.items()}


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
