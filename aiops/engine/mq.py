"""訊息佇列（非同步邊）的純函式：從 span 找出「誰發的訊息、什麼時候發的、等了多久才被消費」。

同步呼叫的因果在同一個時間窗裡；訊息佇列不是：消費者 12:20 處理到的訊息可能是生產者 12:00 發的，
要看生產者有沒有問題，得回到「這批訊息發布的那段時間」去看，而不是消費者出事的那段時間。

發布時間（T_pub）三個來源，依序：
  parent  消費者 transaction 的 parent 就是生產者的 send span（trace context 跟著訊息 header 傳）：T_pub = 那個 span 的時間
  link    批次消費：消費者是新的 trace，用 span.links 指回每一則訊息的 send span：T_pub = 被連結 span 的時間
  age     兩個都對不到，但消費者記了訊息年齡（message.age）：T_pub = 消費時間 − age；生產者是誰不知道（留空，不猜）
三個都沒有就是對不到（unresolved），不跨時窗。

放在 engine：撈取（ingestion）、根因（rca）與拓撲沉澱（control_plane）都要用，而它們彼此不能互相 import。
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from aiops.engine.context import MqEdge, SpanRecord
from aiops.engine.types import as_utc

LINKED_BY = ("parent", "link", "age")


@dataclass(frozen=True)
class Message:
    """一則被消費的訊息：哪個消費者入口處理的、誰發的（不知道就是空字串）、哪個佇列、什麼時候發的。"""

    consumer: SpanRecord
    producer: str
    queue: str
    published: datetime
    linked_by: str

    @property
    def lag_ms(self) -> float:
        return max((as_utc(self.consumer.ts) - self.published).total_seconds() * 1000.0, 0.0)


def percentile(values: Sequence[float], q: float) -> float:
    """線性內插分位數（q 在 0~1）。空的回 0。"""
    if not values:
        return 0.0
    ordered = sorted(values)
    pos = (len(ordered) - 1) * min(max(q, 0.0), 1.0)
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def consumer_entries(spans: Iterable[SpanRecord], entities: Iterable[str] | None,
                     start: datetime, end: datetime) -> list[SpanRecord]:
    """[start, end) 內的訊息消費者入口；entities 給了就只留這些服務的。依 (時間, span_id) 排序，重跑順序一樣。"""
    wanted = set(entities) if entities is not None else None
    found = [s for s in spans if s.consumer and (wanted is None or s.service in wanted) and start <= as_utc(s.ts) < end]
    return sorted(found, key=lambda s: (as_utc(s.ts), s.span_id))


def spread(items: list, limit: int) -> list:
    """超過 limit 就等距挑 limit 個（不是拿最前面的）：只拿最早的那幾筆，發布時窗會偏向積壓剛開始的時候。"""
    if limit <= 0 or len(items) <= limit:
        return list(items)
    step = len(items) / limit
    return [items[int(i * step)] for i in range(limit)]


def unresolved_trace_ids(spans: Iterable[SpanRecord], consumers: Iterable[SpanRecord]) -> list[str]:
    """這些消費者指向的生產者 span 還不在手上的，要去補撈的 trace id（去重、依消費者順序）。
    parent 不在：生產者那一段跟消費者同一個 trace，只是時間在撈取窗之前；link 不在：是另一條 trace。"""
    known = {s.span_id for s in spans}
    out: dict[str, None] = {}
    for c in consumers:
        if c.parent_id and c.parent_id not in known:
            out.setdefault(c.trace_id)
        for trace_id, span_id in c.links:
            if span_id not in known:
                out.setdefault(trace_id)
    return list(out)


def messages(spans: Iterable[SpanRecord], consumers: Iterable[SpanRecord]) -> tuple[list[Message], int]:
    """消費者入口 -> 它處理的訊息。回 (訊息清單, 對不到發布時間的消費者數)。
    一個批次消費者有幾個對得到的 link 就是幾則訊息。"""
    by_id = {s.span_id: s for s in spans}
    out: list[Message] = []
    unresolved = 0
    for c in consumers:
        parent = by_id.get(c.parent_id) if c.parent_id else None
        linked = [by_id[span_id] for _, span_id in c.links if span_id in by_id]
        if parent is not None:
            out.append(Message(c, parent.service, c.queue or parent.resource, as_utc(parent.ts), "parent"))
        elif linked:
            out += [Message(c, p.service, c.queue or p.resource, as_utc(p.ts), "link") for p in linked]
        elif c.age_ms is not None and c.age_ms >= 0:
            out.append(Message(c, "", c.queue, as_utc(c.ts) - timedelta(milliseconds=c.age_ms), "age"))
        else:
            unresolved += 1
    return out, unresolved


def age_p95_ms(consumers: Iterable[SpanRecord]) -> float | None:
    """消費者自己記的訊息年齡 P95（ms）；都沒記就是 None。補撈之前先用它判斷延遲有沒有超過上限。"""
    ages = [c.age_ms for c in consumers if c.age_ms is not None and c.age_ms >= 0]
    return percentile(ages, 0.95) if ages else None


def _grouped(msgs: Iterable[Message]) -> dict[tuple[str, str, str], list[Message]]:
    groups: dict[tuple[str, str, str], list[Message]] = defaultdict(list)
    for m in msgs:
        groups[(m.producer, m.consumer.service, m.queue)].append(m)
    return groups


def own_failed(spans: Iterable[SpanRecord]) -> set[str]:
    """失敗、而且同步子孫裡沒有任何失敗 span 的 span_id：這筆失敗是它自己造成的，不是下游連帶。
    跟 stages/rca/spans._subtree_failed 同一個定義（非同步的消費者不算子孫）；engine 不能 import stages，
    所以各寫一份，tests/test_rca_messaging.py 斷言兩邊一致。"""
    spans = list(spans)
    children: dict[str, list[SpanRecord]] = defaultdict(list)
    for s in spans:
        if s.parent_id and not s.consumer:
            children[s.parent_id].append(s)
    out: set[str] = set()
    for s in spans:
        if not s.error:
            continue
        stack, seen, below = list(children.get(s.span_id, ())), {s.span_id}, False
        while stack and not below:
            child = stack.pop()
            if child.span_id in seen:
                continue
            seen.add(child.span_id)
            if child.error:
                below = True
            else:
                stack.extend(children.get(child.span_id, ()))
        if not below:
            out.add(s.span_id)
    return out


def build_edges(spans: Sequence[SpanRecord], entities: Iterable[str], start: datetime, end: datetime,
                base_start: datetime, pad: timedelta) -> tuple[list[MqEdge], int, int]:
    """事件期間 [start, end) 被事件實體消費的訊息 -> 每條 (生產者, 消費者, 佇列) 一個 MqEdge。
    回 (邊, 事件期間消費者入口數, 其中對不到發布時間的入口數)。

    生產者時窗 = 這批訊息發布時間的 P5~P95，兩側各留 pad。不用 min~max：一兩則卡了很久的訊息會把時窗拉到幾小時前。
    lag_ms_p95_before = 同一條邊在事件前 [base_start, start) 的延遲 P95（事件前沒有這條邊的訊息就是 None）。
    failed = 這批訊息裡消費者「自己失敗」的入口數（own_failed；批次消費一個入口處理多則訊息只算一次）。
    邊依訊息數由多到少排。"""
    entities = set(entities)
    during = consumer_entries(spans, entities, start, end)
    msgs, unresolved = messages(spans, during)
    before = _grouped(messages(spans, consumer_entries(spans, entities, base_start, start))[0])
    own = own_failed(spans)
    edges = []
    for (producer, consumer, queue), group in _grouped(msgs).items():
        lags = [m.lag_ms for m in group]
        published = [m.published.timestamp() for m in group]
        origin = group[0].published.tzinfo
        lo = datetime.fromtimestamp(percentile(published, 0.05), tz=origin) - pad
        hi = datetime.fromtimestamp(percentile(published, 0.95), tz=origin) + pad
        by = max(LINKED_BY, key=lambda kind: sum(m.linked_by == kind for m in group))
        earlier = before.get((producer, consumer, queue))
        edges.append(MqEdge(producer, consumer, queue, len(group), percentile(lags, 0.5), percentile(lags, 0.95),
                            lo, hi, by,
                            lag_ms_p95_before=percentile([m.lag_ms for m in earlier], 0.95) if earlier else None,
                            failed=len({m.consumer.span_id for m in group if m.consumer.span_id in own})))
    edges.sort(key=lambda e: (-e.messages, e.producer, e.consumer, e.queue))
    return edges, len(during), unresolved


def async_lags(spans: Sequence[SpanRecord]) -> dict[tuple[str, str], tuple[float, int]]:
    """拓撲沉澱用：這批 span 裡看得到的非同步邊 (生產者, 消費者) -> (延遲 P95 毫秒, 訊息數)。
    只收對得到生產者的（parent 或 link 在這批 span 裡）；生產者與消費者是同一個服務的不算邊。
    span 只需要 span_id／parent_id／service／ts／consumer／links 這幾個屬性。"""
    by_id = {s.span_id: s for s in spans if s.span_id}
    lags: dict[tuple[str, str], list[float]] = defaultdict(list)
    for c in spans:
        if not getattr(c, "consumer", False):
            continue
        sources = [by_id[c.parent_id]] if c.parent_id in by_id else [by_id[sid] for _, sid in getattr(c, "links", ()) if sid in by_id]
        for p in sources:
            if p.service != c.service:
                lags[(p.service, c.service)].append(max((as_utc(c.ts) - as_utc(p.ts)).total_seconds() * 1000.0, 0.0))
    return {edge: (percentile(values, 0.95), len(values)) for edge, values in sorted(lags.items())}
