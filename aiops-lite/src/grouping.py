"""跨系統收斂：把這一輪各規則的異常併成事故，一場事故從初發到結案只有一個 uid、一筆註記。

  1. 同實體合併：同一個 service 的多條規則算同一件事。
  2. 拓撲鏈收斂：兩個異常的 service 之間有關係邊（不分方向，A→B 或 B→A）就併在一起；可以一路串下去
     （A、B、C 都異常且 A→B、B→C 有邊 → 一張單）。中間的系統沒有異常就不會串過去：不替沒看到的東西補洞。
  3. 併入進行中事故：新的一群只要跟進行中事故有同一個 service、或隔一條邊相鄰，就併進去（同時碰到好幾張就全部併成最早那張）。
  4. 嚴重度只升不降：取成員規則裡最高的 aiops_severity，併入後不會因為後來的異常比較輕而降級。
  5. 連續 N 輪（預設 3）沒有新異常自動結案。某一輪「抓不到資料」的系統不算「沒有異常」，那一輪不計入。

關係邊只有系統層級（3～5 條），不假設是 DAG，有環也可以。純函式，不碰網路與資料庫。
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

SEVERITY_RANK = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}


@dataclass(frozen=True)
class Signal:
    """一條規則在這一輪的新異常（已經過 new_only）。"""
    rule_uid: str
    title: str
    service: str
    severity: str
    first_ts: datetime
    last_ts: datetime
    count: int
    peak_score: float
    detectors: tuple[str, ...]


@dataclass
class Incident:
    uid: str
    status: str                      # open／closed
    severity: str
    services: list[str]
    signals: dict[str, dict[str, Any]]  # rule_uid -> {title, service, severity, first_ts, last_ts, count, peak_score, detectors}
    started_at: datetime
    last_seen_at: datetime
    ended_at: datetime | None = None
    quiet_rounds: int = 0
    alert_count: int = 0             # 收斂進來的異常點總數
    summary: str = ""                # 註記第 2 行（LLM 或原始摘要）
    analyzed: bool = False           # 第 2 行是不是通過檢查的 LLM 推論
    annotation_id: int | None = None
    annotated: bool = False          # 目前的內容寫回 Grafana 了沒（沒寫成功下一輪補）
    changed: bool = field(default=False, compare=False)  # 這一輪有沒有變（只活在記憶體，不存檔）
    fresh: bool = field(default=False, compare=False)    # 這一輪成員規則或嚴重度變了（只活在記憶體）：要重新分析


def worse(a: str, b: str) -> str:
    return a if SEVERITY_RANK[a] <= SEVERITY_RANK[b] else b


def neighbours(edges: list[tuple[str, str]]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for a, b in edges:
        out.setdefault(a, set()).add(b)
        out.setdefault(b, set()).add(a)
    return out


def _touches(services: set[str], other: set[str], adjacent: dict[str, set[str]]) -> bool:
    return bool(services & other) or any(adjacent.get(s, set()) & other for s in services)


def group_signals(signals: list[Signal], edges: list[tuple[str, str]]) -> list[list[Signal]]:
    """同實體 + 拓撲鏈：回傳一群一群的 Signal（每群依時間排好），群的順序依最早的異常時間。"""
    adjacent = neighbours(edges)
    groups: list[tuple[set[str], list[Signal]]] = []
    for sig in sorted(signals, key=lambda s: (s.first_ts, s.rule_uid)):
        services, members = {sig.service}, [sig]
        rest = []
        for other_services, other_members in groups:
            if _touches(services, other_services, adjacent):
                services |= other_services
                members = other_members + members
            else:
                rest.append((other_services, other_members))
        groups = rest + [(services, members)]
    return sorted((sorted(m, key=lambda s: (s.first_ts, s.rule_uid)) for _s, m in groups),
                  key=lambda m: m[0].first_ts)


def _add_signal(incident: Incident, sig: Signal) -> None:
    prev = incident.signals.get(sig.rule_uid)
    incident.signals[sig.rule_uid] = {
        "title": sig.title, "service": sig.service, "severity": sig.severity,
        "first_ts": min(prev["first_ts"], sig.first_ts) if prev else sig.first_ts,
        "last_ts": max(prev["last_ts"], sig.last_ts) if prev else sig.last_ts,
        "count": (prev["count"] if prev else 0) + sig.count,
        "peak_score": max(prev["peak_score"] if prev else 0.0, sig.peak_score),
        "detectors": sorted({*(prev["detectors"] if prev else ()), *sig.detectors}),
    }
    incident.services = sorted({*incident.services, sig.service})
    incident.severity = worse(incident.severity, sig.severity)
    incident.started_at = min(incident.started_at, sig.first_ts)
    incident.last_seen_at = max(incident.last_seen_at, sig.last_ts)
    incident.alert_count += sig.count


def _fold(target: Incident, other: Incident) -> None:
    """把另一張進行中事故併進 target（兩張原本各自成立，後來被同一群異常連起來）。other 以結案收掉。"""
    for uid, s in other.signals.items():
        _add_signal(target, Signal(uid, s["title"], s["service"], s["severity"], s["first_ts"], s["last_ts"],
                                   s["count"], s["peak_score"], tuple(s["detectors"])))
    other.status, other.ended_at, other.changed, other.annotated = "closed", other.last_seen_at, True, False
    other.summary = f"[已併入事故 {target.uid}] {other.summary}".strip()


def converge(open_incidents: list[Incident], signals: list[Signal], edges: list[tuple[str, str]], *,
             blind_services: set[str], close_after_rounds: int) -> list[Incident]:
    """這一輪的收斂。回傳所有事故（進行中的、這一輪新開的、這一輪結案的）；有變動的 changed = True。"""
    adjacent = neighbours(edges)
    incidents = sorted(open_incidents, key=lambda i: (i.started_at, i.uid))
    for inc in incidents:
        inc.changed = inc.fresh = False
    for members in group_signals(signals, edges):
        services = {s.service for s in members}
        hits = [i for i in incidents if i.status == "open" and _touches(services, set(i.services), adjacent)]
        if hits:
            target = hits[0]
            for other in hits[1:]:
                _fold(target, other)
        else:
            first = members[0]
            target = Incident(uid=uuid.uuid4().hex[:12], status="open", severity=first.severity, services=[],
                              signals={}, started_at=first.first_ts, last_seen_at=first.last_ts)
            incidents.append(target)
        before = (set(target.signals), target.severity)
        for sig in members:
            _add_signal(target, sig)
        # 同一批規則只是又多了幾個異常點：筆數與時間更新，不用重新問 LLM；有新規則加入或升級才重新分析
        target.fresh = target.fresh or (set(target.signals), target.severity) != before
        target.quiet_rounds, target.changed, target.annotated = 0, True, False
    for inc in incidents:
        if inc.status != "open" or inc.changed:
            continue
        if set(inc.services) & blind_services:
            continue  # 這一輪看不到它的資料：不能當成「沒有異常」
        inc.quiet_rounds += 1
        inc.changed = True  # 計數變了要存檔，但註記內容沒變，不用重寫
        if inc.quiet_rounds >= close_after_rounds:
            inc.status, inc.ended_at, inc.annotated = "closed", inc.last_seen_at, False
    return incidents
