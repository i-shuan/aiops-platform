"""跨系統收斂：把 debounce 視窗內收到的 Grafana 告警併成事故，一場事故從初發到結案只有一個 uid、一筆註記。

  1. 同實體合併：同一個 service 的多筆告警算同一件事。
  2. 拓撲鏈收斂：兩個有告警的 service 之間有關係邊（不分方向，A→B 或 B→A）就併在一起；可以一路串下去
     （A、B、C 都有告警且 A→B、B→C 有邊 → 一張單）。中間的系統沒有告警就不會串過去：不替沒看到的東西補洞。
  3. 併入進行中事故：新的一群只要跟進行中事故有同一個 service、或隔一條邊相鄰，就併進去（同時碰到好幾張就全部併成最早那張）。
  4. 嚴重度只升不降：取成員告警裡最高的 aiops_severity。
  5. 結案：成員告警全部 resolved，而且持續 close_after_seconds 沒有再響，才結案（防抖動：剛恢復又響不會另開一張）。

「什麼算異常」由各系統自己在 Grafana 的告警規則決定；這裡不判斷數值，只收斂。
關係邊只有系統層級（3～5 條），不假設是 DAG，有環也可以。純函式，不碰網路與資料庫。
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

SEVERITY_RANK = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}


@dataclass(frozen=True)
class Alert:
    """Grafana webhook 裡的一筆告警（一條規則的一組標籤 = 一個 fingerprint）。"""
    fingerprint: str
    status: str                 # firing／resolved
    rule_uid: str
    title: str
    service: str
    severity: str
    starts_at: datetime
    ends_at: datetime | None    # resolved 才有
    values: dict[str, Any] = field(default_factory=dict, hash=False)  # 觸發當下規則各節點的數值（給 LLM 當證據）


@dataclass
class Incident:
    uid: str
    status: str                      # open／closed
    severity: str
    services: list[str]
    signals: dict[str, dict[str, Any]]  # fingerprint -> {rule_uid, title, service, severity, status, starts_at, ends_at, values}
    started_at: datetime
    last_seen_at: datetime           # 最後一次收到成員告警（firing 或 resolved）的時間
    ended_at: datetime | None = None
    resolved_at: datetime | None = None  # 成員全部 resolved 的那一刻；還有 firing 的就是 None
    alert_count: int = 0             # 收斂進來的告警筆數（不同的 fingerprint）
    summary: str = ""                # 註記第 2 行（LLM 或原始摘要）
    analyzed: bool = False           # 第 2 行是不是通過檢查的 LLM 推論
    annotation_id: int | None = None
    annotated: bool = False          # 目前的內容寫回 Grafana 了沒（沒寫成功下一輪補）
    llm_failures: int = 0            # LLM 分析連續失敗幾次（成功就歸零）；退避用
    llm_next_try: datetime | None = None  # 退避中：這個時間之前不再查日誌、不再打 LLM
    candidates: str = ""             # 註記上根因分析底下的列點（LLM 列的、程式檢查過的）；沒有就是空的
    evidence_note: str = ""          # 註記上「證據｜…」那一行：已有哪些、缺哪些與原因、沒接哪些（程式產生）
    changed: bool = field(default=False, compare=False)  # 這一輪有沒有變（只活在記憶體，不存檔）
    fresh: bool = field(default=False, compare=False)    # 這一輪成員或嚴重度變了（只活在記憶體）：要重新分析


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


def group_alerts(alerts: list[Alert], edges: list[tuple[str, str]]) -> list[list[Alert]]:
    """同實體 + 拓撲鏈：回傳一群一群的告警（每群依開始時間排好），群的順序依最早的告警。"""
    adjacent = neighbours(edges)
    key = lambda a: (a.starts_at, a.fingerprint)
    groups: list[tuple[set[str], list[Alert]]] = []
    for alert in sorted(alerts, key=key):
        services, members = {alert.service}, [alert]
        rest = []
        for other_services, other_members in groups:
            if _touches(services, other_services, adjacent):
                services |= other_services
                members = other_members + members
            else:
                rest.append((other_services, other_members))
        groups = rest + [(services, members)]
    return sorted((sorted(m, key=key) for _s, m in groups), key=lambda m: key(m[0]))


def _member(alert: Alert) -> dict[str, Any]:
    return {"rule_uid": alert.rule_uid, "title": alert.title, "service": alert.service, "severity": alert.severity,
            "status": alert.status, "starts_at": alert.starts_at, "ends_at": alert.ends_at, "values": dict(alert.values)}


def _add(incident: Incident, fingerprint: str, member: dict[str, Any]) -> None:
    incident.signals[fingerprint] = member
    incident.services = sorted({*incident.services, member["service"]})
    incident.severity = worse(incident.severity, member["severity"])
    incident.started_at = min(incident.started_at, member["starts_at"])
    incident.alert_count = len(incident.signals)


def _fold(target: Incident, other: Incident) -> None:
    """把另一張進行中事故併進 target（兩張原本各自成立，後來被同一群告警連起來）。other 以結案收掉。"""
    for fingerprint, member in other.signals.items():
        _add(target, fingerprint, member)
    target.last_seen_at = max(target.last_seen_at, other.last_seen_at)
    other.status, other.ended_at, other.changed, other.annotated = "closed", other.last_seen_at, True, False
    other.summary = f"[已併入事故 {target.uid}] {other.summary}".strip()


def converge(open_incidents: list[Incident], firing: list[Alert], resolved: list[Alert],
             edges: list[tuple[str, str]], *, now: datetime, close_after_seconds: int) -> list[Incident]:
    """這一輪的收斂。firing = 這個 debounce 視窗內響過的告警；resolved = 視窗結束時最新狀態是 resolved 的告警
    （視窗內先響又恢復的告警兩邊都有：先開事故、再記成已恢復，不會憑空消失）。
    回傳所有事故（進行中的、新開的、這一輪結案的）；有變動的 changed = True。"""
    adjacent = neighbours(edges)
    incidents = sorted(open_incidents, key=lambda i: (i.started_at, i.uid))
    for inc in incidents:
        inc.changed = inc.fresh = False

    for members in group_alerts(firing, edges):
        services = {a.service for a in members}
        hits = [i for i in incidents if i.status == "open"
                and (_touches(services, set(i.services), adjacent) or any(a.fingerprint in i.signals for a in members))]
        if hits:
            target = hits[0]
            for other in hits[1:]:
                _fold(target, other)
        else:
            first = members[0]
            target = Incident(uid=uuid.uuid4().hex[:12], status="open", severity=first.severity, services=[],
                              signals={}, started_at=first.starts_at, last_seen_at=now)
            incidents.append(target)
        before = (set(target.signals), target.severity)
        for alert in members:
            _add(target, alert.fingerprint, {**_member(alert), "status": "firing", "ends_at": None})
        # 同一批告警只是重複通知：不用重新問 LLM；有新告警加入或升級才重新分析
        target.fresh = target.fresh or (set(target.signals), target.severity) != before
        target.last_seen_at, target.changed, target.annotated = max(target.last_seen_at, now), True, False

    # resolved：只更新已經是成員的告警；沒見過的 resolved（事故早就結案、或從沒進來過）不開新事故
    owner = {fp: inc for inc in incidents if inc.status == "open" for fp in inc.signals}
    for alert in resolved:
        inc = owner.get(alert.fingerprint)
        if inc is not None and inc.signals[alert.fingerprint]["status"] != "resolved":
            inc.signals[alert.fingerprint].update(status="resolved", ends_at=alert.ends_at)
            inc.last_seen_at, inc.changed, inc.annotated = max(inc.last_seen_at, now), True, False

    for inc in incidents:
        if inc.status != "open":
            continue
        if any(m["status"] == "firing" for m in inc.signals.values()):
            if inc.resolved_at is not None:  # 剛恢復又響：同一張事故繼續
                inc.resolved_at, inc.changed, inc.annotated = None, True, False
            continue
        if inc.resolved_at is None:
            inc.resolved_at, inc.changed = now, True
        if now - inc.resolved_at >= timedelta(seconds=close_after_seconds):
            ends = [m["ends_at"] for m in inc.signals.values() if m["ends_at"]]
            inc.status, inc.ended_at = "closed", max(ends) if ends else inc.resolved_at
            inc.changed, inc.annotated = True, False
    return incidents
