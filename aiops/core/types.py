"""平台的核心語言：異常事件、事件群、對外通知。純資料與純函式，不 import 平台其他任何一層。

領域、算子、通知出口都依賴這裡；這裡不依賴任何人（tests/test_architecture.py 檢查）。
"""
from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def new_incident_uid() -> str:
    """對外識別（通知、Alertmanager 分組）用的全域唯一 id。整數 id 會因資料庫重建從 1 重來，不能拿去外面用。"""
    return f"inc-{uuid.uuid4().hex}"


def as_utc(dt: datetime) -> datetime:
    """比較時間前統一成 UTC；沒帶時區的視為 UTC，避免 naive／aware 混用直接報錯。"""
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


@dataclass
class AnomalyEvent:
    entity: str
    metric: str
    ts: datetime
    score: float
    attrs: dict[str, Any] = field(default_factory=dict)
    value: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"entity": self.entity, "metric": self.metric, "ts": self.ts.isoformat(),
                "score": self.score, "attrs": self.attrs, "value": self.value}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> AnomalyEvent:
        return cls(d["entity"], d["metric"], datetime.fromisoformat(d["ts"]), d["score"], d.get("attrs", {}),
                   d.get("value"))


@dataclass
class Incident:
    events: list[AnomalyEvent]
    score: float = 0.0
    tags: list[str] = field(default_factory=list)
    # 被抑制時的原因（None = 照發）。由抑制算子填，不再另外傳一份旁路資料
    suppressed_by: str | None = None
    severity: str = ""
    uid: str = field(default_factory=new_incident_uid)

    @property
    def entities(self) -> list[str]:
        return sorted({e.entity for e in self.events})

    @property
    def depts(self) -> list[str]:
        """事件 attrs["dept"] 帶進來的部門（給通知路由用）。"""
        return sorted({str(e.attrs["dept"]) for e in self.events if e.attrs.get("dept")})

    @property
    def fingerprint(self) -> str:
        """同一組實體視為同一件事，用來判斷是否重複通知。"""
        return "|".join(self.entities)

    @property
    def start(self) -> datetime:
        return min(e.ts for e in self.events)

    @property
    def end(self) -> datetime:
        return max(e.ts for e in self.events)

    @classmethod
    def combine(cls, incidents: list[Incident], tag: str | None = None) -> Incident:
        tags: list[str] = []
        for t in [t for i in incidents for t in i.tags] + ([tag] if tag and len(incidents) > 1 else []):
            if t not in tags:
                tags.append(t)
        return cls(
            events=sorted((e for i in incidents for e in i.events), key=lambda e: e.ts),
            score=max(i.score for i in incidents),
            tags=tags,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": round(self.score, 4),
            "severity": self.severity,
            "tags": self.tags,
            "entities": self.entities,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "events": [e.to_dict() for e in self.events],
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any], uid: str | None = None) -> Incident:
        inc = cls([AnomalyEvent.from_dict(e) for e in d["events"]], d["score"], list(d.get("tags", [])),
                  severity=d.get("severity", ""))
        if uid:
            inc.uid = uid
        return inc


def gap_seconds(a: Incident, b: Incident) -> float:
    """兩個 Incident 時間區間的間隔秒數（重疊為 0）。"""
    return max(0.0, (max(a.start, b.start) - min(a.end, b.end)).total_seconds())


def merge_incidents(incidents: list[Incident], should_merge: Callable[[Incident, Incident], bool],
                    tag: str | None = None) -> list[Incident]:
    """依 should_merge 做遞移合併（A~B、B~C → A、B、C 同一群）。"""
    parent = list(range(len(incidents)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i in range(len(incidents)):
        for j in range(i + 1, len(incidents)):
            if should_merge(incidents[i], incidents[j]):
                parent[find(j)] = find(i)

    groups: dict[int, list[Incident]] = {}
    for i, inc in enumerate(incidents):
        groups.setdefault(find(i), []).append(inc)
    return [Incident.combine(g, tag) for g in groups.values()]


@dataclass
class Notification:
    """要送給接收端的一則通知。接收端只依 labels 路由。"""

    labels: dict[str, str]
    annotations: dict[str, str]
    starts_at: datetime
    # 有值 = 結案（恢復通知）；None = 仍在發生
    ends_at: datetime | None = None
