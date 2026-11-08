"""外掛抽象契約與外掛之間共用的資料型別。

領域層只依賴這裡與 registry，不 import 具體外掛。
外掛是純運算零件：不連 DB、不打外部服務、不認識任何領域。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import pandas as pd

GOLDEN_SIGNALS = ("latency", "errors", "traffic", "saturation")


def as_utc(dt: datetime) -> datetime:
    """比較時間前統一成 UTC；沒帶時區的視為 UTC，避免 naive／aware 混用直接報錯。"""
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


class _Plugin:
    name: str = ""

    def __init__(self, params: dict[str, Any] | None = None):
        self.params = params or {}


# ── 偵測器：生命週期只有 fit / detect，collect、train、tune 是領域 Command 的事 ──


@dataclass
class AnomalyResult:
    timestamps: list
    values: list
    scores: list
    is_anomaly: list

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamps": self.timestamps,
            "values": self.values,
            "scores": self.scores,
            "is_anomaly": self.is_anomaly,
            "anomaly_count": int(sum(self.is_anomaly)),
        }


class BaseDetector(_Plugin, ABC):
    """輸入固定為 DataFrame(ts, value)。fit 回傳 self，detect 前必須先 fit。"""

    fitted: bool = False

    @abstractmethod
    def fit(self, df: pd.DataFrame) -> BaseDetector: ...

    @abstractmethod
    def detect(self, df: pd.DataFrame) -> AnomalyResult: ...

    def _require_fitted(self) -> None:
        if not self.fitted:
            raise RuntimeError(f"{self.name}: detect 前必須先 fit")


# ── 告警收斂：異常事件 → 事件群（Incident） ──


@dataclass
class AnomalyEvent:
    entity: str
    metric: str
    ts: datetime
    score: float
    attrs: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"entity": self.entity, "metric": self.metric, "ts": self.ts.isoformat(),
                "score": self.score, "attrs": self.attrs}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> AnomalyEvent:
        return cls(d["entity"], d["metric"], datetime.fromisoformat(d["ts"]), d["score"], d.get("attrs", {}))


@dataclass
class Incident:
    events: list[AnomalyEvent]
    score: float = 0.0
    tags: list[str] = field(default_factory=list)

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
            "tags": self.tags,
            "entities": self.entities,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "events": [e.to_dict() for e in self.events],
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Incident:
        return cls([AnomalyEvent.from_dict(e) for e in d["events"]], d["score"], list(d.get("tags", [])))


def gap_seconds(a: Incident, b: Incident) -> float:
    """兩個 Incident 時間區間的間隔秒數（重疊為 0）。"""
    return max(0.0, (max(a.start, b.start) - min(a.end, b.end)).total_seconds())


def merge_incidents(
    incidents: list[Incident], should_merge: Callable[[Incident, Incident], bool], tag: str | None = None
) -> list[Incident]:
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


class BaseGrouper(_Plugin, ABC):
    @abstractmethod
    def group(self, incidents: list[Incident], context: dict[str, Any]) -> list[Incident]: ...


class BaseScorer(_Plugin, ABC):
    @abstractmethod
    def score(self, incident: Incident, context: dict[str, Any]) -> float:
        """回傳調整後的分數，不得修改 incident。"""


class BaseSuppressor(_Plugin, ABC):
    @abstractmethod
    def suppress(self, incident: Incident, context: dict[str, Any]) -> str | None:
        """回傳抑制原因（字串）；None 表示不抑制。不得修改 incident。"""


# ── RCA 報告段落 ──


@dataclass
class ReportSection:
    key: str
    title: str
    body: str

    def to_dict(self) -> dict[str, str]:
        return {"key": self.key, "title": self.title, "body": self.body}


class BaseSectionBuilder(_Plugin, ABC):
    title: str = ""

    def applies(self, incident: Incident, context: dict[str, Any]) -> bool:
        """這個事件群有沒有足夠資料產生本段；沒有就略過，不輸出空殼。"""
        return True

    @abstractmethod
    def build(self, incident: Incident, context: dict[str, Any]) -> ReportSection: ...


# ── LLM ──


class BaseLLMProvider(_Plugin, ABC):
    @abstractmethod
    def complete(self, prompt: str) -> str: ...
