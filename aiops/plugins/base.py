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
    """輸入固定為 DataFrame(ts, value)。fit 回傳 self，detect 前必須先 fit（或 load_state）。

    get_state／load_state 讓批次訓練的結果能保存到 DB、下次直接載入，不用每次重新 fit。
    """

    fitted: bool = False
    # True：df 除了 ts、value 還必須有 peer_* 欄位（同儕序列），由 Command 依 request 的 peers 準備
    requires_peers: bool = False
    # 可調參數：名稱 -> (預設值, 最小值, 最大值)。tuning（含 LLM 從回饋抽規則）只能改這些，且不能超出範圍
    tunable: dict[str, tuple[float, float, float]] = {}

    def validate_params(self) -> None:
        """參數本身不合理就丟 ValueError（Command 會轉成 400）。預設檢查 tunable 範圍。"""
        for key, (_default, low, high) in self.tunable.items():
            if key in self.params and not (low <= float(self.params[key]) <= high):
                raise ValueError(f"{self.name}.{key}={self.params[key]} 超出範圍 [{low}, {high}]")

    def param(self, key: str) -> float:
        return float(self.params.get(key, self.tunable[key][0]))

    @abstractmethod
    def fit(self, df: pd.DataFrame) -> BaseDetector: ...

    @abstractmethod
    def detect(self, df: pd.DataFrame) -> AnomalyResult: ...

    @abstractmethod
    def get_state(self) -> dict[str, Any]:
        """fit 學到的狀態（基準線參數），必須能 JSON 序列化。"""

    @abstractmethod
    def load_state(self, state: dict[str, Any]) -> BaseDetector:
        """還原 get_state() 的結果，之後視同已 fit。回傳 self。"""

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


# ── 拓撲：統一成「異常會從 A 傳到 B」的有向邊 ──


def propagation_edges(context: dict[str, Any]) -> set[tuple[str, str]]:
    """IT 的 topology 是 [caller, callee]：callee（被呼叫的依賴）出事會拖垮 caller，所以傳播方向是 callee -> caller。
    工廠的 lot_edges 是 {"from", "to", "lots"}：上游機台出事會流到下游，傳播方向是 from -> to。
    """
    edges = {(e[1], e[0]) for e in context.get("topology", []) if len(e) == 2 and e[0] != e[1]}
    min_lots = int(context.get("min_lots", 1))
    edges |= {
        (e["from"], e["to"]) for e in context.get("lot_edges", [])
        if e.get("from") != e.get("to") and int(e.get("lots", 1)) >= min_lots
    }
    return edges
