"""平台的核心語言：異常事件、事件群、對外通知。純資料與純函式，不 import 平台其他任何一層。

領域、算子、通知出口都依賴這裡；這裡不依賴任何人（tests/test_architecture.py 檢查）。
"""
from __future__ import annotations

import math
import uuid
from numbers import Real
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


ANOMALY = "anomaly"
WARNING = "warning"


@dataclass(frozen=True)
class AnomalyEvent:
    """一筆異常。建立後不可修改（frozen）：偵測器名稱、分數一旦產生，下游（收斂、計分）只能讀、不能改，
    要調整就產生新的事件。"""

    entity: str
    metric: str
    ts: datetime
    score: float
    attrs: dict[str, Any] = field(default_factory=dict)
    value: float | None = None
    # 資料血緣：哪個演算法（算子名，例如 detector.zscore）、哪個 Workflow 節點抓到的。外部匯入的異常留空
    detector: str = ""
    detector_node: str = ""
    # 從哪個資料源來（RCA 重抓時序用）。外部匯入的異常留空
    source_type: str = ""
    # 舊版表決節點（已移除的 detector.ensemble／soft_vote）的裁決事件由哪些節點投票通過。現在沒有任何算子會填，
    # 欄位留著只為了讀得回資料庫裡舊的事件群快照
    voted_by: tuple[str, ...] = ()
    # anomaly（越過門檻）或 warning（接近門檻的預警，偵測節點設了 warning_ratio 才會有）
    level: str = ANOMALY

    def __post_init__(self) -> None:
        """NaN／±Inf 在入口就拒絕：分數是 NaN 的事件排序、比大小、多數決全部靜默算錯（NaN 跟誰比都是 False），
        寫進 JSON 欄位還會變成不合法的 JSON。偵測器算出非有限分數是 bug 或壞資料，要讓那條序列報錯，不是悄悄變成一筆異常。
        bool 不是數值（True 會被當成 1.0 分）。"""
        for name, number in (("score", self.score), ("value", self.value)):
            if name == "value" and number is None:
                continue  # 外部匯入的異常可以沒有原始值
            if isinstance(number, bool) or not isinstance(number, Real) or not math.isfinite(number):
                raise ValueError(f"AnomalyEvent.{name} 必須是有限數值，收到 {number!r}"
                                 f"（entity={self.entity!r}, detector_node={self.detector_node!r}, ts={self.ts}）")

    def to_dict(self) -> dict[str, Any]:
        return {"entity": self.entity, "metric": self.metric, "ts": self.ts.isoformat(),
                "score": self.score, "attrs": self.attrs, "value": self.value,
                "detector": self.detector, "detector_node": self.detector_node,
                "source_type": self.source_type, "voted_by": list(self.voted_by), "level": self.level}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> AnomalyEvent:
        return cls(d["entity"], d["metric"], datetime.fromisoformat(d["ts"]), d["score"], d.get("attrs", {}),
                   d.get("value"), d.get("detector", ""), d.get("detector_node", ""), d.get("source_type", ""),
                   tuple(d.get("voted_by", ())), d.get("level", ANOMALY))


# 嚴重度門檻的預設值（分數 >= 門檻就是該等級）：alert.notify_payload 定嚴重度用這一份
DEFAULT_SEVERITY = {"P0": 10.0, "P1": 6.0, "P2": 3.0, "P3": 0.0}


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
    def level(self) -> str:
        """有任何一筆越過門檻就是 anomaly；全部都只是接近門檻才是 warning。"""
        return ANOMALY if any(e.level == ANOMALY for e in self.events) else WARNING

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
    # 「看來源」連結（Alertmanager 的 generatorURL，Grafana 卡片上的 See source）；沒設定 Grafana 就是 None
    generator_url: str | None = None


@dataclass(frozen=True)
class GrafanaLinks:
    """深度連結的設定：基底網址來自部署（環境變數），儀表板、變數、資料源 uid 來自管線 YAML。
    alert.notify_payload 組好放進 ctx.grafana_links，alert.heartbeat 續命時用同一份，卡片每一輪都帶連結。"""

    base_url: str
    dashboard: str | None = None  # 例如 d/apm-overview（不含基底網址）
    var: str = "service"  # 儀表板上鎖服務用的變數名稱
    pad_minutes: float = 15  # 事故前後各多看幾分鐘
    datasources: dict[str, str] = field(default_factory=dict)  # {traces: apm-traces, logs: apm-logs}：Explore 連結用
