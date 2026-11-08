"""第 2 層：故障劇本注入器。讀劇本 YAML：什麼時候開始、根因是誰、用哪種機制、隨時間怎麼擴散（lag）。

兩種機制（跟平台 engine/topology.propagation_edges 的語意有關，不能混用）：

- callee_slowdown：根因是最下游的服務（它自己或它的外部依賴變慢）。劇本只寫根因自己的偏移；上游 caller 的耗時
  「自然」變長，因為上游的整段耗時本來就包含呼叫下游的時間（trace 是加總出來的，不另外假造）。延遲方向 callee -> caller，
  跟平台的傳播圖同向，拓撲／PageRank 應該指向最下游。
- load_amplification：根因在上游（例如 cart-api 重試風暴、連線池耗盡）。根因對下游的呼叫次數變成 call_multiplier 倍，
  下游的請求量（qps）立刻上升，下游自己的處理時間在 lag 分鐘之後才上升（排隊）。這是 caller -> callee 的方向，
  跟平台傳播圖相反，所以拓撲／PageRank 很可能會指錯人；這正是這份資料要量出來的。

所有受影響的實體共用同一條包絡線 I(t)∈[0,1]（緩升、高原、指數衰減），各自只差 lag 與振幅。
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from .documents import check_namespace
from .topology import Topology

MECHANISMS = ("callee_slowdown", "load_amplification")


def parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"時間要帶時區（例如結尾 Z）：{value}")
    return dt.astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


@dataclass(frozen=True)
class Envelope:
    """I(t)：onset 之後 ramp 分鐘線性升到 1、維持 plateau 分鐘、再用 decay 分鐘指數衰減回 0（尾端平滑接到 0）。"""

    ramp_minutes: float = 10.0
    plateau_minutes: float = 90.0
    decay_minutes: float = 20.0

    @property
    def total_minutes(self) -> float:
        return self.ramp_minutes + self.plateau_minutes + self.decay_minutes

    def __call__(self, minutes: np.ndarray) -> np.ndarray:
        m = np.asarray(minutes, dtype=float)
        out = np.zeros_like(m)
        r, p, d = self.ramp_minutes, self.plateau_minutes, self.decay_minutes
        ramp = (m >= 0) & (m < r)
        out[ramp] = m[ramp] / r if r > 0 else 1.0
        out[(m >= r) & (m < r + p)] = 1.0
        dec = (m >= r + p) & (m < r + p + d)
        x = (m[dec] - r - p) / d
        k = 3.0  # 衰減到 e^-3 ≈ 5% 時剛好走完 decay 分鐘；再減掉那 5% 讓它平滑接到 0
        out[dec] = (np.exp(-k * x) - math.exp(-k)) / (1 - math.exp(-k))
        return out


@dataclass(frozen=True)
class EntityFault:
    entity: str
    lag_minutes: float = 0.0
    latency_shift_ms: float = 0.0     # 這個服務「自己的」處理時間增加多少（不含下游）
    trace_error_rate: float = 0.0     # 這個服務「自己」出錯的機率（高原時）
    call_multiplier: float = 1.0      # 對下游服務的呼叫次數變幾倍（重試風暴；只有 load_amplification 能用）


@dataclass(frozen=True)
class Incident:
    id: str
    mechanism: str
    start: datetime
    end: datetime
    root_cause: str
    envelope: Envelope
    faults: tuple[EntityFault, ...]

    def onset(self, entity: str) -> datetime | None:
        for f in self.faults:
            if f.entity == entity:
                return datetime.fromtimestamp(self.start.timestamp() + f.lag_minutes * 60, tz=timezone.utc)
        return None


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime
    step_seconds: int = 60

    @property
    def minutes(self) -> int:
        return int((self.end - self.start).total_seconds() // self.step_seconds)


@dataclass(frozen=True)
class Scenario:
    name: str
    seed: int
    window: Window
    tz: str
    incidents: tuple[Incident, ...]
    traces: dict[str, float]   # dense_per_minute、sparse_per_hour、margin_minutes
    base_rate_per_minute: float
    namespace: str = "gen"     # 寫進 ES 時的 data stream namespace（gen 開頭）


def load_scenario(path: str | Path) -> Scenario:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return scenario_from_dict(raw, Path(path).stem)


def scenario_from_dict(raw: dict[str, Any], default_name: str = "scenario") -> Scenario:
    w = raw["window"]
    window = Window(parse_ts(w["start"]), parse_ts(w["end"]), int(w.get("step_seconds", 60)))
    if window.step_seconds != 60:
        raise ValueError("目前只支援 step_seconds: 60（APM 的 transaction metrics 是一分鐘一筆）")
    if window.end <= window.start:
        raise ValueError("window.end 必須晚於 window.start")
    incidents = []
    for inc in raw.get("incidents", []):
        mechanism = inc.get("mechanism")
        if mechanism not in MECHANISMS:
            raise ValueError(f"事件 {inc.get('id')} 的 mechanism 必須是 {MECHANISMS} 之一（要明講傳播方向）：{mechanism}")
        env = Envelope(**(inc.get("envelope") or {}))
        faults = tuple(EntityFault(**f) for f in inc.get("propagation", []))
        incidents.append(Incident(str(inc["id"]), mechanism, parse_ts(inc["start"]), parse_ts(inc["end"]),
                                  str(inc["root_cause"]), env, faults))
    tr = raw.get("traces") or {}
    return Scenario(
        name=str(raw.get("name", default_name)), seed=int(raw["seed"]), window=window,
        tz=str(raw.get("tz", "Asia/Taipei")), incidents=tuple(incidents),
        traces={"dense_per_minute": float(tr.get("dense_per_minute", 12)),
                "sparse_per_hour": float(tr.get("sparse_per_hour", 6)),
                "margin_minutes": float(tr.get("margin_minutes", 120))},
        base_rate_per_minute=float(raw.get("base_rate_per_minute", 120)),
        namespace=check_namespace(str(raw.get("namespace", "gen"))),
    )


def validate(scenario: Scenario, topo: Topology) -> list[str]:
    """劇本跟拓撲對不對得起來。錯的直接丟 ValueError；只是可疑的回傳警告字串。"""
    warnings: list[str] = []
    for inc in scenario.incidents:
        names = [f.entity for f in inc.faults]
        for n in names:
            if not topo.is_service(n):
                raise ValueError(f"{inc.id}：{n} 不是拓撲裡的服務（外部依賴沒有自己的指標，請把故障掛在呼叫它的服務上）")
        if len(set(names)) != len(names):
            raise ValueError(f"{inc.id}：propagation 同一個實體寫了兩次")
        if inc.root_cause not in names:
            raise ValueError(f"{inc.id}：root_cause {inc.root_cause} 必須出現在 propagation 裡")
        root = next(f for f in inc.faults if f.entity == inc.root_cause)
        if root.lag_minutes != 0 or any(f.lag_minutes < 0 for f in inc.faults):
            raise ValueError(f"{inc.id}：根因的 lag 必須是 0，其他實體的 lag 不能是負的")
        if inc.mechanism == "callee_slowdown":
            if topo.service_calls(inc.root_cause):
                raise ValueError(f"{inc.id}：callee_slowdown 的根因要是最下游的服務（不再呼叫其他服務），"
                                 f"{inc.root_cause} 還會呼叫 {[c.callee for c in topo.service_calls(inc.root_cause)]}")
            if len(inc.faults) != 1:
                raise ValueError(f"{inc.id}：callee_slowdown 只寫根因自己；上游變慢是呼叫樹加總出來的，不要另外寫偏移")
            if any(f.call_multiplier != 1 for f in inc.faults):
                raise ValueError(f"{inc.id}：callee_slowdown 不能有 call_multiplier（那是 load_amplification）")
        else:
            if root.call_multiplier <= 1:
                raise ValueError(f"{inc.id}：load_amplification 的根因要有 call_multiplier > 1（對下游的呼叫量放大）")
            reach = topo.downstream(inc.root_cause)
            for f in inc.faults:
                if f.entity != inc.root_cause and f.entity not in reach:
                    raise ValueError(f"{inc.id}：{f.entity} 不在 {inc.root_cause} 的下游，負載放大傳不過去")
                if f.entity != inc.root_cause and f.call_multiplier != 1:
                    raise ValueError(f"{inc.id}：只有根因能放大呼叫量")
        span = inc.envelope.total_minutes + max(f.lag_minutes for f in inc.faults)
        actual = (inc.end - inc.start).total_seconds() / 60
        if abs(actual - span) > 1:
            warnings.append(f"{inc.id}：end − start = {actual:.0f} 分鐘，但包絡線＋最大 lag = {span:.0f} 分鐘"
                            "（ground truth 的結束時間以 end 為準，訊號以包絡線為準）")
        if not (scenario.window.start <= inc.start < inc.end <= scenario.window.end):
            raise ValueError(f"{inc.id}：事件時間要落在 window 裡")
    return warnings
