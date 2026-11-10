"""事件群計分。每個算子回傳調整後的分數，直接寫回 incident.score。"""
from __future__ import annotations

from aiops.engine.context import PipelineContext
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import DEFAULT_SEVERITY
from aiops.engine.operator import NodeInfo
from aiops.stages._common import FATAL_TAG, ensure_incidents
from aiops.engine.operator import BaseOperator

GOLDEN_SIGNALS = ("latency", "errors", "traffic", "saturation")


@OperatorRegistry.register("alert.golden_signals")
class GoldenSignalsScorer(BaseOperator):
    """事件群含黃金訊號（latency／errors／traffic／saturation）時加權。params: boost（倍數）、signals。"""

    scope = "batch"

    def __init__(self, boost: float = 1.5, signals: list[str] | None = None):
        self.boost = float(boost)
        self.signals = set(signals or GOLDEN_SIGNALS)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        for inc in ensure_incidents(ctx):
            hit = any(e.metric in self.signals or e.attrs.get("signal") in self.signals for e in inc.events)
            if hit:
                inc.score *= self.boost
        return ctx


@OperatorRegistry.register("alert.p0_boost")
class P0BoostScorer(BaseOperator):
    """事件群牽涉 P0 實體時加分。params: entities（P0 實體名稱）、boost（加分）。"""

    scope = "batch"

    def __init__(self, entities: list[str] | None = None, boost: float = 10):
        self.entities = set(entities or [])
        self.boost = float(boost)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        for inc in ensure_incidents(ctx):
            if self.entities & set(inc.entities):
                inc.score += self.boost
        return ctx


@OperatorRegistry.register("alert.fatal_floor")
class FatalFloorScorer(BaseOperator):
    """致命地板（雙軌制）：事件群裡只要有一筆來自致命硬上限節點的事件，分數至少拉到指定嚴重度的門檻，
    並打上 fatal:<節點名稱> 標籤（抑制算子靠它放行、通知帶 fatal 欄位、RCA 以它當觸發條件）。只拉高、不降低。

    params: nodes（必填：哪些偵測節點算致命，必須是不投票的偵測節點）、severity（預設 P0）。
    地板分數 = 同一條 workflow 的 alert.notify_payload 的 severity_thresholds[severity]，載入期換算；
    workflow 沒有 payload 節點、或它沒有這個等級，直接報錯。要排在其他計分算子後面、alert.absorb_open 前面：
    進行中的事件群吸收到致命事件時分數取最大值，嚴重度跟著升級（只升不降）。
    """

    scope = "batch"

    def __init__(self, nodes: list[str], severity: str = "P0"):
        if isinstance(nodes, str) or not isinstance(nodes, (list, tuple)) or not nodes:
            raise TypeError(f"nodes 必須是偵測節點名稱的清單（至少一個），不是 {nodes!r}")
        self.nodes = frozenset(str(n) for n in nodes)
        self.severity = str(severity)
        if self.severity not in DEFAULT_SEVERITY:  # 沒經過引擎直接用時的地板；經過引擎會在 check_workflow 換成 payload 的門檻
            raise TypeError(f"severity {self.severity!r} 不是已知的嚴重度；可用：{sorted(DEFAULT_SEVERITY)}")
        self.floor = DEFAULT_SEVERITY[self.severity]

    def check_workflow(self, nodes: list[NodeInfo]) -> None:
        me = next(i for i, n in enumerate(nodes) if n.op is self)
        detectors = {n.name: n.op for n in nodes if getattr(n.op, "has_baseline", False)}
        unknown = sorted(self.nodes - set(detectors))
        if unknown:
            raise ValueError(f"nodes 裡的 {unknown} 不是這條 workflow 的偵測節點；可用：{sorted(detectors)}")
        voting = sorted(n for n in self.nodes if getattr(detectors[n], "votes", True))
        if voting:
            raise ValueError(f"{voting} 是參與多數決的統計偵測節點，不能當致命節點（統計分數沒有「踩到就是出事」的意義）")
        payloads = [(i, n) for i, n in enumerate(nodes) if n.operator == "alert.notify_payload"]
        if not payloads:
            raise ValueError("這條 workflow 沒有 alert.notify_payload，無從得知各嚴重度的分數門檻")
        index, payload = payloads[0]
        thresholds = payload.params.get("severity_thresholds") or DEFAULT_SEVERITY
        if self.severity not in thresholds:
            raise ValueError(f"severity {self.severity!r} 不在 {payload.name} 的 severity_thresholds 裡：{sorted(thresholds)}")
        late = [n.name for n in nodes[:me] if n.operator in ("alert.absorb_open", "alert.notify_payload")]
        if late:
            raise ValueError(f"必須排在 {'、'.join(late)} 前面：分數要先拉到地板，吸收與定嚴重度才看得到")
        self.floor = float(thresholds[self.severity])

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        for inc in ensure_incidents(ctx):
            hit = sorted({e.detector_node for e in inc.events if e.detector_node in self.nodes})
            if not hit:
                continue
            for node in hit:
                tag = f"{FATAL_TAG}:{node}"
                if tag not in inc.tags:
                    inc.tags.append(tag)
            ctx.log(f"fatal:{','.join(hit)}:{'|'.join(inc.entities)}:score={inc.score:.2f}->floor={self.floor:g}")
            inc.score = max(inc.score, self.floor)
        return ctx
