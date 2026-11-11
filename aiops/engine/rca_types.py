"""認知型 RCA 三段之間的接口型別（docs/COGNITIVE_RCA_DESIGN.md）。

事實（Fact）由證據層從原始資料算出，確定性、不帶結論；證據項（EvidenceItem）由 LLM 從非結構化日誌抽出，
必須通過驗證才進得了判斷層。兩者都是 frozen：建立後不准改。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

# LLM 只能從這份封閉詞表挑，詞表外的一律拒絕。新增一種證據 = 在這裡加一項。
EVIDENCE_KINDS: frozenset[str] = frozenset({
    "circuit_breaker_open",       # 呼叫端熔斷：快速失敗，不是服務自己壞
    "gc_pause",                   # JVM 停頓
    "oom",                        # 記憶體耗盡
    "connection_pool_exhausted",  # 連線池／執行緒池耗盡
    "deadlock",                   # DB／鎖死結
    "lock_wait",                  # 等鎖逾時
    "client_timeout",             # 呼叫端主動逾時放棄
    "dependency_unreachable",     # 連不上下游
    "deployment_change",          # 版本／設定變更
    "broker_unavailable",         # 訊息中介異常
})

FactKind = str  # 例如 self_time_delta、wait_delta、entry_failure_rate、fanout_ratio、child_outlives_parent、log_template_surge、mq_lag


@dataclass(frozen=True)
class Fact:
    """證據層算出的一個事實。只描述「資料裡實際發生了什麼」，不寫誰是兇手。"""

    kind: FactKind
    entity: str                      # 服務／資源名稱
    value: float
    unit: str = ""                   # ms、ratio、count、per_min …
    window: Literal["incident", "baseline", "delta"] = "incident"
    detail: dict[str, Any] = field(default_factory=dict)  # 結構化補充，例如 {"before": 12.0, "during": 340.0, "n": 812}
    source: str = ""                 # 產出它的模組，例如 stages.rca.facts.spans


@dataclass(frozen=True)
class EvidenceItem:
    """LLM 從日誌抽出、且通過驗證的證據。quote 必須逐字出現在送進去的某筆日誌裡，否則視為幻覺丟棄。"""

    kind: str                        # 必須在 EVIDENCE_KINDS
    entity: str                      # 必須是事件群或拓撲裡已知的實體
    state: Literal["present", "absent"] = "present"
    confidence: float = 1.0          # 0～1，LLM 自評
    quote: str = ""
    log_ts: str = ""                 # 該筆日誌的時間（ISO），給報告引用


RcaStatus = Literal["ok", "undetermined"]
