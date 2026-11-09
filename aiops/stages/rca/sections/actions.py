"""建議處置：確定性 Runbook Matrix（docs/RCA_STEP2_DESIGN.md 第 3 節）。

把已經算好的證據（根因候選的下游資源歸因 attribution、範例 trace、日誌樣板、事件的訊號類別）翻成處方；
不看 metric 名稱的子字串：以前 `transaction.duration` 含 `io` 會叫人去查磁碟。

訊號類別（latency／errors／traffic／saturation）的判定順序：
  1. target 的 attrs.signal（跟 alert.golden_signals 同一組字；真實 ES target 的 metric 是整段查詢 JSON，沒有這個就判不出類別）
  2. 沒有時把 metric 字串切成整字 token（非字母數字為界），整字相等才算（duration、latency → latency；io、disk → saturation）
  3. 都沒有 → 類別未知，只靠歸因、trace、日誌的規則
命中的規則全部列出（依 priority），一條都沒命中才給保底並寫明「沒有命中任何故障特徵」。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable

from aiops.engine.context import PipelineContext, RootCandidate, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import Incident, as_utc
from aiops.stages.rca.base import RcaSection
from aiops.stages.rca.logs import rank_patterns
from aiops.stages.rca.spans import resource_label, sample_trace

_TOKEN = re.compile(r"[A-Za-z0-9]+")
# 整字 token -> 訊號類別。故意不放 "request"（requests_total 是流量、request_duration 是延遲，靠 duration 判）
SIGNAL_TOKENS: dict[str, str] = {
    "latency": "latency", "duration": "latency", "p50": "latency", "p95": "latency", "p99": "latency", "slow": "latency",
    "rtt": "latency", "response": "latency",
    "error": "errors", "errors": "errors", "5xx": "errors", "fail": "errors", "failed": "errors", "failure": "errors",
    "failures": "errors", "exception": "errors", "exceptions": "errors",
    "qps": "traffic", "rps": "traffic", "tps": "traffic", "requests": "traffic", "traffic": "traffic", "throughput": "traffic",
    "cpu": "saturation", "memory": "saturation", "mem": "saturation", "oom": "saturation", "load": "saturation",
    "saturation": "saturation", "disk": "saturation", "io": "saturation", "iops": "saturation", "inode": "saturation",
    "fs": "saturation", "filesystem": "saturation",
}
DISK_TOKENS = frozenset({"disk", "io", "iops", "inode", "fs", "filesystem"})
FACTORY_TOKENS = frozenset({"rf", "power", "pressure", "temp", "temperature", "vacuum", "flow", "recipe", "chamber"})


def metric_tokens(metric: str) -> set[str]:
    return {t.lower() for t in _TOKEN.findall(metric or "")}


def signal_of(metric: str, attrs: dict[str, Any] | None) -> str:
    """一筆異常事件的訊號類別：attrs.signal 優先；沒有就看 metric 的整字 token；都沒有回空字串。"""
    declared = str((attrs or {}).get("signal") or "").strip().lower()
    if declared:
        return declared
    hits = {SIGNAL_TOKENS[t] for t in metric_tokens(metric) if t in SIGNAL_TOKENS}
    if len(hits) == 1:
        return hits.pop()
    # 同時像兩類（例如 request_duration_errors）就都不認，寧可少一條建議也不要亂給
    return ""


@dataclass
class RunbookFacts:
    """規則評估用的特徵，全部來自 ctx 裡已經算好的東西（純函式，可單測）。"""

    signals: set[str] = field(default_factory=set)
    metric_tokens: set[str] = field(default_factory=set)
    top: RootCandidate | None = None
    sample: SpanRecord | None = None
    sample_failed_resource: str = ""
    log_patterns: list[dict] = field(default_factory=list)
    insufficient: bool = False

    @property
    def attribution(self) -> dict[str, Any]:
        return self.top.attribution if self.top else {}

    @property
    def share(self) -> float:
        return float(self.attribution.get("share", 0.0))

    @property
    def sample_failed(self) -> bool:
        return bool(self.sample) and (bool(self.sample.error) or self.sample.status_code >= 500)

    @classmethod
    def from_ctx(cls, incident: Incident, ctx: PipelineContext) -> "RunbookFacts":
        facts = cls()
        for e in incident.events:
            facts.metric_tokens |= metric_tokens(e.metric)
            s = signal_of(e.metric, e.attrs)
            if s:
                facts.signals.add(s)
        facts.top = ctx.rca_root_candidates[0] if ctx.rca_root_candidates else None
        facts.insufficient = not ctx.rca_root_candidates and ctx.rca_root_note.startswith("證據不足")
        if ctx.rca_spans:
            start, end = as_utc(incident.start), as_utc(incident.end) + timedelta(minutes=1)
            facts.sample = sample_trace(ctx.rca_spans, start, end)
            if facts.sample is not None:
                failed = sorted({resource_label(c) for c in ctx.rca_spans if c.trace_id == facts.sample.trace_id
                                 and c.error and c.kind not in ("", "app")})
                facts.sample_failed_resource = "、".join(failed)
        if ctx.rca_logs:
            facts.log_patterns = rank_patterns(ctx.rca_logs, ctx.rca_logs_baseline, ctx.rca_log_window_minutes)[:3]
        return facts

    def fmt(self) -> dict[str, str]:
        """處方文字裡可以帶入的欄位。"""
        a = self.attribution
        return {"entity": self.top.entity if self.top else "", "resource": a.get("resource", ""),
                "kind": "/".join(p for p in (a.get("kind", ""), a.get("subtype", "")) if p),
                "endpoint": (self.sample.name if self.sample and self.sample.name else "範例 trace"),
                "status": str(self.sample.status_code) if self.sample and self.sample.status_code else "失敗"}


@dataclass(frozen=True)
class Runbook:
    id: str
    name: str
    priority: int
    condition: Callable[[RunbookFacts], bool]
    actions: tuple[str, ...]


RUNBOOK_RULES: tuple[Runbook, ...] = (
    Runbook("RB-01", "下游外部服務超時或激增", 100,
            lambda f: f.attribution.get("kind") == "external" and f.share >= 0.6,
            ("檢查下游依賴目標 {resource} 的健康狀態與網路延遲（{entity} 的增量幾乎全在等它）",
             "確認 {entity} 對 {resource} 的 HTTP client timeout 與連線池上限（Max Connections）設定",
             "若 {resource} 持續惡化，評估對它啟動降級或熔斷（circuit breaker）")),
    Runbook("RB-02", "資料庫／快取查詢瓶頸或連線阻塞", 90,
            lambda f: f.attribution.get("kind") in ("db", "cache")
            and (f.share >= 0.5 or f.attribution.get("failures", {}).get("during", 0) > f.attribution.get("failures", {}).get("before", 0)),
            ("排查 {resource} 當前慢查詢（slow log）、鎖等待（lock wait）與活躍連線數",
             "檢查近期是否有缺少索引的查詢或批次搬移作業上線",
             "確認 {entity} 的連線池（connection pool）是否打滿導致取連線阻塞")),
    Runbook("RB-03", "應用程式內部邏輯異常或狀態機錯誤", 80,
            lambda f: "errors" in f.signals and f.sample_failed and (f.top is None or f.share < 0.3),
            ("點下方範例 trace 檢查 {endpoint}（HTTP {status}）的入參與堆疊日誌",
             "核對事故初發前 1 小時內 {entity} 是否有部署、設定變更或 feature flag 開啟",
             "若為新版本導致，建議立即回滾（rollback）")),
    Runbook("RB-04", "流量突波引發容量飽和", 70,
            lambda f: {"traffic", "latency"} <= f.signals and not f.sample_failed,
            ("確認突發流量來源是業務活動／行銷大促，還是異常爬蟲／惡意攻擊",
             "觸發 HPA 水平擴展或手動擴容 replica 數量",
             "視情況在入口閘道啟用 API 限流（rate limiting）")),
    Runbook("RB-05", "磁碟 I/O 壓力", 50,
            lambda f: bool(f.metric_tokens & DISK_TOKENS),
            ("檢查主機／節點磁碟使用率與 IOPS 是否飽和",
             "檢查是否有未滾動的 log 檔或 core dump 佔滿空間")),
    Runbook("RB-06", "共同外部因素（證據不足以指認單一服務）", 40,
            lambda f: f.insufficient,
            ("不指認單一服務：幾個服務一起變慢且程度相近，先查共用的資料源、網路、DNS 與未被追蹤的依賴",
             "比對同一時間其他部門／叢集是否也有告警")),
    Runbook("RB-F1", "製程機台參數偏移", 30,
            lambda f: bool(f.metric_tokens & FACTORY_TOKENS)
            or any(t in FACTORY_TOKENS for s in f.signals for t in metric_tokens(s)),  # attrs.signal: rf_power → rf、power
            ("檢查機台保養（PM）紀錄與近期配方（recipe）變更",)),
)
FALLBACK = Runbook("RB-00", "沒有命中任何故障特徵", 0, lambda f: True,
                   ("比對事件時間前後的變更紀錄（部署、設定、配方）",
                    "這次沒有命中任何故障特徵：證據不足以分類，請以上面的根因候選與 trace／日誌佐證人工判斷"))


def evaluate(facts: RunbookFacts) -> list[Runbook]:
    hits = sorted((r for r in RUNBOOK_RULES if r.condition(facts)), key=lambda r: -r.priority)
    return hits or [FALLBACK]


@OperatorRegistry.register("rca.section.suggested_actions")
class SuggestedActions(RcaSection):
    key, title = "suggested_actions", "建議處置"

    def build(self, incident, ctx):
        facts = RunbookFacts.from_ctx(incident, ctx)
        hits = evaluate(facts)
        values = facts.fmt()
        actions: list[str] = []
        if ctx.rca_root_candidates:
            top = ctx.rca_root_candidates[0]
            actions.append(f"優先處理可能的源頭：{', '.join(c.entity for c in ctx.rca_root_candidates[:3])}")
            if top.dependency:  # 下鑽歸因：時間花在等下游資源，去查那個資源而不是這個服務的程式
                actions.append(f"{top.entity} 的增量主要在等待 {top.dependency}：先查該依賴的延遲、錯誤與連線池／配額，"
                               f"而不是 {top.entity} 自己的程式與機器")
        for rb in hits:
            actions.append(f"[{rb.id}] {rb.name}")
            actions += [a.format(**values) for a in rb.actions]
        primary = hits[0]
        ctx.rca_runbook = {"id": primary.id, "name": primary.name,
                           "actions": [a.format(**values) for a in primary.actions],
                           "also": [rb.id for rb in hits[1:]],
                           "signals": sorted(facts.signals)}
        return "\n".join(f"{i}. {a}" for i, a in enumerate(actions, 1))
