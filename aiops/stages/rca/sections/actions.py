"""建議處置：確定性 Runbook Matrix（docs/RCA_STEP2_DESIGN.md 第 3 節）。

把已經算好的證據（根因候選的下游資源歸因 attribution、範例 trace、日誌樣板、事件的訊號類別）翻成處方；
不看 metric 名稱的子字串：以前 `transaction.duration` 含 `io` 會叫人去查磁碟。

訊號類別（latency／errors／traffic／saturation／lag）的判定順序：
  1. target 的 attrs.signal（跟 alert.golden_signals 同一組字；真實 ES target 的 metric 是整段查詢 JSON，沒有這個就判不出類別）
  2. 沒有時把 metric 字串切成整字 token（非字母數字為界），整字相等才算（duration、latency → latency；io、disk → saturation）
  3. 都沒有 → 類別未知，只靠歸因、trace、日誌的規則
命中的規則全部列出（依 priority），一條都沒命中才給保底並寫明「沒有命中任何故障特徵」。

流量類的兩條處方靠 trace 證據分流（docs/MQ_QPS_DESIGN.md P1）：
  RB-08 內部呼叫放大：第一名帶 attribution["amplifier"]（它對下游的扇出暴增，把下游灌爆）→ 查重試、迴圈、熔斷；擴容沒有用。
  RB-04 容量飽和：流量與延遲兩種訊號都在而且範例 trace 沒失敗（原規則），或第一名帶 attribution["ingress_surge"]
        （流量確定是外部入口打進來的）——後者不看範例 trace 有沒有失敗：被灌爆的服務本來就會拋錯。

日誌特徵處方（RB-09 記憶體耗盡、RB-10 連線池耗盡、RB-11 死結／鎖等待）：trace 只看得到「慢了、錯了」，看不到為什麼；
錯誤日誌的例外類型與訊息看得到。條件三個都要成立：
  1. 問題在服務自己或它的資料庫／快取（`_local_fault`）：沒有單一下游主導（share < 0.3），或主導的是 db／cache。
     外部服務主導（RB-01）時不看——那時服務自己的連線池逾時是結果，不是原因。
  2. 樣板是嫌疑服務的（第一名候選；沒有候選時是事件裡的實體），而且在暴增倍率前 LOG_TOP_N 名。
     別的服務的日誌不算：被灌爆的受害者滿地都是 pool exhausted，不能拿來開放大者的處方。
  3. 樣板是「突發」的：正常期間沒出現過，或頻率達正常期間的 MIN_BURST_RATIO 倍。長年都在噴的 Deadlock 不是這次的原因。
比對是整字關鍵字的正則（LOG_SIGNATURES），對象是例外類型＋樣板＋原始訊息範例；命中後排在泛用的 RB-02／RB-03 前面。
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
    "lag": "lag", "backlog": "lag",  # 佇列積壓（kafka_consumer_lag、queue_backlog）：第五類，不併進 saturation
}
LOG_TOP_N = 3            # 只看嫌疑服務暴增倍率前幾名的樣板
MIN_BURST_RATIO = 3.0    # 既有樣板要暴增到幾倍才算「這次事故的特徵」（新出現的樣板一律算）
# 日誌特徵 -> 正則（不分大小寫）。故意寫得窄："worker pool exhausted"（執行緒池）不是連線池、"memory" 一個字不是 OOM
LOG_SIGNATURES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("oom", re.compile(r"OutOfMemory|OOMKilled|Java heap space|GC overhead limit exceeded|Cannot allocate memory"
                       r"|\bMemoryError\b|JavaScript heap out of memory", re.I)),
    ("pool", re.compile(r"ConnectionPoolExhausted|Connection\s?Pool\w*(?:Exhausted|Timeout)"
                        r"|connection pool (?:is |was )?(?:exhausted|full|timeout|timed out)"
                        r"|Connection is not available, request timed out"  # HikariCP
                        r"|QueuePool limit of size"                         # SQLAlchemy
                        r"|Timeout waiting for (?:idle object|connection from pool)"
                        r"|too many (?:connections|clients)|remaining connection slots are reserved"
                        r"|CannotGetJdbcConnection|Unable to acquire JDBC Connection", re.I)),
    ("deadlock", re.compile(r"Deadlock|LockAcquisitionException|Lock wait timeout exceeded|CannotAcquireLock", re.I)),
)


def match_log_signatures(patterns: list[dict]) -> dict[str, dict]:
    """patterns 是嫌疑服務依暴增倍率排好的樣板（rank_patterns 的輸出）。回傳 特徵 -> 命中的那個樣板（倍率最高的一個）。
    只看前 LOG_TOP_N 名裡「突發」的：新出現，或頻率達正常期間的 MIN_BURST_RATIO 倍。"""
    hits: dict[str, dict] = {}
    for pattern in patterns[:LOG_TOP_N]:
        if not (pattern.get("new") or float(pattern.get("ratio", 0.0)) >= MIN_BURST_RATIO):
            continue
        text = " ".join(str(pattern.get(k, "")) for k in ("error_type", "template", "example"))
        for key, regex in LOG_SIGNATURES:
            if key not in hits and regex.search(text):
                hits[key] = pattern
    return hits


def _log_evidence(pattern: dict | None) -> str:
    """處方裡引用的日誌證據：例外類型、樣板、事件期間與正常期間的筆數。"""
    if not pattern:
        return ""
    name = f"{pattern['error_type']}：" if pattern.get("error_type") else ""
    burst = "正常期間沒出現過" if pattern.get("new") else f"正常期間 {pattern.get('baseline', 0):g} 筆，頻率 {pattern.get('ratio', 0):.0f} 倍"
    return f"「{name}{pattern.get('template', '')}」事件期間 {pattern.get('during', 0):g} 筆（{burst}）"


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
    # 嫌疑服務（第一名候選；沒有候選時是事件裡的實體）依暴增倍率排好的前 LOG_TOP_N 個錯誤日誌樣板（rank_patterns 的輸出）
    log_patterns: list[dict] = field(default_factory=list)
    insufficient: bool = False

    @property
    def log_hits(self) -> dict[str, dict]:
        """日誌特徵（oom／pool／deadlock）-> 命中的樣板。只在問題出在服務自己或它的資料庫／快取時才看（見模組說明）。"""
        return match_log_signatures(self.log_patterns) if _local_fault(self) else {}

    @property
    def consumer_sample(self) -> bool:
        """範例 trace 的入口是訊息消費者（從佇列拿到訊息），不是 HTTP 請求。"""
        return bool(self.sample) and self.sample.consumer

    @property
    def attribution(self) -> dict[str, Any]:
        return self.top.attribution if self.top else {}

    @property
    def share(self) -> float:
        return float(self.attribution.get("share", 0.0))

    @property
    def async_info(self) -> dict[str, Any]:
        """第一名候選的非同步邊資訊（attribution["async"]，見 stages/rca/mq.async_attribution）；沒有就是空的。"""
        return self.attribution.get("async") or {}

    @property
    def amplifier(self) -> dict[str, Any]:
        """第一名候選把下游流量放大的證據（attribution["amplifier"]，見 rca.root_cause.evidence 的「扇出」）；沒有就是空的。"""
        return self.attribution.get("amplifier") or {}

    @property
    def ingress_surge(self) -> dict[str, Any]:
        """第一名候選的流量是外部入口打進來的證據（attribution["ingress_surge"]）；沒有就是空的。"""
        return self.attribution.get("ingress_surge") or {}

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
            suspects = {facts.top.entity.lower()} if facts.top else {e.entity.lower() for e in incident.events}
            ranked = rank_patterns(ctx.rca_logs, ctx.rca_logs_baseline, ctx.rca_log_window_minutes)
            facts.log_patterns = [p for p in ranked if str(p["service"]).lower() in suspects][:LOG_TOP_N]
        return facts

    def _failure(self) -> tuple[str, str]:
        """範例 trace 是怎麼失敗的、該看它的什麼輸入。訊息消費者沒有 HTTP 狀態碼：以前一律寫「HTTP 失敗」，
        Kafka 消費者的處方變成「檢查 Kafka RECEIVE（HTTP 失敗）的入參」。"""
        if self.consumer_sample:
            queue = f"佇列 {self.sample.queue}" if self.sample.queue else "佇列名稱未知"
            return f"訊息消費失敗，{queue}", "訊息內容（payload、headers）"
        if self.sample and self.sample.status_code:
            return f"HTTP {self.sample.status_code}", "入參"
        return "失敗", "入參"

    def fmt(self) -> dict[str, str]:
        """處方文字裡可以帶入的欄位。"""
        a = self.attribution
        mq = self.async_info
        amp, fan = self.amplifier, self.amplifier.get("fanout") or {}
        hits = self.log_hits
        failure, inputs = self._failure()
        return {"failure": failure, "inputs": inputs,
                "log_oom": _log_evidence(hits.get("oom")), "log_pool": _log_evidence(hits.get("pool")),
                "log_deadlock": _log_evidence(hits.get("deadlock")),
                "log_service": next((p["service"] for p in hits.values()), ""),
                "db": a.get("resource") if a.get("kind") in ("db", "cache") else "資料庫",
                "victim": amp.get("victim", ""), "fanout_before": str(fan.get("before", "?")), "fanout_during": str(fan.get("during", "?")),
                "fanout_ratio": str(amp.get("fanout_ratio", "?")), "entry": self.ingress_surge.get("entry", ""),
                "entity": self.top.entity if self.top else "", "resource": a.get("resource", ""),
                "kind": "/".join(p for p in (a.get("kind", ""), a.get("subtype", "")) if p),
                "endpoint": (self.sample.name if self.sample and self.sample.name else "範例 trace"),
                "queue": mq.get("queue") or "（佇列名稱未知）", "producer": mq.get("producer") or "（trace 未能辨識）",
                "pub_window": mq.get("pub_window_text") or "（trace 未能還原）",
                "lag": (mq.get("lag_text") or {}).get("during") or "未知"}


def _local_fault(f: RunbookFacts) -> bool:
    """問題在服務自己（沒有單一下游主導）或它的資料庫／快取：這時才拿日誌關鍵字開處方。
    幾個服務一起變慢、證據不足以指認單一服務（RB-06）時不看：那時每個服務的日誌都只是症狀。"""
    return not f.insufficient and (f.attribution.get("kind") in ("db", "cache") or f.share < 0.3)


def _queue_backlog(f: RunbookFacts) -> bool:
    """RB-07：第一名是訊息生產者（它在發布時窗有異常才會上榜），或訊息延遲暴增（lag 訊號、或 trace 還原的延遲達到積壓判準）
    而且沒有單一下游資源主導——有資源主導時是那個資源慢才積壓，該查的是資源（RB-01／RB-02），不是佇列。"""
    mq = f.async_info
    return mq.get("role") == "producer" or (("lag" in f.signals or bool(mq.get("backlog"))) and not f.attribution.get("by"))


def _queue_order(f: RunbookFacts) -> tuple[int, ...]:
    """RB-07 三條處方先做哪一條：第一名是生產者（訊息是它發的）→ 先回發布時段查它，積壓深度放最後；
    其餘（延遲暴增）→ 先看積壓。三行摘要的「立即止血動作」只引用第一條，順序錯會把人帶去看錯的地方。"""
    return (1, 2, 0) if f.async_info.get("role") == "producer" else (0, 1, 2)


@dataclass(frozen=True)
class Runbook:
    id: str
    name: str
    priority: int
    condition: Callable[[RunbookFacts], bool]
    actions: tuple[str, ...]
    order: Callable[[RunbookFacts], tuple[int, ...]] | None = None  # 處方順序要看事實時才給；不給就照 actions 的順序

    def ordered(self, facts: RunbookFacts) -> tuple[str, ...]:
        return tuple(self.actions[i] for i in self.order(facts)) if self.order else self.actions


RUNBOOK_RULES: tuple[Runbook, ...] = (
    Runbook("RB-01", "下游外部服務超時或激增", 100,
            lambda f: f.attribution.get("kind") == "external" and f.share >= 0.6,
            ("檢查下游依賴目標 {resource} 的健康狀態與網路延遲（{entity} 的增量幾乎全在等它）",
             "確認 {entity} 對 {resource} 的 HTTP client timeout 與連線池上限（Max Connections）設定",
             "若 {resource} 持續惡化，評估對它啟動降級或熔斷（circuit breaker）")),
    Runbook("RB-07", "非同步佇列積壓／訊息夾帶失敗", 95, _queue_backlog,
            ("檢查佇列 {queue} 的積壓深度（consumer lag）與消費者並行度／消費速率（事件期間訊息延遲 P95 {lag}）",
             "回到訊息發布時段 {pub_window} 查生產者 {producer} 當時的部署、設定變更與它的依賴——不是看消費者出事的那段時間",
             "抽查失敗訊息的內容與 dead-letter queue：是否有格式不合或毒訊息（poison message）反覆重試"),
            order=_queue_order),
    Runbook("RB-09", "記憶體耗盡（OutOfMemory）", 93,
            lambda f: "oom" in f.log_hits,
            ("{log_service} 的錯誤日誌出現記憶體耗盡：{log_oom}。先確認實例是否被 OOMKilled／反覆重啟"
             "（kubectl describe pod 的 Last State）與 heap、RSS 用量曲線",
             "抓 heap dump 或記憶體 profile 找出佔用最多的物件；核對近期部署是否引入沒有上限的快取，或一次把整批資料載進記憶體的查詢",
             "止血：重啟受影響的實例並暫時調高記憶體 limit（JVM 的 -Xmx）；這只是爭取時間，洩漏不修會再發生")),
    Runbook("RB-10", "連線池耗盡", 92,
            lambda f: "pool" in f.log_hits,
            ("{log_service} 的錯誤日誌出現連線池耗盡：{log_pool}。查連線池的 active／idle／pending 數與上限（maxPoolSize），"
             "確認是池子打滿，而不是 {db} 拒絕連線",
             "找出佔著連線不還的呼叫：長交易、沒有關閉的連線（connection leak）、交易裡夾著外部 HTTP 呼叫；打開連線池的 leak detection",
             "核對 {db} 端的連線數上限（max_connections）與目前連線數：所有實例的池上限加總不能超過它，調大池子之前先確認 {db} 扛得住")),
    Runbook("RB-11", "資料庫死結／鎖等待", 91,
            lambda f: "deadlock" in f.log_hits,
            ("{log_service} 的錯誤日誌出現死結或鎖等待逾時：{log_deadlock}。到 {db} 查死結紀錄"
             "（PostgreSQL log 的 deadlock detected、MySQL 的 SHOW ENGINE INNODB STATUS）找出互鎖的兩段 SQL",
             "核對近期上線的交易是否用不同順序更新同一批資料列，或批次作業跟線上交易搶同一批資料",
             "止血：先暫停互鎖的批次作業；應用端對死結錯誤做有限次數的重試，並把交易縮短")),
    Runbook("RB-02", "資料庫／快取查詢瓶頸或連線阻塞", 90,
            lambda f: f.attribution.get("kind") in ("db", "cache")
            and (f.share >= 0.5 or f.attribution.get("failures", {}).get("during", 0) > f.attribution.get("failures", {}).get("before", 0)),
            ("排查 {resource} 當前慢查詢（slow log）、鎖等待（lock wait）與活躍連線數",
             "檢查近期是否有缺少索引的查詢或批次搬移作業上線",
             "確認 {entity} 的連線池（connection pool）是否打滿導致取連線阻塞")),
    Runbook("RB-08", "內部呼叫放大（重試風暴／迴圈呼叫）", 85,
            lambda f: f.amplifier.get("type") == "traffic_amplifier",
            ("檢查 {entity} 呼叫 {victim} 的 retry policy 與 backoff：每處理一個請求呼叫 {victim} 的次數由 {fanout_before} 次"
             "變成 {fanout_during} 次（扇出 {fanout_ratio} 倍），先確認是不是重試風暴",
             "核對 {entity} 近期的部署與設定變更，是否引入 tight loop 或逐筆呼叫 {victim}（原本批次查一次，變成每筆查一次）",
             "在 {entity} 啟用客戶端熔斷（circuit breaker）或由閘道對 {victim} 限流（rate limiting）先保住 {victim}；"
             "擴容 {victim} 沒有用，流量是 {entity} 放大出來的")),
    Runbook("RB-03", "應用程式內部邏輯異常或狀態機錯誤", 80,
            lambda f: "errors" in f.signals and f.sample_failed and (f.top is None or f.share < 0.3),
            ("點下方範例 trace 檢查 {endpoint}（{failure}）的{inputs}與堆疊日誌",
             "核對事故初發前 1 小時內 {entity} 是否有部署、設定變更或 feature flag 開啟",
             "若為新版本導致，建議立即回滾（rollback）")),
    Runbook("RB-04", "流量突波引發容量飽和", 70,
            lambda f: ({"traffic", "latency"} <= f.signals and not f.sample_failed) or bool(f.ingress_surge),
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
            if facts.async_info.get("suspect_payload"):  # 已知死區：trace 看不到訊息內容，照實交給人查
                actions.append(f"{top.entity} 處理佇列 {values['queue']} 的訊息失敗，而生產者 {values['producer']} 在發布時段 "
                               f"{values['pub_window']} 沒有症狀——trace 看不到訊息內容，這不代表它發的內容沒問題："
                               f"抽查失敗訊息與 dead-letter queue，並核對 {values['producer']} 當時的部署與設定變更")
        for rb in hits:
            actions.append(f"[{rb.id}] {rb.name}")
            actions += [a.format(**values) for a in rb.ordered(facts)]
        primary = hits[0]
        ctx.rca_runbook = {"id": primary.id, "name": primary.name,
                           "actions": [a.format(**values) for a in primary.ordered(facts)],
                           "also": [rb.id for rb in hits[1:]],
                           "signals": sorted(facts.signals),
                           "log_signatures": sorted(facts.log_hits)}
        return "\n".join(f"{i}. {a}" for i, a in enumerate(actions, 1))
