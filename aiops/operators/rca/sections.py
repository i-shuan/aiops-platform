"""RCA 報告的 8 個段落。每個段落是一個小物件：applies 判斷資料夠不夠，不夠就略過（不輸出空殼），build 產生內容。

段落清單由 workflow 的 rca.report 節點決定，換場景（IT／工廠／純指標）只改 YAML。
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta

from aiops.core.context import PipelineContext, ReportSection
from aiops.core.topology import propagation_edges
from aiops.core.types import Incident, as_utc

GOLDEN_SIGNALS = ("latency", "errors", "traffic", "saturation")
_TRACE_MARGIN = timedelta(minutes=10)


class Section:
    key = ""
    title = ""

    def applies(self, incident: Incident, ctx: PipelineContext) -> bool:
        return True

    def build(self, incident: Incident, ctx: PipelineContext) -> ReportSection:
        raise NotImplementedError

    def _out(self, body: str) -> ReportSection:
        return ReportSection(self.key, self.title, body)


class Summary(Section):
    key, title = "summary", "摘要"

    def build(self, incident, ctx):
        body = (f"{len(incident.entities)} 個實體、{len(incident.events)} 筆異常，"
                f"{incident.start.isoformat()} ~ {incident.end.isoformat()}，分數 {incident.score:.2f}")
        if incident.tags:
            body += f"；收斂方式：{', '.join(incident.tags)}"
        return self._out(body)


class Timeline(Section):
    key, title = "timeline", "時間軸"

    def build(self, incident, ctx):
        return self._out("\n".join(f"{e.ts.isoformat()} {e.entity} {e.metric} score={e.score:.2f}"
                                   for e in incident.events))


class AffectedEntities(Section):
    key, title = "affected_entities", "受影響實體"

    def build(self, incident, ctx):
        counts = Counter(e.entity for e in incident.events)
        return self._out("\n".join(f"{entity}：{n} 筆" for entity, n in counts.most_common()))


def _golden(incident: Incident):
    return [e for e in incident.events if e.metric in GOLDEN_SIGNALS or e.attrs.get("signal") in GOLDEN_SIGNALS]


class GoldenSignals(Section):
    """IT 場景專用：只有事件群真的含黃金訊號時才產生。"""

    key, title = "golden_signals", "黃金訊號"

    def applies(self, incident, ctx):
        return bool(_golden(incident))

    def build(self, incident, ctx):
        return self._out("\n".join(f"{e.entity} {e.attrs.get('signal', e.metric)} score={e.score:.2f}"
                                   for e in _golden(incident)))


def _first_seen(incident: Incident) -> dict[str, tuple[datetime, float]]:
    seen: dict[str, tuple[datetime, float]] = {}
    for e in incident.events:
        ts, score = seen.get(e.entity, (e.ts, e.score))
        seen[e.entity] = (min(ts, e.ts), max(score, e.score))
    return seen


def root_candidates(incident: Incident, edges: set[tuple[str, str]]) -> list[str]:
    """沒有「異常的上游」的異常實體，依最早出現、分數高低排序。全部互為上下游（環）時就全部列出。"""
    anomalous = set(incident.entities)
    seen = _first_seen(incident)
    roots = [e for e in anomalous if not any((src, e) in edges for src in anomalous if src != e)] or list(anomalous)
    return sorted(roots, key=lambda e: (seen[e][0], -seen[e][1]))


def _in_graph(entity: str, edges: set[tuple[str, str]]) -> bool:
    return any(entity in edge for edge in edges)


class RootCauseCandidates(Section):
    key, title = "root_cause_candidates", "可能的源頭"

    def applies(self, incident, ctx):
        edges = propagation_edges(ctx.topology, ctx.lot_edges)
        return any(_in_graph(e, edges) for e in incident.entities)

    def build(self, incident, ctx):
        edges = propagation_edges(ctx.topology, ctx.lot_edges)
        seen = _first_seen(incident)
        lines = []
        for i, entity in enumerate(root_candidates(incident, edges), 1):
            ts, score = seen[entity]
            reason = "上游沒有其他異常" if _in_graph(entity, edges) else "不在拓撲中，無法判斷上下游"
            lines.append(f"{i}. {entity}：最早 {ts.isoformat()}，最高分數 {score:.2f}；{reason}")
        return self._out("\n".join(lines))


class PropagationPath(Section):
    key, title = "propagation_path", "傳播路徑"

    def applies(self, incident, ctx):
        anomalous = set(incident.entities)
        return any(a in anomalous and b in anomalous for a, b in propagation_edges(ctx.topology, ctx.lot_edges))

    def build(self, incident, ctx):
        edges = propagation_edges(ctx.topology, ctx.lot_edges)
        anomalous = set(incident.entities)
        seen = _first_seen(incident)
        paths: list[list[str]] = []

        def walk(path: list[str]) -> None:
            nexts = sorted(b for a, b in edges if a == path[-1] and b in anomalous and b not in path)
            if not nexts or len(paths) >= 10:
                if len(path) > 1:
                    paths.append(path)
                return
            for b in nexts:
                walk(path + [b])

        for root in root_candidates(incident, edges):
            walk([root])
        return self._out("\n".join(" → ".join(f"{e}（{seen[e][0].strftime('%H:%M:%S')}）" for e in p) for p in paths))


def _trace_rows(incident: Incident, ctx: PipelineContext):
    start, end = as_utc(incident.start) - _TRACE_MARGIN, as_utc(incident.end) + _TRACE_MARGIN
    return [r for s in incident.entities for r in ctx.trace_rows.get(s, []) if start <= as_utc(r.ts) <= end]


class TraceEvidence(Section):
    """IT 場景：用 trace 合成出的每服務錯誤率與 p95 佐證。載入的 trace 時序沒有相關資料就略過。"""

    key, title = "trace_evidence", "Trace 佐證"

    def applies(self, incident, ctx):
        return bool(_trace_rows(incident, ctx))

    def build(self, incident, ctx):
        by_service: dict[str, list] = {}
        for r in _trace_rows(incident, ctx):
            by_service.setdefault(r.service, []).append(r)
        lines = []
        for service in sorted(by_service):
            rows = by_service[service]
            worst_err = max(rows, key=lambda r: r.error_rate)
            worst_p95 = max(rows, key=lambda r: r.p95_ms)
            lines.append(f"{service}：請求 {sum(r.count for r in rows)} 次；"
                         f"錯誤率最高 {worst_err.error_rate:.0%}（{worst_err.ts.isoformat()}）；"
                         f"p95 最高 {worst_p95.p95_ms:.0f} ms（{worst_p95.ts.isoformat()}）")
        return self._out("\n".join(lines))


# (指標名稱關鍵字, 建議)。依指標名稱比對，命中幾條就列幾條；都沒命中就給通用建議
_RULES: tuple[tuple[tuple[str, ...], str], ...] = (
    (("latency", "duration", "p95", "p99", "slow"), "檢查下游依賴與慢查詢，並比對同時間的部署或設定變更"),
    (("error", "5xx", "fail", "exception"), "查看錯誤日誌與最近一次部署，必要時回滾"),
    (("cpu", "saturation", "load", "memory", "mem", "oom"), "確認資源用量與限制（limits、HPA），必要時擴容"),
    (("disk", "io", "iops", "inode"), "檢查磁碟容量與 I/O 等待"),
    (("traffic", "qps", "rps", "request"), "確認流量來源是否異常（活動、爬蟲、重試風暴）"),
    (("rf_", "power", "pressure", "temp", "vacuum", "flow"), "檢查機台保養（PM）紀錄與近期配方（recipe）變更"),
)
_GENERIC = "比對事件時間前後的變更紀錄（部署、設定、配方）"


class SuggestedActions(Section):
    """依指標種類給處置建議（規則式、可預期）；有拓撲時先指出要優先處理的源頭。"""

    key, title = "suggested_actions", "建議處置"

    def build(self, incident, ctx):
        actions: list[str] = []
        edges = propagation_edges(ctx.topology, ctx.lot_edges)
        if any(e in pair for e in incident.entities for pair in edges):
            actions.append(f"優先處理可能的源頭：{', '.join(root_candidates(incident, edges)[:3])}")
        names = " ".join(f"{e.metric} {e.attrs.get('signal', '')}" for e in incident.events).lower()
        hits = [action for keywords, action in _RULES if any(k in names for k in keywords)]
        actions += hits or [_GENERIC]
        return self._out("\n".join(f"{i}. {a}" for i, a in enumerate(actions, 1)))


SECTIONS: dict[str, Section] = {s.key: s for s in (
    Summary(), Timeline(), AffectedEntities(), GoldenSignals(), RootCauseCandidates(), PropagationPath(),
    TraceEvidence(), SuggestedActions())}
