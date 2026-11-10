"""佐證段落：trace。

優先用這一輪 ingestion.incident_traces 隨需撈回來的 span（ctx.rca_spans）：各服務事件前 vs 事件期間的請求數、平均總耗時、
每請求自身耗時（扣掉等下游的時間）、錯誤率，再附一條事件期間最慢的 trace 的呼叫鏈（依實際 parent.id 還原，標每段耗時與自身耗時）。
這一輪沒有 span 時，才退回資料庫裡批次匯入的 trace 時序（ctx.trace_rows）。兩者都沒有就略過。
"""
from __future__ import annotations

from datetime import timedelta

from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import as_utc
from aiops.stages.notification.links import explore_url, grafana_base_url
from aiops.stages.rca.base import RcaSection
from aiops.stages.rca.mq import edge_line, producer_windows
from aiops.stages.rca.spans import (call_chain, is_resource_leaf, resource_kinds, resource_label, sample_trace,
                                    sample_trace_reason, window_stats)

_TRACE_MARGIN = timedelta(minutes=10)
MAX_CHAIN_LINES = 15


def _trace_rows(incident, ctx):
    start, end = as_utc(incident.start) - _TRACE_MARGIN, as_utc(incident.end) + _TRACE_MARGIN
    return [r for s in incident.entities for r in ctx.trace_rows.get(s, []) if start <= as_utc(r.ts) <= end]


def _trace_link(trace_id: str, ts, traces_datasource: str | None) -> str:
    """最慢 trace 的 Grafana Explore 連結（trace.id 鎖定、時間鎖在那一刻前後）；沒設 AIOPS_GRAFANA_URL 或資料源就回空字串。"""
    base = grafana_base_url()
    if base is None or not traces_datasource:
        return ""
    return explore_url(base, traces_datasource, f'trace.id:"{trace_id}"', ts, ts, pad_minutes=5)


def _from_spans(incident, ctx, traces_datasource: str | None = None) -> str:
    spans = ctx.rca_spans
    start, end = as_utc(incident.start), as_utc(incident.end) + timedelta(minutes=1)
    # 補撈過訊息發布時段時，事件前基準仍從主撈取窗算起（跟根因算子同一個起點）
    first_ts = as_utc(ctx.rca_main_start) if ctx.rca_main_start else min(as_utc(s.ts) for s in spans)
    before = window_stats(spans, first_ts, start) if first_ts < start else {}
    during = window_stats(spans, start, end)
    base_min = (start - first_ts).total_seconds() / 60
    kinds = resource_kinds(spans)
    extra = ((ctx.result.get("trace_fetch") or {}).get("mq") or {}).get("extra_spans", 0)
    lines = [f"這一輪撈到 {len(spans)} 個 span、{len({s.trace_id for s in spans})} 條 trace"
             + (f"（其中 {extra} 個是訊息發布時段補撈的）" if extra else "") + "；"
             f"事件前 {base_min:.0f} 分鐘 vs 事件期間（每請求）："]
    lookback = ctx.rca_trace_lookback
    if lookback and lookback[0] != lookback[1]:
        lines.append(f"（事件前基準窗因入口請求不足，已由 {lookback[0]} 分鐘擴展至 {lookback[1]} 分鐘；"
                     f"{ctx.result.get('trace_fetch', {}).get('extended_for', '')}）".replace("；）", "）"))
    for service in sorted(during, key=lambda s: -(during[s].self_ms_per_request - (before[s].self_ms_per_request if s in before else 0))):
        d, b = during[service], before.get(service)
        mark = "＊" if service in incident.entities else "  "
        if b:
            lines.append(f"{mark}{service}：請求 {b.requests}→{d.requests}；總耗時 {b.total_ms_mean:.1f}→{d.total_ms_mean:.1f} ms；"
                         f"自身耗時 {b.self_ms_per_request:.1f}→{d.self_ms_per_request:.1f} ms"
                         f"（{d.self_ms_per_request - b.self_ms_per_request:+.1f}）；錯誤率 {b.error_rate:.0%}→{d.error_rate:.0%}")
        else:
            lines.append(f"{mark}{service}：請求 {d.requests}；總耗時 {d.total_ms_mean:.1f} ms；自身耗時 {d.self_ms_per_request:.1f} ms；"
                         f"錯誤率 {d.error_rate:.0%}（事件前沒有資料）")
        resources = sorted(set(d.wait_ms_per_request) | set(b.wait_ms_per_request if b else {}),
                           key=lambda res: -d.wait_ms_per_request.get(res, 0.0))
        if resources:  # 只有帶 span.type 的資料才有這一行；mock 與舊索引一字不多
            waits = "；".join(
                f"等待 {res}{'（' + kinds[res] + '）' if kinds.get(res) else ''} "
                + (f"{b.wait_ms_per_request.get(res, 0.0):.1f}→" if b else "") + f"{d.wait_ms_per_request.get(res, 0.0):.1f} ms"
                + f"（資源失敗 {(str(b.wait_failures.get(res, 0)) + '→') if b else ''}{d.wait_failures.get(res, 0)} 筆）"
                for res in resources)
            app = (f"{b.app_ms_per_request:.1f}→" if b else "") + f"{d.app_ms_per_request:.1f} ms"
            lines.append(f"    程式自身 {app}；{waits}")
    skew = {svc: d.clock_skew_spans for svc, d in during.items() if d.clock_skew_spans}
    if skew:  # 沒有時鐘偏移就一字不多
        lines.append("（時鐘偏移：事件期間有子 span 超出父 span 的時間範圍，超出的部分沒算進扣除——"
                     + "、".join(f"{svc} {n} 筆" for svc, n in sorted(skew.items())) + "）")
    for edge in ctx.rca_mq:  # 沒有非同步邊就一字不多
        lines.append(edge_line(edge))
        pb, pd = producer_windows(spans, edge)
        if pb and pd:
            lines.append(f"    {edge.producer} 在發布時窗：請求 {pb.requests}→{pd.requests}；自身耗時 "
                         f"{pb.self_ms_per_request:.1f}→{pd.self_ms_per_request:.1f} ms"
                         f"（{pd.self_ms_per_request - pb.self_ms_per_request:+.1f}）；自身錯誤率 "
                         f"{pb.own_error_rate:.0%}→{pd.own_error_rate:.0%}（對照：發布時窗之前等長的一段）")
    if ctx.rca_mq_note:
        lines.append(ctx.rca_mq_note)
    sample = sample_trace(spans, start, end)
    if sample is not None:
        link = _trace_link(sample.trace_id, as_utc(sample.ts), traces_datasource)
        why = sample_trace_reason(sample, spans)
        title = (f"事件期間最具代表性的失敗 trace（{why}；{sample.trace_id}，{sample.duration_ms:.0f} ms）" if why
                 else f"事件期間最慢的一條 trace（{sample.trace_id}，{sample.duration_ms:.0f} ms）")
        lines.append(title + "：" + (f"\n  {link}" if link else ""))
        chain = call_chain(spans, sample.trace_id)
        children = {}
        for s in spans:
            if s.trace_id == sample.trace_id and s.parent_id:
                children.setdefault(s.parent_id, []).append(s)
        for depth, s, own in chain[:MAX_CHAIN_LINES]:
            what = f" {s.name}" if s.name else ""
            what += f" {s.kind}→{resource_label(s)}" if is_resource_leaf(s, children) else ""
            lines.append(f"  {'  ' * depth}{s.service}{what}：{s.duration_ms:.1f} ms（自身 {own:.1f} ms）{'，失敗' if s.error else ''}")
        if len(chain) > MAX_CHAIN_LINES:
            lines.append(f"  …（還有 {len(chain) - MAX_CHAIN_LINES} 個 span）")
    lines.append("＊ = 事件裡的服務。自身耗時 = span 耗時扣掉直接子 span 佔用的時間（並行的 I/O 子 span 重疊的部分只扣一次；含等外部資源）；程式自身 = 再扣掉等資源的時間。")
    return "\n".join(lines)


@OperatorRegistry.register("rca.section.trace_evidence")
class TraceEvidence(RcaSection):
    """IT 場景：trace 佐證。優先用這一輪撈到的 span，沒有才用資料庫裡的 trace 時序。
    params: traces_datasource（Grafana 的 trace 資料源 uid，例如 apm-traces）：有設、且環境變數 AIOPS_GRAFANA_URL 有值時，
    最慢的那條 trace 附 Explore 連結；兩者缺一就只印 trace id。"""

    key, title = "trace_evidence", "Trace 佐證"

    def __init__(self, traces_datasource: str | None = None):
        self.traces_datasource = traces_datasource or None

    def applies(self, incident, ctx):
        return bool(ctx.rca_spans) or bool(_trace_rows(incident, ctx))

    def build(self, incident, ctx):
        if ctx.rca_spans:
            return _from_spans(incident, ctx, self.traces_datasource)
        by_service: dict[str, list] = {}
        for r in _trace_rows(incident, ctx):
            by_service.setdefault(r.service, []).append(r)
        lines = ["（這一輪沒有撈到 span，以下是資料庫裡批次匯入的 trace 時序）"]
        for service in sorted(by_service):
            rows = by_service[service]
            worst_err = max(rows, key=lambda r: r.error_rate)
            worst_p95 = max(rows, key=lambda r: r.p95_ms)
            lines.append(f"{service}：請求 {sum(r.count for r in rows)} 次；"
                         f"錯誤率最高 {worst_err.error_rate:.0%}（{worst_err.ts.isoformat()}）；"
                         f"p95 最高 {worst_p95.p95_ms:.0f} ms（{worst_p95.ts.isoformat()}）")
        return "\n".join(lines)
