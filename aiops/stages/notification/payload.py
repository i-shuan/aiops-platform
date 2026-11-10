"""通知內容由平台決定：嚴重度、標題、摘要、路由標籤。接收端（Alertmanager）只照 labels 分流。

labels 完全由事件群內容決定（uid、嚴重度、等級、實體、部門），所以心跳重送時重建出來的 labels 跟上次一模一樣，
接收端才認得是同一筆。Alertmanager 用「整組 labels」辨識一則告警：嚴重度或實體變了就是另一則，
所以升級時由 alert.heartbeat 先對舊 labels 送 endsAt 關掉、再送新 labels（同一個 incident_uid，Alertmanager 會分在同一群）。
annotations 不影響辨識，每輪都依最新的事件群重算：已持續幾分鐘、累計幾筆、起跳值 -> 峰值 -> 最新值，
以及深度連結（設定了 AIOPS_GRAFANA_URL 才有）：指標大盤鎖在事故裡的服務與時間範圍、trace／log 的 Explore 查詢。
對外只用全域唯一的 incident_uid，絕不放整數 id（整數 id 在資料庫重建後會重複）。
"""
from __future__ import annotations

from datetime import datetime

from aiops.engine.context import PipelineContext
from aiops.engine.params import number
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import DEFAULT_SEVERITY, WARNING, GrafanaLinks, Incident, Notification, as_utc
from aiops.stages._common import FATAL_TAG, ensure_incidents, tagged_with
from aiops.stages.notification.links import incident_links, links_config
from aiops.engine.operator import BaseOperator

UNASSIGNED_DEPT = "unassigned"
# 由輕到重；不在表上的（自訂等級）視為跟 P3 一樣
SEVERITY_RANK = {WARNING: 0, "P3": 1, "P2": 2, "P1": 3, "P0": 4}


def severity_for(score: float, thresholds: dict[str, float]) -> str:
    for level, minimum in sorted(thresholds.items(), key=lambda kv: kv[1], reverse=True):
        if score >= minimum:
            return level
    return min(thresholds, key=thresholds.get)


def rank(severity: str) -> int:
    return SEVERITY_RANK.get(severity, 1)


def _fmt(v) -> str:
    return "—" if v is None else f"{float(v):.4g}"


def rolling_stats(incident: Incident, now: datetime | None = None) -> dict[str, str]:
    """一張卡上要看的事故全貌。峰值 = 分數最高的那一點（不一定是數值最大：流量掉到谷底也算最嚴重）。"""
    events = sorted(incident.events, key=lambda e: e.ts)
    start, last = as_utc(events[0].ts), as_utc(events[-1].ts)
    until = as_utc(now) if now is not None and as_utc(now) > last else last
    peak = max(events, key=lambda e: e.score)
    return {
        "first_seen": start.isoformat(), "last_seen": last.isoformat(),
        "duration_minutes": f"{(until - start).total_seconds() / 60:.0f}",
        "event_count": str(len(events)),
        "first_value": _fmt(events[0].value), "peak_value": _fmt(peak.value), "latest_value": _fmt(events[-1].value),
        "peak_at": as_utc(peak.ts).isoformat(), "peak_score": f"{peak.score:.2f}",
    }


ESCALATED_TAG = "escalated:"


def escalation_path(incident: Incident) -> list[str]:
    """tags 裡的升級紀錄 -> ["P3", "P2", "P0"]（沒升級過回空清單）。"""
    steps = [t[len(ESCALATED_TAG):].split(">") for t in incident.tags if t.startswith(ESCALATED_TAG)]
    return [steps[0][0], *(b for _, b in steps)] if steps else []


def build_notifications(incident: Incident, preset: str, severity: str, ends_at: datetime | None = None,
                        now: datetime | None = None, links: GrafanaLinks | None = None) -> list[Notification]:
    """一個部門一則，接收端才能用 dept 標籤直接分送；沒帶部門的歸 unassigned。
    links 有給就在 annotations 加 grafana_*_url（時間範圍 = 初發到最近一筆，前後各留 pad 分鐘），
    儀表板連結同時當 generatorURL（Grafana 卡片上的 See source）。"""
    entities = ", ".join(incident.entities)
    s = rolling_stats(incident, ends_at or now)
    urls = incident_links(links, incident.entities, incident.start, incident.end) if links else {}
    path = escalation_path(incident)
    prefix = f"[Escalated from {path[-2]}] " if len(path) >= 2 and path[-1] == severity else ""
    state = "已恢復" if ends_at is not None else "已持續"
    title = f"{prefix}[{severity}] {entities} 異常：{state} {s['duration_minutes']} 分鐘，累計 {s['event_count']} 筆"
    tags = [t for t in incident.tags if not t.startswith(ESCALATED_TAG)]
    description = (
        f"初發 {s['first_seen']}，最近一筆 {s['last_seen']}；"
        f"數值 起跳 {s['first_value']} → 峰值 {s['peak_value']}（{s['peak_at']}，分數 {s['peak_score']}）→ 最新 {s['latest_value']}"
        + (f"；嚴重度 {' → '.join(path)}" if path else "")
        + (f"；收斂方式：{', '.join(tags)}" if tags else "")
    )
    annotations = {"summary": title, "description": description, **s, **urls}
    fatal = tagged_with(incident, [FATAL_TAG])
    if fatal:  # 致命硬上限踩線：接收端（值班、自動觸發 RCA 的 webhook）靠這個欄位知道不用等統計確認
        annotations["fatal"] = ", ".join(fatal)
    if path:
        annotations["escalation_path"] = " -> ".join(path)
    return [
        Notification(
            labels={"alertname": "aiops_incident", "incident_uid": incident.uid, "severity": severity,
                    "level": incident.level, "preset": preset, "dept": dept, "entities": entities},
            annotations=dict(annotations),
            starts_at=incident.start,  # 鎖定初發時間：續命、升級、結案都用同一個 startsAt
            ends_at=ends_at,
            generator_url=urls.get("grafana_dashboard_url") or next(iter(urls.values()), None),
        )
        for dept in (incident.depts or [UNASSIGNED_DEPT])
    ]


@OperatorRegistry.register("alert.notify_payload")
class NotifyPayload(BaseOperator):
    """替每個事件群定嚴重度，並為沒被抑制的新事件群產生通知（放進 ctx.notifications，由 notifier 算子送出）。
    進行中的事件群（被 alert.absorb_open 更新過的）只重算嚴重度、只升不降（降級容易在門檻附近來回跳）；
    它們的通知由 alert.heartbeat 送，所以 heartbeat 要排在這個算子後面。
    params: severity_thresholds（分數 >= 門檻就是該等級，例如 {P0: 10, P1: 6, P2: 3, P3: 0}）、
            notify_warnings（預設 true）：只有預警（每一筆都只是接近門檻）的事件群，嚴重度固定是 warning，
            通知照發並帶 level=warning 標籤，讓 Alertmanager 分到比較輕的通道；設 false 就只記錄不通知。
            links（深度連結，選填）：{dashboard: d/apm-overview, var: service, pad_minutes: 15,
            datasources: {traces: apm-traces, logs: apm-logs}}。基底網址只從環境變數 AIOPS_GRAFANA_URL 讀，
            沒設就不產生任何連結（卡片其他內容照常）。組好的設定放進 ctx.grafana_links 給 alert.heartbeat 續命時沿用。
    """

    scope = "batch"

    def __init__(self, severity_thresholds: dict[str, float] | None = None, notify_warnings: bool = True,
                 links: dict | None = None):
        if severity_thresholds is not None and (not isinstance(severity_thresholds, dict) or not severity_thresholds):
            raise TypeError(f"severity_thresholds 必須是「嚴重度: 最低分數」的對照，不是 {severity_thresholds!r}")
        unknown = sorted(set(severity_thresholds or ()) - set(DEFAULT_SEVERITY))
        if unknown:  # 打錯的等級（p0、PO）會變成一個排不進升級順序的新等級
            raise TypeError(f"severity_thresholds 有不認得的嚴重度 {unknown}；可用：{sorted(DEFAULT_SEVERITY)}")
        self.thresholds = {k: number(f"severity_thresholds.{k}", v, minimum=0)
                           for k, v in (severity_thresholds or DEFAULT_SEVERITY).items()}
        ordered = [self.thresholds[k] for k in sorted(self.thresholds)]  # P0、P1、P2、P3
        if any(high <= low for high, low in zip(ordered, ordered[1:])):
            raise ValueError(f"severity_thresholds 必須 P0 > P1 > P2 > P3（越嚴重門檻越高），不是 {self.thresholds}")
        self.notify_warnings = bool(notify_warnings)
        self.links = dict(links or {})

    def severity_of(self, inc: Incident) -> str:
        return WARNING if inc.level == WARNING else severity_for(inc.score, self.thresholds)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        ctx.grafana_links = links_config(self.links)  # 每輪重讀環境變數：常駐進程換了設定不用重啟
        for inc in ensure_incidents(ctx):
            inc.severity = self.severity_of(inc)
            if inc.level == WARNING and not self.notify_warnings and not inc.suppressed_by:
                inc.suppressed_by = "warning_only：只是預警，這條管線設定不通知"
            if not inc.suppressed_by:
                ctx.notifications.extend(build_notifications(inc, ctx.pipeline, inc.severity, now=ctx.now,
                                                             links=ctx.grafana_links))
        for o in ctx.open_incidents:
            if not o.changed:
                continue
            new = self.severity_of(o.incident)
            if rank(new) > rank(o.severity):
                ctx.log(f"escalate:{o.uid}:{o.severity}->{new}")
                o.remember()
                o.incident.tags.append(f"{ESCALATED_TAG}{o.severity}>{new}")  # 跟著事件群存檔，之後每輪的卡都看得到
                o.severity = new
            o.incident.severity = o.severity
        return ctx
