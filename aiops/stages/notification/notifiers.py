from __future__ import annotations

import base64
import html
import json
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aiops.engine.context import PipelineContext, TargetError
from aiops.engine.errors import InfrastructureError
from aiops.engine.registry import OperatorRegistry
from aiops.engine.operator import BaseOperator
from aiops.engine.types import Notification, as_utc
from aiops.stages.notification.transport import AlertmanagerTransport, GrafanaAnnotations, LogTransport, logger


@OperatorRegistry.register("notifier.log")
class LogNotifier(BaseOperator):
    """只寫日誌、不真的送。開發用。"""

    scope = "after_persist"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        LogTransport().send(ctx.notifications)
        return ctx


@OperatorRegistry.register("notifier.auto")
class AutoNotifier(BaseOperator):
    """依環境變數 AIOPS_NOTIFIER 選擇出口：log（預設，只寫日誌）或 alertmanager。
    內建 workflow 用它，部署時才決定；要在 YAML 裡固定，就改用 notifier.log／notifier.alertmanager。

    所有 notifier 都在 after_persist：事件群先寫進資料庫、通知才送出。送不出去時事件群已經是「進行中」，
    下一輪 alert.heartbeat 會用同一組標籤補送；通知紀錄與結案要等送成功（hooks.finalize_batch）才寫。
    只有 notifier.watchdog 更晚（after_finalize）：連結案都寫成功了才續命。"""

    scope = "after_persist"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        kind = os.environ.get("AIOPS_NOTIFIER", "log").strip().lower() or "log"
        if kind == "log":
            return LogNotifier().execute(ctx)
        if kind == "alertmanager":
            return AlertmanagerNotifier().execute(ctx)
        raise InfrastructureError(f"未知的 AIOPS_NOTIFIER：{kind}（可用：log、alertmanager）")


WATCHDOG_ALERTNAME = "AIOpsWatchdog"


@OperatorRegistry.register("notifier.watchdog")
class WatchdogNotifier(BaseOperator):
    """Dead Man's Snitch：每輪成功跑完（寫入、通知、結案紀錄都成功）就送一則「我還活著」的心跳告警，endsAt = 現在 + ttl。

    排程停了、Pod 掛了、資料源或 DB 連不上讓整輪失敗 -> 沒有人再續命，ttl 一到 Alertmanager 把它標成 resolved；
    在 Alertmanager 給 alertname=AIOpsWatchdog 一條 send_resolved: true 的路由（或接 Healthchecks 這類 DMS 服務），
    「心跳恢復通知」就等於「AIOps 自己掛了」。設定方式見 README「自我健康心跳」。

    params: ttl_minutes（預設 15；設成排程間隔的 2～3 倍，偶爾慢一輪不會誤報）。
    出口跟 notifier.auto 一樣看 AIOPS_NOTIFIER：log 只寫日誌（不經過 LogTransport，不會混進事件通知）。
    時間用牆上時鐘而不是 ctx.now：心跳是「程式現在活著」，回放舊資料時也一樣。
    """

    scope = "after_finalize"  # 比其他 notifier 更晚：finalize_batch（已通知、結案）寫失敗這一輪也不算活著

    def __init__(self, ttl_minutes: float = 15):
        if ttl_minutes <= 0:
            raise ValueError("ttl_minutes 必須大於 0")
        self.ttl = timedelta(minutes=float(ttl_minutes))

    def heartbeat(self, ctx: PipelineContext, now: datetime) -> Notification:
        return Notification(
            labels={"alertname": WATCHDOG_ALERTNAME, "pipeline": ctx.pipeline, "severity": "none", "source": "aiops"},
            annotations={"summary": f"AIOps 管線 {ctx.pipeline} 心跳（停止續命 = 管線沒在跑）",
                         "last_run": now.isoformat(), "target_errors": str(len(ctx.errors)),
                         "anomalies": str(len(ctx.anomalies)), "notifications": str(len(ctx.notifications))},
            starts_at=now, ends_at=now + self.ttl)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        beat = self.heartbeat(ctx, datetime.now(timezone.utc))
        kind = os.environ.get("AIOPS_NOTIFIER", "log").strip().lower() or "log"
        if kind == "alertmanager":
            url = os.environ.get("AIOPS_ALERTMANAGER_URL", "").strip()
            if not url:
                raise InfrastructureError("notifier.watchdog：AIOPS_NOTIFIER=alertmanager 但沒有設定 AIOPS_ALERTMANAGER_URL")
            AlertmanagerTransport(url, os.environ.get("AIOPS_ALERTMANAGER_TOKEN") or None).send([beat])
        elif kind == "log":
            logger.info("heartbeat %s", json.dumps(beat.labels | beat.annotations, ensure_ascii=False))
        else:
            raise InfrastructureError(f"未知的 AIOPS_NOTIFIER：{kind}（可用：log、alertmanager）")
        ctx.result["heartbeat"] = {"ends_at": beat.ends_at.isoformat()}
        ctx.log(f"heartbeat:{kind}")
        return ctx


@OperatorRegistry.register("notifier.alertmanager")
class AlertmanagerNotifier(BaseOperator):
    """把 ctx.notifications 推到 Alertmanager。params: endpoint（不給就用環境變數 AIOPS_ALERTMANAGER_URL）。
    token 只從環境變數 AIOPS_ALERTMANAGER_TOKEN 讀，不寫在 YAML。送不出去丟 InfrastructureError，整輪失敗、不會假裝成功。
    """

    scope = "after_persist"

    def __init__(self, endpoint: str | None = None):
        self.endpoint = endpoint

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        url = self.endpoint or os.environ.get("AIOPS_ALERTMANAGER_URL", "").strip()
        if not url:
            raise InfrastructureError("notifier.alertmanager 沒有 endpoint，也沒有設定 AIOPS_ALERTMANAGER_URL")
        AlertmanagerTransport(url, os.environ.get("AIOPS_ALERTMANAGER_TOKEN") or None).send(ctx.notifications)
        return ctx


def _grafana_annotations() -> GrafanaAnnotations | None:
    """依環境變數建 Grafana 註記的出口；網址或認證沒設回 None（不寫註記）。跟讀資料用同一組設定
    （AIOPS_GRAFANA_URL＋AIOPS_GRAFANA_TOKEN，或 USER／PASSWORD），但寫註記需要 Editor 權限。"""
    url, token = os.environ.get("AIOPS_GRAFANA_URL", "").strip(), os.environ.get("AIOPS_GRAFANA_TOKEN", "").strip()
    user, password = os.environ.get("AIOPS_GRAFANA_USER", "").strip(), os.environ.get("AIOPS_GRAFANA_PASSWORD", "").strip()
    if url and token:
        return GrafanaAnnotations(url, {"Authorization": f"Bearer {token}"})
    if url and user and password:
        basic = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        return GrafanaAnnotations(url, {"Authorization": f"Basic {basic}"})
    return None


DISPLAY_TZ_ENV = "AIOPS_DISPLAY_TZ"  # 註記文字裡的時間用哪個時區（IANA 名稱，例如 Asia/Taipei）；沒設 = UTC
TZ_LABEL = {"Asia/Taipei": "台灣時間", "UTC": "UTC"}
_ISO_TIME = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})")


def _display_tz() -> tuple[ZoneInfo, str]:
    name = os.environ.get(DISPLAY_TZ_ENV, "").strip() or "UTC"
    try:
        return ZoneInfo(name), TZ_LABEL.get(name, name)
    except (ZoneInfoNotFoundError, ValueError):
        return ZoneInfo("UTC"), "UTC"


def _clock(ts: datetime, tz: ZoneInfo) -> str:
    return ts.astimezone(tz).strftime("%H:%M")


def _localize(text: str, tz: ZoneInfo) -> str:
    """文字裡的 ISO 時間（演算法的理由常帶 2026-11-20T13:43:00+00:00）換成顯示時區的 HH:MM，跟 Grafana 圖表的 X 軸對得起來。"""
    return _ISO_TIME.sub(lambda m: _clock(datetime.fromisoformat(m.group(0).replace("Z", "+00:00")), tz), text)


def _headline(severity: str, entities: list[str], count: int | str) -> str:
    """第一行：幾個實體、累計幾筆異常。多個實體併成一件時寫出來——這是收斂做的事。"""
    names = html.escape(" / ".join(entities))
    if len(entities) > 1:
        return f"[{html.escape(severity)} 跨系統異常收斂] {names} 累計 {count} 筆異常 → 自動合併為 1 件"
    return f"[{html.escape(severity)} 異常] {names} 累計 {count} 筆異常"


def _priority_link(rule_urls: str, pipeline: str) -> str:
    """回 Grafana 調整優先權的連結：一條，指到告警規則清單、篩出交給這條管線的規則（aiops_detect 標籤）。
    網址的前半段取自規則連結（…/alerting/grafana/<uid>/view），沒有規則連結（外部告警的事件群）就沒有這條。"""
    first = next(iter(rule_urls.split()), "")
    if "/alerting/" not in first or not pipeline:
        return ""
    url = first.split("/alerting/")[0] + "/alerting/list?search=" + quote(f"label:aiops_detect={pipeline}")
    return f'｜<a href="{html.escape(url, quote=True)}">點此調整告警優先權</a>'


def _footer(start: datetime, end: datetime, rule_urls: str, pipeline: str) -> str:
    tz, label = _display_tz()
    return f"異常起訖：{_clock(start, tz)} ~ {_clock(end, tz)}（{label}）" + _priority_link(rule_urls, pipeline)


# 註記上的結論等級用短的說法（完整的說明在 RCA 報告的「可能的源頭」段落）
CONCLUSION_SHORT = {"confirmed": "有直接證據", "symptom_location": "僅症狀位置，根因未確認",
                    "topology_inference": "依關係圖推測", "undetermined": "無法判定"}


@OperatorRegistry.register("notifier.grafana_annotations")
class GrafanaAnnotationNotifier(BaseOperator):
    """把事件群標在 Grafana 圖表的時間軸上（Annotations API），使用者不用離開 Grafana 就看得到平台判了什麼。

    註記是三行給人看的摘要（GrafanaAnnotations）：
      [P0 跨系統異常收斂] mq-broker / cim-eap / amhs-mcs 累計 161 筆異常 → 自動合併為 1 件
      RCA 根因（依關係圖推測）：mq-broker（首次異常 21:43（事件群中最早）…；連帶影響 cim-eap、amhs-mcs）
      異常起訖：21:43 ~ 22:00（台灣時間）｜點此調整告警優先權
    時間用 AIOPS_DISPLAY_TZ 的時區（沒設 = UTC），跟 Grafana 圖表的 X 軸對齊。結論等級照實寫（推測就寫推測）。
    放在兩種管線：
      - 偵測管線（notify 之後）：這一輪送出通知的每個事件群更新第一、三行；事件群持續中不會越標越多。
      - RCA 管線（最後一個節點）：更新第二行。
    標籤：aiops、inc:<uid 前 8 碼>、severity:<P0～P3>、entity:<實體>——儀表板用標籤篩（deploy/helm/aiops-grafana 的 aiops-incidents）。

    這是旁路：Grafana 沒設定就不做；寫失敗只記進 errors（實體 node:grafana_annotations），不影響通知、結案與 RCA 報告。
    不放進 after_finalize、也不擋心跳：註記寫不進去不代表告警沒送到。"""

    scope = "after_persist"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        client = _grafana_annotations()
        if client is None:
            ctx.log("grafana_annotations:skip:not-configured")
            return ctx
        try:
            written = self._rca(ctx, client) if ctx.rca_root_algorithm else self._alerts(ctx, client)
            ctx.log(f"grafana_annotations:{written}")
        except InfrastructureError as e:
            ctx.errors.append(TargetError("node:grafana_annotations", f"Grafana 註記沒寫成：{e}"))
        return ctx

    @staticmethod
    def _alerts(ctx: PipelineContext, client: GrafanaAnnotations) -> int:
        # 同一個事件群這一輪可能有好幾則通知：升級或實體變多時，alert.heartbeat 先送一則「舊標籤結束」再送新的。
        # 註記要寫的是現在的樣子，所以取每個事件群的最後一則（實測：取第一則會把註記停在升級前，寫成「已恢復」）
        latest = {n.labels["incident_uid"]: n for n in ctx.notifications
                  if n.labels.get("incident_uid") and n.labels.get("alertname") == "aiops_incident"}
        seen = set(latest)
        for uid, n in latest.items():
            a = n.annotations
            start = datetime.fromisoformat(a["first_seen"])
            end = datetime.fromisoformat(a.get("last_seen") or a["first_seen"])
            entities = [e.strip() for e in n.labels.get("entities", "").split(",") if e.strip()]
            severity = n.labels.get("severity", "")
            client.upsert(uid, start, end, [f"severity:{severity}", *(f"entity:{e}" for e in entities)],
                          headline=_headline(severity, entities, a.get("event_count", "")),
                          footer=_footer(start, end, a.get("grafana_rule_url", ""), ctx.pipeline))
        return len(seen)

    @staticmethod
    def _rca(ctx: PipelineContext, client: GrafanaAnnotations) -> int:
        if not ctx.incidents:
            return 0
        incident = ctx.incidents[0]
        tz, _ = _display_tz()
        label = CONCLUSION_SHORT.get(ctx.rca_conclusion_type, ctx.rca_conclusion_type)
        if ctx.rca_root_candidates:
            top = ctx.rca_root_candidates[0]
            others = [e for e in incident.entities if e != top.entity]
            affected = f"；連帶影響 {'、'.join(others)}" if others else ""
            rca = f"RCA 根因（{label}）：<b>{html.escape(top.entity)}</b>（{html.escape(_localize(top.reason, tz)[:240])}{html.escape(affected)}）"
        else:
            why = _localize(ctx.rca_conclusion_reason or ctx.rca_root_note or "", tz)[:240]
            rca = f"RCA：{label}（{html.escape(why)}）" if why else f"RCA：{label}"
        tags = [f"severity:{incident.severity}", *(f"entity:{e}" for e in incident.entities)]
        if client.find(incident.uid):
            client.upsert(incident.uid, incident.start, incident.end, tags, rca=rca)
        else:  # 這個事件群還沒有註記（外部告警的事件群，或註記被清掉了）：另外兩行從事件群本身補上
            urls = " ".join(sorted({str(e.attrs["grafana_rule_url"]) for e in incident.events if e.attrs.get("grafana_rule_url")}))
            client.upsert(incident.uid, incident.start, incident.end, tags, rca=rca,
                          headline=_headline(incident.severity, list(incident.entities), len(incident.events)),
                          footer=_footer(as_utc(incident.start), as_utc(incident.end), urls, ctx.incident_pipeline))
        return 1

