from __future__ import annotations

import base64
import html
import json
import os
from datetime import datetime, timedelta, timezone

from aiops.engine.context import PipelineContext, TargetError
from aiops.engine.errors import InfrastructureError
from aiops.engine.registry import OperatorRegistry
from aiops.engine.operator import BaseOperator
from aiops.engine.types import Notification
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


def _links(urls: str) -> str:
    return "".join(f'<br><a href="{html.escape(u, quote=True)}">看這條規則、調整優先權</a>' for u in urls.split())


# 註記上的結論等級用短的說法（完整的說明在 RCA 報告的「可能的源頭」段落）
CONCLUSION_SHORT = {"confirmed": "有直接證據", "symptom_location": "僅症狀位置，根因未確認",
                    "topology_inference": "依關係圖推測", "undetermined": "無法判定"}


@OperatorRegistry.register("notifier.grafana_annotations")
class GrafanaAnnotationNotifier(BaseOperator):
    """把事件群標在 Grafana 圖表的時間軸上（Annotations API），使用者不用離開 Grafana 就看得到平台判了什麼。

    放在兩種管線：
      - 偵測管線（notify 之後）：這一輪送出通知的每個事件群一筆註記——嚴重度、摘要、連回 Grafana 規則的連結；
        事件群持續中每一輪更新結束時間與文字，不會越標越多。
      - RCA 管線（最後一個節點）：把結論等級與第一名候選補進同一筆註記。
    標籤：aiops、incident:<uid>、severity:<P0～P3>、entity:<實體>——儀表板用標籤篩（deploy/helm/aiops-grafana 的 aiops-incidents）。

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
        seen: set[str] = set()
        for n in ctx.notifications:
            uid = n.labels.get("incident_uid", "")
            if not uid or uid in seen or n.labels.get("alertname") != "aiops_incident":
                continue
            seen.add(uid)
            a = n.annotations
            start = datetime.fromisoformat(a["first_seen"])
            end = datetime.fromisoformat(a.get("last_seen") or a["first_seen"])
            entities = [e.strip() for e in n.labels.get("entities", "").split(",") if e.strip()]
            text = html.escape(a.get("summary", "")) + _links(a.get("grafana_rule_url", ""))
            client.upsert(uid, start, end, [f"severity:{n.labels.get('severity', '')}", *(f"entity:{e}" for e in entities)], alert=text)
        return len(seen)

    @staticmethod
    def _rca(ctx: PipelineContext, client: GrafanaAnnotations) -> int:
        if not ctx.incidents:
            return 0
        incident = ctx.incidents[0]
        label = CONCLUSION_SHORT.get(ctx.rca_conclusion_type, ctx.rca_conclusion_type)
        if ctx.rca_root_candidates:
            top = ctx.rca_root_candidates[0]
            text = f"<b>根因（{label}）：{html.escape(top.entity)}</b><br>{html.escape(top.reason[:400])}"
        else:
            text = f"<b>根因：{label}</b><br>{html.escape((ctx.rca_conclusion_reason or ctx.rca_root_note or '')[:400])}"
        entities = [f"entity:{e}" for e in incident.entities]
        # 這個事件群還沒有註記（例如外部告警的事件群，偵測管線沒掛這個節點）：連告警那一段一起補上
        alert = None if client.find(incident.uid) else html.escape(f"[{incident.severity}] {', '.join(incident.entities)} 異常")
        client.upsert(incident.uid, incident.start, incident.end, [f"severity:{incident.severity}", *entities], alert=alert, rca=text)
        return 1

