"""各團隊自己的 Grafana 告警 -> 平台的外部異常（admin.ingest_grafana_alerts）。

權責分界（CLAUDE.md §1.1）：什麼算異常由設告警規則的團隊決定，規則放在他們自己的監控系統／Grafana，不放在平台的
aiops-grafana chart。平台只把推進來的告警轉成外部異常，下一輪由那條管線的收斂（ServiceGraph）、抑制、RCA 處理，
結論帶等級（engine/rca_types.ConclusionType）。對方沒設規則、沒有推告警，平台就不會產生任何東西。

一筆告警怎麼轉（欄位對照都寫在 pipelines/control/admin_ingest_grafana_alerts.yaml 的 params，不寫在這裡）：
  - status = resolved：不轉（略過並計數）。平台的事件群照自己的規則結案：管線的 alert.heartbeat 安靜滿 resolve_after_minutes。
  - 實體：labels 裡依序找 entity_labels 的第一個有值的標籤（預設 service、service_name）。都沒有就拒收並寫原因，不拿別的標籤猜：
    實體名稱要跟關係圖（trace 的 service.name）一致，收斂與 RCA 才對得上，這是設規則的人要對齊的（docs/EXTERNAL_ALERTS.md）。
  - 指標：labels[metric_label]（預設 alertname）。沒有就拒收。
  - 分數：labels[severity_label]（預設 severity，不分大小寫）查 severity_scores；查不到用 default_score，attrs 記 score_source。
    default_score 設成 null 就改成拒收。分數對到管線 alert.notify_payload 的 severity_thresholds（P0≥10、P1≥6、P2≥3）。
  - 時間：收到的這一分鐘（ctx.now 取整到分）。Grafana 對還在發生的告警會每隔 repeat interval 重送一次，每次重送記成一個新的點，
    事件群才不會在告警還沒結束時就因為安靜太久被結案；同一分鐘內重送的同一筆只收一次（external_id = 指紋@分鐘）。
    告警真正開始的時間（startsAt）記在 attrs。
  - 指紋：Grafana 的 fingerprint；沒有就用排好序的 labels 算一個（同一組標籤 = 同一筆告警，跟 Alertmanager 的認法一樣）。
  - attrs：alertname、labels、starts_at、generator_url、value_string、values、score_source、grafana_receiver。
    annotations（summary／description 這類自由文字）不收：平台只存結構化欄位。

不做的事：不驗證管線名稱存不存在（控制面不認識 workflow 檔；寫錯的話告警會一直等在 pending，見文件的已知限制）、
不改對方的告警內容、不替沒有實體標籤的告警猜實體。
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from typing import Any

from aiops.engine.context import PipelineContext
from aiops.engine.errors import BadRequest
from aiops.engine.operator import BaseOperator
from aiops.engine.params import number
from aiops.engine.registry import OperatorRegistry
from aiops.engine.requests import GrafanaAlertIngest
from aiops.engine.types import as_utc
from aiops.storage.repository import ExternalAnomalies

logger = logging.getLogger("aiops.control_plane")

MAX_ALERTS = 5000  # 一次推送最多幾筆（跟 IngestAnomalies 同上限）


def _starts_at(value: Any) -> str:
    """Grafana 的 startsAt -> ISO 字串（UTC）。看不懂的格式原樣留下並記一行 warning：只是佐證欄位，不因此拒收整筆告警。"""
    if not isinstance(value, str) or not value:
        return ""
    try:
        return as_utc(datetime.fromisoformat(value.replace("Z", "+00:00"))).isoformat()
    except ValueError as e:
        logger.warning("Grafana 告警的 startsAt 看不懂，原樣保留：%r（%s）", value[:64], e)
        return value[:64]


def _fingerprint(alert: dict[str, Any], labels: dict[str, str]) -> str:
    given = alert.get("fingerprint")
    if isinstance(given, str) and given:
        return given[:64]
    return hashlib.sha1(json.dumps(labels, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


@OperatorRegistry.register("admin.ingest_grafana_alerts")
class IngestGrafanaAlerts(BaseOperator):
    """params：entity_labels（依序找實體的標籤）、metric_label、severity_label、severity_scores（嚴重度 -> 分數，不分大小寫）、
    default_score（嚴重度查不到時的分數；null = 拒收）。見模組說明。"""

    scope = "batch"

    def __init__(self, entity_labels: list[str] | None = None, metric_label: str = "alertname",
                 severity_label: str = "severity", severity_scores: dict[str, float] | None = None,
                 default_score: float | None = 3.0):
        # 只有沒寫才用預設；寫了空清單是設定錯誤（不然每一筆都會被拒收，又看不出為什麼）
        self.entity_labels = ["service", "service_name"] if entity_labels is None else list(entity_labels)
        if not self.entity_labels or not all(isinstance(k, str) and k for k in self.entity_labels):
            raise ValueError("entity_labels 至少要有一個標籤名稱")
        if not isinstance(metric_label, str) or not metric_label or not isinstance(severity_label, str) or not severity_label:
            raise ValueError("metric_label、severity_label 不能是空的")
        self.metric_label, self.severity_label = metric_label, severity_label
        scores = severity_scores or {}
        self.severity_scores = {str(k).lower(): number(f"severity_scores.{k}", v, minimum=0) for k, v in scores.items()}
        self.default_score = None if default_score is None else number("default_score", default_score, minimum=0)

    def _score(self, labels: dict[str, str]) -> tuple[float | None, str]:
        severity = str(labels.get(self.severity_label, "")).lower()
        if severity in self.severity_scores:
            return self.severity_scores[severity], f"{self.severity_label}={severity}"
        return self.default_score, "default"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = ctx.request
        if not isinstance(req, GrafanaAlertIngest):
            raise BadRequest("admin.ingest_grafana_alerts 需要 GrafanaAlertIngest 請求")
        alerts = req.payload.get("alerts")
        if not isinstance(alerts, list):
            raise BadRequest("Grafana webhook 內容缺少 alerts 陣列（通知管道要選 Webhook，內容格式用預設的）")
        if len(alerts) > MAX_ALERTS:
            raise BadRequest(f"一次最多 {MAX_ALERTS} 筆告警，收到 {len(alerts)} 筆")
        minute = as_utc(ctx.now).replace(second=0, microsecond=0)
        receiver = req.payload.get("receiver") if isinstance(req.payload.get("receiver"), str) else ""
        rows: list[dict[str, Any]] = []
        rejected: list[dict[str, str]] = []
        resolved = 0
        for alert in alerts:
            if not isinstance(alert, dict):
                rejected.append({"alertname": "", "fingerprint": "", "reason": "不是 JSON 物件"})
                continue
            raw_labels = alert.get("labels") if isinstance(alert.get("labels"), dict) else {}
            labels = {str(k): str(v) for k, v in raw_labels.items()}
            alertname = labels.get(self.metric_label, "")
            fingerprint = _fingerprint(alert, labels)
            if alert.get("status") == "resolved":
                resolved += 1
                continue
            if alert.get("status") != "firing":
                rejected.append({"alertname": alertname, "fingerprint": fingerprint,
                                 "reason": f"status 必須是 firing 或 resolved，收到 {alert.get('status')!r}"})
                continue
            entity = next((labels[k] for k in self.entity_labels if labels.get(k)), "")
            if not entity:
                rejected.append({"alertname": alertname, "fingerprint": fingerprint,
                                 "reason": f"沒有實體標籤（要有 {'、'.join(self.entity_labels)} 其中一個）；"
                                           f"這筆告警的標籤：{'、'.join(sorted(labels)) or '（沒有）'}"})
                continue
            if not alertname:
                rejected.append({"alertname": "", "fingerprint": fingerprint, "reason": f"沒有 {self.metric_label} 標籤"})
                continue
            score, score_source = self._score(labels)
            if score is None:
                rejected.append({"alertname": alertname, "fingerprint": fingerprint,
                                 "reason": f"{self.severity_label} 標籤是 {labels.get(self.severity_label, '（沒有）')!r}，"
                                           f"不在 severity_scores 裡（可用：{'、'.join(sorted(self.severity_scores))}）"})
                continue
            values =alert.get("values") if isinstance(alert.get("values"), dict) else {}
            rows.append({
                "external_id": f"{fingerprint}@{minute:%Y-%m-%dT%H:%M}", "entity": entity[:256], "metric": alertname[:512],
                "ts": minute, "score": float(score), "value": None,
                "attrs": {"alertname": alertname, "labels": labels, "score_source": score_source,
                          "starts_at": _starts_at(alert.get("startsAt")),
                          "generator_url": str(alert.get("generatorURL") or "")[:1000],
                          "value_string": str(alert.get("valueString") or "")[:1000],
                          "values": {str(k): v for k, v in values.items() if isinstance(v, (int, float)) and not isinstance(v, bool)},
                          "grafana_receiver": receiver[:128]},
            })
        unique = {r["external_id"]: r for r in rows}  # 同一次推送裡同一筆告警重複出現：只收一次
        seen = ExternalAnomalies.existing_ids(req.source, list(unique))
        fresh = [{"pipeline": req.pipeline, "source": req.source, **r} for k, r in unique.items() if k not in seen]
        accepted = ExternalAnomalies.create_many(fresh)
        if rejected:
            logger.warning("Grafana 告警拒收 %d 筆（pipeline=%s source=%s）：%s", len(rejected), req.pipeline, req.source,
                           rejected[0]["reason"])
        ctx.result = {"pipeline": req.pipeline, "source": req.source, "received": len(alerts), "accepted": accepted,
                      "duplicates": len(rows) - accepted, "resolved_ignored": resolved, "rejected": rejected}
        return ctx
