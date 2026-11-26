"""註記以外的兩個出口。兩個都可以只當「佔位」：還沒有下游系統時，不送任何東西，只把「本來會送什麼」記進資料庫。

  事故 API（outputs.incident_api）    事故開單、內容更新、結案時，把事故與根因分析 POST 給下游（告警中心、派工系統）。
  自動修復（outputs.remediation）     告警規則上貼了 aiops_remediation=<流程名稱> 的告警在響時，請下游（Conductor）跑那個流程。

三種狀態，都記在 deliveries 表（跨重啟保留；同樣的內容只處理一次）：
  placeholder  沒設 url（或自動修復沒有 enabled）：沒有送出，payload 留在表裡給人看
  sent         送出去，下游回 2xx
  failed       送了但失敗：之後每一輪再試，試到 MAX_ATTEMPTS 次為止
佔位記過的不會補送：之後才把出口打開，只對打開之後的事故與內容生效。

自動修復的界線（不准放寬，除非使用者明確要求）：
  - 跑哪個流程只由告警規則上的標籤決定，是人事先寫好的。**LLM 的推論不會觸發、也不會選擇修復流程**；
    根因分析只當成說明附在請求裡。
  - 只對還在響（firing）的告警、還沒結案的事故送；同一張事故、同一條告警、同一個流程只送一次。
  - 預設關閉（enabled: false）。打開之前，下游的介面要先跟對方確認：這裡送的格式是自己定的，還沒有跟任何真的系統對過。
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from src.grouping import Incident

log = logging.getLogger("aiops_lite.outputs")

INCIDENT_API, REMEDIATION = "incident_api", "remediation"
MAX_ATTEMPTS = 10   # 送失敗最多試幾次（每一輪一次）。沒有依據的數字，未驗證泛化；調它之前先看下游多久會恢復


STATUS_TEXT = {"placeholder": "佔位，沒有送出", "sent": "已送出", "failed": "送出失敗，會再試", "gave_up": "送出失敗，已放棄"}


@dataclass(frozen=True)
class OutputOptions:
    """config.yaml 的 outputs。url 是空的 = 佔位。"""
    incident_api_url: str = ""
    incident_api_token: str = ""          # 有填就帶 Authorization: Bearer
    remediation_url: str = ""
    remediation_token: str = ""
    remediation_enabled: bool = False     # false = 就算有 url 也只記錄、不送
    timeout_seconds: float = 10.0


def _iso(t: datetime | None) -> str | None:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ") if t else None


def _key(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:32]


def incident_payload(incident: Incident, grafana_url: str, event: str) -> dict[str, Any]:
    """事故 API 的內容（格式是這裡自己定的，schema 欄位標版本）。event：opened／updated／closed。"""
    alerts = sorted(({"fingerprint": fp, "rule_uid": m["rule_uid"], "title": m["title"], "service": m["service"],
                      "severity": m["severity"], "status": m["status"], "starts_at": _iso(m["starts_at"])}
                     for fp, m in incident.signals.items()), key=lambda a: (a["starts_at"] or "", a["fingerprint"]))
    return {"schema": "aiops-lite.incident.v1", "event": event,
            "incident": {"uid": incident.uid, "status": incident.status, "severity": incident.severity, "services": incident.services,
                         "started_at": _iso(incident.started_at), "ended_at": _iso(incident.ended_at),
                         "alert_count": incident.alert_count, "alerts": alerts},
            # analyzed=false 時 summary 是原始摘要（[未經分析]）；candidates 是註記上列的 1～3 個可能原因（信心分數不是機率）
            "root_cause": {"analyzed": incident.analyzed, "summary": incident.summary,
                           "candidates": [line for line in incident.candidates.splitlines() if line.strip()]},
            "evidence": incident.evidence_note,
            "grafana": {"url": grafana_url, "annotation_id": incident.annotation_id, "tag": f"aiops-incident:{incident.uid}"}}


def remediation_payloads(incident: Incident) -> list[dict[str, Any]]:
    """這張事故現在可以請下游跑的修復流程：每一條還在響、而且規則上貼了修復流程標籤的告警一個。"""
    if incident.status != "open":
        return []
    out = []
    for fingerprint, m in sorted(incident.signals.items()):
        workflow = str(m.get("remediation") or "").strip()
        if workflow and m["status"] == "firing":
            out.append({"schema": "aiops-lite.remediation.v1", "workflow": workflow, "incident_uid": incident.uid,
                        "service": m["service"],
                        "alert": {"fingerprint": fingerprint, "rule_uid": m["rule_uid"], "title": m["title"], "severity": m["severity"]},
                        # 只是給人看的說明；下游不應該拿它當條件
                        "context": {"root_cause_analyzed": incident.analyzed, "root_cause_summary": incident.summary}})
    return out


class Outputs:
    def __init__(self, options: OutputOptions, db: Any, transport: httpx.BaseTransport | None = None):
        self.options, self.db = options, db
        self._http = httpx.Client(timeout=options.timeout_seconds, transport=transport)

    def close(self) -> None:
        self._http.close()

    def _deliver(self, channel: str, incident_uid: str, key: str, payload: dict[str, Any], url: str, token: str,
                 live: bool, now: datetime) -> tuple[str, str]:
        """處理一筆：已經處理過的不重送；佔位只記錄；失敗的留著下一輪再試。回傳 (狀態, 失敗的話是原因)。"""
        previous = self.db.delivery(channel, key)
        if previous and previous["status"] in ("sent", "placeholder"):
            # 佔位記過的之後也不補送：之後才把出口打開，不會回頭對舊事故送修復請求
            return previous["status"], ""
        attempts = previous["attempts"] if previous else 0
        if attempts >= MAX_ATTEMPTS:
            return "gave_up", ""
        if not live:
            self.db.record_delivery(channel, key, incident_uid, "placeholder", payload, "", attempts, now)
            return "placeholder", ""
        try:
            r = self._http.post(url, json=payload, headers={"Authorization": f"Bearer {token}"} if token else {})
            detail = "" if r.status_code < 300 else f"HTTP {r.status_code}: {r.text[:200]}"
        except httpx.HTTPError as e:
            detail = f"{type(e).__name__}: {e}"
        status = "failed" if detail else "sent"
        self.db.record_delivery(channel, key, incident_uid, status, payload, detail, attempts + 1, now)
        return status, detail

    def remediate(self, incident: Incident, now: datetime) -> list[str]:
        """送（或記下）這張事故的修復請求，並更新註記上「自動修復｜…」那一行。回傳錯誤訊息。"""
        o, errors = self.options, []
        live = bool(o.remediation_url and o.remediation_enabled)
        for payload in remediation_payloads(incident):
            key = _key({k: payload[k] for k in ("workflow", "incident_uid")} | {"fingerprint": payload["alert"]["fingerprint"]})
            _status, detail = self._deliver(REMEDIATION, incident.uid, key, payload, o.remediation_url, o.remediation_token, live, now)
            if detail:
                errors.append(f"事故 {incident.uid} 的修復請求 {payload['workflow']} 沒送成功：{detail}")
        # 註記上列的是這張事故處理過的全部修復請求（從資料庫讀）：告警恢復、事故結案之後，當時送過什麼還看得到
        shown = []
        for row in self.db.deliveries_of(REMEDIATION, incident.uid):
            payload = json.loads(row["payload"])
            status = "gave_up" if row["status"] == "failed" and row["attempts"] >= MAX_ATTEMPTS else row["status"]
            shown.append(f"{payload['workflow']}（{payload['service']}：{STATUS_TEXT[status]}）")
        note = f"自動修復｜{'、'.join(shown)}" if shown else ""
        if note != incident.actions_note:
            incident.actions_note, incident.changed, incident.annotated = note, True, False
        return errors

    def publish(self, incident: Incident, grafana_url: str, now: datetime) -> list[str]:
        """事故 API：內容跟上次處理過的一樣就不送。"""
        o = self.options
        event = "closed" if incident.status == "closed" else "updated" if self.db.has_delivery(INCIDENT_API, incident.uid) else "opened"
        payload = incident_payload(incident, grafana_url, event)
        # 這幾樣變了不算內容變了、不重送：註記重建換了 id；LLM 還沒通時「連續失敗幾次、幾分鐘後重試」的數字（每次重試都會變）
        steady = re.sub(r"（連續失敗 \d+ 次，約 \d+ 分鐘後重試）", "", payload["root_cause"]["summary"])
        same = {**payload, "event": "", "grafana": {**payload["grafana"], "annotation_id": None},
                "root_cause": {**payload["root_cause"], "summary": steady}}
        _status, detail = self._deliver(INCIDENT_API, incident.uid, _key(same), payload, o.incident_api_url, o.incident_api_token,
                                        bool(o.incident_api_url), now)
        return [f"事故 {incident.uid} 沒送到事故 API：{detail}"] if detail else []


