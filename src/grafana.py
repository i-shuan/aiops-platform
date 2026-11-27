"""跟公司 Grafana 講話的唯一出口：只用 config.yaml 的 grafana.url + 一把 Editor Token，不直連任何資料庫。

  parse_webhook       解析 Grafana 告警 webhook（Contact Point 推進來的）：哪條規則、哪個系統、嚴重度、firing／resolved
  Grafana.request     所有 HTTP 呼叫共用（重試、錯誤訊息）。日誌與 Trace 的查詢在 src/logs.py、src/traces.py，都經這裡走資料源代理
  Grafana.query_range 指標：GET /api/datasources/proxy/uid/<uid>/api/v1/query_range（Prometheus 相容：VictoriaMetrics、Prometheus、Mimir）
  Grafana.query_instant  同一種資料源的單點查詢（/api/v1/query），讀 Tempo service graph 的指標用
  Grafana.upsert_annotation  POST／PATCH /api/annotations，每個事故一筆註記

格式以 tests/fixtures/grafana13/ 為準：Grafana 13.0.7 實際送出與回應的內容。
"""
from __future__ import annotations

import json
import logging
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from src.grouping import Alert

log = logging.getLogger("aiops_lite.grafana")

RETRY_STATUS = {429, 502, 503, 504}
SEVERITIES = ("P0", "P1", "P2", "P3")
TRUE_VALUES = {"true", "1", "yes", "on"}


@dataclass(frozen=True)
class LabelNames:
    """告警規則上的標籤名稱（config.yaml 的 labels）。default_severity：嚴重度標籤沒寫或寫錯時當成哪一級。
    remediation：規則上有這個標籤，值就是「這條告警響的時候可以跑的修復流程名稱」（見 src/outputs.py）。"""
    detect: str = "aiops_detect"
    severity: str = "aiops_severity"
    service: str = "service"
    default_severity: str = "P3"
    remediation: str = "aiops_remediation"


class GrafanaError(Exception):
    """Grafana 連不上、Token 沒權限、回應不是預期的格式。"""


class WebhookError(ValueError):
    """推進來的內容不是 Grafana 告警 webhook 的格式：整包拒收（HTTP 400）。"""


def _when(raw: Any) -> datetime | None:
    """Grafana 的時間（RFC3339，可能帶奈秒）；還沒結束的告警 endsAt 是 0001-01-01。"""
    text = str(raw or "")
    if not text or text.startswith("0001-"):
        return None
    # 小數秒最多留 6 位（Grafana 給奈秒，舊版 Python 的 fromisoformat 不吃）
    return datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", text.replace("Z", "+00:00"))).astimezone(timezone.utc)


def parse_webhook(payload: Any, names: LabelNames = LabelNames()) -> tuple[list[Alert], list[str]]:
    """回傳 (納管的告警, 問題清單)。只收貼了 aiops_detect=true 的告警；少了 service 標籤的略過並列出來。
    標籤名稱由 names 決定（預設 aiops_detect／aiops_severity／service）。
    格式不對（不是物件、沒有 alerts、告警缺 fingerprint／status／startsAt）丟 WebhookError，整包不收。"""
    if not isinstance(payload, dict) or not isinstance(payload.get("alerts"), list):
        raise WebhookError("不是 Grafana 告警 webhook：要有 alerts 陣列")
    alerts, problems = [], []
    for item in payload["alerts"]:
        if not isinstance(item, dict) or not isinstance(item.get("labels"), dict):
            raise WebhookError("alerts 裡的每一筆都要是帶 labels 的物件")
        labels = item["labels"]
        title = str(labels.get("alertname", ""))
        status, fingerprint, starts = item.get("status"), str(item.get("fingerprint") or ""), _when(item.get("startsAt"))
        if status not in ("firing", "resolved") or not fingerprint or starts is None:
            raise WebhookError(f"告警「{title}」缺 status／fingerprint／startsAt")
        if str(labels.get(names.detect, "")).strip().lower() not in TRUE_VALUES:
            continue
        service = str(labels.get(names.service, "")).strip()
        if not service:
            problems.append(f"告警「{title}」沒有 {names.service} 標籤：略過")
            continue
        severity = str(labels.get(names.severity, "")).strip().upper()
        if severity not in SEVERITIES:
            problems.append(f"告警「{title}」的 {names.severity}={labels.get(names.severity)!r} 不是 P0～P3："
                            f"當成 {names.default_severity}")
            severity = names.default_severity
        values = item.get("values") if isinstance(item.get("values"), dict) else {}
        alerts.append(Alert(fingerprint=fingerprint, status=status, rule_uid=str(item.get("ruleUID") or ""),
                            title=title, service=service, severity=severity, starts_at=starts,
                            ends_at=_when(item.get("endsAt")) if status == "resolved" else None, values=values,
                            remediation=str(labels.get(names.remediation, "")).strip()))
    return alerts, problems


class Grafana:
    def __init__(self, url: str, token: str, transport: httpx.BaseTransport | None = None, timeout: float = 30.0,
                 retries: int = 2, retry_backoff_seconds: float = 0.5):
        self.url = url.rstrip("/")
        self.retries, self.retry_backoff_seconds = retries, retry_backoff_seconds
        self._http = httpx.Client(base_url=self.url, timeout=timeout, transport=transport,
                                  headers={"Authorization": f"Bearer {token}"})

    def close(self) -> None:
        self._http.close()

    def request(self, method: str, path: str, *, accept: tuple[int, ...] = (), with_status: bool = False,
                 empty_ok: bool = False, raw: bool = False, **kw: Any) -> Any:
        """短暫抖動（連線失敗、逾時、429／502／503／504）重試 retries 次（預設 2），每次多等 retry_backoff_seconds；
        其他錯誤直接丟 GrafanaError。"""
        last = ""
        for attempt in range(self.retries + 1):
            if attempt:
                time.sleep(self.retry_backoff_seconds * attempt)
            try:
                r = self._http.request(method, path, **kw)
            except httpx.TransportError as e:
                last = f"{type(e).__name__}: {e}"
                continue
            if r.status_code in RETRY_STATUS:
                last = f"HTTP {r.status_code}"
                continue
            if r.status_code >= 400 and r.status_code not in accept:
                # Victoria 的錯誤訊息前面有一段 remoteAddr 與整條網址（requestURI），真正的原因在最後一段
                reason = r.text.split("; ", 2)[-1] if r.text.startswith("remoteAddr:") else r.text
                raise GrafanaError(f"{method} {path} -> HTTP {r.status_code}: {reason.strip()[:300]}")
            if raw:   # VictoriaLogs 一行回一個 JSON 物件，不是一份 JSON
                return r.text
            try:
                data = None if empty_ok and not r.content.strip() else r.json()
            except ValueError as e:
                raise GrafanaError(f"{method} {path} 回的不是 JSON：{r.text[:200]}") from e
            return (r.status_code, data) if with_status else data
        raise GrafanaError(f"{method} {path} 重試後仍失敗：{last}")

    # ── 指標（經 Grafana 的 Prometheus 相容資料源代理：Prometheus、VictoriaMetrics、Mimir…）──

    def query_range(self, datasource_uid: str, expr: str, start: datetime, end: datetime, step_seconds: int
                    ) -> list[dict[str, Any]]:
        """區間查詢（Prometheus HTTP API 的 /api/v1/query_range）。回傳每條序列 {"labels": {...}, "points": [(時間, 數值)]}；
        NaN 與無限大的點丟掉（除以零的查詢會回這種值，算不了統計）。查詢語法錯、資料源不對丟 GrafanaError。"""
        raw = self.request("GET", f"/api/datasources/proxy/uid/{datasource_uid}/api/v1/query_range", params={
            "query": expr, "start": int(start.timestamp()), "end": int(end.timestamp()), "step": step_seconds})
        data = raw.get("data") if isinstance(raw, dict) else None
        if not isinstance(data, dict) or raw.get("status") != "success" or not isinstance(data.get("result"), list):
            raise GrafanaError(f"指標查詢的回應不是預期的格式：{str(raw)[:200]}")
        if data.get("resultType") != "matrix":
            raise GrafanaError(f"指標查詢要回時間序列（matrix），這條回的是 {data.get('resultType')}")
        series = []
        for item in data["result"]:
            points = []
            for ts, value in item.get("values") or []:
                number = float(value)
                if math.isfinite(number):
                    points.append((datetime.fromtimestamp(float(ts), timezone.utc), number))
            series.append({"labels": dict(item.get("metric") or {}), "points": points})
        return series

    def query_instant(self, datasource_uid: str, expr: str, at: datetime) -> list[dict[str, Any]]:
        """單一時間點的查詢（Prometheus HTTP API 的 /api/v1/query）。回傳每條序列 {"labels": {...}, "value": 數值}；
        NaN 與無限大的丟掉。查詢語法錯、資料源不對丟 GrafanaError。"""
        raw = self.request("GET", f"/api/datasources/proxy/uid/{datasource_uid}/api/v1/query", params={
            "query": expr, "time": int(at.timestamp())})
        data = raw.get("data") if isinstance(raw, dict) else None
        if not isinstance(data, dict) or raw.get("status") != "success" or not isinstance(data.get("result"), list):
            raise GrafanaError(f"指標查詢的回應不是預期的格式：{str(raw)[:200]}")
        if data.get("resultType") != "vector":
            raise GrafanaError(f"單點查詢要回 vector，這條回的是 {data.get('resultType')}")
        out = []
        for item in data["result"]:
            number = float((item.get("value") or [0, "nan"])[1])
            if math.isfinite(number):
                out.append({"labels": dict(item.get("metric") or {}), "value": number})
        return out

    # ── 註記 ──

    def upsert_annotation(self, annotation_id: int | None, uid_tag: str, body: dict[str, Any]) -> int:
        """建立或更新一筆註記，回傳它的 id。沒有 id 時先用事故標籤找（上一輪建立成功、但 id 沒存到的情況不會重複建）。

        標籤有變（open→closed、嚴重度升級、新系統加入）就**刪掉舊的、建一筆新的**，不用 PATCH：Grafana 13.0.7 實測，
        更新註記的標籤之後，用「舊標籤」查還是查得到它（標籤索引沒有跟著更新），儀表板的「進行中」清單會一直列著已結案的事故。
        只有文字、時間變（標籤相同）才原地 PATCH。"""
        if annotation_id is None:
            found = self.request("GET", "/api/annotations", params={"tags": uid_tag, "limit": 1})
            if isinstance(found, list) and found:
                annotation_id = int(found[0]["id"])
        if annotation_id is not None:
            status, current = self.request("GET", f"/api/annotations/{annotation_id}", accept=(403, 404), with_status=True)
            if status in (403, 404):
                annotation_id = None  # 被人刪掉了（Grafana 對不存在的 id 有時回 403，有時回 404）：重建；真的沒權限的話下面建立時會報錯
            elif set(current.get("tags") or []) == set(body["tags"]):
                self.request("PATCH", f"/api/annotations/{annotation_id}", json=body)
                return annotation_id
            else:
                self.request("DELETE", f"/api/annotations/{annotation_id}", accept=(403, 404))
        return int(self.request("POST", "/api/annotations", json=body)["id"])
