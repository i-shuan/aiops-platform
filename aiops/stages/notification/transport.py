"""通知出口的傳輸實作。只負責把已決定好的通知送出去，不做任何判斷。"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import httpx

from aiops.engine.errors import InfrastructureError
from aiops.engine.types import Notification

logger = logging.getLogger("aiops.notifiers")


def _iso_utc(dt: datetime) -> str:
    dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


class AlertmanagerTransport:
    """推到 Alertmanager v2 API：POST {base_url}/api/v2/alerts。接收端只依 labels 路由到 Contact Point。"""

    def __init__(self, base_url: str, token: str | None = None, timeout: float = 5.0):
        self.url = base_url.rstrip("/") + "/api/v2/alerts"
        self.token = token
        self.timeout = timeout

    def to_body(self, notifications: list[Notification]) -> list[dict]:
        body = []
        for n in notifications:
            alert = {"labels": n.labels, "annotations": n.annotations, "startsAt": _iso_utc(n.starts_at)}
            if n.ends_at is not None:
                alert["endsAt"] = _iso_utc(n.ends_at)
            if n.generator_url:
                alert["generatorURL"] = n.generator_url  # Grafana 卡片上的 See source
            body.append(alert)
        return body

    def send(self, notifications: list[Notification]) -> None:
        if not notifications:
            return
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        try:
            resp = httpx.post(self.url, json=self.to_body(notifications), headers=headers, timeout=self.timeout)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise InfrastructureError(f"推送通知到 {self.url} 失敗：{e}") from e


class LogTransport:
    """只寫日誌、不保留任何東西（AIOPS_NOTIFIER 沒設時的預設出口，常駐進程裡也會一直被呼叫）。
    正式環境請用 alertmanager。測試要檢查送了什麼，由測試自己包 send 來錄（tests/conftest.py）。"""

    def send(self, notifications: list[Notification]) -> None:
        for n in notifications:
            logger.info(json.dumps({"labels": n.labels, "annotations": n.annotations}, ensure_ascii=False))


class GrafanaAnnotations:
    """Grafana 註記（Annotations API）：把事件群標在 Grafana 圖表的時間軸上。一個事件群一筆註記，用標籤
    inc:<uid 的前 8 碼> 找回來更新（平台不另外存註記的編號；完整的 uid 太長，在浮出視窗上只是一串看不懂的字）。

    註記的文字固定三行，用 <br> 隔開，各自更新、互不覆蓋：
      第一行 headline：嚴重度、哪些實體、累計幾筆異常（偵測管線每一輪更新）
      第二行 rca     ：RCA 的結論（跑 RCA 時更新；還沒跑過是 RCA_PENDING）
      第三行 footer  ：異常起訖、回 Grafana 調整優先權的連結（偵測管線每一輪更新）
    三行裡不能再有 <br>（呼叫端的文字都先跳脫過）。Grafana 顯示註記時會過濾 HTML，只留連結、換行、粗體這類安全的標記。

    需要的權限比其他地方多：讀資料只要 Viewer，寫註記要 Editor 的 service account token。
    已在 Grafana 13.0.7 驗證（2026-11-20）：POST／PATCH／GET /api/annotations，tags 用 AND 篩選。"""

    TAG = "aiops"
    BREAK = "<br>"
    BREAK_COUNT = 2  # 三行之間的換行數
    RCA_PENDING = "RCA：尚未分析"

    def __init__(self, base_url: str, headers: dict[str, str], client: httpx.Client | None = None, timeout: float = 5.0):
        self.base_url, self.headers = base_url.rstrip("/"), dict(headers)
        self.client = client or httpx.Client(timeout=timeout)

    def _call(self, method: str, path: str, **kwargs) -> httpx.Response:
        try:
            resp = self.client.request(method, f"{self.base_url}{path}", headers=self.headers, **kwargs)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise InfrastructureError(f"Grafana 註記 {method} {path} 失敗：{e}") from e
        return resp

    @staticmethod
    def incident_tag(incident_uid: str) -> str:
        return "inc:" + incident_uid.removeprefix("inc-")[:8]

    def find(self, incident_uid: str) -> dict | None:
        rows = self._call("GET", "/api/annotations", params={"tags": [self.TAG, self.incident_tag(incident_uid)], "limit": 1,
                                                             "from": 0, "to": 4102444800000}).json()
        return rows[0] if rows else None

    def upsert(self, incident_uid: str, start: datetime, end: datetime, tags: list[str], headline: str | None = None,
               footer: str | None = None, rca: str | None = None) -> int:
        """建立或更新這個事件群的註記；headline／footer／rca 給 None = 那一行保留原樣。回傳註記編號。"""
        existing = self.find(incident_uid)
        text = (existing or {}).get("text", "")
        # 剛好兩個換行才是這個類別寫的三行；不是的（別的版本寫的，或還沒有註記）不沿用
        three_lines = text.count(self.BREAK) == self.BREAK_COUNT
        old_headline, old_rca, old_footer = text.split(self.BREAK) if three_lines else ("", self.RCA_PENDING, "")
        lines = [old_headline if headline is None else headline, old_rca if rca is None else rca,
                 old_footer if footer is None else footer]
        body = {"time": int(start.timestamp() * 1000), "timeEnd": int(end.timestamp() * 1000),
                "tags": sorted({self.TAG, self.incident_tag(incident_uid), *tags}), "text": self.BREAK.join(lines)}
        if existing:
            if headline is None:  # RCA 只補上結論：時間範圍與嚴重度標籤以告警那一邊為準
                body = {"text": body["text"], "tags": sorted({*existing.get("tags", []), *body["tags"]})}
            self._call("PATCH", f"/api/annotations/{existing['id']}", json=body)
            return int(existing["id"])
        return int(self._call("POST", "/api/annotations", json=body).json()["id"])
