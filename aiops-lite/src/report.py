"""把一個事故寫成 Grafana 註記：固定三行。數字與時間全部由程式產生，LLM 只提供第 2 行的文字。

  第 1 行（程式）  受影響系統、嚴重度、收斂的告警筆數
  第 2 行          [LLM 推論] …  或  [未經分析] 原始異常摘要
  第 3 行（程式）  起訖時間（台灣時間）+ 調整優先權的規則連結

Grafana 的註記文字會當 HTML 顯示（消毒過），所以換行用 <br>、連結用 <a>；系統名稱、規則名稱、LLM 的文字一律先跳脫，
日誌或模型輸出裡的 <script>、<img> 之類不會變成標籤。
"""
from __future__ import annotations

import html
from datetime import datetime
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from src.grouping import SEVERITY_RANK, Incident

MAX_LINKS = 5
STATUS_TEXT = {"open": "進行中", "closed": "已結案"}


def local_time(t: datetime, tz: str) -> str:
    return t.astimezone(ZoneInfo(tz)).strftime("%Y-%m-%d %H:%M")


def rule_link(grafana_url: str, rule_uid: str, title: str) -> str:
    """Grafana 告警規則的編輯頁：在那裡改 aiops_severity 標籤，下一輪就生效。"""
    href = f"{grafana_url.rstrip('/')}/alerting/{quote(rule_uid, safe='')}/edit"
    return f'<a href="{html.escape(href, quote=True)}" target="_blank">{html.escape(title)}</a>'


def render(incident: Incident, grafana_url: str, tz: str) -> dict[str, Any]:
    """回傳 POST／PATCH /api/annotations 的內容（time、timeEnd、tags、text）。"""
    line1 = (f"【{incident.severity}・{STATUS_TEXT[incident.status]}】"
             f"受影響系統：{html.escape('、'.join(incident.services))}｜收斂告警 {incident.alert_count} 筆")
    line2 = html.escape(" ".join(incident.summary.split()))
    end = incident.ended_at if incident.status == "closed" and incident.ended_at else None
    span = f"{local_time(incident.started_at, tz)} ～ {local_time(end, tz) if end else '進行中'}（台灣時間）"
    ranked = sorted(incident.signals.items(), key=lambda kv: (SEVERITY_RANK[kv[1]["severity"]], kv[1]["title"]))
    links = "、".join(rule_link(grafana_url, uid, f"{s['title']}（{s['severity']}）") for uid, s in ranked[:MAX_LINKS])
    more = f" 等 {len(ranked)} 條" if len(ranked) > MAX_LINKS else ""
    line3 = f"{span}｜調整優先權：{links}{more}"
    return {
        "time": int(incident.started_at.timestamp() * 1000),
        "timeEnd": int((end or incident.last_seen_at).timestamp() * 1000),
        "tags": ["aiops", uid_tag(incident.uid), incident.severity, incident.status, *incident.services],
        "text": "<br>".join([line1, line2, line3]),
    }


def uid_tag(uid: str) -> str:
    return f"aiops-incident:{uid}"
