"""把一個事故寫成 Grafana 註記。主角是根因分析；數字與時間由程式產生。

  第 1 行（程式）      受影響系統、嚴重度、收斂的告警筆數
  第 2 行              根因分析｜[LLM 推論] 一句結論   或   根因分析｜[未經分析] 原始異常摘要
  列點 1～3 項         1. 系統：可能出了什麼事與依據（信心 N）。模型列的，由最可能排到最不可能；
                       沒有直接證據指向的系統，程式把分數壓在 30 以下。沒有經過模型時沒有這幾行
  證據（程式，一行）   證據｜已有…；缺…（原因）；沒接…。只是補充，還沒收集過證據的事故沒有這一行
  最後一行（程式）     起訖時間（台灣時間）+ 調整優先權的規則連結

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

STATUS_TEXT = {"open": "進行中", "closed": "已結案"}


def local_time(t: datetime, tz: str) -> str:
    return t.astimezone(ZoneInfo(tz)).strftime("%Y-%m-%d %H:%M")


def rule_link(grafana_url: str, rule_uid: str, title: str) -> str:
    """Grafana 告警規則的編輯頁：在那裡改 aiops_severity 標籤，下一輪就生效。"""
    if not rule_uid:  # webhook 沒帶 ruleUID：只顯示名稱，不給壞掉的連結
        return html.escape(title)
    href = f"{grafana_url.rstrip('/')}/alerting/{quote(rule_uid, safe='')}/edit"
    return f'<a href="{html.escape(href, quote=True)}" target="_blank">{html.escape(title)}</a>'


def render(incident: Incident, grafana_url: str, tz: str, max_links: int = 5) -> dict[str, Any]:
    """回傳 POST／PATCH /api/annotations 的內容（time、timeEnd、tags、text）。"""
    line1 = (f"【{incident.severity}・{STATUS_TEXT[incident.status]}】"
             f"受影響系統：{html.escape('、'.join(incident.services))}｜收斂告警 {incident.alert_count} 筆")
    line2 = "根因分析｜" + html.escape(" ".join(incident.summary.split()))   # 這一行是主角；[LLM 推論]／[未經分析] 的標示緊接在後
    end = incident.ended_at if incident.status == "closed" and incident.ended_at else None
    span = f"{local_time(incident.started_at, tz)} ～ {local_time(end, tz) if end else '進行中'}（台灣時間）"
    rules: dict[tuple[str, str], str] = {}  # (rule_uid, 規則名稱) -> 這條規則在這個事故裡最高的嚴重度
    for m in incident.signals.values():
        key = (m["rule_uid"], m["title"])
        rules[key] = min(rules.get(key, m["severity"]), m["severity"], key=SEVERITY_RANK.__getitem__)
    ranked = sorted(rules.items(), key=lambda kv: (SEVERITY_RANK[kv[1]], kv[0][1]))
    links = "、".join(rule_link(grafana_url, uid, f"{title}（{sev}）") for (uid, title), sev in ranked[:max_links])
    more = f" 等 {len(ranked)} 條" if len(ranked) > max_links else ""
    lines = lambda text: [html.escape(" ".join(line.split())) for line in text.splitlines() if line.strip()]
    causes, proof = lines(incident.candidates), lines(incident.evidence_note)
    last = f"{span}｜調整優先權：{links}{more}"
    return {
        "time": int(incident.started_at.timestamp() * 1000),
        "timeEnd": int((end or incident.last_seen_at).timestamp() * 1000),
        "tags": ["aiops", uid_tag(incident.uid), incident.severity, incident.status, *incident.services],
        "text": "<br>".join([line1, line2, *causes, *proof, last]),
    }


def uid_tag(uid: str) -> str:
    return f"aiops-incident:{uid}"
