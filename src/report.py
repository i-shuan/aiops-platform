"""把一個事故寫成 Grafana 註記。主角是根因分析；數字與時間由程式產生。

  第 1 行（程式）      受影響系統、嚴重度、收斂的告警筆數
  第 2 行              根因分析｜[LLM 推論] 一句結論   或   根因分析｜[未經分析] 原始異常摘要
  列點 1～3 項         1. 系統：可能出了什麼事與依據（直接證據／間接證據／推測）。原因是模型列的，等級是程式照證據判定的；
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
    causes = lines(incident.candidates) + lines(incident.suggestions)   # 可能的原因，接著是 LLM 的下一步建議
    proof = lines(incident.evidence_note) + lines(incident.actions_note)
    last = f"{span}｜調整優先權：{links}{more}"
    return {
        "time": int(incident.started_at.timestamp() * 1000),
        "timeEnd": int((end or incident.last_seen_at).timestamp() * 1000),
        "tags": ["aiops", uid_tag(incident.uid), incident.severity, incident.status, *incident.services],
        "text": "<br>".join([line1, line2, *causes, *proof, last]),
    }


def uid_tag(uid: str) -> str:
    return f"aiops-incident:{uid}"


# ── 各行的寫法（rca 只產生結構化的結果，字怎麼排都在這裡）──

GRADE_TEXT = {"direct": "直接證據", "indirect": "間接證據", "inference": "推測"}   # gather.evidence_grade() 的等級在註記上的寫法
MAX_FACT_LINES = 5   # LLM 沒有分析時，註記上最多直接列幾行證據（版面的上限，不是判斷門檻）
FACT_CHARS = 120     # 每一行引用的原文最多幾個字
FAILURE_TEXT = {"unreachable": "LLM 目前連不上", "rejected": "LLM 的回答沒有通過檢查"}


def raw_summary(incident: Incident) -> str:
    """不經 LLM 的原始摘要：各系統哪些告警響了（不含推論）。"""
    by_service: dict[str, list[str]] = {}
    for m in sorted(incident.signals.values(), key=lambda m: (m["starts_at"], m["title"], m["service"])):
        titles = by_service.setdefault(m["service"], [])
        if m["title"] not in titles:
            titles.append(m["title"])
    return "；".join(f"{svc}：{'、'.join(titles)}" for svc, titles in by_service.items())


def alerts_fact(incident: Incident) -> str:
    return f"・告警：{raw_summary(incident)}"


def evidence_facts(incident: Incident, evidence: dict[str, Any]) -> list[str]:
    """LLM 沒有分析的時候直接貼在註記上的幾行證據。全部是程式收集到的原文與筆數，沒有推論、沒有排名次：
    第一行是哪些告警響了；再來是錯誤種類（事故前沒出現過的排前面，再照事故期間的筆數）；最後一行是呼叫鏈最底層的失敗點。"""
    clip = lambda text: " ".join(str(text).split())[:FACT_CHARS]
    lines = [alerts_fact(incident)]
    chain = next((sp for c in evidence.get("call_chains") or [] for sp in c["spans"] if sp.get("origin")), None)
    room = MAX_FACT_LINES - 1 - (1 if chain else 0)
    patterns = sorted(evidence.get("error_patterns") or [], key=lambda r: (not r.get("new", False), -r.get("during", 0)))
    for r in patterns[:room]:
        if "during" not in r:
            lines.append(f"・{r['system']}：{clip(r['example'])}")
        else:
            lines.append(f"・{r['system']}{' 新出現' if r['new'] else ''}：{clip(r['example'])}"
                         f"（事故期間 {r['during']} 筆，事故前 {r['before']} 筆）")
    if not patterns:   # 沒有做次數比較（查詢失敗或關掉了）：退而列錯誤日誌的樣本
        for e in (evidence.get("error_logs") or [])[:room]:
            lines.append(f"・{e['system']}：{clip(e['message'])}")
    if chain:
        target = (chain.get("attributes") or {}).get("peer.service")
        path = " → ".join(x for x in (chain.get("called_by") if chain.get("called_by") != chain["service"] else None,
                                      chain["service"], target) if x)
        lines.append(f"・呼叫鏈停在：{path}，{clip(chain.get('error') or chain['span'])}")
    return lines


def failure_line(kind: str, failures: int, retry_seconds: int) -> str:
    """LLM 沒有分析時註記第 2 行的內容：說明原因、什麼時候再試，並交代下面幾行是什麼。"""
    minutes = max(1, round(retry_seconds / 60))
    return (f"[未經分析] {FAILURE_TEXT.get(kind, FAILURE_TEXT['unreachable'])}（連續失敗 {failures} 次，約 {minutes} 分鐘後重試）。"
            f"以下是程式收集到的證據，沒有經過推論：")


def candidates_text(candidates: list[dict[str, Any]], max_chars: int = 300) -> str:
    """可能的原因寫成註記上的列點，依據等級放最後：1. mq-broker：…（直接證據）。等級是程式判定的，見 evidence_grade()。"""
    return "\n".join(f"{n}. {c['source']}：{c['reason'][:max_chars]}（{GRADE_TEXT[c['grade']]}）" for n, c in enumerate(candidates, 1))


def actions_text(actions: list[str]) -> str:
    """註記上的那一行：下一步建議｜1) …；2) …"""
    return "下一步建議｜" + "；".join(f"{n}) {line}" for n, line in enumerate(actions, 1)) if actions else ""
