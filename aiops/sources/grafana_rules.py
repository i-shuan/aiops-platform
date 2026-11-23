"""Grafana 告警規則 -> 偵測對象與時序。純資料轉換，不連線（連線在 sources/grafana.py）。

使用者在自己的 Grafana 上決定「哪條序列交給平台的演算法看、判出來是哪個嚴重度」，平台不另外存這份設定：

  在一條 Grafana 告警規則上加標籤
    aiops_detect: <管線名稱>     這條規則的查詢交給這條偵測管線的演算法看（規則自己會不會響、有沒有暫停都不影響）
    aiops_severity: P0｜P1｜P2｜P3  演算法在這條序列上判出異常時的嚴重度。沒有這個標籤就看 severity 標籤是不是 P0～P3；
                                 都不是（沒寫、或 severity 寫的是 critical／high 這類給 Grafana 自己告警用的字）= 管線的預設（P1）。
                                 分兩個標籤的原因：同一條規則可能同時是 Grafana 自己的告警（severity 給接收端對照用），兩邊的等級不必一樣
    service: <名稱>              事件的實體（沒有就用 aiops_entity，再沒有就用規則標題）

平台每一輪從 Grafana 讀規則（GET /api/v1/provisioning/alert-rules），拿規則「自己的查詢」向 Grafana 要一段時序
（POST /api/ds/query）。查詢語法是使用者在 Grafana 寫好的那一條，平台不組任何後端的查詢語法，
所以後端是 Prometheus、VictoriaMetrics 還是別的，對平台沒有差別。

已在 Grafana 13.0.7 驗證（2026-11-20，演練叢集的真實回應存在 tests/fixtures/grafana13/）：
  - 規則的形狀：uid、title、labels、isPaused、data[]（refId、datasourceUid、model；運算式那幾筆的 datasourceUid 是 __expr__）
  - Prometheus 資料源：把規則的 model 改成 range 查詢送 /api/ds/query，回 results.<refId>.frames[]，
    每個 frame 一條序列：schema.fields = [時間欄(type=time), 數值欄(type=number, labels=序列標籤)]，
    data.values 是欄優先（values[0] 是毫秒時間戳、values[1] 是值）
未驗證：VictoriaMetrics 資料源（Prometheus 相容，預期同形狀）、Elasticsearch／Loki 當指標來源、service account token
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any

DETECT_LABEL = "aiops_detect"
SEVERITY_LABELS = ("aiops_severity", "severity")  # 前面的優先
ENTITY_LABELS = ("aiops_entity", "service")
SEVERITIES = ("P0", "P1", "P2", "P3")
EXPRESSION_DATASOURCE = "__expr__"
REF_ID = "A"


def rule_severity(rule: dict[str, Any]) -> str | None:
    """規則標籤上的嚴重度；不是 P0～P3（沒寫、或寫成 critical 這類給人看的字）回 None = 用管線的預設。"""
    labels = rule.get("labels") or {}
    for name in SEVERITY_LABELS:
        value = str(labels.get(name, "")).strip().upper()
        if value in SEVERITIES:
            return value
    return None


def rule_entity(rule: dict[str, Any]) -> str:
    labels = rule.get("labels") or {}
    return next((str(labels[k]) for k in ENTITY_LABELS if labels.get(k)), str(rule.get("title") or rule["uid"]))


def detection_targets(rules: list[dict[str, Any]], pipeline: str, base_url: str = "") -> list[dict[str, Any]]:
    """標了 aiops_detect: <pipeline> 的規則 -> 偵測管線的 target（engine/pipeline.TargetSpec 的欄位）。

    query 是規則的 uid（事件的 metric 也會是它：穩定、在 Grafana 找得到）。嚴重度與回到 Grafana 的連結放在 attrs，
    跟著事件走到通知：attrs.severity 由 alert.notify_payload 讀、attrs.grafana_rule_url 放上告警卡。
    依 uid 排序：同一組規則每一輪的順序都一樣。"""
    targets = []
    for rule in sorted(rules, key=lambda r: str(r.get("uid", ""))):
        if str((rule.get("labels") or {}).get(DETECT_LABEL, "")).strip() != pipeline:
            continue
        attrs: dict[str, Any] = {"grafana_rule_uid": rule["uid"], "grafana_rule_title": str(rule.get("title") or "")}
        severity = rule_severity(rule)
        if severity:
            attrs["severity"] = severity
        if base_url:
            attrs["grafana_rule_url"] = f"{base_url.rstrip('/')}/alerting/grafana/{rule['uid']}/view"
        targets.append({"source_type": "grafana", "query": rule["uid"], "entity": rule_entity(rule), "attrs": attrs})
    return targets


def data_query(rule: dict[str, Any]) -> dict[str, Any]:
    """規則裡真正向資料源查資料的那一筆（第一筆不是運算式的）。沒有就是這條規則不能當偵測對象。"""
    for item in rule.get("data") or []:
        if item.get("datasourceUid") and item["datasourceUid"] != EXPRESSION_DATASOURCE:
            return item
    raise ValueError(f"Grafana 規則 {rule.get('uid')} 沒有向資料源查資料的查詢（只有運算式）")


def range_query(rule: dict[str, Any], start: datetime, end: datetime, step_seconds: int) -> dict[str, Any]:
    """/api/ds/query 的請求內容：規則自己的查詢，改成 [start, end] 每 step_seconds 一點。
    告警規則存的是「當下那一刻」的查詢（Prometheus 類：instant），偵測要的是一段時序，所以改成 range；
    其他後端的 model 沒有這兩個鍵，只給 intervalMs／maxDataPoints（Grafana 所有資料源共通的欄位）。"""
    item = data_query(rule)
    step_ms = int(step_seconds) * 1000
    points = math.ceil((end - start).total_seconds() / step_seconds) + 1
    model = {**(item.get("model") or {}), "refId": REF_ID, "datasource": {"uid": item["datasourceUid"]},
             "intervalMs": step_ms, "maxDataPoints": points}
    if "expr" in model:
        model.update(instant=False, range=True)
    return {"queries": [model], "from": str(int(start.timestamp() * 1000)), "to": str(int(end.timestamp() * 1000))}


def series_from_response(payload: dict[str, Any]) -> list[tuple[dict[str, str], list[tuple[int, float]]]]:
    """/api/ds/query 的回應 -> [(序列標籤, [(毫秒時間戳, 值), ...])]，一個 frame 一條。
    值是 null／NaN／Inf 的點丟掉（跟沒資料一樣，不能學成基準線）。格式不對丟 ValueError，訊息說是哪裡不對。"""
    result = (payload.get("results") or {}).get(REF_ID)
    if result is None:
        raise ValueError(f"Grafana 回應裡沒有 results.{REF_ID}")
    if result.get("error"):
        raise ValueError(f"Grafana 查詢失敗：{result['error']}")
    out = []
    for frame in result.get("frames") or []:
        fields = (frame.get("schema") or {}).get("fields") or []
        values = (frame.get("data") or {}).get("values") or []
        if not fields:  # 這段時間完全沒有資料時，Grafana 回一個沒有欄位的空 frame（實測：查詢區間早於 Prometheus 的保留期）
            continue
        time_at = next((i for i, f in enumerate(fields) if f.get("type") == "time"), None)
        value_at = next((i for i, f in enumerate(fields) if f.get("type") == "number"), None)
        if time_at is None or value_at is None or len(values) <= max(time_at, value_at):
            raise ValueError(f"Grafana 回的 frame 不是時序（要有一個時間欄與一個數值欄）：{[f.get('type') for f in fields]}")
        points = [(int(t), float(v)) for t, v in zip(values[time_at], values[value_at])
                  if t is not None and isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)]
        out.append((dict(fields[value_at].get("labels") or {}), points))
    return out
