"""深度連結：讓告警卡片與 RCA 報告帶上可直達案發現場的 Grafana 網址（指標大盤、trace、log）。

基底網址只從環境變數 AIOPS_GRAFANA_URL 讀（哪個網址維運點得到只有部署知道）；沒設就完全不產生連結，
卡片其他內容照常——跟 LLM 一樣是可選能力，不准阻斷管線。看哪個儀表板、用哪個變數、哪些資料源，
是場景的事，寫在管線 YAML（alert.notify_payload 的 links 參數、rca.report 的 section_params）。

這裡只有純函式：拼網址、算時間範圍，不做任何判斷、不連外。
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from urllib.parse import quote, urlencode

from aiops.engine.types import GrafanaLinks, as_utc

GRAFANA_URL_ENV = "AIOPS_GRAFANA_URL"


def grafana_base_url() -> str | None:
    """部署給的 Grafana 基底網址（去掉結尾斜線）；沒設回 None = 不產生任何連結。"""
    base = os.environ.get(GRAFANA_URL_ENV, "").strip().rstrip("/")
    return base or None


def links_config(params: dict | None) -> GrafanaLinks | None:
    """YAML 的 links 參數 + 環境變數 -> GrafanaLinks；缺基底網址或缺 dashboard／datasources 回 None。"""
    base = grafana_base_url()
    if base is None or not params:
        return None
    cfg = GrafanaLinks(
        base_url=base,
        dashboard=str(params.get("dashboard") or "").strip("/") or None,
        var=str(params.get("var", "service")),
        pad_minutes=float(params.get("pad_minutes", 15)),
        datasources={str(k): str(v) for k, v in (params.get("datasources") or {}).items()},
    )
    return cfg if cfg.dashboard or cfg.datasources else None


def time_range_ms(start: datetime, end: datetime, pad_minutes: float) -> tuple[int, int]:
    """事故前後各多留 pad 分鐘，讓人看得到異常前的常態；回傳 Grafana 用的 epoch 毫秒。"""
    pad = timedelta(minutes=pad_minutes)
    return int((as_utc(start) - pad).timestamp() * 1000), int((as_utc(end) + pad).timestamp() * 1000)


def dashboard_url(cfg: GrafanaLinks, entities: list[str], start: datetime, end: datetime) -> str | None:
    """/d/<uid>?var-<var>=<entity>&from=..&to=..：同一個儀表板，變數鎖在事故裡的服務、時間鎖在事故期間。"""
    if not cfg.dashboard:
        return None
    frm, to = time_range_ms(start, end, cfg.pad_minutes)
    query = [(f"var-{cfg.var}", e) for e in entities] + [("from", str(frm)), ("to", str(to))]
    return f"{cfg.base_url}/{cfg.dashboard}?{urlencode(query)}"


def explore_url(base_url: str, datasource_uid: str, lucene: str, start: datetime, end: datetime,
                pad_minutes: float = 15) -> str:
    """Explore 的 panes 網址（Grafana 10 以後的格式）：Elasticsearch 資料源、以文件（logs）形式列出符合查詢的紀錄。
    trace 與 log 都是「找出這些文件」，用同一種查詢；指標大盤用 dashboard_url。"""
    frm, to = time_range_ms(start, end, pad_minutes)
    panes = {"aiops": {
        "datasource": datasource_uid,
        "queries": [{"refId": "A", "datasource": {"type": "elasticsearch", "uid": datasource_uid},
                     "query": lucene, "metrics": [{"type": "logs", "id": "1"}], "bucketAggs": []}],
        "range": {"from": str(frm), "to": str(to)},
    }}
    return f"{base_url}/explore?schemaVersion=1&orgId=1&panes={quote(json.dumps(panes, separators=(',', ':')))}"


def lucene_any(field: str, values: list[str]) -> str:
    """service.name:("a" OR "b")。值裡的雙引號與反斜線先跳脫。"""
    quoted = " OR ".join('"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"' for v in values)
    return f"{field}:({quoted})"


def incident_links(cfg: GrafanaLinks, entities: list[str], start: datetime, end: datetime) -> dict[str, str]:
    """一張卡上的所有連結（annotations 鍵 -> 網址）：grafana_dashboard_url、grafana_<資料源名>_url。
    datasources 的鍵是場景自己取的名字（metrics／traces／logs），值是 Grafana 資料源 uid；查詢一律鎖 service.name。"""
    out: dict[str, str] = {}
    dash = dashboard_url(cfg, entities, start, end)
    if dash:
        out["grafana_dashboard_url"] = dash
    for name, uid in cfg.datasources.items():
        out[f"grafana_{name}_url"] = explore_url(cfg.base_url, uid, lucene_any("service.name", entities), start, end,
                                                 cfg.pad_minutes)
    return out
