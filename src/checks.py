"""管理介面的「連線檢查」：照現在生效的設定，實際去問一次資料庫、Grafana、日誌（ES）、Tempo、LLM。

每一項回傳 {"key", "name", "status", "detail", "hints"}：
  ok       通了，而且看起來可以用
  warn     連得到，但照現在的設定拿不到東西（例如日誌的服務欄位是空的）
  fail     連不上、沒權限、回應不對
  skipped  沒設定這一項

只讀不寫：不建註記、不改任何資料。LLM 那一項會真的送一句很短的話給模型（有費用的話會算一次），所以只在按下按鈕時才跑。
這裡不重試、不快取，結果只代表按下去那一刻。
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import httpx

from src import rca
from src.db import Database
from src.grafana import Grafana, GrafanaError
from src.main import Settings

LOG_WINDOW_HOURS = 6        # 日誌檢查看最近幾小時：夠長到離峰時段也有資料，夠短到查詢不重
LLM_CHECK_TIMEOUT = 30.0    # 連線測試不用等到 llm.timeout_seconds 那麼久


def _item(key: str, name: str, status: str, detail: str, hints: list[str] | None = None) -> dict[str, Any]:
    return {"key": key, "name": name, "status": status, "detail": detail, "hints": hints or []}


def check_database(db: Database) -> dict[str, Any]:
    try:
        db.ping()
        return _item("database", "資料庫", "ok", f"連得到（{db.engine.dialect.name}）")
    except Exception as e:
        return _item("database", "資料庫", "fail", f"{type(e).__name__}: {str(e)[:300]}")


def check_grafana(grafana: Grafana) -> dict[str, Any]:
    try:
        org = grafana.whoami()
        return _item("grafana", "Grafana", "ok", f"Token 有效，組織：{org.get('name', '?')}（id {org.get('id', '?')}）")
    except GrafanaError as e:
        return _item("grafana", "Grafana", "fail", str(e)[:400],
                     ["確認 grafana.url 從 Pod 裡連得到", "確認 grafana.token 是 Editor 服務帳號的 Token、沒有過期"])


def check_logs(grafana: Grafana, settings: Settings, now: datetime) -> dict[str, Any]:
    name = "日誌（ES）"
    if not settings.es_datasource_uid:
        return _item("logs", name, "skipped", "沒有設定 es.datasource_uid：RCA 不會有日誌證據")
    fields, levels = settings.log_fields, settings.evidence.error_levels
    try:
        got = grafana.probe_logs(settings.es_datasource_uid, settings.log_index, now - timedelta(hours=LOG_WINDOW_HOURS), now,
                                 log_fields=fields, error_levels=levels)
    except GrafanaError as e:
        return _item("logs", name, "fail", str(e)[:400],
                     ["確認 es.datasource_uid 是 Grafana 上 Elasticsearch 資料源的 uid", "確認 Token 的角色看得到這個資料源"])
    show = lambda pairs: "、".join(f"{k}（{n}）" for k, n in pairs) or "沒有值"
    detail = (f"索引 {settings.log_index} 最近 {LOG_WINDOW_HOURS} 小時 {got['total']} 筆，其中照現在的設定算錯誤日誌的 {got['errors']} 筆。"
              f"服務欄位 {fields.service}：{show(got['services'])}。")
    detail += f"等級欄位 {fields.level}：{show(got['levels'])}。" if fields.level else "沒有等級欄位：用訊息文字裡的等級字樣挑錯誤日誌。"
    hints = list(got["problems"])
    if got["total"] == 0:
        hints.append("這段時間沒有任何日誌：檢查 es.log_index 的名稱，以及收日誌的那一端有沒有在送")
    else:
        if not got["services"]:
            hints.append(f"日誌裡沒有 {fields.service} 這個欄位（或它不能統計）：把 es.log_fields.service 改成實際的欄位名稱")
        if fields.level and not got["levels"]:
            hints.append(f"日誌裡沒有 {fields.level} 這個欄位：改成實際的欄位名稱；日誌沒有拆出等級就把 es.log_fields.level 留空")
        elif fields.level and not {k for k, _n in got["levels"]} & set(levels):
            hints.append(f"等級欄位的值沒有一個在 es.error_levels（{'、'.join(levels)}）裡：把你們的寫法加進去")
        known = {s for edge in settings.edges for s in edge}
        missing = sorted(known - {k for k, _n in got["services"]}) if got["services"] and known else []
        if missing:
            hints.append(f"關係邊裡的 {'、'.join(missing)} 不在服務欄位最常見的值裡：名稱要跟告警的 {settings.labels.service} 標籤完全一樣"
                         "（也可能只是這段時間沒有它的日誌）")
    status = "ok" if got["total"] and got["services"] and not hints else "warn"
    return _item("logs", name, status, detail, hints)


def check_tempo(grafana: Grafana, settings: Settings, now: datetime) -> dict[str, Any]:
    name = "呼叫鏈（Tempo）"
    if not settings.tempo_datasource_uid:
        return _item("tempo", name, "skipped", "沒有設定 tempo.datasource_uid：RCA 不會有呼叫鏈證據（沒有埋點的系統本來就不需要）")
    services = sorted({s for edge in settings.edges for s in edge}) or ["aiops-lite-connectivity-check"]
    try:
        found = grafana.search_error_traces(settings.tempo_datasource_uid, services, now - timedelta(hours=1), now, 5)
    except GrafanaError as e:
        return _item("tempo", name, "fail", str(e)[:400], ["確認 tempo.datasource_uid 是 Grafana 上 Tempo 資料源的 uid"])
    return _item("tempo", name, "ok", f"查得動；最近 1 小時關係邊裡的系統有 {len(found)} 條帶錯誤的 Trace（0 條不代表有問題）")


def check_llm(settings: Settings, transport: httpx.BaseTransport | None = None,
              clock: Callable[[], float] = time.monotonic) -> dict[str, Any]:
    if not (settings.llm_api_url and settings.llm_model):
        return _item("llm", "LLM", "skipped", "沒有設定 llm.url 與 llm.model：註記第 2 行會是 [未經分析] 的原始摘要")
    started = clock()
    try:
        answer = rca.call_llm(settings.llm_api_url, settings.llm_api_key, settings.llm_model, "這是連線測試，請只回覆 ok。",
                              transport=transport, timeout=min(settings.llm.timeout_seconds, LLM_CHECK_TIMEOUT),
                              temperature=settings.llm.temperature, system_prompt="你是連線測試的對象，照使用者說的回覆。")
    except Exception as e:   # 連不上、逾時、4xx／5xx、回應格式不是 OpenAI 相容的
        reason = f"{type(e).__name__}: {e}".replace(settings.llm_api_key or "\0", "<api_key>")[:400]
        return _item("llm", "LLM", "fail", reason, [
            "401／403：金鑰不對；404：網址或模型名稱不對；400：模型不接受某個參數（例如 temperature）",
            "502／503：閘道後面的模型服務沒回應，通常是對方的問題，aiops-lite 會自動退避重試"])
    return _item("llm", "LLM", "ok", f"模型 {settings.llm_model} 有回應（{clock() - started:.1f} 秒）：{answer.strip()[:80]}")


def run_all(settings: Settings, db: Database, *, now: datetime | None = None,
            grafana_transport: httpx.BaseTransport | None = None, llm_transport: httpx.BaseTransport | None = None,
            include_llm: bool = True) -> list[dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    grafana = Grafana(settings.grafana_url, settings.grafana_token, transport=grafana_transport,
                      timeout=settings.grafana_timeout_seconds, retries=0)
    try:
        items = [check_database(db), check_grafana(grafana), check_logs(grafana, settings, now),
                 check_tempo(grafana, settings, now)]
    finally:
        grafana.close()
    if include_llm:
        items.append(check_llm(settings, llm_transport))
    return items
