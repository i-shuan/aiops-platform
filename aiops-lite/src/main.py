"""設定，以及「一輪」的流程。常駐服務（src/server.py）在 debounce 時間到的時候呼叫 process()；
也可以手動 `python -m src.main` 立刻處理一輪（不等 debounce，除錯用）。

  1. 取出這個 debounce 視窗內收到的告警（Webhook 收到時已經先存進資料庫）
  2. 收斂     同實體、關係邊、併入進行中事故；成員全部 resolved 並持續一段時間才結案
  3. RCA      收集 4 包證據交給 LLM（失敗退回原始摘要）
  4. 寫回     先寫自己的資料庫，再寫 Grafana 註記（每個事故一筆，三行）

Grafana 連不上時註記寫不進去：事故照樣存在資料庫，下一輪補寫。資料庫連不上這一輪才算失敗，告警留在 inbox 下一輪再處理。

時間設定與關係邊寫在 YAML（AIOPS_CONFIG_FILE，預設 /etc/aiops-lite/config.yaml；Helm 由 values 產生）：

    debounce_seconds: 60        # 收到第一個 Webhook 後等多久才開始分析（等上下游告警到齊、等 ES 日誌落盤）
    followup_seconds: 60        # 有進行中事故時，沒有新 Webhook 也每隔多久再跑一輪（結案、補寫註記、重試 LLM）
    close_after_seconds: 300    # 成員告警全部 resolved 之後，再安靜多久才結案
    edges:
      - [mq-broker, cim-eap]    # [上游, 下游]

環境變數（★ 必填）：
  ★ GRAFANA_URL、★ GRAFANA_TOKEN（Editor 服務帳號）、★ AIOPS_WEBHOOK_TOKEN（Grafana Contact Point 帶的 Bearer）
  DATABASE_URL（預設 sqlite:///aiops-lite.db；正式環境 postgresql+psycopg://…）、AIOPS_PORT（8080）
  LLM_API_URL、LLM_API_KEY、LLM_MODEL（沒設 = 不分析，註記第 2 行是原始摘要）
  AIOPS_ES_DATASOURCE_UID（Grafana 上 ES 資料源的 uid；沒設 = 沒有日誌證據）、AIOPS_LOG_INDEX、AIOPS_TRACE_INDEX、
  AIOPS_ID_FIELDS（預設 eqp_id,lot_id,carrier_id）、AIOPS_TZ（Asia/Taipei）
"""
from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import httpx
import yaml

from src import grouping, rca, report
from src.db import Database
from src.grafana import Grafana, GrafanaError

log = logging.getLogger("aiops_lite")

TIMING_DEFAULTS = {"debounce_seconds": 60, "followup_seconds": 60, "close_after_seconds": 300}
TIMING_MINIMUM = {"debounce_seconds": 0, "followup_seconds": 1, "close_after_seconds": 0}


class ConfigError(Exception):
    """環境變數或設定檔寫錯：啟動時就報錯，不跑。"""


@dataclass(frozen=True)
class Settings:
    grafana_url: str
    grafana_token: str
    webhook_token: str
    database_url: str = "sqlite:///aiops-lite.db"
    port: int = 8080
    llm_api_url: str = ""
    llm_api_key: str = ""
    llm_model: str = ""
    es_datasource_uid: str = ""
    log_index: str = "*"
    trace_index: str = ""
    id_fields: tuple[str, ...] = ("eqp_id", "lot_id", "carrier_id")
    tz: str = "Asia/Taipei"
    debounce_seconds: int = TIMING_DEFAULTS["debounce_seconds"]
    followup_seconds: int = TIMING_DEFAULTS["followup_seconds"]
    close_after_seconds: int = TIMING_DEFAULTS["close_after_seconds"]
    edges: tuple[tuple[str, str], ...] = ()

    @classmethod
    def load(cls, env: Mapping[str, str]) -> "Settings":
        missing = [k for k in ("GRAFANA_URL", "GRAFANA_TOKEN", "AIOPS_WEBHOOK_TOKEN") if not env.get(k)]
        if missing:
            raise ConfigError(f"缺少環境變數：{'、'.join(missing)}")
        tz = env.get("AIOPS_TZ") or cls.tz
        try:
            ZoneInfo(tz)
        except (KeyError, ValueError):
            raise ConfigError(f"AIOPS_TZ={tz!r} 不是合法時區") from None
        try:
            port = int(env.get("AIOPS_PORT") or cls.port)
        except ValueError:
            raise ConfigError(f"AIOPS_PORT={env.get('AIOPS_PORT')!r} 不是整數") from None
        ids = tuple(f.strip() for f in (env.get("AIOPS_ID_FIELDS") or ",".join(cls.id_fields)).split(",") if f.strip())
        return cls(
            grafana_url=env["GRAFANA_URL"], grafana_token=env["GRAFANA_TOKEN"],
            webhook_token=env["AIOPS_WEBHOOK_TOKEN"], database_url=env.get("DATABASE_URL") or cls.database_url,
            port=port, llm_api_url=env.get("LLM_API_URL", ""), llm_api_key=env.get("LLM_API_KEY", ""),
            llm_model=env.get("LLM_MODEL", ""), es_datasource_uid=env.get("AIOPS_ES_DATASOURCE_UID", ""),
            log_index=env.get("AIOPS_LOG_INDEX") or cls.log_index, trace_index=env.get("AIOPS_TRACE_INDEX", ""),
            id_fields=ids, tz=tz, **load_config(env.get("AIOPS_CONFIG_FILE") or "/etc/aiops-lite/config.yaml"))


def load_config(path: str) -> dict[str, Any]:
    """讀 YAML 設定檔，回傳 {debounce_seconds, followup_seconds, close_after_seconds, edges}。
    檔案不存在 = 全部用預設值、沒有關係邊；寫錯（打錯鍵名、不是整數、邊不是一對名稱）直接報錯。"""
    file = Path(path)
    raw: Any = {}
    if file.exists():
        try:
            raw = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as e:
            raise ConfigError(f"設定檔 {path} 不是合法的 YAML：{e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"設定檔 {path} 最外層要是物件")
    unknown = sorted(set(raw) - set(TIMING_DEFAULTS) - {"edges"})
    if unknown:
        raise ConfigError(f"設定檔 {path} 有不認得的設定 {unknown}；可用：{sorted([*TIMING_DEFAULTS, 'edges'])}")
    out: dict[str, Any] = {}
    for key, default in TIMING_DEFAULTS.items():
        value = raw.get(key, default)
        if isinstance(value, bool) or not isinstance(value, int) or value < TIMING_MINIMUM[key]:
            raise ConfigError(f"設定檔的 {key}={value!r} 要是 {TIMING_MINIMUM[key]} 以上的整數（秒）")
        out[key] = value
    items = raw.get("edges", [])
    if not isinstance(items, list):
        raise ConfigError("設定檔的 edges 要是清單")
    edges = []
    for item in items:
        pair = [p.strip() for p in item.split("->")] if isinstance(item, str) else item
        if (not isinstance(pair, list) or len(pair) != 2 or not all(isinstance(p, str) and p.strip() for p in pair)
                or pair[0].strip() == pair[1].strip()):
            raise ConfigError(f"關係邊 {item!r} 寫錯：要是 [上游, 下游] 兩個不同的系統名稱")
        edges.append((pair[0].strip(), pair[1].strip()))
    out["edges"] = tuple(edges)
    return out


def split_batch(alerts: list[grouping.Alert]) -> tuple[list[grouping.Alert], list[grouping.Alert]]:
    """把一個視窗內依序收到的告警整理成 (響過的, 最新狀態是 resolved 的)；同一個 fingerprint 各留最後一筆。"""
    firing: dict[str, grouping.Alert] = {}
    latest: dict[str, grouping.Alert] = {}
    for alert in alerts:
        latest[alert.fingerprint] = alert
        if alert.status == "firing":
            firing[alert.fingerprint] = alert
    return list(firing.values()), [a for a in latest.values() if a.status == "resolved"]


def process(settings: Settings, *, now: datetime | None = None, grafana_transport: httpx.BaseTransport | None = None,
            llm_transport: httpx.BaseTransport | None = None) -> dict[str, Any]:
    """跑一輪，回傳這一輪的摘要（needs_followup = 還有事要做，沒有新 Webhook 也要再跑）。
    transport 參數只是 HTTP 連線的注入點（預設真的連出去）。"""
    started = datetime.now(timezone.utc)
    now = (now or started).astimezone(timezone.utc)
    edges = list(settings.edges)
    db = Database(settings.database_url)
    grafana = Grafana(settings.grafana_url, settings.grafana_token, transport=grafana_transport)
    errors: list[str] = []
    incidents: list[grouping.Incident] = []
    batch: list[grouping.Alert] = []
    status = "failed"
    try:
        ids, batch = db.pending()
        firing, resolved = split_batch(batch)
        incidents = grouping.converge(db.open_incidents(), firing, resolved, edges, now=now,
                                      close_after_seconds=settings.close_after_seconds)
        known = {i.uid for i in incidents}
        incidents += [i for i in db.unannotated_incidents() if i.uid not in known]
        can_analyze = bool(settings.llm_api_url and settings.llm_model)
        for inc in incidents:
            # 成員或嚴重度變了就重新分析；上一輪 LLM 沒成功的進行中事故，這一輪再試一次
            if inc.status == "open" and (inc.fresh or not inc.summary or (can_analyze and not inc.analyzed)):
                evidence = rca.collect_evidence(
                    inc, edges, grafana, es_datasource_uid=settings.es_datasource_uid, log_index=settings.log_index,
                    trace_index=settings.trace_index, id_fields=list(settings.id_fields), now=now)
                before = (inc.summary, inc.analyzed)
                inc.summary, inc.analyzed, reason = rca.analyze(
                    inc, evidence, api_url=settings.llm_api_url, api_key=settings.llm_api_key,
                    model=settings.llm_model, transport=llm_transport)
                if (inc.summary, inc.analyzed) != before:
                    inc.changed, inc.annotated = True, False
                if reason and can_analyze:
                    errors.append(f"事故 {inc.uid} 未經 LLM 分析：{reason}")
            elif not inc.summary:
                inc.summary = f"[未經分析] {rca.raw_summary(inc)}"
        # 寫入順序：先落地自己的資料庫（事故 + 這批告警處理完了），再寫 Grafana；註記寫成功才記 annotated
        db.save_incidents([i for i in incidents if i.changed], now, processed=ids)
        written = []
        for inc in incidents:
            if inc.annotated:
                continue
            try:
                inc.annotation_id = grafana.upsert_annotation(
                    inc.annotation_id, report.uid_tag(inc.uid), report.render(inc, settings.grafana_url, settings.tz))
                inc.annotated = True
                written.append(inc)
            except GrafanaError as e:
                log.warning("事故 %s 的註記沒寫成功，下一輪補：%s", inc.uid, e)
                errors.append(f"事故 {inc.uid} 的註記寫入失敗：{e}")
        db.save_incidents(written, now)
        status = "degraded" if errors else "ok"
    except Exception as e:
        log.exception("這一輪失敗")
        errors.append(f"{type(e).__name__}: {e}")
        raise
    finally:
        open_count = sum(1 for i in incidents if i.status == "open")
        summary = {"status": status, "alerts": len(batch), "open_incidents": open_count, "errors": errors,
                   "incidents": [i.uid for i in incidents if i.changed or i.annotated],
                   "needs_followup": open_count > 0 or any(not i.annotated for i in incidents)}
        try:
            db.record_round(started_at=started, finished_at=datetime.now(timezone.utc), status=status,
                            alerts=len(batch), open_incidents=open_count, errors=errors)
        except Exception:  # 連執行紀錄都寫不進去（資料庫斷線）：留一行 log，不蓋掉原本的錯誤
            log.exception("這一輪的執行紀錄寫不進資料庫")
        grafana.close()
        db.close()
    return summary


def setup_logging() -> None:
    logging.basicConfig(level=os.environ.get("AIOPS_LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # 每個 HTTP 請求一行太吵；錯誤我們自己會記


def log_summary(summary: dict[str, Any]) -> None:
    for line in summary["errors"]:
        log.warning("%s", line)
    log.info("這一輪結束：status=%s alerts=%d open_incidents=%d", summary["status"], summary["alerts"],
             summary["open_incidents"])


def main() -> int:
    setup_logging()
    try:
        log_summary(process(Settings.load(os.environ)))
    except ConfigError as e:
        log.error("設定錯誤：%s", e)
        return 2
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
