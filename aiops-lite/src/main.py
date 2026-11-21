"""aiops-lite 的單一入口：CronJob 每 5 分鐘跑一次 `python -m src.main`，跑完就結束。沒有常駐行程、沒有 HTTP API。

  1. 讀規則   Grafana 上貼了 aiops_detect 標籤的告警規則（aiops_severity、service）
  2. 取時序   重放規則的查詢（/api/ds/query）；基準線沒有或到期才抓訓練窗
  3. 偵測     季節基準 + 4 個演算法 + new_only
  4. 收斂     同實體、關係邊、併入進行中事故、連續 N 輪無異常結案
  5. RCA      收集 4 包證據交給 LLM（失敗退回原始摘要）
  6. 寫回     先寫自己的資料庫，再寫 Grafana 註記（每個事故一筆，三行）

一條規則出錯（查詢寫錯、資料源逾時）只記進這一輪的 errors，其他規則照跑。Grafana 的規則清單讀不到、或資料庫連不上，
這一輪才算失敗（行程結束碼 1，CronJob 看得到）。

環境變數（★ 必填）：
  ★ GRAFANA_URL、★ GRAFANA_TOKEN（Editor 服務帳號）
  DATABASE_URL（預設 sqlite:///aiops-lite.db；正式環境 postgresql+psycopg://…）
  AIOPS_EDGES_FILE（關係邊 YAML，預設 /etc/aiops-lite/edges.yaml；檔案不存在 = 沒有關係邊）
  LLM_API_URL、LLM_API_KEY、LLM_MODEL（沒設 = 不分析，註記第 2 行是原始摘要）
  AIOPS_ES_DATASOURCE_UID（Grafana 上 ES 資料源的 uid；沒設 = 沒有日誌證據）、AIOPS_LOG_INDEX、AIOPS_TRACE_INDEX、
  AIOPS_ID_FIELDS（預設 eqp_id,lot_id,carrier_id）
  AIOPS_TZ（Asia/Taipei）、AIOPS_STEP_SECONDS（60）、AIOPS_LOOKBACK_MINUTES（60）、AIOPS_TRAIN_DAYS（14）、
  AIOPS_RETRAIN_HOURS（24）、AIOPS_CLOSE_AFTER_ROUNDS（3）
"""
from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
import yaml

from src import detectors, grouping, rca, report
from src.db import Database
from src.grafana import Grafana, GrafanaError, Rule

log = logging.getLogger("aiops_lite")

MIN_KEEP_AFTER_EXCLUSION = detectors.MIN_TRAIN_POINTS  # 扣掉事故窗之後剩不到這麼多點，就不扣（寧可用整段學）


class ConfigError(Exception):
    """環境變數或關係邊檔寫錯：啟動時就報錯，不跑。"""


@dataclass(frozen=True)
class Settings:
    grafana_url: str
    grafana_token: str
    database_url: str = "sqlite:///aiops-lite.db"
    edges_file: str = "/etc/aiops-lite/edges.yaml"
    llm_api_url: str = ""
    llm_api_key: str = ""
    llm_model: str = ""
    es_datasource_uid: str = ""
    log_index: str = "*"
    trace_index: str = ""
    id_fields: tuple[str, ...] = ("eqp_id", "lot_id", "carrier_id")
    tz: str = "Asia/Taipei"
    step_seconds: int = 60
    lookback_minutes: int = 60
    train_days: int = 14
    retrain_hours: int = 24
    close_after_rounds: int = 3

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "Settings":
        missing = [k for k in ("GRAFANA_URL", "GRAFANA_TOKEN") if not env.get(k)]
        if missing:
            raise ConfigError(f"缺少環境變數：{'、'.join(missing)}")

        def whole(name: str, default: int) -> int:
            raw = env.get(name, "")
            try:
                value = int(raw) if raw else default
            except ValueError:
                raise ConfigError(f"{name}={raw!r} 不是整數") from None
            if value < 1:
                raise ConfigError(f"{name} 必須是 1 以上的整數，不是 {value}")
            return value

        tz = env.get("AIOPS_TZ") or cls.tz
        try:
            ZoneInfo(tz)
        except (KeyError, ValueError):
            raise ConfigError(f"AIOPS_TZ={tz!r} 不是合法時區") from None
        ids = tuple(f.strip() for f in (env.get("AIOPS_ID_FIELDS") or ",".join(cls.id_fields)).split(",") if f.strip())
        return cls(
            grafana_url=env["GRAFANA_URL"], grafana_token=env["GRAFANA_TOKEN"],
            database_url=env.get("DATABASE_URL") or cls.database_url,
            edges_file=env.get("AIOPS_EDGES_FILE") or cls.edges_file,
            llm_api_url=env.get("LLM_API_URL", ""), llm_api_key=env.get("LLM_API_KEY", ""),
            llm_model=env.get("LLM_MODEL", ""), es_datasource_uid=env.get("AIOPS_ES_DATASOURCE_UID", ""),
            log_index=env.get("AIOPS_LOG_INDEX") or cls.log_index, trace_index=env.get("AIOPS_TRACE_INDEX", ""),
            id_fields=ids, tz=tz, step_seconds=whole("AIOPS_STEP_SECONDS", cls.step_seconds),
            lookback_minutes=whole("AIOPS_LOOKBACK_MINUTES", cls.lookback_minutes),
            train_days=whole("AIOPS_TRAIN_DAYS", cls.train_days),
            retrain_hours=whole("AIOPS_RETRAIN_HOURS", cls.retrain_hours),
            close_after_rounds=whole("AIOPS_CLOSE_AFTER_ROUNDS", cls.close_after_rounds))


def load_edges(path: str) -> list[tuple[str, str]]:
    """關係邊 YAML：`edges: [[上游, 下游], ...]`，也接受 "上游 -> 下游" 字串。檔案不存在 = 沒有關係邊；寫錯直接報錯。"""
    file = Path(path)
    if not file.exists():
        return []
    try:
        raw = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"關係邊檔 {path} 不是合法的 YAML：{e}") from e
    items = raw.get("edges") if isinstance(raw, dict) else None
    if not isinstance(items, list):
        raise ConfigError(f"關係邊檔 {path} 要有 edges 清單")
    edges = []
    for item in items:
        pair = [p.strip() for p in item.split("->")] if isinstance(item, str) else item
        if (not isinstance(pair, list) or len(pair) != 2 or not all(isinstance(p, str) and p.strip() for p in pair)
                or pair[0].strip() == pair[1].strip()):
            raise ConfigError(f"關係邊 {item!r} 寫錯：要是 [上游, 下游] 兩個不同的系統名稱")
        edges.append((pair[0].strip(), pair[1].strip()))
    return edges


def _baseline(rule: Rule, settings: Settings, grafana: Grafana, db: Database, now: datetime) -> tuple[dict | None, Any]:
    """拿這條規則的基準線：沒有、到期、查詢改了、格式版本不同才重訓。回傳 (state 或 None, 資料庫裡的紀錄)。"""
    saved = db.load_rule(rule.uid)
    fresh = (saved is not None and saved["query_hash"] == rule.query_hash and saved["trained_at"] is not None
             and now - saved["trained_at"] < timedelta(hours=settings.retrain_hours)
             and (saved["state"] is None or saved["state"].get("version") == detectors.STATE_VERSION
                  and saved["state"].get("tz") == settings.tz))
    if fresh:
        return saved["state"], saved
    # 訓練窗結束在這一輪的觀察窗之前：正在看的這一小時不會被自己學成常態
    train_end = now - timedelta(minutes=settings.lookback_minutes)
    train = grafana.query_series(rule, train_end - timedelta(days=settings.train_days), train_end, settings.step_seconds)
    keep = pd.Series(True, index=train.index)
    for start, end in db.incident_windows(rule.service, train_end - timedelta(days=settings.train_days)):
        keep &= ~train["ts"].between(start, end)
    if int(keep.sum()) >= MIN_KEEP_AFTER_EXCLUSION:
        train = train[keep].reset_index(drop=True)
    state = detectors.fit_all(train, settings.tz)
    if state is None and saved and saved["state"] and saved["query_hash"] == rule.query_hash:
        log.warning("規則 %s 重訓的資料只有 %d 點：沿用舊基準線", rule.uid, len(train))
        state = saved["state"]  # 重訓資料不足：沿用舊基準線照判，不留偵測盲區
    db.save_baseline(rule.uid, title=rule.title, service=rule.service, query_hash=rule.query_hash, state=state,
                     train_points=len(train), now=now)
    return state, saved if saved and saved["query_hash"] == rule.query_hash else None


def detect_rule(rule: Rule, settings: Settings, grafana: Grafana, db: Database,
                now: datetime) -> tuple[grouping.Signal | None, str]:
    """一條規則的這一輪：回傳 (新異常的摘要 或 None, 備註)。抓不到資料丟 GrafanaError。"""
    state, saved = _baseline(rule, settings, grafana, db, now)
    if state is None:
        return None, f"規則「{rule.title}」訓練資料不足 {detectors.MIN_TRAIN_POINTS} 點：還不判"
    recent = grafana.query_series(rule, now - timedelta(minutes=settings.lookback_minutes), now, settings.step_seconds)
    if recent.empty:
        raise GrafanaError(f"規則「{rule.title}」({rule.uid}) 最近 {settings.lookback_minutes} 分鐘沒有資料")
    found = detectors.new_only(detectors.detect_all(state, recent), saved["last_anomaly_ts"] if saved else None)
    if not found:
        return None, ""
    names = tuple(sorted({d for a in found for d in a.detectors}))
    return grouping.Signal(rule.uid, rule.title, rule.service, rule.severity, found[0].ts, found[-1].ts, len(found),
                           max(a.score for a in found), names), ""


def run(settings: Settings, *, now: datetime | None = None, grafana_transport: httpx.BaseTransport | None = None,
        llm_transport: httpx.BaseTransport | None = None) -> dict[str, Any]:
    """跑一輪，回傳這一輪的摘要。transport 參數只是 HTTP 連線的注入點（預設真的連出去）。"""
    edges = load_edges(settings.edges_file)
    started = datetime.now(timezone.utc)
    now = (now or started).astimezone(timezone.utc).replace(second=0, microsecond=0)
    db = Database(settings.database_url)
    grafana = Grafana(settings.grafana_url, settings.grafana_token, transport=grafana_transport)
    errors: list[str] = []
    signals: list[grouping.Signal] = []
    blind: set[str] = set()
    rules: list[Rule] = []
    incidents: list[grouping.Incident] = []
    status = "failed"
    try:
        rules, problems = grafana.list_rules()
        errors += problems
        for rule in rules:
            try:
                signal, note = detect_rule(rule, settings, grafana, db, now)
            except Exception as e:  # 一條規則的任何錯誤（含演算法 bug）都不擋其他規則；訊息帶型別
                log.exception("規則 %s 這一輪失敗", rule.uid)
                errors.append(f"{rule.service}／{rule.title}：{type(e).__name__}: {e}")
                blind.add(rule.service)
                continue
            if note:
                errors.append(note)
                blind.add(rule.service)
            if signal:
                signals.append(signal)
        incidents = grouping.converge(db.open_incidents(), signals, edges, blind_services=blind,
                                      close_after_rounds=settings.close_after_rounds)
        known = {i.uid for i in incidents}
        incidents += [i for i in db.unannotated_incidents() if i.uid not in known]
        can_analyze = bool(settings.llm_api_url and settings.llm_model)
        for inc in incidents:
            # 成員規則或嚴重度變了就重新分析；上一輪 LLM 沒成功的進行中事故，這一輪再試一次
            if inc.status == "open" and (inc.fresh or not inc.summary or (can_analyze and not inc.analyzed)):
                evidence = rca.collect_evidence(
                    inc, edges, grafana, es_datasource_uid=settings.es_datasource_uid, log_index=settings.log_index,
                    trace_index=settings.trace_index, id_fields=list(settings.id_fields))
                before = (inc.summary, inc.analyzed)
                inc.summary, inc.analyzed, reason = rca.analyze(
                    inc, evidence, api_url=settings.llm_api_url, api_key=settings.llm_api_key,
                    model=settings.llm_model, transport=llm_transport)
                if (inc.summary, inc.analyzed) != before:
                    inc.changed, inc.annotated = True, False
                if reason and can_analyze:
                    errors.append(f"事故 {inc.uid} 未經 LLM 分析：{reason}")
        # 寫入順序：先落地自己的資料庫（事故 + 各規則已報到哪），再寫 Grafana；註記寫成功才記 annotated
        marks = {s.rule_uid: s.last_ts for s in signals}
        db.save_incidents([i for i in incidents if i.changed], marks, now)
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
        db.save_incidents(written, {}, now)
        status = "degraded" if errors else "ok"
    except Exception as e:
        log.exception("這一輪失敗")
        errors.append(f"{type(e).__name__}: {e}")
        raise
    finally:
        summary = {"status": status, "rules": len(rules), "anomalies": sum(s.count for s in signals),
                   "open_incidents": sum(1 for i in incidents if i.status == "open"), "errors": errors,
                   "incidents": [i.uid for i in incidents if i.changed or i.annotated]}
        try:
            db.record_round(started_at=started, finished_at=datetime.now(timezone.utc), status=status,
                            rules_count=len(rules), anomalies=summary["anomalies"],
                            open_incidents=summary["open_incidents"], errors=errors)
        except Exception:  # 連執行紀錄都寫不進去（資料庫斷線）：留一行 log，不蓋掉原本的錯誤
            log.exception("這一輪的執行紀錄寫不進資料庫")
        grafana.close()
        db.close()
    return summary


def main() -> int:
    logging.basicConfig(level=os.environ.get("AIOPS_LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # 每個 HTTP 請求一行太吵；錯誤我們自己會記
    try:
        settings = Settings.from_env(os.environ)
        summary = run(settings)
    except ConfigError as e:
        log.error("設定錯誤：%s", e)
        return 2
    except Exception:
        return 1
    for line in summary["errors"]:
        log.warning("%s", line)
    log.info("這一輪結束：status=%s rules=%d anomalies=%d open_incidents=%d", summary["status"], summary["rules"],
             summary["anomalies"], summary["open_incidents"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
