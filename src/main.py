"""設定，以及「一輪」的流程。常駐服務（src/server.py）在 debounce 時間到的時候呼叫 process()；
也可以手動 `python -m src.main` 立刻處理一輪（不等 debounce，除錯用）。

  1. 取出這個 debounce 視窗內收到的告警（Webhook 收到時已經先存進資料庫）
  2. 收斂     同實體、關係邊、併入進行中事故；成員全部 resolved 並持續一段時間才結案
  3. RCA      收集 4 包證據交給 LLM（失敗退回原始摘要）
  4. 寫回     先寫自己的資料庫，再寫 Grafana 註記（每個事故一筆，三行）

Grafana 連不上時註記寫不進去：事故照樣存在資料庫，下一輪補寫。資料庫連不上這一輪才算失敗，告警留在 inbox 下一輪再處理。

**設定的來源有兩層**：config.yaml 是基底（沒有其他環境變數）；管理介面改過的可調項目存在資料庫（settings_overrides），
每一輪開始時疊在 config.yaml 上面（apply_overrides）。Token、密碼、連線位址、監聽的 port 只認 config.yaml，
管理介面看不到也改不了（EDITABLE 以外的鍵一律拒絕）。資料庫裡沒有覆寫 = 行為跟只有 config.yaml 時完全一樣。
檔案路徑預設 /etc/aiops/config.yaml，要換用環境變數
AIOPS_CONFIG_FILE 指定（唯一的一個）。檔案不存在、鍵名打錯、型別不對、必填沒填，啟動時直接報錯。完整範例見 config.example.yaml。
檔案裡有 Token 與密碼：不要推上 Git（.gitignore 已排除 config.yaml）；Kubernetes 用 Secret 掛載。
"""
from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import yaml

from src import grouping, rca, report
from src.db import Database
from src.grafana import Grafana, GrafanaError, LabelNames, LogFields, TraceFields

log = logging.getLogger("aiops_lite")

DEFAULT_CONFIG_PATH = "/etc/aiops/config.yaml"
CONFIG_ENV = "AIOPS_CONFIG_FILE"
TIMING_DEFAULTS = {"debounce_seconds": 60, "followup_seconds": 60, "close_after_seconds": 300}
TIMING_MINIMUM = {"debounce_seconds": 0, "followup_seconds": 1, "close_after_seconds": 0}
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")
ADMIN_TOKEN_MIN_CHARS = 16   # 管理密鑰太短等於沒設：能改設定的密鑰至少這麼長

# 管理介面可以改的鍵（結構跟 config.yaml 一樣；"" = 最外層）。不在這裡的一律只認 config.yaml：
# Token／金鑰／連線字串（機密）、grafana.url 與 llm.url（改了等於把資料送到別的地方）、port／host（要重啟）、
# log_level 與 healthcheck（行程層級，啟動時決定）、llm.system_prompt_file（是檔案路徑）。
EDITABLE: dict[str, frozenset[str]] = {
    "": frozenset({"debounce_seconds", "followup_seconds", "close_after_seconds", "timezone", "edges"}),
    "labels": frozenset({"detect", "severity", "service", "default_severity"}),
    "es": frozenset({"datasource_uid", "log_index", "trace_index", "id_fields", "log_fields", "trace_fields", "error_levels",
                     "error_statuses", "max_hits", "window_padding_minutes", "timeline_fields"}),
    "tempo": frozenset({"datasource_uid", "max_traces", "max_spans", "trace_id_field"}),
    "llm": frozenset({"model", "temperature", "timeout_seconds", "retry_initial_seconds", "retry_max_seconds",
                      "max_log_samples", "max_trace_samples", "max_message_chars", "top_ids", "max_summary_chars",
                      "id_pattern", "max_timeline_ids", "max_timeline_events"}),
    "annotation": frozenset({"max_rule_links"}),
}


class ConfigError(Exception):
    """設定檔寫錯：啟動時就報錯，不跑。"""


@dataclass(frozen=True)
class Settings:
    grafana_url: str
    grafana_token: str = field(repr=False)     # repr=False：設定被印進 log 或例外訊息時不會洩漏
    webhook_token: str = field(repr=False)
    database_url: str = field(default="sqlite:///aiops-lite.db", repr=False)  # 連線字串裡有密碼
    port: int = 8080
    webhook_host: str = "0.0.0.0"
    webhook_max_body_bytes: int = 1_000_000
    grafana_timeout_seconds: float = 30.0
    grafana_retries: int = 2
    grafana_retry_backoff_seconds: float = 0.5
    llm_api_url: str = ""
    llm_api_key: str = field(default="", repr=False)
    llm_model: str = ""
    llm: rca.LlmOptions = rca.LlmOptions()
    llm_retry_initial_seconds: int = 60
    llm_retry_max_seconds: int = 1800
    es_datasource_uid: str = ""
    tempo_datasource_uid: str = ""             # Grafana 上 Tempo 資料源的 uid；空的 = 沒有呼叫鏈證據
    log_index: str = "*"
    trace_index: str = ""
    id_fields: tuple[str, ...] = ("eqp_id", "lot_id", "carrier_id")
    log_fields: LogFields = LogFields()
    trace_fields: TraceFields = TraceFields()
    evidence: rca.EvidenceLimits = rca.EvidenceLimits()
    labels: LabelNames = LabelNames()
    tz: str = "Asia/Taipei"
    log_level: str = "INFO"
    debounce_seconds: int = TIMING_DEFAULTS["debounce_seconds"]
    followup_seconds: int = TIMING_DEFAULTS["followup_seconds"]
    close_after_seconds: int = TIMING_DEFAULTS["close_after_seconds"]
    retry_failed_round_seconds: int = 30       # 一輪失敗（資料庫斷線）之後多久再試
    healthcheck_stuck_seconds: int = 600       # 同一輪跑超過這麼久，/healthz 回 503
    rounds_retention_days: int = 90            # rounds 表保留幾天
    annotation_max_rule_links: int = 5         # 註記第 3 行最多列幾條規則連結
    edges: tuple[tuple[str, str], ...] = ()
    admin_token: str = field(default="", repr=False)   # 管理介面的密鑰；空的 = 管理介面與 /api 整個關掉
    # 讀進來的 config.yaml 原樣留著：管理介面的覆寫要疊在它上面重新驗證。裡面有 Token，所以不印、不參與比較
    raw: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)
    base_dir: Path | None = field(default=None, repr=False, compare=False)

    @classmethod
    def load(cls, path: str | None = None) -> "Settings":
        """讀 config.yaml。path 沒給就看環境變數 AIOPS_CONFIG_FILE，再沒有就用 /etc/aiops/config.yaml。"""
        path = path or os.environ.get(CONFIG_ENV) or DEFAULT_CONFIG_PATH
        return cls.from_dict(read_config(path), base_dir=Path(path).parent)

    @classmethod
    def from_dict(cls, raw: dict[str, Any], base_dir: Path | None = None) -> "Settings":
        """base_dir：llm.system_prompt_file 寫相對路徑時相對於哪裡（= 設定檔所在的資料夾）。"""
        known = {"grafana", "webhook", "database", "llm", "es", "tempo", "labels", "healthcheck", "annotation", "admin", "timezone",
                 "log_level", "edges", "retry_failed_round_seconds", "rounds_retention_days", *TIMING_DEFAULTS}
        check_keys(raw, known, "")
        grafana = section(raw, "grafana", {"url", "token", "timeout_seconds", "retries", "retry_backoff_seconds"})
        webhook = section(raw, "webhook", {"token", "port", "host", "max_body_bytes"})
        database = section(raw, "database", {"url"})
        llm = section(raw, "llm", {"url", "api_key", "model", "temperature", "timeout_seconds", "retry_initial_seconds",
                                   "retry_max_seconds", "max_log_samples", "max_trace_samples", "max_message_chars",
                                   "top_ids", "max_summary_chars", "system_prompt_file", "id_pattern",
                                   "max_timeline_ids", "max_timeline_events"})
        es = section(raw, "es", {"datasource_uid", "log_index", "trace_index", "id_fields", "log_fields", "trace_fields",
                                 "error_levels", "error_statuses", "max_hits", "window_padding_minutes",
                                 "timeline_fields"})
        tempo = section(raw, "tempo", {"datasource_uid", "max_traces", "max_spans", "trace_id_field"})
        labels = section(raw, "labels", {"detect", "severity", "service", "default_severity"})
        health = section(raw, "healthcheck", {"stuck_seconds"})
        annotation = section(raw, "annotation", {"max_rule_links"})
        admin_token = text(section(raw, "admin", {"token"}), "token", "", "admin.")
        if admin_token and len(admin_token) < ADMIN_TOKEN_MIN_CHARS:
            raise ConfigError(f"admin.token 至少要 {ADMIN_TOKEN_MIN_CHARS} 個字元（它能改設定；不開管理介面就整段不要寫）")
        if admin_token and admin_token == text(webhook, "token", "", "webhook."):
            raise ConfigError("admin.token 不能跟 webhook.token 一樣：Webhook 密鑰是填在 Grafana 上的，看得到的人不該因此能改設定")
        missing = [name for name, value in (("grafana.url", grafana.get("url")), ("grafana.token", grafana.get("token")),
                                            ("webhook.token", webhook.get("token"))) if not value]
        if missing:
            raise ConfigError(f"缺少必填設定：{'、'.join(missing)}")
        tz = text(raw, "timezone", "Asia/Taipei")
        try:
            ZoneInfo(tz)
        except (KeyError, ValueError):
            raise ConfigError(f"timezone={tz!r} 不是合法時區") from None
        level = text(raw, "log_level", "INFO").upper()
        if level not in LOG_LEVELS:
            raise ConfigError(f"log_level={level!r} 要是 {'、'.join(LOG_LEVELS)} 其中之一")
        d_ev, d_llm, d_lab = rca.EvidenceLimits(), rca.LlmOptions(), LabelNames()
        default_severity = text(labels, "default_severity", d_lab.default_severity, "labels.").upper()
        if default_severity not in grouping.SEVERITY_RANK:
            raise ConfigError(f"labels.default_severity={default_severity!r} 要是 P0～P3")
        names = LabelNames(**{k: text(labels, k, getattr(d_lab, k), "labels.") for k in ("detect", "severity", "service")},
                           default_severity=default_severity)
        if not all((names.detect, names.severity, names.service)) or len({names.detect, names.severity, names.service}) < 3:
            raise ConfigError("labels.detect、labels.severity、labels.service 要是三個不同、非空的標籤名稱")
        temperature = llm.get("temperature")
        if temperature is not None:
            temperature = number(llm, "temperature", 0.0, 0, 2, "llm.")
        id_pattern = text(llm, "id_pattern", d_llm.id_pattern, "llm.")
        try:
            re.compile(id_pattern)
        except re.error as e:
            raise ConfigError(f"llm.id_pattern 不是合法的正規表示式：{e}") from None
        retry_initial = integer(llm, "retry_initial_seconds", cls.llm_retry_initial_seconds, 1, prefix="llm.")
        retry_max = integer(llm, "retry_max_seconds", cls.llm_retry_max_seconds, 1, prefix="llm.")
        if retry_max < retry_initial:
            raise ConfigError("llm.retry_max_seconds 不能比 llm.retry_initial_seconds 小")
        ids = es.get("id_fields", list(cls.id_fields))
        if not isinstance(ids, list) or not ids or not all(isinstance(i, str) and i.strip() for i in ids):
            raise ConfigError("es.id_fields 要是非空的欄位名稱清單")
        ids = [i.strip() for i in ids]
        timeline = es.get("timeline_fields")
        if timeline is None:
            timeline = [f for f in d_ev.timeline_fields if f in ids]   # 預設：lot_id、carrier_id 裡有在統計的那幾個
        if not isinstance(timeline, list) or not all(isinstance(i, str) and i.strip() for i in timeline):
            raise ConfigError("es.timeline_fields 要是欄位名稱清單（空清單 = 不做關聯時間線）")
        timeline = [i.strip() for i in timeline]
        strangers = [f for f in timeline if f not in ids]
        if strangers:
            raise ConfigError(f"es.timeline_fields 的 {strangers} 不在 es.id_fields 裡：時間線只能用有在統計的識別碼")
        return cls(
            grafana_url=text(grafana, "url", "", "grafana."), grafana_token=text(grafana, "token", "", "grafana."),
            grafana_timeout_seconds=number(grafana, "timeout_seconds", cls.grafana_timeout_seconds, 1, 600, "grafana."),
            grafana_retries=integer(grafana, "retries", cls.grafana_retries, 0, 10, "grafana."),
            grafana_retry_backoff_seconds=number(grafana, "retry_backoff_seconds", cls.grafana_retry_backoff_seconds,
                                                 0, 60, "grafana."),
            webhook_token=text(webhook, "token", "", "webhook."),
            port=integer(webhook, "port", cls.port, 1, 65535, "webhook."),
            webhook_host=text(webhook, "host", cls.webhook_host, "webhook.") or cls.webhook_host,
            webhook_max_body_bytes=integer(webhook, "max_body_bytes", cls.webhook_max_body_bytes, 1024, prefix="webhook."),
            database_url=text(database, "url", cls.database_url, "database.") or cls.database_url,
            llm_api_url=text(llm, "url", "", "llm."), llm_api_key=text(llm, "api_key", "", "llm."),
            llm_model=text(llm, "model", "", "llm."),
            llm=rca.LlmOptions(
                timeout_seconds=number(llm, "timeout_seconds", d_llm.timeout_seconds, 1, 3600, "llm."),
                temperature=temperature,
                max_summary_chars=integer(llm, "max_summary_chars", d_llm.max_summary_chars, 20, prefix="llm."),
                system_prompt=read_prompt(text(llm, "system_prompt_file", "", "llm."), base_dir),
                id_pattern=id_pattern),
            llm_retry_initial_seconds=retry_initial, llm_retry_max_seconds=retry_max,
            es_datasource_uid=text(es, "datasource_uid", "", "es."), log_index=text(es, "log_index", "*", "es.") or "*",
            trace_index=text(es, "trace_index", "", "es."), id_fields=tuple(ids),
            tempo_datasource_uid=text(tempo, "datasource_uid", "", "tempo."),
            log_fields=field_map(es, "log_fields", LogFields), trace_fields=field_map(es, "trace_fields", TraceFields),
            evidence=rca.EvidenceLimits(
                max_hits=integer(es, "max_hits", d_ev.max_hits, 1, 10000, "es."),
                window_padding_minutes=integer(es, "window_padding_minutes", d_ev.window_padding_minutes, 0, 1440, "es."),
                error_levels=words(es, "error_levels", d_ev.error_levels), error_statuses=words(es, "error_statuses", d_ev.error_statuses),
                max_log_samples=integer(llm, "max_log_samples", d_ev.max_log_samples, 0, prefix="llm."),
                max_trace_samples=integer(llm, "max_trace_samples", d_ev.max_trace_samples, 0, prefix="llm."),
                max_message_chars=integer(llm, "max_message_chars", d_ev.max_message_chars, 20, prefix="llm."),
                top_ids=integer(llm, "top_ids", d_ev.top_ids, 1, prefix="llm."),
                timeline_fields=tuple(timeline),
                max_timeline_ids=integer(llm, "max_timeline_ids", d_ev.max_timeline_ids, 0, 20, "llm."),
                max_timeline_events=integer(llm, "max_timeline_events", d_ev.max_timeline_events, 1, 500, "llm."),
                trace_id_field=text(tempo, "trace_id_field", d_ev.trace_id_field, "tempo."),
                max_traces=integer(tempo, "max_traces", d_ev.max_traces, 0, 20, "tempo."),
                max_trace_spans=integer(tempo, "max_spans", d_ev.max_trace_spans, 1, 500, "tempo.")),
            labels=names, tz=tz, log_level=level, edges=parse_edges(raw.get("edges", [])),
            retry_failed_round_seconds=integer(raw, "retry_failed_round_seconds", cls.retry_failed_round_seconds, 1),
            healthcheck_stuck_seconds=integer(health, "stuck_seconds", cls.healthcheck_stuck_seconds, 30, prefix="healthcheck."),
            rounds_retention_days=integer(raw, "rounds_retention_days", cls.rounds_retention_days, 1),
            annotation_max_rule_links=integer(annotation, "max_rule_links", cls.annotation_max_rule_links, 0, prefix="annotation."),
            admin_token=admin_token, raw=raw, base_dir=base_dir,
            **{key: integer(raw, key, default, TIMING_MINIMUM[key]) for key, default in TIMING_DEFAULTS.items()})


def editable_view(s: Settings) -> dict[str, Any]:
    """把一份設定裡「管理介面可以改」的部分攤成 config.yaml 的結構（EDITABLE 的每個鍵都有值，沒設的是預設值）。
    沒有任何機密：這個結果會原樣回給瀏覽器。"""
    ev, llm = s.evidence, s.llm
    return {
        "debounce_seconds": s.debounce_seconds, "followup_seconds": s.followup_seconds,
        "close_after_seconds": s.close_after_seconds, "timezone": s.tz, "edges": [list(e) for e in s.edges],
        "labels": asdict(s.labels),
        "es": {"datasource_uid": s.es_datasource_uid, "log_index": s.log_index, "trace_index": s.trace_index,
               "id_fields": list(s.id_fields), "log_fields": asdict(s.log_fields), "trace_fields": asdict(s.trace_fields),
               "error_levels": list(ev.error_levels), "error_statuses": list(ev.error_statuses), "max_hits": ev.max_hits,
               "window_padding_minutes": ev.window_padding_minutes, "timeline_fields": list(ev.timeline_fields)},
        "tempo": {"datasource_uid": s.tempo_datasource_uid, "max_traces": ev.max_traces, "max_spans": ev.max_trace_spans,
                  "trace_id_field": ev.trace_id_field},
        "llm": {"model": s.llm_model, "temperature": llm.temperature, "timeout_seconds": llm.timeout_seconds,
                "retry_initial_seconds": s.llm_retry_initial_seconds, "retry_max_seconds": s.llm_retry_max_seconds,
                "max_log_samples": ev.max_log_samples, "max_trace_samples": ev.max_trace_samples,
                "max_message_chars": ev.max_message_chars, "top_ids": ev.top_ids,
                "max_summary_chars": llm.max_summary_chars, "id_pattern": llm.id_pattern,
                "max_timeline_ids": ev.max_timeline_ids, "max_timeline_events": ev.max_timeline_events},
        "annotation": {"max_rule_links": s.annotation_max_rule_links},
    }


def check_editable(doc: Any, what: str = "設定") -> None:
    """doc 只能有 EDITABLE 裡的鍵；多出來的（機密、不認得的）直接拒絕。"""
    if not isinstance(doc, dict):
        raise ConfigError(f"{what}要是物件")
    for key, value in doc.items():
        if key in EDITABLE[""]:
            continue
        if key not in EDITABLE:
            raise ConfigError(f"{key!r} 不能在管理介面修改（只認 config.yaml），或不是認得的設定")
        if not isinstance(value, dict):
            raise ConfigError(f"{key} 要是一組設定（物件）")
        extra = sorted(set(value) - EDITABLE[key])
        if extra:
            raise ConfigError(f"{key} 的 {extra} 不能在管理介面修改（只認 config.yaml），或不是認得的設定")


def diff_overrides(wanted: dict[str, Any], base: dict[str, Any]) -> dict[str, Any]:
    """wanted、base 都是 editable_view 的結構：只留跟 base（config.yaml）不一樣的鍵。跟 config.yaml 一樣的不存，
    之後 config.yaml 改了那個值才會跟著變。"""
    out: dict[str, Any] = {}
    for key, value in wanted.items():
        if key in EDITABLE[""]:
            if value != base.get(key):
                out[key] = value
            continue
        changed = {k: v for k, v in value.items() if v != (base.get(key) or {}).get(k)}
        if changed:
            out[key] = changed
    return out


def apply_overrides(base: Settings, overrides: dict[str, Any]) -> Settings:
    """把管理介面的覆寫疊在 config.yaml 上，整份重新驗證（跟啟動時同一套檢查）。沒有覆寫就原樣回傳 base。
    覆寫裡的 null = 那一項回到預設值。"""
    if not overrides:
        return base
    check_editable(overrides, "管理介面的設定")
    raw = dict(base.raw)
    for key, value in overrides.items():
        if key in EDITABLE[""]:
            raw[key] = value
            continue
        merged = dict(raw.get(key) or {})
        for k, v in value.items():   # log_fields／trace_fields 是對照表：逐欄合併，沒改到的欄位留著
            merged[k] = {**(merged.get(k) or {}), **v} if isinstance(v, dict) and isinstance(merged.get(k), dict) else v
        raw[key] = merged
    return Settings.from_dict(raw, base.base_dir)


def effective_settings(base: Settings, db: Database) -> Settings:
    """現在生效的設定 = config.yaml 疊上資料庫裡管理介面的覆寫。覆寫套不上去（存的時候合法，之後 config.yaml 改了才會發生）
    就只用 config.yaml 並記一行錯誤：寧可照基底跑，也不要整個服務停掉。資料庫讀不到的例外照丟，由呼叫端決定。"""
    try:
        return apply_overrides(base, db.get_overrides())
    except ConfigError as e:
        log.error("資料庫裡管理介面的設定套不上去，只用 config.yaml：%s", e)
        return base


def integer(raw: dict[str, Any], key: str, default: int, minimum: int, maximum: int | None = None, prefix: str = "") -> int:
    value = raw.get(key)
    if value is None:
        return default
    if isinstance(value, float) and value.is_integer():
        value = int(value)  # Helm 產生的 YAML 會把整數寫成 60.0 這種形式
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or (maximum is not None and value > maximum):
        limit = f"{minimum}～{maximum}" if maximum is not None else f"{minimum} 以上"
        raise ConfigError(f"{prefix}{key}={value!r} 要是 {limit} 的整數")
    return value


def number(raw: dict[str, Any], key: str, default: float, minimum: float, maximum: float, prefix: str = "") -> float:
    value = raw.get(key)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not minimum <= value <= maximum:
        raise ConfigError(f"{prefix}{key}={value!r} 要是 {minimum}～{maximum} 的數字")
    return float(value)


def words(es: dict[str, Any], key: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = es.get(key)
    if value is None:
        return default
    if not isinstance(value, list) or not value or not all(isinstance(v, str) and v.strip() for v in value):
        raise ConfigError(f"es.{key} 要是非空的文字清單（大小寫不同要分開列）")
    return tuple(v.strip() for v in value)


def read_prompt(path: str, base_dir: Path | None) -> str:
    """讀 llm.system_prompt_file。提示詞一定要保留輸出格式（summary／systems／ids 的 JSON），不然回答解析不了。"""
    if not path:
        return ""
    file = Path(path) if Path(path).is_absolute() or base_dir is None else base_dir / path
    if not file.exists():
        raise ConfigError(f"llm.system_prompt_file 指到的檔案不存在：{file}")
    prompt = file.read_text(encoding="utf-8").strip()
    lacking = [word for word in ("summary", "systems", "ids") if word not in prompt]
    if lacking:
        raise ConfigError(f"llm.system_prompt_file 的提示詞少了輸出格式的欄位名稱 {lacking}："
                          f"必須要求模型只輸出含 summary、systems、ids 的 JSON（照 src/rca.py 的 SYSTEM_PROMPT 改）")
    return prompt


def read_config(path: str) -> dict[str, Any]:
    file = Path(path)
    if not file.exists():
        raise ConfigError(f"找不到設定檔 {path}（範例見 config.example.yaml；要換位置用環境變數 {CONFIG_ENV}）")
    try:
        raw = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"設定檔 {path} 不是合法的 YAML：{e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"設定檔 {path} 最外層要是物件")
    return raw


def check_keys(raw: dict[str, Any], known: set[str], where: str) -> None:
    unknown = sorted(set(raw) - known)
    if unknown:
        raise ConfigError(f"設定檔{('的 ' + where + ' ') if where else ''}有不認得的設定 {unknown}；可用：{sorted(known)}")


def section(raw: dict[str, Any], name: str, known: set[str]) -> dict[str, Any]:
    value = raw.get(name) or {}
    if not isinstance(value, dict):
        raise ConfigError(f"{name} 要是一組設定（物件）")
    check_keys(value, known, name)
    return value


def text(raw: dict[str, Any], key: str, default: str, prefix: str = "") -> str:
    value = raw.get(key, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise ConfigError(f"{prefix}{key}={value!r} 要是文字（數字開頭的密鑰請加引號）")
    return value.strip()


def field_map(es: dict[str, Any], key: str, cls: type) -> Any:
    raw = es.get(key) or {}
    names = {f.name for f in fields(cls)}
    if not isinstance(raw, dict):
        raise ConfigError(f"es.{key} 要是 欄位用途: 欄位名稱 的對照")
    check_keys(raw, names, f"es.{key}")
    values = {k: text(raw, k, getattr(cls(), k), f"es.{key}.") for k in raw}
    # 唯一可以留空的是日誌的 level：日誌沒有等級欄位時，改用訊息文字挑錯誤日誌（grafana.search_errors）
    optional = {"level"} if cls is LogFields else set()
    if not all(v for k, v in values.items() if k not in optional):
        raise ConfigError(f"es.{key} 的欄位名稱不能是空的" + ("（只有 level 可以留空）" if optional else ""))
    return cls(**values)


def parse_edges(items: Any) -> tuple[tuple[str, str], ...]:
    if not isinstance(items, list):
        raise ConfigError("edges 要是清單")
    edges = []
    for item in items:
        pair = [p.strip() for p in item.split("->")] if isinstance(item, str) else item
        if (not isinstance(pair, list) or len(pair) != 2 or not all(isinstance(p, str) and p.strip() for p in pair)
                or pair[0].strip() == pair[1].strip()):
            raise ConfigError(f"關係邊 {item!r} 寫錯：要是 [上游, 下游] 兩個不同的系統名稱")
        edges.append((pair[0].strip(), pair[1].strip()))
    return tuple(edges)


def split_batch(alerts: list[grouping.Alert]) -> tuple[list[grouping.Alert], list[grouping.Alert]]:
    """把一個視窗內依序收到的告警整理成 (響過的, 最新狀態是 resolved 的)；同一個 fingerprint 各留最後一筆。"""
    firing: dict[str, grouping.Alert] = {}
    latest: dict[str, grouping.Alert] = {}
    for alert in alerts:
        latest[alert.fingerprint] = alert
        if alert.status == "firing":
            firing[alert.fingerprint] = alert
    return list(firing.values()), [a for a in latest.values() if a.status == "resolved"]


def backoff_seconds(failures: int, initial: int, maximum: int) -> int:
    """連續失敗第 n 次之後要等多久：initial、2×、4×…，最多 maximum（指數退避）。"""
    return min(initial * 2 ** min(max(failures - 1, 0), 30), maximum)


def process(settings: Settings, *, now: datetime | None = None, grafana_transport: httpx.BaseTransport | None = None,
            llm_transport: httpx.BaseTransport | None = None) -> dict[str, Any]:
    """跑一輪，回傳這一輪的摘要（needs_followup = 還有事要做，沒有新 Webhook 也要再跑）。
    transport 參數只是 HTTP 連線的注入點（預設真的連出去）。"""
    started = datetime.now(timezone.utc)
    now = (now or started).astimezone(timezone.utc)
    edges = list(settings.edges)
    db = Database(settings.database_url)
    grafana = Grafana(settings.grafana_url, settings.grafana_token, transport=grafana_transport,
                      timeout=settings.grafana_timeout_seconds, retries=settings.grafana_retries,
                      retry_backoff_seconds=settings.grafana_retry_backoff_seconds)
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
            if inc.status != "open":
                if not inc.summary:
                    inc.summary = f"[未經分析] {rca.raw_summary(inc)}"
                continue
            # 成員或嚴重度變了就重新分析；上一輪 LLM 沒成功的進行中事故，到了退避時間再試
            if not (inc.fresh or not inc.summary or (can_analyze and not inc.analyzed)):
                continue
            if can_analyze and inc.llm_next_try is not None and now < inc.llm_next_try:
                # 退避中：不查 ES、不打 LLM。摘要退回原始版本，但成員變了要跟著更新（誠實：沒有分析過）
                raw = f"[未經分析] {rca.raw_summary(inc)}"
                if (inc.summary, inc.analyzed) != (raw, False):
                    inc.summary, inc.analyzed, inc.changed, inc.annotated = raw, False, True, False
                log.debug("事故 %s 的 LLM 分析退避中，%s 之前不再試", inc.uid, inc.llm_next_try.isoformat())
                continue
            evidence = rca.collect_evidence(
                inc, edges, grafana, es_datasource_uid=settings.es_datasource_uid, log_index=settings.log_index,
                trace_index=settings.trace_index, id_fields=list(settings.id_fields), now=now,
                log_fields=settings.log_fields, trace_fields=settings.trace_fields, limits=settings.evidence,
                    tempo_datasource_uid=settings.tempo_datasource_uid)
            before = (inc.summary, inc.analyzed)
            inc.summary, inc.analyzed, reason = rca.analyze(
                inc, evidence, api_url=settings.llm_api_url, api_key=settings.llm_api_key,
                model=settings.llm_model, transport=llm_transport, options=settings.llm)
            if (inc.summary, inc.analyzed) != before:
                inc.changed, inc.annotated = True, False
            if not can_analyze:
                continue  # 沒設定 LLM 不是故障：不算失敗、不退避
            if inc.analyzed:
                if inc.llm_failures or inc.llm_next_try:
                    inc.llm_failures, inc.llm_next_try, inc.changed = 0, None, True
            else:
                inc.llm_failures += 1
                delay = backoff_seconds(inc.llm_failures, settings.llm_retry_initial_seconds, settings.llm_retry_max_seconds)
                inc.llm_next_try, inc.changed = now + timedelta(seconds=delay), True
                errors.append(f"事故 {inc.uid} 未經 LLM 分析（連續第 {inc.llm_failures} 次）：{reason}；{delay} 秒後再試")
        # 寫入順序：先落地自己的資料庫（事故 + 這批告警處理完了），再寫 Grafana；註記寫成功才記 annotated
        db.save_incidents([i for i in incidents if i.changed], now, processed=ids)
        written = []
        for inc in incidents:
            if inc.annotated:
                continue
            try:
                inc.annotation_id = grafana.upsert_annotation(
                    inc.annotation_id, report.uid_tag(inc.uid), report.render(inc, settings.grafana_url, settings.tz,
                                                                        settings.annotation_max_rule_links))
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
                            alerts=len(batch), open_incidents=open_count, errors=errors,
                            retention_days=settings.rounds_retention_days)
        except Exception:  # 連執行紀錄都寫不進去（資料庫斷線）：留一行 log，不蓋掉原本的錯誤
            log.exception("這一輪的執行紀錄寫不進資料庫")
        grafana.close()
        db.close()
    return summary


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # 每個 HTTP 請求一行太吵；錯誤我們自己會記


def log_summary(summary: dict[str, Any]) -> None:
    for line in summary["errors"]:
        log.warning("%s", line)
    log.info("這一輪結束：status=%s alerts=%d open_incidents=%d", summary["status"], summary["alerts"],
             summary["open_incidents"])


def main() -> int:
    setup_logging()
    try:
        settings = Settings.load()
        setup_logging(settings.log_level)
        log_summary(process(settings))
    except ConfigError as e:
        log.error("設定錯誤：%s", e)
        return 2
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
