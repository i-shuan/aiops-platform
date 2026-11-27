"""設定：讀 config.yaml、嚴格檢查、變成 Settings。**所有設定都在一份 config.yaml**（沒有其他環境變數）。

檔案路徑預設 /etc/aiops/config.yaml，要換用環境變數 AIOPS_CONFIG_FILE 指定（唯一的一個）。
檔案不存在、鍵名打錯、型別不對、必填沒填、數值超出範圍，啟動時直接報錯（ConfigError）。完整範例見 config.example.yaml，
每一項的說明見 CONFIG.md。檔案裡有 Token 與密碼：不要推上 Git（.gitignore 已排除 config.yaml）；Kubernetes 用 Secret 掛載。
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

from src import grouping, rca
from src import outputs as outlets
from src.grafana import LabelNames
from src.logs import LogFields

DEFAULT_CONFIG_PATH = "/etc/aiops/config.yaml"
CONFIG_ENV = "AIOPS_CONFIG_FILE"
TIMING_DEFAULTS = {"debounce_seconds": 60, "followup_seconds": 60, "close_after_seconds": 300}
TIMING_MINIMUM = {"debounce_seconds": 0, "followup_seconds": 1, "close_after_seconds": 0}
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


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
    logs_type: str = "victorialogs"            # 日誌的來源：victorialogs／elasticsearch（logs.type）
    logs_datasource_uid: str = ""              # Grafana 上那個資料源的 uid；空的 = 沒有日誌證據
    logs_filter: str = ""                      # 只有 VictoriaLogs：加在每條日誌查詢前面的 LogsQL 條件（logs.extra_filter）
    logs_index: str = "*"                      # 只有 Elasticsearch：要查的索引（logs.index）
    traces_type: str = "victoriatraces"        # Trace 的來源：victoriatraces（Grafana 的 Jaeger 型別資料源）／tempo
    traces_datasource_uid: str = ""            # Grafana 上那個資料源的 uid；空的 = 沒有呼叫鏈證據
    graph_retention_days: int = 30             # 學到的系統關係多久沒再看到就丟掉（traces.graph_retention_days）；0 = 不學、不用、也不跨系統合併
    service_graph_datasource_uid: str = ""     # 只有 Tempo：存 service graph 指標的那個 Prometheus 相容資料源的 uid；空的 = 沒有這個來源
    #                                            # （VictoriaTraces 的 service graph 從 traces_datasource_uid 讀，不用另外設）
    metrics: rca.MetricOptions = rca.MetricOptions()   # 指標走勢；沒設資料源或沒有查詢 = 不查
    id_fields: tuple[str, ...] = ("eqp_id", "lot_id", "carrier_id")
    log_fields: LogFields = LogFields()
    outputs: outlets.OutputOptions = outlets.OutputOptions()   # 註記以外的出口：事故 API、自動修復（沒設 url = 佔位）
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
    annotation_max_rule_links: int = 5         # 註記最後一行最多列幾條規則連結

    @classmethod
    def load(cls, path: str | None = None) -> "Settings":
        """讀 config.yaml。path 沒給就看環境變數 AIOPS_CONFIG_FILE，再沒有就用 /etc/aiops/config.yaml。"""
        path = path or os.environ.get(CONFIG_ENV) or DEFAULT_CONFIG_PATH
        return cls.from_dict(read_config(path), base_dir=Path(path).parent)

    @classmethod
    def from_dict(cls, raw: dict[str, Any], base_dir: Path | None = None) -> "Settings":
        """base_dir：llm.system_prompt_file 寫相對路徑時相對於哪裡（= 設定檔所在的資料夾）。"""
        known = {"grafana", "webhook", "database", "llm", "logs", "traces", "outputs", "labels", "healthcheck", "annotation", "metrics", "timezone",
                 "log_level", "retry_failed_round_seconds", "rounds_retention_days", *TIMING_DEFAULTS}
        moved = [f"{old}（改成 {new}）" for old, new in (("es", "logs，並加上 type: elasticsearch"), ("tempo", "traces，並加上 type: tempo"))
                 if old in raw]
        if moved:
            raise ConfigError(f"設定的區段改名了：{'、'.join(moved)}。logs 底下的 log_index 改叫 index、log_fields 改叫 fields，"
                              f"trace_index 與 trace_fields 沒有了，見 config.example.yaml")
        if "edges" in raw:
            raise ConfigError("edges 已經移除：系統之間的關係現在全部由程式從 Tempo 的 service graph、事故的呼叫鏈與錯誤日誌學，"
                              "不用也不能手寫。請把設定檔裡的 edges 整段刪掉（見 README 的「系統之間的關係」）")
        check_keys(raw, known, "")
        grafana = section(raw, "grafana", {"url", "token", "timeout_seconds", "retries", "retry_backoff_seconds"})
        webhook = section(raw, "webhook", {"token", "port", "host", "max_body_bytes"})
        database = section(raw, "database", {"url"})
        llm = section(raw, "llm", {"url", "api_key", "model", "temperature", "timeout_seconds", "retry_initial_seconds",
                                   "retry_max_seconds", "max_log_samples", "max_message_chars",
                                   "top_ids", "max_summary_chars", "system_prompt_file", "id_pattern",
                                   "max_timeline_ids", "max_timeline_events", "max_log_patterns",
                                   "max_warn_patterns"})
        logs = section(raw, "logs", {"type", "datasource_uid", "extra_filter", "index", "id_fields", "fields", "error_levels", "warn_levels",
                                     "max_hits", "window_padding_minutes", "timeline_fields", "baseline_minutes"})
        traces = section(raw, "traces", {"type", "datasource_uid", "graph_retention_days", "service_graph_datasource_uid", "max_traces", "max_spans", "trace_id_field"})
        labels = section(raw, "labels", {"detect", "severity", "service", "default_severity", "remediation"})
        metrics = section(raw, "metrics", {"datasource_uid", "queries", "lookback_minutes", "step_seconds", "max_points",
                                           "max_series"})
        health = section(raw, "healthcheck", {"stuck_seconds"})
        annotation = section(raw, "annotation", {"max_rule_links"})
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
        names = LabelNames(**{k: text(labels, k, getattr(d_lab, k), "labels.") for k in ("detect", "severity", "service", "remediation")},
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
        logs_type = one_of(logs, "type", "victorialogs", LOG_STORES, "logs.")
        for key, only in (("extra_filter", "victorialogs"), ("index", "elasticsearch")):
            if key in logs and logs_type != only:
                raise ConfigError(f"logs.{key} 只有 logs.type 是 {only} 的時候才有用（現在是 {logs_type}）")
        ids = logs.get("id_fields", list(cls.id_fields))
        if not isinstance(ids, list) or not ids or not all(isinstance(i, str) and i.strip() for i in ids):
            raise ConfigError("logs.id_fields 要是非空的欄位名稱清單")
        ids = [i.strip() for i in ids]
        timeline = logs.get("timeline_fields")
        if timeline is None:
            timeline = [f for f in d_ev.timeline_fields if f in ids]   # 預設：lot_id、carrier_id 裡有在統計的那幾個
        if not isinstance(timeline, list) or not all(isinstance(i, str) and i.strip() for i in timeline):
            raise ConfigError("logs.timeline_fields 要是欄位名稱清單（空清單 = 不做關聯時間線）")
        timeline = [i.strip() for i in timeline]
        strangers = [f for f in timeline if f not in ids]
        if strangers:
            raise ConfigError(f"logs.timeline_fields 的 {strangers} 不在 logs.id_fields 裡：時間線只能用有在統計的識別碼")
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
            logs_type=logs_type, logs_datasource_uid=text(logs, "datasource_uid", "", "logs."),
            logs_filter=text(logs, "extra_filter", "", "logs."), logs_index=text(logs, "index", "*", "logs.") or "*",
            id_fields=tuple(ids), traces_type=one_of(traces, "type", "victoriatraces", TRACE_STORES, "traces."),
            traces_datasource_uid=text(traces, "datasource_uid", "", "traces."),
            graph_retention_days=integer(traces, "graph_retention_days", cls.graph_retention_days, 0, 365, "traces."),
            service_graph_datasource_uid=service_graph_uid(traces),
            metrics=metric_options(metrics),
            log_fields=log_field_names(logs, logs_type), outputs=output_options(raw),
            evidence=rca.EvidenceLimits(
                max_hits=integer(logs, "max_hits", d_ev.max_hits, 1, 10000, "logs."),
                window_padding_minutes=integer(logs, "window_padding_minutes", d_ev.window_padding_minutes, 0, 1440, "logs."),
                error_levels=words(logs, "error_levels", d_ev.error_levels),
                max_log_samples=integer(llm, "max_log_samples", d_ev.max_log_samples, 0, prefix="llm."),
                max_message_chars=integer(llm, "max_message_chars", d_ev.max_message_chars, 20, prefix="llm."),
                top_ids=integer(llm, "top_ids", d_ev.top_ids, 1, prefix="llm."),
                timeline_fields=tuple(timeline),
                max_timeline_ids=integer(llm, "max_timeline_ids", d_ev.max_timeline_ids, 0, 20, "llm."),
                max_timeline_events=integer(llm, "max_timeline_events", d_ev.max_timeline_events, 1, 500, "llm."),
                trace_id_field=text(traces, "trace_id_field", d_ev.trace_id_field, "traces."),
                max_traces=integer(traces, "max_traces", d_ev.max_traces, 0, 20, "traces."),
                max_trace_spans=integer(traces, "max_spans", d_ev.max_trace_spans, 1, 500, "traces."),
                baseline_minutes=integer(logs, "baseline_minutes", d_ev.baseline_minutes, 5, 1440, "logs."),
                max_log_patterns=integer(llm, "max_log_patterns", d_ev.max_log_patterns, 0, 20, "llm."),
                warn_levels=warn_levels(logs, d_ev.warn_levels),
                max_warn_patterns=integer(llm, "max_warn_patterns", d_ev.max_warn_patterns, 0, 20, "llm.")),
            labels=names, tz=tz, log_level=level,
            retry_failed_round_seconds=integer(raw, "retry_failed_round_seconds", cls.retry_failed_round_seconds, 1),
            healthcheck_stuck_seconds=integer(health, "stuck_seconds", cls.healthcheck_stuck_seconds, 30, prefix="healthcheck."),
            rounds_retention_days=integer(raw, "rounds_retention_days", cls.rounds_retention_days, 1),
            annotation_max_rule_links=integer(annotation, "max_rule_links", cls.annotation_max_rule_links, 0, prefix="annotation."),
            **{key: integer(raw, key, default, TIMING_MINIMUM[key]) for key, default in TIMING_DEFAULTS.items()})


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


def words(logs: dict[str, Any], key: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = logs.get(key)
    if value is None:
        return default
    if not isinstance(value, list) or not value or not all(isinstance(v, str) and v.strip() for v in value):
        raise ConfigError(f"logs.{key} 要是非空的文字清單（大小寫不同要分開列）")
    return tuple(v.strip() for v in value)


def warn_levels(logs: dict[str, Any], default: tuple[str, ...]) -> tuple[str, ...]:
    """logs.warn_levels：跟 error_levels 一樣是文字清單，但可以是空的（= 不撈警告）。"""
    value = logs.get("warn_levels")
    if value is None:
        return default
    if not isinstance(value, list) or not all(isinstance(v, str) and v.strip() for v in value):
        raise ConfigError("logs.warn_levels 要是文字清單（大小寫不同要分開列；空清單 = 不撈警告）")
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


def section(raw: dict[str, Any], name: str, known: set[str], prefix: str = "") -> dict[str, Any]:
    value = raw.get(name) or {}
    if not isinstance(value, dict):
        raise ConfigError(f"{prefix}{name} 要是一組設定（物件）")
    check_keys(value, known, f"{prefix}{name}")
    return value


def text(raw: dict[str, Any], key: str, default: str, prefix: str = "") -> str:
    value = raw.get(key, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise ConfigError(f"{prefix}{key}={value!r} 要是文字（數字開頭的密鑰請加引號）")
    return value.strip()


def output_options(raw: dict[str, Any]) -> outlets.OutputOptions:
    """outputs：註記以外的兩個出口。url 留空 = 佔位（不送，只把本來會送的內容記進資料庫）。"""
    out = section(raw, "outputs", {"incident_api", "remediation", "timeout_seconds"})
    api = section(out, "incident_api", {"url", "token"}, "outputs.")
    fix = section(out, "remediation", {"url", "token", "enabled"}, "outputs.")
    enabled = fix.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ConfigError("outputs.remediation.enabled 要是 true 或 false")
    if enabled and not text(fix, "url", "", "outputs.remediation."):
        raise ConfigError("outputs.remediation.enabled 是 true，但沒有設 outputs.remediation.url")
    for name, part in (("incident_api", api), ("remediation", fix)):
        url = text(part, "url", "", f"outputs.{name}.")
        if url and not url.startswith(("http://", "https://")):
            raise ConfigError(f"outputs.{name}.url 要是 http:// 或 https:// 開頭的網址")
    return outlets.OutputOptions(
        incident_api_url=text(api, "url", "", "outputs.incident_api."), incident_api_token=text(api, "token", "", "outputs.incident_api."),
        remediation_url=text(fix, "url", "", "outputs.remediation."), remediation_token=text(fix, "token", "", "outputs.remediation."),
        remediation_enabled=enabled, timeout_seconds=number(out, "timeout_seconds", 10.0, 1, 120, "outputs."))


def service_graph_uid(traces: dict[str, Any]) -> str:
    uid = text(traces, "service_graph_datasource_uid", "", "traces.")
    if uid and traces.get("type", "victoriatraces") != "tempo":
        raise ConfigError("traces.service_graph_datasource_uid 只有 traces.type 是 tempo 的時候才有用："
                          "VictoriaTraces 的 service graph 從 traces.datasource_uid 讀，不用另外設")
    return uid


LOG_STORES = ("victorialogs", "elasticsearch")
TRACE_STORES = ("victoriatraces", "tempo")


def one_of(section: dict[str, Any], key: str, default: str, choices: tuple[str, ...], prefix: str) -> str:
    value = section.get(key, default)
    if value not in choices:
        raise ConfigError(f"{prefix}{key} 要是 {'／'.join(choices)} 其中之一，現在是 {value!r}")
    return value


def log_field_names(logs: dict[str, Any], logs_type: str) -> LogFields:
    """logs.fields：日誌裡各個欄位叫什麼。service、level 兩種來源都有；time、message 只有 Elasticsearch 能設
    （VictoriaLogs 的時間與訊息固定是 _time、_msg）。唯一可以留空的是 level：改用訊息文字挑錯誤日誌。"""
    raw = logs.get("fields") or {}
    if not isinstance(raw, dict):
        raise ConfigError("logs.fields 要是 欄位用途: 欄位名稱 的對照")
    allowed = {"service", "level"} | ({"time", "message"} if logs_type == "elasticsearch" else set())
    check_keys(raw, allowed, "logs.fields")
    values = {k: text(raw, k, getattr(LogFields(), k), "logs.fields.") for k in raw}
    empty = [k for k, v in values.items() if not v and k != "level"]
    if empty:
        raise ConfigError(f"logs.fields 的欄位名稱不能是空的（只有 level 可以留空）：{empty}")
    return LogFields(**values)


MAX_METRIC_QUERIES = 20   # 每張事故要查「查詢數 × 系統數」次：設個上限，避免一份設定把指標資料庫打爆


def metric_options(raw: dict[str, Any]) -> rca.MetricOptions:
    d = rca.MetricOptions()
    items = raw.get("queries") or []
    if not isinstance(items, list) or len(items) > MAX_METRIC_QUERIES:
        raise ConfigError(f"metrics.queries 要是清單，最多 {MAX_METRIC_QUERIES} 條")
    queries, seen = [], set()
    for item in items:
        if not isinstance(item, dict):
            raise ConfigError(f"metrics.queries 的每一條要是 {{name, expr}}：{item!r}")
        check_keys(item, {"name", "expr", "services"}, "metrics.queries")
        name, expr = text(item, "name", "", "metrics.queries."), text(item, "expr", "", "metrics.queries.")
        if not name or not expr:
            raise ConfigError(f"metrics.queries 的每一條都要有 name 與 expr：{item!r}")
        if name in seen:
            raise ConfigError(f"metrics.queries 有兩條都叫 {name!r}：名稱會出現在給 LLM 的證據裡，不能重複")
        seen.add(name)
        if rca.SERVICE_PLACEHOLDER not in expr:
            raise ConfigError(f"metrics.queries「{name}」的 expr 裡沒有 {rca.SERVICE_PLACEHOLDER}：每個系統會各查一次，"
                              f"查詢要用它指定系統，例如 {{service=\"{rca.SERVICE_PLACEHOLDER}\"}}")
        only = item.get("services") or []
        if not isinstance(only, list) or not all(isinstance(s, str) and s.strip() for s in only):
            raise ConfigError(f"metrics.queries「{name}」的 services 要是系統名稱清單")
        queries.append(rca.MetricQuery(name=name, expr=expr, services=tuple(s.strip() for s in only)))
    return rca.MetricOptions(
        datasource_uid=text(raw, "datasource_uid", "", "metrics."), queries=tuple(queries),
        lookback_minutes=integer(raw, "lookback_minutes", d.lookback_minutes, 5, 1440, "metrics."),
        step_seconds=integer(raw, "step_seconds", d.step_seconds, 5, 3600, "metrics."),
        max_points=integer(raw, "max_points", d.max_points, 2, 60, "metrics."),
        max_series=integer(raw, "max_series", d.max_series, 1, 20, "metrics."))


