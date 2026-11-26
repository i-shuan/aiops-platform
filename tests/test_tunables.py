"""config.yaml 的選填調整項（原本寫死在程式裡的值）：每一項都要真的影響行為，沒寫時行為跟以前一樣。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from src import db as dbmod
from src import rca, report
from src.grafana import Grafana, GrafanaError, LabelNames, parse_webhook
from src.grouping import converge
from src.main import ConfigError, Settings
from src.logs import VictoriaLogs
from tests.conftest import collect_evidence, replay, webhook

T0 = datetime(2026, 11, 21, 2, 41, 10, tzinfo=timezone.utc)
NOW = T0 + timedelta(seconds=64)
EDGES = [("mq-broker", "cim-eap")]
MINIMAL = "grafana: {url: 'http://g', token: 't'}\nwebhook: {token: 'w'}\n"
LLM = {"api_url": "http://llm.test/v1", "api_key": "k", "model": "m"}
ROOT = Path(__file__).resolve().parents[1]

ALL_OPTIONAL = """
grafana: {url: 'http://g', token: 't', timeout_seconds: 10, retries: 4, retry_backoff_seconds: 1.5}
webhook: {token: 'w', host: 127.0.0.1, max_body_bytes: 2000000}
llm:
  timeout_seconds: 180
  max_log_samples: 5
  max_message_chars: 120
  top_ids: 3
  max_summary_chars: 80
  system_prompt_file: prompt.txt
  id_pattern: "EQP-[A-Z]+-[0-9]+"
  max_timeline_ids: 4
  max_timeline_events: 50
logs:
  error_levels: [E, SEVERE]
  max_hits: 500
  window_padding_minutes: 30
  timeline_fields: [carrier_id]
traces: {datasource_uid: traces-1, max_traces: 5, max_spans: 60}
labels: {detect: managed_by_aiops, severity: priority, service: system, default_severity: p2}
healthcheck: {stuck_seconds: 1200}
annotation: {max_rule_links: 2}
retry_failed_round_seconds: 10
rounds_retention_days: 30
"""
PROMPT = '你是工廠 SRE。只輸出 JSON：{"summary": "...", "systems": [], "ids": []}'


def load(tmp_path, text):
    (tmp_path / "prompt.txt").write_text(PROMPT, encoding="utf-8")
    f = tmp_path / "config.yaml"
    f.write_text(text, encoding="utf-8")
    return Settings.load(str(f))


def incident(*names):
    alerts = [a for n in (names or ("firing_mq", "firing_cim")) for a in parse_webhook(webhook(n))[0]]
    [inc] = converge([], alerts, [], EDGES, now=NOW, close_after_seconds=300)
    return inc


def llm_returning(answer):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(answer, ensure_ascii=False)}}]})

    return httpx.MockTransport(handler), seen


def test_every_optional_setting_is_read_from_the_file(tmp_path):
    s = load(tmp_path, ALL_OPTIONAL)
    assert (s.grafana_timeout_seconds, s.grafana_retries, s.grafana_retry_backoff_seconds) == (10.0, 4, 1.5)
    assert (s.webhook_host, s.webhook_max_body_bytes) == ("127.0.0.1", 2000000)
    assert (s.llm.timeout_seconds, s.llm.max_summary_chars, s.llm.id_pattern) == (180.0, 80, "EQP-[A-Z]+-[0-9]+")
    assert s.llm.system_prompt == PROMPT
    e = s.evidence
    assert (e.max_log_samples, e.max_message_chars, e.top_ids) == (5, 120, 3)
    assert (e.error_levels, e.max_hits, e.window_padding_minutes) == (("E", "SEVERE"), 500, 30)
    assert (e.timeline_fields, e.max_timeline_ids, e.max_timeline_events) == (("carrier_id",), 4, 50)
    assert (s.traces_datasource_uid, e.max_traces, e.max_trace_spans) == ("traces-1", 5, 60)
    assert s.labels == LabelNames("managed_by_aiops", "priority", "system", "P2")
    assert (s.healthcheck_stuck_seconds, s.annotation_max_rule_links) == (1200, 2)
    assert (s.retry_failed_round_seconds, s.rounds_retention_days) == (10, 30)


def test_without_the_optional_settings_everything_keeps_its_old_value(tmp_path):
    """這些預設值就是原本寫死在程式裡的數字：改預設值等於改所有沒寫設定的部署的行為。"""
    s = load(tmp_path, MINIMAL)
    assert (s.grafana_timeout_seconds, s.grafana_retries, s.grafana_retry_backoff_seconds) == (30.0, 2, 0.5)
    assert (s.webhook_host, s.webhook_max_body_bytes) == ("0.0.0.0", 1_000_000)
    assert (s.llm.timeout_seconds, s.llm.temperature, s.llm.max_summary_chars, s.llm.system_prompt) == (60.0, None, 300, "")
    assert s.llm.id_pattern == rca.DEFAULT_ID_PATTERN
    e = s.evidence
    assert (e.max_hits, e.window_padding_minutes, e.max_log_samples, e.max_message_chars, e.top_ids) == (100, 10, 20, 300, 10)
    assert e.error_levels == ("ERROR", "error", "Error", "FATAL", "fatal", "CRITICAL", "critical")
    assert (e.timeline_fields, e.max_timeline_ids, e.max_timeline_events) == (("lot_id", "carrier_id"), 2, 30)
    assert (s.traces_datasource_uid, e.max_traces, e.max_trace_spans) == ("", 3, 30)
    assert s.labels == LabelNames("aiops_detect", "aiops_severity", "service", "P3")
    assert (s.healthcheck_stuck_seconds, s.annotation_max_rule_links, s.retry_failed_round_seconds, s.rounds_retention_days) == (600, 5, 30, 90)


@pytest.mark.parametrize("text, message", [
    ("grafana: {url: g, token: t, timeout_seconds: 0}\nwebhook: {token: w}", "grafana.timeout_seconds"),
    ("grafana: {url: g, token: t, retries: 11}\nwebhook: {token: w}", "grafana.retries"),
    ("grafana: {url: g, token: t, retries: 1.5}\nwebhook: {token: w}", "grafana.retries"),
    ("grafana: {url: g, token: t, retry_backoff_seconds: -1}\nwebhook: {token: w}", "grafana.retry_backoff_seconds"),
    ("grafana: {url: g, token: t}\nwebhook: {token: w, max_body_bytes: 10}", "webhook.max_body_bytes"),
    (MINIMAL + "llm: {timeout_seconds: fast}", "llm.timeout_seconds"),
    (MINIMAL + "llm: {max_log_samples: -1}", "llm.max_log_samples"),
    (MINIMAL + "llm: {max_message_chars: 5}", "llm.max_message_chars"),
    (MINIMAL + "llm: {top_ids: 0}", "llm.top_ids"),
    (MINIMAL + "llm: {max_summary_chars: 3}", "llm.max_summary_chars"),
    (MINIMAL + "llm: {id_pattern: '[A-'}", "llm.id_pattern"),
    (MINIMAL + "llm: {system_prompt_file: nowhere.txt}", "不存在"),
    (MINIMAL + "llm: {system_prompt: hi}", "不認得的設定"),
    (MINIMAL + "logs: {error_levels: []}", "logs.error_levels"),
    (MINIMAL + "logs: {error_levels: ERROR}", "logs.error_levels"),
    (MINIMAL + "logs: {error_statuses: [FAILED]}", "logs 有不認得的設定"),      # 這一項隨 ES 的 Trace 片段一起拿掉了
    (MINIMAL + "llm: {max_trace_samples: 3}", "llm 有不認得的設定"),
    (MINIMAL + "logs: {max_hits: 0}", "logs.max_hits"),
    (MINIMAL + "logs: {max_hits: 20000}", "logs.max_hits"),
    (MINIMAL + "logs: {window_padding_minutes: -5}", "logs.window_padding_minutes"),
    (MINIMAL + "labels: {default_severity: critical}", "labels.default_severity"),
    (MINIMAL + "labels: {service: ''}", "三個不同、非空"),
    (MINIMAL + "labels: {detect: service}", "三個不同、非空"),
    (MINIMAL + "labels: {name: x}", "labels 有不認得的設定"),
    (MINIMAL + "healthcheck: {stuck_seconds: 5}", "healthcheck.stuck_seconds"),
    (MINIMAL + "annotation: {max_rule_links: -1}", "annotation.max_rule_links"),
    (MINIMAL + "retry_failed_round_seconds: 0", "retry_failed_round_seconds"),
    (MINIMAL + "rounds_retention_days: 0", "rounds_retention_days"),
])
def test_optional_settings_are_validated_at_startup(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load(tmp_path, text + "\n")


def test_integers_written_as_whole_floats_are_accepted(tmp_path):
    """Helm 把數字轉成 YAML 時可能寫成 500.0；不是整數的小數照樣拒絕。"""
    assert load(tmp_path, MINIMAL + "logs: {max_hits: 500.0}\ndebounce_seconds: 90.0\n").evidence.max_hits == 500


def test_a_custom_prompt_must_keep_the_output_contract(tmp_path):
    (tmp_path / "bad.txt").write_text("你是 SRE，請用一句話回答。", encoding="utf-8")
    with pytest.raises(ConfigError, match="summary.*systems.*ids"):
        load(tmp_path, MINIMAL + "llm: {system_prompt_file: bad.txt}\n")
    absolute = tmp_path / "abs.txt"
    absolute.write_text(PROMPT, encoding="utf-8")
    assert load(tmp_path, MINIMAL + f"llm: {{system_prompt_file: {absolute}}}\n").llm.system_prompt == PROMPT


def test_every_optional_setting_is_documented_in_the_example_file():
    text = (ROOT / "config.example.yaml").read_text(encoding="utf-8")
    for key in ("timeout_seconds", "retries", "retry_backoff_seconds", "host", "max_body_bytes", "max_log_samples",
                "max_message_chars", "top_ids", "max_summary_chars", "system_prompt_file",
                "id_pattern", "error_levels", "logs:", "extra_filter", "max_hits", "window_padding_minutes", "labels:",
                "default_severity", "stuck_seconds", "max_rule_links", "retry_failed_round_seconds",
                "rounds_retention_days", "temperature", "timeline_fields", "max_timeline_ids",
                "max_timeline_events", "traces:", "max_traces", "max_spans", "trace_id_field"):
        assert key in text, f"config.example.yaml 沒有提到 {key}"


# ── 每一項真的影響行為 ──

def test_label_names_and_default_severity_are_configurable():
    names = LabelNames(detect="managed_by_aiops", severity="priority", service="system", default_severity="P1")
    payload = webhook("firing_mq")
    labels = payload["alerts"][0]["labels"]
    assert parse_webhook(payload, names) == ([], [])                       # 原本的標籤名稱不被認
    payload["alerts"][0]["labels"] = {"alertname": "MQ", "managed_by_aiops": "true", "priority": "P0", "system": "mq"}
    [alert], problems = parse_webhook(payload, names)
    assert (alert.service, alert.severity, problems) == ("mq", "P0", [])
    del payload["alerts"][0]["labels"]["priority"]
    [alert], problems = parse_webhook(payload, names)
    assert alert.severity == "P1" and "priority" in problems[0] and "當成 P1" in problems[0]
    del payload["alerts"][0]["labels"]["system"]
    assert "沒有 system 標籤" in parse_webhook(payload, names)[1][0]
    assert labels["aiops_detect"] == "true"                                 # 樣本本身用的是預設名稱


def test_log_query_uses_the_configured_levels_size_and_padding(fake):
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    limits = rca.EvidenceLimits(max_hits=7, window_padding_minutes=30, error_levels=("E", "SEVERE"))
    inc = incident()
    collect_evidence(inc, EDGES, g, logs_datasource_uid="vl", id_fields=["eqp_id"], now=NOW, limits=limits)
    [sent] = [q for q in fake.log_queries if "sort by (_time) desc" in q["query"]]
    assert sent["query"].endswith('"log.level":in("E","SEVERE") | sort by (_time) desc | limit 7') and "ERROR" not in sent["query"]
    assert sent["start"] == (inc.started_at - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    assert sent["end"] == NOW.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def test_evidence_limits_control_how_much_the_llm_sees(fake):
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    limits = rca.EvidenceLimits(max_log_samples=2, max_message_chars=20, top_ids=1)
    ev = collect_evidence(incident(), EDGES, g, logs_datasource_uid="vl", id_fields=["eqp_id", "lot_id"], now=NOW, limits=limits)
    assert len(ev["error_logs"]) == 2
    assert all(len(l["message"]) <= 20 for l in ev["error_logs"])
    assert ev["id_counts"] == {"eqp_id": {"EQP-CVD-07": 3}, "lot_id": {"LOT24A0031": 2}}  # 統計看全部撈到的，不是只看樣本
    none = collect_evidence(incident(), EDGES, g, logs_datasource_uid="vl", id_fields=["eqp_id"], now=NOW,
                                limits=rca.EvidenceLimits(max_log_samples=0))
    assert none["error_logs"] == [] and none["id_counts"]["eqp_id"]["EQP-CVD-07"] == 3


def evidence(fake):
    g = Grafana("http://grafana.test", "t", transport=fake.transport)
    return collect_evidence(incident(), EDGES, g, logs_datasource_uid="vl", id_fields=["eqp_id", "lot_id", "carrier_id"], now=NOW)


GOOD = {"summary": "源頭可能是 mq-broker，錯誤集中在 EQP-CVD-07", "systems": ["mq-broker"], "ids": ["EQP-CVD-07"]}


def test_llm_timeout_and_system_prompt_reach_the_request(fake):
    transport, seen = llm_returning(GOOD)
    rca.analyze(incident(), evidence(fake), transport=transport, options=rca.LlmOptions(timeout_seconds=7, system_prompt=PROMPT), **LLM)
    assert seen[0].extensions["timeout"]["read"] == 7
    assert json.loads(seen[0].content)["messages"][0]["content"] == PROMPT
    transport, seen = llm_returning(GOOD)
    rca.analyze(incident(), evidence(fake), transport=transport, **LLM)
    assert seen[0].extensions["timeout"]["read"] == 60
    assert json.loads(seen[0].content)["messages"][0]["content"] == rca.SYSTEM_PROMPT


def test_generic_terms_with_digits_are_rejected_by_default_and_the_check_can_be_tuned_or_disabled(fake):
    """預設樣式的已知副作用：模型寫了 HTTP2 這種泛稱、證據裡沒有，整段會被當成幻覺。可以換樣式或關掉。"""
    generic = {"summary": "mq-broker 的 HTTP2 連線中斷，影響 cim-eap", "systems": ["mq-broker", "cim-eap"], "ids": []}
    ev = evidence(fake)
    line, analyzed, reason = rca.analyze(incident(), ev, transport=llm_returning(generic)[0], **LLM)
    assert not analyzed and "HTTP2" in reason
    only_equipment = rca.LlmOptions(id_pattern=r"EQP-[A-Z]+-\d+")
    line, analyzed, _ = rca.analyze(incident(), ev, transport=llm_returning(generic)[0], options=only_equipment, **LLM)
    assert analyzed and "HTTP2" in line
    fake_equipment = {**generic, "summary": "集中在 EQP-LITHO-11"}
    assert not rca.analyze(incident(), ev, transport=llm_returning(fake_equipment)[0], options=only_equipment, **LLM)[1]
    off = rca.LlmOptions(id_pattern="")
    assert rca.analyze(incident(), ev, transport=llm_returning(fake_equipment)[0], options=off, **LLM)[1]  # 文字不檢查了
    listed = {**generic, "ids": ["EQP-LITHO-11"]}
    assert not rca.analyze(incident(), ev, transport=llm_returning(listed)[0], options=off, **LLM)[1]      # 清單照樣檢查
    invented = {**generic, "summary": "erp-core 的 HTTP2 連線中斷", "systems": ["erp-core"]}
    assert not rca.analyze(incident(), ev, transport=llm_returning(invented)[0], options=off, **LLM)[1]


def test_summary_is_cut_at_the_configured_length(fake):
    long = {"summary": "源頭可能是 mq-broker。" + "很長的說明" * 100, "systems": ["mq-broker"], "ids": []}
    line, analyzed, _ = rca.analyze(incident(), evidence(fake), transport=llm_returning(long)[0],
                                    options=rca.LlmOptions(max_summary_chars=40), **LLM)
    assert analyzed and len(line) == len("[LLM 推論] ") + 40


def test_grafana_retries_backoff_and_timeout_are_configurable(monkeypatch):
    slept = []
    monkeypatch.setattr("src.grafana.time.sleep", slept.append)
    now = datetime(2026, 11, 21, tzinfo=timezone.utc)
    attempts = []

    def down(request):
        attempts.append(request)
        return httpx.Response(503)

    def search(**kw):
        g = Grafana("http://grafana.test", "t", transport=httpx.MockTransport(down), **kw)
        with pytest.raises(GrafanaError):
            VictoriaLogs(g, "u").search_errors(["a"], now, now)

    search(retries=0)
    assert len(attempts) == 1 and slept == []                    # 0 = 不重試
    attempts.clear()
    search(retries=4, retry_backoff_seconds=2)
    assert len(attempts) == 5 and slept == [2, 4, 6, 8]
    attempts.clear()
    search(timeout=3)
    assert attempts[0].extensions["timeout"]["read"] == 3 and len(attempts) == 3   # 預設重試 2 次


def test_round_records_older_than_the_retention_are_deleted(db_url):
    import sqlalchemy as sa
    database = dbmod.Database(db_url)
    base = dict(status="ok", alerts=0, open_incidents=0, errors=[])
    for days_ago in (100, 40, 10):
        t = NOW - timedelta(days=days_ago)
        database.record_round(started_at=t, finished_at=t, retention_days=3650, **base)

    def count():
        with database.engine.connect() as conn:  # 連線要關：留著不關，PostgreSQL 上測試結束清表時會被它卡住
            return conn.execute(sa.select(sa.func.count()).select_from(dbmod.rounds)).scalar()

    database.record_round(started_at=NOW, finished_at=NOW, **base)                      # 預設 90 天：100 天前的那筆清掉
    assert count() == 3
    database.record_round(started_at=NOW, finished_at=NOW, retention_days=30, **base)   # 30 天：40 天前的也清掉
    assert count() == 3
    database.close()


def test_number_of_rule_links_in_the_annotation_is_configurable():
    inc = incident("firing_mq", "firing_cim")
    inc.summary = "x"
    one = report.render(inc, "http://g", "Asia/Taipei", max_links=1)["text"]
    assert one.count("<a href=") == 1 and "MQ queue depth（P1）" in one and "等 2 條" in one
    assert report.render(inc, "http://g", "Asia/Taipei")["text"].count("<a href=") == 2
    none = report.render(inc, "http://g", "Asia/Taipei", max_links=0)["text"]
    assert "<a href=" not in none and "等 2 條" in none
