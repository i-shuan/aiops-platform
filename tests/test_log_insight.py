"""錯誤日誌的前處理：同類去重、各系統最早的錯誤不被擠掉、事故前後的次數、警告。
都是歸類與算術，沒有「算不算異常」的門檻。假的 Grafana 從錄下來的真實樣本（es_proxy_logs_traces.json）組回應；
真的 Elasticsearch 上的行為由 eval/run.py 的十個案例驗證（結果記在 EVAL.md）。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import rca
from src.grafana import Grafana, LogFields
from src.grouping import Alert, converge
from src.main import ConfigError, Settings

T0 = datetime(2026, 11, 21, 2, 41, 10, tzinfo=timezone.utc)
NOW = T0 + timedelta(seconds=64)
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")]
MINIMAL = "grafana: {url: 'http://g', token: 't'}\nwebhook: {token: 'w'}\n"


def incident(*services):
    alerts = [Alert(f"f{i}", "firing", f"r{i}", f"{s} 告警", s, "P2", T0 + timedelta(seconds=i), None, {"A": 1})
              for i, s in enumerate(services or ("mq-broker", "cim-eap", "amhs-mcs"))]
    [inc] = converge([], alerts, [], EDGES, now=NOW, close_after_seconds=300)
    return inc


def collect(fake, inc=None, **kw):
    grafana = Grafana("http://grafana.test", "t", transport=fake.transport, retries=0)
    return rca.collect_evidence(inc or incident(), EDGES, grafana, es_datasource_uid="es", log_index="factory-logs-*",
                                trace_index="factory-traces", id_fields=["eqp_id", "lot_id", "carrier_id"], now=NOW, **kw)


# ── 樣板 ──

@pytest.mark.parametrize("message, pattern, phrase", [
    ("heartbeat to MES retry failed attempt=312", "heartbeat to MES retry failed attempt=*", ("heartbeat to MES retry failed", 3)),
    ("request 17 failed", "request * failed", ("request failed", 3)),
    ("PLC connection lost on STK01 (10.3.2.15:502), transfers halted", "PLC connection lost on STK* (*), transfers halted",
     ("PLC connection lost on transfers halted", 6)),
    ("publish to eap.events timed out after 5000ms", "publish to eap.events timed out after *ms",
     ("publish to eap.events timed out after", 3)),
    ("id=3f2a9c1e-11aa-4b2c-9d3e-aabbccddeeff hash deadbeef12 ok", "id=* hash * ok", ("hash ok", 6)),
    ("timeout", "timeout", None),                       # 只有一個詞：不數（會把不相干的訊息都算進來）
    ("code 500 at 10.0.0.1:8080", "code * at *", ("code at", 6)),
    ("err 42", "err *", None),
])
def test_messages_become_patterns_and_countable_phrases(message, pattern, phrase):
    assert rca.log_pattern(message) == pattern
    assert rca.pattern_phrase(pattern) == phrase


def test_same_kind_of_error_with_different_numbers_is_one_pattern_but_words_are_not_touched():
    a, b = rca.log_pattern("T3 timeout eqp=EQP-CVD-07 after 5000ms"), rca.log_pattern("T3 timeout eqp=EQP-CVD-12 after 30ms")
    assert a == b == "T* timeout eqp=EQP-CVD-* after *ms"
    assert rca.log_pattern("decode failed") == "decode failed" and rca.log_pattern("cafe  bad\tface") == "cafe bad face"


# ── 去重：同一種只留最早的一筆原文 ──

def test_error_logs_keep_one_original_example_per_kind_with_the_count(fake):
    ev = collect(fake)
    assert (len(ev["error_logs"]), sum(e.get("similar_in_sample", 1) for e in ev["error_logs"])) == (4, 6)
    times = [e["time"] for e in ev["error_logs"]]
    assert times == sorted(times)                                       # 照各種錯誤第一次出現的時間排
    assert all("*" not in e["message"] for e in ev["error_logs"])        # 給模型看的是原文，不是樣板
    assert any(e.get("eqp_id") == "EQP-CVD-07" for e in ev["error_logs"])  # 識別碼欄位還在
    assert "EQP-CVD-07" in json.dumps(ev, ensure_ascii=False)            # 幻覺檢查比對得到


# ── 每個系統最早的幾筆：各系統分開問，不會互相擠掉 ──

def test_each_system_is_asked_separately_for_its_oldest_errors(fake):
    collect(fake)
    [profile] = fake.profile_requests
    per_system, warnings = profile[:-1], profile[-1]
    assert [q["query"]["bool"]["filter"][1]["bool"]["should"][0]["terms"]["service.name"] for q in per_system] == \
        [[s] for s in incident().services]
    assert all(q["sort"] == [{"@timestamp": "asc"}] and q["track_total_hits"] and q["size"] == rca.FIRST_ERRORS_PER_SYSTEM for q in per_system)
    assert "WARN" in warnings["query"]["bool"]["filter"][2]["bool"]["should"][0]["terms"]["log.level"]


def test_a_system_flooded_out_of_the_newest_sample_still_shows_its_earliest_errors(fake):
    """最新的那一批樣本全是 mq-broker（它在大量報錯）；cim-eap、amhs-mcs 較早的錯誤靠各系統分開問的那一次補回來。"""
    everything = [h["_source"] for h in fake.es["responses"][0]["hits"]["hits"]]
    fake.profile_docs = everything
    fake.es["responses"][0]["hits"]["hits"] = [h for h in fake.es["responses"][0]["hits"]["hits"]
                                               if h["_source"]["service"]["name"] == "mq-broker"]
    ev = collect(fake)
    assert {e["system"] for e in ev["error_logs"]} == {"mq-broker", "cim-eap", "amhs-mcs"}
    assert {r["system"] for r in ev["error_patterns"]} == {"mq-broker", "cim-eap", "amhs-mcs"}   # 每個系統最早的那一種都有比較


# ── 事故前後的次數 ──

def test_patterns_are_counted_during_and_before_and_new_is_a_fact_not_a_threshold(fake):
    fake.before_counts = {"timeout waiting reply": 40}   # 「T3 timeout waiting S6F12 reply」的樣板是 T* … S*F*，片語不含那兩個詞
    ev = collect(fake)
    by_example = {r["example"]: r for r in ev["error_patterns"]}
    old = by_example["T3 timeout waiting S6F12 reply"]
    assert (old["during"], old["before"], old["new"]) == (3, 40, False)
    fresh = [r for r in ev["error_patterns"] if r.get("new")]
    assert fresh and all(r["before"] == 0 for r in fresh)
    assert ev["count_windows"]["before_minutes"] == 30
    # 計數查詢：兩兩一組，同一個片語，時間窗一個是事故期間、一個是事故前緊接的 30 分鐘
    [phrases] = fake.phrase_requests
    during, before = phrases[0]["query"]["bool"]["filter"][0]["range"]["@timestamp"], phrases[1]["query"]["bool"]["filter"][0]["range"]["@timestamp"]
    assert before["lt"] == during["gte"] and during["gte"] - before["gte"] == 30 * 60 * 1000
    assert phrases[0]["query"]["bool"]["filter"][3] == phrases[1]["query"]["bool"]["filter"][3]
    prompt = rca.build_prompt(ev)
    assert "【3d. 主要的錯誤種類與事故前後的次數" in prompt and "事故前 30 分鐘" in prompt


def test_a_phrase_that_matches_nothing_gets_no_counts_instead_of_a_wrong_zero(fake):
    original = fake.msearch

    def zero_counts(body):
        response = original(body)
        if "match_phrase" in body:
            data = response.json()
            for r in data["responses"]:
                r["hits"]["total"]["value"] = 0
            return httpx.Response(200, json=data)
        return response

    fake.msearch = zero_counts
    ev = collect(fake)
    assert ev["error_patterns"] and all("during" not in r and "new" not in r for r in ev["error_patterns"])
    assert "事故前後的次數比較" not in rca.overview_text(rca.overview(ev))


def test_comparison_can_be_turned_off_and_baseline_length_is_configurable(fake):
    off = collect(fake, limits=rca.EvidenceLimits(max_log_patterns=0, max_warn_patterns=0))
    assert off["error_patterns"] == [] and off["warnings"] == [] and fake.phrase_requests == []
    assert len(off["error_logs"]) == 4 and fake.profile_requests       # 去重與各系統最早的樣本不受影響
    collect(fake, limits=rca.EvidenceLimits(baseline_minutes=120))
    window = [q["query"]["bool"]["filter"][0]["range"]["@timestamp"] for q in fake.phrase_requests[-1][:2]]
    assert window[0]["gte"] - window[1]["gte"] == 120 * 60 * 1000


# ── 警告 ──

def warning(seconds, service, message):
    return {"@timestamp": (T0 - timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ"), "service": {"name": service},
            "log": {"level": "WARN"}, "message": message}


def test_warnings_are_grouped_counted_and_shown_separately_from_errors(fake):
    fake.warning_docs = [warning(120, "mq-broker", "disk usage 91% on /data"), warning(90, "mq-broker", "disk usage 95% on /data"),
                         warning(60, "cim-eap", "publish retry 3 of 5")]
    ev = collect(fake)
    assert [(w["system"], w["pattern"]) for w in ev["warnings"]] == [("mq-broker", "disk usage *% on /data"), ("cim-eap", "publish retry * of *")]
    assert ev["warnings"][0]["example"] == "disk usage 91% on /data"      # 原文、最早的那一筆
    assert "【3e. 警告" in rca.build_prompt(ev) and "警告 2 種" in rca.overview_text(rca.overview(ev))
    assert all("WARN" not in str(e.get("level", "")) for e in ev["error_logs"])


def test_no_warnings_are_asked_for_without_a_level_field_or_when_turned_off(fake):
    collect(fake, log_fields=LogFields(level=""))
    assert len(fake.profile_requests[-1]) == 3                            # 只有三個系統各一個子查詢，沒有警告那一個
    collect(fake, limits=rca.EvidenceLimits(warn_levels=()))
    assert len(fake.profile_requests[-1]) == 3


# ── 失敗不擋分析 ──

def test_if_the_extra_queries_fail_the_original_samples_are_kept_and_it_is_noted(fake):
    original = fake.msearch
    fake.msearch = lambda body: httpx.Response(500, json={"message": "boom"}) if '"track_total_hits"' in body else original(body)
    ev = collect(fake)
    assert len(ev["error_logs"]) == 4 and ev["error_patterns"] == []
    assert any("錯誤概況查詢失敗" in n for n in ev["notes"])


def test_no_extra_queries_when_there_are_no_error_logs(fake):
    for resp in fake.es["responses"]:
        resp["hits"] = {"total": {"value": 0, "relation": "eq"}, "max_score": None, "hits": []}
    ev = collect(fake)
    assert fake.profile_requests == [] and fake.phrase_requests == [] and ev["error_patterns"] == []


# ── 設定 ──

def load(tmp_path, text):
    path = tmp_path / "config.yaml"
    path.write_text(MINIMAL + text, encoding="utf-8")
    return Settings.load(str(path))


def test_insight_settings_are_read_from_config(tmp_path):
    s = load(tmp_path, "es: {baseline_minutes: 90, warn_levels: [W, WARN]}\nllm: {max_log_patterns: 3, max_warn_patterns: 0}\n")
    assert (s.evidence.baseline_minutes, s.evidence.warn_levels, s.evidence.max_log_patterns, s.evidence.max_warn_patterns) == (90, ("W", "WARN"), 3, 0)
    d = load(tmp_path, "").evidence
    assert (d.baseline_minutes, d.max_log_patterns, d.max_warn_patterns) == (30, 6, 5) and "WARN" in d.warn_levels
    assert load(tmp_path, "es: {warn_levels: []}\n").evidence.warn_levels == ()   # 空清單 = 不撈警告


@pytest.mark.parametrize("text, message", [
    ("es: {baseline_minutes: 0}\n", "baseline_minutes"), ("llm: {max_log_patterns: 99}\n", "max_log_patterns"),
    ("es: {warn_levels: WARN}\n", "warn_levels"), ("es: {warn_levels: [1]}\n", "warn_levels"),
])
def test_bad_insight_settings_are_rejected_at_startup(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load(tmp_path, text)
