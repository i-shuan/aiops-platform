"""錯誤日誌的前處理：同類去重、各系統最早的錯誤不被擠掉、事故前後的次數、警告。
都是歸類與算術，沒有「算不算異常」的門檻。假的 Grafana 拿錄下來的真實樣本（victoria_logs.json）照查詢條件篩；
真的 VictoriaLogs 上的行為由 eval/run.py 的案例驗證（結果記在 README.md 第 12 節）。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import evidence as gather
from src import rca
from src.grafana import Grafana
from src.logs import LogFields, VictoriaLogs
from src.grouping import Alert, converge
from src.main import ConfigError, Settings
from tests.conftest import collect_evidence, SHIFT, log_row, recorded_rows, shifted

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
    return collect_evidence(inc or incident(), EDGES, grafana, logs_datasource_uid="vl",
                                id_fields=["eqp_id", "lot_id", "carrier_id"], now=NOW, **kw)


PROFILE, COUNTS = 'partition by ("service.name")', "stats count() if"   # 各系統最早的錯誤、事故前後的次數：各是一條查詢


# ── 樣板 ──

@pytest.mark.parametrize("message, pattern, words", [
    ("heartbeat to MES retry failed attempt=312", "heartbeat to MES retry failed attempt=*", ["heartbeat", "to", "MES", "retry", "failed"]),
    ("request 17 failed", "request * failed", ["request", "failed"]),
    ("PLC connection lost on STK01 (10.3.2.15:502), transfers halted", "PLC connection lost on STK* (*), transfers halted",
     ["PLC", "connection", "lost", "on", "transfers", "halted"]),
    ("publish to eap.events timed out after 5000ms", "publish to eap.events timed out after *ms",
     ["publish", "to", "eap.events", "timed", "out", "after"]),
    ("id=3f2a9c1e-11aa-4b2c-9d3e-aabbccddeeff hash deadbeef12 ok", "id=* hash * ok", ["hash", "ok"]),
    ("timeout", "timeout", None),                       # 只有一個詞：不數（會把不相干的訊息都算進來）
    ("code 500 at 10.0.0.1:8080", "code * at *", ["code", "at"]),
    ("err 42", "err *", None),
])
def test_messages_become_patterns_and_countable_words(message, pattern, words):
    assert gather.log_pattern(message) == pattern
    assert gather.pattern_words(pattern) == words


def test_same_kind_of_error_with_different_numbers_is_one_pattern_but_words_are_not_touched():
    a, b = gather.log_pattern("T3 timeout eqp=EQP-CVD-07 after 5000ms"), gather.log_pattern("T3 timeout eqp=EQP-CVD-12 after 30ms")
    assert a == b == "T* timeout eqp=EQP-CVD-* after *ms"
    assert gather.log_pattern("decode failed") == "decode failed" and gather.log_pattern("cafe  bad\tface") == "cafe bad face"


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

def test_each_system_gets_its_own_oldest_errors_in_one_query(fake):
    collect(fake)
    [profile] = fake.queries(PROFILE)
    assert profile == ('"service.name":in("amhs-mcs","cim-eap","mq-broker") "log.level":in("ERROR","error","Error","FATAL","fatal",'
                       f'"CRITICAL","critical") | sort by (_time) partition by ("service.name") limit {gather.FIRST_ERRORS_PER_SYSTEM}')
    # 警告不再另外問：跟錯誤同一次查詢撈（最新的在前），撈回來再照等級分開
    assert fake.queries('"log.level":in("WARN"') == []
    [both] = fake.queries("sort by (_time) desc")
    assert '"log.level":in("ERROR","error","Error","FATAL","fatal","CRITICAL","critical",\"WARN\",\"warn\",\"Warn\",\"WARNING\",\"warning\",\"Warning\")' in both


def test_a_system_flooded_out_of_the_newest_sample_still_shows_its_earliest_errors(fake):
    """最新的那一批樣本全是 mq-broker（它在大量報錯）；cim-eap、amhs-mcs 較早的錯誤靠各系統各取最早幾筆的那一次補回來。"""
    fake.logs += [log_row(T0 + timedelta(seconds=n), "mq-broker", "ERROR", f"publish rejected: queue full ({n})") for n in range(8)]
    limits = gather.EvidenceLimits(max_hits=8)
    newest = VictoriaLogs(Grafana("http://grafana.test", "t", transport=fake.transport), "vl").search_errors(
        incident().services, T0 - timedelta(minutes=10), NOW, 8)
    assert {row["service.name"] for row in newest} == {"mq-broker"}      # 只看最新的 8 筆的話，另外兩個系統完全不見
    ev = collect(fake, limits=limits)
    assert {e["system"] for e in ev["error_logs"]} == {"mq-broker", "cim-eap", "amhs-mcs"}
    assert {r["system"] for r in ev["error_patterns"]} == {"mq-broker", "cim-eap", "amhs-mcs"}   # 每個系統最早的那一種都有比較


# ── 事故前後的次數 ──

def test_patterns_are_counted_during_and_before_and_new_is_a_fact_not_a_threshold(fake):
    start = T0 - timedelta(minutes=10)                   # 事故時間窗的起點
    fake.logs += [log_row(start - timedelta(seconds=30 * n + 5), "cim-eap", "ERROR", f"T3 timeout waiting S{n % 9}F12 reply")
                  for n in range(40)]                    # 事故前 30 分鐘裡，同一種錯誤本來就有 40 筆
    fake.logs.append(log_row(start - timedelta(minutes=31), "cim-eap", "ERROR", "T3 timeout waiting S6F12 reply"))   # 更早的不算
    ev = collect(fake)
    by_example = {r["example"]: r for r in ev["error_patterns"]}
    old = by_example["T3 timeout waiting S6F12 reply"]
    assert (old["during"], old["before"], old["new"]) == (3, 40, False)
    fresh = [r for r in ev["error_patterns"] if r.get("new")]
    assert fresh and all(r["before"] == 0 for r in fresh)
    assert ev["count_windows"]["before_minutes"] == 30
    # 計數：一條查詢、一次請求。查詢的時間範圍從事故前 30 分鐘開始，每一種錯誤用時間窗的起點切成「事故期間」與「事故前」
    [counts] = [q for q in fake.log_queries if COUNTS in q["query"]]
    assert counts["start"] == "2026-11-21T02:01:10.000000Z"
    words = '"service.name":="cim-eap" "log.level":in("ERROR","error","Error","FATAL","fatal","CRITICAL","critical") _msg:seq("timeout","waiting","reply")'
    assert f"count() if ({words} _time:>=2026-11-21T02:31:10.000000Z) as d" in counts["query"]   # 樣板是 T* … S*F*，帶 * 的詞不進條件
    assert f"count() if ({words} _time:<2026-11-21T02:31:10.000000Z) as b" in counts["query"]
    prompt = rca.build_prompt(ev)
    assert "【3d. 主要的錯誤種類與事故前後的次數" in prompt and "事故前 30 分鐘" in prompt


def test_a_pattern_that_counts_nothing_gets_no_counts_instead_of_a_wrong_zero(fake):
    """計數的條件是近似的（固定的詞照順序出現）。萬一它連樣本自己那一筆都比不到，就不給次數。"""
    original = fake.logsql

    def zero_counts(request):
        response = original(request)
        if COUNTS in fake.log_queries[-1]["query"]:
            return httpx.Response(200, text=json.dumps({key: "0" for key in json.loads(response.text)}) + "\n")
        return response

    fake.logsql = zero_counts
    ev = collect(fake)
    assert ev["error_patterns"] and all("during" not in r and "new" not in r for r in ev["error_patterns"])
    assert "事故前後的次數比較" not in gather.overview_text(gather.overview(ev))


def test_comparison_can_be_turned_off_and_baseline_length_is_configurable(fake):
    off = collect(fake, limits=gather.EvidenceLimits(max_log_patterns=0, max_warn_patterns=0))
    assert off["error_patterns"] == [] and off["warnings"] == [] and fake.queries(COUNTS) == []
    assert len(off["error_logs"]) == 4 and fake.queries(PROFILE)       # 去重與各系統最早的樣本不受影響
    collect(fake, limits=gather.EvidenceLimits(baseline_minutes=120))
    [counts] = [q for q in fake.log_queries if COUNTS in q["query"]]
    assert counts["start"] == "2026-11-21T00:31:10.000000Z"            # 事故時間窗的起點再往前 120 分鐘


# ── 警告 ──

def warning(seconds, service, message):
    return log_row(T0 - timedelta(seconds=seconds), service, "WARN", message)


def test_warnings_are_grouped_counted_and_shown_separately_from_errors(fake):
    fake.logs += [warning(120, "mq-broker", "disk usage 91% on /data"), warning(90, "mq-broker", "disk usage 95% on /data"),
                  warning(60, "cim-eap", "publish retry 3 of 5"), warning(30, "erp", "unrelated system")]
    ev = collect(fake)
    assert [(w["system"], w["pattern"]) for w in ev["warnings"]] == [("mq-broker", "disk usage *% on /data"), ("cim-eap", "publish retry * of *")]
    assert ev["warnings"][0]["example"] == "disk usage 91% on /data"      # 原文、最早的那一筆
    assert (ev["warnings"][0]["during"], ev["warnings"][0]["before"], ev["warnings"][0]["new"]) == (2, 0, True)
    assert "【3e. 警告" in rca.build_prompt(ev) and "警告 2 種" in gather.overview_text(gather.overview(ev))
    assert all("WARN" not in str(e.get("level", "")) for e in ev["error_logs"])


def test_no_warnings_are_asked_for_without_a_level_field_or_when_turned_off(fake):
    asked_for_warnings = lambda: [q for q in fake.log_queries if "warn" in q["query"].lower()]
    collect(fake)
    assert len(asked_for_warnings()) == 1                                 # 對照：預設會問一次
    fake.log_queries.clear()
    collect(fake, limits=gather.EvidenceLimits(warn_levels=()))
    assert fake.queries(PROFILE) and asked_for_warnings() == []
    fake.log_queries.clear()
    fake.logs = shifted(recorded_rows("unparsed"), SHIFT)                 # 沒有等級欄位：分不出警告，不問
    ev = collect(fake, log_fields=LogFields(service="kubernetes.container_name", level=""))
    assert ev["error_logs"] and fake.queries('partition by ("kubernetes.container_name")') and asked_for_warnings() == []


# ── 失敗不擋分析 ──

def test_if_the_extra_queries_fail_the_original_samples_are_kept_and_it_is_noted(fake):
    fake.fail_queries = {PROFILE}
    ev = collect(fake)
    assert len(ev["error_logs"]) == 4 and ev["error_patterns"] == [] and fake.queries(COUNTS) == []
    assert any("錯誤概況查詢失敗" in n for n in ev["notes"])
    fake.fail_queries = {COUNTS}
    ev = collect(fake)
    assert len(ev["error_logs"]) == 4 and all("during" not in r for r in ev["error_patterns"])
    assert any("事故前後的次數比較查詢失敗" in n for n in ev["notes"])


def test_without_error_logs_the_warnings_are_still_collected(fake):
    """延遲類的事故常常只有警告：沒有錯誤日誌也要撈警告（之前是有錯誤才撈，只有警告的事故 LLM 什麼日誌都看不到）。"""
    fake.logs = [row for row in fake.logs if row["log.level"] != "ERROR"]   # 錄下來的這批沒有警告：加兩筆
    fake.logs += [log_row(T0 - timedelta(seconds=40), "cim-eap", "WARN", "slow reply from EQP-CVD-07: 4800ms (limit 5000ms)"),
                  log_row(T0 - timedelta(seconds=20), "cim-eap", "WARN", "slow reply from EQP-CVD-07: 4950ms (limit 5000ms)")]
    ev = collect(fake)
    # 警告是跟錯誤同一次查詢撈回來的；沒有錯誤就不問「各系統最早的錯誤」
    assert ev["error_logs"] == [] and ev["error_patterns"] == [] and fake.queries(PROFILE) == []
    [warning] = ev["warnings"]                                          # 兩筆同一種：歸成一類
    assert (warning["system"], warning["example"]) == ("cim-eap", "slow reply from EQP-CVD-07: 4800ms (limit 5000ms)")
    assert f"警告 {len(ev['warnings'])} 種" in gather.overview_text(gather.overview(ev))


def test_no_extra_queries_when_these_systems_have_no_logs_at_all(fake):
    fake.logs = []
    ev = collect(fake)
    assert fake.queries(PROFILE) == [] and fake.queries(COUNTS) == [] and ev["error_patterns"] == [] and ev["warnings"] == []


# ── 設定 ──

def load(tmp_path, text):
    path = tmp_path / "config.yaml"
    path.write_text(MINIMAL + text, encoding="utf-8")
    return Settings.load(str(path))


def test_insight_settings_are_read_from_config(tmp_path):
    s = load(tmp_path, "logs: {baseline_minutes: 90, warn_levels: [W, WARN]}\nllm: {max_log_patterns: 3, max_warn_patterns: 0}\n")
    assert (s.evidence.baseline_minutes, s.evidence.warn_levels, s.evidence.max_log_patterns, s.evidence.max_warn_patterns) == (90, ("W", "WARN"), 3, 0)
    d = load(tmp_path, "").evidence
    assert (d.baseline_minutes, d.max_log_patterns, d.max_warn_patterns) == (30, 6, 5) and "WARN" in d.warn_levels
    assert load(tmp_path, "logs: {warn_levels: []}\n").evidence.warn_levels == ()   # 空清單 = 不撈警告


@pytest.mark.parametrize("text, message", [
    ("logs: {baseline_minutes: 0}\n", "baseline_minutes"), ("llm: {max_log_patterns: 99}\n", "max_log_patterns"),
    ("logs: {warn_levels: WARN}\n", "warn_levels"), ("logs: {warn_levels: [1]}\n", "warn_levels"),
])
def test_bad_insight_settings_are_rejected_at_startup(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load(tmp_path, text)


# ── 時間窗內沒有錯誤的系統：往回找最後一筆錯誤（只給時間差與原文）──

def test_a_system_whose_errors_stopped_before_the_window_gets_its_last_error_and_how_long_ago(fake):
    """錯誤在告警之前就停了（感測器逾時之後才開始偏移）：時間窗內 0 筆，但不是「沒有送日誌」，也不能當成沒事。"""
    start = T0 - timedelta(minutes=10)                                   # 事故時間窗的起點（預設往前 10 分鐘）
    fake.logs = [log_row(start - timedelta(minutes=m), "amhs-mcs", "ERROR", f"thermocouple TC-4 read timeout ({m})") for m in (33, 40, 55)]
    fake.logs.append(log_row(start - timedelta(hours=30), "cim-eap", "ERROR", "older than the lookback"))
    ev = collect(fake)
    assert ev["error_logs"] == []
    by_system = {r["system"]: r for r in ev["last_errors"]}
    assert by_system["amhs-mcs"]["minutes_before_window"] == 33 and by_system["amhs-mcs"]["message"] == "thermocouple TC-4 read timeout (33)"
    assert by_system["cim-eap"] == {"system": "cim-eap", "none_in_hours": gather.LAST_ERROR_LOOKBACK_HOURS}   # 30 小時前的不算，照實寫沒有
    assert by_system["mq-broker"] == {"system": "mq-broker", "none_in_hours": 24}
    text = gather.overview_text(gather.overview(ev))
    assert "amhs-mcs 最後一筆錯誤在時間窗之前 33 分鐘" in text and "cim-eap 之前 24 小時內也沒有錯誤" in text
    prompt = rca.build_prompt(ev, text)
    assert "【3p. 時間窗內沒有錯誤日誌的系統，往回找到的最後一筆錯誤" in prompt and "thermocouple TC-4 read timeout (33)" in prompt
    assert "none_in_hours" not in prompt                                  # 往回也沒有的只寫在證據概況那一行，提示詞不另外佔一段
    fake.logs = []
    nothing = collect(fake)
    assert all("none_in_hours" in r for r in nothing["last_errors"]) and "【3p." not in rca.build_prompt(nothing, gather.overview_text(gather.overview(nothing)))
    assert gather.evidence_grade("amhs-mcs", ev) == "inference"         # 時間窗之前的錯誤不拉高依據等級：只是給人與模型參考


def test_only_the_systems_without_errors_in_the_window_are_looked_up_and_a_failure_is_only_noted(fake):
    ev = collect(fake)                                                    # 錄下來的樣本：三個系統在時間窗內都有錯誤
    assert "last_errors" not in ev and not [q for q in fake.queries("sort by (_time) desc") if q.endswith("limit 1")]
    fake.logs = [row for row in fake.logs if row["service.name"] != "amhs-mcs"]
    ev = collect(fake)
    assert [r["system"] for r in ev["last_errors"]] == ["amhs-mcs"]       # 只查沒有錯誤的那一個
    assert len([q for q in fake.queries("sort by (_time) desc") if q.endswith("limit 1")]) == 1
    fake.fail_queries = {'in("amhs-mcs") "log.level"'}                      # 只有「最後一筆錯誤」那一條只查 amhs-mcs 一個系統
    ev = collect(fake)
    assert "last_errors" not in ev and ev["error_logs"] and any("找不到 amhs-mcs 在時間窗之前最後一筆錯誤" in n for n in ev["notes"])


# ── 同類去重之後：會變的數字附上樣本裡的範圍 ──

@pytest.mark.parametrize("messages, expected", [
    (["statement timeout after 30s, active sessions 0", "statement timeout after 30s, active sessions 45", "statement timeout after 30s, active sessions 12"],
     [{"in_example": "0", "min": 0, "max": 45}]),                                                  # 沒變的 30 不列
    (["took 4.5ms attempt=1", "took 120.25ms attempt=3"], [{"in_example": "4.5", "min": 4.5, "max": 120.25}, {"in_example": "1", "min": 1, "max": 3}]),
    (["EQP-CVD-07 lot LOT24A0031 failed", "EQP-CVD-09 lot LOT24A0060 failed"], []),                # 機台、批號裡的數字不算
    (["host 10.3.2.15 down", "host 10.3.2.16 down"], []),                                          # 位址後面幾段也不算（前面是點）
    (["2026-11-28T13:58:59.000Z | ERROR | w-1 | disk usage 90% on /data", "2026-11-28T14:08:09.120Z | ERROR | w-1 | disk usage 97% on /data"],
     [{"in_example": "90", "min": 90, "max": 97}]),                                                # 訊息裡的時間戳不算（沒拆欄位的日誌）
    (["at 14:08:09 retry 1", "at 14:09:10 retry 4"], [{"in_example": "1", "min": 1, "max": 4}]),
    (["only one sample 5"], []), ([], []),
    (["a 1 b 2", "a 1"], []),                                                                      # 數字個數不一樣：對不起來，不給
])
def test_number_ranges_cover_the_standalone_numbers_that_vary_within_a_kind(messages, expected):
    assert gather.number_ranges(messages) == expected


def test_deduplicated_errors_carry_the_range_so_the_first_sample_is_not_taken_as_a_fact(fake):
    fake.logs += [log_row(T0 - timedelta(seconds=s), "mq-broker", "ERROR", f"statement timeout after 30s, active sessions {n}")
                  for s, n in ((90, 0), (60, 45), (30, 12))]
    ev = collect(fake)
    [kept] = [e for e in ev["error_logs"] if e["message"].startswith("statement timeout")]
    assert kept["message"].endswith("active sessions 0") and kept["similar_in_sample"] == 3       # 原文還是最早的那一筆
    [row] = [r for r in ev["error_patterns"] if r["example"].startswith("statement timeout")]
    assert row["numbers_in_sample"] == [{"in_example": "0", "min": 0, "max": 45}]
    assert all("numbers_in_sample" not in e for e in ev["error_logs"])     # 只放在錯誤種類那一包，不重複放兩處
    assert "numbers_in_sample＝example 裡會變的數字在同類樣本中的範圍" in rca.build_prompt(ev)
