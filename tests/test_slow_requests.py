"""延遲類的事故：沒有錯誤日誌、沒有失敗的請求，只有警告與變慢的請求。三件事一起驗：
  1. 沒有錯誤日誌也照樣撈警告
  2. 沒有失敗的 Trace 時，改取搜尋到的請求裡耗時最長的幾條（只是排序，沒有門檻）
  3. 呼叫鏈 → 日誌：用 trace id 撈同一個請求的日誌，不限等級
樣本是 record_slow_requests.py 從真的元件錄的（slow_requests.json）：同一批資料送進 VictoriaLogs＋VictoriaTraces 與
Elasticsearch＋Tempo，經 Grafana 13.0.7 的資料源代理查回來。測試照順序重播，並檢查程式送出的請求跟錄的時候一模一樣。

已知限制（沒有粉飾）：
  - 兩種 Trace 資料庫的搜尋都不能照耗時排序，「最慢」只是搜尋回來那一批（預設 20 條）裡最慢的。
  - 日誌要有記 trace id，而且寫法跟 Trace 資料庫的編號一樣；沒有的話第 3 件事什麼都撈不到，也不會提。
  - 兩種日誌資料庫回的時間寫法不同（VictoriaLogs 去掉小數秒尾端的 0），送給 LLM 的字因此不完全一樣；內容相同。
"""
from __future__ import annotations

import json
import pathlib
import re
from datetime import datetime, timedelta

import httpx

from src import rca
from src.grafana import Grafana
from src.grouping import Alert, converge
from src.logs import Elasticsearch, LogFields, VictoriaLogs
from src.traces import Tempo, VictoriaTraces
from tests.conftest import collect_evidence, log_row
from tests.test_correlation import NOW, collect, incident, trace_copy

FIX = json.loads((pathlib.Path(__file__).parent / "fixtures/grafana13/slow_requests.json").read_text(encoding="utf-8"))
BASE = datetime.fromisoformat(FIX["base"])
WINDOW = (BASE - timedelta(minutes=10), BASE)
SLOW, MEDIUM, FAST = (FIX["trace_ids"][k] for k in ("slow", "medium", "fast"))
SLOW_QUERY = "slow query 4180ms: SELECT * FROM lot_history WHERE carrier_id=? (no index used, rows examined 2400000)"


def replaying(name: str) -> Grafana:
    """照順序回錄下來的真實回應，並檢查這一次送出的請求（路徑、參數、內容）跟錄的時候一樣。"""
    queue = list(FIX["exchanges"][name])

    def handler(request: httpx.Request) -> httpx.Response:
        reply = queue.pop(0)
        want = reply["request"]
        assert (request.method, request.url.path, dict(request.url.params), request.content.decode()) == \
               (want["method"], want["path"].replace("<", "").replace(">", ""), want["params"], want["body"])
        return httpx.Response(reply["status"], text=reply["response"], headers={"Content-Type": "application/json"})
    return Grafana("http://grafana.test", "t", transport=httpx.MockTransport(handler), retries=0)


vlogs = lambda g: VictoriaLogs(g, "vl", LogFields(), f'{{dataset="{FIX["dataset"]}"}}')
eslogs = lambda g: Elasticsearch(g, "es", LogFields(), FIX["index"])


def slow_incident():
    alert = Alert("slow-1", "firing", "rule-slow", "Orders API latency p95", "orders-api", "P2", BASE - timedelta(seconds=60), None, {"A": 4.2})
    [inc] = converge([], [alert], [], [], now=BASE, close_after_seconds=300)
    return inc


def evidence_from(stack: str) -> dict:
    g = replaying(f"collect_evidence_{stack}")
    logs, traces = (vlogs(g), VictoriaTraces(g, "vt")) if stack == "victoria" else (eslogs(g), Tempo(g, "tempo"))
    return rca.collect_evidence(slow_incident(), [], g, logs=logs, traces=traces, id_fields=["eqp_id", "lot_id", "carrier_id"], now=BASE)


# ── 不限失敗的搜尋：兩種 Trace 資料庫 ──

def test_searching_without_the_error_filter_returns_durations_from_both_trace_stores():
    for name, store in (("vt", VictoriaTraces(replaying("vt_search_traces"), "vt")), ("tempo", Tempo(replaying("tempo_search_traces"), "tempo"))):
        found = {t["id"]: t["duration"] for t in store.search_traces(["orders-api"], *WINDOW, 20)}
        assert {i: found[i] for i in (SLOW, MEDIUM, FAST)} == {SLOW: 4_200_000, MEDIUM: 300_000, FAST: 90_000}, name   # 微秒
    assert VictoriaTraces(replaying("vt_search_error_traces"), "vt").search_error_traces(["orders-api"], *WINDOW, 20) == []
    assert Tempo(replaying("tempo_search_error_traces"), "tempo").search_error_traces(["orders-api"], *WINDOW, 20) == []


# ── 用 trace id 查日誌：兩種日誌資料庫 ──

def test_logs_of_a_request_are_found_by_trace_id_at_any_level_in_both_log_stores():
    wanted = [SLOW, FAST, "ff" * 16]
    from_vl = vlogs(replaying("vl_logs_of_traces")).search_timeline("trace_id", wanted, *WINDOW, 10)
    from_es = eslogs(replaying("es_logs_of_traces")).search_timeline("trace_id", wanted, *WINDOW, 10)
    shape = lambda got: {k: ([(d["service.name"], d["log.level"], d["_msg"]) for d in v["hits"]], v["total"]) for k, v in got.items()}
    assert shape(from_vl) == shape(from_es)
    events, total = shape(from_vl)[SLOW]
    assert total == 4 and ("orders-api", "WARN", SLOW_QUERY) in events and ("orders-db", "INFO", "seq scan on lot_history finished, 2400000 rows") in events
    assert shape(from_vl)[FAST][1] == 2 and shape(from_vl)["ff" * 16] == ([], 0)
    assert eslogs(replaying("es_logs_of_unknown_trace")).search_timeline("trace_id", ["11" * 16], *WINDOW, 10) == {"11" * 16: {"hits": [], "total": 0}}


# ── 整包證據：兩組來源一樣 ──

def test_a_latency_incident_gets_warnings_slowest_requests_and_their_logs_from_either_stack():
    a, b = evidence_from("victoria"), evidence_from("es_tempo")
    # 時間的寫法不一樣：VictoriaLogs 把小數秒尾端的 0 去掉（.180Z → .18Z、.000Z → Z），Elasticsearch 回原文。
    # 是同一個時間，只是送給 LLM 的字不同；比較前把兩邊都寫成去掉尾端 0 的樣子
    trim = lambda text: re.sub(r"\.(\d*?)0*Z", lambda m: f".{m.group(1)}Z" if m.group(1) else "Z", text)
    same = lambda ev: json.loads(trim(json.dumps({k: v for k, v in ev.items() if k not in ("log_source", "trace_source")},
                                                 ensure_ascii=False)))
    assert same(a) == same(b)
    assert (a["log_source"]["name"], a["trace_source"], b["log_source"]["name"], b["trace_source"]) == ("VictoriaLogs", "VictoriaTraces", "Elasticsearch", "Tempo")
    assert a["error_logs"] == [] and a["sources"] == {"logs": "empty", "call_chains": "ok", "metrics": "off"}
    # 1. 沒有錯誤日誌，警告照樣在
    assert sorted(w["example"] for w in a["warnings"]) == ["connection pool usage 70% (14/20 active)", SLOW_QUERY]
    # 2. 沒有失敗的請求：取耗時最長的，由長到短，而且標明只是樣本
    assert [c["sample"] for c in a["call_chains"]] == ["slowest"] * 3
    took = [c["spans"][0]["duration_ms"] for c in a["call_chains"]]
    assert took == sorted(took, reverse=True) and took[0] == 4200 and a["call_chains"][0]["trace_id"] == SLOW
    assert a["call_chains"][0]["systems"] == ["orders-api", "orders-db"] and all(sp["status"] == "ok" for c in a["call_chains"] for sp in c["spans"])
    assert any("沒有失敗的 Trace" in n and "只是樣本" in n for n in a["notes"])
    # 3. 呼叫鏈帶出同一個請求的日誌：警告與 INFO 都在，包含沒有告警的 orders-db
    [of_slow] = [t for t in a["trace_logs"] if t["trace_id"] == SLOW]
    assert of_slow["systems"] == ["orders-api", "orders-db"] and not of_slow["truncated"]
    assert [(e["system"], e["level"]) for e in of_slow["events"]] == [("orders-api", "INFO"), ("orders-db", "INFO"), ("orders-api", "WARN"), ("orders-api", "INFO")]
    assert of_slow["events"][2]["message"] == SLOW_QUERY


def test_the_note_and_the_prompt_say_what_these_chains_are_and_the_database_counts_as_evidence():
    ev = evidence_from("victoria")
    view = rca.overview(ev)
    text = rca.overview_text(view)
    assert "警告 2 種" in text and "沒有失敗的請求，改列耗時最長的請求的呼叫鏈 3 條" in text and "失敗請求的呼叫鏈" not in text
    assert f"呼叫鏈對應的日誌 {sum(len(t['events']) for t in ev['trace_logs'])} 筆" in text
    assert "缺錯誤日誌｜" in text and "log.level 沒有一筆是 ERROR／FATAL／CRITICAL" in text   # 缺的照實寫，不因為有警告就不提
    prompt = rca.build_prompt(ev, text)
    assert "這段時間沒有失敗的請求，這是搜尋到的請求裡耗時最長的幾條（sample=slowest；只是樣本）" in prompt
    assert "【6b. 請求的日誌" in prompt and SLOW_QUERY in prompt and "【3e. 警告" in prompt
    # orders-db 沒有告警、不在關係邊：它在呼叫鏈與請求的日誌裡，是直接證據，分數不壓到 30
    answer = json.dumps({"summary": "orders-api 的 lot_history 查詢沒有用到索引，請求變慢", "systems": ["orders-api", "orders-db"], "ids": [],
                         "candidates": [{"source": "orders-db", "kind": "evidence", "confidence": 72, "reason": "慢查詢全表掃描"}]})
    _summary, [candidate] = rca.read_answer(answer, ev)
    assert (candidate["source"], candidate["confidence"]) == ("orders-db", 72)


# ── 不該多做的時候不多做（假的 Grafana 照查詢條件篩資料）──

def test_failed_requests_still_come_first_and_no_second_search_is_made(fake):
    ev = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid")
    assert ev["call_chains"] and all("sample" not in c for c in ev["call_chains"])
    searches = [r for r in fake.trace_requests if r.url.path.endswith("/api/traces")]
    assert searches and all("tags" in r.url.params for r in searches)            # 只做了限失敗的搜尋
    assert "失敗請求的呼叫鏈" in rca.overview_text(rca.overview(ev))


def test_only_ok_traces_means_the_slowest_of_them_are_listed(fake):
    ok = fake.traces["33" * 16]                                                   # 錄下來的那條成功的 Trace
    fake.traces = {"33" * 16: ok}
    ev = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid")
    assert [(c["trace_id"], c["sample"]) for c in ev["call_chains"]] == [("33" * 16, "slowest")]
    fake.traces = {}
    ev = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid")
    assert ev["call_chains"] == [] and any("沒有這些系統的 Trace" in n for n in ev["notes"])
    assert "裡沒有這些系統的請求（失敗的與一般的都沒有）" in rca.overview_text(rca.overview(ev))


def test_logs_carrying_the_trace_id_are_attached_and_a_failed_lookup_is_only_noted(fake):
    trace_id = "11" * 16
    fake.logs += [log_row(NOW - timedelta(seconds=30), "mes-api", "INFO", "track-in accepted, calling mes-db", trace_id=trace_id),
                  log_row(NOW - timedelta(seconds=29), "mes-db", "WARN", "too many connections (max 200)", trace_id=trace_id)]
    ev = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid")
    [logs_of] = [t for t in ev["trace_logs"] if t["trace_id"] == trace_id]
    assert [(e["system"], e["level"], e["message"]) for e in logs_of["events"]] == [
        ("mes-api", "INFO", "track-in accepted, calling mes-db"), ("mes-db", "WARN", "too many connections (max 200)")]
    assert logs_of["systems"] == ["mes-api", "mes-db"]
    # 關掉欄位：不查。日誌裡沒有這個欄位：沒有這一包，也不寫成「缺」
    asked = len(fake.queries('"trace_id"'))
    off = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid", limits=rca.EvidenceLimits(trace_id_field=""))
    assert "trace_logs" not in off and len(fake.queries('"trace_id"')) == asked
    fake.logs = [row for row in fake.logs if "trace_id" not in row]
    none = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid")
    assert "trace_logs" not in none and "呼叫鏈對應的日誌" not in rca.overview_text(rca.overview(none))
    fake.fail_queries = {'"trace_id"'}
    failed = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid")
    assert failed["call_chains"] and "trace_logs" not in failed and any("呼叫鏈對應的日誌撈取失敗" in n for n in failed["notes"])
