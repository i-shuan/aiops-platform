"""延遲類的事故：沒有錯誤日誌、沒有失敗的請求，只有警告。驗三件事：
  1. 錯誤與警告是同一次查詢撈回來的：沒有錯誤日誌，警告照樣在
  2. 沒有失敗的 Trace 就沒有呼叫鏈：不會改挑「最慢的幾條」充數（2026-11-27 拿掉：沒有對照組的單一樣本會把偶發的慢當成線索；
     延遲看 service graph 的統計，見 test_service_graph_latency.py）
  3. 呼叫鏈 → 日誌：用 trace id 撈同一個請求的日誌，不限等級（INFO 只在這時撈）
樣本是 record_slow_requests.py 從真的元件錄的（slow_requests.json）：同一批資料送進 VictoriaLogs＋VictoriaTraces 與
Elasticsearch＋Tempo，經 Grafana 13.0.7 的資料源代理查回來。測試照順序重播，並檢查程式送出的請求跟錄的時候一模一樣。

已知限制（沒有粉飾）：
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
from tests.conftest import log_row
from tests.test_correlation import NOW, collect, incident

FIX = json.loads((pathlib.Path(__file__).parent / "fixtures/grafana13/slow_requests.json").read_text(encoding="utf-8"))
BASE = datetime.fromisoformat(FIX["base"])
WINDOW = (BASE - timedelta(minutes=10), BASE)
SLOW, FAST = FIX["trace_ids"]["slow"], FIX["trace_ids"]["fast"]
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
# 時間的寫法：VictoriaLogs 把小數秒尾端的 0 去掉（.180Z → .18Z、.000Z → Z），Elasticsearch 回原文。比較前統一
trim = lambda text: re.sub(r"\.(\d*?)0*Z", lambda m: f".{m.group(1)}Z" if m.group(1) else "Z", text)


def slow_incident():
    alert = Alert("slow-1", "firing", "rule-slow", "Orders API latency p95", "orders-api", "P2", BASE - timedelta(seconds=60), None, {"A": 4.2})
    [inc] = converge([], [alert], [], [], now=BASE, close_after_seconds=300)
    return inc


def evidence_from(stack: str) -> dict:
    g = replaying(f"collect_evidence_{stack}")
    logs, traces = (vlogs(g), VictoriaTraces(g, "vt")) if stack == "victoria" else (eslogs(g), Tempo(g, "tempo"))
    return rca.collect_evidence(slow_incident(), [], g, logs=logs, traces=traces, id_fields=["eqp_id", "lot_id", "carrier_id"], now=BASE)


# ── 兩種 Trace 資料庫：成功的請求不會被搜成「失敗的」──

def test_successful_requests_are_not_returned_by_the_failed_trace_search_of_either_store():
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

def test_a_latency_incident_gets_its_warnings_and_no_made_up_call_chains_from_either_stack():
    a, b = evidence_from("victoria"), evidence_from("es_tempo")
    same = lambda ev: json.loads(trim(json.dumps({k: v for k, v in ev.items() if k not in ("log_source", "trace_source")}, ensure_ascii=False)))
    assert same(a) == same(b)
    assert (a["log_source"]["name"], a["trace_source"], b["log_source"]["name"], b["trace_source"]) == ("VictoriaLogs", "VictoriaTraces", "Elasticsearch", "Tempo")
    assert a["error_logs"] == [] and a["sources"] == {"logs": "empty", "call_chains": "empty", "metrics": "off"}
    assert sorted(w["example"] for w in a["warnings"]) == ["connection pool usage 70% (14/20 active)", SLOW_QUERY]   # 1. 警告照樣在
    assert a["call_chains"] == [] and "trace_logs" not in a                                                          # 2. 不挑最慢的充數
    assert any("沒有這些系統的錯誤 Trace" in n for n in a["notes"])


def test_the_note_and_the_prompt_show_the_warnings_and_say_there_were_no_failed_requests():
    ev = evidence_from("victoria")
    text = rca.overview_text(rca.overview(ev))
    assert "警告 2 種" in text and "VictoriaTraces 裡沒有這些系統的失敗請求" in text
    assert "缺錯誤日誌｜" in text and "log.level 沒有一筆是 ERROR／FATAL／CRITICAL" in text   # 缺的照實寫，不因為有警告就不提
    prompt = rca.build_prompt(ev, text)
    assert "【3e. 警告" in prompt and SLOW_QUERY in prompt and "【6." not in prompt and "slowest" not in prompt


# ── 不該多做的時候不多做（假的 Grafana 照查詢條件篩資料）──

def test_only_failed_traces_are_searched_for_even_when_none_are_found(fake):
    ev = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid")
    assert ev["call_chains"] and all(set(c) == {"trace_id", "spans", "systems", "truncated"} for c in ev["call_chains"])
    fake.traces = {"33" * 16: fake.traces["33" * 16]}                              # 只剩錄下來的那條成功的 Trace
    ev = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid")
    assert ev["call_chains"] == []
    searches = [r for r in fake.trace_requests if r.url.path.endswith("/api/traces")]
    assert searches and all(json.loads(r.url.params["tags"]) == {"error": "true"} for r in searches)   # 從來沒有不限失敗的搜尋


def test_logs_carrying_the_trace_id_are_attached_and_a_failed_lookup_is_only_noted(fake):
    trace_id = "11" * 16
    fake.logs += [log_row(NOW - timedelta(seconds=30), "mes-api", "INFO", "track-in accepted, calling mes-db", trace_id=trace_id),
                  log_row(NOW - timedelta(seconds=29), "mes-db", "WARN", "too many connections (max 200)", trace_id=trace_id)]
    ev = collect(fake, incident("web-portal", "mes-api"), traces="vt-uid")
    [logs_of] = [t for t in ev["trace_logs"] if t["trace_id"] == trace_id]
    assert [(e["system"], e["level"], e["message"]) for e in logs_of["events"]] == [
        ("mes-api", "INFO", "track-in accepted, calling mes-db"), ("mes-db", "WARN", "too many connections (max 200)")]
    assert logs_of["systems"] == ["mes-api", "mes-db"]
    text = rca.overview_text(rca.overview(ev))
    assert f"呼叫鏈對應的日誌 {sum(len(t['events']) for t in ev['trace_logs'])} 筆" in text
    assert "【6b. 請求的日誌" in rca.build_prompt(ev, text)
    # mes-db 沒有告警：它在請求的日誌裡，是直接證據，分數不壓到 30
    answer = json.dumps({"summary": "mes-db 連線數滿了，mes-api 的 track-in 等不到連線", "systems": ["mes-db"], "ids": [],
                         "candidates": [{"source": "mes-db", "kind": "evidence", "confidence": 72, "reason": "too many connections"}]})
    assert [(c["source"], c["confidence"]) for c in rca.read_answer(answer, ev)[1]] == [("mes-db", 72)]
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
