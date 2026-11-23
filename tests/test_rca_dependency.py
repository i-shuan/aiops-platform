"""RCA 第一步強化（docs/RCA_STEP1_DESIGN.md）：下游資源耗時剝離與 dependency 下鑽、範例 trace 失敗優先、基準窗自適應回溯。

鐵律：排名與分數不變（候選仍是服務）；沒有 span.type 的資料（mock、舊索引）統計與報告逐字跟以前一樣；
基準窗只擴連續窗口、不跨週，報告要照實標出實際用了幾分鐘。全部直接用 ctx 測，不連外。
"""
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from aiops.engine.context import PipelineContext, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.sources.grafana import _span
from grafana_fake import grafana_spec
from aiops.stages.ingestion import fetch
from aiops.stages.rca.spans import is_resource_leaf, resource_kinds, sample_trace, sample_trace_reason, window_stats

T0 = datetime(2026, 11, 3, 14, 50, tzinfo=timezone.utc)
MIN = timedelta(minutes=1)


def _tx(trace, sid, svc, ts, ms, error=False, status=0, name="", parent=None):
    return SpanRecord(trace, sid, parent, svc, ts, ms, error, status_code=status, name=name)


def _leaf(trace, sid, parent, svc, ts, ms, kind, subtype, resource, error=False):
    return SpanRecord(trace, sid, parent, svc, ts, ms, error, kind=kind, subtype=subtype, resource=resource)


def _request(i, ts, wait_ms, app_ms, kind="external", subtype="http", resource="gw:443", error=False, svc="A", status=0):
    """服務 A 一個請求：入口 transaction（總耗時 = app + wait）、一個 app 子 span、一個等資源的葉節點。"""
    tid = f"t{i}"
    return [_tx(tid, f"{tid}x", svc, ts, app_ms + wait_ms, error, status, "POST /x"),
            _leaf(tid, f"{tid}a", f"{tid}x", svc, ts, app_ms, "app", "internal", ""),
            _leaf(tid, f"{tid}w", f"{tid}x", svc, ts, wait_ms, kind, subtype, resource, error)]


def _traffic(before_n, during_n, before_wait, during_wait, app=20.0, during_error_every=0):
    spans = []
    for i in range(before_n):
        spans += _request(f"b{i}", T0 - timedelta(minutes=15) + i * (15 * MIN / before_n), before_wait, app)
    for i in range(during_n):
        err = bool(during_error_every) and i % during_error_every == 0
        spans += _request(f"d{i}", T0 + i * (30 * MIN / during_n), during_wait, app, error=err, status=500 if err else 200)
    return spans


def _incident():
    return Incident([AnomalyEvent("A", "latency", T0 + m * MIN, 6.0) for m in range(0, 30, 5)])


def _rank(spans, **params):
    ctx = PipelineContext(pipeline="rca", now=T0 + timedelta(hours=1))
    ctx.rca_spans = spans
    ctx.incidents = [_incident()]
    ctx.incidents_built = True
    return OperatorRegistry.create("rca.root_cause.evidence", params).rank(ctx.incidents[0], ctx), ctx


# 1. 資源葉節點從自身耗時拆出來；self_ms_per_request 不變
def test_resource_leaf_is_split_out_of_self_time_but_self_time_itself_is_unchanged():
    spans = _request("1", T0, wait_ms=80.0, app_ms=20.0)
    st = window_stats(spans, T0, T0 + MIN)["A"]
    assert st.self_ms_per_request == pytest.approx(100.0)  # 入口自身 0 + app 20 + 等資源 80：排名用的定義不變
    assert st.app_ms_per_request == pytest.approx(20.0)
    assert st.wait_ms_per_request == {"gw:443": pytest.approx(80.0)} and st.wait_failures == {"gw:443": 0}
    assert resource_kinds(spans) == {"gw:443": "external/http"}


# 2. 沒有 span.type 的資料（mock 樣子）：統計跟以前一字不差、沒有資源行
def test_spans_without_kind_behave_exactly_as_before():
    spans = [_tx("t", "x", "A", T0, 100.0), SpanRecord("t", "db", "x", "A", T0, 80.0)]  # 子 span 沒有 kind
    st = window_stats(spans, T0, T0 + MIN)["A"]
    assert st.self_ms_per_request == pytest.approx(100.0) and st.app_ms_per_request == pytest.approx(100.0)
    assert st.wait_ms_per_request == {} and st.wait_failures == {} and resource_kinds(spans) == {}
    ctx = PipelineContext(pipeline="rca", now=T0 + MIN)
    ctx.incidents, ctx.incidents_built, ctx.rca_spans = [Incident([AnomalyEvent("A", "latency", T0, 6.0)])], True, spans
    body = OperatorRegistry.create("rca.section.trace_evidence", {}).execute(ctx).rca_sections[0].body
    assert "程式自身" not in body.split("＊ = ")[0] and "等待" not in body and "最慢的一條 trace" in body


# 3. 服務對服務：下游 transaction 是子 span，上游的 client span 不是葉節點，不算資源
def test_service_to_service_call_is_not_a_resource_leaf():
    spans = [_tx("t", "ax", "A", T0, 100.0),
             _leaf("t", "ac", "ax", "A", T0, 90.0, "external", "http", "B:8080"),  # 看起來像外部，但下游有 transaction
             _tx("t", "bx", "B", T0, 85.0, parent="ac")]
    children = {"ax": [spans[1]], "ac": [spans[2]]}
    assert not is_resource_leaf(spans[1], children)
    st = window_stats(spans, T0, T0 + MIN)
    assert st["A"].wait_ms_per_request == {} and st["A"].self_ms_per_request == pytest.approx(15.0)  # 100 − 90 + (90 − 85)
    assert st["B"].self_ms_per_request == pytest.approx(85.0)


# 4. 範例 trace 排序鍵：outcome 失敗 > 狀態碼 500 > 子樹失敗 > 最慢
def test_sample_trace_prefers_failures_over_latency():
    slow_ok = _tx("slow", "s", "A", T0, 900.0, status=200)
    fast_500 = _tx("f500", "f", "A", T0, 50.0, status=500)  # outcome 沒標、狀態碼有
    failed = _tx("fail", "e", "A", T0, 300.0, error=True, status=500)
    child_failed = [_tx("child", "c", "A", T0, 400.0, status=200),
                    _leaf("child", "cw", "c", "A", T0, 380.0, "db", "redis", "redis", error=True)]
    spans = [slow_ok, fast_500, failed, *child_failed]
    assert sample_trace(spans, T0, T0 + MIN) is failed
    assert sample_trace_reason(failed, spans) == "入口 500"
    spans.remove(failed)
    assert sample_trace(spans, T0, T0 + MIN) is fast_500
    spans.remove(fast_500)
    picked = sample_trace(spans, T0, T0 + MIN)
    assert picked is child_failed[0] and sample_trace_reason(picked, spans) == "redis 失敗"
    assert sample_trace([slow_ok], T0, T0 + MIN) is slow_ok and sample_trace_reason(slow_ok, [slow_ok]) == ""
    assert sample_trace([], T0, T0 + MIN) is None


# 5. evidence 歸因：等待增量佔比 >= 0.6 才填 dependency；排名與分數不受影響
def test_dependency_is_filled_only_when_one_resource_dominates_the_increase():
    # 等待 12 → 45（+33），程式自身 20 → 20：增量 100% 是資源
    ranked, _ = _rank(_traffic(40, 60, before_wait=12.0, during_wait=45.0))
    [cand] = ranked
    assert cand.entity == "A" and cand.dependency == "gw:443（external/http）"
    assert "等待 gw:443，external/http 12.0→45.0 ms（+33.0" in cand.reason and "程式自身 20.0→20.0 ms（+0.0）" in cand.reason
    # 等待 +10、程式自身 +30：資源只佔 25%，不填 dependency，但拆解數字照印
    spans = _traffic(40, 60, before_wait=10.0, during_wait=20.0)
    for s in spans:
        if s.span_id.startswith("td") and s.kind == "app":
            object.__setattr__(s, "duration_ms", 50.0)
        if s.span_id.startswith("td") and s.kind == "":
            object.__setattr__(s, "duration_ms", 70.0)
    ranked, _ = _rank(spans)
    assert ranked[0].dependency == "" and "程式自身 20.0→50.0 ms（+30.0）" in ranked[0].reason
    # 分數跟沒有 kind 的同一批資料一樣（dependency 不改計分）
    plain = [SpanRecord(s.trace_id, s.span_id, s.parent_id, s.service, s.ts, s.duration_ms, s.error) for s in spans]
    assert _rank(plain)[0][0].score == pytest.approx(ranked[0].score)
    assert _rank(plain)[0][0].dependency == ""


# 5b. 失敗歸因：入口失敗全是該資源回錯（等待沒有變長）
def test_dependency_by_failures_when_the_resource_errors_without_slowing_down():
    spans = _traffic(40, 60, before_wait=1.0, during_wait=1.0, during_error_every=3)
    for s in spans:  # 事件期間程式自己也變慢（讓它進候選），但失敗全在 redis
        if s.span_id.startswith("td") and s.kind == "app":
            object.__setattr__(s, "duration_ms", 40.0)
        if s.span_id.startswith("td") and s.kind == "":
            object.__setattr__(s, "duration_ms", 41.0)
        if s.kind == "external":
            object.__setattr__(s, "kind", "db"); object.__setattr__(s, "subtype", "redis"); object.__setattr__(s, "resource", "redis")
    ranked, _ = _rank(spans)
    assert ranked[0].dependency == "redis（db/redis）"  # 等待沒變長（+0.0）：靠「入口失敗全是 redis 回錯」填
    inc = _incident()
    stats = window_stats(spans, inc.start, inc.end + MIN)["A"]  # 跟算子同一個事件期間窗
    failures = stats.wait_failures["redis"]
    assert failures >= 15 and stats.error_rate > 0.3 and stats.own_error_rate == 0.0  # 失敗全是下游連帶，不算自身錯誤
    assert f"資源失敗 0→{failures} 筆" in ranked[0].reason and "等待 redis，db/redis 1.0→1.0 ms（+0.0" in ranked[0].reason


class _SparseES:
    """假 ES：事件前每 2 分鐘 1 條 trace（15 分鐘約 8 筆 < 20），事件期間每分鐘 2 條（60 筆 >= 20）。記下每次查詢的時間範圍。"""

    def __init__(self):
        self.ranges = []
        docs = []
        t = T0 - timedelta(minutes=90)
        n = 0
        while t < T0 + timedelta(minutes=30):
            per = 2 if t >= T0 else (1 if (int((T0 - t).total_seconds()) // 60) % 2 == 0 else 0)
            for _ in range(per):
                n += 1
                docs += [{"@timestamp": t.isoformat(), "trace": {"id": f"t{n}"}, "service": {"name": "A"},
                          "transaction": {"id": f"x{n}", "duration": {"us": 100_000}, "name": "GET /a"},
                          "event": {"outcome": "success"}, "http": {"response": {"status_code": 200}}},
                         {"@timestamp": t.isoformat(), "trace": {"id": f"t{n}"}, "service": {"name": "A"}, "parent": {"id": f"x{n}"},
                          "span": {"id": f"w{n}", "type": "external", "subtype": "http", "duration": {"us": 80_000},
                                   "destination": {"service": {"resource": "gw:443"}}}, "event": {"outcome": "success"}}]
            t += MIN
        self.docs = docs

    def handler(self, req):
        import json
        body = json.loads(req.content)
        rng = body["query"]["bool"]["filter"][0]["range"]["@timestamp"]
        lo = datetime.fromisoformat(rng["gte"])
        hi_key = "lte" if "lte" in rng else "lt"
        hi = datetime.fromisoformat(rng[hi_key])
        self.ranges.append((lo, hi))
        hits = [d for d in self.docs if lo <= datetime.fromisoformat(d["@timestamp"]) and
                (datetime.fromisoformat(d["@timestamp"]) <= hi if hi_key == "lte" else datetime.fromisoformat(d["@timestamp"]) < hi)]
        start = body["search_after"][0] + 1 if "search_after" in body else 0
        page = hits[start:start + body["size"]]
        out = {"hits": {"hits": [{"_source": d, "sort": [start + k]} for k, d in enumerate(page)]}}
        if "track_total_hits" in body:
            out["hits"]["total"] = {"value": min(len(hits), body["track_total_hits"]), "relation": "eq"}
        return httpx.Response(200, json=out)


def _use(monkeypatch, fake):
    spec = grafana_spec(fake.handler)[0]
    monkeypatch.setattr(fetch, "get_datasource_spec", lambda _t: spec)
    monkeypatch.setattr(fetch, "is_configured", lambda *_: True)


def _rca_ctx():
    ctx = PipelineContext(pipeline="rca_report", now=T0 + timedelta(hours=1))
    ctx.incidents, ctx.incidents_built = [_incident()], True
    return ctx


# 6. 回溯：15 分鐘 8 筆 < 20 → 只撈 [−30, −15) → 15 筆仍 < 20 → 只撈 [−60, −30) → 30 筆；報告標註
def test_lookback_extends_the_baseline_window_in_contiguous_steps_and_the_report_says_so(monkeypatch):
    fake = _SparseES()
    _use(monkeypatch, fake)
    ctx = OperatorRegistry.create("ingestion.incident_traces", {"max_before_minutes": 60}).execute(_rca_ctx())
    assert ctx.rca_trace_lookback == (15, 60)
    end = _incident().end + timedelta(minutes=1)  # after_minutes 0：事件最後一點那一分鐘的結尾（以前是最後一點的起點，docs/E2E_DRILL.md 問題 6）
    # 每次撈取是「一頁 + 一次 search_after 的空頁」兩個請求，所以看去重後的範圖；三段連續、不重疊、不跨週
    assert list(dict.fromkeys(fake.ranges)) == [(T0 - timedelta(minutes=15), end),
                                                (T0 - timedelta(minutes=30), T0 - timedelta(minutes=15)),
                                                (T0 - timedelta(minutes=60), T0 - timedelta(minutes=30))]
    tf = ctx.result["trace_fetch"]
    assert tf["before_minutes"] == 15 and tf["before_minutes_used"] == 60 and tf["extended_for"].startswith("A ")
    assert len({s.span_id for s in ctx.rca_spans}) == len(ctx.rca_spans)  # 邊界重複的文件只留一份
    body = OperatorRegistry.create("rca.section.trace_evidence", {}).execute(ctx).rca_sections[0].body
    assert "已由 15 分鐘擴展至 60 分鐘" in body and "事件前 60 分鐘 vs 事件期間" in body
    assert "等待 gw:443（external/http）" in body
    assert "A GET /a：100.0 ms" in body and "A external→gw:443：80.0 ms" in body  # 呼叫鏈：入口印 transaction.name、葉節點印資源
    # 關掉（預設）：只撈一次、欄位是 (15, 15)、報告沒有擴展字樣
    fake2 = _SparseES()
    _use(monkeypatch, fake2)
    ctx2 = OperatorRegistry.create("ingestion.incident_traces", {}).execute(_rca_ctx())
    assert ctx2.rca_trace_lookback == (15, 15) and len(set(fake2.ranges)) == 1
    body2 = OperatorRegistry.create("rca.section.trace_evidence", {}).execute(ctx2).rca_sections[0].body
    assert "擴展" not in body2


# 7. 事件期間也不夠：不擴展（留給根因算子寫證據不足）
def test_lookback_does_not_extend_when_the_incident_window_itself_is_too_thin(monkeypatch):
    fake = _SparseES()
    fake.docs = [d for d in fake.docs if datetime.fromisoformat(d["@timestamp"]) < T0 or int(d["trace"]["id"][1:]) % 7 == 0]
    _use(monkeypatch, fake)
    ctx = OperatorRegistry.create("ingestion.incident_traces", {"max_before_minutes": 60}).execute(_rca_ctx())
    assert ctx.rca_trace_lookback == (15, 15) and len(set(fake.ranges)) == 1 and ctx.result["trace_fetch"]["extended_for"] == ""


# 8. to_dict 帶 dependency；ES 欄位對映與參數驗證
def test_to_dict_carries_dependency_and_es_fields_map_to_the_record():
    ranked, ctx = _rank(_traffic(40, 60, before_wait=12.0, during_wait=45.0))
    ctx.rca_root_candidates = ranked
    ctx.rca_skipped = ["golden_signals"]  # 讓 to_result 走 RCA 那段輸出
    out = ctx.to_result()["root_cause"]["candidates"][0]
    assert out["entity"] == "A" and out["dependency"] == "gw:443（external/http）"
    doc = {"@timestamp": "2026-11-03T15:00:00Z", "trace": {"id": "t"}, "service": {"name": "bill-svc"}, "parent": {"id": "x"},
           "transaction": {"id": "x"}, "span": {"id": "s", "type": "external", "subtype": "http", "duration": {"us": 223000},
                                                "destination": {"service": {"resource": "tax-gw:443"}}},
           "event": {"outcome": "failure"}, "http": {"response": {"status_code": "502"}}}
    rec = SpanRecord(**_span(doc))
    assert (rec.kind, rec.subtype, rec.resource, rec.status_code, rec.error) == ("external", "http", "tax-gw:443", 502, True)
    tx = SpanRecord(**_span({"@timestamp": "2026-11-03T15:00:00Z", "trace": {"id": "t"}, "service": {"name": "s"},
                             "transaction": {"id": "x", "duration": {"us": 1000}, "name": "POST /invoice"}}))
    assert (tx.kind, tx.resource, tx.status_code, tx.name) == ("", "", 0, "POST /invoice")
    with pytest.raises(ValueError):
        OperatorRegistry.create("ingestion.incident_traces", {"max_before_minutes": 10})  # 小於 before_minutes 15
    with pytest.raises(ValueError):
        OperatorRegistry.create("rca.root_cause.evidence", {"min_dependency_share": 0})
