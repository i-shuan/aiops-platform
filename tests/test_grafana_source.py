"""Grafana 資料源的傳輸層（aiops/sources/grafana.py 的 _msearch 與 check）：認證標頭、網址、NDJSON、錯誤分工、設定。

這裡的回應外形照 2026-11-20 對 Grafana 13.0.7 手動探測到的寫（grafana.py 檔頭「已驗證」），但全部是 MockTransport：
測試過了代表程式跟那份紀錄一致，不代表對真的 Grafana 跑過（那要跑 scripts/verify_grafana_source.py）。
文件解析、分頁、抽樣的測試在 test_dynamic_topology／test_es_data_models／test_messaging_pipeline（都經假 Grafana）。
"""
import base64
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from aiops.engine.errors import InfrastructureError, PlatformError
from aiops.engine.registry import OperatorRegistry
from aiops.engine.context import PipelineContext
from aiops.engine.types import AnomalyEvent, Incident
from aiops.sources import check_sources, get_datasource_spec, is_configured, source_types
from aiops.sources.base import InvalidQueryError
from aiops.sources.grafana import GrafanaSpec
from grafana_fake import TOKEN, UIDS, URL, FakeGrafana, clear_grafana_env, grafana_spec, set_grafana_env

T = datetime(2026, 11, 3, 15, 0, tzinfo=timezone.utc)
EMPTY = {"took": 1, "timed_out": False, "_shards": {"total": 3, "successful": 3, "skipped": 0, "failed": 0},
         "hits": {"total": {"value": 0, "relation": "eq"}, "max_score": None, "hits": []}, "status": 200}


def _spec(reply, **kw):
    """直接回 reply(req) 的 GrafanaSpec（不經假 Grafana），回傳 (spec, 收到的請求)。"""
    seen: list[httpx.Request] = []

    def handler(req):
        seen.append(req)
        return reply(req)

    kw.setdefault("token", TOKEN)
    kw.setdefault("uids", dict(UIDS))
    return GrafanaSpec(URL + "/", client=httpx.Client(transport=httpx.MockTransport(handler)), **kw), seen


def _ok(req):
    return httpx.Response(200, json={"took": 1, "responses": [EMPTY]})


# ── 線上格式 ──


def test_search_is_one_msearch_post_through_the_datasource_proxy_with_bearer_token_and_ndjson():
    spec, seen = _spec(_ok)
    assert spec.fetch_spans(T, T + timedelta(minutes=5), ["cart-api"]) == ([], False)
    [req] = seen
    assert (req.method, str(req.url)) == ("POST", "http://grafana/api/datasources/proxy/uid/traces-uid/_msearch")
    assert req.headers["authorization"] == f"Bearer {TOKEN}" and req.headers["content-type"] == "application/x-ndjson"
    text = req.content.decode("utf-8")
    assert text.endswith("\n") and text.count("\n") == 2  # 兩行，各以換行結尾
    header, body = (json.loads(line) for line in text.splitlines())
    assert header == {"index": "traces-apm-*", "ignore_unavailable": True}
    assert body["track_total_hits"] == 20001 and body["query"]["bool"]["filter"][0]["range"]["@timestamp"]["gte"] == T.isoformat()


def test_logs_go_to_the_logs_datasource_and_index():
    spec, seen = _spec(_ok)
    assert spec.fetch_logs(T, T + timedelta(minutes=5), ["cart-api"]) == ([], False)
    assert seen[0].url.path == "/api/datasources/proxy/uid/logs-uid/_msearch"
    assert json.loads(seen[0].content.decode("utf-8").splitlines()[0])["index"] == "logs-apm.*"


def test_basic_auth_is_used_only_when_there_is_no_token():
    expected = "Basic " + base64.b64encode("amy:s3cret".encode()).decode()
    spec, seen = _spec(_ok, token=None, basic=("amy", "s3cret"))
    spec.fetch_spans(T, T, None)
    assert seen[0].headers["authorization"] == expected
    spec, seen = _spec(_ok, basic=("amy", "s3cret"))  # token 也給了：用 token
    spec.fetch_spans(T, T, None)
    assert seen[0].headers["authorization"] == f"Bearer {TOKEN}"


def test_the_platform_only_ever_sends_msearch_and_grafana_refuses_any_other_post():
    """只讀：一輪撈 span（含超過上限的抽樣）、撈日誌、依 trace 撈 span、check，對 Grafana 只出現三種請求。"""
    from test_dynamic_topology import _FakeES, _traffic

    spec, grafana = grafana_spec(_FakeES(_traffic(minutes=3, traces_per_minute=50)).handler)
    spec.fetch_spans(T, T + timedelta(minutes=3), None, max_spans=100)   # 600 筆 > 100：走抽樣
    spec.fetch_linked_spans(["t000001"], T, T + timedelta(minutes=3))
    spec.fetch_logs(T, T + timedelta(minutes=3), None)
    assert spec.check()["ok"] is True
    allowed = {("POST", f"/api/datasources/proxy/uid/{uid}/_msearch") for uid in UIDS.values()}
    allowed |= {("GET", "/api/health")} | {("GET", f"/api/datasources/uid/{uid}") for uid in UIDS.values()}
    assert set(grafana.calls) <= allowed and not grafana.rejected and len(grafana.searches) > 3
    # 假 Grafana 跟真的一樣只放行 _msearch（13.0.7 實測：其他 POST 回 403）
    direct = spec.client.post(f"{URL}/api/datasources/proxy/uid/traces-uid/traces-apm-*/_search",
                              headers=spec.headers, json={})
    assert direct.status_code == 403 and "except on /_msearch" in direct.json()["message"]


# ── 錯誤分工 ──


def test_top_level_error_with_http_400_is_an_invalid_query():
    """查詢寫錯：Grafana 13.0.7 實測回 HTTP 400、最外層 {"error": ..., "status": 400}，沒有 responses。"""
    bad = {"error": {"root_cause": [{"type": "parsing_exception", "reason": "unknown query [nope]"}],
                     "type": "parsing_exception", "reason": "unknown query [nope]"}, "status": 400}
    spec, _ = _spec(lambda req: httpx.Response(400, json=bad))
    with pytest.raises(InvalidQueryError, match=r"grafana 撈 trace失敗（HTTP 400）：unknown query \[nope\]"):
        spec.fetch_spans(T, T, None)


def test_per_search_error_inside_responses_is_an_invalid_query():
    """ES _msearch 規格：HTTP 200，但那一筆查詢的結果是 error＋status（沒有實際在 Grafana 看過，照規格處理）。"""
    entry = {"error": {"type": "search_phase_execution_exception", "reason": "all shards failed"}, "status": 503}
    spec, _ = _spec(lambda req: httpx.Response(200, json={"responses": [entry]}))
    with pytest.raises(InvalidQueryError, match="grafana 撈日誌失敗（Elasticsearch 回 503）：all shards failed"):
        spec.fetch_logs(T, T, None)


@pytest.mark.parametrize("status", [401, 403])
def test_datasource_side_auth_error_inside_responses_is_infrastructure(status):
    entry = {"error": {"type": "security_exception", "reason": "unable to authenticate"}, "status": status}
    spec, _ = _spec(lambda req: httpx.Response(200, json={"responses": [entry]}))
    with pytest.raises(InfrastructureError, match=f"uid=traces-uid 連 Elasticsearch 認證失敗（{status}）"):
        spec.fetch_spans(T, T, None)


@pytest.mark.parametrize("status", [401, 403])
def test_grafana_rejecting_the_token_is_infrastructure_and_not_retried(status, retry_waits):
    spec, seen = _spec(lambda req: httpx.Response(status, json={"message": "Unauthorized"}))
    with pytest.raises(InfrastructureError, match=f"grafana 認證失敗（HTTP {status}）"):
        spec.fetch_spans(T, T, None)
    assert len(seen) == 1 and retry_waits == []


def test_unknown_datasource_uid_is_infrastructure_and_names_the_env_var():
    spec, _ = _spec(lambda req: httpx.Response(404, json={"message": "Unable to find datasource", "traceID": ""}))
    with pytest.raises(InfrastructureError, match="uid=logs-uid，請檢查 AIOPS_GRAFANA_LOGS_UID：.*Unable to find datasource"):
        spec.fetch_logs(T, T, None)


def test_grafana_restarting_is_retried_then_infrastructure(retry_waits):
    spec, seen = _spec(lambda req: httpx.Response(503, text="starting"))
    with pytest.raises(InfrastructureError, match="已重試 2 次"):
        spec.fetch_spans(T, T, None)
    assert len(seen) == 3 and retry_waits == [0.5, 1.5]


@pytest.mark.parametrize("payload", [b"<html>login</html>", b"{}", b'{"responses": []}', b'{"responses": [1]}',
                                     b'{"responses": [{}, {}]}', b'{"hits": {"hits": []}}', b"[]"],
                         ids=["html", "empty-object", "no-entries", "entry-not-object", "two-entries", "bare-es-response", "list"])
def test_malformed_200_response_is_a_platform_error_not_an_empty_result(payload):
    """回 200 但外層不是 {"responses": [一筆]}：不能當成「撈到 0 筆」（報告會寫成沒有 trace），要報格式不對。"""
    spec, _ = _spec(lambda req: httpx.Response(200, content=payload))
    with pytest.raises(PlatformError, match="回應格式不對"):
        spec.fetch_spans(T, T, None)


def test_zero_shards_is_not_an_error_but_leaves_a_warning(caplog):
    """13.0.7 實測：索引樣式對不到東西／資料源的 ES 帳號沒權限時回 200、_shards.total = 0、0 筆。照樣回空，但留日誌。"""
    none = {**EMPTY, "_shards": {"total": 0, "successful": 0, "skipped": 0, "failed": 0}}
    spec, _ = _spec(lambda req: httpx.Response(200, json={"responses": [none]}))
    with caplog.at_level("WARNING", logger="aiops.sources"):
        assert spec.fetch_logs(T, T, None) == ([], False)
    assert "logs-apm.*" in caplog.text and "對不到任何 shard" in caplog.text
    caplog.clear()
    spec, _ = _spec(_ok)
    with caplog.at_level("WARNING", logger="aiops.sources"):
        spec.fetch_logs(T, T, None)
    assert caplog.text == ""  # 有 shard 只是沒資料：不吵


# ── 設定 ──


def test_configured_needs_url_and_a_credential(monkeypatch):
    clear_grafana_env(monkeypatch)
    assert "grafana" in source_types() and "elasticsearch" not in source_types()
    assert is_configured("grafana") is False
    monkeypatch.setenv("AIOPS_GRAFANA_URL", URL)  # 只有網址（深度連結本來就會設）：不算有設定
    assert is_configured("grafana") is False
    with pytest.raises(InfrastructureError, match="AIOPS_GRAFANA_TOKEN"):
        get_datasource_spec("grafana")
    monkeypatch.setenv("AIOPS_GRAFANA_TOKEN", TOKEN)
    assert is_configured("grafana") is True  # UID 不看
    monkeypatch.delenv("AIOPS_GRAFANA_TOKEN")
    monkeypatch.setenv("AIOPS_GRAFANA_USER", "amy")
    assert is_configured("grafana") is False  # 只有帳號沒有密碼
    monkeypatch.setenv("AIOPS_GRAFANA_PASSWORD", "s3cret")
    assert is_configured("grafana") is True
    assert get_datasource_spec("grafana").headers["Authorization"].startswith("Basic ")
    monkeypatch.delenv("AIOPS_GRAFANA_URL")
    assert is_configured("grafana") is False
    with pytest.raises(InfrastructureError, match="AIOPS_GRAFANA_URL"):
        get_datasource_spec("grafana")


@pytest.mark.parametrize("signal, env, call", [("traces", "AIOPS_GRAFANA_TRACES_UID", "fetch_spans"),
                                               ("logs", "AIOPS_GRAFANA_LOGS_UID", "fetch_logs")])
def test_missing_uid_fails_that_signal_with_the_env_var_name_and_sends_nothing(signal, env, call):
    spec, seen = _spec(_ok, uids={k: v for k, v in UIDS.items() if k != signal})
    with pytest.raises(InfrastructureError, match=f"沒有設定 {env}"):
        getattr(spec, call)(T, T, None)
    assert seen == []
    other = "fetch_logs" if call == "fetch_spans" else "fetch_spans"
    assert getattr(spec, other)(T, T, None) == ([], False)  # 另一種訊號不受影響


@pytest.mark.parametrize("operator, key, env", [("ingestion.incident_traces", "trace_fetch", "AIOPS_GRAFANA_TRACES_UID"),
                                                ("ingestion.incident_logs", "log_fetch", "AIOPS_GRAFANA_LOGS_UID")])
def test_rca_fetch_without_that_kinds_uid_is_skipped_quietly_like_an_unconfigured_source(monkeypatch, operator, key, env):
    """沒設 trace（或日誌）的資料源 UID = 這個環境沒有那種資料，跟整個資料源沒設定一樣安靜略過、不記錯誤。
    2026-11-20 在真實環境改的：純指標的產線管線沒有 trace，原本缺 UID 會記成「trace 來源讀取失敗」，
    根因演算法因此拒絕用關係圖推測、結論變成無法判定。（直接呼叫 fetch_spans 而沒有 UID 仍然丟錯，見上一個測試。）"""
    set_grafana_env(monkeypatch)
    monkeypatch.delenv(env)
    ctx = PipelineContext(pipeline="rca_report", now=T)
    ctx.incidents = [Incident([AnomalyEvent("cart-api", "lat", T, 5.0)])]
    ctx = OperatorRegistry.create(operator, {}).execute(ctx)
    assert "沒有設定" in ctx.result[key]["skipped"] and ctx.errors == []
    clear_grafana_env(monkeypatch)
    quiet = PipelineContext(pipeline="rca_report", now=T)
    quiet.incidents = ctx.incidents
    quiet = OperatorRegistry.create(operator, {}).execute(quiet)
    assert "沒有設定" in quiet.result[key]["skipped"] and quiet.errors == []


# ── check() ──


def _check(fake: FakeGrafana, uids=None, token=TOKEN):
    client = httpx.Client(transport=httpx.MockTransport(fake.handler))
    return GrafanaSpec(URL, token, dict(UIDS) if uids is None else uids, client=client).check()


def test_check_reports_health_and_each_datasource():
    fake = FakeGrafana(lambda req: httpx.Response(500))
    out = _check(fake)
    assert out["ok"] is True and "latency_ms" in out and "traces=traces-uid" in out["detail"] and "logs=logs-uid" in out["detail"]
    assert out["datasources"] == {
        "traces": {"uid": "traces-uid", "configured": True, "ok": True, "detail": "elasticsearch：fake traces-uid"},
        "logs": {"uid": "logs-uid", "configured": True, "ok": True, "detail": "elasticsearch：fake logs-uid"}}
    assert fake.calls == [("GET", "/api/health"), ("GET", "/api/datasources/uid/traces-uid"),
                          ("GET", "/api/datasources/uid/logs-uid")]  # 只有 GET，沒有查詢


def test_check_fails_on_a_bad_token_even_though_health_needs_no_login():
    out = _check(FakeGrafana(lambda req: httpx.Response(500)), token="wrong")
    assert out["ok"] is False and "認證失敗（HTTP 401）" in out["detail"]
    assert out["datasources"]["traces"]["ok"] is False and out["datasources"]["logs"]["ok"] is False


def test_check_fails_on_an_unknown_uid_or_a_datasource_of_another_type():
    fake = FakeGrafana(lambda req: httpx.Response(500), datasources={"traces-uid": "tempo"})
    out = _check(fake)
    assert out["ok"] is False
    assert "'tempo'" in out["datasources"]["traces"]["detail"]          # 這個資料源目前只會講 Elasticsearch 的 _msearch
    assert out["datasources"]["logs"]["detail"].startswith("HTTP 404")  # UID 不存在
    assert "traces：" in out["detail"] and "logs：" in out["detail"]


def test_check_does_not_count_an_unset_uid_as_a_failure_but_says_the_token_is_unverified():
    out = _check(FakeGrafana(lambda req: httpx.Response(500)), uids={"logs": "logs-uid"})
    assert out["ok"] is True
    assert out["datasources"]["traces"] == {"uid": None, "configured": False, "ok": None,
                                            "detail": "未設定 AIOPS_GRAFANA_TRACES_UID"}
    none = _check(FakeGrafana(lambda req: httpx.Response(500)), uids={}, token="wrong")
    assert none["ok"] is True and "token 沒有驗證" in none["detail"]  # 沒有 UID 可以問：只知道 Grafana 活著


def test_check_never_raises_when_grafana_is_unreachable(monkeypatch, retry_waits):
    set_grafana_env(monkeypatch, "http://127.0.0.1:1")
    out = next(r for r in check_sources() if r["source_type"] == "grafana")
    assert out["configured"] is True and out["ok"] is False and "連不上" in out["detail"]


# ── 平台不直連 Elasticsearch（CLAUDE.md 第 1 節「外部源適配」、第 5 節周邊元件守則）──


def test_platform_code_holds_no_direct_elasticsearch_connection():
    """aiops/ 裡不准再出現 ES 的連線設定、ApiKey 認證、直連的 _search 網址或任何寫入路徑；唯一的出口是 grafana.py 的 _msearch。"""
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "aiops"
    banned = re.compile(r"AIOPS_ES_URL|AIOPS_ES_API_KEY|ApiKey |\}/_search|/_bulk|/_doc\b|/_update|/_delete_by_query")
    bad = [f"{p.relative_to(root).as_posix()}:{n}" for p in sorted(root.rglob("*.py"))
           for n, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1) if banned.search(line)]
    assert not bad, bad
    grafana = (root / "sources" / "grafana.py").read_text(encoding="utf-8")
    # 整個檔只有兩個 POST：ES 資料源的 _msearch、規則查詢重放的 /api/ds/query（2026-11-20 加上指標；兩個都是唯讀查詢）
    assert grafana.count('"POST"') == 2 and "/_msearch" in grafana and "/api/ds/query" in grafana
    assert not (root / "sources" / "elasticsearch.py").exists() and not (root / "sources" / "query" / "elasticsearch.py").exists()
