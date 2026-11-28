"""測試共用：讀 Grafana 13.0.7（後面接 Victoria 元件）的真實樣本，以及一個照那些樣本回話的假 Grafana（httpx.MockTransport）。"""
from __future__ import annotations

import copy
from datetime import timedelta
import json
import os
import sys
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
FIXTURES = Path(__file__).parent / "fixtures" / "grafana13"

from tests import logsql  # noqa: E402
from src import evidence as gather


def recorded(name: str) -> dict:
    """{"request", "status", "response"}：record.py 從真的 Grafana 13.0.7 錄下來的原樣。"""
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def webhook(name: str) -> dict:
    """Grafana 13.0.7 實際推出來的告警 webhook 內容（firing_mq／firing_cim／firing_amhs／resolved_*）。"""
    return copy.deepcopy(recorded(f"webhook_{name}")["body"])


def replay(name: str) -> httpx.Response:
    rec = recorded(name)
    return httpx.Response(rec["status"], json=rec["response"])


def victoria(kind: str) -> dict:
    """victoria_logs.json／victoria_traces.json：record_victoria.py 經 Grafana 13.0.7 的資料源代理，從真的 VictoriaLogs v1.17.0、
    VictoriaTraces v0.5.0 錄下來的請求與回應（{"exchanges": {名稱: [{"request", "status", "response"}]}}，回應是原樣的文字）。"""
    return json.loads((FIXTURES / f"victoria_{kind}.json").read_text(encoding="utf-8"))


def recorded_rows(dataset: str) -> list[dict]:
    """某個 dataset 在真的 VictoriaLogs 裡的每一列（由舊到新）。factory＝6 筆錯誤 + 1 筆 INFO。"""
    text = victoria("logs")["exchanges"][f"dump_{dataset}"][0]["response"]
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def recorded_service_graph(name: str) -> list[dict]:
    """service_graph.json：Tempo 2.9.0 的 service graph 指標寫進 VictoriaMetrics，經 Grafana 13.0.7 查回來的請求與回應。"""
    return json.loads((FIXTURES / "service_graph.json").read_text(encoding="utf-8"))["exchanges"][name]


def recorded_trace(name: str = "get_trace") -> dict:
    """錄下來的一條 Trace（Jaeger 格式）。get_trace＝失敗的那條（1111…）；get_trace_ok＝成功的（3333…）。"""
    return json.loads(victoria("traces")["exchanges"][name][0]["response"])["data"][0]


def log_row(time, service: str, level: str, message: str, **fields: str) -> dict:
    """照 VictoriaLogs 回的樣子造一列日誌（欄位攤平成一層）。測試要加自己的資料時用，形狀跟 recorded_rows 的一樣。
    time 可以是字串或 datetime。"""
    time = time if isinstance(time, str) else time.strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"_time": time, "_stream": '{dataset="factory"}', "_msg": message, "dataset": "factory", "log.level": level,
            "service.name": service, **fields}


# 樣本是 2026-11-21 00:26～00:31 的資料，告警 webhook 的樣本是 02:41 發的（兩批是分開錄的）。假的 Grafana 會照查詢的時間範圍
# 篩資料，所以預設把日誌與 Trace 的時間整批往後移這麼多，讓 6 筆錯誤落在 02:37:08～02:40:48（事故開始前的幾分鐘）。
# 只移時間，其他內容原樣。要原始時間就用 FakeGrafana(shift=timedelta(0))。
SHIFT = timedelta(hours=2, minutes=10, seconds=30)


def shifted(rows: list[dict], by: timedelta) -> list[dict]:
    return [{**row, "_time": (logsql.when(row["_time"]) + by).strftime("%Y-%m-%dT%H:%M:%SZ")} for row in rows]


def shifted_trace(trace: dict, by: timedelta) -> dict:
    micros = int(by.total_seconds() * 1_000_000)
    return {**trace, "spans": [{**span, "startTime": span["startTime"] + micros} for span in trace["spans"]]}


TRACE_MISSING = victoria("traces")["exchanges"]["get_trace_missing"][0]


class FakeGrafana:
    """假的 Grafana：VictoriaLogs／Jaeger（VictoriaTraces）／Prometheus 相容資料源的代理，加上註記。
    Elasticsearch（_msearch）與 Tempo（資料源 uid 以 tempo 開頭）則是照錄下來的真實回應原樣回話。

    日誌：self.logs 是 VictoriaLogs 會回的那種列；查詢交給 tests/logsql.py 照條件真的去篩（看不懂的語法回 400）。
    Trace：self.traces 是 {trace id: Jaeger 格式的 Trace}；搜尋照 service 與 error 標籤挑。
    預設的資料都是從真的元件錄下來的（tests/fixtures/grafana13/victoria_*.json）。"""

    def __init__(self, shift: timedelta = SHIFT):
        self.annotations: dict[int, dict] = {}
        self.tag_index: dict[int, set[str]] = {}   # 照 Grafana 13.0.7 的實測行為：更新標籤後，舊標籤還查得到
        self.calls: list[tuple[str, str]] = []
        self.fail: set[str] = set()   # 路徑開頭在這裡面的請求回 500
        self.fail_queries: set[str] = set()   # LogsQL 裡含這些字的查詢回 500（讓某一種查詢單獨失敗）
        self.logs: list[dict] = shifted(recorded_rows("factory"), shift)
        self.log_queries: list[dict[str, str]] = []   # 每一次日誌查詢送的表單：query、start、end
        self.traces: dict[str, dict] = {}
        for name in ("search_error_traces", "get_trace_ok"):
            for exchange in victoria("traces")["exchanges"][name]:
                for trace in json.loads(exchange["response"])["data"]:
                    self.traces[trace["traceID"]] = shifted_trace(trace, shift)
        self.trace_requests: list[httpx.Request] = []
        # Tempo 的 service graph（存在指標資料源裡）：(呼叫的, 被呼叫的, 請求數, 失敗數)。回應的格式照 service_graph.json 錄下來的
        self.service_graph: list[tuple[str, str, int, int]] = []
        self.service_graph_requests: list[httpx.Request] = []
        # VictoriaTraces 的 service graph（Jaeger 的 /api/dependencies）：(呼叫的, 被呼叫的)。回應的格式照錄下來的
        self.dependencies: list[tuple[str, str]] = []
        self.dependency_requests: list[httpx.Request] = []
        # Elasticsearch 與 Tempo：照錄下來的真實回應回話（不篩資料）。msearch_replies 是照順序用掉的 {"status", "response"}
        self.msearch_replies: list[dict] = []
        self.msearch_requests: list[str] = []
        self.tempo_search = recorded("tempo_search")["response"]
        self.tempo_traces = {"11" * 16: recorded("tempo_trace")["response"]}
        self.metric_requests: list[httpx.Request] = []
        self.transport = httpx.MockTransport(self.handle)

    def queries(self, containing: str) -> list[str]:
        """送過的 LogsQL 裡含這段字的那些。"""
        return [q["query"] for q in self.log_queries if containing in q["query"]]

    @property
    def error_searches(self) -> int:
        """撈錯誤日誌的次數（= 做了幾次證據收集）：search_errors 是唯一用「最新的在前」排序的查詢。"""
        return len(self.queries("sort by (_time) desc"))

    def query_range(self, query: str) -> httpx.Response:
        """指標資料源的代理：回 Grafana 13.0.7 + VictoriaMetrics v1.110.0 實際錄下來的回應（序列來自 RabbitMQ 3.12.1）
        （prom_query_range_*.json）。只有 mq-broker 有佇列的指標；其他系統查得到但沒有資料；語法錯的回 400。"""
        if "[1m)" in query:
            name = "prom_query_range_bad_query"
        elif 'service="mq-broker"' in query and "messages_ready" in query:
            name = "prom_query_range_queue_ready"
        elif 'service="mq-broker"' in query and "consumers" in query:
            name = "prom_query_range_consumers"
        else:
            name = "prom_query_range_empty"
        return replay(name)

    def instant_query(self, query: str) -> httpx.Response:
        """service graph 的請求數、失敗數（事故期間與基線都回同一組數字）。拿錄下來的真實回應當樣板，把每條序列的標籤與數值換成 self.service_graph 的。
        延遲的查詢（_seconds）回沒有資料：跟沒開延遲直方圖的 Tempo 一樣。延遲的換算另外用錄下來的回應原樣重播測（test_service_graph_latency.py）。"""
        real = recorded_service_graph("window_with_traffic")[0]["response"]
        if "_seconds" in query:
            return httpx.Response(200, json={**real, "data": {**real["data"], "result": []}})
        failed = "request_failed_total" in query
        sample = real["data"]["result"][0]
        result = [{**sample, "metric": {"client": caller, "server": callee}, "value": [sample["value"][0], str(bad if failed else total)]}
                  for caller, callee, total, bad in self.service_graph if not failed or bad]
        return httpx.Response(200, json={**real, "data": {**real["data"], "result": result}})

    def logsql(self, request: httpx.Request) -> httpx.Response:
        form = dict(httpx.QueryParams(request.content.decode()))
        self.log_queries.append(form)
        if any(part in form["query"] for part in self.fail_queries):
            return httpx.Response(500, text="boom")
        try:
            rows = logsql.run(self.logs, form["query"], logsql.when(form["start"]), logsql.when(form["end"]))
        except logsql.LogsQLError as e:   # 真的 VictoriaLogs 也是回 400 與一段文字
            return httpx.Response(400, text=f'remoteAddr: "127.0.0.1:1"; requestURI: /select/logsql/query; {e}')
        return httpx.Response(200, text="".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
                              headers={"Content-Type": "application/stream+json"})

    def jaeger(self, request: httpx.Request) -> httpx.Response:
        self.trace_requests.append(request)
        path = request.url.path
        envelope = lambda traces: {"data": traces, "errors": None, "limit": 0, "offset": 0, "total": 0}
        if "/api/traces/" in path:
            trace = self.traces.get(path.rsplit("/", 1)[1])
            return httpx.Response(200, json=envelope([trace])) if trace else httpx.Response(404, text=TRACE_MISSING["response"])
        params = request.url.params
        tags = json.loads(params.get("tags") or "{}")
        start, end = int(params["start"]), int(params["end"])

        def matches(trace: dict) -> bool:
            # Jaeger 的搜尋：這個服務有一個 span 帶著全部指定的標籤，而且 Trace 落在時間範圍裡
            for span in trace["spans"]:
                own = {t["key"]: str(t["value"]) for t in span.get("tags") or []}
                if trace["processes"][span["processID"]]["serviceName"] == params["service"] \
                        and all(own.get(k) == v for k, v in tags.items()) and start <= span["startTime"] <= end:
                    return True
            return False
        return httpx.Response(200, json=envelope([t for t in self.traces.values() if matches(t)][:int(params.get("limit", 20))]))

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append((request.method, path))
        if any(path.startswith(p) for p in self.fail):
            return httpx.Response(500, json={"message": "boom"})
        if path.startswith("/api/datasources/proxy/uid/") and path.endswith("/select/logsql/query"):
            return self.logsql(request)
        if path.startswith("/api/datasources/proxy/uid/") and path.endswith("/api/v1/query_range"):
            self.metric_requests.append(request)
            return self.query_range(request.url.params["query"])
        if path.startswith("/api/datasources/proxy/uid/") and path.endswith("/api/v1/query"):
            self.service_graph_requests.append(request)
            return self.instant_query(request.url.params["query"])
        if path.startswith("/api/datasources/proxy/uid/") and path.endswith("/api/dependencies"):
            self.dependency_requests.append(request)
            real = recorded_service_graph("victoriatraces_dependencies")[0]["response"]
            sample = real["data"][0]
            return httpx.Response(200, json={**real, "data": [{**sample, "parent": a, "child": b} for a, b in self.dependencies],
                                             "total": len(self.dependencies)})
        if path.startswith("/api/datasources/proxy/uid/") and path.endswith("/_msearch"):
            self.msearch_requests.append(request.content.decode())
            reply = self.msearch_replies.pop(0)
            return httpx.Response(reply["status"], text=reply["response"], headers={"Content-Type": "application/json"})
        if path.startswith("/api/datasources/proxy/uid/tempo") and path.endswith("/api/search"):
            self.trace_requests.append(request)
            return httpx.Response(200, json=self.tempo_search)
        if path.startswith("/api/datasources/proxy/uid/tempo") and "/api/traces/" in path:
            self.trace_requests.append(request)
            trace = self.tempo_traces.get(path.rsplit("/", 1)[1])
            return httpx.Response(200, json=trace) if trace else httpx.Response(404)   # Tempo 的 404 沒有內容
        if path.startswith("/api/datasources/proxy/uid/") and "/api/traces" in path:
            return self.jaeger(request)
        if path == "/api/annotations" and request.method == "POST":
            self.next_id = getattr(self, "next_id", 0) + 1
            new_id = self.next_id
            self.annotations[new_id] = json.loads(request.content)
            self.tag_index[new_id] = set(self.annotations[new_id]["tags"])
            return httpx.Response(200, json={"id": new_id, "message": "Annotation added"})
        if path == "/api/annotations" and request.method == "GET":
            tag = request.url.params["tags"]
            return httpx.Response(200, json=[{"id": i, **self.annotations[i]} for i, tags in self.tag_index.items()
                                             if tag in tags])
        if path.startswith("/api/annotations/"):
            ann_id = int(path.rsplit("/", 1)[1])
            if ann_id not in self.annotations:
                return httpx.Response(403, json={"message": "You'll need additional permissions to perform this action."})
            if request.method == "GET":
                return httpx.Response(200, json={"id": ann_id, **self.annotations[ann_id]})
            if request.method == "PATCH":
                self.annotations[ann_id].update(json.loads(request.content))
                self.tag_index[ann_id] |= set(self.annotations[ann_id]["tags"])   # 只加不減
                return httpx.Response(200, json={"message": "Annotation patched"})
            if request.method == "DELETE":
                del self.annotations[ann_id], self.tag_index[ann_id]
                return httpx.Response(200, json={"message": "Annotation deleted"})
        return httpx.Response(404, json={"message": "Not found"})


def collect_evidence(incident, edges, grafana, *, logs_datasource_uid: str = "", traces_datasource_uid: str = "",
                     log_fields=None, logs_filter: str = "", **kw):
    """測試用的捷徑：用 VictoriaLogs／VictoriaTraces 當來源呼叫 gather.collect_evidence（uid 是空的 = 沒接）。"""
    from src import logs, traces
    store = logs.VictoriaLogs(grafana, logs_datasource_uid, log_fields or logs.LogFields(), logs_filter) if logs_datasource_uid else None
    chains = traces.VictoriaTraces(grafana, traces_datasource_uid) if traces_datasource_uid else None
    return gather.collect_evidence(incident, edges, grafana, logs=store, traces=chains, **kw)


@pytest.fixture
def fake() -> FakeGrafana:
    return FakeGrafana()


@pytest.fixture
def db_url(tmp_path):
    """資料庫連線字串：預設每個測試一個全新的 SQLite 檔；設 TEST_DATABASE_URL=postgresql+psycopg://… 就改在真的 PostgreSQL 上跑
    （每個測試前後把三張表清掉重建）。測試程式碼完全一樣，換的只是資料庫。"""
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        yield f"sqlite:///{tmp_path}/lite.db"
        return
    import sqlalchemy as sa
    from src import db as dbmod
    engine = sa.create_engine(url)
    dbmod.metadata.drop_all(engine)
    yield url
    dbmod.metadata.drop_all(engine)
    engine.dispose()
