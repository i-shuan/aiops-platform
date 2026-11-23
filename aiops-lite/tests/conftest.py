"""測試共用：讀 Grafana 13.0.7 的真實樣本，以及一個照那些樣本的格式回話的假 Grafana（httpx.MockTransport）。"""
from __future__ import annotations

import copy
import json
import os
import sys
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
FIXTURES = Path(__file__).parent / "fixtures" / "grafana13"


def recorded(name: str) -> dict:
    """{"request", "status", "response"}：record.py 從真的 Grafana 13.0.7 錄下來的原樣。"""
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def webhook(name: str) -> dict:
    """Grafana 13.0.7 實際推出來的告警 webhook 內容（firing_mq／firing_cim／firing_amhs／resolved_*）。"""
    return copy.deepcopy(recorded(f"webhook_{name}")["body"])


def replay(name: str) -> httpx.Response:
    rec = recorded(name)
    return httpx.Response(rec["status"], json=rec["response"])


class FakeGrafana:
    """假的 Grafana：ES 代理與註記。回應格式照 tests/fixtures/grafana13/ 的真實樣本。"""

    def __init__(self):
        self.annotations: dict[int, dict] = {}
        self.tag_index: dict[int, set[str]] = {}   # 照 Grafana 13.0.7 的實測行為：更新標籤後，舊標籤還查得到
        self.calls: list[tuple[str, str]] = []
        self.fail: set[str] = set()   # 路徑開頭在這裡面的請求回 500
        self.es = recorded("es_proxy_logs_traces")["response"]
        self.es_requests: list[str] = []
        self.error_searches = 0        # 撈錯誤日誌的次數（= 做了幾次證據收集）
        self.timeline_requests: list[list[dict]] = []
        self.timeline_docs: dict[str, list[dict]] | None = None   # None = 從錯誤日誌樣本裡挑同一個識別碼的紀錄
        self.tempo_search = recorded("tempo_search")["response"]
        self.tempo_traces = {"11111111111111111111111111111111": recorded("tempo_trace")["response"]}
        self.tempo_requests: list[httpx.Request] = []
        self.transport = httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append((request.method, path))
        if any(path.startswith(p) for p in self.fail):
            return httpx.Response(500, json={"message": "boom"})
        if path.startswith("/api/datasources/proxy/uid/") and path.endswith("/_msearch"):
            return self.msearch(request.content.decode())
        if path.startswith("/api/datasources/proxy/uid/") and path.endswith("/api/search"):
            self.tempo_requests.append(request)
            return httpx.Response(200, json=self.tempo_search)
        if path.startswith("/api/datasources/proxy/uid/") and "/api/traces/" in path:
            self.tempo_requests.append(request)
            trace = self.tempo_traces.get(path.rsplit("/", 1)[1])
            return httpx.Response(200, json=trace) if trace else httpx.Response(404)   # Tempo 的 404 沒有內容
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


def _msearch(self: FakeGrafana, body: str) -> httpx.Response:
    """ES 代理。撈錯誤日誌的查詢有三個條件（時間、系統、等級）→ 回錄下來的真實回應；
    關聯時間線的查詢只有兩個條件（時間、某個識別碼）→ 回帶那個識別碼的紀錄，由舊到新。"""
    lines = [json.loads(line) for line in body.splitlines() if line.strip()]
    queries = lines[1::2]
    if any(len(q["query"]["bool"]["filter"]) == 3 for q in queries):
        self.es_requests.append(body)
        self.error_searches += 1
        return httpx.Response(200, json=self.es)
    self.timeline_requests.append(queries)
    responses = []
    for q in queries:
        (field, values), = q["query"]["bool"]["filter"][1]["bool"]["should"][0]["terms"].items()
        if self.timeline_docs is not None:
            docs = self.timeline_docs.get(values[0], [])
        else:
            docs = sorted((h["_source"] for h in self.es["responses"][0]["hits"]["hits"] if h["_source"].get(field) == values[0]),
                          key=lambda d: str(d.get(next(iter(q["sort"][0])), "")))
        shown = docs[:q["size"]]
        responses.append({"hits": {"total": {"value": len(docs), "relation": "eq"}, "hits": [{"_source": d} for d in shown]}})
    return httpx.Response(200, json={"responses": responses})


FakeGrafana.msearch = _msearch


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
