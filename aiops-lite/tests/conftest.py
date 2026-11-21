"""測試共用：讀 Grafana 13.0.7 的真實回應樣本，以及一個照那些樣本的格式回話的假 Grafana（httpx.MockTransport）。"""
from __future__ import annotations

import copy
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
FIXTURES = Path(__file__).parent / "fixtures" / "grafana13"


def recorded(name: str) -> dict:
    """{"request", "status", "response"}：record.py 從真的 Grafana 13.0.7 錄下來的原樣。"""
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def replay(name: str) -> httpx.Response:
    rec = recorded(name)
    return httpx.Response(rec["status"], json=rec["response"])


def noise(ts_ms: np.ndarray, seed: float) -> np.ndarray:
    """決定性的假雜訊（同一個時間點每次查都一樣），大約是 ±1 的均勻分布。"""
    x = np.sin(ts_ms / 60000.0 * 12.9898 + seed) * 43758.5453
    return (x - np.floor(x)) * 2 - 1


class FakeGrafana:
    """假的 Grafana：規則清單、/api/ds/query、ES 代理、註記。回應格式照 tests/fixtures/grafana13/ 的真實樣本。

    series[datasource_uid, expr 或 rawSql] = 函式(ts_ms 陣列) -> 值陣列；沒登記的查詢回 Grafana 的查詢錯誤格式。"""

    def __init__(self):
        self.rules = copy.deepcopy(recorded("alert_rules")["response"])
        self.series = {}
        self.annotations: dict[int, dict] = {}
        self.calls: list[tuple[str, str]] = []
        self.queries: list[dict] = []
        self.fail: set[str] = set()   # 路徑開頭在這裡面的請求回 500
        self.es = recorded("es_proxy_logs_traces")["response"]
        self.es_requests: list[str] = []
        self.transport = httpx.MockTransport(self.handle)

    def rule(self, title: str) -> dict:
        return next(r for r in self.rules if r["title"] == title)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append((request.method, path))
        if any(path.startswith(p) for p in self.fail):
            return httpx.Response(500, json={"message": "boom"})
        if path == "/api/v1/provisioning/alert-rules":
            return httpx.Response(200, json=self.rules)
        if path == "/api/ds/query":
            return self.query(json.loads(request.content))
        if path.startswith("/api/datasources/proxy/uid/") and path.endswith("/_msearch"):
            self.es_requests.append(request.content.decode())
            return httpx.Response(200, json=self.es)
        if path == "/api/annotations" and request.method == "POST":
            new_id = len(self.annotations) + 1
            self.annotations[new_id] = json.loads(request.content)
            return httpx.Response(200, json={"id": new_id, "message": "Annotation added"})
        if path == "/api/annotations" and request.method == "GET":
            tag = request.url.params["tags"]
            return httpx.Response(200, json=[{"id": i, **a} for i, a in self.annotations.items() if tag in a["tags"]])
        if path.startswith("/api/annotations/") and request.method == "PATCH":
            self.annotations[int(path.rsplit("/", 1)[1])].update(json.loads(request.content))
            return httpx.Response(200, json={"message": "Annotation patched"})
        return httpx.Response(404, json={"message": "Not found"})

    def query(self, body: dict) -> httpx.Response:
        q = body["queries"][0]
        self.queries.append(body)
        fn = self.series.get((q["datasource"]["uid"], q.get("expr") or q.get("rawSql")))
        if fn is None:
            return httpx.Response(400, json={"results": {"A": {
                "error": "bad_data: no such series", "errorSource": "downstream", "status": 400, "frames": []}}})
        step = int(q["intervalMs"])
        ts = np.arange(math.ceil(int(body["from"]) / step) * step, int(body["to"]) + 1, step, dtype="int64")
        values = np.asarray(fn(ts), dtype=float)
        keep = np.isfinite(values)
        return httpx.Response(200, json={"results": {"A": {"status": 200, "frames": [{
            "schema": {"refId": "A", "fields": [{"name": "Time", "type": "time"}, {"name": "Value", "type": "number"}]},
            "data": {"values": [ts[keep].tolist(), values[keep].tolist()]}}]}}})


def ms(t: datetime) -> int:
    return int(t.replace(tzinfo=t.tzinfo or timezone.utc).timestamp() * 1000)


@pytest.fixture
def fake() -> FakeGrafana:
    return FakeGrafana()
