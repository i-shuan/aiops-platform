"""測試共用：讀 Grafana 13.0.7 的真實樣本，以及一個照那些樣本的格式回話的假 Grafana（httpx.MockTransport）。"""
from __future__ import annotations

import copy
import json
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
        self.calls: list[tuple[str, str]] = []
        self.fail: set[str] = set()   # 路徑開頭在這裡面的請求回 500
        self.es = recorded("es_proxy_logs_traces")["response"]
        self.es_requests: list[str] = []
        self.transport = httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append((request.method, path))
        if any(path.startswith(p) for p in self.fail):
            return httpx.Response(500, json={"message": "boom"})
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


@pytest.fixture
def fake() -> FakeGrafana:
    return FakeGrafana()
