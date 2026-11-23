"""假的 Grafana（測試與 scripts/chaos 共用）：把 GrafanaSpec 送來的資料源 proxy 請求解開，交給「假 ES」的處理函式。

照 aiops/sources/grafana.py 檔頭的線上格式做；狀態碼與錯誤外形照 2026-11-20 對 Grafana 13.0.7 探測到的（那一段的「已驗證」）：
  POST /api/datasources/proxy/uid/<uid>/_msearch，body 是 NDJSON 兩行（標頭 {"index", "ignore_unavailable"}、search body）
  -> 以「直連 ES」的樣子（POST /<index>/_search、JSON body）轉給 es_handler -> 回 {"responses": [<ES 回應>]}；
  ES 回非 2xx 時：HTTP 同一個狀態碼、最外層 {"error": ..., "status": N}（沒有 responses）。
  資料源 proxy 的其他 POST -> 403（Grafana 只放行 _msearch）；UID 不存在 -> 404 "Unable to find datasource"。
  GET /api/health（不需要登入）、GET /api/datasources/uid/<uid> 給 check() 用。
任何其他方法或路徑都記在 FakeGrafana.rejected：平台只准送上面這三種（只讀），測試用它確認。
這仍然是替身：沒探測過的部分（Bearer token、random_score）它照樣放行，不代表真的 Grafana 也是。
格式不對（少 Bearer、Content-Type 不對、NDJSON 不是兩行）直接 AssertionError，不默默接受。
"""
from __future__ import annotations

import json
import re
from typing import Callable

import httpx

from aiops.sources.grafana import GrafanaSpec

URL, TOKEN = "http://grafana", "glsa_test_token"
UIDS = {"traces": "traces-uid", "logs": "logs-uid"}
_PROXY = re.compile(r"^/api/datasources/proxy/uid/(?P<uid>[^/]+)/_msearch$")
_DATASOURCE = re.compile(r"^/api/datasources/uid/(?P<uid>[^/]+)$")


class FakeGrafana:
    def __init__(self, es_handler: Callable[[httpx.Request], httpx.Response], token: str = TOKEN,
                 datasources: dict[str, str] | None = None):
        self.es_handler, self.token = es_handler, token
        # uid -> 資料源型別；proxy 與 /api/datasources/uid 只認這些
        self.datasources = datasources if datasources is not None else {uid: "elasticsearch" for uid in UIDS.values()}
        self.calls: list[tuple[str, str]] = []      # 每個收到的請求 (method, path)
        self.searches: list[tuple[str, str]] = []   # 每次 _msearch 的 (uid, index)
        self.rejected: list[tuple[str, str]] = []   # 不認得的方法／路徑

    def handler(self, req: httpx.Request) -> httpx.Response:
        self.calls.append((req.method, req.url.path))
        if req.method == "GET" and req.url.path == "/api/health":
            return httpx.Response(200, json={"database": "ok", "version": "13.0.0"})
        if req.headers.get("authorization") != f"Bearer {self.token}":
            return httpx.Response(401, json={"message": "Unauthorized"})
        ds = _DATASOURCE.match(req.url.path)
        if req.method == "GET" and ds:
            kind = self.datasources.get(ds["uid"])
            if kind is None:
                return httpx.Response(404, json={"message": "Unable to find datasource", "traceID": ""})
            return httpx.Response(200, json={"uid": ds["uid"], "type": kind, "name": f"fake {ds['uid']}", "access": "proxy"})
        proxy = _PROXY.match(req.url.path)
        if req.method == "POST" and proxy:
            if proxy["uid"] not in self.datasources:
                return httpx.Response(404, json={"message": "Unable to find datasource", "traceID": ""})
            return self._msearch(proxy["uid"], req)
        self.rejected.append((req.method, req.url.path))
        if req.method == "POST" and req.url.path.startswith("/api/datasources/proxy/uid/"):
            return httpx.Response(403, json={"message": "posts not allowed on proxied Elasticsearch datasource except on /_msearch"})
        return httpx.Response(404, json={"message": "Not found"})

    def _msearch(self, uid: str, req: httpx.Request) -> httpx.Response:
        assert req.headers.get("content-type") == "application/x-ndjson", req.headers.get("content-type")
        text = req.content.decode("utf-8")
        assert text.endswith("\n"), "NDJSON 最後一行也要以換行結尾"
        lines = text.splitlines()
        assert len(lines) == 2, f"預期 NDJSON 兩行（標頭＋查詢），收到 {len(lines)} 行"
        header, body = json.loads(lines[0]), json.loads(lines[1])
        assert set(header) == {"index", "ignore_unavailable"} and header["ignore_unavailable"] is True, header
        self.searches.append((uid, header["index"]))
        inner = self.es_handler(httpx.Request("POST", f"http://es/{header['index']}/_search", json=body))
        if inner.is_success:
            return httpx.Response(200, content=b'{"responses": [' + inner.content + b"]}",
                                  headers={"content-type": "application/json"})
        payload = inner.json() if inner.content else {}
        return httpx.Response(inner.status_code, json={"error": payload.get("error", {"reason": inner.text[:200]}),
                                                       "status": inner.status_code})


def grafana_spec(es_handler: Callable[[httpx.Request], httpx.Response], **kw) -> tuple[GrafanaSpec, FakeGrafana]:
    """接到假 Grafana 的 GrafanaSpec；es_handler 是假 ES（以前直接接在直連的資料源後面）（看到的請求形狀不變）。"""
    fake = FakeGrafana(es_handler, **kw)
    return GrafanaSpec(URL, TOKEN, dict(UIDS), client=httpx.Client(transport=httpx.MockTransport(fake.handler))), fake


def set_grafana_env(monkeypatch, url: str = URL) -> None:
    monkeypatch.setenv("AIOPS_GRAFANA_URL", url)
    monkeypatch.setenv("AIOPS_GRAFANA_TOKEN", TOKEN)
    monkeypatch.setenv("AIOPS_GRAFANA_TRACES_UID", UIDS["traces"])
    monkeypatch.setenv("AIOPS_GRAFANA_LOGS_UID", UIDS["logs"])


def clear_grafana_env(monkeypatch) -> None:
    for var in ("AIOPS_GRAFANA_URL", "AIOPS_GRAFANA_TOKEN", "AIOPS_GRAFANA_USER", "AIOPS_GRAFANA_PASSWORD",
                "AIOPS_GRAFANA_TRACES_UID", "AIOPS_GRAFANA_LOGS_UID"):
        monkeypatch.delenv(var, raising=False)
