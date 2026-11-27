"""Trace 的兩種來源，對外是同一組方法、回同一種形狀。都只經 Grafana 的資料源代理。

  VictoriaTraces  Grafana 的 Jaeger 型別資料源（URL 指到 VictoriaTraces 的 /select/jaeger），Jaeger 相容的查詢介面
  Tempo           Grafana 的 Tempo 資料源，TraceQL 搜尋

config.yaml 的 traces.type 決定用哪一個（main.py 的 trace_store）。

  search_error_traces   時間窗內、這些服務參與、而且有失敗 span 的 Trace：[{"id", "start"（微秒）, "trace"（有的話）}]
  get_trace             用編號取回整條 Trace；找不到回 None

一條 Trace 的形狀固定是 Jaeger 的那一種（VictoriaTraces 原本回的樣子）：
  {"traceID", "spans": [{"spanID", "operationName", "processID", "startTime"（微秒）, "duration"（微秒）,
                         "references": [{"refType": "CHILD_OF", "spanID"}], "tags": [{"key", "value"}]}],
   "processes": {processID: {"serviceName"}}}
失敗的 span 帶 error=true 標籤，錯誤訊息在 otel.status_description。Tempo 回的 OTLP JSON 由 _from_otlp() 轉成這一種。
格式以 tests/fixtures/grafana13/ 的 victoria_traces.json、tempo_*.json 為準（經 Grafana 13.0.7 實際錄下來的）。
"""
from __future__ import annotations

import base64
import json
import re
from datetime import datetime
from typing import Any
from urllib.parse import quote

from src.grafana import Grafana, GrafanaError


class VictoriaTraces:
    name = "VictoriaTraces"

    def __init__(self, grafana: Grafana, datasource_uid: str):
        self.grafana, self.uid = grafana, datasource_uid

    def _get(self, path: str, **kw: Any) -> tuple[int, list[dict[str, Any]]]:
        status, raw = self.grafana.request("GET", f"/api/datasources/proxy/uid/{self.uid}{path}", accept=(404,), with_status=True, **kw)
        data = raw.get("data") if isinstance(raw, dict) else None
        if status != 404 and not isinstance(data, list):
            raise GrafanaError(f"Trace 查詢的回應沒有 data：{str(raw)[:200]}")
        return status, [t for t in data or [] if isinstance(t, dict)]

    def search_error_traces(self, services: list[str], start: datetime, end: datetime, limit: int) -> list[dict[str, Any]]:
        """Jaeger 的搜尋一次只能指定一個服務，所以每個服務問一次，同一條 Trace 只留一份。回來的已經是完整的 Trace。
        VictoriaTraces 收到 Trace 後幾秒內就搜尋得到（實測 v0.5.0 三秒）。"""
        found: dict[str, dict[str, Any]] = {}
        for service in services:
            _status, traces = self._get("/api/traces", params={
                "service": service, "tags": json.dumps({"error": "true"}), "limit": limit,
                "start": int(start.timestamp() * 1_000_000), "end": int(end.timestamp() * 1_000_000) + 1_000_000})
            for trace in traces:
                trace_id = str(trace.get("traceID") or "")
                if trace_id:
                    began = min((int(sp.get("startTime") or 0) for sp in trace.get("spans") or []), default=0)
                    found.setdefault(trace_id, {"id": trace_id, "start": began, "trace": trace})
        return list(found.values())

    def get_trace(self, trace_id: str) -> dict[str, Any] | None:
        status, traces = self._get(f"/api/traces/{quote(trace_id, safe='')}")
        return None if status == 404 or not traces else traces[0]


def _traceql_regex(name: str) -> str:
    """服務名稱放進 TraceQL 的 =~ "…" 裡：正規表示式的特殊字元要跳脫，字串裡的反斜線與引號也要跳脫。"""
    escaped = re.sub(r'([.^$*+?()\[\]{}|\\])', r'\\\1', name)
    return escaped.replace("\\", "\\\\").replace('"', '\\"')


def _attributes(items: Any) -> dict[str, str]:
    """OTLP 的 attributes（[{key, value: {stringValue|intValue|…}}]）→ {key: 字串化的值}。"""
    out = {}
    for item in items or []:
        value = item.get("value") if isinstance(item, dict) else None
        if isinstance(value, dict) and value:
            out[str(item.get("key"))] = str(next(iter(value.values())))
    return out


SPAN_KINDS = {"SPAN_KIND_SERVER": "server", "SPAN_KIND_CLIENT": "client", "SPAN_KIND_PRODUCER": "producer", "SPAN_KIND_CONSUMER": "consumer",
              "SPAN_KIND_INTERNAL": "internal", 1: "internal", 2: "server", 3: "client", 4: "producer", 5: "consumer"}


def _hex(raw: Any) -> str:
    """Tempo 經 Grafana 回的 trace id／span id 是 base64；轉成十六進位（Jaeger 的寫法，也是用編號取 Trace 時要的寫法）。
    本來就是十六進位的原樣回傳。"""
    text = str(raw or "")
    if not text or re.fullmatch(r"[0-9a-fA-F]{16}|[0-9a-fA-F]{32}", text):
        return text.lower()
    try:
        return base64.b64decode(text, validate=True).hex()
    except ValueError:
        return text


def _from_otlp(raw: dict[str, Any], trace_id: str) -> dict[str, Any]:
    """Tempo 回的 OTLP JSON（batches／resourceSpans → scopeSpans → spans，時間是奈秒）→ Jaeger 的形狀。
    span 的種類放進 span.kind 標籤；「連結」（links：這一段接續另一條 Trace 的某一段，訊息佇列常見）轉成 FOLLOWS_FROM 的 reference，
    跟 VictoriaTraces 回的寫法一樣。"""
    spans, processes = [], {}
    for n, batch in enumerate(raw.get("batches") or raw.get("resourceSpans") or (raw.get("trace") or {}).get("resourceSpans") or []):
        process = f"p{n + 1}"
        processes[process] = {"serviceName": _attributes((batch.get("resource") or {}).get("attributes")).get("service.name", "")}
        for scope in batch.get("scopeSpans") or []:
            for sp in scope.get("spans") or []:
                status = sp.get("status") or {}
                failed = status.get("code") in ("STATUS_CODE_ERROR", 2, "2")
                tags = {**_attributes(sp.get("attributes")), "error": "true" if failed else "false"}
                if status.get("message"):
                    tags["otel.status_description"] = str(status["message"])
                if sp.get("kind") in SPAN_KINDS:
                    tags["span.kind"] = SPAN_KINDS[sp["kind"]]
                start = int(sp.get("startTimeUnixNano") or 0)
                parent = sp.get("parentSpanId")
                references = [{"refType": "CHILD_OF", "spanID": _hex(parent), "traceID": trace_id}] if parent else []
                references += [{"refType": "FOLLOWS_FROM", "spanID": _hex(link.get("spanId")), "traceID": _hex(link.get("traceId"))}
                               for link in sp.get("links") or [] if isinstance(link, dict) and link.get("traceId")]
                spans.append({"traceID": trace_id, "spanID": _hex(sp.get("spanId")), "operationName": str(sp.get("name", "")),
                              "processID": process, "startTime": start // 1000,
                              "duration": max(0, int(sp.get("endTimeUnixNano") or 0) - start) // 1000,
                              "references": references,
                              "tags": [{"key": k, "type": "string", "value": v} for k, v in tags.items()]})
    return {"traceID": trace_id, "spans": spans, "processes": processes}


class Tempo:
    name = "Tempo"

    def __init__(self, grafana: Grafana, datasource_uid: str):
        self.grafana, self.uid = grafana, datasource_uid

    def search_error_traces(self, services: list[str], start: datetime, end: datetime, limit: int) -> list[dict[str, Any]]:
        """TraceQL 搜尋，一次問完所有服務。回來的只有摘要（編號與開始時間），整條 Trace 要另外用 get_trace 取。
        注意：Tempo 收到 Trace 之後要過一陣子才搜尋得到（實測 2.9.0 要 8～40 秒以上）；用編號直接取則立刻取得到。"""
        names = "|".join(_traceql_regex(s) for s in services)
        raw = self.grafana.request("GET", f"/api/datasources/proxy/uid/{self.uid}/api/search", params={
            "q": '{ resource.service.name =~ "' + names + '" && status = error }',
            "start": int(start.timestamp()), "end": int(end.timestamp()) + 1, "limit": limit})
        traces = raw.get("traces") if isinstance(raw, dict) else None
        if not isinstance(traces, list):
            raise GrafanaError(f"Tempo 搜尋的回應沒有 traces：{str(raw)[:200]}")
        return [{"id": str(t["traceID"]), "start": int(t.get("startTimeUnixNano") or 0) // 1000, "trace": None}
                for t in traces if isinstance(t, dict) and t.get("traceID")]

    def get_trace(self, trace_id: str) -> dict[str, Any] | None:
        """Tempo 已經沒有這條（過了保留期限、被取樣掉）回 None。Tempo 的 404 沒有內容。"""
        status, raw = self.grafana.request("GET", f"/api/datasources/proxy/uid/{self.uid}/api/traces/{quote(trace_id, safe='')}",
                                           accept=(404,), with_status=True, empty_ok=True)
        if status == 404 or not isinstance(raw, dict):
            return None
        trace = _from_otlp(raw, trace_id)
        return trace if trace["spans"] else None
