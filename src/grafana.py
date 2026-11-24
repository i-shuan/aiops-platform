"""跟公司 Grafana 講話的唯一出口：只用 config.yaml 的 grafana.url + 一把 Editor Token，不直連任何資料庫、Prometheus 或 ES。

  parse_webhook     解析 Grafana 告警 webhook（Contact Point 推進來的）：哪條規則、哪個系統、嚴重度、firing／resolved
  search_errors     POST /api/datasources/proxy/uid/<uid>/_msearch  經 Grafana 的 ES 資料源查錯誤日誌與 Trace 片段
                    （欄位名稱由 LogFields／TraceFields 決定，來自 config.yaml）
  search_timeline   同一個代理：用批號／載具這類關聯碼把跨系統的日誌撈回來（不限等級），給「關聯時間線」用
  search_error_traces／get_trace  GET /api/datasources/proxy/uid/<uid>/api/search、/api/traces/<id>
                    經 Grafana 的 Tempo 資料源找出錯誤 Trace、取回整條呼叫鏈
  upsert_annotation POST／PATCH /api/annotations                  每個事故一筆註記

格式以 tests/fixtures/grafana13/（Grafana 13.0.7 實際送出、實際回應的內容）為準。
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from src.grouping import Alert

log = logging.getLogger("aiops_lite.grafana")

RETRY_STATUS = {429, 502, 503, 504}
SEVERITIES = ("P0", "P1", "P2", "P3")
TRUE_VALUES = {"true", "1", "yes", "on"}
ERROR_LEVELS = ("ERROR", "error", "Error", "FATAL", "fatal", "CRITICAL", "critical")  # es.error_levels 的預設值
ERROR_STATUS = ("error", "ERROR", "Error", "STATUS_CODE_ERROR")                       # es.error_statuses 的預設值


@dataclass(frozen=True)
class LabelNames:
    """告警規則上的標籤名稱（config.yaml 的 labels）。default_severity：嚴重度標籤沒寫或寫錯時當成哪一級。"""
    detect: str = "aiops_detect"
    severity: str = "aiops_severity"
    service: str = "service"
    default_severity: str = "P3"


@dataclass(frozen=True)
class LogFields:
    """日誌文件的欄位名稱（config.yaml 的 es.log_fields）；巢狀物件與帶點的扁平鍵都讀得到。"""
    time: str = "@timestamp"
    service: str = "service.name"
    level: str = "log.level"   # "" = 日誌沒有等級欄位：改用訊息文字裡的等級字樣挑錯誤日誌（見 Grafana.search_errors）
    message: str = "message"


@dataclass(frozen=True)
class TraceFields:
    """Trace／span 文件的欄位名稱（config.yaml 的 es.trace_fields）。"""
    time: str = "@timestamp"
    service: str = "service.name"
    span: str = "span.name"
    trace_id: str = "trace.id"
    status: str = "status.code"
    error: str = "error.message"


class GrafanaError(Exception):
    """Grafana 連不上、Token 沒權限、回應不是預期的格式。"""


class WebhookError(ValueError):
    """推進來的內容不是 Grafana 告警 webhook 的格式：整包拒收（HTTP 400）。"""


def _ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


def _when(raw: Any) -> datetime | None:
    """Grafana 的時間（RFC3339，可能帶奈秒）；還沒結束的告警 endsAt 是 0001-01-01。"""
    text = str(raw or "")
    if not text or text.startswith("0001-"):
        return None
    # 小數秒最多留 6 位（Grafana 給奈秒，舊版 Python 的 fromisoformat 不吃）
    return datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", text.replace("Z", "+00:00"))).astimezone(timezone.utc)


def _traceql_regex(name: str) -> str:
    """服務名稱放進 TraceQL 的 =~ "…" 裡：正規表示式的特殊字元要跳脫，字串裡的反斜線與引號也要跳脫。"""
    escaped = re.sub(r'([.^$*+?()\[\]{}|\\])', r'\\\1', name)
    return escaped.replace("\\", "\\\\").replace('"', '\\"')


def parse_webhook(payload: Any, names: LabelNames = LabelNames()) -> tuple[list[Alert], list[str]]:
    """回傳 (納管的告警, 問題清單)。只收貼了 aiops_detect=true 的告警；少了 service 標籤的略過並列出來。
    標籤名稱由 names 決定（預設 aiops_detect／aiops_severity／service）。
    格式不對（不是物件、沒有 alerts、告警缺 fingerprint／status／startsAt）丟 WebhookError，整包不收。"""
    if not isinstance(payload, dict) or not isinstance(payload.get("alerts"), list):
        raise WebhookError("不是 Grafana 告警 webhook：要有 alerts 陣列")
    alerts, problems = [], []
    for item in payload["alerts"]:
        if not isinstance(item, dict) or not isinstance(item.get("labels"), dict):
            raise WebhookError("alerts 裡的每一筆都要是帶 labels 的物件")
        labels = item["labels"]
        title = str(labels.get("alertname", ""))
        status, fingerprint, starts = item.get("status"), str(item.get("fingerprint") or ""), _when(item.get("startsAt"))
        if status not in ("firing", "resolved") or not fingerprint or starts is None:
            raise WebhookError(f"告警「{title}」缺 status／fingerprint／startsAt")
        if str(labels.get(names.detect, "")).strip().lower() not in TRUE_VALUES:
            continue
        service = str(labels.get(names.service, "")).strip()
        if not service:
            problems.append(f"告警「{title}」沒有 {names.service} 標籤：略過")
            continue
        severity = str(labels.get(names.severity, "")).strip().upper()
        if severity not in SEVERITIES:
            problems.append(f"告警「{title}」的 {names.severity}={labels.get(names.severity)!r} 不是 P0～P3："
                            f"當成 {names.default_severity}")
            severity = names.default_severity
        values = item.get("values") if isinstance(item.get("values"), dict) else {}
        alerts.append(Alert(fingerprint=fingerprint, status=status, rule_uid=str(item.get("ruleUID") or ""),
                            title=title, service=service, severity=severity, starts_at=starts,
                            ends_at=_when(item.get("endsAt")) if status == "resolved" else None, values=values))
    return alerts, problems


class Grafana:
    def __init__(self, url: str, token: str, transport: httpx.BaseTransport | None = None, timeout: float = 30.0,
                 retries: int = 2, retry_backoff_seconds: float = 0.5):
        self.url = url.rstrip("/")
        self.retries, self.retry_backoff_seconds = retries, retry_backoff_seconds
        self._http = httpx.Client(base_url=self.url, timeout=timeout, transport=transport,
                                  headers={"Authorization": f"Bearer {token}"})

    def close(self) -> None:
        self._http.close()

    def _request(self, method: str, path: str, *, accept: tuple[int, ...] = (), with_status: bool = False,
                 empty_ok: bool = False, **kw: Any) -> Any:
        """短暫抖動（連線失敗、逾時、429／502／503／504）重試 retries 次（預設 2），每次多等 retry_backoff_seconds；
        其他錯誤直接丟 GrafanaError。"""
        last = ""
        for attempt in range(self.retries + 1):
            if attempt:
                time.sleep(self.retry_backoff_seconds * attempt)
            try:
                r = self._http.request(method, path, **kw)
            except httpx.TransportError as e:
                last = f"{type(e).__name__}: {e}"
                continue
            if r.status_code in RETRY_STATUS:
                last = f"HTTP {r.status_code}"
                continue
            if r.status_code >= 400 and r.status_code not in accept:
                raise GrafanaError(f"{method} {path} -> HTTP {r.status_code}: {r.text[:300]}")
            try:
                data = None if empty_ok and not r.content.strip() else r.json()  # Tempo 的 404 沒有內容
            except ValueError as e:
                raise GrafanaError(f"{method} {path} 回的不是 JSON：{r.text[:200]}") from e
            return (r.status_code, data) if with_status else data
        raise GrafanaError(f"{method} {path} 重試後仍失敗：{last}")

    # ── 日誌與 Trace（經 Grafana 的 ES 資料源代理）──

    def search_errors(self, datasource_uid: str, log_index: str, trace_index: str | None, services: list[str],
                      start: datetime, end: datetime, size: int = 100, *, log_fields: LogFields = LogFields(),
                      trace_fields: TraceFields = TraceFields(), error_levels: tuple[str, ...] = ERROR_LEVELS,
                      error_statuses: tuple[str, ...] = ERROR_STATUS) -> dict[str, list[dict[str, Any]]]:
        """撈時間窗內這些系統的錯誤日誌與錯誤 span，回傳 {"logs": [...], "traces": [...]}（各是 ES 的 _source）。

        log_fields.level 是空字串 = 日誌沒有獨立的等級欄位（例如沒拆欄位的容器標準輸出）：改成在訊息欄位做全文比對，
        訊息裡有 error_levels 任何一個字樣就算錯誤日誌。這是比對「詞」，不是比對等級，所以會混入不是錯誤的紀錄
        （INFO 的訊息裡寫到 error 這個詞也會中）；訊息欄位必須是 text 型別（keyword 型別比不到，結果是 0 筆）。"""
        def window(field: str) -> dict[str, Any]:
            return {"range": {field: {"gte": _ms(start), "lte": _ms(end), "format": "epoch_millis"}}}

        def either(name: str, values: Any) -> dict[str, Any]:
            values = list(values)
            # 欄位是 keyword 或 text（動態 mapping 會多一個 .keyword）都查得到
            return {"bool": {"minimum_should_match": 1, "should": [
                {"terms": {name: values}}, {"terms": {f"{name}.keyword": values}}]}}

        def search(index: str, time_field: str, service_field: str, extra: dict[str, Any]) -> str:
            query = {"bool": {"filter": [window(time_field), either(service_field, services), extra]}}
            return (json.dumps({"index": index, "ignore_unavailable": True}) + "\n"
                    + json.dumps({"size": size, "sort": [{time_field: "desc"}], "query": query}) + "\n")

        if log_fields.level:
            is_error = either(log_fields.level, error_levels)
        else:  # 全文比對不分大小寫：ERROR／error／Error 只送一次
            words = " ".join(dict.fromkeys(level.lower() for level in error_levels))
            is_error = {"match": {log_fields.message: {"query": words, "operator": "or"}}}
        kinds = [("logs", log_index, search(log_index, log_fields.time, log_fields.service, is_error))]
        if trace_index:
            kinds.append(("traces", trace_index, search(trace_index, trace_fields.time, trace_fields.service,
                                                        either(trace_fields.status, error_statuses))))
        raw = self._request("POST", f"/api/datasources/proxy/uid/{datasource_uid}/_msearch",
                            content="".join(body for _k, _index, body in kinds),
                            headers={"Content-Type": "application/x-ndjson"})
        responses = raw.get("responses") if isinstance(raw, dict) else None
        if not isinstance(responses, list) or len(responses) != len(kinds):
            raise GrafanaError("ES _msearch 的回應筆數跟請求對不上")
        out: dict[str, list[dict[str, Any]]] = {"logs": [], "traces": []}
        for (kind, index, _body), resp in zip(kinds, responses):
            if resp.get("error"):
                raise GrafanaError(f"ES 查詢 {index} 失敗：{json.dumps(resp['error'], ensure_ascii=False)[:300]}")
            out[kind] = [h.get("_source") or {} for h in (resp.get("hits") or {}).get("hits") or []]
        return out

    def search_timeline(self, datasource_uid: str, index: str, field: str, values: list[str], start: datetime,
                        end: datetime, size: int, *, log_fields: LogFields = LogFields()) -> dict[str, dict[str, Any]]:
        """每個關聯碼（批號、載具…）各查一次：時間窗內帶這個值的所有日誌，**不限等級、不限系統**，由舊到新。
        回傳 {值: {"hits": [ES 的 _source…], "total": 總筆數}}；一次 _msearch 送完。"""
        if not values:
            return {}
        window = {"range": {log_fields.time: {"gte": _ms(start), "lte": _ms(end), "format": "epoch_millis"}}}
        body = ""
        for value in values:
            same = {"bool": {"minimum_should_match": 1, "should": [
                {"terms": {field: [value]}}, {"terms": {f"{field}.keyword": [value]}}]}}
            body += (json.dumps({"index": index, "ignore_unavailable": True}) + "\n" + json.dumps({
                "size": size, "track_total_hits": True, "sort": [{log_fields.time: "asc"}],
                "query": {"bool": {"filter": [window, same]}}}) + "\n")
        raw = self._request("POST", f"/api/datasources/proxy/uid/{datasource_uid}/_msearch", content=body,
                            headers={"Content-Type": "application/x-ndjson"})
        responses = raw.get("responses") if isinstance(raw, dict) else None
        if not isinstance(responses, list) or len(responses) != len(values):
            raise GrafanaError("ES _msearch（關聯時間線）的回應筆數跟請求對不上")
        out: dict[str, dict[str, Any]] = {}
        for value, resp in zip(values, responses):
            if resp.get("error"):
                raise GrafanaError(f"ES 查詢 {field}={value} 失敗：{json.dumps(resp['error'], ensure_ascii=False)[:300]}")
            hits = resp.get("hits") or {}
            total = (hits.get("total") or {}).get("value", len(hits.get("hits") or []))
            out[value] = {"hits": [h.get("_source") or {} for h in hits.get("hits") or []], "total": int(total)}
        return out

    # ── Trace（經 Grafana 的 Tempo 資料源代理）──

    def search_error_traces(self, datasource_uid: str, services: list[str], start: datetime, end: datetime,
                            limit: int) -> list[dict[str, Any]]:
        """找時間窗內、這些服務參與、而且有錯誤 span 的 Trace（TraceQL），回傳 Tempo 的搜尋結果（traceID、rootServiceName…）。
        注意：Tempo 收到 Trace 之後要過幾秒才搜尋得到（實測 2.9.0 八秒內搜不到）；debounce 的等待剛好涵蓋這段。"""
        names = "|".join(_traceql_regex(s) for s in services)
        query = '{ resource.service.name =~ "' + names + '" && status = error }'
        raw = self._request("GET", f"/api/datasources/proxy/uid/{datasource_uid}/api/search", params={
            "q": query, "start": int(start.timestamp()), "end": int(end.timestamp()) + 1, "limit": limit})
        traces = raw.get("traces") if isinstance(raw, dict) else None
        if not isinstance(traces, list):
            raise GrafanaError(f"Tempo 搜尋的回應沒有 traces：{str(raw)[:200]}")
        return traces

    def get_trace(self, datasource_uid: str, trace_id: str) -> dict[str, Any] | None:
        """取回整條 Trace（OTLP JSON：batches → scopeSpans → spans）。Tempo 已經沒有這條（過了保留期限）回 None。"""
        status, raw = self._request("GET", f"/api/datasources/proxy/uid/{datasource_uid}/api/traces/{trace_id}",
                                    accept=(404,), with_status=True, empty_ok=True)
        return None if status == 404 or not isinstance(raw, dict) else raw

    # ── 註記 ──

    def upsert_annotation(self, annotation_id: int | None, uid_tag: str, body: dict[str, Any]) -> int:
        """建立或更新一筆註記，回傳它的 id。沒有 id 時先用事故標籤找（上一輪建立成功、但 id 沒存到的情況不會重複建）。

        標籤有變（open→closed、嚴重度升級、新系統加入）就**刪掉舊的、建一筆新的**，不用 PATCH：Grafana 13.0.7 實測，
        更新註記的標籤之後，用「舊標籤」查還是查得到它（標籤索引沒有跟著更新），儀表板的「進行中」清單會一直列著已結案的事故。
        只有文字、時間變（標籤相同）才原地 PATCH。"""
        if annotation_id is None:
            found = self._request("GET", "/api/annotations", params={"tags": uid_tag, "limit": 1})
            if isinstance(found, list) and found:
                annotation_id = int(found[0]["id"])
        if annotation_id is not None:
            status, current = self._request("GET", f"/api/annotations/{annotation_id}", accept=(403, 404), with_status=True)
            if status in (403, 404):
                annotation_id = None  # 被人刪掉了（Grafana 對不存在的 id 有時回 403，有時回 404）：重建；真的沒權限的話下面建立時會報錯
            elif set(current.get("tags") or []) == set(body["tags"]):
                self._request("PATCH", f"/api/annotations/{annotation_id}", json=body)
                return annotation_id
            else:
                self._request("DELETE", f"/api/annotations/{annotation_id}", accept=(403, 404))
        return int(self._request("POST", "/api/annotations", json=body)["id"])
