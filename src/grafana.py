"""跟公司 Grafana 講話的唯一出口：只用 config.yaml 的 grafana.url + 一把 Editor Token，不直連任何資料庫或 Victoria 元件。

  parse_webhook     解析 Grafana 告警 webhook（Contact Point 推進來的）：哪條規則、哪個系統、嚴重度、firing／resolved
  日誌（VictoriaLogs）  POST /api/datasources/proxy/uid/<uid>/select/logsql/query，查詢語言是 LogsQL
      search_errors     時間窗內這些系統的錯誤日誌，最新的在前
      error_profile     每個系統各自最早的幾筆錯誤，加上這些系統的警告
      count_phrases     幾種錯誤在事故期間與事故前各出現幾筆（一次請求）
      count_logs        錯誤日誌是 0 筆時：整段時間有幾筆、這些系統有幾筆
      search_timeline   用批號／載具這類關聯碼把跨系統的日誌撈回來（不限等級）
  Trace（VictoriaTraces）  GET /api/datasources/proxy/uid/<uid>/api/traces…，Jaeger 相容的查詢介面
      search_error_traces／get_trace   找出有錯誤的 Trace、用編號取回一條
  指標（VictoriaMetrics）  GET /api/datasources/proxy/uid/<uid>/api/v1/query_range，Prometheus 相容
      query_range
  upsert_annotation POST／PATCH /api/annotations                  每個事故一筆註記

Grafana 上要有三個資料源：VictoriaLogs（外掛 victoriametrics-logs-datasource）、Jaeger 型別指到 VictoriaTraces 的
/select/jaeger、Prometheus 型別指到 VictoriaMetrics。格式以 tests/fixtures/grafana13/ 為準：Grafana 13.0.7 實際送出與回應的內容，
其中 Victoria 的部分是 VictoriaLogs v1.17.0、VictoriaTraces v0.5.0、VictoriaMetrics v1.110.0 經 Grafana 代理實際回的。
"""
from __future__ import annotations

import json
import logging
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from urllib.parse import quote

import httpx

from src.grouping import Alert

log = logging.getLogger("aiops_lite.grafana")

RETRY_STATUS = {429, 502, 503, 504}
SEVERITIES = ("P0", "P1", "P2", "P3")
TRUE_VALUES = {"true", "1", "yes", "on"}
WARN_LEVELS = ("WARN", "warn", "Warn", "WARNING", "warning", "Warning")  # logs.warn_levels 的預設值
ERROR_LEVELS = ("ERROR", "error", "Error", "FATAL", "fatal", "CRITICAL", "critical")  # logs.error_levels 的預設值


@dataclass(frozen=True)
class LabelNames:
    """告警規則上的標籤名稱（config.yaml 的 labels）。default_severity：嚴重度標籤沒寫或寫錯時當成哪一級。"""
    detect: str = "aiops_detect"
    severity: str = "aiops_severity"
    service: str = "service"
    default_severity: str = "P3"


TIME_FIELD, MESSAGE_FIELD = "_time", "_msg"   # VictoriaLogs 固定的欄位名稱：時間與訊息本文（寫入時用 _time_field／_msg_field 對應）


@dataclass(frozen=True)
class LogFields:
    """日誌的欄位名稱（config.yaml 的 logs.fields）。時間與訊息在 VictoriaLogs 固定是 _time、_msg，不用設定。"""
    service: str = "service.name"
    level: str = "log.level"   # "" = 日誌沒有等級欄位：改用訊息文字裡的等級字樣挑錯誤日誌（見 Grafana.search_errors）


def _q(value: Any) -> str:
    """把欄位名稱或值寫成 LogsQL 的雙引號字串（引號、反斜線、換行都跳脫）。系統名稱來自告警標籤，是外部輸入。"""
    return json.dumps(str(value), ensure_ascii=False)


def _in(field: str, values: Any) -> str:
    """欄位的值是這幾個其中之一（完全相等）。"""
    return f"{_q(field)}:in({','.join(_q(v) for v in values)})"


def _has_level(log_fields: LogFields, levels: tuple[str, ...]) -> str:
    """「這筆日誌是這些等級」的條件。有等級欄位就比等級；沒有（level 是空字串）就在訊息文字裡找等級字樣（不分大小寫）。"""
    if log_fields.level:
        return _in(log_fields.level, levels)
    words = dict.fromkeys(level.lower() for level in levels)   # ERROR／error／Error 只留一個
    return "(" + " or ".join(f"{MESSAGE_FIELD}:i({_q(word)})" for word in words) + ")"


def _seq(words: list[str]) -> str:
    """訊息裡照順序出現這些詞（中間可以隔別的字）。"""
    return f"{MESSAGE_FIELD}:seq({','.join(_q(w) for w in words)})"


def _rfc3339(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class GrafanaError(Exception):
    """Grafana 連不上、Token 沒權限、回應不是預期的格式。"""


class WebhookError(ValueError):
    """推進來的內容不是 Grafana 告警 webhook 的格式：整包拒收（HTTP 400）。"""


def _when(raw: Any) -> datetime | None:
    """Grafana 的時間（RFC3339，可能帶奈秒）；還沒結束的告警 endsAt 是 0001-01-01。"""
    text = str(raw or "")
    if not text or text.startswith("0001-"):
        return None
    # 小數秒最多留 6 位（Grafana 給奈秒，舊版 Python 的 fromisoformat 不吃）
    return datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", text.replace("Z", "+00:00"))).astimezone(timezone.utc)


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
                 empty_ok: bool = False, raw: bool = False, **kw: Any) -> Any:
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
                # Victoria 的錯誤訊息前面有一段 remoteAddr 與整條網址（requestURI），真正的原因在最後一段
                reason = r.text.split("; ", 2)[-1] if r.text.startswith("remoteAddr:") else r.text
                raise GrafanaError(f"{method} {path} -> HTTP {r.status_code}: {reason.strip()[:300]}")
            if raw:   # VictoriaLogs 一行回一個 JSON 物件，不是一份 JSON
                return r.text
            try:
                data = None if empty_ok and not r.content.strip() else r.json()
            except ValueError as e:
                raise GrafanaError(f"{method} {path} 回的不是 JSON：{r.text[:200]}") from e
            return (r.status_code, data) if with_status else data
        raise GrafanaError(f"{method} {path} 重試後仍失敗：{last}")

    # ── 日誌（經 Grafana 的 VictoriaLogs 資料源代理；查詢語言是 LogsQL）──

    def _logsql(self, datasource_uid: str, query: str, start: datetime, end: datetime, *, extra_filter: str = "",
                limit: int | None = None) -> list[dict[str, Any]]:
        """送一條 LogsQL，回傳每一列（VictoriaLogs 一行回一個 JSON 物件）。start、end 是查詢的時間範圍。
        extra_filter 是 config.yaml 的 logs.extra_filter：加在每條查詢最前面的條件（例如只看某個 stream 或命名空間）。"""
        full = f"{extra_filter} {query}".strip()
        data = {"query": full, "start": _rfc3339(start), "end": _rfc3339(end), **({"limit": limit} if limit is not None else {})}
        text = self._request("POST", f"/api/datasources/proxy/uid/{datasource_uid}/select/logsql/query", data=data, raw=True)
        rows = []
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError as e:
                raise GrafanaError(f"VictoriaLogs 回的不是一行一個 JSON：{line[:200]}") from e
            if not isinstance(row, dict):
                raise GrafanaError(f"VictoriaLogs 回的每一行要是物件：{line[:200]}")
            rows.append(row)
        return rows

    def search_errors(self, datasource_uid: str, services: list[str], start: datetime, end: datetime, size: int = 100, *,
                      log_fields: LogFields = LogFields(), error_levels: tuple[str, ...] = ERROR_LEVELS,
                      extra_filter: str = "") -> list[dict[str, Any]]:
        """撈時間窗內這些系統的錯誤日誌，最新的在前，最多 size 筆。每筆是 VictoriaLogs 的一列（欄位攤平成一層，
        例如 "service.name"、"_time"、"_msg"）。

        log_fields.level 是空字串 = 日誌沒有獨立的等級欄位（例如沒拆欄位的容器標準輸出）：改成在訊息文字裡找
        error_levels 的字樣（不分大小寫，比對完整的詞）。這是比對「詞」，不是比對等級，所以會混入不是錯誤的紀錄
        （INFO 的訊息裡寫到 error 這個詞也會中）。"""
        query = f"{_in(log_fields.service, services)} {_has_level(log_fields, error_levels)} | sort by ({TIME_FIELD}) desc | limit {size}"
        return self._logsql(datasource_uid, query, start, end, extra_filter=extra_filter)

    def count_logs(self, datasource_uid: str, services: list[str], start: datetime, end: datetime, *,
                   log_fields: LogFields = LogFields(), extra_filter: str = "") -> dict[str, int]:
        """錯誤日誌撈到 0 筆的時候用：同一個時間窗裡總共有幾筆（"all"）、其中屬於這些系統的有幾筆（"services"，不限等級）。
        三個數字放在一起就知道是哪一關沒過：這段時間沒資料、系統名稱對不上、還是等級對不上（或真的沒有錯誤）。"""
        query = f"* | stats count() as total, count() if ({_in(log_fields.service, services)}) as services"
        rows = self._logsql(datasource_uid, query, start, end, extra_filter=extra_filter)
        row = rows[0] if rows else {}
        return {"all": int(row.get("total", 0)), "services": int(row.get("services", 0))}

    def error_profile(self, datasource_uid: str, services: list[str], start: datetime, end: datetime, *,
                      log_fields: LogFields = LogFields(), error_levels: tuple[str, ...] = ERROR_LEVELS,
                      warn_levels: tuple[str, ...] = (), first_n: int = 3, warn_n: int = 50, extra_filter: str = ""
                      ) -> dict[str, Any]:
        """每個系統各自最早的幾筆錯誤日誌（由舊到新），另外撈這些系統的警告（由舊到新）。

        為什麼要另外問：search_errors 是全部系統一起、從最新的往回取。某個系統大量報錯時，其他系統較早、較少的錯誤會整個
        被擠掉。這裡用 partition by 讓每個系統各自排序、各取最早的 first_n 筆，不會互相擠。
        回傳 {"services": {系統: [文件…]}, "warnings": [文件…]}；沒有錯誤的系統是空清單。warn_levels 是空的就不問警告。"""
        base = _in(log_fields.service, services)
        firsts = self._logsql(datasource_uid, f"{base} {_has_level(log_fields, error_levels)} | sort by ({TIME_FIELD}) "
                                              f"partition by ({_q(log_fields.service)}) limit {first_n}", start, end,
                              extra_filter=extra_filter)
        out: dict[str, Any] = {"services": {service: [] for service in services}, "warnings": []}
        for row in firsts:
            out["services"].setdefault(str(row.get(log_fields.service, "")), []).append(row)
        if warn_levels:
            out["warnings"] = self._logsql(datasource_uid, f"{base} {_has_level(log_fields, warn_levels)} | sort by ({TIME_FIELD}) "
                                                           f"| limit {warn_n}", start, end, extra_filter=extra_filter)
        return out

    def count_phrases(self, datasource_uid: str, items: list[tuple[str, list[str], tuple[str, ...]]], start: datetime,
                      end: datetime, before_start: datetime, *, log_fields: LogFields = LogFields(), extra_filter: str = ""
                      ) -> list[tuple[int, int]]:
        """每個 (系統, 照順序出現的詞, 等級) 各數兩次：在 [start, end]（事故期間）與 [before_start, start)（事故前）出現幾筆。
        全部寫成同一條查詢的 stats count() if (…)，一次請求。「照順序出現的詞」是樣板裡固定不變的詞（seq）：
        別種訊息剛好有同樣的詞、順序也一樣時會被算在一起。回傳 [(事故期間, 事故前), …]，順序跟 items 一樣。"""
        if not items:
            return []
        boundary, parts = _rfc3339(start), []
        for n, (service, words, levels) in enumerate(items):
            match = f"{_q(log_fields.service)}:={_q(service)} {_has_level(log_fields, levels)} {_seq(words)}"
            parts.append(f"count() if ({match} {TIME_FIELD}:>={boundary}) as d{n}")
            parts.append(f"count() if ({match} {TIME_FIELD}:<{boundary}) as b{n}")
        services = list(dict.fromkeys(service for service, _w, _l in items))
        rows = self._logsql(datasource_uid, f"{_in(log_fields.service, services)} | stats {', '.join(parts)}", before_start, end,
                            extra_filter=extra_filter)
        row = rows[0] if rows else {}
        return [(int(row.get(f"d{n}", 0)), int(row.get(f"b{n}", 0))) for n in range(len(items))]

    def search_timeline(self, datasource_uid: str, field: str, values: list[str], start: datetime, end: datetime, size: int, *,
                        extra_filter: str = "") -> dict[str, dict[str, Any]]:
        """每個關聯碼（批號、載具…）：時間窗內帶這個值的所有日誌，**不限等級、不限系統**，由舊到新各取 size 筆，另外數總筆數。
        回傳 {值: {"hits": [文件…], "total": 總筆數}}；兩次請求（內容、筆數）。"""
        if not values:
            return {}
        base = _in(field, values)
        rows = self._logsql(datasource_uid, f"{base} | sort by ({TIME_FIELD}) partition by ({_q(field)}) limit {size}", start, end,
                            extra_filter=extra_filter)
        totals = self._logsql(datasource_uid, f"{base} | stats by ({_q(field)}) count() as total", start, end, extra_filter=extra_filter)
        out: dict[str, dict[str, Any]] = {value: {"hits": [], "total": 0} for value in values}
        for row in rows:
            out.setdefault(str(row.get(field, "")), {"hits": [], "total": 0})["hits"].append(row)
        for row in totals:
            out.setdefault(str(row.get(field, "")), {"hits": [], "total": 0})["total"] = int(row.get("total", 0))
        return out

    # ── 指標（經 Grafana 的 Prometheus 相容資料源代理：Prometheus、VictoriaMetrics、Mimir…）──

    def query_range(self, datasource_uid: str, expr: str, start: datetime, end: datetime, step_seconds: int
                    ) -> list[dict[str, Any]]:
        """區間查詢（Prometheus HTTP API 的 /api/v1/query_range）。回傳每條序列 {"labels": {...}, "points": [(時間, 數值)]}；
        NaN 與無限大的點丟掉（除以零的查詢會回這種值，算不了統計）。查詢語法錯、資料源不對丟 GrafanaError。"""
        raw = self._request("GET", f"/api/datasources/proxy/uid/{datasource_uid}/api/v1/query_range", params={
            "query": expr, "start": int(start.timestamp()), "end": int(end.timestamp()), "step": step_seconds})
        data = raw.get("data") if isinstance(raw, dict) else None
        if not isinstance(data, dict) or raw.get("status") != "success" or not isinstance(data.get("result"), list):
            raise GrafanaError(f"指標查詢的回應不是預期的格式：{str(raw)[:200]}")
        if data.get("resultType") != "matrix":
            raise GrafanaError(f"指標查詢要回時間序列（matrix），這條回的是 {data.get('resultType')}")
        series = []
        for item in data["result"]:
            points = []
            for ts, value in item.get("values") or []:
                number = float(value)
                if math.isfinite(number):
                    points.append((datetime.fromtimestamp(float(ts), timezone.utc), number))
            series.append({"labels": dict(item.get("metric") or {}), "points": points})
        return series

    # ── Trace（經 Grafana 的 Jaeger 資料源代理，後面是 VictoriaTraces 的 /select/jaeger）──

    def _jaeger(self, datasource_uid: str, path: str, **kw: Any) -> tuple[int, list[dict[str, Any]]]:
        status, raw = self._request("GET", f"/api/datasources/proxy/uid/{datasource_uid}{path}", accept=(404,),
                                    with_status=True, **kw)
        data = raw.get("data") if isinstance(raw, dict) else None
        if status != 404 and not isinstance(data, list):
            raise GrafanaError(f"Trace 查詢的回應沒有 data：{str(raw)[:200]}")
        return status, [t for t in data or [] if isinstance(t, dict)]

    def search_error_traces(self, datasource_uid: str, services: list[str], start: datetime, end: datetime,
                            limit: int) -> list[dict[str, Any]]:
        """找時間窗內、這些服務參與、而且有錯誤 span 的 Trace。Jaeger 的搜尋一次只能指定一個服務，所以每個服務問一次，
        同一條 Trace 只留一份。回傳的每一條已經是完整的 Trace（spans、processes），不用再另外取。
        VictoriaTraces 收到 Trace 後幾秒內就搜尋得到（實測 v0.5.0 三秒）。"""
        found: dict[str, dict[str, Any]] = {}
        for service in services:
            _status, traces = self._jaeger(datasource_uid, "/api/traces", params={
                "service": service, "tags": json.dumps({"error": "true"}), "limit": limit,
                "start": int(start.timestamp() * 1_000_000), "end": int(end.timestamp() * 1_000_000) + 1_000_000})
            for item in traces:
                found.setdefault(str(item.get("traceID") or ""), item)
        found.pop("", None)
        return list(found.values())

    def get_trace(self, datasource_uid: str, trace_id: str) -> dict[str, Any] | None:
        """用編號取回整條 Trace（Jaeger 格式：spans、processes）。找不到（過了保留期限、編號不對）回 None。"""
        status, traces = self._jaeger(datasource_uid, f"/api/traces/{quote(trace_id, safe='')}")
        return None if status == 404 or not traces else traces[0]

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
