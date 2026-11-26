"""日誌的兩種來源，對外是同一組方法、回同一種形狀。都只經 Grafana 的資料源代理（src/grafana.py 的 Grafana.request）。

  VictoriaLogs    POST /api/datasources/proxy/uid/<uid>/select/logsql/query   查詢語言 LogsQL
  Elasticsearch   POST /api/datasources/proxy/uid/<uid>/_msearch              查詢語言 Query DSL

config.yaml 的 logs.type 決定用哪一個（main.py 的 log_store）。分析的程式（src/rca.py）不知道後面是誰：

  search_errors     時間窗內這些系統的錯誤日誌，最新的在前
  error_profile     每個系統各自最早的幾筆錯誤，加上這些系統的警告
  count_phrases     幾種錯誤在事故期間與事故前各出現幾筆
  count_logs        錯誤日誌是 0 筆時：整段時間有幾筆、這些系統有幾筆
  search_timeline   用批號／載具這類關聯碼把跨系統的日誌撈回來（不限等級）

每一筆日誌都是「一層的字典」：時間在 "_time"（RFC3339 字串）、訊息本文在 "_msg"，其他欄位用帶點的鍵（"service.name"）。
這是 VictoriaLogs 原本回的樣子；Elasticsearch 的巢狀文件由 _row() 攤平成同一種。
格式以 tests/fixtures/grafana13/ 的 victoria_logs.json、elasticsearch.json 為準（經 Grafana 13.0.7 實際錄下來的）。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.grafana import Grafana, GrafanaError

WARN_LEVELS = ("WARN", "warn", "Warn", "WARNING", "warning", "Warning")  # logs.warn_levels 的預設值
ERROR_LEVELS = ("ERROR", "error", "Error", "FATAL", "fatal", "CRITICAL", "critical")  # logs.error_levels 的預設值
TIME_FIELD, MESSAGE_FIELD = "_time", "_msg"   # 每一筆日誌固定有的兩個鍵（不管來源是哪一種）


@dataclass(frozen=True)
class LogFields:
    """日誌的欄位名稱（config.yaml 的 logs.fields）。time、message 只有 Elasticsearch 用得到：
    VictoriaLogs 的時間與訊息固定是 _time、_msg（寫入時用 _time_field／_msg_field 對應）。"""
    service: str = "service.name"
    level: str = "log.level"   # "" = 日誌沒有等級欄位：改用訊息文字裡的等級字樣挑錯誤日誌
    time: str = "@timestamp"
    message: str = "message"


def _level_words(levels: tuple[str, ...]) -> list[str]:
    """沒有等級欄位時要在訊息裡找的字樣：ERROR／error／Error 只留一個（比對不分大小寫）。"""
    return list(dict.fromkeys(level.lower() for level in levels))


# ── VictoriaLogs ──

def _q(value: Any) -> str:
    """把欄位名稱或值寫成 LogsQL 的雙引號字串（引號、反斜線、換行都跳脫）。系統名稱來自告警標籤，是外部輸入。"""
    return json.dumps(str(value), ensure_ascii=False)


def _in(field: str, values: Any) -> str:
    return f"{_q(field)}:in({','.join(_q(v) for v in values)})"


def _rfc3339(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class VictoriaLogs:
    name = "VictoriaLogs"
    scope_hint = "檢查日誌有沒有在送進 VictoriaLogs（有設 logs.extra_filter 的話也檢查它）"   # 「一筆都沒有」時給人的下一步

    def __init__(self, grafana: Grafana, datasource_uid: str, fields: LogFields = LogFields(), extra_filter: str = ""):
        """extra_filter 是 config.yaml 的 logs.extra_filter：加在每條查詢最前面的 LogsQL 條件（例如只看某個命名空間）。"""
        self.grafana, self.uid, self.fields, self.extra_filter = grafana, datasource_uid, fields, extra_filter

    def _query(self, query: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        """送一條 LogsQL，回傳每一列（VictoriaLogs 一行回一個 JSON 物件）。"""
        data = {"query": f"{self.extra_filter} {query}".strip(), "start": _rfc3339(start), "end": _rfc3339(end)}
        text = self.grafana.request("POST", f"/api/datasources/proxy/uid/{self.uid}/select/logsql/query", data=data, raw=True)
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

    def _has_level(self, levels: tuple[str, ...]) -> str:
        """「這筆日誌是這些等級」。有等級欄位就比等級；沒有就在訊息文字裡找等級字樣（完整的詞、不分大小寫）。"""
        if self.fields.level:
            return _in(self.fields.level, levels)
        return "(" + " or ".join(f"{MESSAGE_FIELD}:i({_q(word)})" for word in _level_words(levels)) + ")"

    def search_errors(self, services: list[str], start: datetime, end: datetime, size: int = 100, *,
                      error_levels: tuple[str, ...] = ERROR_LEVELS) -> list[dict[str, Any]]:
        """時間窗內這些系統的錯誤日誌，最新的在前，最多 size 筆。
        沒有等級欄位（fields.level 是空字串）時改成在訊息文字裡找 error_levels 的字樣：這是比對「詞」，不是比對等級，
        會混入不是錯誤的紀錄（INFO 的訊息裡寫到 error 這個詞也會中）。"""
        return self._query(f"{_in(self.fields.service, services)} {self._has_level(error_levels)} "
                           f"| sort by ({TIME_FIELD}) desc | limit {size}", start, end)

    def count_logs(self, services: list[str], start: datetime, end: datetime) -> dict[str, int]:
        """錯誤日誌撈到 0 筆的時候用：同一個時間窗裡總共有幾筆（"all"）、其中屬於這些系統的有幾筆（"services"，不限等級）。
        三個數字放在一起就知道是哪一關沒過：這段時間沒資料、系統名稱對不上、還是等級對不上（或真的沒有錯誤）。"""
        rows = self._query(f"* | stats count() as total, count() if ({_in(self.fields.service, services)}) as services", start, end)
        row = rows[0] if rows else {}
        return {"all": int(row.get("total", 0)), "services": int(row.get("services", 0))}

    def error_profile(self, services: list[str], start: datetime, end: datetime, *, error_levels: tuple[str, ...] = ERROR_LEVELS,
                      warn_levels: tuple[str, ...] = (), first_n: int = 3, warn_n: int = 50) -> dict[str, Any]:
        """每個系統各自最早的幾筆錯誤日誌（由舊到新），另外撈這些系統的警告（由舊到新）。
        為什麼要另外問：search_errors 是全部系統一起、從最新的往回取。某個系統大量報錯時，其他系統較早、較少的錯誤會整個
        被擠掉。回傳 {"services": {系統: [日誌…]}, "warnings": [日誌…]}；沒有錯誤的系統是空清單。warn_levels 是空的就不問警告。"""
        base = _in(self.fields.service, services)
        firsts = self._query(f"{base} {self._has_level(error_levels)} | sort by ({TIME_FIELD}) "
                             f"partition by ({_q(self.fields.service)}) limit {first_n}", start, end)
        out: dict[str, Any] = {"services": {service: [] for service in services}, "warnings": []}
        for row in firsts:
            out["services"].setdefault(str(row.get(self.fields.service, "")), []).append(row)
        if warn_levels:
            out["warnings"] = self._query(f"{base} {self._has_level(warn_levels)} | sort by ({TIME_FIELD}) | limit {warn_n}", start, end)
        return out

    def count_phrases(self, items: list[tuple[str, list[str], tuple[str, ...]]], start: datetime, end: datetime,
                      before_start: datetime) -> list[tuple[int, int]]:
        """每個 (系統, 照順序出現的詞, 等級) 各數兩次：在 [start, end]（事故期間）與 [before_start, start)（事故前）出現幾筆。
        全部寫成同一條查詢的 stats count() if (…)，一次請求。詞用 seq 比對（照順序出現，中間可以隔別的字、分大小寫）：
        是近似，別種訊息剛好有同樣的詞、順序也一樣時會被算在一起。回傳 [(事故期間, 事故前), …]，順序跟 items 一樣。"""
        if not items:
            return []
        boundary, parts = _rfc3339(start), []
        for n, (service, words, levels) in enumerate(items):
            match = (f"{_q(self.fields.service)}:={_q(service)} {self._has_level(levels)} "
                     f"{MESSAGE_FIELD}:seq({','.join(_q(w) for w in words)})")
            parts.append(f"count() if ({match} {TIME_FIELD}:>={boundary}) as d{n}")
            parts.append(f"count() if ({match} {TIME_FIELD}:<{boundary}) as b{n}")
        services = list(dict.fromkeys(service for service, _w, _l in items))
        rows = self._query(f"{_in(self.fields.service, services)} | stats {', '.join(parts)}", before_start, end)
        row = rows[0] if rows else {}
        return [(int(row.get(f"d{n}", 0)), int(row.get(f"b{n}", 0))) for n in range(len(items))]

    def search_timeline(self, field: str, values: list[str], start: datetime, end: datetime, size: int) -> dict[str, dict[str, Any]]:
        """每個關聯碼（批號、載具…）：時間窗內帶這個值的所有日誌，**不限等級、不限系統**，由舊到新各取 size 筆，另外數總筆數。
        回傳 {值: {"hits": [日誌…], "total": 總筆數}}；兩次請求（內容、筆數）。"""
        if not values:
            return {}
        base = _in(field, values)
        rows = self._query(f"{base} | sort by ({TIME_FIELD}) partition by ({_q(field)}) limit {size}", start, end)
        totals = self._query(f"{base} | stats by ({_q(field)}) count() as total", start, end)
        out: dict[str, dict[str, Any]] = {value: {"hits": [], "total": 0} for value in values}
        for row in rows:
            out.setdefault(str(row.get(field, "")), {"hits": [], "total": 0})["hits"].append(row)
        for row in totals:
            out.setdefault(str(row.get(field, "")), {"hits": [], "total": 0})["total"] = int(row.get("total", 0))
        return out


# ── Elasticsearch ──

def _ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


def _either(name: str, values: Any) -> dict[str, Any]:
    """欄位是 keyword 或 text（動態 mapping 會多一個 .keyword）都查得到。"""
    values = list(values)
    return {"bool": {"minimum_should_match": 1, "should": [{"terms": {name: values}}, {"terms": {f"{name}.keyword": values}}]}}


def _flatten(doc: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in doc.items():
        if isinstance(value, dict):
            out.update(_flatten(value, f"{prefix}{key}."))
        else:
            out[f"{prefix}{key}"] = value
    return out


class Elasticsearch:
    name = "Elasticsearch"
    scope_hint = "檢查 logs.index，以及日誌有沒有在送進 Elasticsearch"

    def __init__(self, grafana: Grafana, datasource_uid: str, fields: LogFields = LogFields(), index: str = "*"):
        self.grafana, self.uid, self.fields, self.index = grafana, datasource_uid, fields, index

    def _row(self, hit: dict[str, Any]) -> dict[str, Any]:
        """ES 的一筆文件（巢狀）→ 跟 VictoriaLogs 同一種形狀：攤平成一層，時間與訊息另外放在 _time、_msg。"""
        row = _flatten(hit.get("_source") or {})
        return {**row, TIME_FIELD: str(row.get(self.fields.time, "")), MESSAGE_FIELD: str(row.get(self.fields.message, ""))}

    def _msearch(self, bodies: list[dict[str, Any]], what: str) -> list[dict[str, Any]]:
        """多個子查詢一次送出；任何一個子查詢失敗就整個丟 GrafanaError。"""
        header = json.dumps({"index": self.index, "ignore_unavailable": True})
        raw = self.grafana.request("POST", f"/api/datasources/proxy/uid/{self.uid}/_msearch",
                                   content="".join(f"{header}\n{json.dumps(body, ensure_ascii=False)}\n" for body in bodies).encode(),
                                   headers={"Content-Type": "application/x-ndjson"})
        responses = raw.get("responses") if isinstance(raw, dict) else None
        if not isinstance(responses, list) or len(responses) != len(bodies):
            raise GrafanaError("ES _msearch 的回應筆數跟請求對不上")
        for resp in responses:
            if resp.get("error"):
                raise GrafanaError(f"ES {what}失敗：{json.dumps(resp['error'], ensure_ascii=False)[:300]}")
        return responses

    @staticmethod
    def _total(resp: dict[str, Any]) -> int:
        return int(((resp.get("hits") or {}).get("total") or {}).get("value", 0))

    def _hits(self, resp: dict[str, Any]) -> list[dict[str, Any]]:
        return [self._row(h) for h in (resp.get("hits") or {}).get("hits") or []]

    def _window(self, start: datetime, end: datetime, *, end_exclusive: bool = False) -> dict[str, Any]:
        return {"range": {self.fields.time: {"gte": _ms(start), "lt" if end_exclusive else "lte": _ms(end), "format": "epoch_millis"}}}

    def _has_level(self, levels: tuple[str, ...]) -> dict[str, Any]:
        """有等級欄位就比等級；沒有就在訊息欄位做全文比對（訊息欄位要是 text 型別）。"""
        if self.fields.level:
            return _either(self.fields.level, levels)
        return {"match": {self.fields.message: {"query": " ".join(_level_words(levels)), "operator": "or"}}}

    def search_errors(self, services: list[str], start: datetime, end: datetime, size: int = 100, *,
                      error_levels: tuple[str, ...] = ERROR_LEVELS) -> list[dict[str, Any]]:
        body = {"size": size, "sort": [{self.fields.time: "desc"}], "query": {"bool": {"filter": [
            self._window(start, end), _either(self.fields.service, services), self._has_level(error_levels)]}}}
        return self._hits(self._msearch([body], "錯誤日誌查詢")[0])

    def count_logs(self, services: list[str], start: datetime, end: datetime) -> dict[str, int]:
        window = self._window(start, end)
        everything, ours = self._msearch([
            {"size": 0, "track_total_hits": True, "query": {"bool": {"filter": [window]}}},
            {"size": 0, "track_total_hits": True, "query": {"bool": {"filter": [window, _either(self.fields.service, services)]}}}], "計數")
        return {"all": self._total(everything), "services": self._total(ours)}

    def error_profile(self, services: list[str], start: datetime, end: datetime, *, error_levels: tuple[str, ...] = ERROR_LEVELS,
                      warn_levels: tuple[str, ...] = (), first_n: int = 3, warn_n: int = 50) -> dict[str, Any]:
        """每個系統一個子查詢（各自由舊到新取 first_n 筆），警告再一個；全部一次 _msearch 送出。"""
        window, oldest_first = self._window(start, end), [{self.fields.time: "asc"}]
        bodies = [{"size": first_n, "sort": oldest_first, "query": {"bool": {"filter": [
            window, _either(self.fields.service, [service]), self._has_level(error_levels)]}}} for service in services]
        if warn_levels:
            bodies.append({"size": warn_n, "sort": oldest_first, "query": {"bool": {"filter": [
                window, _either(self.fields.service, services), self._has_level(warn_levels)]}}})
        responses = self._msearch(bodies, "錯誤概況查詢")
        return {"services": {service: self._hits(resp) for service, resp in zip(services, responses)},
                "warnings": self._hits(responses[-1]) if warn_levels else []}

    def count_phrases(self, items: list[tuple[str, list[str], tuple[str, ...]]], start: datetime, end: datetime,
                      before_start: datetime) -> list[tuple[int, int]]:
        """詞用 intervals 比對（照順序出現，中間可以隔別的字；訊息欄位要是 text 型別，分析器會轉小寫，所以不分大小寫）。
        跟 VictoriaLogs 的 seq 一樣是近似，而且兩邊斷詞的規則不同，同一批資料數出來的筆數可能差一點。"""
        def body(service: str, words: list[str], levels: tuple[str, ...], window: dict[str, Any]) -> dict[str, Any]:
            in_order = {"intervals": {self.fields.message: {"all_of": {"ordered": True, "intervals": [
                {"match": {"query": word, "ordered": True, "max_gaps": 0}} for word in words]}}}}
            return {"size": 0, "track_total_hits": True, "query": {"bool": {"filter": [
                window, _either(self.fields.service, [service]), self._has_level(levels), in_order]}}}

        bodies = []
        for service, words, levels in items:
            bodies.append(body(service, words, levels, self._window(start, end)))
            bodies.append(body(service, words, levels, self._window(before_start, start, end_exclusive=True)))
        if not bodies:
            return []
        totals = [self._total(resp) for resp in self._msearch(bodies, "次數比較查詢")]
        return list(zip(totals[0::2], totals[1::2]))

    def search_timeline(self, field: str, values: list[str], start: datetime, end: datetime, size: int) -> dict[str, dict[str, Any]]:
        if not values:
            return {}
        bodies = [{"size": size, "track_total_hits": True, "sort": [{self.fields.time: "asc"}],
                   "query": {"bool": {"filter": [self._window(start, end), _either(field, [value])]}}} for value in values]
        responses = self._msearch(bodies, "關聯時間線查詢")
        return {value: {"hits": self._hits(resp), "total": self._total(resp)} for value, resp in zip(values, responses)}
