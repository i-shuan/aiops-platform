"""Elasticsearch 裡 trace／日誌的「資料模型設定檔（profile）」：同一套 RCA，接 Elastic APM 或 OTel Collector 寫進來的文件。

每個 profile 決定四件事：預設索引、查詢要用的欄位（服務過濾、trace.id、排序、_source）、怎麼把一筆文件解析成
fetch_spans／fetch_logs 的標準 dict（下游 SpanRecord／LogRecord 不變），以及日誌要加的過濾（OTel 的日誌所有等級混在一起）。

選哪個：環境變數 AIOPS_ES_DATA_MODEL（elastic_apm〔預設〕／otel）。索引仍可用 AIOPS_ES_TRACE_INDEX／AIOPS_ES_LOG_INDEX 改。

欄位覆蓋：已核准的欄位對照（ctx.field_maps[source_type]，來源欄 -> 標準欄）裡，標準欄是 span.*／log.* 的那幾條
會蓋過 profile 的預設來源欄（STANDARD_FIELDS 是可以蓋的清單）。例如舊版 semconv 的 HTTP 狀態碼：
  {"attributes.http.status_code": "span.status_code"}
指標用的標準欄（ts、value、entity）不受影響，兩者共用同一份對照。

文件形狀的依據是 tests/fixtures/otel_es/（OTel Collector 0.165.0 + ES 8.19.23 實際寫出的文件，README 有實測結論）。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Callable

from aiops.sources.base import InvalidQueryError, as_utc_datetime, canonical_service

DATA_MODEL_ENV = "AIOPS_ES_DATA_MODEL"

# 可以用欄位對照蓋掉的標準欄（標準欄 -> 意義）。mappings.py 用這份清單檢查提交的對照
STANDARD_FIELDS: dict[str, str] = {
    "span.trace_id": "trace ID",
    "span.span_id": "span ID",
    "span.parent_id": "父 span ID",
    "span.service": "服務名稱（也用在查詢的服務過濾）",
    "span.duration": "耗時（單位依 profile：APM 微秒、OTel 奈秒）",
    "span.status_code": "HTTP 回應狀態碼",
    "span.name": "入口名稱（transaction.name／OTel span name）",
    "span.db_system": "資料庫種類（postgresql、redis）",
    "span.peer_service": "下游服務名稱（peer.service）",
    "span.server_address": "下游位址（server.address）",
    "span.server_port": "下游連接埠（server.port）",
    "span.messaging_system": "訊息系統（kafka、rabbitmq）",
    "span.messaging_destination": "佇列／topic 名稱",
    "log.service": "日誌的服務名稱",
    "log.message": "日誌訊息",
    "log.error_type": "例外型別",
    "log.trace_id": "日誌對應的 trace ID",
}


def get_path(doc: Any, path: str) -> Any:
    """依點號路徑取值，巢狀物件與「平的點號鍵」都吃：OTel 的 attributes 是 {"db.system.name": ...}（鍵本身含點），
    APM 是 {"span": {"destination": ...}}。每一層先試最長的鍵，取不到再試短的。沒有就回 None。"""
    if not isinstance(doc, dict) or not path:
        return None
    if path in doc:
        return doc[path]
    parts = path.split(".")
    for i in range(len(parts) - 1, 0, -1):
        head = ".".join(parts[:i])
        if head in doc:
            found = get_path(doc[head], ".".join(parts[i:]))
            if found is not None:
                return found
    return None


def _first(doc: dict, paths: tuple[str, ...]) -> Any:
    for p in paths:
        v = get_path(doc, p)
        if v is not None and v != "":
            return v
    return None


def _int_status(v: Any) -> int:
    """HTTP 狀態碼：數字或數字字串（"502"）；其他型別一律 0（沒有），不吞例外。"""
    if isinstance(v, bool):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    return int(v) if isinstance(v, str) and v.isdigit() else 0


def _links(raw: Any, trace_key: str, span_key: str) -> tuple[tuple[str, str], ...]:
    """span links 整理成 ((trace_id, span_id), ...)。可能是陣列或單一物件；缺任一個 ID 的丟掉。"""
    items = raw if isinstance(raw, list) else [raw] if isinstance(raw, dict) else []
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        trace_id, span_id = get_path(item, trace_key), get_path(item, span_key)
        if trace_id and span_id:
            out.append((str(trace_id), str(span_id)))
    return tuple(out)


@dataclass(frozen=True)
class EsDataModel:
    name: str
    trace_index: str
    log_index: str
    # 標準欄 -> 預設來源欄（依序取第一個有值的；新舊版 semconv 並存時列多個）
    fields: dict[str, tuple[str, ...]]
    span_sort_tail: tuple[str, ...]  # 排序：@timestamp 之後的穩定鍵（trace、span ID）
    log_sort: list[dict[str, Any]]
    extra_span_source: tuple[str, ...]  # _source 要多帶、但不能被覆蓋的欄位（結構性欄位）
    extra_log_source: tuple[str, ...]
    parse_span: Callable[["Resolved", dict[str, Any]], dict[str, Any] | None]
    parse_log: Callable[["Resolved", dict[str, Any]], dict[str, Any] | None]
    log_filters: tuple[dict[str, Any], ...] = ()

    def resolve(self, field_map: dict[str, str] | None) -> "Resolved":
        """套用已核准的欄位對照：標準欄是 span.*／log.* 的，把來源欄放到最前面（優先），profile 預設的留著當後備。"""
        paths = dict(self.fields)
        overridden = set()
        for source, standard in (field_map or {}).items():
            if standard in STANDARD_FIELDS:
                paths[standard] = (source, *(p for p in paths.get(standard, ()) if p != source))
                overridden.add(standard)
        return Resolved(self, paths, frozenset(overridden))


@dataclass(frozen=True)
class Resolved:
    """一個 profile 套上欄位對照之後的樣子：查詢與解析都用它。"""

    model: EsDataModel
    paths: dict[str, tuple[str, ...]] = field(default_factory=dict)
    overridden: frozenset[str] = frozenset()  # 被欄位對照蓋過的標準欄

    def get(self, doc: dict, standard: str) -> Any:
        return _first(doc, self.paths.get(standard, ()))

    def override(self, doc: dict, standard: str) -> Any:
        """只看欄位對照指定的來源欄（沒被蓋過回 None）：APM 的結構性欄位（span／transaction 二選一）用它，
        沒設對照時一字不差走原本的邏輯。"""
        return get_path(doc, self.paths[standard][0]) if standard in self.overridden else None

    def query_field(self, standard: str) -> str:
        """查詢（term、terms、排序、_source）用的實際欄位：取第一個來源欄。"""
        return self.paths[standard][0]

    @property
    def span_source(self) -> list[str]:
        keys = [p for k, ps in self.paths.items() if k.startswith("span.") for p in ps]
        return list(dict.fromkeys(["@timestamp", *keys, *self.model.extra_span_source]))

    @property
    def log_source(self) -> list[str]:
        keys = [p for k, ps in self.paths.items() if k.startswith("log.") for p in ps]
        return list(dict.fromkeys(["@timestamp", *keys, *self.model.extra_log_source]))

    @property
    def span_sort(self) -> list[dict[str, Any]]:
        tail = [{self.query_field(k): {"order": "asc", "missing": "_first"}} for k in self.model.span_sort_tail]
        tail[0] = {self.query_field(self.model.span_sort_tail[0]): "asc"}
        return [{"@timestamp": "asc"}, *tail]

    def span(self, src: dict[str, Any]) -> dict[str, Any] | None:
        return self.model.parse_span(self, src)

    def log(self, src: dict[str, Any]) -> dict[str, Any] | None:
        return self.model.parse_log(self, src)


# ── Elastic APM（現況，預設；行為跟改之前一字不差）──


def _apm_first_exception(exc: Any) -> dict[str, Any]:
    """error.exception 可能是陣列（APM 標準）、單一物件、字串、或陣列裡放字串；一律整理成 {message, type}。"""
    first = exc[0] if isinstance(exc, list) and exc else exc
    if isinstance(first, dict):
        return first
    return {"message": first} if isinstance(first, str) and first.strip() else {}


def _apm_span(r: Resolved, src: dict[str, Any]) -> dict[str, Any] | None:
    span, txn = src.get("span") or {}, src.get("transaction") or {}
    # 是 span 文件還是 transaction 文件，看有沒有 span.id：批次消費的 transaction 文件帶 span.links，
    # 所以會有 span 物件但沒有 span.id，不能用「有沒有 span 物件」判斷（會把 transaction 的耗時讀成 0）
    own = span if span.get("id") else txn
    span_id = r.override(src, "span.span_id") or own.get("id")
    service = r.get(src, "span.service")
    trace_id = r.get(src, "span.trace_id")
    if not (span_id and service and trace_id):
        return None
    message = txn.get("message") or {}
    age = (message.get("age") or {}).get("ms")
    return {"trace_id": trace_id, "span_id": span_id, "parent_id": r.get(src, "span.parent_id"),
            "service": canonical_service(service), "ts": as_utc_datetime(src["@timestamp"]),
            "duration_ms": float(r.override(src, "span.duration") or (own.get("duration") or {}).get("us") or 0) / 1000.0,
            "error": (src.get("event") or {}).get("outcome") == "failure",
            # transaction 文件沒有 span.type（kind 留空 = 入口）；span 文件的 type 是 app／db／external／messaging…
            "kind": str(span.get("type") or ""), "subtype": str(span.get("subtype") or ""),
            "resource": str(((span.get("destination") or {}).get("service") or {}).get("resource") or ""),
            "status_code": _int_status(r.get(src, "span.status_code")),
            "name": str(r.override(src, "span.name") or txn.get("name") or ""),
            # 訊息消費者：transaction.type = messaging 的 transaction 文件。它的 parent／links 指向生產者的 send span（非同步邊）
            "consumer": own is txn and txn.get("type") == "messaging",
            "queue": str((message.get("queue") or {}).get("name") or ""),
            "age_ms": float(age) if isinstance(age, (int, float)) and not isinstance(age, bool) else None,
            "links": _links(span.get("links"), "trace.id", "span.id")}


def _apm_log(r: Resolved, src: dict[str, Any]) -> dict[str, Any] | None:
    service = r.get(src, "log.service")
    if not service or "@timestamp" not in src:
        return None
    error = src.get("error") or {}
    first = _apm_first_exception(error.get("exception"))
    message = r.override(src, "log.message") or first.get("message") \
        or (error.get("log") or {}).get("message") or src.get("message") or ""
    if not message:
        return None
    return {"ts": as_utc_datetime(src["@timestamp"]), "service": canonical_service(service),
            "message": str(message), "error_type": str(first.get("type") or ""),
            "trace_id": str(r.get(src, "log.trace_id") or "")}


ELASTIC_APM = EsDataModel(
    name="elastic_apm",
    trace_index="traces-apm-*",
    log_index="logs-apm.*",
    fields={
        "span.trace_id": ("trace.id",), "span.span_id": ("span.id", "transaction.id"), "span.parent_id": ("parent.id",),
        "span.service": ("service.name",), "span.duration": ("span.duration.us", "transaction.duration.us"),
        "span.status_code": ("http.response.status_code",), "span.name": ("transaction.name",),
        "log.service": ("service.name",), "log.trace_id": ("trace.id",), "log.message": ("message",),
    },
    span_sort_tail=("span.trace_id", "span.span_id"),
    # 分頁要穩定的排序：時間＋每筆唯一的 error.id（不用 _doc，跨分片沒有 PIT 時不保證順序）
    log_sort=[{"@timestamp": "asc"}, {"error.id": {"order": "asc", "missing": "_last"}}],
    extra_span_source=("event.outcome",
                       # 下游資源歸因與範例 trace 排序用；索引沒有就是空字串／0，行為退回只看服務
                       "span.type", "span.subtype", "span.destination.service.resource",
                       # 訊息佇列（非同步邊）：消費者 transaction 的型別、佇列名、訊息年齡，與批次消費的 span.links
                       "transaction.type", "transaction.message.queue.name", "transaction.message.age.ms", "span.links"),
    extra_log_source=("error.exception", "error.log", "error.culprit"),
    parse_span=_apm_span,
    parse_log=_apm_log,
)


# ── OTel Collector elasticsearchexporter，mapping mode = otel（0.165 的預設）──

# OTel 的 duration 是奈秒（span 的 endTimeUnixNano − startTimeUnixNano，exporter 原樣存成 long；
# 實測 120 ms -> 120000000，見 tests/fixtures/otel_es/README.md）
OTEL_NS_PER_MS = 1_000_000.0
# OTel Logs Data Model：SeverityNumber 17～20 是 ERROR、21～24 是 FATAL
# （https://opentelemetry.io/docs/specs/otel/logs/data-model/#field-severitynumber）
OTEL_ERROR_SEVERITY = 17


def _otel_kind(r: Resolved, src: dict[str, Any]) -> tuple[str, str, bool]:
    """(kind, subtype, consumer)，換成下游 SpanRecord 的語意（跟 APM 的 span.type／subtype 對齊）：
    Server -> 入口（kind 空）；Consumer -> 入口＋訊息消費者；Internal／沒寫 -> app（服務自己的程式）；
    Producer -> messaging；Client -> 有 db.system 是 db、有 messaging.system 是 messaging、其他是 external。"""
    kind = str(src.get("kind") or "").lower()
    db = r.get(src, "span.db_system")
    mq = r.get(src, "span.messaging_system")
    if kind == "server":
        return "", "", False
    if kind == "consumer":
        return "", str(mq or ""), True
    if kind == "producer":
        return "messaging", str(mq or ""), False
    if kind == "client":
        if db:
            return "db", str(db), False
        if mq:
            return "messaging", str(mq), False
        attrs = src.get("attributes") or {}
        http = any(k.startswith(("http.", "url.")) for k in attrs) if isinstance(attrs, dict) else False
        rpc = get_path(src, "attributes.rpc.system")
        return "external", "http" if http else str(rpc or ""), False
    return "app", "", False


def _otel_resource(r: Resolved, src: dict[str, Any], kind: str, subtype: str) -> str:
    """下游資源名稱（同 APM span.destination.service.resource 的用途）：
    peer.service › 訊息系統/佇列（kafka/orders，跟 APM 的寫法一樣）› server.address:port › server.address › 資料庫種類。"""
    if kind in ("", "app"):
        return ""
    peer = r.get(src, "span.peer_service")
    if peer:
        return str(peer)
    dest = r.get(src, "span.messaging_destination")
    if kind == "messaging" and subtype and dest:
        return f"{subtype}/{dest}"
    address, port = r.get(src, "span.server_address"), r.get(src, "span.server_port")
    if address:
        return f"{address}:{port}" if port not in (None, "") else str(address)
    return subtype


def _otel_span(r: Resolved, src: dict[str, Any]) -> dict[str, Any] | None:
    span_id, trace_id, service = r.get(src, "span.span_id"), r.get(src, "span.trace_id"), r.get(src, "span.service")
    if not (span_id and trace_id and service) or "@timestamp" not in src:
        return None
    kind, subtype, consumer = _otel_kind(r, src)
    duration = r.get(src, "span.duration")
    status = src.get("status") or {}
    entry = kind == ""
    return {"trace_id": str(trace_id), "span_id": str(span_id), "parent_id": r.get(src, "span.parent_id") or None,
            "service": canonical_service(service), "ts": as_utc_datetime(src["@timestamp"]),
            "duration_ms": float(duration or 0) / OTEL_NS_PER_MS,
            # OTel 的權威失敗旗標：status.code = Error（UNSET 時 exporter 存成空物件）
            "error": isinstance(status, dict) and str(status.get("code") or "").lower() == "error",
            "kind": kind, "subtype": subtype, "resource": _otel_resource(r, src, kind, subtype),
            "status_code": _int_status(r.get(src, "span.status_code")),
            "name": str(r.get(src, "span.name") or "") if entry else "",
            "consumer": consumer,
            "queue": str(r.get(src, "span.messaging_destination") or "") if consumer else "",
            # OTel 沒有標準的「訊息在佇列等了多久」屬性（APM 的 transaction.message.age.ms）：一律 None，
            # 佇列延遲只能靠 parent／links 找到生產者 span 的發布時間（engine/mq.py 的 T_pub）
            "age_ms": None,
            "links": _links(src.get("links"), "trace_id", "span_id")}


def _otel_message(r: Resolved, src: dict[str, Any]) -> str:
    """訊息：body.text › exception.message › 結構化 body 的 message／msg › 整個結構化 body（JSON）。"""
    text = r.get(src, "log.message")
    if text:
        return str(text)
    structured = get_path(src, "body.structured")
    if isinstance(structured, dict):
        for key in ("message", "msg"):
            if isinstance(structured.get(key), str) and structured[key].strip():
                return structured[key]
        return json.dumps(structured, ensure_ascii=False, sort_keys=True)[:500] if structured else ""
    return str(structured) if structured not in (None, "") else ""


def _otel_log(r: Resolved, src: dict[str, Any]) -> dict[str, Any] | None:
    service = r.get(src, "log.service")
    if not service or "@timestamp" not in src:
        return None
    message = _otel_message(r, src)
    if not message:
        return None
    return {"ts": as_utc_datetime(src["@timestamp"]), "service": canonical_service(service), "message": message,
            "error_type": str(r.get(src, "log.error_type") or ""), "trace_id": str(r.get(src, "log.trace_id") or "")}


OTEL = EsDataModel(
    name="otel",
    # exporter 依 data_stream 路由：traces-<dataset>.otel-<namespace>（預設 traces-generic.otel-default）
    trace_index="traces-*.otel-*",
    log_index="logs-*.otel-*",
    fields={
        "span.trace_id": ("trace_id",), "span.span_id": ("span_id",), "span.parent_id": ("parent_span_id",),
        "span.service": ("resource.attributes.service.name",), "span.duration": ("duration",),
        # 新舊 semconv 並存：新的在前（HTTP 1.23、DB 1.26+ 的 db.system.name、messaging 1.24 的 destination.name）
        "span.status_code": ("attributes.http.response.status_code", "attributes.http.status_code"),
        "span.name": ("name",),
        "span.db_system": ("attributes.db.system.name", "attributes.db.system"),
        "span.peer_service": ("attributes.peer.service",),
        "span.server_address": ("attributes.server.address", "attributes.net.peer.name"),
        "span.server_port": ("attributes.server.port", "attributes.net.peer.port"),
        "span.messaging_system": ("attributes.messaging.system",),
        "span.messaging_destination": ("attributes.messaging.destination.name", "attributes.messaging.destination"),
        "log.service": ("resource.attributes.service.name",), "log.trace_id": ("trace_id",),
        "log.message": ("body.text", "attributes.exception.message"),
        "log.error_type": ("attributes.exception.type", "attributes.error.type"),
    },
    span_sort_tail=("span.trace_id", "span.span_id"),
    # OTel 日誌沒有每筆唯一的 ID 欄位：時間＋trace＋span 當排序鍵。同一毫秒、同一個 span（或都沒有 trace）的多筆
    # 剛好落在分頁邊界時，search_after 可能漏掉同鍵的其他筆（只有超過 5000 筆一頁時才會發生）
    log_sort=[{"@timestamp": "asc"}, {"trace_id": {"order": "asc", "missing": "_last"}},
              {"span_id": {"order": "asc", "missing": "_last"}}],
    extra_span_source=("kind", "status", "links", "attributes.rpc.system", "attributes.http.request.method",
                       "attributes.url.full"),
    extra_log_source=("body.structured", "severity_number", "event_name"),
    parse_span=_otel_span,
    parse_log=_otel_log,
    # 這個資料流所有等級的日誌都在一起（APM 的 logs-apm.error 只有錯誤）：只要 ERROR 以上，或 span 的 exception 事件
    # （exporter 把 span 事件拆成獨立的日誌文件，event_name = exception）
    log_filters=({"bool": {"should": [{"range": {"severity_number": {"gte": OTEL_ERROR_SEVERITY}}},
                                      {"term": {"event_name": "exception"}}],
                           "minimum_should_match": 1}},),
)


MODELS = {m.name: m for m in (ELASTIC_APM, OTEL)}


def current_model() -> EsDataModel:
    name = os.environ.get(DATA_MODEL_ENV, "").strip().lower() or ELASTIC_APM.name
    if name not in MODELS:
        raise InvalidQueryError(f"{DATA_MODEL_ENV}={name} 不認得；可用：{'、'.join(MODELS)}")
    return MODELS[name]
