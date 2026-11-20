"""Grafana 資料源：平台只連 Grafana（網址＋service account token），由 Grafana 的資料源 proxy 轉給後面的 Elasticsearch。

平台不再持有 Elasticsearch 的網址與金鑰（2026-11-20 起）。這個檔只處理「後端是 Elasticsearch 型別資料源」的 trace 與日誌
（Elastic APM 或 OTel Collector 寫進 ES 的文件，文件形狀見 sources/es_models.py）。指標時序（fetch_data）與 Tempo 的 trace
還沒做：之後依訊號（traces／logs／…）各自選傳輸方式，查詢組裝與文件解析不用動。

環境變數
- AIOPS_GRAFANA_URL：Grafana 基底網址（跟告警卡片深度連結用的是同一個，stages/notification/links.py）
- AIOPS_GRAFANA_TOKEN：service account token，送 Authorization: Bearer <token>（機密，只能來自 Secret）
- AIOPS_GRAFANA_USER＋AIOPS_GRAFANA_PASSWORD：沒有 token 時改用 HTTP basic（密碼是機密；兩種都給時用 token）。
  正式環境用 token；basic 是給還沒建 service account 的環境驗證用的
- AIOPS_GRAFANA_TRACES_UID／AIOPS_GRAFANA_LOGS_UID：Grafana 裡 Elasticsearch 型別資料源的 UID（不是機密）
- AIOPS_ES_DATA_MODEL、AIOPS_ES_TRACE_INDEX、AIOPS_ES_LOG_INDEX：照舊，描述的是 ES 裡文件的形狀與索引，不是連線
configured() 只看網址與認證（token，或帳號＋密碼）；UID 沒設時那一種訊號的撈取丟 InfrastructureError 並寫明缺哪個環境變數
（ingestion.incident_traces／incident_logs 會接住、記進 errors、報告照出；跟「沒有設定」的安靜略過不同，這是設定只做了一半）。

線上格式（只有 GrafanaSpec._msearch 一個地方知道）
  POST {AIOPS_GRAFANA_URL}/api/datasources/proxy/uid/{uid}/_msearch
  Authorization: Bearer <token>
  Content-Type: application/x-ndjson
  body 兩行（各以換行結尾）：
    {"index": "<索引樣式>", "ignore_unavailable": true}
    {<一般的 ES search body>}
  成功：HTTP 200 {"took": .., "responses": [ {<一般的 ES search 回應>, "status": 200} ]}
  查詢寫錯：HTTP 400，**最外層**就是 {"error": {"type", "reason", ...}, "status": 400}（沒有 responses）
  個別查詢失敗也可能放在 responses[0].error（ES _msearch 的規格；兩種都處理）
只讀：只送這一個 POST，以及 check() 的 GET /api/health、GET /api/datasources/uid/{uid}；不呼叫任何寫入路徑。
Grafana 自己也只放行這一條：對 ES 資料源 proxy 的其他 POST 一律 403。

已在 Grafana 13.0.7（Elasticsearch plugin 12.8.0）驗證（2026-11-20，手動探測，認證用 HTTP basic）
1. 上面的路由、NDJSON 框法、Content-Type 可用，回 HTTP 200 與 {"responses": [...]}，每一筆是原生的 ES search 回應
   （hits.total、hits.hits[]._source 是原生巢狀文件、sort）。
2. track_total_hits、sort、search_after、_source 過濾、aggs 經 proxy 都照常運作。
3. NDJSON 標頭的 index 會被採用，即使跟資料源設定的索引不同。索引樣式對不到東西或資料源的 ES 帳號沒有權限時**不是錯誤**
   （ignore_unavailable）：回 status 200、_shards.total = 0、hits.total.value = 0。所以 _msearch 看到 0 個 shard 會寫一筆
   warning 日誌（「撈到 0 筆」有可能是索引樣式寫錯或權限不足，不一定是真的沒資料）。
4. 查詢 body 寫錯 -> HTTP 400，最外層 {"error": {...}, "status": 400}。
5. 其他 POST 路徑（例如 .../<index>/_search）-> HTTP 403
   {"message": "posts not allowed on proxied Elasticsearch datasource except on /_msearch"}。
6. 資料源 UID 不存在 -> HTTP 404 {"message": "Unable to find datasource"}。
7. GET /api/datasources/uid/{uid} -> 200，有 type（"elasticsearch"）與 name；GET /api/health -> {"database": "ok", "version": ...}，不需要登入。

未驗證
1. service account token（Authorization: Bearer）能不能通過資料源 proxy、需要的最小角色／資料源權限。探測時用的是管理員的 HTTP basic。
2. function_score／random_score（超過上限時的抽樣查詢）經 proxy 的行為：直連 ES 8.19 時驗證過，經 Grafana 沒有單獨探測。
3. 這個類別的 fetch_spans／fetch_logs 整條路（分頁、抽樣、解析）還沒有對真的 Grafana 跑過；tests/ 用的是照上面格式寫的假 Grafana
   （tests/grafana_fake.py），測試全過只代表程式跟這份說明一致。
4. responses[0].error（HTTP 200 但個別查詢失敗）沒有實際看過，是照 ES _msearch 的規格處理。
驗證方式（一個指令）：.venv/Scripts/python scripts/verify_grafana_source.py
"""
import base64
import json
import logging
import math
import os
from datetime import datetime
from typing import Any, Callable

import httpx
import pandas as pd

from aiops.engine.errors import InfrastructureError
from aiops.sources import _http
from aiops.sources.base import BaseDataSourceSpec, InvalidQueryError, canonical_service
from aiops.sources.es_models import ELASTIC_APM, Resolved, current_model, get_path
from aiops.sources.es_models import _links as _apm_links

logger = logging.getLogger("aiops.sources")
URL_ENV, TOKEN_ENV = "AIOPS_GRAFANA_URL", "AIOPS_GRAFANA_TOKEN"
USER_ENV, PASSWORD_ENV = "AIOPS_GRAFANA_USER", "AIOPS_GRAFANA_PASSWORD"  # 沒有 token 時的 HTTP basic
# 訊號 -> 那個訊號的 Grafana 資料源 UID 放在哪個環境變數
UID_ENVS = {"traces": "AIOPS_GRAFANA_TRACES_UID", "logs": "AIOPS_GRAFANA_LOGS_UID"}
ES_DATASOURCE_TYPE = "elasticsearch"  # Grafana 資料源的 type；check() 用它確認 UID 指到的是這個檔會講的後端


class GrafanaSpec(BaseDataSourceSpec):
    """經 Grafana 資料源 proxy 讀 Elasticsearch 裡的 trace 與錯誤日誌（線上格式與未驗證的部分見檔頭）。只讀。"""

    source_type = "grafana"

    def __init__(self, base_url: str, token: str | None = None, uids: dict[str, str] | None = None,
                 client: httpx.Client | None = None, timeout: float = 15.0, basic: tuple[str, str] | None = None):
        self.base_url = base_url.rstrip("/")
        if token:
            self.headers = {"Authorization": f"Bearer {token}"}
        elif basic:
            self.headers = {"Authorization": "Basic " + base64.b64encode(":".join(basic).encode("utf-8")).decode("ascii")}
        else:
            self.headers = {}
        self.uids = {k: v.strip() for k, v in (uids or {}).items() if v and v.strip()}
        self.client = client or httpx.Client(timeout=timeout)

    @classmethod
    def from_env(cls) -> "GrafanaSpec":
        url, token, basic = _http.env_required(URL_ENV), _env(TOKEN_ENV), (_env(USER_ENV), _env(PASSWORD_ENV))
        if not token and not all(basic):
            raise InfrastructureError(f"沒有設定 {TOKEN_ENV}（或 {USER_ENV}＋{PASSWORD_ENV}）")
        return cls(url, token or None, {signal: _env(env) for signal, env in UID_ENVS.items()},
                   basic=None if token else basic)

    @classmethod
    def configured(cls) -> bool:
        return bool(_env(URL_ENV) and (_env(TOKEN_ENV) or (_env(USER_ENV) and _env(PASSWORD_ENV))))

    def fetch_data(self, query: str, start: datetime, end: datetime, step_seconds: int,
                   field_map: dict[str, str] | None = None) -> pd.DataFrame:
        raise InvalidQueryError("grafana 資料源目前只撈 trace 與日誌，不支援指標時序（指標經 Grafana 查詢還沒做）")

    def fetch_spans(self, start: datetime, end: datetime, services: list[str] | None = None,
                    max_spans: int = 20000, field_map: dict[str, str] | None = None,
                    environment: str | None = None) -> tuple[list[dict[str, Any]], bool]:
        """[start, end] 內的 span。欄位與索引依資料模型（AIOPS_ES_DATA_MODEL，見 sources/es_models.py）：
        elastic_apm（預設）的 transaction／span 文件、或 OTel Collector（otel mapping）的 span 文件。
        field_map 是已核准的欄位對照，標準欄 span.* 的那幾條會蓋過模型的預設欄位。只送 _msearch 查詢，不寫任何索引。

        區間內 span 總數 <= max_spans：用 search_after 全部撈完（完整，不抽樣）。
        超過：不能只拿時間最早的 max_spans 筆（尖峰時那只是窗口開頭幾秒，拓撲會偏），改成 _sampled_spans：
        把窗口切成時間桶，每桶隨機挑 trace，輪流把「整條 trace」的 span 撈回來直到湊滿 max_spans。
        以 trace 為單位抽樣，父子才接得起來（抽 span 會把跨服務的邊拆斷）。回傳的 truncated 為 True。"""
        r = current_model().resolve(field_map)
        index = _trace_index(r)
        env = r.environment_filters("span.environment", environment)  # 在查詢裡濾：別環境的 span 不佔 max_spans 的額度
        rows, over = self._paginate("traces", index, [*_window_filters(start, end, services, r.query_field("span.service")), *env],
                                    r.span_source, r.span_sort, r.span, max_spans, "撈 trace")
        if over:
            return self._sampled_spans(r, index, start, end, services, max_spans, env), True
        return rows[:max_spans], over is None

    def fetch_linked_spans(self, trace_ids: list[str], start: datetime, end: datetime,
                           max_spans: int = 20000, field_map: dict[str, str] | None = None) -> tuple[list[dict[str, Any]], bool]:
        """訊息佇列跨時窗：把指定 trace 在 [start, end] 內的 span 全撈回來（消費者 parent／span.links 指到的生產者那一段）。
        不帶服務過濾：生產者是誰要撈回來才知道。trace ID 每 500 個一批、每批 search_after 分頁，湊滿 max_spans 就停。"""
        r = current_model().resolve(field_map)
        index = _trace_index(r)
        ids = sorted({t for t in trace_ids if t})
        window = _window_filters(start, end, None)
        spans: list[dict[str, Any]] = []
        for i in range(0, len(ids), LINKED_TRACES_PER_QUERY):
            batch = ids[i:i + LINKED_TRACES_PER_QUERY]
            after = None
            while True:
                if len(spans) >= max_spans:
                    return spans[:max_spans], True
                body: dict[str, Any] = {"size": min(5000, max_spans - len(spans)), "_source": r.span_source,
                                        "sort": r.span_sort,
                                        "query": {"bool": {"filter": [*window, {"terms": {r.query_field("span.trace_id"): batch}}]}}}
                if after is not None:
                    body["search_after"] = after
                hits = self._search("traces", index, body, "撈訊息生產者 trace").get("hits", {}).get("hits", [])
                if not hits:
                    break
                spans += [s for s in (r.span(h["_source"]) for h in hits) if s is not None]
                after = hits[-1]["sort"]
        return spans, False

    def _paginate(self, signal: str, index: str, filters: list[dict[str, Any]], source: list[str], sort: list[dict[str, Any]],
                  parse: Callable[[dict[str, Any]], dict[str, Any] | None], limit: int,
                  what: str) -> tuple[list[dict[str, Any]], bool | None]:
        """search_after 分頁撈到 limit 筆（fetch_spans 與 fetch_logs 共用）。回傳 (rows, over)：
        over=True：第一頁就知道總數超過 limit（呼叫端改走分桶抽樣，rows 是空的）；
        over=False：撈完了（沒有下一頁）；over=None：撈滿 limit 筆就停了（當成被截斷）。"""
        rows: list[dict[str, Any]] = []
        after = None
        first = True
        while len(rows) < limit:
            body: dict[str, Any] = {"size": min(5000, limit - len(rows)), "query": {"bool": {"filter": filters}},
                                    "_source": source, "sort": sort}
            if first:
                body["track_total_hits"] = limit + 1  # 只要知道「有沒有超過上限」，不必數完全部
            if after is not None:
                body["search_after"] = after
            payload = self._search(signal, index, body, what)
            total = ((payload.get("hits") or {}).get("total") or {}).get("value")
            if first and total is not None and total > limit:
                return [], True
            first = False
            hits = payload.get("hits", {}).get("hits", [])
            if not hits:
                return rows, False
            rows += [x for x in (parse(h["_source"]) for h in hits) if x is not None]
            after = hits[-1]["sort"]
        return rows[:limit], None

    def _uid(self, signal: str) -> str:
        uid = self.uids.get(signal)
        if not uid:
            raise InfrastructureError(f"沒有設定 {UID_ENVS[signal]}（Grafana 裡 {signal} 用的 Elasticsearch 資料源 UID）")
        return uid

    def _search(self, signal: str, index: str, body: dict[str, Any], what: str) -> dict[str, Any]:
        """一次 ES search：signal（traces／logs）決定走哪個 Grafana 資料源。回傳一般的 ES search 回應。"""
        return self._msearch(signal, self._uid(signal), index, body, what)

    def _msearch(self, signal: str, uid: str, index: str, body: dict[str, Any], what: str) -> dict[str, Any]:
        """唯一知道線上格式的地方（檔頭「線上格式」「已驗證」「未驗證」）。Grafana 那邊格式不一樣時只改這個函式。

        錯誤分工：連不上、401／403、5xx -> InfrastructureError（_http.request）；404 -> InfrastructureError（UID 設錯，是部署設定）；
        400／422（最外層 error）與 responses[0].error -> InvalidQueryError（查詢寫錯）；
        200 但外層不是 {"responses": [一筆]} -> InvalidQueryError。"""
        ndjson = json.dumps({"index": index, "ignore_unavailable": True}) + "\n" + json.dumps(body) + "\n"
        resp = _http.request(self.client, self.source_type, "POST",
                             f"{self.base_url}/api/datasources/proxy/uid/{uid}/_msearch",
                             headers={**self.headers, "Content-Type": "application/x-ndjson"},
                             content=ndjson.encode("utf-8"))
        if resp.status_code == 404:
            raise InfrastructureError(f"grafana {what}失敗（HTTP 404）：找不到資料源 uid={uid}，請檢查 {UID_ENVS[signal]}"
                                      f"：{resp.text[:200]}")
        try:
            payload = resp.json() if resp.content else {}
        except ValueError as e:
            raise InvalidQueryError(f"grafana {what}的回應格式不對（HTTP {resp.status_code}）：不是 JSON：{resp.text[:200]}") from e
        if not resp.is_success:
            raise InvalidQueryError(f"grafana {what}失敗（HTTP {resp.status_code}）：{_reason(payload) or resp.text[:200]}")
        responses = payload.get("responses") if isinstance(payload, dict) else None
        if not isinstance(responses, list) or len(responses) != 1 or not isinstance(responses[0], dict):
            raise InvalidQueryError(f"grafana {what}的回應格式不對：預期 {{\"responses\": [一筆]}}，收到 {resp.text[:200]}")
        result = responses[0]
        if result.get("error"):
            status = result.get("status")
            if status in (401, 403):  # Grafana 資料源自己存的 ES 帳密不對或權限不足：部署要修，不是查詢寫錯
                raise InfrastructureError(f"grafana {what}失敗：資料源 uid={uid} 連 Elasticsearch 認證失敗（{status}）：{_reason(result)}")
            raise InvalidQueryError(f"grafana {what}失敗（Elasticsearch 回 {status}）：{_reason(result) or str(result['error'])[:200]}")
        shards = result.get("_shards")
        if isinstance(shards, dict) and shards.get("total") == 0:
            # 不是錯誤（ignore_unavailable），但「0 筆」很可能不是真的沒資料：留一筆日誌讓人查得到
            logger.warning("grafana %s：索引 %s 在資料源 uid=%s 對不到任何 shard（索引樣式寫錯，或 Grafana 資料源的 "
                           "Elasticsearch 帳號沒有這個索引的讀取權限）；這次查詢回 0 筆", what, index, uid)
        return result

    def _sampled_spans(self, r: Resolved, index: str, start: datetime, end: datetime, services: list[str] | None,
                       max_spans: int, env: list[dict[str, Any]] | tuple = ()) -> list[dict[str, Any]]:
        """時間分桶 + 以 trace 為單位的均勻抽樣。

        1. 窗口切成 min(分鐘數, SAMPLE_MAX_BUCKETS) 個桶；每桶用固定 seed 的 random_score 挑一批候選文件，只拿 trace ID
           （固定 seed：同一個事件重跑報告，抽到的 trace 一樣）。
        2. 一輪從每個桶各拿 SAMPLE_TRACES_PER_ROUND 條還沒撈過的 trace，用 terms trace ID 把它們的 span 全撈回來；
           整條放得下才收（不收半條），湊滿 max_spans 或候選用完就停。各桶輪流拿，所以每個時段的 trace 數差不多。
        """
        trace_field, service_field = r.query_field("span.trace_id"), r.query_field("span.service")
        buckets = max(1, min(SAMPLE_MAX_BUCKETS, math.ceil((end - start).total_seconds() / 60)))
        step = (end - start) / buckets
        candidates_per_bucket = min(5000, max(SAMPLE_TRACES_PER_ROUND * 4, max_spans // buckets))
        queues: list[list[str]] = []
        seen: set[str] = set()
        for b in range(buckets):
            lo, hi = start + step * b, (start + step * (b + 1) if b < buckets - 1 else end)
            body = {"size": candidates_per_bucket, "_source": [trace_field],
                    "query": {"function_score": {"query": {"bool": {"filter": [*_window_filters(
                                                     lo, hi, services, service_field, b == buckets - 1), *env]}},
                                                 "random_score": {"seed": SAMPLE_SEED, "field": "_seq_no"},
                                                 "boost_mode": "replace"}}}
            ids = []
            for h in self._search("traces", index, body, "抽樣 trace").get("hits", {}).get("hits", []):
                tid = get_path(h.get("_source") or {}, trace_field)
                if tid and tid not in seen:
                    seen.add(tid)
                    ids.append(tid)
            queues.append(ids)

        spans: list[dict[str, Any]] = []
        window = [*_window_filters(start, end, services, service_field), *env]
        while len(spans) < max_spans and any(queues):
            # 交錯排（桶0第1條、桶1第1條…桶0第2條…），最後一輪放不下時各桶少的一樣多，不會偏向前面的桶
            batch = [q[i] for i in range(SAMPLE_TRACES_PER_ROUND) for q in queues if i < len(q)]
            queues = [q[SAMPLE_TRACES_PER_ROUND:] for q in queues]
            by_trace: dict[str, list[dict[str, Any]]] = {}
            after = None
            while True:
                body = {"size": 5000, "_source": r.span_source, "sort": r.span_sort,
                        "query": {"bool": {"filter": [*window, {"terms": {trace_field: batch}}]}}}
                if after is not None:
                    body["search_after"] = after
                hits = self._search("traces", index, body, "撈抽樣 trace").get("hits", {}).get("hits", [])
                if not hits:
                    break
                for s in (r.span(h["_source"]) for h in hits):
                    if s is not None:
                        by_trace.setdefault(s["trace_id"], []).append(s)
                after = hits[-1]["sort"]
            for tid in batch:  # 依抽樣順序收整條 trace；放不下的跳過，換下一條比較短的
                trace = by_trace.get(tid, [])
                if trace and len(spans) + len(trace) <= max_spans:
                    spans += trace
        return sorted(spans, key=lambda s: s["ts"])

    def fetch_logs(self, start: datetime, end: datetime, services: list[str] | None = None,
                   max_logs: int = 5000, field_map: dict[str, str] | None = None,
                   environment: str | None = None) -> tuple[list[dict[str, Any]], bool]:
        """錯誤日誌。elastic_apm：APM 錯誤文件（processor.event = error），訊息依序取 error.exception（陣列或單一物件都可）
        的 message、error.log.message、頂層 message。otel：OTel 日誌資料流裡 ERROR 以上（severity_number >= 17）與
        span 的 exception 事件，訊息取 body.text › exception.message › 結構化 body。索引用 AIOPS_ES_LOG_INDEX 改。只送 _msearch 查詢。

        區間內筆數 <= max_logs：全部撈（每筆 weight=1）。超過：不再只拿時間最早的 max_logs 筆（事故後段的暴增會整段被丟掉），
        改成把區間切成時間桶，每桶撈固定筆數，每筆帶 weight = 該桶總筆數 ÷ 撈到的筆數，暴增倍率用加權後的筆數算。
        回傳的 truncated 為 True。"""
        r = current_model().resolve(field_map)
        index = _log_index(r)
        env = r.environment_filters("log.environment", environment)
        filters = [*_window_filters(start, end, services, r.query_field("log.service")), *r.model.log_filters, *env]
        rows, over = self._paginate("logs", index, filters, r.log_source, r.model.log_sort, r.log, max_logs, "撈日誌")
        if over:
            return self._sampled_logs(r, index, start, end, services, max_logs, env), True
        return rows, over is None

    def _sampled_logs(self, r: Resolved, index: str, start: datetime, end: datetime, services: list[str] | None,
                      max_logs: int, env: list[dict[str, Any]] | tuple = ()) -> list[dict[str, Any]]:
        buckets = max(1, min(SAMPLE_MAX_BUCKETS, math.ceil((end - start).total_seconds() / 60)))
        step = (end - start) / buckets
        quota = max(1, min(5000, max_logs // buckets))
        out: list[dict[str, Any]] = []
        for b in range(buckets):
            lo, hi = start + step * b, (start + step * (b + 1) if b < buckets - 1 else end)
            filters = [*_window_filters(lo, hi, services, r.query_field("log.service"), b == buckets - 1),
                       *r.model.log_filters, *env]
            body = {"size": quota, "track_total_hits": True, "_source": r.log_source, "sort": r.model.log_sort,
                    "query": {"bool": {"filter": filters}}}
            payload = self._search("logs", index, body, "抽樣日誌")
            hits = payload.get("hits", {}).get("hits", [])
            rows = [x for x in (r.log(h["_source"]) for h in hits) if x is not None]
            total = ((payload.get("hits") or {}).get("total") or {}).get("value") or len(rows)
            weight = total / len(rows) if rows else 1.0
            out += [{**x, "weight": weight} for x in rows]
        return out

    def check(self) -> dict[str, Any]:
        """Grafana 活著嗎（GET /api/health）＋ token 讀不讀得到每個有設定的資料源（GET /api/datasources/uid/{uid}）。
        回傳跟其他資料源一樣的 ok／detail／latency_ms，另外多 datasources：{訊號: {uid, configured, ok, detail}}。
        UID 沒設的訊號標 configured=False、ok=None，不算失敗（例如 trace 之後改走 Tempo）；
        一個 UID 都沒設時 token 無從驗證（/api/health 不需要登入），detail 會寫明。"""
        health = _http.check(self.client, self.source_type, f"{self.base_url}/api/health", headers=self.headers)
        datasources = {signal: self._check_datasource(signal, env) for signal, env in UID_ENVS.items()}
        checked = {signal: d for signal, d in datasources.items() if d["configured"]}
        bad = [f"{signal}：{d['detail']}" for signal, d in checked.items() if not d["ok"]]
        if not health["ok"]:
            detail = health["detail"]
        elif bad:
            detail = "；".join(bad)
        elif not checked:
            detail = f"Grafana 連得上；沒有設定任何資料源 UID（{'／'.join(UID_ENVS.values())}），token 沒有驗證"
        else:
            detail = "Grafana 連得上，token 讀得到資料源 " + "、".join(f"{signal}={d['uid']}" for signal, d in checked.items())
        return {**health, "ok": bool(health["ok"] and not bad), "detail": detail, "datasources": datasources}

    def _check_datasource(self, signal: str, env: str) -> dict[str, Any]:
        uid = self.uids.get(signal)
        if not uid:
            return {"uid": None, "configured": False, "ok": None, "detail": f"未設定 {env}"}
        try:
            resp = _http.request(self.client, self.source_type, "GET", f"{self.base_url}/api/datasources/uid/{uid}",
                                 headers=self.headers)
            info = resp.json() if resp.is_success and resp.content else {}
        except (InfrastructureError, ValueError) as e:
            return {"uid": uid, "configured": True, "ok": False, "detail": str(e)}
        if not resp.is_success:
            return {"uid": uid, "configured": True, "ok": False, "detail": f"HTTP {resp.status_code}：{env} 指到的資料源不存在"}
        kind = info.get("type") if isinstance(info, dict) else None
        if kind != ES_DATASOURCE_TYPE:
            return {"uid": uid, "configured": True, "ok": False,
                    "detail": f"資料源型別是 {kind!r}；這個資料源目前只會經 proxy 送 Elasticsearch 的 _msearch"}
        return {"uid": uid, "configured": True, "ok": True, "detail": f"{kind}：{str(info.get('name') or '')[:80]}"}


def _env(name: str) -> str:
    return os.environ.get(name, "").strip()


def _reason(payload: Any) -> str | None:
    """ES 風格的錯誤（{"error": {"reason": ...}}）或 Grafana 風格的錯誤（{"message": ...}）裡給人看的那一句。"""
    if not isinstance(payload, dict):
        return None
    error = payload.get("error")
    if isinstance(error, dict):
        return error.get("reason") or error.get("type")
    return (error if isinstance(error, str) else None) or payload.get("message")


SAMPLE_MAX_BUCKETS = 30
LINKED_TRACES_PER_QUERY = 500
SAMPLE_TRACES_PER_ROUND = 20
SAMPLE_SEED = 20261103


def _trace_index(r: Resolved) -> str:
    return os.environ.get("AIOPS_ES_TRACE_INDEX", "").strip() or r.model.trace_index


def _log_index(r: Resolved) -> str:
    return os.environ.get("AIOPS_ES_LOG_INDEX", "").strip() or r.model.log_index


def _window_filters(start: datetime, end: datetime, services: list[str] | None, service_field: str = "service.name",
                    end_inclusive: bool = True) -> list[dict[str, Any]]:
    """時間區間＋服務過濾。分桶抽樣時，除了最後一桶都要 end_inclusive=False（含頭不含尾），
    不然剛好落在桶邊界的文件會被相鄰兩桶各算一次，日誌權重就會偏高。"""
    upper = "lte" if end_inclusive else "lt"
    filters: list[dict[str, Any]] = [{"range": {"@timestamp": {"gte": start.isoformat(), upper: end.isoformat()}}}]
    if services:
        # 大小寫不分（case_insensitive，ES 7.10+）：事件實體叫 cart-api、索引裡寫 Cart-API 一樣要撈得到
        names = sorted({canonical_service(s) for s in services})
        filters.append({"bool": {"should": [{"term": {service_field: {"value": n, "case_insensitive": True}}}
                                            for n in names], "minimum_should_match": 1}})
    return filters


# 相容舊介面（測試與合成資料驗證直接拿來解析單筆 APM 文件）：一律走 Elastic APM 模型、不套欄位對照
_APM = ELASTIC_APM.resolve(None)


def _span(src: dict[str, Any]) -> dict[str, Any] | None:
    return _APM.span(src)


def _log(src: dict[str, Any]) -> dict[str, Any] | None:
    return _APM.log(src)


def _links(raw: Any) -> tuple[tuple[str, str], ...]:
    return _apm_links(raw, "trace.id", "span.id")
