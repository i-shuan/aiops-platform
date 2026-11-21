"""錄製 tests/fixtures/grafana13/*.json 的腳本（留著是為了交代來源，測試不會執行它）。

前提：本機拋棄式環境——Grafana 13.0.7（127.0.0.1:23000，admin/admin）、Prometheus（:29090，已回填
mq_queue_depth、cim_t3_timeout_total）、PostgreSQL（:5432，amhs.transfer_stats，唯讀帳號）、
Elasticsearch 8.19.23（:29200，關安全性）。

做的事：用 admin 建資料源、Editor 服務帳號與 Token、告警規則；之後**所有被錄下來的請求都用那把 Editor Token 發**，
回應原樣寫檔（只做 JSON 排版，不改內容）。每個檔案外層是 {"request": ..., "status": ..., "response": ...}。
"""
import json
import pathlib
import time

import httpx

G = "http://127.0.0.1:23000"
HERE = pathlib.Path(__file__).parent
admin = httpx.Client(base_url=G, auth=("admin", "admin"), timeout=30)


def ok(r):
    assert r.status_code < 300, (r.request.url, r.status_code, r.text[:400])
    return r.json()


# ── Elasticsearch 測試文件 ──
es = httpx.Client(base_url="http://127.0.0.1:29200", timeout=30)
now = time.time()
iso = lambda ago: time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(now - ago))
lines = []
logs = [
    (600, "cim-eap", "ERROR", "T3 timeout waiting S6F12 reply", "EQP-CVD-07", "LOT24A0031", "FOUP0912"),
    (540, "cim-eap", "ERROR", "T3 timeout waiting S2F42 reply", "EQP-CVD-07", "LOT24A0031", "FOUP0912"),
    (500, "cim-eap", "ERROR", "equipment disconnected (HSMS NOT SELECTED)", "EQP-CVD-07", "LOT24A0044", "FOUP0377"),
    (420, "amhs-mcs", "ERROR", "transfer job aborted: destination port occupied", "STK01", "LOT24A0031", "FOUP0912"),
    (400, "cim-eap", "ERROR", "T3 timeout waiting S6F12 reply", "EQP-ETCH-02", "LOT24A0058", "FOUP0120"),
    (380, "mq-broker", "ERROR", "consumer lag exceeded on queue lot.dispatch", None, None, None),
    (300, "cim-eap", "INFO", "lot track-in complete", "EQP-CVD-03", "LOT24A0060", "FOUP0555"),
]
for ago, svc, lvl, msg, eqp, lot, car in logs:
    doc = {"@timestamp": iso(ago), "service": {"name": svc}, "log": {"level": lvl}, "message": msg}
    doc.update({k: v for k, v in (("eqp_id", eqp), ("lot_id", lot), ("carrier_id", car)) if v})
    lines += [json.dumps({"index": {"_index": "factory-logs"}}), json.dumps(doc)]
spans = [
    (590, "cim-eap", "SECS S6F11 report", "t-0001", "error", 45000000, "T3 timeout"),
    (585, "mq-broker", "publish lot.dispatch", "t-0001", "ok", 1200, None),
    (410, "amhs-mcs", "transfer FOUP0912", "t-0002", "error", 9000000, "destination port occupied"),
]
for ago, svc, name, tid, status, dur, err in spans:
    doc = {"@timestamp": iso(ago), "service": {"name": svc}, "span": {"name": name}, "trace": {"id": tid},
           "status": {"code": status}, "duration": dur}
    if err:
        doc["error"] = {"message": err}
    lines += [json.dumps({"index": {"_index": "factory-traces"}}), json.dumps(doc)]
r = es.post("/_bulk?refresh=true", content="\n".join(lines) + "\n", headers={"Content-Type": "application/x-ndjson"})
assert not ok(r)["errors"]

# ── 資料源 ──
def datasource(body):
    r = admin.post("/api/datasources", json=body)
    if r.status_code == 409:
        return ok(admin.get(f"/api/datasources/name/{body['name']}"))["uid"]
    return ok(r)["datasource"]["uid"]


prom = datasource({"name": "prom", "type": "prometheus", "access": "proxy", "url": "http://127.0.0.1:29090"})
pg = datasource({"name": "amhs-pg", "type": "grafana-postgresql-datasource", "access": "proxy", "url": "127.0.0.1:5432",
                 "user": "grafana_ro", "secureJsonData": {"password": "ro"},
                 "jsonData": {"database": "amhs", "sslmode": "disable"}})
esds = datasource({"name": "factory-es", "type": "elasticsearch", "access": "proxy", "url": "http://127.0.0.1:29200",
                   "jsonData": {"index": "factory-*", "timeField": "@timestamp"}})

# ── Editor 服務帳號 ──
sa = ok(admin.post("/api/serviceaccounts", json={"name": f"aiops-lite-{int(now)}", "role": "Editor"}))
token = ok(admin.post(f"/api/serviceaccounts/{sa['id']}/tokens", json={"name": "rec"}))["key"]
editor = httpx.Client(base_url=G, headers={"Authorization": f"Bearer {token}"}, timeout=30)

# ── 告警規則（各系統自己在 Grafana 設的那種；這裡用 admin 建）──
r = admin.post("/api/folders", json={"uid": "factory", "title": "factory"})
assert r.status_code in (200, 409, 412), r.text


def rule(title, labels, ds_uid, model, threshold):
    return {"title": title, "ruleGroup": "factory", "folderUID": "factory", "orgID": 1, "condition": "C",
            "for": "1m", "noDataState": "OK", "execErrState": "OK", "labels": labels,
            "annotations": {"summary": title},
            "data": [
                {"refId": "A", "relativeTimeRange": {"from": 600, "to": 0}, "datasourceUid": ds_uid,
                 "model": {"refId": "A", **model}},
                {"refId": "B", "datasourceUid": "__expr__", "relativeTimeRange": {"from": 0, "to": 0},
                 "model": {"refId": "B", "type": "reduce", "expression": "A", "reducer": "last"}},
                {"refId": "C", "datasourceUid": "__expr__", "relativeTimeRange": {"from": 0, "to": 0},
                 "model": {"refId": "C", "type": "threshold", "expression": "B",
                           "conditions": [{"evaluator": {"type": "gt", "params": [threshold]}}]}},
            ]}


SQL_OK = ("SELECT $__timeGroupAlias(ts,'1m'), avg(transfer_duration) AS value FROM transfer_stats "
          "WHERE $__timeFilter(ts) GROUP BY 1 ORDER BY 1")
rules = [
    rule("MQ queue depth", {"aiops_detect": "true", "aiops_severity": "P1", "service": "mq-broker"}, prom,
         {"expr": 'max(mq_queue_depth{service="mq-broker"})', "instant": False, "range": True}, 200),
    rule("CIM T3 timeout", {"aiops_detect": "true", "aiops_severity": "P2", "service": "cim-eap"}, prom,
         {"expr": 'sum(increase(cim_t3_timeout_total{service="cim-eap"}[1m]))', "instant": False, "range": True}, 5),
    rule("AMHS transfer duration", {"aiops_detect": "true", "aiops_severity": "P2", "service": "amhs-mcs"}, pg,
         {"rawSql": SQL_OK, "format": "time_series", "rawQuery": True}, 90),
    rule("Not managed by aiops", {"team": "infra"}, prom, {"expr": "up", "instant": True}, 0),
]
if not ok(editor.get("/api/v1/provisioning/alert-rules")):
    for body in rules:
        ok(admin.post("/api/v1/provisioning/alert-rules", json=body, headers={"X-Disable-Provenance": "true"}))


def record(name, method, path, **kw):
    r = editor.request(method, path, **kw)
    try:
        body = r.json()
    except ValueError:
        body = r.text
    req = {"method": method, "path": path, **{k: v for k, v in kw.items() if k in ("json", "params")}}
    if "content" in kw:
        req["content"] = kw["content"]
    (HERE / f"{name}.json").write_text(json.dumps({"request": req, "status": r.status_code, "response": body},
                                                  ensure_ascii=False, indent=1) + "\n")
    print(name, r.status_code, len(r.content))
    return body


to_ms = int(now) // 60 * 60 * 1000
from_ms = to_ms - 3600 * 1000


def dsq(name, ds_uid, model, frm=from_ms, to=to_ms):
    return record(name, "POST", "/api/ds/query", json={
        "from": str(frm), "to": str(to),
        "queries": [{"refId": "A", "datasource": {"uid": ds_uid}, "intervalMs": 60000, "maxDataPoints": 5000, **model}]})


record("alert_rules", "GET", "/api/v1/provisioning/alert-rules")
dsq("query_promql", prom, {"expr": 'max(mq_queue_depth{service="mq-broker"})', "range": True, "instant": False})
dsq("query_sql_single_value", pg, {"rawSql": SQL_OK, "format": "time_series", "rawQuery": True})
dsq("query_sql_multi_value", pg, {"rawQuery": True, "format": "time_series", "rawSql":
    "SELECT $__timeGroupAlias(ts,'1m'), avg(transfer_duration) AS transfer_duration, sum(pending_jobs) AS pending_jobs "
    "FROM transfer_stats WHERE $__timeFilter(ts) GROUP BY 1 ORDER BY 1"})
dsq("query_sql_long_format", pg, {"rawQuery": True, "format": "table", "rawSql":
    "SELECT ts AS time, eqp_id, transfer_duration FROM transfer_stats WHERE $__timeFilter(ts) ORDER BY 1"})
dsq("query_sql_no_timefilter", pg, {"rawQuery": True, "format": "time_series", "rawSql":
    "SELECT ts AS time, transfer_duration AS value FROM transfer_stats ORDER BY 1"},
    frm=to_ms - 15 * 60 * 1000)
dsq("query_promql_bad", prom, {"expr": "max(mq_queue_depth{", "range": True})


def msearch(index, query, size):
    head = json.dumps({"index": index, "ignore_unavailable": True})
    body = json.dumps({"size": size, "sort": [{"@timestamp": "desc"}], "query": query})
    return f"{head}\n{body}\n"


window = {"range": {"@timestamp": {"gte": from_ms, "lte": to_ms + 60000, "format": "epoch_millis"}}}
content = (msearch("factory-logs", {"bool": {"filter": [window, {"terms": {"log.level": ["ERROR", "error"]}}]}}, 50)
           + msearch("factory-traces", {"bool": {"filter": [window, {"term": {"status.code": "error"}}]}}, 50))
record("es_proxy_logs_traces", "POST", f"/api/datasources/proxy/uid/{esds}/_msearch", content=content,
       headers={"Content-Type": "application/x-ndjson"})

# ── 註記：建立、更新、依標籤查回 ──
created = record("annotation_create", "POST", "/api/annotations", json={
    "time": from_ms, "timeEnd": to_ms, "tags": ["aiops", "aiops-incident:rec-0001", "P1", "open"],
    "text": "line1\nline2\nline3"})
record("annotation_patch", "PATCH", f"/api/annotations/{created['id']}", json={
    "timeEnd": to_ms + 60000, "tags": ["aiops", "aiops-incident:rec-0001", "P1", "closed"], "text": "l1\nl2\nl3"})
record("annotation_list", "GET", "/api/annotations", params={"tags": "aiops-incident:rec-0001", "limit": 10})
(HERE / "meta.json").write_text(json.dumps({
    "grafana": ok(admin.get("/api/health")), "recorded_at": iso(0), "token_role": "Editor",
    "datasource_uids": {"prometheus": prom, "postgres": pg, "elasticsearch": esds}}, indent=1) + "\n")
