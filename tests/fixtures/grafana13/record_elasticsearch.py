"""錄製 elasticsearch.json 的腳本（留著是為了交代來源，測試不會執行它）。

前提：本機拋棄式的 Grafana 13.0.7（127.0.0.1:23000，admin/admin）與 Elasticsearch 8.19.23（:29200，關安全性）。
做的事：
  1. 把跟 record_victoria.py 一模一樣的假資料直接寫進 Elasticsearch（這是這支腳本唯一直連的地方）。
     rec-factory 有明確的 mapping（keyword／text）；rec-unparsed 是動態 mapping（沒拆欄位的容器日誌）。
  2. **用 Editor 服務帳號的 Token**，呼叫 src/logs.py 的 Elasticsearch 每一個查詢方法，另外跑一次完整的
     rca.collect_evidence；它實際送給 Grafana 資料源代理的請求與 Grafana 回的內容原樣記下來。

用法：python record_elasticsearch.py <Editor Token 的檔案>
"""
import json
import pathlib
import sys
from datetime import datetime, timedelta, timezone

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))
from src import rca  # noqa: E402
from src.grafana import Grafana, GrafanaError  # noqa: E402
from src.grouping import Alert, converge  # noqa: E402
from src.logs import Elasticsearch, LogFields  # noqa: E402

G, ES = "http://127.0.0.1:23000", "http://127.0.0.1:29200"
HERE = pathlib.Path(__file__).parent
token = pathlib.Path(sys.argv[1]).read_text().strip()
admin = httpx.Client(base_url=G, auth=("admin", "admin"), timeout=30)
es = httpx.Client(base_url=ES, timeout=30)
BASE = datetime(2026, 11, 21, 0, 36, 38, tzinfo=timezone.utc)   # 跟 record_victoria.py 同一個基準時間
START, END = BASE - timedelta(minutes=60), BASE + timedelta(minutes=1)
SERVICES = ["mq-broker", "cim-eap", "amhs-mcs"]
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")]
inserted = json.loads((HERE / "victoria_logs.json").read_text(encoding="utf-8"))["inserted"]   # 同一批資料
INDEXES = {"factory": "rec-factory", "count-mismatch": "rec-count-mismatch", "count-no-errors": "rec-count-no-errors",
           "unparsed": "rec-unparsed"}
EXPLICIT = {"@timestamp": {"type": "date"}, "service": {"properties": {"name": {"type": "keyword"}}},
            "log": {"properties": {"level": {"type": "keyword"}}}, "message": {"type": "text"}}

for dataset, index in INDEXES.items():
    es.delete(f"/{index}")
    mapping = {"@timestamp": {"type": "date"}} if dataset == "unparsed" else EXPLICIT
    es.put(f"/{index}", json={"mappings": {"properties": mapping}}).raise_for_status()
    lines = []
    for doc in inserted[dataset]:
        lines += [json.dumps({"index": {}}), json.dumps({k: v for k, v in doc.items() if k != "dataset"}, ensure_ascii=False)]
    r = es.post(f"/{index}/_bulk?refresh=true", content="\n".join(lines) + "\n", headers={"Content-Type": "application/x-ndjson"})
    assert r.status_code == 200 and not r.json()["errors"], r.text[:300]
es.delete("/rec-count-empty")
es.put("/rec-count-empty", json={"mappings": {"properties": EXPLICIT}}).raise_for_status()

old = admin.get("/api/datasources/name/rec-elasticsearch")
if old.status_code == 200:
    admin.delete(f"/api/datasources/uid/{old.json()['uid']}")
uid = admin.post("/api/datasources", json={"name": "rec-elasticsearch", "type": "elasticsearch", "access": "proxy", "url": ES,
                                           "jsonData": {"index": "rec-*", "timeField": "@timestamp"}}).json()["datasource"]["uid"]
seen: list[dict] = []


def recording(request: httpx.Request) -> httpx.Response:
    with httpx.Client(timeout=30) as http:
        r = http.request(request.method, str(request.url), headers={k: v for k, v in request.headers.items() if k != "host"},
                         content=request.content)
    seen.append({"request": {"method": request.method, "path": request.url.path.replace(uid, "<uid>"), "body": request.content.decode()},
                 "status": r.status_code, "response": r.text})
    return httpx.Response(r.status_code, content=r.content, headers={"Content-Type": r.headers.get("content-type", "")})


grafana = Grafana(G, token, transport=httpx.MockTransport(recording), retries=0)
exchanges: dict[str, list[dict]] = {}
results: dict[str, object] = {}


def record(name, call):
    try:
        results[name] = call()
        shown = json.dumps(results[name], ensure_ascii=False, default=str)
    except GrafanaError as e:
        shown = f"GrafanaError: {e}"
    exchanges[name] = seen[:]
    seen.clear()
    print(f"{name}: {len(exchanges[name])} 個請求 -> {shown[:170]}")


store = lambda dataset, fields=LogFields(): Elasticsearch(grafana, uid, fields, INDEXES.get(dataset, f"rec-{dataset}"))
K8S = LogFields(service="kubernetes.container_name", level="")
factory = store("factory")
record("search_errors", lambda: factory.search_errors(SERVICES, START, END, 50))
record("search_errors_size_2", lambda: factory.search_errors(SERVICES, START, END, 2))
record("search_errors_window", lambda: factory.search_errors(SERVICES, BASE - timedelta(seconds=450), BASE - timedelta(seconds=390), 50))
record("search_errors_unparsed", lambda: store("unparsed", K8S).search_errors(["cim-eap", "mq-broker"], START, END, 50))
record("error_profile", lambda: factory.error_profile(SERVICES, START, END, warn_levels=("WARN", "WARNING"), first_n=2))
record("error_profile_warnings", lambda: store("count-no-errors").error_profile(SERVICES, START, END, warn_levels=("WARN", "WARNING")))
record("count_phrases", lambda: factory.count_phrases(
    [("cim-eap", ["timeout", "waiting", "reply"], ("ERROR",)), ("cim-eap", ["equipment", "disconnected"], ("ERROR",)),
     ("mq-broker", ["consumer", "lag", "exceeded", "on", "queue", "lot.dispatch"], ("ERROR",)),
     ("amhs-mcs", ["no", "such", "words"], ("ERROR",))], BASE - timedelta(seconds=450), END, START))
record("count_phrases_unparsed", lambda: store("unparsed", K8S).count_phrases(
    [("cim-eap", ["publish", "to", "eap.events", "timed", "out"], ("ERROR", "error")), ("cim-eap", ["health", "probe"], ("ERROR",))],
    START, END, START - timedelta(minutes=30)))
for name in ("factory", "count-mismatch", "count-no-errors", "count-empty"):
    record(f"count_logs_{name}", lambda: store(name).count_logs(SERVICES, START, END))
record("search_timeline", lambda: factory.search_timeline("lot_id", ["LOT24A0031", "LOT24A0060", "LOT-NOPE"], START, END, 2))
record("missing_index", lambda: store("nope").search_errors(SERVICES, START, END, 5))
record("bad_field_type", lambda: Elasticsearch(grafana, uid, LogFields(time="message"), "rec-factory").search_errors(SERVICES, START, END, 5))

# 完整的一次證據收集：三個系統的事故，送出的每一個請求照順序錄下來（測試會照順序重播，跟 VictoriaLogs 的結果比）
alerts = [Alert(f"f{i}", "firing", f"r{i}", f"{s} 告警", s, "P2", BASE - timedelta(seconds=330 - i), None, {"A": 1})
          for i, s in enumerate(SERVICES)]
[incident] = converge([], alerts, [], EDGES, now=BASE, close_after_seconds=300)
record("collect_evidence", lambda: rca.collect_evidence(incident, EDGES, grafana, logs=factory, id_fields=["eqp_id", "lot_id", "carrier_id"],
                                                        now=BASE))
(HERE / "elasticsearch.json").write_text(json.dumps({
    "versions": {"grafana": admin.get("/api/health").json()["version"], "elasticsearch": es.get("/").json()["version"]["number"]},
    "token_role": "Editor", "exchanges": exchanges}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
admin.delete(f"/api/datasources/uid/{uid}")
