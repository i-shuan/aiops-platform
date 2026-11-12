"""OTel 端到端：拋棄式 ES（otel 資料模型）-> 拓撲沉澱 -> 偵測 -> 事件群 -> rca_report（noop LLM）。

先跑 gen_incident.py 灌資料，再在 repo 根目錄：python tests/fixtures/otel_es/e2e/run_e2e.py
資料庫是暫存 SQLite；只連 127.0.0.1:29200（拋棄式 ES），不碰任何叢集。結果見同資料夾 RESULTS.md。
"""
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
HERE = Path(__file__).parent
OUT = Path(tempfile.mkdtemp(prefix="otel_e2e_"))
db = OUT / "e2e.sqlite"
os.environ.update(AIOPS_ES_URL="http://127.0.0.1:29200", AIOPS_ES_DATA_MODEL="otel", AIOPS_NOTIFIER="log",
                  AIOPS_DATABASE_URL=f"sqlite:///{db.as_posix()}")
os.environ.pop("AIOPS_ES_API_KEY", None)
sys.path.insert(0, os.getcwd())

from collections import Counter  # noqa: E402

from aiops.sources import _http  # noqa: E402

calls = []
orig = _http.request


def traced(client, source, method, url, **kw):
    t0 = time.perf_counter()
    resp = orig(client, source, method, url, **kw)
    calls.append({"url": url.split("/", 3)[-1], "ms": round((time.perf_counter() - t0) * 1000, 1), "status": resp.status_code})
    return resp


_http.request = traced

from aiops.engine.requests import TopologyRefresh  # noqa: E402
from aiops.interfaces.bootstrap import init_infrastructure  # noqa: E402
from aiops.interfaces.runner import run, run_context  # noqa: E402

init_infrastructure()
now = datetime.now(timezone.utc)

print("== 1. 拓撲沉澱（admin.topology_refresh，lookback 90 分鐘）")
n = len(calls)
topo = run("admin_topology_refresh", request=TopologyRefresh(lookback_minutes=90))
print(json.dumps({k: v for k, v in topo.items() if k != "edges"}, ensure_ascii=False, default=str)[:600])
print("edges", topo.get("edges"))
print("ES 查詢", len(calls) - n, "次")

print("== 2. 偵測（ES 指標查詢 avg(duration) of Server spans，MAD）")
n = len(calls)
ctx = run_context(str(HERE / "otel_detect.yaml"), now=now, strict=False, allow_file=True)
res = ctx.to_result()
print("ES 查詢", len(calls) - n, "次，耗時", [c["ms"] for c in calls[n:]])
print("errors", res.get("errors"))
by_entity = {}
for a in ctx.anomalies:
    by_entity.setdefault(a.entity, []).append(a.ts)
for e, ts in sorted(by_entity.items()):
    print(f"  異常 {e}: {len(ts)} 點 {min(ts):%H:%M}–{max(ts):%H:%M}Z")
for i in res.get("incidents", []):
    print("  事件群", i["id"], i["uid"], i["severity"], i["entities"], i["start"], "->", i["end"], "suppressed", i["suppressed_by"])

incidents = [i for i in res.get("incidents", []) if i["id"]]
if not incidents:
    print("沒有事件群，RCA 跳過")
    sys.exit(0)
target = max(incidents, key=lambda i: (len(i["entities"]), i["score"]))

print(f"== 3. rca_report（incident {target['id']}，實體 {target['entities']}）")
n = len(calls)
t0 = time.perf_counter()
rca = run("rca_report", incident_id=target["id"])
print("耗時", round(time.perf_counter() - t0, 2), "s；ES 查詢", len(calls) - n, "次")
print("  依索引", Counter(c["url"].split("/")[0] for c in calls[n:]), "最慢", max(c["ms"] for c in calls[n:]), "ms")
print("trace_fetch", json.dumps(rca.get("trace_fetch"), ensure_ascii=False, default=str))
print("log_fetch", json.dumps(rca.get("log_fetch"), ensure_ascii=False, default=str))
rc = rca.get("root_cause") or {}
print("root_cause", rc.get("algorithm"))
for c in rc.get("candidates", [])[:5]:
    print(f"  {c['entity']}  {c['score']}  {c['reason'][:400]}")
print("skipped", rca.get("skipped"))
print("errors", rca.get("errors"))
for s in rca.get("sections", []):
    if s.get("key") in ("trace_evidence", "log_patterns", "root_cause_candidates", "propagation_path"):
        print(f"--- 段落 {s.get('key')} ---")
        print(json.dumps(s, ensure_ascii=False, default=str)[:1500])
print("完整結果", OUT / "rca_result.json")
(OUT / "rca_result.json").write_text(json.dumps(rca, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
