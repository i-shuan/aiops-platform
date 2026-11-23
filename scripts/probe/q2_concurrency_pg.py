"""Q2：真 PostgreSQL 上的併發撞擊。需要 PROBE_PG_URL（例：postgresql+psycopg://postgres:x@127.0.0.1:55437/aiops）。
會 drop_all / create_all 那個資料庫：只能指向拋棄式容器。

A. 10 執行緒同時對同一個 incident_uid 做 Knowledge.upsert + Knowledge.merge_context（同 admin.feedback_submit 的 _golden）
   + 1 個讀者執行緒一直讀（看不看得到「列已建好、context 還沒 series」的半套狀態）
B. true_positive／false_positive 兩種回饋同時進來（各 5 執行緒），重複 20 輪：最後 verdict 是誰
C. 10 執行緒同時 DetectorModels.save(expected_version=0)，再 10 執行緒同時 save(expected_version=1)
D. 10 執行緒同時 ActiveConfigs.set(expected_version=0)，再同時 set(expected_version=1)
E. 10 執行緒同時對同一個事件群跑 rca_report（provider 換成計數用的假 LLM；YAML 複製到暫存檔，平台檔案不動）：
   有沒有互斥、LLM 被叫幾次、資料庫多了幾列
F. run_in_transaction：第一次嘗試在寫完異常點之後模擬斷線 -> 重試後異常點是 N 筆還是 2N 筆、ctx.incident_ids 對不對；
   F2 示範「fn 會改輸入」的寫法在重試時會怎樣（平台沒有機制擋，只靠 docstring）

用法：PROBE_PG_URL=... python scripts/probe/q2_concurrency_pg.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402

N = 10


def barrier_run(fn, n=N):
    bar = threading.Barrier(n)
    out, errs = [None] * n, []

    def worker(i):
        bar.wait()
        try:
            out[i] = fn(i)
        except Exception as e:  # noqa: BLE001
            errs.append(f"{type(e).__name__}: {str(e)[:160]}")

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return out, errs


def main() -> int:
    root = use_root(sys.argv)
    url = os.environ["PROBE_PG_URL"]
    for k in list(os.environ):
        if k.startswith("AIOPS_"):
            del os.environ[k]
    from sqlalchemy import text

    from aiops.storage import db
    from aiops.storage import models  # noqa: F401
    db.configure(url)
    db.drop_all()
    db.create_all()
    from aiops.storage.repository import ActiveConfigs, DetectorModels, Incidents, Knowledge

    now = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)
    base = dict(pipeline="it_otel", entities=["cart-api"], metrics=["transaction.duration"], signals=["latency"],
                verdict="true_positive", severity="P1", window_start=now, window_end=now + timedelta(minutes=5))

    # ── A ──
    uid = "inc-probe-a"
    torn, stop = Counter(), threading.Event()

    def reader():
        while not stop.is_set():
            r = Knowledge.get(uid)
            if r is None:
                torn["no_row"] += 1
            elif "series" not in (r["context"] or {}):
                torn["row_without_series"] += 1
            else:
                torn["complete"] += 1

    rt = threading.Thread(target=reader)
    rt.start()

    def golden(i):
        Knowledge.upsert(uid, **base, author=f"w{i}", resolution=f"res-{i}", root_cause=f"rc-{i}")
        time.sleep(0.005)  # 兩步之間的正常延遲
        return Knowledge.merge_context(uid, {f"tag_w{i}": i, "series": [{"w": i}]})

    _, errs = barrier_run(golden)
    stop.set()
    rt.join()
    row = Knowledge.get(uid)
    keys = sorted(row["context"])
    print(f"[A] errors={errs or 0} context tag keys={len([k for k in keys if k.startswith('tag_')])}/10 "
          f"series={row['context']['series']} author={row['author']} resolution={row['resolution']} "
          f"root_cause={row['root_cause']}")
    print(f"[A] reader snapshots: {dict(torn)}")
    with db.read_session() as s:
        print("[A] rows for uid:", s.execute(text("select count(*) from incident_knowledge where incident_uid=:u"),
                                             {"u": uid}).scalar())

    # ── B ──
    finals = Counter()
    for trial in range(20):
        u = f"inc-probe-b{trial}"
        Knowledge.upsert(u, **base, author="seed")

        def fb(i, u=u):
            if i % 2 == 0:
                Knowledge.upsert(u, **base, author=f"tp{i}")
                return Knowledge.merge_context(u, {"series": []})
            if Knowledge.get(u):
                return Knowledge.upsert(u, verdict="false_positive")
            return None

        barrier_run(fb)
        finals[Knowledge.get(u)["verdict"]] += 1
    print(f"[B] 20 輪 TP×5 + FP×5 同時送，最後 verdict 分布：{dict(finals)}")

    # ── C ──
    key = dict(pipeline="it_otel", entity="cart-api", metric="q", detector="detector.mad", node="mad")
    out, errs = barrier_run(lambda i: DetectorModels.save(**key, state={"w": i}, train_start=now, train_end=now,
                                                         points=100, expected_version=0))
    win0 = [i for i, r in enumerate(out) if r]
    out, errs1 = barrier_run(lambda i: DetectorModels.save(**key, state={"w": 100 + i}, train_start=now,
                                                          train_end=now, points=100, expected_version=1))
    win1 = [i for i, r in enumerate(out) if r]
    final = DetectorModels.get(key["pipeline"], key["entity"], key["metric"], key["detector"], key["node"])
    print(f"[C] v0 insert winners={len(win0)} errors={errs or 0}; v1 update winners={len(win1)} errors={errs1 or 0}; "
          f"final version={final['version']} state={final['state']}")

    # ── D ──
    out, errs = barrier_run(lambda i: ActiveConfigs.set("detector.mad", {"threshold": 4 + i}, "probe", i,
                                                        expected_version=0, pipeline="it_otel"))
    w0 = [i for i, r in enumerate(out) if r]
    out, errs1 = barrier_run(lambda i: ActiveConfigs.set("detector.mad", {"threshold": 10 + i}, "probe", i,
                                                         expected_version=1, pipeline="it_otel"))
    w1 = [i for i, r in enumerate(out) if r]
    r = ActiveConfigs.get("detector.mad", "it_otel")
    print(f"[D] v0 winners={len(w0)} errors={errs or 0}; v1 winners={len(w1)} errors={errs1 or 0}; "
          f"final version={r['version']} params={r['params']}")

    # ── E ──
    from aiops.engine.pipeline import Pipeline, load_workflow
    from aiops.engine.types import AnomalyEvent, Incident
    from aiops.interfaces import runner
    from aiops.stages.enrichment import registry as llm_registry
    from aiops.stages.enrichment.base import BaseLLM
    from aiops.storage.hooks import StorageHooks
    runner.load_all()
    calls = Counter()
    lock = threading.Lock()

    class CountingLLM(BaseLLM):
        name = "probe"

        def complete(self, system, prompt):
            with lock:
                calls["n"] += 1
            time.sleep(0.05)  # 模擬 LLM 延遲
            return '{"summary": "x", "claims": []}'

    if "probe" not in llm_registry.names():
        llm_registry.register("probe", requires_model=False)(lambda model, base_url, api_key: CountingLLM())
    inc = Incident([AnomalyEvent("cart-api", "transaction.duration", now, 9.0, {"signal": "latency"})], 9.0,
                   severity="P1")
    row = Incidents.create(inc.uid, "it_otel", inc.to_dict(), "P1", None, "it_otel", {})
    src = (root / "aiops/pipelines/rca/rca_report.yaml").read_text(encoding="utf-8").replace("provider: noop",
                                                                                           "provider: probe")
    tmp = Path(tempfile.mkdtemp()) / "rca_report.yaml"
    tmp.write_text(src, encoding="utf-8")

    def table_counts():
        with db.read_session() as s:
            names = [r[0] for r in s.execute(text("select tablename from pg_tables where schemaname='public'"))]
            return {n: s.execute(text(f'select count(*) from "{n}"')).scalar() for n in names}

    before = table_counts()
    t0 = time.perf_counter()
    out, errs = barrier_run(lambda i: Pipeline(load_workflow(tmp), StorageHooks()).run(incident_id=row["id"],
                                                                                      strict=True))
    dt = time.perf_counter() - t0
    after = table_counts()
    diff = {k: after[k] - before.get(k, 0) for k in after if after[k] != before.get(k, 0)}
    ok = [c for c in out if c is not None]
    print(f"[E] 10 個 rca_report 同時跑同一個事件群：完成={len(ok)} errors={errs or 0} 耗時={dt:.2f}s")
    print(f"[E] LLM complete() 總呼叫次數={calls['n']}；generated_by={dict(Counter(c.llm_generated_by for c in ok))} "
          f"briefing_by={dict(Counter(c.llm_briefing_generated_by for c in ok))}")
    print(f"[E] llm_rejected={dict(Counter(c.llm_rejected for c in ok))} briefing_rejected={dict(Counter(c.llm_briefing_rejected for c in ok))}")
    print(f"[E] 資料表列數變化：{diff or '無（RCA 報告不落地，只回給呼叫端）'}")

    # ── F ──
    from sqlalchemy.exc import OperationalError

    import aiops.storage.hooks as hooks_mod
    from aiops.engine.context import PipelineContext
    from aiops.storage import repository
    ctx = PipelineContext(pipeline="probe_f", now=now)
    evs = [AnomalyEvent("svc-f", "m", now + timedelta(minutes=i), 5.0, detector="detector.mad") for i in range(3)]
    ctx.detected_anomalies = list(evs)
    ctx.incidents = [Incident(list(evs), 5.0, severity="P2")]
    real_create, state = repository.Incidents.create, {"n": 0}

    def flaky_create(*a, **k):
        state["n"] += 1
        if state["n"] == 1:
            raise OperationalError("INSERT", {}, Exception("simulated: server closed the connection"))
        return real_create(*a, **k)

    hooks_mod.Incidents.create = staticmethod(flaky_create)
    try:
        StorageHooks().persist_batch(ctx)
    finally:
        hooks_mod.Incidents.create = staticmethod(real_create)
    with db.read_session() as s:
        n_anom = s.execute(text("select count(*) from anomalies where pipeline='probe_f'")).scalar()
        inc_ids = [r[0] for r in s.execute(text("select id from incidents where pipeline='probe_f'"))]
    print(f"[F] persist_batch 第 1 次嘗試寫完 3 筆異常點後斷線 -> 重試：Incidents.create 呼叫 {state['n']} 次；"
          f"anomalies={n_anom}（應 3）incidents={inc_ids} ctx.incident_ids={dict(ctx.incident_ids)}")

    # F2：會改輸入的 fn（平台裡沒有，示範契約只靠 docstring）
    pending = list(range(3))
    attempts = {"n": 0}

    def bad_fn():
        attempts["n"] += 1
        while pending:
            i = pending.pop()
            repository.Anomalies.create_many([{"pipeline": "probe_f2", "entity": f"e{i}", "metric": "m", "ts": now,
                                               "value": 0.0, "score": 1.0, "detector_name": "", "detector_node": "",
                                               "level": "anomaly", "attrs": {}}])
            if attempts["n"] == 1 and i == 1:
                raise OperationalError("INSERT", {}, Exception("simulated"))

    db.run_in_transaction(bad_fn)
    with db.read_session() as s:
        n2 = s.execute(text("select count(*) from anomalies where pipeline='probe_f2'")).scalar()
    print(f"[F2] fn 自己 pop 輸入清單：嘗試 {attempts['n']} 次，最後寫入 {n2} 筆（應 3）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
