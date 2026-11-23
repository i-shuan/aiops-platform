"""Q1：stages/rca 能不能在「沒有 storage／sources／FastAPI／SQLAlchemy／httpx」下用純記憶體物件跑完。

做法：在 sys.meta_path 最前面裝一個攔截器，任何 import aiops.storage / aiops.sources / aiops.interfaces /
aiops.control_plane / sqlalchemy / fastapi / httpx / psycopg / elasticsearch 直接 ImportError；
清掉所有 AIOPS_* 環境變數，並記錄執行期間誰讀了 os.environ。
只 import aiops.engine.* 與 aiops.stages.rca.*，手刻 SpanRecord／Incident／PipelineContext，
跑 rca.root_cause.evidence → rca.report（rca_report.yaml 的 10 個段落）。

用法：python scripts/probe/q1_rca_pure_memory.py [--root <aiops 原始碼根>]
"""
from __future__ import annotations

import importlib.abc
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402

BLOCKED = ("aiops.storage", "aiops.sources", "aiops.interfaces", "aiops.control_plane",
           "sqlalchemy", "fastapi", "starlette", "httpx", "psycopg", "psycopg2", "elasticsearch", "requests")


class Blocker(importlib.abc.MetaPathFinder):
    hits: list[str] = []

    def find_spec(self, name, path=None, target=None):
        if any(name == b or name.startswith(b + ".") for b in BLOCKED):
            Blocker.hits.append(name)
            raise ImportError(f"probe blocked: {name}")
        return None


def main() -> int:
    use_root(sys.argv)
    for k in list(os.environ):
        if k.startswith("AIOPS_"):
            del os.environ[k]
    sys.meta_path.insert(0, Blocker())

    env_reads: list[str] = []
    real_get = os.environ.get

    def tracking_get(key, default=None):
        env_reads.append(key)
        return real_get(key, default)

    os.environ.get = tracking_get  # type: ignore[method-assign]

    from aiops.engine.context import KnowledgeCase, LogRecord, PipelineContext, SpanRecord
    from aiops.engine.registry import OperatorRegistry
    from aiops.engine.types import AnomalyEvent, Incident
    # 註冊：平時由 interfaces/runner 走訪套件；這裡只手動 import rca 自己的模組
    import aiops.stages.rca.algorithms.topological  # noqa: F401  evidence 的 fallback 在 __init__ 就要查到它
    import aiops.stages.rca.algorithms.evidence  # noqa: F401
    import aiops.stages.rca.report  # noqa: F401
    for m in ("overview", "graph", "evidence", "log_patterns", "similar_cases", "actions"):
        __import__(f"aiops.stages.rca.sections.{m}")
    import aiops.stages.rca.topology_builder  # noqa: F401

    t0 = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)
    inc_start = t0 + timedelta(minutes=15)
    spans: list[SpanRecord] = []
    n = 0
    for minute in range(20):  # 0..14 事件前、15..19 事件中
        during = minute >= 15
        for k in range(12):
            n += 1
            ts = t0 + timedelta(minutes=minute, seconds=k * 5)
            cart_self = 200.0 if during else 5.0
            db_ms = 3.0
            cart_ms = cart_self + db_ms
            front_ms = cart_ms + 4.0
            tid = f"t{n}"
            spans.append(SpanRecord(tid, f"{tid}-f", None, "front", ts, front_ms, error=False, name="GET /cart"))
            spans.append(SpanRecord(tid, f"{tid}-c", f"{tid}-f", "cart-api", ts + timedelta(milliseconds=2), cart_ms))
            spans.append(SpanRecord(tid, f"{tid}-d", f"{tid}-c", "cart-api", ts + timedelta(milliseconds=3), db_ms,
                                    kind="db", subtype="postgresql", resource="postgresql"))
    events = [AnomalyEvent("front", "transaction.duration", inc_start + timedelta(minutes=1), 6.0, {"signal": "latency"}),
              AnomalyEvent("cart-api", "transaction.duration", inc_start, 9.0, {"signal": "latency"})]
    incident = Incident(events, score=9.0, severity="P1")
    logs = [LogRecord(inc_start + timedelta(seconds=i), "cart-api", f"Timeout waiting for pool id={i}", "TimeoutError")
            for i in range(10)]
    ctx = PipelineContext(pipeline="rca_report", now=inc_start + timedelta(minutes=10), incidents=[incident],
                          rca_spans=spans, rca_logs=logs, rca_logs_baseline=[], rca_log_window_minutes=(10.0, 60.0),
                          topology=[("front", "cart-api")], incident_pipeline="it_otel",
                          knowledge_cases=[KnowledgeCase("inc-old", "it_otel", ["cart-api", "front"],
                                                         ["transaction.duration"], ["latency"], "擴 DB pool",
                                                         "cart-api 連線池", inc_start - timedelta(days=7))])

    ctx = OperatorRegistry.create("rca.build_dynamic_topology", {"mode": "merge", "min_calls": 1}).execute(ctx)
    ctx = OperatorRegistry.create("rca.root_cause.evidence").execute(ctx)
    sections = ["summary", "timeline", "affected_entities", "golden_signals", "root_cause_candidates",
                "propagation_path", "trace_evidence", "log_patterns", "similar_cases", "suggested_actions"]
    ctx = OperatorRegistry.create("rca.report", {"sections": sections,
                                                 "section_params": {"trace_evidence": {"traces_datasource": "apm-traces"}}}
                                  ).execute(ctx)

    print("candidates:", [(c.entity, round(c.score, 3), c.dependency) for c in ctx.rca_root_candidates])
    print("root_note:", ctx.rca_root_note or "(空)")
    print("sections built:", [s.key for s in ctx.rca_sections])
    print("sections skipped:", ctx.rca_skipped)
    print("blocked import attempts:", sorted(set(Blocker.hits)) or "無")
    loaded = sorted(m for m in sys.modules if m.startswith("aiops."))
    print("aiops modules loaded:", len(loaded))
    print("  non-engine/non-rca:", [m for m in loaded if not m.startswith(("aiops.engine", "aiops.stages.rca"))])
    heavy = sorted({m.split(".")[0] for m in sys.modules} & {"sqlalchemy", "fastapi", "httpx", "psycopg", "pydantic", "yaml", "pandas", "numpy"})
    print("third-party loaded (subset):", heavy)
    print("os.environ.get AIOPS_* reads during run:", sorted({k for k in env_reads if k.startswith("AIOPS_")}) or "無")
    ok = len(ctx.rca_sections) > 0 and not Blocker.hits
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
