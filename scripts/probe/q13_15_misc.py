"""Q13～Q15（補足的三題）。

Q13 workflow 名稱的路徑安全：interfaces/runner.workflow_path 對各種惡意名稱的反應。
Q14 LLM 降級：enrichment.llm_summary／llm_briefing 遇到「LLM 回奇怪的東西」或「供應商丟非 LLMError 的例外」時，
    是退回模板、還是例外漏出去（漏出去時只有 YAML 標了 optional: true 才不會讓 RCA 失敗）。
Q15 db.get_engine 的 lazy 初始化沒有鎖：10 執行緒同時第一次取 engine，會建幾個 Engine（PROBE_PG_URL 有設才跑）。
"""
from __future__ import annotations

import os
import sys
import threading
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402


def main() -> int:
    use_root(sys.argv)
    for k in list(os.environ):
        if k.startswith("AIOPS_"):
            del os.environ[k]
    from aiops.interfaces import runner
    runner.load_all()

    print("── Q13 workflow_path")
    for name in ["rca_report", "../rca/rca_report", "rca_report.yaml", "rca_report\n", "rca_report\x00", "", "*",
                 "ｒｃａ_report", "RCA_REPORT", "a" * 129, "C:\\Windows\\win", "rca_report;rm"]:
        try:
            p = runner.workflow_path(name)
            print(f"   {name!r:28} -> {p.relative_to(runner.WORKFLOW_DIR).as_posix()}")
        except Exception as e:  # noqa: BLE001
            print(f"   {name!r:28} -> {type(e).__name__}: {str(e)[:60]}")

    print("── Q14 LLM 回奇怪的東西")
    from aiops.engine.context import PipelineContext, ReportSection
    from aiops.engine.types import AnomalyEvent, Incident
    from aiops.stages.enrichment import registry
    from aiops.stages.enrichment.base import BaseLLM
    from aiops.stages.enrichment.summarizer import LLMBriefing, LLMSummarizer
    reply = {"v": ""}

    class Weird(BaseLLM):
        name = "weird"

        def complete(self, system, prompt):
            if reply["v"] == "RAISE_RUNTIME":
                raise RuntimeError("SDK 內部炸掉（不是 LLMError）")
            if reply["v"] == "RAISE_TIMEOUT":
                raise TimeoutError("read timeout（不是 LLMError）")
            return reply["v"]

    if "weird" not in registry.names():
        registry.register("weird", requires_model=False)(lambda m, b, k: Weird())
    now = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)
    inc = Incident([AnomalyEvent("cart", "latency", now, 5.0)], 5.0, severity="P2")
    for v in ["not json", "[]", "null", '{"summary": 5, "claims": "x"}', '{"summary": "x", "claims": [{"text": 1}]}',
              '{"summary_line": null}', '{"summary_line": ["a"], "action_line": {}, "verification_line": 3}',
              "RAISE_RUNTIME", "RAISE_TIMEOUT"]:
        reply["v"] = v
        out = []
        for op in (LLMSummarizer(provider="weird"), LLMBriefing(provider="weird")):
            ctx = PipelineContext("rca", now, incidents=[inc],
                                  rca_sections=[ReportSection("summary", "摘要", "cart 變慢 5 分")])
            try:
                ctx = op.execute(ctx)
                by = ctx.llm_generated_by if isinstance(op, LLMSummarizer) else ctx.llm_briefing_generated_by
                why = ctx.llm_rejected if isinstance(op, LLMSummarizer) else ctx.llm_briefing_rejected
                out.append(f"{type(op).__name__}: {by}（{(why or '')[:36]}）")
            except Exception as e:  # noqa: BLE001
                out.append(f"{type(op).__name__}: 漏出 {type(e).__name__}: {str(e)[:40]}")
        print(f"   reply={v[:40]!r:44} " + " | ".join(out))

    url = os.environ.get("PROBE_PG_URL")
    if url:
        print("── Q15 get_engine 併發初始化")
        import sqlalchemy

        from aiops.storage import db
        db.configure(url)
        made = []
        real = db.create_engine

        def counting(*a, **k):
            import time
            time.sleep(0.05)  # 放大競態窗口（真實情況是建 engine 本身的耗時）
            e = real(*a, **k)
            made.append(e)
            return e

        db.create_engine = counting
        bar = threading.Barrier(10)
        got = []

        def w():
            bar.wait()
            got.append(id(db.get_engine()))

        ts = [threading.Thread(target=w) for _ in range(10)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        db.create_engine = real
        print(f"   create_engine 被呼叫 {len(made)} 次；10 個執行緒拿到 {len(set(got))} 個不同的 Engine；"
              f"最後留在模組上的是其中 1 個，其他 {len(made) - 1} 個的連線池沒有人 dispose")
        del sqlalchemy
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
