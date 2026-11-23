"""Q4 補：參數寫錯是「載入期」（Pipeline.__init__ / check_override）還是「執行期」（execute 裡的 validate_params）才爆；
Workflow／NodeSpec 有沒有 extra="forbid"（拼錯鍵會不會被默默忽略）。

管線：ingestion.fetch（mock 資料源）→ detector.mad。NullHooks、strict=False（排程模式）。
用法：python scripts/probe/q4_config_validation.py [--root <aiops 原始碼根>]
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402


def main() -> int:
    use_root(sys.argv)
    from aiops.engine.pipeline import Pipeline, Workflow
    from aiops.interfaces import runner
    runner.load_all()

    now = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)
    target = {"source_type": "mock", "query": "cpu", "entity": "svc-a"}

    def wf(node_params=None, node_extra=None, wf_extra=None, override=None):
        t = dict(target)
        if override is not None:
            t["overrides"] = {"mad": override}
        d = {"name": "probe", "nodes": [
            {"name": "fetch", "operator": "ingestion.fetch", "params": {"lookback_minutes": 120}},
            {"name": "mad", "operator": "detector.mad", "params": dict(node_params or {}), **(node_extra or {})}],
            "targets": [t], **(wf_extra or {})}
        return d

    cases = {
        "node params min_scale=-1": wf({"min_scale": -1, "min_train_points": 10}),
        "node params min_scale='abc'": wf({"min_scale": "abc", "min_train_points": 10}),
        "node params min_scale='5'（字串數字）": wf({"min_scale": "5", "min_train_points": 10}),
        "node params min_scale_ratio=2": wf({"min_scale_ratio": 2, "min_train_points": 10}),
        "node params threshold=-3": wf({"threshold": -3, "min_train_points": 10}),
        "node params threshold='abc'": wf({"threshold": "abc", "min_train_points": 10}),
        "target override min_scale=-1": wf({"min_train_points": 10}, override={"min_scale": -1}),
        "target override threshold=-3": wf({"min_train_points": 10}, override={"threshold": -3}),
        "node 拼錯 optinal: true": wf({"min_train_points": 10}, node_extra={"optinal": True}),
        "workflow 拼錯 max_failed_target_ration": wf({"min_train_points": 10}, wf_extra={"max_failed_target_ration": 0.5}),
        "target 拼錯 detector:（少 s）": None,
    }
    t_bad = wf({"min_train_points": 10})
    t_bad["targets"][0]["detector"] = ["mad"]
    cases["target 拼錯 detector:（少 s）"] = t_bad

    for name, d in cases.items():
        try:
            w = Workflow.model_validate(d)
        except Exception as e:  # noqa: BLE001
            print(f"{name:42} LOAD(pydantic) {type(e).__name__}: {str(e).splitlines()[0][:70]}")
            continue
        try:
            p = Pipeline(w)
        except Exception as e:  # noqa: BLE001
            print(f"{name:42} LOAD(Pipeline.__init__) {type(e).__name__}: {str(e)[:90]}")
            continue
        try:
            ctx = p.run(now=now, strict=False)
            errs = [f"{e.entity}: {e.message[:80]}" for e in ctx.errors]
            print(f"{name:42} RUNTIME ok-run errors={errs or 0} anomalies={len(ctx.anomalies)}")
        except Exception as e:  # noqa: BLE001
            print(f"{name:42} RUNTIME RAISES {type(e).__name__}: {str(e)[:90]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
