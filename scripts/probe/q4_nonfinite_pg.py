"""Q4 補：NaN／Inf 寫進 PostgreSQL 的 JSON 欄位會怎樣（PROBE_PG_URL 指向拋棄式容器）。

1. DetectorModels.save(state={"mean": inf, "std": nan})（zscore 訓練段含 +Inf 時 get_state() 的真實輸出）
2. persist_batch：一條序列的異常分數是 +Inf（mad 在偵測段遇到 Inf 值），另一條是正常分數，同一輪寫入
   -> 是只丟那一條、還是整輪（含健康序列）都寫不進去；例外型別會不會被 run_in_transaction 重試
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402


def main() -> int:
    use_root(sys.argv)
    for k in list(os.environ):
        if k.startswith("AIOPS_"):
            del os.environ[k]
    from sqlalchemy import text

    from aiops.storage import db
    from aiops.storage import models  # noqa: F401
    db.configure(os.environ["PROBE_PG_URL"])
    db.drop_all()
    db.create_all()
    from aiops.engine.context import PipelineContext
    from aiops.engine.types import AnomalyEvent, Incident
    from aiops.storage.hooks import StorageHooks
    from aiops.storage.repository import DetectorModels

    now = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)
    try:
        r = DetectorModels.save("p", "e", "m", "detector.zscore", {"mean": float("inf"), "std": float("nan")}, now, now,
                                100, expected_version=0, node="z")
        print(f"[1] DetectorModels.save(inf/nan state) -> {'寫入成功' if r else 'None（被當成版本衝突）'}")
        if r:
            with db.read_session() as s:
                print("    db 內容:", s.execute(text("select state::text from detector_models")).scalar())
    except Exception as e:  # noqa: BLE001
        print(f"[1] DetectorModels.save(inf/nan state) RAISES {type(e).__module__}.{type(e).__name__}: {str(e).splitlines()[0][:120]}")

    ctx = PipelineContext(pipeline="probe_inf", now=now)
    bad = AnomalyEvent("svc-bad", "m", now, float("inf"), value=float("inf"), detector="detector.mad")
    good = AnomalyEvent("svc-good", "m", now + timedelta(minutes=1), 7.0, value=120.0, detector="detector.mad")
    ctx.detected_anomalies = [bad, good]
    ctx.incidents = [Incident([bad], float("inf"), severity="P1"), Incident([good], 7.0, severity="P2")]
    try:
        StorageHooks().persist_batch(ctx)
        print("[2] persist_batch 成功")
    except Exception as e:  # noqa: BLE001
        print(f"[2] persist_batch RAISES {type(e).__module__}.{type(e).__name__}: {str(e).splitlines()[0][:120]}")
    with db.read_session() as s:
        print("    anomalies 寫入:", s.execute(text("select entity, score from anomalies order by id")).all(),
              " incidents 寫入:", s.execute(text("select count(*) from incidents")).scalar())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
