"""Q12：run_in_transaction 重試的另一個輸入污染面——COMMIT 其實在伺服器端成功了，但回應途中連線斷掉
（客戶端看到 OperationalError）。run_in_transaction 會把它當「整筆回復了」重跑 fn。

做法：真 PostgreSQL（PROBE_PG_URL），把 Session.commit 包一層：第一次呼叫先真的 commit，再丟 OperationalError。
看 persist_batch 重跑後：anomalies 有沒有變兩倍（anomalies 沒有唯一鍵）、incidents.uid 唯一鍵會不會讓重試丟 IntegrityError。
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
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.orm import Session

    from aiops.engine.context import PipelineContext
    from aiops.engine.types import AnomalyEvent, Incident
    from aiops.storage import db
    from aiops.storage import models  # noqa: F401
    from aiops.storage.hooks import StorageHooks
    db.configure(os.environ["PROBE_PG_URL"])
    db.drop_all()
    db.create_all()

    now = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)
    real_commit, state = Session.commit, {"n": 0}

    def flaky_commit(self):
        state["n"] += 1
        real_commit(self)
        if state["n"] == 1:
            raise OperationalError("COMMIT", {}, Exception("simulated: connection lost after COMMIT was sent"))

    for label, with_incident in (("A 新事件群＋3 筆異常點", True), ("B 只有 3 筆異常點（被進行中事件群吸收，沒有新事件群）", False)):
        pipeline = f"probe_commit_{int(with_incident)}"
        ctx = PipelineContext(pipeline=pipeline, now=now)
        evs = [AnomalyEvent("svc", "m", now + timedelta(minutes=i), 5.0, detector="detector.mad") for i in range(3)]
        ctx.detected_anomalies = list(evs)
        ctx.incidents = [Incident(list(evs), 5.0, severity="P2")] if with_incident else []
        state["n"] = 0
        Session.commit = flaky_commit
        try:
            StorageHooks().persist_batch(ctx)
            outcome = "persist_batch 回報成功"
        except Exception as e:  # noqa: BLE001
            outcome = f"persist_batch RAISES {type(e).__name__}: {str(e).splitlines()[0][:100]}"
        finally:
            Session.commit = real_commit
        with db.read_session() as s:
            n_anom = s.execute(text("select count(*) from anomalies where pipeline=:p"), {"p": pipeline}).scalar()
            n_inc = s.execute(text("select count(*) from incidents where pipeline=:p"), {"p": pipeline}).scalar()
        print(f"[Q12] {label}：commit 呼叫 {state['n']} 次；{outcome}")
        print(f"[Q12]   資料庫實際：anomalies={n_anom}（應 3）incidents={n_inc}（應 {int(with_incident)}）"
              f"ctx.incident_ids={dict(ctx.incident_ids)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
